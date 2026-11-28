use std::{
    ffi::OsString,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use std::os::windows::io::{AsRawHandle, FromRawHandle};

use super::{
    windows_job::KillOnCloseJob,
    windows_spawn::{
        FailurePoint, ProcessSpec, quote_windows_argument, spawn_contained, spawn_with_failure,
    },
};
use windows_sys::Win32::{
    Foundation::{WAIT_OBJECT_0, WAIT_TIMEOUT},
    System::Threading::{OpenProcess, PROCESS_SYNCHRONIZE, WaitForSingleObject},
};

const CHILD_FIXTURE_ENV: &str = "FLUXNET_JOB_OBJECT_TEST_CHILD";
const CHILD_READY_ENV: &str = "FLUXNET_JOB_OBJECT_TEST_READY_FILE";
const CHILD_CHECK_ARGS_ENV: &str = "FLUXNET_JOB_OBJECT_TEST_CHECK_ARGS";
const CHILD_GRANDCHILD_PID_ENV: &str = "FLUXNET_JOB_OBJECT_TEST_GRANDCHILD_PID_FILE";
const CHILD_GRANDCHILD_READY_ENV: &str = "FLUXNET_JOB_OBJECT_TEST_GRANDCHILD_READY_FILE";
const CHILD_MODE_ENV: &str = "FLUXNET_JOB_OBJECT_TEST_MODE";
const CHILD_EXIT_ENV: &str = "FLUXNET_JOB_OBJECT_TEST_EXIT";
const SPECIAL_ARGUMENTS: [&str; 6] = [
    "two words",
    "quote\"inside",
    "C:\\path with space\\",
    "\u{09ac}\u{be16}\u{09b2}\u{09be} \u{09ad}\u{09bf}\u{09a1}\u{09bf}\u{0993}.mp4",
    "ampersand&percent%caret^",
    "",
];

#[test]
fn job_object_can_be_created_and_closed() {
    let job = KillOnCloseJob::new().expect("Job Object setup should succeed");
    drop(job);
}

#[test]
fn closing_job_object_terminates_assigned_local_fixture() {
    let mut fixture = spawn_fixture();
    let job = KillOnCloseJob::new().expect("Job Object setup should succeed");
    job.assign_process(fixture.child.as_raw_handle())
        .expect("fixture process should be assigned to its Job Object");

    let deadline = Instant::now() + Duration::from_secs(5);
    while !fixture.ready_path.exists() && Instant::now() < deadline {
        if let Some(status) = fixture
            .child
            .try_wait()
            .expect("fixture process wait should succeed")
        {
            panic!("fixture exited before signaling readiness: {status}");
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        fixture.ready_path.exists(),
        "fixture did not signal readiness before the timeout"
    );

    // KILL_ON_JOB_CLOSE is applied by Windows when this is the last job
    // handle, so the fixture needs no shell or external executable.
    drop(job);

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match fixture
            .child
            .try_wait()
            .expect("fixture process wait should succeed")
        {
            Some(_) => break,
            None if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
            None => panic!("fixture remained alive after its Job Object was closed"),
        }
    }
}

#[test]
fn job_object_rejects_null_process_handle() {
    let job = KillOnCloseJob::new().expect("Job Object setup should succeed");
    let error = job
        .assign_process(std::ptr::null_mut())
        .expect_err("null process handles must fail closed");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

/// Harmless child fixture: the test harness launches itself directly, writes
/// a readiness marker, then waits. The parent assigns it to a Job Object and
/// verifies that closing the object terminates the fixture.
#[test]
fn job_object_test_child_fixture() {
    if std::env::var_os(CHILD_FIXTURE_ENV).is_none() {
        return;
    }

    if let Ok(fixture_url) = std::env::var("FLUXNET_JOB_OBJECT_TEST_FIXTURE_URL") {
        use std::io::Write;
        let cwd = std::env::current_dir().unwrap();
        let part = cwd.join("fluxnet-media.mp4.part");
        let existing = std::fs::read(&part).unwrap_or_default();
        if fixture_url.contains("fixture-pause-resume") {
            match existing.len() {
                0 => {
                    std::fs::write(&part, b"PARTIAL_MP4_PREFIX_BYTES_32_____").unwrap();
                    thread::sleep(Duration::from_millis(20));
                    let mut stdout = std::io::stdout();
                    stdout
                        .write_all(b"FLUXNET_PROGRESS\tdownloading\t32\t100\t100\t64.0\t1\n")
                        .unwrap();
                    stdout.flush().unwrap();
                    loop {
                        thread::sleep(Duration::from_millis(25));
                    }
                }
                32 => {
                    assert_eq!(existing, b"PARTIAL_MP4_PREFIX_BYTES_32_____");
                    let mut next = existing;
                    next.extend_from_slice(b"SECOND_CHUNK_BYTES_32___________");
                    std::fs::write(&part, &next).unwrap();
                    thread::sleep(Duration::from_millis(20));
                    let mut stdout = std::io::stdout();
                    stdout
                        .write_all(b"FLUXNET_PROGRESS\tdownloading\t64\t100\t100\t64.0\t1\n")
                        .unwrap();
                    stdout.flush().unwrap();
                    loop {
                        thread::sleep(Duration::from_millis(25));
                    }
                }
                64 => {
                    assert_eq!(
                        existing,
                        b"PARTIAL_MP4_PREFIX_BYTES_32_____SECOND_CHUNK_BYTES_32___________"
                    );
                    thread::sleep(Duration::from_millis(20));
                    let final_bytes = valid_test_muxed_mp4();
                    std::fs::write(&part, &final_bytes).unwrap();
                    std::fs::rename(&part, cwd.join("fluxnet-media.mp4")).unwrap();
                    std::process::exit(0);
                }
                other => panic!("unexpected partial size in fixture-pause-resume: {other}"),
            }
        } else if fixture_url.contains("fixture-fast-complete") {
            let final_bytes = valid_test_muxed_mp4();
            std::fs::write(&part, &final_bytes).unwrap();
            std::fs::rename(&part, cwd.join("fluxnet-media.mp4")).unwrap();
            let mut stdout = std::io::stdout();
            stdout
                .write_all(b"FLUXNET_PROGRESS\tdownloading\t100\t100\t100\t128.0\t0\n")
                .unwrap();
            stdout.flush().unwrap();
            thread::sleep(Duration::from_millis(40));
            std::process::exit(0);
        } else if fixture_url.contains("fixture-stall-idle") {
            std::fs::write(&part, b"PARTIAL_MP4_PREFIX_BYTES_32_____").unwrap();
            let mut stdout = std::io::stdout();
            stdout
                .write_all(b"FLUXNET_PROGRESS\tdownloading\t32\t100\t100\t64.0\t1\n")
                .unwrap();
            stdout.flush().unwrap();
            let executable = std::env::current_exe().expect("fixture executable should resolve");
            let ready_marker = std::env::var_os("TEMP")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| cwd.clone())
                .join("stall-grandchild.ready");
            let _grandchild = Command::new(executable)
                .arg("--exact")
                .arg("process::tests::job_object_test_child_fixture")
                .arg("--nocapture")
                .env(CHILD_FIXTURE_ENV, "wait")
                .env(CHILD_READY_ENV, ready_marker)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("stalled fixture descendant should start");
            loop {
                thread::sleep(Duration::from_secs(1));
            }
        } else if fixture_url.contains("fixture-aggregate-overflow") {
            const CHUNK_220_MIB: u64 = 220 * 1024 * 1024;
            let f1 = std::fs::File::create(&part).unwrap();
            f1.set_len(CHUNK_220_MIB).unwrap();
            let f2 = std::fs::File::create(cwd.join("sidecar-chunk.tmp")).unwrap();
            f2.set_len(CHUNK_220_MIB).unwrap();
            if let Some(temp) = std::env::var_os("TEMP") {
                let mei = std::path::PathBuf::from(temp).join("_MEI_overflow");
                std::fs::create_dir_all(&mei).unwrap();
                for name in ["chunk-a.bin", "chunk-b.bin", "chunk-c.bin"] {
                    let ft = std::fs::File::create(mei.join(name)).unwrap();
                    ft.set_len(CHUNK_220_MIB).unwrap();
                }
            }
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        } else if fixture_url.contains("fixture-split-ok")
            || fixture_url.contains("fixture-split-merge-fail")
        {
            let role = std::env::var("FLUXNET_JOB_OBJECT_TEST_STREAM_ROLE").unwrap_or_default();
            let ffmpeg = crate::media::ffmpeg::resolve_verified_ffmpeg_executable(None)
                .expect("verified LGPL ffmpeg.exe should resolve in split fixture");
            if role == "video" {
                let out = cwd.join("fluxnet-media.mp4");
                let status = Command::new(&ffmpeg)
                    .args([
                        "-nostdin",
                        "-hide_banner",
                        "-v",
                        "error",
                        "-y",
                        "-f",
                        "lavfi",
                        "-i",
                        "testsrc=duration=0.5:size=160x120:rate=10",
                        "-c:v",
                        "mpeg4",
                        "-an",
                    ])
                    .arg(&out)
                    .status()
                    .expect("generate split video-only stream");
                assert!(status.success());
            } else if fixture_url.contains("fixture-split-merge-fail") {
                std::fs::write(cwd.join("fluxnet-media.m4a"), b"CORRUPT_AUDIO_BYTES").unwrap();
            } else {
                let out = cwd.join("fluxnet-media.m4a");
                let status = Command::new(&ffmpeg)
                    .args([
                        "-nostdin",
                        "-hide_banner",
                        "-v",
                        "error",
                        "-y",
                        "-f",
                        "lavfi",
                        "-i",
                        "sine=frequency=440:duration=0.5",
                        "-c:a",
                        "aac",
                        "-vn",
                    ])
                    .arg(&out)
                    .status()
                    .expect("generate split audio-only stream");
                assert!(status.success());
            }
            let mut stdout = std::io::stdout();
            stdout
                .write_all(b"FLUXNET_PROGRESS\tdownloading\t50\t100\t100\t128.0\t0\n")
                .unwrap();
            stdout.flush().unwrap();
            std::process::exit(0);
        }
    }

    match std::env::var(CHILD_MODE_ENV).as_deref() {
        Ok("yt-dlp-json") => {
            use std::io::Write;
            let mut stdout = std::io::stdout();
            stdout.write_all(b"FXJSONSTART").unwrap();
            stdout.write_all(br#"{"title":"Fixture video","duration":20,"extractor_key":"FixtureSite","webpage_url":"https://example.test/v?token=SECRET","formats":[{"format_id":"video1080","height":1080,"width":1920,"fps":30,"vcodec":"avc1","acodec":"none","ext":"mp4","url":"https://cdn.test/v?signature=SECRET"},{"format_id":"audio","vcodec":"none","acodec":"opus","ext":"webm"}]}"#).unwrap();
            stdout.write_all(b"FXJSONEND").unwrap();
            std::process::exit(0);
        }
        Ok("yt-dlp-json-muxed") => {
            use std::io::Write;
            let mut stdout = std::io::stdout();
            stdout.write_all(b"FXJSONSTART").unwrap();
            stdout.write_all(br#"{"title":"Fixture video","duration":20,"extractor_key":"FixtureSite","webpage_url":"https://example.test/v?token=SECRET","formats":[{"format_id":"video1080","height":1080,"width":1920,"fps":30,"vcodec":"avc1","acodec":"none","ext":"mp4","url":"https://cdn.test/v?signature=SECRET"},{"format_id":"audio","vcodec":"none","acodec":"opus","ext":"webm"},{"format_id":"muxed720","height":720,"width":1280,"fps":30,"vcodec":"avc1","acodec":"mp4a","ext":"mp4","filesize":4096}]}"#).unwrap();
            stdout.write_all(b"FXJSONEND").unwrap();
            std::process::exit(0);
        }
        Ok("yt-dlp-malformed") => {
            use std::io::Write;
            let mut stdout = std::io::stdout();
            stdout.write_all(b"FXJSONSTART{").unwrap();
            stdout.write_all(b"FXJSONEND").unwrap();
            std::process::exit(0);
        }
        Ok("yt-dlp-no-formats") => {
            use std::io::Write;
            let mut stdout = std::io::stdout();
            stdout.write_all(b"FXJSONSTART").unwrap();
            stdout
                .write_all(br#"{"title":"Fixture video","formats":[]}"#)
                .unwrap();
            stdout.write_all(b"FXJSONEND").unwrap();
            std::process::exit(0);
        }
        Ok("yt-dlp-large") => {
            use std::io::Write;
            let block = [b'x'; 8192];
            for _ in 0..512 {
                std::io::stdout().write_all(&block).unwrap();
            }
            std::process::exit(0);
        }
        Ok("yt-dlp-wait") => loop {
            thread::sleep(Duration::from_secs(1));
        },
        Ok("yt-dlp-download-pause") => {
            use std::io::Write;
            if let Some(temp) = std::env::var_os("TEMP") {
                let mei = std::path::PathBuf::from(temp).join("_MEI987654");
                let _ = std::fs::create_dir(&mei);
                let _ = std::fs::write(mei.join("extracted.dll"), b"pyinstaller");
            }
            let part = std::env::current_dir()
                .unwrap()
                .join("fluxnet-media.mp4.part");
            std::fs::write(&part, b"PARTIAL_MP4_PREFIX_BYTES_32_____").unwrap();
            let mut stdout = std::io::stdout();
            stdout
                .write_all(b"FLUXNET_PROGRESS\tdownloading\t32\t100\t100\t64.0\t1\n")
                .unwrap();
            stdout.flush().unwrap();
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }
        Ok("yt-dlp-download-resume") => {
            if let Some(temp) = std::env::var_os("TEMP") {
                let mei = std::path::PathBuf::from(temp).join("_MEI987655");
                let _ = std::fs::create_dir(&mei);
                let _ = std::fs::write(mei.join("extracted.dll"), b"pyinstaller");
            }
            let cwd = std::env::current_dir().unwrap();
            let part = cwd.join("fluxnet-media.mp4.part");
            let existing = std::fs::read(&part).expect("retained .part file must exist in stage");
            assert_eq!(existing, b"PARTIAL_MP4_PREFIX_BYTES_32_____");
            let final_bytes = valid_test_muxed_mp4();
            std::fs::write(&part, &final_bytes).unwrap();
            std::fs::rename(&part, cwd.join("fluxnet-media.mp4")).unwrap();
            std::process::exit(0);
        }
        Ok("yt-dlp-download-corrupt-resume") => {
            let cwd = std::env::current_dir().unwrap();
            let part = cwd.join("fluxnet-media.mp4.part");
            let mut existing =
                std::fs::read(&part).expect("retained .part file must exist in stage");
            existing.extend_from_slice(b"INCOMPATIBLE_NON_MP4_RANGE_BYTES");
            std::fs::write(&part, &existing).unwrap();
            std::fs::rename(&part, cwd.join("fluxnet-media.mp4")).unwrap();
            std::process::exit(0);
        }
        Ok("yt-dlp-file-growth") => {
            let cwd = std::env::current_dir().unwrap();
            let part = cwd.join("fluxnet-media.mp4.part");
            let mut data = Vec::new();
            for _ in 0..6 {
                data.extend_from_slice(&[b'G'; 32]);
                std::fs::write(&part, &data).unwrap();
                thread::sleep(Duration::from_millis(85));
            }
            std::process::exit(0);
        }
        _ => {}
    }
    let ready_path = std::env::var_os(CHILD_READY_ENV)
        .expect("parent must provide a readiness path for the child fixture");
    match std::env::var(CHILD_MODE_ENV).as_deref() {
        Ok("exit") => std::process::exit(
            std::env::var(CHILD_EXIT_ENV)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
        ),
        Ok("flood") => {
            use std::io::Write;
            let chunk = [b'x'; 8192];
            for _ in 0..256 {
                std::io::stdout().write_all(&chunk).unwrap();
                std::io::stderr().write_all(&chunk).unwrap();
            }
            std::process::exit(0);
        }
        Ok("progress") => {
            use std::io::Write;
            std::io::stdout()
                .write_all(b"FLUXNET_PROGRESS\tdownloading\t25\t100\t100\t12.5\t6\n")
                .unwrap();
            std::process::exit(0);
        }
        _ => {}
    }
    if std::env::var_os(CHILD_CHECK_ARGS_ENV).is_some() {
        let received = std::env::args_os().skip(4).collect::<Vec<_>>();
        let expected = SPECIAL_ARGUMENTS
            .iter()
            .map(OsString::from)
            .collect::<Vec<_>>();
        assert_eq!(received, expected, "Windows argv did not round-trip");
    }
    let grandchild = match std::env::var_os(CHILD_GRANDCHILD_PID_ENV) {
        Some(pid_path) => {
            let grandchild_ready = std::env::var_os(CHILD_GRANDCHILD_READY_ENV)
                .expect("grandchild readiness path should be supplied");
            let executable = std::env::current_exe().expect("fixture executable should resolve");
            let child = Command::new(executable)
                .arg("--exact")
                .arg("process::tests::job_object_test_child_fixture")
                .arg("--nocapture")
                .env(CHILD_FIXTURE_ENV, "wait")
                .env(CHILD_READY_ENV, grandchild_ready)
                .env_remove(CHILD_CHECK_ARGS_ENV)
                .env_remove(CHILD_GRANDCHILD_PID_ENV)
                .env_remove(CHILD_GRANDCHILD_READY_ENV)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("fixture descendant should start without a shell");
            std::fs::write(pid_path, child.id().to_string())
                .expect("grandchild PID marker should be written");
            Some(child)
        }
        None => None,
    };
    std::fs::write(ready_path, b"ready").expect("fixture readiness marker should be written");
    if std::env::var(CHILD_MODE_ENV).as_deref() == Ok("spawn-exit") {
        std::process::exit(0);
    }

    loop {
        let _keep_descendant_handle = &grandchild;
        thread::sleep(Duration::from_secs(1));
    }
}

#[test]
fn supervisor_classifies_zero_and_nonzero_exit_and_bounds_both_streams() {
    use super::supervisor::{ProcessOutcome, run};
    use crate::download_manager::DownloadTaskControl;
    for (code, expected) in [(0, 0), (7, 7)] {
        let (mut spec, _) = process_fixture_spec();
        spec.args.truncate(3);
        spec.environment.retain(|(k, _)| {
            k != CHILD_CHECK_ARGS_ENV
                && k != CHILD_GRANDCHILD_PID_ENV
                && k != CHILD_GRANDCHILD_READY_ENV
        });
        spec.environment
            .push((OsString::from(CHILD_MODE_ENV), OsString::from("exit")));
        spec.environment.push((
            OsString::from(CHILD_EXIT_ENV),
            OsString::from(code.to_string()),
        ));
        let outcome =
            run(&spec, &DownloadTaskControl::default()).expect("fixture should be supervised");
        match outcome {
            ProcessOutcome::Exited { code, .. } => assert_eq!(code, expected),
            other => panic!("unexpected outcome: {other:?}"),
        }
    }
    let (mut spec, _) = process_fixture_spec();
    spec.args.truncate(3);
    spec.environment.retain(|(k, _)| {
        k != CHILD_CHECK_ARGS_ENV
            && k != CHILD_GRANDCHILD_PID_ENV
            && k != CHILD_GRANDCHILD_READY_ENV
    });
    spec.environment
        .push((OsString::from(CHILD_MODE_ENV), OsString::from("flood")));
    spec.stdout_limit = 1024;
    spec.stderr_limit = 1536;
    let outcome =
        run(&spec, &DownloadTaskControl::default()).expect("flood should be drained concurrently");
    match outcome {
        ProcessOutcome::Exited { stdout, stderr, .. } => {
            assert_eq!(stdout.bytes.len(), 1024);
            assert!(stdout.truncated);
            assert_eq!(stderr.bytes.len(), 1536);
            assert!(stderr.truncated);
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[test]
fn supervisor_timeout_and_cancellation_terminate_fixture() {
    use super::supervisor::{ProcessOutcome, run};
    use crate::download_manager::DownloadTaskControl;
    let (mut spec, _) = process_fixture_spec();
    spec.args.truncate(3);
    spec.environment.retain(|(k, _)| {
        k != CHILD_CHECK_ARGS_ENV
            && k != CHILD_GRANDCHILD_PID_ENV
            && k != CHILD_GRANDCHILD_READY_ENV
    });
    spec.timeout = Duration::from_millis(150);
    assert!(matches!(
        run(&spec, &DownloadTaskControl::default()).unwrap(),
        ProcessOutcome::TimedOut { .. }
    ));

    let (mut spec, _) = process_fixture_spec();
    spec.args.truncate(3);
    spec.environment.retain(|(k, _)| {
        k != CHILD_CHECK_ARGS_ENV
            && k != CHILD_GRANDCHILD_PID_ENV
            && k != CHILD_GRANDCHILD_READY_ENV
    });
    let control = DownloadTaskControl::default();
    let cancellation = std::sync::Arc::new(control);
    let thread_control = cancellation.clone();
    let worker = std::thread::spawn(move || run(&spec, &thread_control));
    std::thread::sleep(Duration::from_millis(250));
    cancellation.request_cancel();
    assert!(matches!(
        worker.join().unwrap().unwrap(),
        ProcessOutcome::Cancelled { .. }
    ));
}

#[test]
fn cancellation_wins_when_accepted_during_child_exit() {
    use super::supervisor::{ProcessOutcome, run};
    use crate::download_manager::DownloadTaskControl;
    let (mut spec, _) = process_fixture_spec();
    spec.args.truncate(3);
    spec.environment.retain(|(k, _)| {
        k != CHILD_CHECK_ARGS_ENV
            && k != CHILD_GRANDCHILD_PID_ENV
            && k != CHILD_GRANDCHILD_READY_ENV
    });
    spec.environment
        .push((OsString::from(CHILD_MODE_ENV), OsString::from("flood")));
    let control = std::sync::Arc::new(DownloadTaskControl::default());
    let child_control = control.clone();
    let worker = thread::spawn(move || run(&spec, &child_control));
    thread::sleep(Duration::from_millis(5));
    control.request_cancel();
    assert!(matches!(
        worker.join().unwrap().unwrap(),
        ProcessOutcome::Cancelled { .. }
    ));
}

#[test]
fn supervisor_pause_terminates_tree_releases_files_and_captures_output() {
    use super::supervisor::{ProcessOutcome, run};
    use crate::download_manager::DownloadTaskControl;

    let (mut spec, ready_path) = process_fixture_spec();
    spec.args.truncate(3);
    spec.environment.retain(|(k, _)| k != CHILD_CHECK_ARGS_ENV);
    let grandchild_pid_path = ready_path.with_extension("grandchild.pid");
    let grandchild_ready_path = ready_path.with_extension("grandchild.ready");

    let control = std::sync::Arc::new(DownloadTaskControl::default());
    let worker_control = std::sync::Arc::clone(&control);
    let worker = thread::spawn(move || run(&spec, &worker_control));

    let deadline = Instant::now() + Duration::from_secs(5);
    while (!ready_path.exists() || !grandchild_pid_path.exists() || !grandchild_ready_path.exists())
        && Instant::now() < deadline
    {
        thread::sleep(Duration::from_millis(15));
    }
    assert!(
        ready_path.exists() && grandchild_pid_path.exists() && grandchild_ready_path.exists(),
        "fixture and descendant did not signal readiness before pause"
    );

    control.request_pause();
    assert!(control.is_pause_requested());

    let outcome = worker
        .join()
        .unwrap()
        .expect("supervisor should return Paused outcome");
    assert!(matches!(outcome, ProcessOutcome::Paused { .. }));

    let grandchild_pid = std::fs::read_to_string(&grandchild_pid_path)
        .expect("fixture should write descendant PID")
        .parse::<u32>()
        .unwrap();
    assert_process_terminated(grandchild_pid);

    std::fs::remove_file(&ready_path)
        .expect("ready marker file handle must be released after pause");
    std::fs::remove_file(&grandchild_pid_path)
        .expect("grandchild pid file handle must be released after pause");
    std::fs::remove_file(&grandchild_ready_path)
        .expect("grandchild ready file handle must be released after pause");
}

#[test]
fn supervisor_cancellation_takes_priority_over_pause_before_and_during_execution() {
    use super::supervisor::{ProcessOutcome, run};
    use crate::download_manager::DownloadTaskControl;

    // 1. Pre-requested pause + cancel before spawn: cancellation must win.
    let (mut spec, _) = process_fixture_spec();
    spec.args.truncate(3);
    spec.environment.retain(|(k, _)| {
        k != CHILD_CHECK_ARGS_ENV
            && k != CHILD_GRANDCHILD_PID_ENV
            && k != CHILD_GRANDCHILD_READY_ENV
    });
    let pre_both = DownloadTaskControl::default();
    pre_both.request_pause();
    pre_both.request_cancel();
    assert!(matches!(
        run(&spec, &pre_both).unwrap(),
        ProcessOutcome::Cancelled { .. }
    ));

    // 2. Pre-requested pause alone before spawn returns Paused immediately without spawning.
    let pre_pause = DownloadTaskControl::default();
    pre_pause.request_pause();
    assert!(matches!(
        run(&spec, &pre_pause).unwrap(),
        ProcessOutcome::Paused { .. }
    ));

    // 3. Pause requested while running, raced with cancellation during shutdown: cancellation must win.
    let (mut spec, ready_path) = process_fixture_spec();
    spec.args.truncate(3);
    spec.environment.retain(|(k, _)| k != CHILD_CHECK_ARGS_ENV);
    let grandchild_pid_path = ready_path.with_extension("grandchild.pid");
    let grandchild_ready_path = ready_path.with_extension("grandchild.ready");
    let control = std::sync::Arc::new(DownloadTaskControl::default());
    let worker_control = std::sync::Arc::clone(&control);
    let worker = thread::spawn(move || run(&spec, &worker_control));

    let deadline = Instant::now() + Duration::from_secs(5);
    while (!ready_path.exists() || !grandchild_pid_path.exists() || !grandchild_ready_path.exists())
        && Instant::now() < deadline
    {
        thread::sleep(Duration::from_millis(15));
    }
    assert!(ready_path.exists());

    control.request_pause();
    control.request_cancel();

    let outcome = worker.join().unwrap().unwrap();
    assert!(
        matches!(outcome, ProcessOutcome::Cancelled { .. }),
        "cancellation must always take priority over pause"
    );

    let grandchild_pid = std::fs::read_to_string(&grandchild_pid_path)
        .unwrap()
        .parse::<u32>()
        .unwrap();
    assert_process_terminated(grandchild_pid);

    let _ = std::fs::remove_file(&ready_path);
    let _ = std::fs::remove_file(&grandchild_pid_path);
    let _ = std::fs::remove_file(&grandchild_ready_path);
}

#[test]
fn supervisor_kills_descendant_after_root_exits_successfully() {
    use super::supervisor::{ProcessOutcome, run};
    use crate::download_manager::DownloadTaskControl;
    let (mut spec, _) = process_fixture_spec();
    spec.args.truncate(3);
    spec.environment.retain(|(k, _)| k != CHILD_CHECK_ARGS_ENV);
    spec.environment
        .push((OsString::from(CHILD_MODE_ENV), OsString::from("spawn-exit")));
    let pid_path = spec
        .environment
        .iter()
        .find(|(k, _)| k == CHILD_GRANDCHILD_PID_ENV)
        .unwrap()
        .1
        .clone();
    let result = run(&spec, &DownloadTaskControl::default())
        .expect("supervisor should reap parent and descendants");
    assert!(matches!(result, ProcessOutcome::Exited { code: 0, .. }));
    let pid = std::fs::read_to_string(pid_path)
        .expect("fixture should write descendant PID")
        .parse()
        .unwrap();
    assert_process_terminated(pid);
}

#[test]
fn supervisor_streams_output_and_enforces_task_output_size_limit() {
    use super::supervisor::{MonitoredOutput, ProcessOutcome, ProcessStream, run_with_output};
    use crate::download_manager::DownloadTaskControl;
    use std::sync::{Arc, Mutex};

    let (mut spec, _) = process_fixture_spec();
    spec.args.truncate(3);
    spec.environment.retain(|(k, _)| {
        k != CHILD_CHECK_ARGS_ENV
            && k != CHILD_GRANDCHILD_PID_ENV
            && k != CHILD_GRANDCHILD_READY_ENV
    });
    spec.environment
        .push((OsString::from(CHILD_MODE_ENV), OsString::from("progress")));
    let seen = Arc::new(Mutex::new(Vec::<(ProcessStream, Vec<u8>)>::new()));
    let callback_seen = Arc::clone(&seen);
    let handler = Arc::new(move |stream, bytes: &[u8]| {
        callback_seen.lock().unwrap().push((stream, bytes.to_vec()));
    });
    assert!(matches!(
        run_with_output(&spec, &DownloadTaskControl::default(), handler, None,).unwrap(),
        ProcessOutcome::Exited { code: 0, .. }
    ));
    assert!(
        seen.lock()
            .unwrap()
            .iter()
            .any(|(stream, bytes)| *stream == ProcessStream::Stdout
                && bytes.starts_with(b"FLUXNET_PROGRESS\t"))
    );

    let monitored_directory = spec.working_directory.join(format!(
        "fluxnet-output-limit-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&monitored_directory).unwrap();
    std::fs::write(
        monitored_directory.join("fluxnet-media.mp4.part"),
        [0u8; 11],
    )
    .unwrap();
    spec.environment.retain(|(key, _)| key != CHILD_MODE_ENV);
    spec.environment
        .push((OsString::from(CHILD_MODE_ENV), OsString::from("wait")));
    let outcome = run_with_output(
        &spec,
        &DownloadTaskControl::default(),
        Arc::new(|_, _| {}),
        Some(MonitoredOutput {
            directory: monitored_directory.clone(),
            filename_prefix: "fluxnet-media.".into(),
            maximum_bytes: 10,
        }),
    )
    .unwrap();
    assert!(matches!(
        outcome,
        ProcessOutcome::OutputLimitExceeded { .. }
    ));
    std::fs::remove_dir_all(monitored_directory).unwrap();

    // Gap 5: Many files across stage + nested runtime/temp summing over 1 GiB (MAX_VIDEO_DOWNLOAD_BYTES)
    // must trigger OutputLimitExceeded even when no single file exceeds 1 GiB!
    const ONE_GIB: u64 = 1024 * 1024 * 1024;
    const CHUNK_220_MIB: u64 = 220 * 1024 * 1024; // 5 * 220 MiB = 1,100 MiB > 1,024 MiB (1 GiB), each < 1 GiB
    let aggregate_stage = std::env::temp_dir().join(format!(
        "fluxnet-aggregate-1gib-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let nested_runtime = aggregate_stage
        .join(".fluxnet-yt-dlp-runtime-sim")
        .join("_MEI_nested");
    std::fs::create_dir_all(&nested_runtime).unwrap();
    for name in ["fluxnet-media.mp4.part", "sidecar-frag-1.tmp"] {
        let f = std::fs::File::create(aggregate_stage.join(name)).unwrap();
        f.set_len(CHUNK_220_MIB).unwrap();
    }
    for name in ["temp-a.bin", "temp-b.bin", "temp-c.bin"] {
        let f = std::fs::File::create(nested_runtime.join(name)).unwrap();
        f.set_len(CHUNK_220_MIB).unwrap();
    }
    let agg_outcome = run_with_output(
        &spec,
        &DownloadTaskControl::default(),
        Arc::new(|_, _| {}),
        Some(MonitoredOutput {
            directory: aggregate_stage.clone(),
            filename_prefix: "fluxnet-media.".into(),
            maximum_bytes: ONE_GIB,
        }),
    )
    .unwrap();
    assert!(
        matches!(agg_outcome, ProcessOutcome::OutputLimitExceeded { .. }),
        "5 x 220 MiB files across stage + runtime summing over 1 GiB must trigger OutputLimitExceeded; got {agg_outcome:?}"
    );
    std::fs::remove_dir_all(aggregate_stage).unwrap();
}

#[test]
fn process_creation_failure_does_not_start_a_child() {
    let (spec, ready_path) = process_fixture_spec();
    let mut created_pid = None;
    let result = spawn_with_failure(&spec, FailurePoint::Create, &mut created_pid);
    assert!(result.is_err());
    assert!(created_pid.is_none());
    assert!(!ready_path.exists());
}

#[test]
fn native_process_creation_failure_returns_without_a_child() {
    let (mut spec, ready_path) = process_fixture_spec();
    spec.executable = spec
        .working_directory
        .join("missing-fixture-executable.exe");
    let error = match spawn_contained(&spec) {
        Ok(_) => panic!("missing executable unexpectedly started"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
    assert!(!ready_path.exists());
}

#[test]
fn assignment_failure_terminates_the_suspended_child() {
    let (spec, ready_path) = process_fixture_spec();
    let mut created_pid = None;
    let result = spawn_with_failure(&spec, FailurePoint::Assign, &mut created_pid);
    assert!(result.is_err());
    assert!(
        !ready_path.exists(),
        "unassigned child must never be resumed"
    );
    assert_process_terminated(created_pid.expect("process was created before assignment"));
}

#[test]
fn resume_failure_terminates_the_assigned_suspended_child() {
    let (spec, ready_path) = process_fixture_spec();
    let mut created_pid = None;
    let result = spawn_with_failure(&spec, FailurePoint::Resume, &mut created_pid);
    assert!(result.is_err());
    assert!(!ready_path.exists(), "failed resume must not run the child");
    assert_process_terminated(created_pid.expect("process was created before resume"));
}

#[test]
fn contained_spawn_resumes_only_after_assignment_and_closes_with_job() {
    let (spec, ready_path) = process_fixture_spec();
    let grandchild_pid_path = ready_path.with_extension("grandchild.pid");
    let grandchild_ready_path = ready_path.with_extension("grandchild.ready");
    let child = spawn_contained(&spec).expect("contained fixture should start");
    let process_id = child.process_id;
    let deadline = Instant::now() + Duration::from_secs(5);
    while (!ready_path.exists() || !grandchild_pid_path.exists() || !grandchild_ready_path.exists())
        && Instant::now() < deadline
    {
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        ready_path.exists() && grandchild_pid_path.exists() && grandchild_ready_path.exists(),
        "resumed fixture and its contained descendant should signal readiness"
    );
    let grandchild_pid = std::fs::read_to_string(&grandchild_pid_path)
        .expect("grandchild PID should be recorded")
        .parse::<u32>()
        .expect("grandchild PID should be numeric");
    drop(child);
    assert_process_terminated(process_id);
    assert_process_terminated(grandchild_pid);
    let _ = std::fs::remove_file(ready_path);
    let _ = std::fs::remove_file(grandchild_pid_path);
    let _ = std::fs::remove_file(grandchild_ready_path);
}

#[test]
fn windows_argument_quoting_handles_unicode_quotes_and_trailing_slashes() {
    let cases = [
        ("", "\"\""),
        ("two words", "\"two words\""),
        ("say\"hi", "\"say\\\"hi\""),
        ("C:\\path with space\\", "\"C:\\path with space\\\\\""),
        ("বাংলা ভিডিও.mp4", "\"বাংলা ভিডিও.mp4\""),
    ];
    for (input, expected) in cases {
        let actual =
            String::from_utf16(&quote_windows_argument(std::ffi::OsStr::new(input)).unwrap())
                .expect("quoted UTF-16 should remain valid");
        assert_eq!(actual, expected);
    }
}

fn valid_test_muxed_mp4() -> Vec<u8> {
    fn atom(kind: &[u8; 4], payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(payload.len() + 8);
        out.extend_from_slice(&u32::try_from(payload.len() + 8).unwrap().to_be_bytes());
        out.extend_from_slice(kind);
        out.extend_from_slice(payload);
        out
    }
    let stbl = {
        let mut s = atom(b"stsd", &[0; 8]);
        s.extend(atom(b"stts", &[0; 4]));
        s.extend(atom(b"stsc", &[0; 4]));
        s.extend(atom(b"stsz", &[0; 8]));
        s.extend(atom(b"stco", &[0; 8]));
        atom(b"stbl", &s)
    };
    let minf = atom(b"minf", &stbl);
    let track = |handler: &[u8; 4]| {
        let mut hdlr = vec![0u8; 8];
        hdlr.extend_from_slice(handler);
        let mut mdia = atom(b"mdhd", &[0; 8]);
        mdia.extend(atom(b"hdlr", &hdlr));
        mdia.extend(&minf);
        atom(b"trak", &atom(b"mdia", &mdia))
    };
    let mut moov = atom(b"mvhd", &[0; 8]);
    moov.extend(track(b"vide"));
    moov.extend(track(b"soun"));
    let mut bytes = atom(b"ftyp", b"isom\0\0\0\0isom");
    bytes.extend(atom(b"moov", &moov));
    bytes.extend(atom(
        b"mdat",
        b"PARTIAL_MP4_PREFIX_BYTES_32_____RESUMED_TAIL",
    ));
    bytes
}

fn process_fixture_spec() -> (ProcessSpec, std::path::PathBuf) {
    process_fixture_spec_for_integration()
}

pub(crate) fn process_fixture_spec_for_integration() -> (ProcessSpec, std::path::PathBuf) {
    let executable = std::env::current_exe().expect("test executable path should resolve");
    let working_directory = executable
        .parent()
        .expect("test executable should have a parent directory")
        .to_path_buf();
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let ready_path = std::env::temp_dir().join(format!(
        "fluxnet-suspended-process-test-{}-{unique}.ready",
        std::process::id()
    ));
    let mut environment = Vec::<(OsString, OsString)>::new();
    for key in ["SystemRoot", "WINDIR", "TEMP", "TMP", "PATH"] {
        if let Some(value) = std::env::var_os(key) {
            environment.push((OsString::from(key), value));
        }
    }
    environment.push((OsString::from(CHILD_FIXTURE_ENV), OsString::from("wait")));
    environment.push((
        OsString::from(CHILD_READY_ENV),
        ready_path.clone().into_os_string(),
    ));
    environment.push((OsString::from(CHILD_CHECK_ARGS_ENV), OsString::from("1")));
    environment.push((
        OsString::from(CHILD_GRANDCHILD_PID_ENV),
        ready_path.with_extension("grandchild.pid").into_os_string(),
    ));
    environment.push((
        OsString::from(CHILD_GRANDCHILD_READY_ENV),
        ready_path
            .with_extension("grandchild.ready")
            .into_os_string(),
    ));
    let mut args = vec![
        OsString::from("--exact"),
        OsString::from("process::tests::job_object_test_child_fixture"),
        OsString::from("--nocapture"),
    ];
    args.extend(SPECIAL_ARGUMENTS.iter().map(OsString::from));
    (
        ProcessSpec {
            executable,
            args,
            working_directory,
            environment,
            timeout: Duration::from_secs(30),
            idle_timeout: None,
            stdout_limit: 64 * 1024,
            stderr_limit: 64 * 1024,
        },
        ready_path,
    )
}

#[test]
fn supervisor_idle_timeout_resets_on_file_growth_and_kills_stalled_tree() {
    use super::supervisor::{MonitoredOutput, ProcessOutcome, run, run_with_output};
    use crate::download_manager::DownloadTaskControl;
    use std::sync::Arc;

    // 1. Stalled process with 45-minute wall timeout fails fast on idle_timeout and kills grandchild.
    let (mut spec, ready_path) = process_fixture_spec();
    spec.args.truncate(3);
    spec.environment.retain(|(k, _)| k != CHILD_CHECK_ARGS_ENV);
    let grandchild_pid_path = ready_path.with_extension("grandchild.pid");
    let grandchild_ready_path = ready_path.with_extension("grandchild.ready");
    spec.timeout = Duration::from_secs(45 * 60);
    spec.idle_timeout = Some(Duration::from_millis(280));

    let started = Instant::now();
    let outcome = run(&spec, &DownloadTaskControl::default()).unwrap();
    assert!(
        matches!(outcome, ProcessOutcome::IdleTimedOut { .. }),
        "expected IdleTimedOut, got {outcome:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "idle timeout must fail fast instead of waiting for 45-minute wall timeout"
    );
    if let Ok(pid_text) = std::fs::read_to_string(&grandchild_pid_path) {
        if let Ok(grandchild_pid) = pid_text.trim().parse::<u32>() {
            assert_process_terminated(grandchild_pid);
        }
    }
    let _ = std::fs::remove_file(&ready_path);
    let _ = std::fs::remove_file(&grandchild_pid_path);
    let _ = std::fs::remove_file(&grandchild_ready_path);

    // 2. Active process growing staged output every 90ms (~360ms total) resets 220ms idle timer and exits 0.
    let growth_dir = std::env::temp_dir().join(format!(
        "fluxnet-idle-growth-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&growth_dir).unwrap();
    let (mut growth_spec, _) = process_fixture_spec();
    growth_spec.args.truncate(3);
    growth_spec.working_directory = growth_dir.clone();
    growth_spec.environment.retain(|(k, _)| {
        k != CHILD_CHECK_ARGS_ENV
            && k != CHILD_GRANDCHILD_PID_ENV
            && k != CHILD_GRANDCHILD_READY_ENV
            && k != CHILD_READY_ENV
    });
    growth_spec.environment.push((
        OsString::from(CHILD_MODE_ENV),
        OsString::from("yt-dlp-file-growth"),
    ));
    growth_spec.timeout = Duration::from_secs(10);
    growth_spec.idle_timeout = Some(Duration::from_millis(280));
    let growth_outcome = run_with_output(
        &growth_spec,
        &DownloadTaskControl::default(),
        Arc::new(|_, _| {}),
        Some(MonitoredOutput {
            directory: growth_dir.clone(),
            filename_prefix: "fluxnet-media.".into(),
            maximum_bytes: 1024 * 1024,
        }),
    )
    .unwrap();
    assert!(
        matches!(growth_outcome, ProcessOutcome::Exited { code: 0, .. }),
        "staged file byte growth must reset idle timer; got {growth_outcome:?}"
    );
    let _ = std::fs::remove_dir_all(growth_dir);
}

pub(crate) fn assert_process_terminated(process_id: u32) {
    // SAFETY: OpenProcess returns an owned handle on success. SYNCHRONIZE is
    // sufficient for waiting; the process is our harmless test fixture.
    let process = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, process_id) };
    if process.is_null() {
        // ERROR_INVALID_PARAMETER means the process object has already exited
        // and its ID is no longer openable. Other errors are test failures.
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(87),
            "failed to inspect child process termination"
        );
        return;
    }
    let process = unsafe { std::os::windows::io::OwnedHandle::from_raw_handle(process.cast()) };
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        // SAFETY: the handle is live and SYNCHRONIZE access was requested.
        let wait = unsafe { WaitForSingleObject(process.as_raw_handle() as _, 0) };
        if wait == WAIT_OBJECT_0 {
            break;
        }
        assert_eq!(wait, WAIT_TIMEOUT, "waiting for fixture termination failed");
        assert!(
            Instant::now() < deadline,
            "test child is still running after cleanup"
        );
        thread::sleep(Duration::from_millis(20));
    }
    drop(process);
}

struct FixtureProcess {
    child: Child,
    ready_path: std::path::PathBuf,
}

impl Drop for FixtureProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        let _ = std::fs::remove_file(&self.ready_path);
    }
}

fn spawn_fixture() -> FixtureProcess {
    let executable = std::env::current_exe().expect("test executable path should resolve");
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let ready_path = std::env::temp_dir().join(format!(
        "fluxnet-job-object-test-{}-{unique}.ready",
        std::process::id()
    ));
    let child = Command::new(executable)
        .arg("--exact")
        .arg("process::tests::job_object_test_child_fixture")
        .arg("--nocapture")
        .env(CHILD_FIXTURE_ENV, "wait")
        .env(CHILD_READY_ENV, &ready_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("local test fixture should start without a shell");
    FixtureProcess { child, ready_path }
}
