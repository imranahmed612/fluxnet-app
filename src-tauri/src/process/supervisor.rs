//! Bounded, contained Windows child-process supervision.

use std::{
    io::{self, Read},
    os::windows::io::AsRawHandle,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use windows_sys::Win32::{
    Foundation::{HANDLE, WAIT_OBJECT_0, WAIT_TIMEOUT},
    System::Threading::{GetExitCodeProcess, TerminateProcess, WaitForSingleObject},
};

use super::windows_spawn::{ProcessSpec, spawn_contained};
use crate::download_manager::DownloadTaskControl;

const POLL: Duration = Duration::from_millis(15);
const JOB_TERMINATION_TIMEOUT: Duration = Duration::from_secs(30);
const EXIT_TERMINATED: u32 = 0xC000_013A;
#[allow(dead_code)]
pub(crate) const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CapturedOutput {
    pub bytes: Vec<u8>,
    pub truncated: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ProcessOutcome {
    Exited {
        code: u32,
        stdout: CapturedOutput,
        stderr: CapturedOutput,
    },
    Cancelled {
        stdout: CapturedOutput,
        stderr: CapturedOutput,
    },
    Paused {
        stdout: CapturedOutput,
        stderr: CapturedOutput,
    },
    TimedOut {
        stdout: CapturedOutput,
        stderr: CapturedOutput,
    },
    IdleTimedOut {
        stdout: CapturedOutput,
        stderr: CapturedOutput,
    },
    OutputLimitExceeded {
        stdout: CapturedOutput,
        stderr: CapturedOutput,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProcessStream {
    Stdout,
    Stderr,
}

pub(crate) type OutputHandler = Arc<dyn Fn(ProcessStream, &[u8]) + Send + Sync>;

#[derive(Clone, Debug)]
pub(crate) struct MonitoredOutput {
    pub directory: PathBuf,
    pub filename_prefix: String,
    pub maximum_bytes: u64,
}

/// Runs an already validated specification in a Job Object. It uses the
/// manager's existing cancellation flag; this module owns no queue/token.
pub(crate) fn run(spec: &ProcessSpec, control: &DownloadTaskControl) -> io::Result<ProcessOutcome> {
    run_with_hooks(spec, control, None, None)
}

pub(crate) fn run_with_output(
    spec: &ProcessSpec,
    control: &DownloadTaskControl,
    handler: OutputHandler,
    monitored_output: Option<MonitoredOutput>,
) -> io::Result<ProcessOutcome> {
    run_with_hooks(spec, control, Some(handler), monitored_output)
}

fn run_with_hooks(
    spec: &ProcessSpec,
    control: &DownloadTaskControl,
    handler: Option<OutputHandler>,
    monitored_output: Option<MonitoredOutput>,
) -> io::Result<ProcessOutcome> {
    if spec.stdout_limit > 4 * 1024 * 1024
        || spec.stderr_limit > 4 * 1024 * 1024
        || spec.timeout.is_zero()
        || spec.idle_timeout.is_some_and(|idle| idle.is_zero())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid process output or timeout limit",
        ));
    }
    if control.is_cancelled() {
        return Ok(ProcessOutcome::Cancelled {
            stdout: CapturedOutput {
                bytes: Vec::new(),
                truncated: false,
            },
            stderr: CapturedOutput {
                bytes: Vec::new(),
                truncated: false,
            },
        });
    }
    if control.is_pause_requested() {
        return Ok(ProcessOutcome::Paused {
            stdout: CapturedOutput {
                bytes: Vec::new(),
                truncated: false,
            },
            stderr: CapturedOutput {
                bytes: Vec::new(),
                truncated: false,
            },
        });
    }
    let mut max_staged_bytes = match monitored_output.as_ref().map(output_exceeds_limit) {
        Some(MonitoredOutputStatus::WithinLimit { staged_bytes }) => staged_bytes,
        _ => 0,
    };
    let child = spawn_contained(spec)?;
    let super::windows_spawn::ContainedProcess {
        _job: job,
        _process: process,
        process_id: _,
        stdout: stdout_pipe,
        stderr: stderr_pipe,
    } = child;
    let started_at = Instant::now();
    let last_activity_ns = Arc::new(AtomicU64::new(0));
    let stdout = match drain(
        stdout_pipe,
        spec.stdout_limit,
        ProcessStream::Stdout,
        handler.clone(),
        Some((started_at, Arc::clone(&last_activity_ns))),
    ) {
        Ok(reader) => reader,
        Err(error) => {
            terminate_and_reap(&job, &process)?;
            return Err(error);
        }
    };
    let stderr = match drain(
        stderr_pipe,
        spec.stderr_limit,
        ProcessStream::Stderr,
        handler,
        Some((started_at, Arc::clone(&last_activity_ns))),
    ) {
        Ok(reader) => reader,
        Err(error) => {
            terminate_and_reap(&job, &process)?;
            let _ = stdout.join();
            return Err(error);
        }
    };
    let deadline = started_at + spec.timeout;
    let mut next_output_check = Instant::now();
    let mut terminal = None;
    loop {
        if control.is_cancelled() {
            terminal = Some(ProcessOutcomeTag::Cancelled);
            break;
        }
        if control.is_pause_requested() {
            terminal = Some(ProcessOutcomeTag::Paused);
            break;
        }
        if Instant::now() >= deadline {
            terminal = Some(ProcessOutcomeTag::TimedOut);
            break;
        }
        if Instant::now() >= next_output_check {
            if let Some(monitored) = monitored_output.as_ref() {
                match output_exceeds_limit(monitored) {
                    MonitoredOutputStatus::ExceedsLimit => {
                        terminal = Some(ProcessOutcomeTag::OutputLimitExceeded);
                        break;
                    }
                    MonitoredOutputStatus::WithinLimit { staged_bytes } => {
                        if staged_bytes > max_staged_bytes {
                            max_staged_bytes = staged_bytes;
                            record_activity(started_at, &last_activity_ns);
                        }
                    }
                }
            }
            next_output_check = Instant::now() + Duration::from_millis(100);
        }
        if let Some(idle_timeout) = spec.idle_timeout {
            let last_ns = last_activity_ns.load(Ordering::Acquire);
            let now_ns = started_at.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            if Duration::from_nanos(now_ns.saturating_sub(last_ns)) >= idle_timeout {
                terminal = Some(ProcessOutcomeTag::IdleTimedOut);
                break;
            }
        }
        // SAFETY: supervisor owns a valid process handle.
        match unsafe { WaitForSingleObject(process.as_raw_handle() as HANDLE, 0) } {
            WAIT_OBJECT_0 => break,
            WAIT_TIMEOUT => thread::sleep(POLL),
            _ => {
                terminal = Some(ProcessOutcomeTag::Failure);
                break;
            }
        }
    }
    // Even when the root exits successfully, an extractor could leave a
    // descendant running. Terminate and wait for the whole private job before
    // returning so task-scoped temporary files can be cleaned safely.
    if let Err(error) = job.terminate_and_wait(EXIT_TERMINATED, JOB_TERMINATION_TIMEOUT) {
        // Kill-on-close remains the fallback if explicit job termination or
        // accounting failed. We still reap the root; callers treat this error
        // as an unproven tree shutdown and retain task temporary evidence.
        drop(job);
        // SAFETY: process remains owned and live or signaled.
        let mut wait = unsafe { WaitForSingleObject(process.as_raw_handle() as HANDLE, 30_000) };
        if wait != WAIT_OBJECT_0 {
            // SAFETY: process remains valid; this is the root-only fallback.
            unsafe { TerminateProcess(process.as_raw_handle() as HANDLE, EXIT_TERMINATED) };
            // SAFETY: process handle remains valid.
            wait = unsafe { WaitForSingleObject(process.as_raw_handle() as HANDLE, 30_000) };
        }
        let _ = stdout.join();
        let _ = stderr.join();
        if wait != WAIT_OBJECT_0 {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "could not reap process after Job Object termination failed",
            ));
        }
        return Err(error);
    }
    // Always wait/reap root before returning. The child remains contained while
    // this wait occurs and kill-on-close is a final cleanup backstop.
    // SAFETY: live owned process handle; termination was requested when needed.
    let wait = unsafe { WaitForSingleObject(process.as_raw_handle() as HANDLE, 30_000) };
    if wait != WAIT_OBJECT_0 {
        // SAFETY: process handle remains owned and valid.
        unsafe {
            TerminateProcess(process.as_raw_handle() as HANDLE, EXIT_TERMINATED);
        }
        // SAFETY: wait again after forced root termination.
        if unsafe { WaitForSingleObject(process.as_raw_handle() as HANDLE, 30_000) }
            != WAIT_OBJECT_0
        {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "could not reap contained process",
            ));
        }
    }
    let mut code = 0u32;
    // SAFETY: process has signaled; output pointer is valid.
    if unsafe { GetExitCodeProcess(process.as_raw_handle() as HANDLE, &mut code) } == 0 {
        return Err(io::Error::last_os_error());
    }
    drop(job); // kill any surviving descendants and close pipe writers.
    drop(process);
    let stdout = stdout
        .join()
        .map_err(|_| io::Error::other("stdout drain panicked"))??;
    let stderr = stderr
        .join()
        .map_err(|_| io::Error::other("stderr drain panicked"))??;
    if control.is_cancelled() {
        terminal = Some(ProcessOutcomeTag::Cancelled);
    } else if control.is_pause_requested() && terminal.is_none() {
        terminal = Some(ProcessOutcomeTag::Paused);
    }
    let (stdout, stderr) = (stdout, stderr);
    Ok(match terminal {
        Some(ProcessOutcomeTag::Cancelled) => ProcessOutcome::Cancelled { stdout, stderr },
        Some(ProcessOutcomeTag::Paused) => ProcessOutcome::Paused { stdout, stderr },
        Some(ProcessOutcomeTag::TimedOut) => ProcessOutcome::TimedOut { stdout, stderr },
        Some(ProcessOutcomeTag::IdleTimedOut) => ProcessOutcome::IdleTimedOut { stdout, stderr },
        Some(ProcessOutcomeTag::OutputLimitExceeded) => {
            ProcessOutcome::OutputLimitExceeded { stdout, stderr }
        }
        Some(ProcessOutcomeTag::Failure) => return Err(io::Error::other("process wait failed")),
        None => ProcessOutcome::Exited {
            code,
            stdout,
            stderr,
        },
    })
}

enum ProcessOutcomeTag {
    Cancelled,
    Paused,
    TimedOut,
    IdleTimedOut,
    OutputLimitExceeded,
    Failure,
}

enum MonitoredOutputStatus {
    ExceedsLimit,
    WithinLimit { staged_bytes: u64 },
}

fn record_activity(started_at: Instant, last_activity_ns: &AtomicU64) {
    let now_ns = started_at.elapsed().as_nanos().min(u64::MAX as u128) as u64;
    last_activity_ns.store(now_ns, Ordering::Release);
}

fn output_exceeds_limit(output: &MonitoredOutput) -> MonitoredOutputStatus {
    use std::os::windows::fs::MetadataExt;
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;

    let mut aggregate_bytes = 0u64;
    let mut staged_bytes = 0u64;
    let mut pending_dirs = vec![(output.directory.clone(), true)];

    while let Some((dir, is_root)) = pending_dirs.pop() {
        let Ok(dir_meta) = std::fs::symlink_metadata(&dir) else {
            return MonitoredOutputStatus::ExceedsLimit;
        };
        if !dir_meta.is_dir() || dir_meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return MonitoredOutputStatus::ExceedsLimit;
        }
        let Ok(entries) = std::fs::read_dir(&dir) else {
            return MonitoredOutputStatus::ExceedsLimit;
        };
        for entry in entries {
            let Ok(entry) = entry else {
                return MonitoredOutputStatus::ExceedsLimit;
            };
            let entry_path = entry.path();
            let Ok(metadata) = std::fs::symlink_metadata(&entry_path) else {
                return MonitoredOutputStatus::ExceedsLimit;
            };
            if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
                return MonitoredOutputStatus::ExceedsLimit;
            }
            let matches_prefix = is_root
                && entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(&output.filename_prefix);
            if metadata.is_dir() {
                if matches_prefix {
                    return MonitoredOutputStatus::ExceedsLimit;
                }
                pending_dirs.push((entry_path, false));
                continue;
            }
            if !metadata.is_file() {
                return MonitoredOutputStatus::ExceedsLimit;
            }
            let len = metadata.len();
            aggregate_bytes = aggregate_bytes.saturating_add(len);
            if aggregate_bytes > output.maximum_bytes {
                return MonitoredOutputStatus::ExceedsLimit;
            }
            if matches_prefix {
                staged_bytes = staged_bytes.saturating_add(len);
            }
        }
    }
    MonitoredOutputStatus::WithinLimit { staged_bytes }
}

fn terminate_and_reap(
    job: &super::windows_job::KillOnCloseJob,
    process: &std::os::windows::io::OwnedHandle,
) -> io::Result<()> {
    job.terminate_and_wait(EXIT_TERMINATED, JOB_TERMINATION_TIMEOUT)?;
    // SAFETY: process is a valid owned handle. The Job Object remains alive
    // until after root reaping; kill-on-close remains the fallback cleanup.
    let mut wait = unsafe { WaitForSingleObject(process.as_raw_handle() as HANDLE, 30_000) };
    if wait != WAIT_OBJECT_0 {
        // SAFETY: force root termination after job termination was requested.
        unsafe {
            TerminateProcess(process.as_raw_handle() as HANDLE, EXIT_TERMINATED);
        }
        // SAFETY: process handle remains valid.
        wait = unsafe { WaitForSingleObject(process.as_raw_handle() as HANDLE, 30_000) };
    }
    if wait != WAIT_OBJECT_0 {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "could not reap process during supervisor setup failure",
        ));
    }
    Ok(())
}

fn drain<R: Read + Send + 'static>(
    mut reader: R,
    limit: usize,
    stream: ProcessStream,
    handler: Option<OutputHandler>,
    activity: Option<(Instant, Arc<AtomicU64>)>,
) -> io::Result<thread::JoinHandle<io::Result<CapturedOutput>>> {
    thread::Builder::new()
        .name("fluxnet-process-output".into())
        .spawn(move || {
            let mut bytes = Vec::with_capacity(limit.min(64 * 1024));
            let mut truncated = false;
            let mut chunk = [0u8; 8192];
            loop {
                let n = reader.read(&mut chunk)?;
                if n == 0 {
                    break;
                }
                if let Some((started_at, ref last_activity_ns)) = activity {
                    if stream == ProcessStream::Stdout
                        || chunk[..n]
                            .windows(b"FLUXNET_PROGRESS\t".len())
                            .any(|w| w == b"FLUXNET_PROGRESS\t")
                    {
                        record_activity(started_at, last_activity_ns);
                    }
                }
                if let Some(handler) = &handler {
                    handler(stream, &chunk[..n]);
                }
                let retain = limit.saturating_sub(bytes.len()).min(n);
                bytes.extend_from_slice(&chunk[..retain]);
                truncated |= retain < n;
            }
            Ok(CapturedOutput { bytes, truncated })
        })
}
