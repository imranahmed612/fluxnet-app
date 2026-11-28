//! Supervised LGPL FFmpeg / ffprobe adapter skeleton (`MediaProcessor` trait,
//! SHA-256 executable verification, local-only safe argv rules, and `ffprobe`
//! inspection). Full split-stream merge execution is deferred to Part 6.

use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    time::Duration,
};

use serde::Deserialize;

use crate::download_manager::DownloadTaskControl;

#[allow(dead_code)]
pub(crate) const VERIFIED_FFMPEG_VERSION: &str = "n9.0.2-982-g836df4e858-20261127";

#[cfg(windows)]
pub(crate) const VERIFIED_FFMPEG_SIZE_BYTES: u64 = 134_501_376;

#[cfg(windows)]
pub(crate) const VERIFIED_FFMPEG_SHA256: [u8; 32] = [
    0x11, 0x31, 0x00, 0xd7, 0x55, 0xa0, 0xaf, 0xdb, 0xa7, 0x3a, 0x45, 0x72, 0x20, 0x47, 0xb1, 0xaf,
    0xbd, 0xf8, 0x50, 0x0e, 0xa6, 0x4f, 0xb5, 0x56, 0xf6, 0xe9, 0xad, 0x3a, 0xd5, 0xb1, 0x54, 0xf5,
];

#[cfg(windows)]
pub(crate) const VERIFIED_FFPROBE_SIZE_BYTES: u64 = 134_300_160;

#[cfg(windows)]
pub(crate) const VERIFIED_FFPROBE_SHA256: [u8; 32] = [
    0xd8, 0xe5, 0x9e, 0x55, 0x12, 0x94, 0x60, 0xf7, 0x7a, 0x1d, 0xc9, 0xc4, 0x0b, 0x5b, 0xa9, 0x08,
    0x46, 0x55, 0x8f, 0xb3, 0x5d, 0xab, 0xe2, 0x1b, 0x96, 0xa2, 0xc1, 0xf0, 0xfe, 0x78, 0xc3, 0x60,
];

#[cfg(windows)]
const VERIFIED_FFMPEG_UNAVAILABLE_MSG: &str = "The verified FFmpeg media processor is not available.";

#[cfg(windows)]
const VERIFIED_FFPROBE_UNAVAILABLE_MSG: &str =
    "The verified ffprobe media inspector is not available.";

#[allow(dead_code)]
pub(crate) const FFMPEG_WALL_TIMEOUT: Duration = Duration::from_secs(15 * 60);
pub(crate) const FFPROBE_WALL_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const FFMPEG_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
pub(crate) const FFMPEG_MAX_THREADS: &str = "4";
#[allow(dead_code)]
pub(crate) const MAX_FFMPEG_OUTPUT_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_FFPROBE_STDOUT_BYTES: usize = 512 * 1024;
const MAX_ENCODERS_STDOUT_BYTES: usize = 512 * 1024;
const MAX_STDERR_BYTES: usize = 16 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum OutputContainer {
    Mp4,
    Webm,
}

impl OutputContainer {
    pub(crate) fn extension(self) -> &'static str {
        match self {
            Self::Mp4 => "mp4",
            Self::Webm => "webm",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct StagedInput {
    pub primary_path: PathBuf,
    pub secondary_path: Option<PathBuf>,
    pub working_directory: PathBuf,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum MediaOperation {
    RemuxCopy { container: OutputContainer },
    MuxSplitCopy { container: OutputContainer },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MediaProbeSummary {
    pub format_name: String,
    pub duration_seconds: Option<f64>,
    pub size_bytes: u64,
    pub stream_count: usize,
    pub video_stream_count: usize,
    pub audio_stream_count: usize,
    pub has_video: bool,
    pub has_audio: bool,
    pub video_codec: Option<String>,
    pub audio_codec: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
#[allow(dead_code)]
pub(crate) struct StagedOutput {
    pub path: PathBuf,
    pub container: OutputContainer,
    pub bytes: u64,
    pub probe: MediaProbeSummary,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EncoderInventory {
    pub total_encoders: usize,
    pub has_libmp3lame: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum EngineError {
    ToolUnavailable,
    InvalidInput,
    InvalidOperation,
    MergeFailed,
    RemuxFailed,
    ValidationFailed,
    TimedOut,
    IdleTimedOut,
    Cancelled,
    OutputTooLarge,
    TemporaryCleanupFailed,
    UnsupportedOperation,
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ToolUnavailable => "The verified FFmpeg media processor is not available.",
            Self::InvalidInput => "The local media input file is invalid or inaccessible.",
            Self::InvalidOperation => "The requested local media operation is invalid.",
            Self::MergeFailed => "Audio and video streams could not be merged.",
            Self::RemuxFailed => "The local media container could not be remuxed.",
            Self::ValidationFailed => "The processed media output failed structural validation.",
            Self::TimedOut => "Media processing timed out.",
            Self::IdleTimedOut => "Media processing stalled without progress and timed out.",
            Self::Cancelled => "Media processing was cancelled.",
            Self::OutputTooLarge => "The processed media exceeded the 1 GiB size limit.",
            Self::TemporaryCleanupFailed => {
                "Media processing could not safely clean up its temporary workspace."
            }
            Self::UnsupportedOperation => {
                "Split-stream merge execution is not enabled in this skeleton yet."
            }
        })
    }
}

impl std::error::Error for EngineError {}

/// Supervised local media-processing adapter contract from the architecture audit.
#[allow(dead_code)]
pub(crate) trait MediaProcessor {
    fn process(
        &self,
        input: &StagedInput,
        operation: &MediaOperation,
        control: &DownloadTaskControl,
    ) -> Result<StagedOutput, EngineError>;

    fn merge(
        &self,
        input: &StagedInput,
        container: OutputContainer,
        control: &DownloadTaskControl,
    ) -> Result<StagedOutput, EngineError>;
}

#[allow(dead_code)]
pub(crate) struct FfmpegAdapter {
    pub(crate) ffmpeg_executable: PathBuf,
    pub(crate) ffprobe_executable: PathBuf,
}

#[cfg(windows)]
impl MediaProcessor for FfmpegAdapter {
    fn process(
        &self,
        input: &StagedInput,
        operation: &MediaOperation,
        control: &DownloadTaskControl,
    ) -> Result<StagedOutput, EngineError> {
        match operation {
            MediaOperation::MuxSplitCopy { container } => self.merge(input, *container, control),
            MediaOperation::RemuxCopy { .. } => {
                if control.is_cancelled() {
                    return Err(EngineError::Cancelled);
                }
                let _verified_ffmpeg = verify_ffmpeg_executable(&self.ffmpeg_executable)
                    .map_err(|_| EngineError::ToolUnavailable)?;
                let _verified_ffprobe = verify_ffprobe_executable(&self.ffprobe_executable)
                    .map_err(|_| EngineError::ToolUnavailable)?;
                let _ = validate_local_media_input_path(&input.primary_path)?;
                Err(EngineError::UnsupportedOperation)
            }
        }
    }

    fn merge(
        &self,
        input: &StagedInput,
        container: OutputContainer,
        control: &DownloadTaskControl,
    ) -> Result<StagedOutput, EngineError> {
        use crate::process::{
            MonitoredOutput, ProcessOutcome, ProcessSpec, run_supervised_with_output,
        };

        if control.is_cancelled() {
            return Err(EngineError::Cancelled);
        }
        let verified_ffmpeg = verify_ffmpeg_executable(&self.ffmpeg_executable)
            .map_err(|_| EngineError::ToolUnavailable)?;
        let verified_ffprobe = verify_ffprobe_executable(&self.ffprobe_executable)
            .map_err(|_| EngineError::ToolUnavailable)?;

        let primary = validate_local_media_input_path(&input.primary_path)?;
        let secondary_raw = input
            .secondary_path
            .as_deref()
            .ok_or(EngineError::InvalidOperation)?;
        let secondary = validate_local_media_input_path(secondary_raw)?;

        let stage = ScopedProcessDir::create(&input.working_directory)
            .map_err(|_| EngineError::TemporaryCleanupFailed)?;
        let runtime = match ScopedProcessDir::create(stage.path()) {
            Ok(dir) => dir,
            Err(_) => {
                let _ = stage.cleanup();
                return Err(EngineError::TemporaryCleanupFailed);
            }
        };

        let stage_dir_clean = strip_verbatim_drive_prefix(stage.path());
        let staged_out =
            stage_dir_clean.join(format!("fluxnet-ffmpeg-out.{}", container.extension()));
        let args = match build_safe_ffmpeg_copy_args(
            &primary,
            Some(&secondary),
            MediaOperation::MuxSplitCopy { container },
            &staged_out,
        ) {
            Ok(args) => args,
            Err(err) => {
                let _ = runtime.cleanup();
                let _ = stage.cleanup();
                return Err(err);
            }
        };

        let spec = ProcessSpec {
            executable: verified_ffmpeg.clone(),
            args,
            working_directory: stage_dir_clean.clone(),
            environment: minimal_tool_environment(runtime.path()),
            timeout: FFMPEG_WALL_TIMEOUT,
            idle_timeout: Some(FFMPEG_IDLE_TIMEOUT),
            stdout_limit: 64 * 1024,
            stderr_limit: 64 * 1024,
        };
        let monitored = MonitoredOutput {
            directory: stage_dir_clean,
            filename_prefix: "fluxnet-ffmpeg-out.".into(),
            maximum_bytes: MAX_FFMPEG_OUTPUT_BYTES,
        };

        let outcome = run_supervised_with_output(
            &spec,
            control,
            std::sync::Arc::new(|_, _| {}),
            Some(monitored),
        );
        let runtime_cleanup = runtime.cleanup();
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(_) => {
                let _ = stage.cleanup();
                return Err(EngineError::TemporaryCleanupFailed);
            }
        };
        if runtime_cleanup.is_err() {
            let _ = stage.cleanup();
            return Err(EngineError::TemporaryCleanupFailed);
        }

        match outcome {
            ProcessOutcome::Exited { code: 0, .. } => {}
            ProcessOutcome::Exited { .. } => {
                let _ = stage.cleanup();
                return Err(EngineError::MergeFailed);
            }
            ProcessOutcome::Cancelled { .. } | ProcessOutcome::Paused { .. } => {
                let _ = stage.cleanup();
                return Err(EngineError::Cancelled);
            }
            ProcessOutcome::TimedOut { .. } => {
                let _ = stage.cleanup();
                return Err(EngineError::TimedOut);
            }
            ProcessOutcome::IdleTimedOut { .. } => {
                let _ = stage.cleanup();
                return Err(EngineError::IdleTimedOut);
            }
            ProcessOutcome::OutputLimitExceeded { .. } => {
                let _ = stage.cleanup();
                return Err(EngineError::OutputTooLarge);
            }
        }

        let validated_out = match validate_local_media_input_path(&staged_out) {
            Ok(path) => path,
            Err(_) => {
                let _ = stage.cleanup();
                return Err(EngineError::ValidationFailed);
            }
        };
        let bytes = match std::fs::metadata(&validated_out) {
            Ok(meta) if meta.len() > 0 && meta.len() <= MAX_FFMPEG_OUTPUT_BYTES => meta.len(),
            Ok(_) => {
                let _ = stage.cleanup();
                return Err(EngineError::OutputTooLarge);
            }
            Err(_) => {
                let _ = stage.cleanup();
                return Err(EngineError::ValidationFailed);
            }
        };

        // 1. Structural probe with ffprobe: must contain exactly 1 video + 1 audio stream and duration > 0.
        let probe = match probe_local_media(
            &verified_ffprobe,
            &validated_out,
            &input.working_directory,
            control,
        ) {
            Ok(summary)
                if summary.video_stream_count == 1
                    && summary.audio_stream_count == 1
                    && summary.duration_seconds.is_some_and(|d| d.is_finite() && d > 0.0) =>
            {
                summary
            }
            Ok(_) => {
                let _ = stage.cleanup();
                return Err(EngineError::ValidationFailed);
            }
            Err(err) => {
                let _ = stage.cleanup();
                return Err(err);
            }
        };

        // 2. Full decode verification: ffmpeg -v error -i <output> -f null -
        if let Err(err) = validate_full_decode(
            &verified_ffmpeg,
            &validated_out,
            &input.working_directory,
            control,
        ) {
            let _ = stage.cleanup();
            return Err(err);
        }

        // Move validated output out of stage into working_directory so stage can be cleaned up cleanly.
        let work_dir_clean = strip_verbatim_drive_prefix(
            &std::fs::canonicalize(&input.working_directory)
                .map_err(|_| EngineError::TemporaryCleanupFailed)?,
        );
        let ready_path = reserve_merged_output_path(&work_dir_clean, container.extension())
            .ok_or(EngineError::TemporaryCleanupFailed)?;
        if std::fs::rename(&validated_out, &ready_path).is_err() {
            let _ = std::fs::remove_file(&ready_path);
            let _ = stage.cleanup();
            return Err(EngineError::TemporaryCleanupFailed);
        }
        if stage.cleanup().is_err() {
            let _ = std::fs::remove_file(&ready_path);
            return Err(EngineError::TemporaryCleanupFailed);
        }

        Ok(StagedOutput {
            path: ready_path,
            container,
            bytes,
            probe,
        })
    }
}

#[cfg(windows)]
fn reserve_merged_output_path(dir: &Path, extension: &str) -> Option<PathBuf> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    for _ in 0..32 {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let candidate = dir.join(format!(
            ".fluxnet-merged-{}-{id}.{extension}",
            std::process::id()
        ));
        if !candidate.exists() {
            return Some(candidate);
        }
    }
    None
}

/// Runs a full local decode check (`ffmpeg -v error -i <output> -f null -`) inside
/// a Job Object with `-protocol_whitelist file` and rejects non-zero exit or stderr errors.
#[cfg(windows)]
pub(crate) fn validate_full_decode(
    ffmpeg_candidate: &Path,
    input_path: &Path,
    base_directory: &Path,
    control: &DownloadTaskControl,
) -> Result<(), EngineError> {
    use crate::process::{ProcessOutcome, ProcessSpec, run_supervised};

    if control.is_cancelled() {
        return Err(EngineError::Cancelled);
    }
    let verified_ffmpeg =
        verify_ffmpeg_executable(ffmpeg_candidate).map_err(|_| EngineError::ToolUnavailable)?;
    let validated_input = validate_local_media_input_path(input_path)?;
    let runtime =
        ScopedProcessDir::create(base_directory).map_err(|_| EngineError::TemporaryCleanupFailed)?;
    let working_dir = strip_verbatim_drive_prefix(runtime.path());

    let spec = ProcessSpec {
        executable: verified_ffmpeg,
        args: vec![
            OsString::from("-nostdin"),
            OsString::from("-hide_banner"),
            OsString::from("-v"),
            OsString::from("error"),
            OsString::from("-threads"),
            OsString::from(FFMPEG_MAX_THREADS),
            OsString::from("-protocol_whitelist"),
            OsString::from("file"),
            OsString::from("-i"),
            validated_input.into_os_string(),
            OsString::from("-f"),
            OsString::from("null"),
            OsString::from("-"),
        ],
        working_directory: working_dir,
        environment: minimal_tool_environment(runtime.path()),
        timeout: FFMPEG_WALL_TIMEOUT,
        idle_timeout: Some(FFMPEG_IDLE_TIMEOUT),
        stdout_limit: 64 * 1024,
        stderr_limit: 64 * 1024,
    };

    let outcome = run_supervised(&spec, control).map_err(|_| EngineError::TemporaryCleanupFailed)?;
    runtime
        .cleanup()
        .map_err(|_| EngineError::TemporaryCleanupFailed)?;

    match outcome {
        ProcessOutcome::Exited {
            code: 0, stderr, ..
        } => {
            if !stderr.bytes.is_empty() {
                return Err(EngineError::ValidationFailed);
            }
            Ok(())
        }
        ProcessOutcome::Exited { .. } => Err(EngineError::ValidationFailed),
        ProcessOutcome::Cancelled { .. } | ProcessOutcome::Paused { .. } => {
            Err(EngineError::Cancelled)
        }
        ProcessOutcome::TimedOut { .. } => Err(EngineError::TimedOut),
        ProcessOutcome::IdleTimedOut { .. } => Err(EngineError::IdleTimedOut),
        ProcessOutcome::OutputLimitExceeded { .. } => Err(EngineError::OutputTooLarge),
    }
}

/// Builds a strict, shell-free argv list for local stream-copy processing:
/// - No network protocols (`-protocol_whitelist file`)
/// - CPU thread cap (`-threads 4`)
/// - Explicit `-i` local inputs only
/// - Explicit `-map` stream selection
/// - `-c copy` stream copy by default (no transcoding or user-controlled args)
pub(crate) fn build_safe_ffmpeg_copy_args(
    primary_input: &Path,
    secondary_input: Option<&Path>,
    operation: MediaOperation,
    output_path: &Path,
) -> Result<Vec<OsString>, EngineError> {
    let primary_str = validate_non_url_local_arg(primary_input)?;
    let output_str = validate_non_url_output_arg(output_path)?;

    let mut args = vec![
        OsString::from("-nostdin"),
        OsString::from("-hide_banner"),
        OsString::from("-loglevel"),
        OsString::from("error"),
        OsString::from("-y"),
        OsString::from("-threads"),
        OsString::from(FFMPEG_MAX_THREADS),
        OsString::from("-protocol_whitelist"),
        OsString::from("file"),
        OsString::from("-i"),
        primary_str,
    ];

    let container = match operation {
        MediaOperation::RemuxCopy { container } => {
            if secondary_input.is_some() {
                return Err(EngineError::InvalidOperation);
            }
            args.extend([
                OsString::from("-map"),
                OsString::from("0:v:0"),
                OsString::from("-map"),
                OsString::from("0:a:0?"),
            ]);
            container
        }
        MediaOperation::MuxSplitCopy { container } => {
            let secondary = secondary_input.ok_or(EngineError::InvalidOperation)?;
            let secondary_str = validate_non_url_local_arg(secondary)?;
            args.extend([
                OsString::from("-i"),
                secondary_str,
                OsString::from("-map"),
                OsString::from("0:v:0"),
                OsString::from("-map"),
                OsString::from("1:a:0"),
            ]);
            container
        }
    };

    args.extend([
        OsString::from("-c"),
        OsString::from("copy"),
        OsString::from("-sn"),
        OsString::from("-dn"),
    ]);

    if container == OutputContainer::Mp4 {
        args.extend([OsString::from("-movflags"), OsString::from("+faststart")]);
    }

    args.extend([
        OsString::from("-f"),
        OsString::from(match container {
            OutputContainer::Mp4 => "mp4",
            OutputContainer::Webm => "webm",
        }),
        output_str,
    ]);

    Ok(args)
}

fn validate_non_url_local_arg(path: &Path) -> Result<OsString, EngineError> {
    if !path.is_absolute() {
        return Err(EngineError::InvalidInput);
    }
    let text = path.to_str().ok_or(EngineError::InvalidInput)?;
    if text.is_empty() || text.starts_with('-') || looks_like_protocol_url(text) {
        return Err(EngineError::InvalidInput);
    }
    Ok(path.as_os_str().to_owned())
}

fn validate_non_url_output_arg(path: &Path) -> Result<OsString, EngineError> {
    if !path.is_absolute() {
        return Err(EngineError::InvalidOperation);
    }
    let text = path.to_str().ok_or(EngineError::InvalidOperation)?;
    if text.is_empty() || text.starts_with('-') || looks_like_protocol_url(text) {
        return Err(EngineError::InvalidOperation);
    }
    Ok(path.as_os_str().to_owned())
}

fn looks_like_protocol_url(text: &str) -> bool {
    let lower = text.trim_start().to_ascii_lowercase();
    if lower.contains("://") {
        return true;
    }
    for prefix in [
        "http:", "https:", "tcp:", "tls:", "udp:", "rtp:", "rtsp:", "rtmp:", "ftp:", "sftp:",
        "file:", "concat:", "subfile:", "pipe:", "data:", "crypto:", "async:", "cache:",
    ] {
        if lower.starts_with(prefix) {
            return true;
        }
    }
    false
}

#[cfg(windows)]
pub(crate) fn validate_local_media_input_path(candidate: &Path) -> Result<PathBuf, EngineError> {
    use std::os::windows::fs::MetadataExt;
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;

    let raw_text = candidate.to_str().ok_or(EngineError::InvalidInput)?;
    if raw_text.is_empty() || raw_text.starts_with('-') || looks_like_protocol_url(raw_text) {
        return Err(EngineError::InvalidInput);
    }
    let initial_meta =
        std::fs::symlink_metadata(candidate).map_err(|_| EngineError::InvalidInput)?;
    if !initial_meta.is_file()
        || initial_meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
        || initial_meta.len() == 0
    {
        return Err(EngineError::InvalidInput);
    }
    let canonical = std::fs::canonicalize(candidate).map_err(|_| EngineError::InvalidInput)?;
    let normalized = strip_verbatim_drive_prefix(&canonical);
    if !normalized.is_absolute() {
        return Err(EngineError::InvalidInput);
    }
    let canonical_meta =
        std::fs::symlink_metadata(&normalized).map_err(|_| EngineError::InvalidInput)?;
    if !canonical_meta.is_file()
        || canonical_meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
        || canonical_meta.len() == 0
    {
        return Err(EngineError::InvalidInput);
    }
    let _ = validate_non_url_local_arg(&normalized)?;
    Ok(normalized)
}

#[cfg(windows)]
fn strip_verbatim_drive_prefix(path: &Path) -> PathBuf {
    if let Some(text) = path.to_str() {
        if let Some(rest) = text.strip_prefix(r"\\?\") {
            let bytes = rest.as_bytes();
            if bytes.len() >= 3
                && bytes[0].is_ascii_alphabetic()
                && bytes[1] == b':'
                && (bytes[2] == b'\\' || bytes[2] == b'/')
            {
                let stripped = PathBuf::from(rest);
                if stripped.is_absolute() {
                    return stripped;
                }
            }
        }
    }
    path.to_path_buf()
}

#[cfg(windows)]
pub(crate) fn verify_ffmpeg_executable(candidate: &Path) -> Result<PathBuf, String> {
    verify_pinned_tool_executable(
        candidate,
        VERIFIED_FFMPEG_SIZE_BYTES,
        &VERIFIED_FFMPEG_SHA256,
        VERIFIED_FFMPEG_UNAVAILABLE_MSG,
    )
}

#[cfg(windows)]
pub(crate) fn verify_ffprobe_executable(candidate: &Path) -> Result<PathBuf, String> {
    verify_pinned_tool_executable(
        candidate,
        VERIFIED_FFPROBE_SIZE_BYTES,
        &VERIFIED_FFPROBE_SHA256,
        VERIFIED_FFPROBE_UNAVAILABLE_MSG,
    )
}

#[cfg(windows)]
fn verify_pinned_tool_executable(
    candidate: &Path,
    expected_size: u64,
    expected_sha256: &[u8; 32],
    unavailable_msg: &'static str,
) -> Result<PathBuf, String> {
    use std::os::windows::fs::MetadataExt;
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;

    let initial_meta =
        std::fs::symlink_metadata(candidate).map_err(|_| unavailable_msg.to_owned())?;
    if !initial_meta.is_file()
        || initial_meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
        || initial_meta.len() != expected_size
    {
        return Err(unavailable_msg.into());
    }
    let canonical = std::fs::canonicalize(candidate).map_err(|_| unavailable_msg.to_owned())?;
    let normalized = strip_verbatim_drive_prefix(&canonical);
    if !normalized.is_absolute() {
        return Err(unavailable_msg.into());
    }
    let canonical_meta =
        std::fs::symlink_metadata(&normalized).map_err(|_| unavailable_msg.to_owned())?;
    if !canonical_meta.is_file()
        || canonical_meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
        || canonical_meta.len() != expected_size
    {
        return Err(unavailable_msg.into());
    }
    let digest = sha256_file(&normalized, expected_size).map_err(|_| unavailable_msg.to_owned())?;
    if &digest != expected_sha256 {
        return Err(unavailable_msg.into());
    }
    Ok(normalized)
}

#[cfg(windows)]
fn collect_trusted_release_tool_candidates(
    app: Option<&tauri::AppHandle>,
    binary_name: &str,
) -> Vec<PathBuf> {
    use tauri::Manager;

    let mut candidates = Vec::new();
    if let Some(app) = app {
        if let Ok(resource_dir) = app.path().resource_dir() {
            candidates.push(resource_dir.join(binary_name));
            candidates.push(resource_dir.join("binaries").join(binary_name));
            candidates.push(resource_dir.join("tools").join(binary_name));
        }
        if let Ok(app_data_dir) = app.path().app_data_dir() {
            candidates.push(app_data_dir.join("tools").join(binary_name));
            candidates.push(app_data_dir.join(binary_name));
        }
    }
    if let Ok(current_exe) = std::env::current_exe() {
        if let Some(exe_dir) = current_exe.parent() {
            candidates.push(exe_dir.join(binary_name));
            candidates.push(exe_dir.join("tools").join(binary_name));
        }
    }
    candidates
}

#[cfg(windows)]
fn collect_tool_candidates_for_profile(
    app: Option<&tauri::AppHandle>,
    binary_name: &str,
    dev_env_vars: &[&str],
    allow_dev_candidates: bool,
) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    #[cfg(any(test, debug_assertions))]
    if allow_dev_candidates {
        for env_name in dev_env_vars {
            if let Some(value) = std::env::var_os(env_name).filter(|v| !v.is_empty()) {
                candidates.push(PathBuf::from(value));
            }
        }
    }
    #[cfg(not(any(test, debug_assertions)))]
    let _ = dev_env_vars;

    candidates.extend(collect_trusted_release_tool_candidates(app, binary_name));

    #[cfg(any(test, debug_assertions))]
    if allow_dev_candidates {
        if let Some(repo_root) = Path::new(env!("CARGO_MANIFEST_DIR")).parent() {
            candidates.push(
                repo_root
                    .join("experiments")
                    .join("media-engine-poc")
                    .join("test-tools")
                    .join("ffmpeg-n9.0.2-win64-lgpl")
                    .join(binary_name),
            );
        }
    }
    #[cfg(not(any(test, debug_assertions)))]
    let _ = allow_dev_candidates;

    candidates
}

#[cfg(windows)]
#[allow(dead_code)]
pub(crate) fn resolve_verified_ffmpeg_executable(
    app: Option<&tauri::AppHandle>,
) -> Result<PathBuf, String> {
    let candidates = collect_tool_candidates_for_profile(
        app,
        "ffmpeg.exe",
        &["FLUXNET_FFMPEG_REAL_EXE", "FLUXNET_FFMPEG_EXE"],
        cfg!(any(test, debug_assertions)),
    );
    for candidate in &candidates {
        if let Ok(verified) = verify_ffmpeg_executable(candidate) {
            return Ok(verified);
        }
    }
    Err(VERIFIED_FFMPEG_UNAVAILABLE_MSG.into())
}

#[cfg(windows)]
#[allow(dead_code)]
pub(crate) fn resolve_verified_ffprobe_executable(
    app: Option<&tauri::AppHandle>,
) -> Result<PathBuf, String> {
    let candidates = collect_tool_candidates_for_profile(
        app,
        "ffprobe.exe",
        &["FLUXNET_FFPROBE_REAL_EXE", "FLUXNET_FFPROBE_EXE"],
        cfg!(any(test, debug_assertions)),
    );
    for candidate in &candidates {
        if let Ok(verified) = verify_ffprobe_executable(candidate) {
            return Ok(verified);
        }
    }
    Err(VERIFIED_FFPROBE_UNAVAILABLE_MSG.into())
}

#[cfg(windows)]
struct ScopedProcessDir {
    parent_root: PathBuf,
    path: PathBuf,
    cleaned: bool,
}

#[cfg(windows)]
impl ScopedProcessDir {
    fn create(base_directory: &Path) -> std::io::Result<Self> {
        use std::os::windows::fs::MetadataExt;
        use std::sync::atomic::{AtomicU64, Ordering};

        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;

        let base_meta = std::fs::symlink_metadata(base_directory)?;
        if !base_meta.is_dir() || base_meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "base workspace is not a regular directory",
            ));
        }
        let parent_root = std::fs::canonicalize(base_directory)?;
        let ticks = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();

        for _ in 0..32 {
            let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            let dir_name = format!(
                ".fluxnet-ffmpeg-runtime-{}-{ticks}-{id}",
                std::process::id()
            );
            let candidate = parent_root.join(dir_name);
            match std::fs::create_dir(&candidate) {
                Ok(()) => {
                    let meta = std::fs::symlink_metadata(&candidate)?;
                    if !meta.is_dir() || meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
                    {
                        let _ = std::fs::remove_dir(&candidate);
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "created runtime directory failed validation",
                        ));
                    }
                    let canonical = std::fs::canonicalize(&candidate)?;
                    if canonical.parent() != Some(parent_root.as_path()) {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "runtime directory escaped parent workspace",
                        ));
                    }
                    return Ok(Self {
                        parent_root,
                        path: canonical,
                        cleaned: false,
                    });
                }
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(err) => return Err(err),
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "could not reserve unique ffmpeg runtime directory",
        ))
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn cleanup(mut self) -> std::io::Result<()> {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;

        let meta = std::fs::symlink_metadata(&self.path)?;
        if !meta.is_dir()
            || meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
            || self.path.parent() != Some(self.parent_root.as_path())
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "ffmpeg runtime directory failed ownership validation",
            ));
        }
        let res = std::fs::remove_dir_all(&self.path);
        self.cleaned = true;
        res
    }
}

#[cfg(windows)]
fn minimal_tool_environment(temp_dir: &Path) -> Vec<(OsString, OsString)> {
    let mut env = Vec::with_capacity(4);
    for key in ["SystemRoot", "WINDIR"] {
        if let Some(val) = std::env::var_os(key) {
            env.push((OsString::from(key), val));
        }
    }
    let temp = strip_verbatim_drive_prefix(temp_dir);
    env.push((OsString::from("TEMP"), temp.as_os_str().to_owned()));
    env.push((OsString::from("TMP"), temp.as_os_str().to_owned()));
    env
}

/// Verifies `ffmpeg.exe` by SHA-256 and queries `-encoders` inside a Job Object
/// to confirm encoder inventory and `libmp3lame` availability without encoding.
#[cfg(windows)]
pub(crate) fn inspect_available_encoders(
    ffmpeg_candidate: &Path,
    base_directory: &Path,
    control: &DownloadTaskControl,
) -> Result<EncoderInventory, EngineError> {
    use crate::process::{ProcessOutcome, ProcessSpec, run_supervised};

    let verified_ffmpeg =
        verify_ffmpeg_executable(ffmpeg_candidate).map_err(|_| EngineError::ToolUnavailable)?;
    let runtime =
        ScopedProcessDir::create(base_directory).map_err(|_| EngineError::TemporaryCleanupFailed)?;
    let working_dir = strip_verbatim_drive_prefix(runtime.path());

    let spec = ProcessSpec {
        executable: verified_ffmpeg,
        args: vec![
            OsString::from("-nostdin"),
            OsString::from("-hide_banner"),
            OsString::from("-encoders"),
        ],
        working_directory: working_dir,
        environment: minimal_tool_environment(runtime.path()),
        timeout: FFPROBE_WALL_TIMEOUT,
        idle_timeout: Some(FFMPEG_IDLE_TIMEOUT),
        stdout_limit: MAX_ENCODERS_STDOUT_BYTES,
        stderr_limit: MAX_STDERR_BYTES,
    };

    let outcome = run_supervised(&spec, control).map_err(|_| EngineError::TemporaryCleanupFailed)?;
    runtime
        .cleanup()
        .map_err(|_| EngineError::TemporaryCleanupFailed)?;

    let stdout = match outcome {
        ProcessOutcome::Exited {
            code: 0, stdout, ..
        } => stdout,
        ProcessOutcome::Exited { .. } => return Err(EngineError::ValidationFailed),
        ProcessOutcome::Cancelled { .. } | ProcessOutcome::Paused { .. } => {
            return Err(EngineError::Cancelled);
        }
        ProcessOutcome::TimedOut { .. } => return Err(EngineError::TimedOut),
        ProcessOutcome::IdleTimedOut { .. } => return Err(EngineError::IdleTimedOut),
        ProcessOutcome::OutputLimitExceeded { .. } => return Err(EngineError::OutputTooLarge),
    };
    if stdout.truncated {
        return Err(EngineError::OutputTooLarge);
    }

    let text = String::from_utf8_lossy(&stdout.bytes);
    let mut total_encoders = 0usize;
    let mut has_libmp3lame = false;
    let mut in_table = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("------") {
            in_table = true;
            continue;
        }
        if !in_table || trimmed.is_empty() {
            continue;
        }
        let mut parts = trimmed.split_whitespace();
        let Some(flags) = parts.next() else {
            continue;
        };
        let Some(name) = parts.next() else {
            continue;
        };
        if flags.len() == 6 {
            total_encoders += 1;
            if name == "libmp3lame" {
                has_libmp3lame = true;
            }
        }
    }
    if total_encoders == 0 {
        return Err(EngineError::ValidationFailed);
    }
    Ok(EncoderInventory {
        total_encoders,
        has_libmp3lame,
    })
}

/// Verifies `ffprobe.exe` by SHA-256 and probes a local non-reparse media file
/// under Job Object containment with `-protocol_whitelist file`.
#[cfg(windows)]
pub(crate) fn probe_local_media(
    ffprobe_candidate: &Path,
    input_path: &Path,
    base_directory: &Path,
    control: &DownloadTaskControl,
) -> Result<MediaProbeSummary, EngineError> {
    use crate::process::{ProcessOutcome, ProcessSpec, run_supervised};

    let verified_ffprobe =
        verify_ffprobe_executable(ffprobe_candidate).map_err(|_| EngineError::ToolUnavailable)?;
    let validated_input = validate_local_media_input_path(input_path)?;
    let input_size = std::fs::metadata(&validated_input)
        .map_err(|_| EngineError::InvalidInput)?
        .len();

    let runtime =
        ScopedProcessDir::create(base_directory).map_err(|_| EngineError::TemporaryCleanupFailed)?;
    let working_dir = strip_verbatim_drive_prefix(runtime.path());

    let spec = ProcessSpec {
        executable: verified_ffprobe,
        args: vec![
            OsString::from("-v"),
            OsString::from("error"),
            OsString::from("-protocol_whitelist"),
            OsString::from("file"),
            OsString::from("-show_format"),
            OsString::from("-show_streams"),
            OsString::from("-of"),
            OsString::from("json"),
            validated_input.into_os_string(),
        ],
        working_directory: working_dir,
        environment: minimal_tool_environment(runtime.path()),
        timeout: FFPROBE_WALL_TIMEOUT,
        idle_timeout: Some(FFMPEG_IDLE_TIMEOUT),
        stdout_limit: MAX_FFPROBE_STDOUT_BYTES,
        stderr_limit: MAX_STDERR_BYTES,
    };

    let outcome = run_supervised(&spec, control).map_err(|_| EngineError::TemporaryCleanupFailed)?;
    runtime
        .cleanup()
        .map_err(|_| EngineError::TemporaryCleanupFailed)?;

    let stdout = match outcome {
        ProcessOutcome::Exited {
            code: 0, stdout, ..
        } => stdout,
        ProcessOutcome::Exited { .. } => return Err(EngineError::ValidationFailed),
        ProcessOutcome::Cancelled { .. } | ProcessOutcome::Paused { .. } => {
            return Err(EngineError::Cancelled);
        }
        ProcessOutcome::TimedOut { .. } => return Err(EngineError::TimedOut),
        ProcessOutcome::IdleTimedOut { .. } => return Err(EngineError::IdleTimedOut),
        ProcessOutcome::OutputLimitExceeded { .. } => return Err(EngineError::OutputTooLarge),
    };
    if stdout.truncated || stdout.bytes.is_empty() {
        return Err(EngineError::ValidationFailed);
    }
    parse_ffprobe_json(&stdout.bytes, input_size)
}

#[derive(Deserialize)]
struct RawProbeResponse {
    #[serde(default)]
    streams: Vec<RawProbeStream>,
    format: Option<RawProbeFormat>,
}

#[derive(Deserialize)]
struct RawProbeStream {
    codec_type: Option<String>,
    codec_name: Option<String>,
}

#[derive(Deserialize)]
struct RawProbeFormat {
    format_name: Option<String>,
    duration: Option<String>,
}

fn parse_ffprobe_json(bytes: &[u8], fallback_size_bytes: u64) -> Result<MediaProbeSummary, EngineError> {
    let parsed: RawProbeResponse =
        serde_json::from_slice(bytes).map_err(|_| EngineError::ValidationFailed)?;
    let format = parsed.format.ok_or(EngineError::ValidationFailed)?;
    let format_name = format
        .format_name
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty() && s.len() <= 128)
        .ok_or(EngineError::ValidationFailed)?;

    if parsed.streams.is_empty() {
        return Err(EngineError::ValidationFailed);
    }

    let duration_seconds = format
        .duration
        .as_deref()
        .and_then(|d| d.parse::<f64>().ok())
        .filter(|d| d.is_finite() && *d >= 0.0);

    let mut video_stream_count = 0usize;
    let mut audio_stream_count = 0usize;
    let mut video_codec = None;
    let mut audio_codec = None;

    for stream in &parsed.streams {
        match stream.codec_type.as_deref() {
            Some("video") => {
                video_stream_count += 1;
                if video_codec.is_none() {
                    video_codec = stream
                        .codec_name
                        .as_deref()
                        .filter(|s| !s.is_empty() && s.len() <= 64)
                        .map(str::to_owned);
                }
            }
            Some("audio") => {
                audio_stream_count += 1;
                if audio_codec.is_none() {
                    audio_codec = stream
                        .codec_name
                        .as_deref()
                        .filter(|s| !s.is_empty() && s.len() <= 64)
                        .map(str::to_owned);
                }
            }
            _ => {}
        }
    }

    let has_video = video_stream_count > 0;
    let has_audio = audio_stream_count > 0;
    if !has_video && !has_audio {
        return Err(EngineError::ValidationFailed);
    }

    Ok(MediaProbeSummary {
        format_name,
        duration_seconds,
        size_bytes: fallback_size_bytes,
        stream_count: parsed.streams.len(),
        video_stream_count,
        audio_stream_count,
        has_video,
        has_audio,
        video_codec,
        audio_codec,
    })
}

#[cfg(windows)]
const SHA256_H0: [u32; 8] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];

#[cfg(windows)]
const SHA256_K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

#[cfg(windows)]
fn sha256_compress(state: &mut [u32; 8], block: &[u8; 64]) {
    let mut w = [0u32; 64];
    for (i, chunk) in block.chunks_exact(4).enumerate() {
        w[i] = u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
    }
    for i in 16..64 {
        let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
        let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16]
            .wrapping_add(s0)
            .wrapping_add(w[i - 7])
            .wrapping_add(s1);
    }
    let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = *state;
    for i in 0..64 {
        let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
        let ch = (e & f) ^ ((!e) & g);
        let temp1 = h
            .wrapping_add(s1)
            .wrapping_add(ch)
            .wrapping_add(SHA256_K[i])
            .wrapping_add(w[i]);
        let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
        let maj = (a & b) ^ (a & c) ^ (b & c);
        let temp2 = s0.wrapping_add(maj);

        h = g;
        g = f;
        f = e;
        e = d.wrapping_add(temp1);
        d = c;
        c = b;
        b = a;
        a = temp1.wrapping_add(temp2);
    }
    state[0] = state[0].wrapping_add(a);
    state[1] = state[1].wrapping_add(b);
    state[2] = state[2].wrapping_add(c);
    state[3] = state[3].wrapping_add(d);
    state[4] = state[4].wrapping_add(e);
    state[5] = state[5].wrapping_add(f);
    state[6] = state[6].wrapping_add(g);
    state[7] = state[7].wrapping_add(h);
}

#[cfg(windows)]
fn sha256_file(path: &Path, expected_bytes: u64) -> std::io::Result<[u8; 32]> {
    use std::io::Read;

    let mut file = std::fs::File::open(path)?;
    let mut state = SHA256_H0;
    let mut buffer = vec![0u8; 64 * 1024];
    let mut tail = [0u8; 64];
    let mut tail_len = 0usize;
    let mut total_bytes = 0u64;

    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        total_bytes = total_bytes.saturating_add(read as u64);
        if total_bytes > expected_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "file exceeded expected size",
            ));
        }
        let mut slice = &buffer[..read];
        if tail_len > 0 {
            let needed = 64 - tail_len;
            if slice.len() >= needed {
                tail[tail_len..64].copy_from_slice(&slice[..needed]);
                sha256_compress(&mut state, &tail);
                tail_len = 0;
                slice = &slice[needed..];
            } else {
                tail[tail_len..tail_len + slice.len()].copy_from_slice(slice);
                tail_len += slice.len();
                slice = &[];
            }
        }
        let mut chunks = slice.chunks_exact(64);
        for chunk in &mut chunks {
            let block: &[u8; 64] = chunk.try_into().expect("64-byte chunk");
            sha256_compress(&mut state, block);
        }
        let remainder = chunks.remainder();
        if !remainder.is_empty() {
            tail[..remainder.len()].copy_from_slice(remainder);
            tail_len = remainder.len();
        }
    }

    if total_bytes != expected_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "file size did not match expected size",
        ));
    }

    tail[tail_len] = 0x80;
    tail[tail_len + 1..].fill(0);
    if tail_len + 1 > 56 {
        sha256_compress(&mut state, &tail);
        tail.fill(0);
    }
    let bit_len = total_bytes.wrapping_mul(8);
    tail[56..64].copy_from_slice(&bit_len.to_be_bytes());
    sha256_compress(&mut state, &tail);

    let mut out = [0u8; 32];
    for (i, word) in state.iter().enumerate() {
        out[i * 4..(i + 1) * 4].copy_from_slice(&word.to_be_bytes());
    }
    Ok(out)
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;

    #[test]
    fn ffprobe_probes_local_fixture_and_verify_ffmpeg_rejects_tampered_binaries() {
        let scratch = std::env::temp_dir().join(format!(
            "fluxnet-ffmpeg-part5-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&scratch).unwrap();

        // 1. Tampered / missing / non-regular ffmpeg.exe & ffprobe.exe MUST fail closed without leaking paths.
        let missing = scratch.join("missing-ffmpeg.exe");
        assert_eq!(
            verify_ffmpeg_executable(&missing).unwrap_err(),
            VERIFIED_FFMPEG_UNAVAILABLE_MSG
        );

        let dir_as_exe = scratch.join("dir-ffmpeg.exe");
        std::fs::create_dir_all(&dir_as_exe).unwrap();
        assert_eq!(
            verify_ffmpeg_executable(&dir_as_exe).unwrap_err(),
            VERIFIED_FFMPEG_UNAVAILABLE_MSG
        );

        let wrong_size = scratch.join("wrong-size-ffmpeg.exe");
        std::fs::write(&wrong_size, b"not a valid ffmpeg binary").unwrap();
        let wrong_size_err = verify_ffmpeg_executable(&wrong_size).unwrap_err();
        assert_eq!(wrong_size_err, VERIFIED_FFMPEG_UNAVAILABLE_MSG);
        assert!(!wrong_size_err.contains(&scratch.to_string_lossy().as_ref()));

        let exact_size_tampered = scratch.join("tampered-exact-size-ffmpeg.exe");
        {
            let f = std::fs::File::create(&exact_size_tampered).unwrap();
            f.set_len(VERIFIED_FFMPEG_SIZE_BYTES).unwrap();
        }
        assert_eq!(
            verify_ffmpeg_executable(&exact_size_tampered).unwrap_err(),
            VERIFIED_FFMPEG_UNAVAILABLE_MSG
        );
        let _ = std::fs::remove_file(&exact_size_tampered);

        // 2. Release-profile candidate list excludes experiments/media-engine-poc paths.
        let release_candidates =
            collect_tool_candidates_for_profile(None, "ffmpeg.exe", &["FLUXNET_FFMPEG_EXE"], false);
        assert!(
            release_candidates.iter().all(|p| {
                let text = p.to_string_lossy().to_ascii_lowercase();
                !text.contains("experiments") && !text.contains("media-engine-poc")
            }),
            "release candidates must not embed PoC paths: {release_candidates:?}"
        );

        // 3. Safe local argv rules reject network URLs / option injection and enforce threads + protocol whitelist + copy.
        assert!(
            build_safe_ffmpeg_copy_args(
                Path::new("https://example.test/remote.mp4"),
                None,
                MediaOperation::RemuxCopy {
                    container: OutputContainer::Mp4
                },
                &scratch.join("out.mp4"),
            )
            .is_err()
        );
        let safe_args = build_safe_ffmpeg_copy_args(
            &scratch.join("video.mp4"),
            Some(&scratch.join("audio.m4a")),
            MediaOperation::MuxSplitCopy {
                container: OutputContainer::Mp4,
            },
            &scratch.join("merged.mp4"),
        )
        .unwrap()
        .into_iter()
        .map(|s| s.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
        assert!(safe_args.windows(2).any(|w| w == ["-threads", "4"]));
        assert!(
            safe_args
                .windows(2)
                .any(|w| w == ["-protocol_whitelist", "file"])
        );
        assert!(safe_args.windows(2).any(|w| w == ["-map", "0:v:0"]));
        assert!(safe_args.windows(2).any(|w| w == ["-map", "1:a:0"]));
        assert!(safe_args.windows(2).any(|w| w == ["-c", "copy"]));

        // 4. Resolve verified LGPL ffmpeg.exe & ffprobe.exe, verify encoder list (libmp3lame), and probe local fixture!
        let verified_ffmpeg = resolve_verified_ffmpeg_executable(None)
            .expect("pinned LGPL ffmpeg.exe should resolve and pass SHA-256 verification");
        let verified_ffprobe = resolve_verified_ffprobe_executable(None)
            .expect("pinned LGPL ffprobe.exe should resolve and pass SHA-256 verification");

        let control = DownloadTaskControl::default();
        let encoders = inspect_available_encoders(&verified_ffmpeg, &scratch, &control)
            .expect("ffmpeg -encoders should succeed in contained Job Object");
        assert!(encoders.total_encoders > 50);
        assert!(
            encoders.has_libmp3lame,
            "selected LGPL FFmpeg build should report libmp3lame encoder"
        );

        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        let fixture_webm = repo_root
            .join("experiments")
            .join("media-engine-poc")
            .join("downloads")
            .join("youtube_audio")
            .join("Big Buck Bunny 60fps 4K - Official Blender Foundation Short Film [aqz-KE-bpKQ].webm");
        let fixture_mp3 = repo_root
            .join("experiments")
            .join("media-engine-poc")
            .join("downloads")
            .join("mp3")
            .join("bbb-128.mp3");
        let probe_target = if fixture_webm.is_file() {
            fixture_webm
        } else {
            fixture_mp3
        };

        let probe_dir = scratch.join("probe-work");
        std::fs::create_dir_all(&probe_dir).unwrap();
        let summary = probe_local_media(&verified_ffprobe, &probe_target, &probe_dir, &control)
            .expect("ffprobe must inspect local fixture media without network access");
        assert!(summary.size_bytes > 0);
        assert!(summary.stream_count >= 1);
        assert!(summary.has_audio);
        assert!(summary.duration_seconds.is_some_and(|d| d > 1.0));
        assert!(
            summary.format_name.contains("webm") || summary.format_name.contains("mp3"),
            "unexpected probed format_name: {}",
            summary.format_name
        );
        assert_eq!(
            std::fs::read_dir(&probe_dir).unwrap().count(),
            0,
            "ffprobe temporary runtime directory must be cleaned up"
        );

        std::fs::remove_dir_all(scratch).unwrap();
    }

    #[test]
    fn merge_combines_local_video_and_audio_streams_and_rejects_corrupt_inputs() {
        let scratch = std::env::temp_dir().join(format!(
            "fluxnet-ffmpeg-part6-merge-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&scratch).unwrap();

        let verified_ffmpeg = resolve_verified_ffmpeg_executable(None)
            .expect("pinned LGPL ffmpeg.exe should resolve");
        let verified_ffprobe = resolve_verified_ffprobe_executable(None)
            .expect("pinned LGPL ffprobe.exe should resolve");

        // Generate two tiny offline local fixture streams (0.5s mpeg4 video-only .mp4 + 0.5s aac audio-only .m4a).
        let video_only = scratch.join("video_only.mp4");
        let audio_only = scratch.join("audio_only.m4a");
        let status_v = std::process::Command::new(&verified_ffmpeg)
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
            .arg(&video_only)
            .status()
            .expect("generate local video-only fixture");
        assert!(status_v.success());
        let status_a = std::process::Command::new(&verified_ffmpeg)
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
            .arg(&audio_only)
            .status()
            .expect("generate local audio-only fixture");
        assert!(status_a.success());

        let work_dir = scratch.join("merge-work");
        std::fs::create_dir_all(&work_dir).unwrap();
        let adapter = FfmpegAdapter {
            ffmpeg_executable: verified_ffmpeg.clone(),
            ffprobe_executable: verified_ffprobe.clone(),
        };
        let control = DownloadTaskControl::default();
        let staged = adapter
            .merge(
                &StagedInput {
                    primary_path: video_only.clone(),
                    secondary_path: Some(audio_only.clone()),
                    working_directory: work_dir.clone(),
                },
                OutputContainer::Mp4,
                &control,
            )
            .expect("MediaProcessor::merge should combine local video + audio and pass ffprobe + full decode");

        assert!(staged.path.is_file());
        assert!(staged.bytes > 0);
        assert_eq!(staged.container, OutputContainer::Mp4);
        assert_eq!(staged.probe.video_stream_count, 1);
        assert_eq!(staged.probe.audio_stream_count, 1);
        assert!(staged.probe.duration_seconds.is_some_and(|d| d > 0.0));
        let _ = std::fs::remove_file(&staged.path);
        assert_eq!(
            std::fs::read_dir(&work_dir).unwrap().count(),
            0,
            "merge staging dir must be completely cleaned up"
        );

        // Corrupt audio input must fail closed and leave zero files in work_dir.
        let corrupt_audio = scratch.join("corrupt_audio.m4a");
        std::fs::write(&corrupt_audio, b"NOT_A_VALID_AUDIO_STREAM_BYTES").unwrap();
        let err = adapter
            .merge(
                &StagedInput {
                    primary_path: video_only,
                    secondary_path: Some(corrupt_audio),
                    working_directory: work_dir.clone(),
                },
                OutputContainer::Mp4,
                &control,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            EngineError::MergeFailed | EngineError::ValidationFailed
        ));
        assert_eq!(
            std::fs::read_dir(&work_dir).unwrap().count(),
            0,
            "failed merge must clean up staging and partial merged files"
        );

        std::fs::remove_dir_all(scratch).unwrap();
    }
}
