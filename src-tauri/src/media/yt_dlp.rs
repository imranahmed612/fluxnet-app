//! Supervised yt-dlp website discovery and muxed-only video download (`download_selected_video`), wired into `DownloadManager::start_website_download`.

use std::{
    collections::HashSet,
    net::IpAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::download_manager::DownloadTaskControl;

const MAX_METADATA_BYTES: usize = 2 * 1024 * 1024;
const MAX_FORMATS: usize = 2048;
const MAX_TITLE_CHARS: usize = 512;
const MAX_TEXT_CHARS: usize = 128;
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(60);
pub(crate) const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const SOCKET_TIMEOUT_SECS: &str = "30";
const MAX_VIDEO_DOWNLOAD_BYTES: u64 = 1024 * 1024 * 1024;
const VIDEO_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(45 * 60);
const PROGRESS_PREFIX: &str = "FLUXNET_PROGRESS";
#[cfg(windows)]
pub(crate) const VERIFIED_YTDLP_SIZE_BYTES: u64 = 23_575_744;
#[cfg(windows)]
pub(crate) const VERIFIED_YTDLP_SHA256: [u8; 32] = [
    0xf5, 0x38, 0x9d, 0x36, 0x98, 0xec, 0xde, 0x4e, 0xf0, 0x6f, 0x4e, 0x6b, 0x1c, 0xe0, 0x29, 0x4f,
    0xe9, 0xac, 0x13, 0x96, 0x22, 0x8c, 0x89, 0x90, 0xb8, 0xd3, 0x41, 0x6d, 0x6d, 0x17, 0x7a, 0x7d,
];
#[cfg(windows)]
const VERIFIED_YTDLP_UNAVAILABLE_MSG: &str = "The verified yt-dlp media engine is not available.";

#[cfg(windows)]
struct RuntimeTempDir {
    task_root: PathBuf,
    path: PathBuf,
    handled: bool,
}

#[cfg(windows)]
impl RuntimeTempDir {
    fn create(task_workspace: &Path) -> std::io::Result<Self> {
        use std::os::windows::fs::MetadataExt;
        use std::sync::atomic::{AtomicU64, Ordering};

        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;

        let workspace_metadata = std::fs::symlink_metadata(task_workspace)?;
        if !workspace_metadata.is_dir()
            || workspace_metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "task workspace is not a non-reparse directory",
            ));
        }
        let task_root = std::fs::canonicalize(task_workspace)?;
        let ticks = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();

        for _ in 0..32 {
            let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            let name = format!(
                ".fluxnet-yt-dlp-runtime-{}-{ticks}-{id}",
                std::process::id()
            );
            let path = task_root.join(name);
            match std::fs::create_dir(&path) {
                Ok(()) => {
                    let mut runtime = Self {
                        task_root,
                        path,
                        handled: false,
                    };
                    let validation = (|| {
                        let metadata = std::fs::symlink_metadata(&runtime.path)?;
                        if !metadata.is_dir()
                            || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
                        {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                "created runtime workspace failed path validation",
                            ));
                        }
                        let canonical = std::fs::canonicalize(&runtime.path)?;
                        if canonical.parent() != Some(runtime.task_root.as_path()) {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                "runtime workspace escaped task workspace",
                            ));
                        }
                        Ok(canonical)
                    })();
                    return match validation {
                        Ok(canonical) => {
                            runtime.path = canonical;
                            Ok(runtime)
                        }
                        Err(error) => {
                            runtime.preserve_with_evidence("runtime_workspace_validation_failed");
                            Err(error)
                        }
                    };
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "could not reserve a unique runtime workspace",
        ))
    }

    fn path(&self) -> &Path {
        &self.path
    }

    /// The exact per-task directory was created exclusively by this adapter.
    /// It is removed only after the supervisor confirms the Job Object has no
    /// active processes. `remove_dir_all` does not follow reparse points.
    fn cleanup(mut self) -> std::io::Result<()> {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;

        let result = (|| {
            let parent = std::fs::canonicalize(&self.task_root)?;
            let metadata = std::fs::symlink_metadata(&self.path)?;
            if !metadata.is_dir()
                || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
                || self.path.parent() != Some(parent.as_path())
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "runtime cleanup path failed ownership validation",
                ));
            }
            std::fs::remove_dir_all(&self.path)
        })();
        if let Err(error) = &result {
            self.write_evidence(&format!("cleanup_failed={:?}", error.kind()));
        }
        self.handled = true;
        result
    }

    fn preserve_with_evidence(&mut self, reason: &str) {
        self.write_evidence(reason);
        self.handled = true;
    }

    fn write_evidence(&self, reason: &str) {
        use std::io::Write;
        let Some(name) = self.path.file_name().and_then(|name| name.to_str()) else {
            return;
        };
        // The generated directory basename and reason are allowlisted; this
        // marker contains no URL, title, account data, or absolute path.
        let marker = self.task_root.join(format!("{name}.cleanup-warning.txt"));
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(marker)
        {
            let safe_reason = if reason
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'=')
            {
                reason
            } else {
                "cleanup_warning"
            };
            let _ = writeln!(file, "runtime_workspace={name}\n{safe_reason}");
        }
    }
}

#[cfg(windows)]
impl Drop for RuntimeTempDir {
    fn drop(&mut self) {
        if !self.handled {
            self.write_evidence("cleanup_not_confirmed");
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct VideoDiscovery {
    pub title: String,
    pub duration_seconds: Option<f64>,
    pub extractor: String,
    pub formats: Vec<DiscoveredFormat>,
    pub best_available: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveredFormat {
    pub format_id: String,
    pub resolution: Option<String>,
    pub height: Option<u32>,
    pub fps: Option<f64>,
    pub video_codec: Option<String>,
    pub audio_codec: Option<String>,
    pub container: Option<String>,
    pub file_size: Option<u64>,
    pub has_video: bool,
    pub has_audio: bool,
    pub requires_separate_audio: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum FormatSelection {
    #[serde(alias = "BestAvailable", alias = "best_available")]
    BestAvailable,
    #[serde(alias = "Exact")]
    Exact(String),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct VideoDownloadProgress {
    pub downloaded_bytes: u64,
    pub total_bytes: Option<u64>,
    pub speed_bytes_per_second: Option<f64>,
    pub eta_seconds: Option<f64>,
    pub percent: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VideoDownloadResult {
    pub destination: PathBuf,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum VideoDownloadError {
    InvalidUrl,
    InvalidDestination,
    InvalidSelection,
    SplitStreamsUnsupported,
    NoMuxedMp4,
    MergeFailed,
    ProcessFailed,
    TimedOut,
    IdleTimedOut,
    Cancelled,
    Paused { downloaded_bytes: u64 },
    OutputTooLarge,
    InvalidMedia,
    DestinationExists,
    Filesystem,
    TemporaryCleanupFailed,
}

impl std::fmt::Display for VideoDownloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidUrl => "Enter a public HTTP or HTTPS video page URL.",
            Self::InvalidDestination => "Choose an absolute MP4 destination in an existing folder.",
            Self::InvalidSelection => "The selected format was not returned by discovery.",
            Self::SplitStreamsUnsupported => {
                "This prototype downloads muxed formats only; separate audio/video merging is not available yet."
            }
            Self::NoMuxedMp4 => "No muxed MP4 or WebM format is available for this video.",
            Self::MergeFailed => "Audio and video streams could not be merged.",
            Self::ProcessFailed => "The video could not be downloaded from this page.",
            Self::TimedOut => "The video download timed out.",
            Self::IdleTimedOut => "The video download stalled without progress and timed out.",
            Self::Cancelled => "The video download was cancelled.",
            Self::Paused { .. } => "The video download was paused.",
            Self::OutputTooLarge => "The video exceeded the configured size limit.",
            Self::InvalidMedia => "The downloaded MP4 is incomplete or invalid.",
            Self::DestinationExists => "The destination appeared before the file could be committed.",
            Self::Filesystem => "A filesystem operation for the video download failed.",
            Self::TemporaryCleanupFailed => {
                "The video download could not safely clean up its task workspace."
            }
        })
    }
}

impl VideoDiscovery {
    /// Checks an explicit UI choice against the returned catalog. Missing
    /// formats are rejected; this prototype never substitutes another ID.
    pub(crate) fn validate_selection(
        &self,
        selection: &FormatSelection,
    ) -> Result<(), DiscoveryError> {
        match selection {
            FormatSelection::BestAvailable if self.best_available => Ok(()),
            FormatSelection::Exact(id)
                if self.formats.iter().any(|format| &format.format_id == id) =>
            {
                Ok(())
            }
            _ => Err(DiscoveryError::NoFormats),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DiscoveryError {
    InvalidUrl,
    ProcessFailed,
    TimedOut,
    IdleTimedOut,
    Cancelled,
    OutputTooLarge,
    InvalidJson,
    PlaylistNotSupported,
    NoFormats,
    TemporaryWorkspaceUnavailable,
    TemporaryCleanupFailed,
}

impl std::fmt::Display for DiscoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidUrl => "Enter a public HTTP or HTTPS video page URL.",
            Self::ProcessFailed => "Video information could not be retrieved from this page.",
            Self::TimedOut => "Video information retrieval timed out.",
            Self::IdleTimedOut => {
                "Video information retrieval stalled without progress and timed out."
            }
            Self::Cancelled => "Video information retrieval was cancelled.",
            Self::OutputTooLarge => "The video information response exceeded the safe size limit.",
            Self::InvalidJson => "The video information response was invalid.",
            Self::PlaylistNotSupported => "Playlist discovery is disabled; provide one video page.",
            Self::NoFormats => "No available media formats were returned for this page.",
            Self::TemporaryWorkspaceUnavailable => {
                "A private temporary workspace could not be prepared for video information."
            }
            Self::TemporaryCleanupFailed => {
                "Video information could not be safely cleaned up after processing."
            }
        })
    }
}

/// Metadata-only discovery using the existing contained process supervisor.
/// `working_directory` must be a private task workspace created by the caller.
#[cfg(windows)]
pub(crate) fn discover(
    executable: &Path,
    working_directory: &Path,
    page_url: &str,
    control: &DownloadTaskControl,
) -> Result<VideoDiscovery, DiscoveryError> {
    discover_with_limits(
        executable,
        working_directory,
        page_url,
        control,
        DISCOVERY_TIMEOUT,
        MAX_METADATA_BYTES,
        Vec::new(),
        None,
    )
}

pub(crate) fn is_muxed_mp4_or_webm(format: &DiscoveredFormat) -> bool {
    let container = format.container.as_deref().map(str::to_ascii_lowercase);
    matches!(container.as_deref(), Some("mp4" | "webm"))
        && format.video_codec.as_deref() != Some("none")
        && format.audio_codec.as_deref() != Some("none")
        && !format.requires_separate_audio
}

pub(crate) fn retain_muxed_formats(
    mut discovery: VideoDiscovery,
) -> Result<VideoDiscovery, VideoDownloadError> {
    discovery.formats.retain(is_muxed_mp4_or_webm);
    if discovery.formats.is_empty() {
        return Err(VideoDownloadError::NoMuxedMp4);
    }
    discovery.best_available = true;
    Ok(discovery)
}

/// Runs metadata-only discovery inside an exclusively created per-call task
/// workspace and returns only available muxed MP4/WebM formats.
#[cfg(windows)]
pub(crate) fn discover_muxed_in_workspace(
    executable: &Path,
    base_directory: &Path,
    page_url: &str,
    control: &DownloadTaskControl,
) -> Result<VideoDiscovery, String> {
    discover_muxed_in_workspace_with_test_hooks(
        executable,
        base_directory,
        page_url,
        control,
        DISCOVERY_TIMEOUT,
        MAX_METADATA_BYTES,
        Vec::new(),
        None,
    )
}

#[cfg(windows)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn discover_muxed_in_workspace_with_test_hooks(
    executable: &Path,
    base_directory: &Path,
    page_url: &str,
    control: &DownloadTaskControl,
    timeout: Duration,
    output_limit: usize,
    test_prefix: Vec<std::ffi::OsString>,
    test_mode: Option<&str>,
) -> Result<VideoDiscovery, String> {
    let mut task_workspace = RuntimeTempDir::create(base_directory)
        .map_err(|_| DiscoveryError::TemporaryWorkspaceUnavailable.to_string())?;
    let discovery = discover_with_limits(
        executable,
        task_workspace.path(),
        page_url,
        control,
        timeout,
        output_limit,
        test_prefix,
        test_mode,
    );
    if matches!(discovery, Err(DiscoveryError::TemporaryCleanupFailed)) {
        task_workspace.preserve_with_evidence("discovery_task_cleanup_failed");
        return Err(DiscoveryError::TemporaryCleanupFailed.to_string());
    }
    task_workspace
        .cleanup()
        .map_err(|_| DiscoveryError::TemporaryCleanupFailed.to_string())?;
    let discovery = discovery.map_err(|error| error.to_string())?;
    retain_muxed_formats(discovery).map_err(|error| error.to_string())
}

#[cfg(windows)]
fn discover_with_limits(
    executable: &Path,
    working_directory: &Path,
    page_url: &str,
    control: &DownloadTaskControl,
    timeout: Duration,
    output_limit: usize,
    test_prefix: Vec<std::ffi::OsString>,
    test_mode: Option<&str>,
) -> Result<VideoDiscovery, DiscoveryError> {
    use crate::process::{ProcessOutcome, ProcessSpec, run_supervised};
    use std::ffi::OsString;

    let normalized_url = validate_public_page_url(page_url)?;
    if output_limit == 0 || output_limit > MAX_METADATA_BYTES {
        return Err(DiscoveryError::OutputTooLarge);
    }
    let mut runtime_temp = RuntimeTempDir::create(working_directory)
        .map_err(|_| DiscoveryError::TemporaryWorkspaceUnavailable)?;
    let mut args = test_prefix;
    if test_mode.is_none() {
        args.extend(yt_dlp_metadata_args(&normalized_url));
    }
    #[cfg(not(test))]
    let _ = test_mode;
    let mut environment = Vec::new();
    for name in ["SystemRoot", "WINDIR"] {
        if let Some(value) = std::env::var_os(name) {
            environment.push((OsString::from(name), value));
        }
    }
    environment.extend([
        (
            OsString::from("TEMP"),
            runtime_temp.path().as_os_str().to_owned(),
        ),
        (
            OsString::from("TMP"),
            runtime_temp.path().as_os_str().to_owned(),
        ),
        (OsString::from("PYTHONNOUSERSITE"), OsString::from("1")),
        (
            OsString::from("PYTHONDONTWRITEBYTECODE"),
            OsString::from("1"),
        ),
    ]);
    #[cfg(test)]
    if let Some(mode) = test_mode {
        environment.push((
            OsString::from("FLUXNET_JOB_OBJECT_TEST_CHILD"),
            OsString::from("1"),
        ));
        environment.push((
            OsString::from("FLUXNET_JOB_OBJECT_TEST_MODE"),
            OsString::from(mode),
        ));
    }
    let spec = ProcessSpec {
        executable: executable.to_path_buf(),
        args,
        working_directory: working_directory.to_path_buf(),
        environment,
        timeout,
        idle_timeout: Some(IDLE_TIMEOUT),
        stdout_limit: output_limit,
        stderr_limit: 16 * 1024,
    };
    let outcome = match run_supervised(&spec, control) {
        Ok(outcome) => outcome,
        Err(_) => {
            runtime_temp.preserve_with_evidence("process_tree_shutdown_unconfirmed");
            return Err(DiscoveryError::TemporaryCleanupFailed);
        }
    };
    runtime_temp
        .cleanup()
        .map_err(|_| DiscoveryError::TemporaryCleanupFailed)?;
    let stdout = match outcome {
        ProcessOutcome::Exited {
            code: 0, stdout, ..
        } => stdout,
        ProcessOutcome::Exited { code, stderr, .. } => {
            #[cfg(test)]
            if std::env::var_os("FLUXNET_YTDLP_TEST_DIAGNOSTICS").is_some() {
                let excerpt = String::from_utf8_lossy(&stderr.bytes);
                eprintln!(
                    "yt-dlp test diagnostic: exit_code={code}, stderr_truncated={}, stderr={}",
                    stderr.truncated,
                    redact_metadata_text(&excerpt, 1024),
                );
            }
            let _ = code;
            let _ = stderr;
            return Err(DiscoveryError::ProcessFailed);
        }
        ProcessOutcome::Cancelled { .. } | ProcessOutcome::Paused { .. } => {
            return Err(DiscoveryError::Cancelled);
        }
        ProcessOutcome::TimedOut { .. } => return Err(DiscoveryError::TimedOut),
        ProcessOutcome::IdleTimedOut { .. } => return Err(DiscoveryError::IdleTimedOut),
        ProcessOutcome::OutputLimitExceeded { .. } => return Err(DiscoveryError::OutputTooLarge),
    };
    if stdout.truncated || stdout.bytes.len() > output_limit {
        return Err(DiscoveryError::OutputTooLarge);
    }
    #[cfg(test)]
    if test_mode.is_some() {
        let start = stdout
            .bytes
            .windows(b"FXJSONSTART".len())
            .position(|window| window == b"FXJSONSTART")
            .ok_or(DiscoveryError::InvalidJson)?
            + b"FXJSONSTART".len();
        let tail = &stdout.bytes[start..];
        let end = tail
            .windows(b"FXJSONEND".len())
            .position(|window| window == b"FXJSONEND")
            .ok_or(DiscoveryError::InvalidJson)?;
        return parse_discovery_json(&tail[..end]);
    }
    parse_discovery_json(&stdout.bytes)
}

fn yt_dlp_metadata_args(url: &str) -> Vec<std::ffi::OsString> {
    use std::ffi::OsString;
    [
        OsString::from("--ignore-config"),
        OsString::from("--no-update"),
        OsString::from("--no-cache-dir"),
        OsString::from("--no-cookies"),
        OsString::from("--no-cookies-from-browser"),
        OsString::from("--no-playlist"),
        OsString::from("--socket-timeout"),
        OsString::from(SOCKET_TIMEOUT_SECS),
        OsString::from("--retries"),
        OsString::from("0"),
        OsString::from("--fragment-retries"),
        OsString::from("0"),
        OsString::from("--skip-download"),
        OsString::from("--dump-single-json"),
        OsString::from("--no-write-info-json"),
        OsString::from("--no-write-thumbnail"),
        OsString::from("--no-write-subs"),
        OsString::from("--no-write-auto-subs"),
        OsString::from("--no-write-description"),
        OsString::from("--no-write-playlist-metafiles"),
        OsString::from("--no-progress"),
        OsString::from("--no-warnings"),
        OsString::from("--"),
        OsString::from(url),
    ]
    .into_iter()
    .collect()
}

#[cfg(windows)]
pub(crate) fn download_video(
    executable: &Path,
    page_url: &str,
    destination: &Path,
    catalog: &VideoDiscovery,
    selection: &FormatSelection,
    control: &DownloadTaskControl,
    on_progress: std::sync::Arc<dyn Fn(VideoDownloadProgress) + Send + Sync>,
) -> Result<VideoDownloadResult, VideoDownloadError> {
    download_video_with_limits(
        executable,
        page_url,
        destination,
        catalog,
        selection,
        control,
        MAX_VIDEO_DOWNLOAD_BYTES,
        VIDEO_DOWNLOAD_TIMEOUT,
        on_progress,
    )
}

#[cfg(windows)]
pub(crate) fn download_selected_video(
    executable: &Path,
    page_url: &str,
    destination: &Path,
    selected_format: &SelectedFormat,
    control: &DownloadTaskControl,
    on_progress: std::sync::Arc<dyn Fn(VideoDownloadProgress) + Send + Sync>,
) -> Result<VideoDownloadResult, VideoDownloadError> {
    download_selected_video_with_test_hooks(
        executable,
        page_url,
        destination,
        selected_format,
        control,
        MAX_VIDEO_DOWNLOAD_BYTES,
        VIDEO_DOWNLOAD_TIMEOUT,
        Vec::new(),
        None,
        on_progress,
    )
}

#[cfg(windows)]
fn download_video_with_limits(
    executable: &Path,
    page_url: &str,
    destination: &Path,
    catalog: &VideoDiscovery,
    selection: &FormatSelection,
    control: &DownloadTaskControl,
    maximum_bytes: u64,
    timeout: Duration,
    on_progress: std::sync::Arc<dyn Fn(VideoDownloadProgress) + Send + Sync>,
) -> Result<VideoDownloadResult, VideoDownloadError> {
    download_video_with_test_hooks(
        executable,
        page_url,
        destination,
        catalog,
        selection,
        control,
        maximum_bytes,
        timeout,
        Vec::new(),
        None,
        on_progress,
    )
}

#[cfg(windows)]
#[allow(clippy::too_many_arguments)]
fn download_video_with_test_hooks(
    executable: &Path,
    page_url: &str,
    destination: &Path,
    catalog: &VideoDiscovery,
    selection: &FormatSelection,
    control: &DownloadTaskControl,
    maximum_bytes: u64,
    timeout: Duration,
    test_prefix: Vec<std::ffi::OsString>,
    test_mode: Option<&str>,
    on_progress: std::sync::Arc<dyn Fn(VideoDownloadProgress) + Send + Sync>,
) -> Result<VideoDownloadResult, VideoDownloadError> {
    let selected_format = select_muxed_format(catalog, selection)?;
    download_selected_video_with_test_hooks(
        executable,
        page_url,
        destination,
        &selected_format,
        control,
        maximum_bytes,
        timeout,
        test_prefix,
        test_mode,
        on_progress,
    )
}

#[cfg(windows)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn download_selected_video_with_test_hooks(
    executable: &Path,
    page_url: &str,
    destination: &Path,
    selected_format: &SelectedFormat,
    control: &DownloadTaskControl,
    maximum_bytes: u64,
    timeout: Duration,
    test_prefix: Vec<std::ffi::OsString>,
    test_mode: Option<&str>,
    on_progress: std::sync::Arc<dyn Fn(VideoDownloadProgress) + Send + Sync>,
) -> Result<VideoDownloadResult, VideoDownloadError> {
    use crate::process::{
        MonitoredOutput, OutputHandler, ProcessOutcome, ProcessSpec, ProcessStream,
        run_supervised_with_output,
    };
    use std::ffi::OsString;

    let normalized_url =
        validate_public_page_url(page_url).map_err(|_| VideoDownloadError::InvalidUrl)?;
    if maximum_bytes == 0 || maximum_bytes > MAX_VIDEO_DOWNLOAD_BYTES || timeout.is_zero() {
        return Err(VideoDownloadError::OutputTooLarge);
    }
    validate_selected_format(selected_format)?;
    let destination_text = destination
        .to_str()
        .ok_or(VideoDownloadError::InvalidDestination)?;
    let (validated_destination, filename) =
        crate::download_manager::validate_resumable_download_destination(destination_text)
            .map_err(|_| VideoDownloadError::InvalidDestination)?;
    if validated_destination
        .extension()
        .and_then(|value| value.to_str())
        .is_none_or(|ext| !ext.eq_ignore_ascii_case(selected_format.extension))
    {
        return Err(VideoDownloadError::InvalidDestination);
    }
    let parent = validated_destination
        .parent()
        .ok_or(VideoDownloadError::InvalidDestination)?;
    let parent =
        std::fs::canonicalize(parent).map_err(|_| VideoDownloadError::InvalidDestination)?;
    let final_destination = parent.join(filename);
    if final_destination.exists() {
        return Err(VideoDownloadError::DestinationExists);
    }
    let partial_destination = crate::download_manager::partial_path(&final_destination);

    let mut stage =
        RuntimeTempDir::create(&parent).map_err(|_| VideoDownloadError::TemporaryCleanupFailed)?;
    let staged_part_path = stage
        .path()
        .join(format!("fluxnet-media.{}.part", selected_format.extension));
    let mut restored_partial_bytes = 0u64;
    if let Err(error) = restore_partial_into_stage(
        &partial_destination,
        &staged_part_path,
        maximum_bytes,
        &mut restored_partial_bytes,
    ) {
        return cleanup_stage_with_error(stage, error);
    }

    let mut runtime = match RuntimeTempDir::create(stage.path()) {
        Ok(runtime) => runtime,
        Err(_) => {
            if restored_partial_bytes > 0 {
                let _ = std::fs::rename(&staged_part_path, &partial_destination);
            }
            stage.preserve_with_evidence("runtime_workspace_create_failed");
            return Err(VideoDownloadError::TemporaryCleanupFailed);
        }
    };
    let mut args = test_prefix;
    #[cfg(test)]
    let url_fixture_mode = if test_mode.is_none() && args.is_empty() {
        normalized_url
            .starts_with("https://example.test/fixture-")
            .then_some(normalized_url.as_str())
    } else {
        None
    };
    #[cfg(not(test))]
    let url_fixture_mode: Option<&str> = None;
    if let Some(_fixture_url) = url_fixture_mode {
        args.extend([
            OsString::from("--exact"),
            OsString::from("process::tests::job_object_test_child_fixture"),
            OsString::from("--nocapture"),
        ]);
    } else if test_mode.is_none() {
        args.extend(yt_dlp_download_args(
            &normalized_url,
            &selected_format.selector,
            maximum_bytes,
        ));
    }
    #[cfg(not(test))]
    let _ = test_mode;
    let mut environment = Vec::new();
    for name in ["SystemRoot", "WINDIR"] {
        if let Some(value) = std::env::var_os(name) {
            environment.push((OsString::from(name), value));
        }
    }
    environment.extend([
        (
            OsString::from("TEMP"),
            runtime.path().as_os_str().to_owned(),
        ),
        (OsString::from("TMP"), runtime.path().as_os_str().to_owned()),
        (OsString::from("PYTHONNOUSERSITE"), OsString::from("1")),
        (
            OsString::from("PYTHONDONTWRITEBYTECODE"),
            OsString::from("1"),
        ),
    ]);
    #[cfg(test)]
    if let Some(mode) = test_mode {
        environment.push((
            OsString::from("FLUXNET_JOB_OBJECT_TEST_CHILD"),
            OsString::from("1"),
        ));
        environment.push((
            OsString::from("FLUXNET_JOB_OBJECT_TEST_MODE"),
            OsString::from(mode),
        ));
    } else if let Some(fixture_url) = url_fixture_mode {
        environment.push((
            OsString::from("FLUXNET_JOB_OBJECT_TEST_CHILD"),
            OsString::from("1"),
        ));
        environment.push((
            OsString::from("FLUXNET_JOB_OBJECT_TEST_FIXTURE_URL"),
            OsString::from(fixture_url),
        ));
    }
    #[cfg(test)]
    let effective_idle_timeout = if url_fixture_mode == Some("https://example.test/fixture-stall-idle") {
        Some(Duration::from_millis(200))
    } else {
        Some(IDLE_TIMEOUT)
    };
    #[cfg(not(test))]
    let effective_idle_timeout = Some(IDLE_TIMEOUT);
    let spec = ProcessSpec {
        executable: executable.to_path_buf(),
        args,
        working_directory: stage.path().to_path_buf(),
        environment,
        timeout,
        idle_timeout: effective_idle_timeout,
        stdout_limit: 64 * 1024,
        stderr_limit: 64 * 1024,
    };
    let progress_buffers =
        std::sync::Arc::new(std::sync::Mutex::new(ProgressLineBuffers::default()));
    let callback_buffers = std::sync::Arc::clone(&progress_buffers);
    let progress_callback = std::sync::Arc::clone(&on_progress);
    let handler: OutputHandler = std::sync::Arc::new(move |stream: ProcessStream, bytes: &[u8]| {
        let lines = match callback_buffers.lock() {
            Ok(mut buffers) => buffers.feed(stream, bytes),
            Err(_) => Vec::new(),
        };
        for line in lines {
            if let Some(progress) = parse_download_progress_line(&line) {
                progress_callback(progress);
            }
        }
    });
    let monitored = MonitoredOutput {
        directory: stage.path().to_path_buf(),
        filename_prefix: "fluxnet-media.".into(),
        maximum_bytes,
    };
    let process_outcome = match run_supervised_with_output(&spec, control, handler, Some(monitored))
    {
        Ok(outcome) => outcome,
        Err(_) => {
            runtime.preserve_with_evidence("process_tree_shutdown_unconfirmed");
            stage.preserve_with_evidence("process_tree_shutdown_unconfirmed");
            return Err(VideoDownloadError::TemporaryCleanupFailed);
        }
    };
    if runtime.cleanup().is_err() {
        stage.preserve_with_evidence("runtime_cleanup_failed");
        return Err(VideoDownloadError::TemporaryCleanupFailed);
    }

    if matches!(process_outcome, ProcessOutcome::Paused { .. }) && !control.is_cancelled() {
        let paused_bytes = match persist_paused_partial_file(
            stage.path(),
            &partial_destination,
            maximum_bytes,
            selected_format.extension,
        ) {
            Ok(bytes) => bytes,
            Err(error) => return cleanup_stage_with_error(stage, error),
        };
        if stage.cleanup().is_err() {
            return Err(VideoDownloadError::TemporaryCleanupFailed);
        }
        return Err(VideoDownloadError::Paused {
            downloaded_bytes: paused_bytes,
        });
    }

    if matches!(process_outcome, ProcessOutcome::IdleTimedOut { .. }) && !control.is_cancelled() {
        let _ = persist_paused_partial_file(
            stage.path(),
            &partial_destination,
            maximum_bytes,
            selected_format.extension,
        );
        return cleanup_stage_with_error(stage, VideoDownloadError::IdleTimedOut);
    }

    let prepared = prepare_download_file(
        process_outcome,
        stage.path(),
        maximum_bytes,
        selected_format.extension,
    );
    let (staged_file, bytes) = match prepared {
        Ok(result) => result,
        Err(error) => {
            if control.is_cancelled() || matches!(error, VideoDownloadError::Cancelled) {
                let _ = std::fs::remove_file(&partial_destination);
            }
            return cleanup_stage_with_error(stage, error);
        }
    };
    let transfer_path = match create_transfer_link(&staged_file, &parent) {
        Ok(path) => path,
        Err(error) => return cleanup_stage_with_error(stage, error),
    };
    if staged_file.exists() && std::fs::remove_file(&staged_file).is_err() {
        let _ = std::fs::remove_file(&transfer_path);
        return cleanup_stage_with_error(stage, VideoDownloadError::Filesystem);
    }
    if stage.cleanup().is_err() {
        let _ = std::fs::remove_file(&transfer_path);
        return Err(VideoDownloadError::TemporaryCleanupFailed);
    }
    if control.is_cancelled() {
        let _ = std::fs::remove_file(&transfer_path);
        let _ = std::fs::remove_file(&partial_destination);
        return Err(VideoDownloadError::Cancelled);
    }
    if control.is_pause_requested() {
        if let Err(error) = move_file_replacing(&transfer_path, &partial_destination) {
            let _ = std::fs::remove_file(&transfer_path);
            return Err(error);
        }
        return Err(VideoDownloadError::Paused {
            downloaded_bytes: bytes,
        });
    }
    if final_destination.exists() {
        let _ = std::fs::remove_file(&transfer_path);
        return Err(VideoDownloadError::DestinationExists);
    }
    let committed = if std::fs::hard_link(&transfer_path, &final_destination).is_ok() {
        if std::fs::remove_file(&transfer_path).is_err() {
            return Err(VideoDownloadError::TemporaryCleanupFailed);
        }
        Ok(())
    } else if !final_destination.exists()
        && std::fs::rename(&transfer_path, &final_destination).is_ok()
    {
        Ok(())
    } else {
        let _ = std::fs::remove_file(&transfer_path);
        Err(if final_destination.exists() {
            VideoDownloadError::DestinationExists
        } else {
            VideoDownloadError::Filesystem
        })
    };
    committed?;
    on_progress(VideoDownloadProgress {
        downloaded_bytes: bytes,
        total_bytes: Some(bytes),
        speed_bytes_per_second: None,
        eta_seconds: Some(0.0),
        percent: Some(100.0),
    });
    Ok(VideoDownloadResult {
        destination: final_destination,
        bytes,
    })
}

#[cfg(windows)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SelectedFormat {
    pub(crate) selector: String,
    pub(crate) extension: &'static str,
}

#[cfg(windows)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SplitFormatSelection {
    pub(crate) video_format_id: String,
    pub(crate) audio_format_id: String,
    pub(crate) container: &'static str,
}

#[cfg(windows)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DownloadPlan {
    Muxed(SelectedFormat),
    Split(SplitFormatSelection),
}

#[cfg(windows)]
impl DownloadPlan {
    pub(crate) fn extension(&self) -> &'static str {
        match self {
            Self::Muxed(m) => m.extension,
            Self::Split(s) => s.container,
        }
    }

    pub(crate) fn primary_format_id(&self) -> &str {
        match self {
            Self::Muxed(m) => &m.selector,
            Self::Split(s) => &s.video_format_id,
        }
    }

    pub(crate) fn audio_format_id(&self) -> Option<&str> {
        match self {
            Self::Muxed(_) => None,
            Self::Split(s) => Some(&s.audio_format_id),
        }
    }
}

#[cfg(windows)]
pub(crate) fn download_plan_from_info(
    format_id: &str,
    audio_format_id: Option<&str>,
    container: &str,
) -> Result<DownloadPlan, VideoDownloadError> {
    if let Some(audio_id) = audio_format_id {
        if !valid_format_id(format_id) || !valid_format_id(audio_id) {
            return Err(VideoDownloadError::InvalidSelection);
        }
        let ext = if container.eq_ignore_ascii_case("mp4") {
            "mp4"
        } else if container.eq_ignore_ascii_case("webm") {
            "webm"
        } else {
            return Err(VideoDownloadError::NoMuxedMp4);
        };
        Ok(DownloadPlan::Split(SplitFormatSelection {
            video_format_id: format_id.to_owned(),
            audio_format_id: audio_id.to_owned(),
            container: ext,
        }))
    } else {
        Ok(DownloadPlan::Muxed(selected_format_from_info(
            format_id, container,
        )?))
    }
}

#[cfg(windows)]
fn is_video_only_format(format: &DiscoveredFormat) -> bool {
    valid_format_id(&format.format_id)
        && format.has_video
        && !format.has_audio
        && format.video_codec.as_deref() != Some("none")
        && format.audio_codec.as_deref() == Some("none")
}

#[cfg(windows)]
fn is_audio_only_format(format: &DiscoveredFormat) -> bool {
    valid_format_id(&format.format_id)
        && format.has_audio
        && !format.has_video
        && format.audio_codec.as_deref() != Some("none")
        && format.video_codec.as_deref() == Some("none")
}

#[cfg(windows)]
fn is_mp4_friendly_audio(format: &DiscoveredFormat) -> bool {
    let ext = format.container.as_deref().map(str::to_ascii_lowercase);
    matches!(ext.as_deref(), Some("m4a" | "mp4"))
}

#[cfg(windows)]
fn is_mp4_friendly_video(format: &DiscoveredFormat) -> bool {
    let ext = format.container.as_deref().map(str::to_ascii_lowercase);
    matches!(ext.as_deref(), Some("mp4"))
}

#[cfg(windows)]
fn best_audio_for_container<'a>(
    formats: &'a [DiscoveredFormat],
    prefer_mp4: bool,
) -> Option<&'a DiscoveredFormat> {
    let audio_candidates = formats
        .iter()
        .filter(|f| is_audio_only_format(f))
        .collect::<Vec<_>>();
    if audio_candidates.is_empty() {
        return None;
    }
    let pick_best = |list: &[&'a DiscoveredFormat]| -> Option<&'a DiscoveredFormat> {
        list.iter()
            .copied()
            .max_by_key(|f| f.file_size.unwrap_or(0))
    };
    if prefer_mp4 {
        let mp4_audio = audio_candidates
            .iter()
            .copied()
            .filter(|f| is_mp4_friendly_audio(f))
            .collect::<Vec<_>>();
        if let Some(best) = pick_best(&mp4_audio) {
            return Some(best);
        }
    } else {
        let webm_audio = audio_candidates
            .iter()
            .copied()
            .filter(|f| {
                f.container
                    .as_deref()
                    .is_some_and(|c| c.eq_ignore_ascii_case("webm"))
            })
            .collect::<Vec<_>>();
        if let Some(best) = pick_best(&webm_audio) {
            return Some(best);
        }
    }
    pick_best(&audio_candidates)
}

#[cfg(windows)]
pub(crate) fn select_split_format(
    catalog: &VideoDiscovery,
    selection: &FormatSelection,
) -> Result<SplitFormatSelection, VideoDownloadError> {
    match selection {
        FormatSelection::BestAvailable => {
            let videos = catalog
                .formats
                .iter()
                .filter(|f| is_video_only_format(f))
                .collect::<Vec<_>>();
            if videos.is_empty() {
                return Err(VideoDownloadError::NoMuxedMp4);
            }
            let mp4_videos = videos
                .iter()
                .copied()
                .filter(|f| is_mp4_friendly_video(f))
                .collect::<Vec<_>>();
            let (chosen_video, prefer_mp4) = if !mp4_videos.is_empty() {
                let v = mp4_videos
                    .into_iter()
                    .max_by_key(|f| {
                        (
                            f.height.unwrap_or(0),
                            f.fps.map(|v| (v * 100.0) as u64).unwrap_or(0),
                            f.file_size.unwrap_or(0),
                        )
                    })
                    .ok_or(VideoDownloadError::NoMuxedMp4)?;
                (v, true)
            } else {
                let v = videos
                    .into_iter()
                    .max_by_key(|f| {
                        (
                            f.height.unwrap_or(0),
                            f.fps.map(|v| (v * 100.0) as u64).unwrap_or(0),
                            f.file_size.unwrap_or(0),
                        )
                    })
                    .ok_or(VideoDownloadError::NoMuxedMp4)?;
                (v, false)
            };
            let chosen_audio = best_audio_for_container(&catalog.formats, prefer_mp4)
                .ok_or(VideoDownloadError::NoMuxedMp4)?;
            let container = if prefer_mp4 && is_mp4_friendly_audio(chosen_audio) {
                "mp4"
            } else if chosen_video
                .container
                .as_deref()
                .is_some_and(|c| c.eq_ignore_ascii_case("webm"))
                && chosen_audio
                    .container
                    .as_deref()
                    .is_some_and(|c| c.eq_ignore_ascii_case("webm"))
            {
                "webm"
            } else {
                "mp4"
            };
            Ok(SplitFormatSelection {
                video_format_id: chosen_video.format_id.clone(),
                audio_format_id: chosen_audio.format_id.clone(),
                container,
            })
        }
        FormatSelection::Exact(id) => {
            if !valid_format_id(id) {
                return Err(VideoDownloadError::InvalidSelection);
            }
            let format = catalog
                .formats
                .iter()
                .find(|format| &format.format_id == id)
                .ok_or(VideoDownloadError::InvalidSelection)?;
            if !is_video_only_format(format) {
                return Err(VideoDownloadError::InvalidSelection);
            }
            let prefer_mp4 = is_mp4_friendly_video(format);
            let chosen_audio = best_audio_for_container(&catalog.formats, prefer_mp4)
                .ok_or(VideoDownloadError::NoMuxedMp4)?;
            let container = if prefer_mp4 && is_mp4_friendly_audio(chosen_audio) {
                "mp4"
            } else if format
                .container
                .as_deref()
                .is_some_and(|c| c.eq_ignore_ascii_case("webm"))
                && chosen_audio
                    .container
                    .as_deref()
                    .is_some_and(|c| c.eq_ignore_ascii_case("webm"))
            {
                "webm"
            } else {
                "mp4"
            };
            Ok(SplitFormatSelection {
                video_format_id: format.format_id.clone(),
                audio_format_id: chosen_audio.format_id.clone(),
                container,
            })
        }
    }
}

/// Prefers muxed MP4/WebM when available; falls back to split video+audio
/// selection (validated IDs, no leading '-') when no muxed format exists.
#[cfg(windows)]
pub(crate) fn select_download_plan(
    catalog: &VideoDiscovery,
    selection: &FormatSelection,
) -> Result<DownloadPlan, VideoDownloadError> {
    match select_muxed_format(catalog, selection) {
        Ok(muxed) => Ok(DownloadPlan::Muxed(muxed)),
        Err(VideoDownloadError::InvalidSelection) => Err(VideoDownloadError::InvalidSelection),
        Err(VideoDownloadError::NoMuxedMp4 | VideoDownloadError::SplitStreamsUnsupported) => {
            Ok(DownloadPlan::Split(select_split_format(catalog, selection)?))
        }
        Err(other) => Err(other),
    }
}

/// Downloads a split video-only + audio-only pair via supervised `yt-dlp`,
/// merges them locally via verified LGPL `ffmpeg.exe` + `ffprobe.exe`,
/// validates 1 video + 1 audio stream + full decode, and commits atomically.
#[cfg(windows)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn download_split_video(
    ytdlp_executable: &Path,
    ffmpeg_executable: &Path,
    ffprobe_executable: &Path,
    page_url: &str,
    destination: &Path,
    split: &SplitFormatSelection,
    control: &DownloadTaskControl,
    on_progress: std::sync::Arc<dyn Fn(VideoDownloadProgress) + Send + Sync>,
) -> Result<VideoDownloadResult, VideoDownloadError> {
    use crate::media::ffmpeg::{
        EngineError, FfmpegAdapter, MediaProcessor, OutputContainer, StagedInput,
    };

    let normalized_url =
        validate_public_page_url(page_url).map_err(|_| VideoDownloadError::InvalidUrl)?;
    if !valid_format_id(&split.video_format_id) || !valid_format_id(&split.audio_format_id) {
        return Err(VideoDownloadError::InvalidSelection);
    }
    let output_container = match split.container {
        "mp4" => OutputContainer::Mp4,
        "webm" => OutputContainer::Webm,
        _ => return Err(VideoDownloadError::NoMuxedMp4),
    };
    let destination_text = destination
        .to_str()
        .ok_or(VideoDownloadError::InvalidDestination)?;
    let (validated_destination, filename) =
        crate::download_manager::validate_resumable_download_destination(destination_text)
            .map_err(|_| VideoDownloadError::InvalidDestination)?;
    if validated_destination
        .extension()
        .and_then(|value| value.to_str())
        .is_none_or(|ext| !ext.eq_ignore_ascii_case(split.container))
    {
        return Err(VideoDownloadError::InvalidDestination);
    }
    let parent = validated_destination
        .parent()
        .ok_or(VideoDownloadError::InvalidDestination)?;
    let parent =
        std::fs::canonicalize(parent).map_err(|_| VideoDownloadError::InvalidDestination)?;
    let final_destination = parent.join(filename);
    if final_destination.exists() {
        return Err(VideoDownloadError::DestinationExists);
    }

    let job_stage =
        RuntimeTempDir::create(&parent).map_err(|_| VideoDownloadError::TemporaryCleanupFailed)?;

    let video_stage = match RuntimeTempDir::create(job_stage.path()) {
        Ok(dir) => dir,
        Err(_) => return cleanup_stage_with_error(job_stage, VideoDownloadError::TemporaryCleanupFailed),
    };
    let (video_file, video_bytes) = match run_ytdlp_stream_into_dir(
        ytdlp_executable,
        &normalized_url,
        &split.video_format_id,
        "video",
        video_stage.path(),
        MAX_VIDEO_DOWNLOAD_BYTES,
        control,
        std::sync::Arc::clone(&on_progress),
    ) {
        Ok(res) => res,
        Err(err) => {
            let _ = video_stage.cleanup();
            return cleanup_stage_with_error(job_stage, err);
        }
    };

    if control.is_cancelled() {
        let _ = video_stage.cleanup();
        return cleanup_stage_with_error(job_stage, VideoDownloadError::Cancelled);
    }
    let remaining_budget = MAX_VIDEO_DOWNLOAD_BYTES.saturating_sub(video_bytes);
    if remaining_budget == 0 {
        let _ = video_stage.cleanup();
        return cleanup_stage_with_error(job_stage, VideoDownloadError::OutputTooLarge);
    }

    let audio_stage = match RuntimeTempDir::create(job_stage.path()) {
        Ok(dir) => dir,
        Err(_) => {
            let _ = video_stage.cleanup();
            return cleanup_stage_with_error(job_stage, VideoDownloadError::TemporaryCleanupFailed);
        }
    };
    let (audio_file, _audio_bytes) = match run_ytdlp_stream_into_dir(
        ytdlp_executable,
        &normalized_url,
        &split.audio_format_id,
        "audio",
        audio_stage.path(),
        remaining_budget,
        control,
        std::sync::Arc::clone(&on_progress),
    ) {
        Ok(res) => res,
        Err(err) => {
            let _ = audio_stage.cleanup();
            let _ = video_stage.cleanup();
            return cleanup_stage_with_error(job_stage, err);
        }
    };

    let adapter = FfmpegAdapter {
        ffmpeg_executable: ffmpeg_executable.to_path_buf(),
        ffprobe_executable: ffprobe_executable.to_path_buf(),
    };
    let merged = match adapter.merge(
        &StagedInput {
            primary_path: video_file,
            secondary_path: Some(audio_file),
            working_directory: job_stage.path().to_path_buf(),
        },
        output_container,
        control,
    ) {
        Ok(out) => out,
        Err(err) => {
            let _ = audio_stage.cleanup();
            let _ = video_stage.cleanup();
            let mapped = match err {
                EngineError::Cancelled => VideoDownloadError::Cancelled,
                EngineError::TimedOut => VideoDownloadError::TimedOut,
                EngineError::IdleTimedOut => VideoDownloadError::IdleTimedOut,
                EngineError::OutputTooLarge => VideoDownloadError::OutputTooLarge,
                EngineError::ValidationFailed => VideoDownloadError::InvalidMedia,
                _ => VideoDownloadError::MergeFailed,
            };
            return cleanup_stage_with_error(job_stage, mapped);
        }
    };

    let _ = audio_stage.cleanup();
    let _ = video_stage.cleanup();

    let transfer_path = match create_transfer_link(&merged.path, &parent) {
        Ok(path) => path,
        Err(err) => return cleanup_stage_with_error(job_stage, err),
    };
    if merged.path.exists() && std::fs::remove_file(&merged.path).is_err() {
        let _ = std::fs::remove_file(&transfer_path);
        return cleanup_stage_with_error(job_stage, VideoDownloadError::Filesystem);
    }
    if job_stage.cleanup().is_err() {
        let _ = std::fs::remove_file(&transfer_path);
        return Err(VideoDownloadError::TemporaryCleanupFailed);
    }
    if control.is_cancelled() {
        let _ = std::fs::remove_file(&transfer_path);
        return Err(VideoDownloadError::Cancelled);
    }
    if final_destination.exists() {
        let _ = std::fs::remove_file(&transfer_path);
        return Err(VideoDownloadError::DestinationExists);
    }
    let committed = if std::fs::hard_link(&transfer_path, &final_destination).is_ok() {
        if std::fs::remove_file(&transfer_path).is_err() {
            return Err(VideoDownloadError::TemporaryCleanupFailed);
        }
        Ok(())
    } else if !final_destination.exists()
        && std::fs::rename(&transfer_path, &final_destination).is_ok()
    {
        Ok(())
    } else {
        let _ = std::fs::remove_file(&transfer_path);
        Err(if final_destination.exists() {
            VideoDownloadError::DestinationExists
        } else {
            VideoDownloadError::Filesystem
        })
    };
    committed?;
    on_progress(VideoDownloadProgress {
        downloaded_bytes: merged.bytes,
        total_bytes: Some(merged.bytes),
        speed_bytes_per_second: None,
        eta_seconds: Some(0.0),
        percent: Some(100.0),
    });
    Ok(VideoDownloadResult {
        destination: final_destination,
        bytes: merged.bytes,
    })
}

#[cfg(windows)]
#[allow(clippy::too_many_arguments)]
fn run_ytdlp_stream_into_dir(
    executable: &Path,
    normalized_url: &str,
    format_id: &str,
    stream_role: &str,
    stream_stage: &Path,
    maximum_bytes: u64,
    control: &DownloadTaskControl,
    on_progress: std::sync::Arc<dyn Fn(VideoDownloadProgress) + Send + Sync>,
) -> Result<(PathBuf, u64), VideoDownloadError> {
    use crate::process::{
        MonitoredOutput, OutputHandler, ProcessOutcome, ProcessSpec, ProcessStream,
        run_supervised_with_output,
    };
    use std::ffi::OsString;

    let mut runtime = RuntimeTempDir::create(stream_stage)
        .map_err(|_| VideoDownloadError::TemporaryCleanupFailed)?;

    #[cfg(test)]
    let url_fixture_mode = normalized_url
        .starts_with("https://example.test/fixture-")
        .then_some(normalized_url);
    #[cfg(not(test))]
    let url_fixture_mode: Option<&str> = None;

    let args = if url_fixture_mode.is_some() {
        vec![
            OsString::from("--exact"),
            OsString::from("process::tests::job_object_test_child_fixture"),
            OsString::from("--nocapture"),
        ]
    } else {
        yt_dlp_download_args(normalized_url, format_id, maximum_bytes)
    };

    let mut environment = Vec::new();
    for name in ["SystemRoot", "WINDIR"] {
        if let Some(value) = std::env::var_os(name) {
            environment.push((OsString::from(name), value));
        }
    }
    environment.extend([
        (OsString::from("TEMP"), runtime.path().as_os_str().to_owned()),
        (OsString::from("TMP"), runtime.path().as_os_str().to_owned()),
        (OsString::from("PYTHONNOUSERSITE"), OsString::from("1")),
        (OsString::from("PYTHONDONTWRITEBYTECODE"), OsString::from("1")),
    ]);
    #[cfg(test)]
    if let Some(fixture_url) = url_fixture_mode {
        environment.push((OsString::from("FLUXNET_JOB_OBJECT_TEST_CHILD"), OsString::from("1")));
        environment.push((
            OsString::from("FLUXNET_JOB_OBJECT_TEST_FIXTURE_URL"),
            OsString::from(fixture_url),
        ));
        environment.push((
            OsString::from("FLUXNET_JOB_OBJECT_TEST_STREAM_ROLE"),
            OsString::from(stream_role),
        ));
    }
    #[cfg(not(test))]
    let _ = stream_role;

    let spec = ProcessSpec {
        executable: executable.to_path_buf(),
        args,
        working_directory: stream_stage.to_path_buf(),
        environment,
        timeout: VIDEO_DOWNLOAD_TIMEOUT,
        idle_timeout: Some(IDLE_TIMEOUT),
        stdout_limit: 64 * 1024,
        stderr_limit: 64 * 1024,
    };
    let progress_buffers =
        std::sync::Arc::new(std::sync::Mutex::new(ProgressLineBuffers::default()));
    let callback_buffers = std::sync::Arc::clone(&progress_buffers);
    let progress_callback = std::sync::Arc::clone(&on_progress);
    let handler: OutputHandler = std::sync::Arc::new(move |stream: ProcessStream, bytes: &[u8]| {
        let lines = match callback_buffers.lock() {
            Ok(mut buffers) => buffers.feed(stream, bytes),
            Err(_) => Vec::new(),
        };
        for line in lines {
            if let Some(progress) = parse_download_progress_line(&line) {
                progress_callback(progress);
            }
        }
    });
    let monitored = MonitoredOutput {
        directory: stream_stage.to_path_buf(),
        filename_prefix: "fluxnet-media.".into(),
        maximum_bytes,
    };
    let outcome = match run_supervised_with_output(&spec, control, handler, Some(monitored)) {
        Ok(outcome) => outcome,
        Err(_) => {
            runtime.preserve_with_evidence("process_tree_shutdown_unconfirmed");
            return Err(VideoDownloadError::TemporaryCleanupFailed);
        }
    };
    if runtime.cleanup().is_err() {
        return Err(VideoDownloadError::TemporaryCleanupFailed);
    }
    match outcome {
        ProcessOutcome::Exited { code: 0, .. } => {}
        ProcessOutcome::Exited { .. } => return Err(VideoDownloadError::ProcessFailed),
        ProcessOutcome::Cancelled { .. } | ProcessOutcome::Paused { .. } => {
            return Err(VideoDownloadError::Cancelled);
        }
        ProcessOutcome::TimedOut { .. } => return Err(VideoDownloadError::TimedOut),
        ProcessOutcome::IdleTimedOut { .. } => return Err(VideoDownloadError::IdleTimedOut),
        ProcessOutcome::OutputLimitExceeded { .. } => {
            return Err(VideoDownloadError::OutputTooLarge);
        }
    }

    let entries = std::fs::read_dir(stream_stage).map_err(|_| VideoDownloadError::Filesystem)?;
    let files = entries
        .map(|entry| entry.map_err(|_| VideoDownloadError::Filesystem))
        .collect::<Result<Vec<_>, _>>()?;
    if files.len() != 1 {
        return Err(VideoDownloadError::InvalidMedia);
    }
    let file = &files[0];
    if !file
        .file_name()
        .to_string_lossy()
        .starts_with("fluxnet-media.")
    {
        return Err(VideoDownloadError::InvalidMedia);
    }
    let bytes = validate_regular_non_reparse_file(&file.path(), maximum_bytes)?;
    if bytes == 0 {
        return Err(VideoDownloadError::InvalidMedia);
    }
    Ok((file.path(), bytes))
}

#[cfg(windows)]
pub(crate) fn validate_selected_format(
    selected: &SelectedFormat,
) -> Result<(), VideoDownloadError> {
    if !matches!(selected.extension, "mp4" | "webm") {
        return Err(VideoDownloadError::NoMuxedMp4);
    }
    let expected_best = format!("best[ext={}]", selected.extension);
    if selected.selector == expected_best || valid_format_id(&selected.selector) {
        Ok(())
    } else {
        Err(VideoDownloadError::InvalidSelection)
    }
}

#[cfg(windows)]
pub(crate) fn selected_format_from_info(
    format_id: &str,
    container: &str,
) -> Result<SelectedFormat, VideoDownloadError> {
    let extension = if container.eq_ignore_ascii_case("mp4") {
        "mp4"
    } else if container.eq_ignore_ascii_case("webm") {
        "webm"
    } else {
        return Err(VideoDownloadError::NoMuxedMp4);
    };
    let selected = SelectedFormat {
        selector: format_id.to_owned(),
        extension,
    };
    validate_selected_format(&selected)?;
    Ok(selected)
}

#[cfg(windows)]
pub(crate) fn select_muxed_format(
    catalog: &VideoDiscovery,
    selection: &FormatSelection,
) -> Result<SelectedFormat, VideoDownloadError> {
    let compatible = |format: &DiscoveredFormat| {
        let container = format.container.as_deref().map(str::to_ascii_lowercase);
        matches!(container.as_deref(), Some("mp4" | "webm"))
            && format.video_codec.as_deref() != Some("none")
            && format.audio_codec.as_deref() != Some("none")
    };
    match selection {
        FormatSelection::BestAvailable => {
            for extension in ["mp4", "webm"] {
                if catalog.formats.iter().any(|format| {
                    compatible(format)
                        && format
                            .container
                            .as_deref()
                            .is_some_and(|container| container.eq_ignore_ascii_case(extension))
                }) {
                    return Ok(SelectedFormat {
                        selector: format!("best[ext={extension}]"),
                        extension,
                    });
                }
            }
            Err(VideoDownloadError::NoMuxedMp4)
        }
        FormatSelection::Exact(id) => {
            let format = catalog
                .formats
                .iter()
                .find(|format| &format.format_id == id)
                .ok_or(VideoDownloadError::InvalidSelection)?;
            if format.video_codec.as_deref() == Some("none")
                || format.audio_codec.as_deref() == Some("none")
            {
                return Err(VideoDownloadError::SplitStreamsUnsupported);
            }
            let extension = if format
                .container
                .as_deref()
                .is_some_and(|container| container.eq_ignore_ascii_case("mp4"))
            {
                "mp4"
            } else if format
                .container
                .as_deref()
                .is_some_and(|container| container.eq_ignore_ascii_case("webm"))
            {
                "webm"
            } else {
                return Err(VideoDownloadError::NoMuxedMp4);
            };
            if !valid_format_id(id) {
                return Err(VideoDownloadError::InvalidSelection);
            }
            Ok(SelectedFormat {
                selector: id.clone(),
                extension,
            })
        }
    }
}

#[cfg(windows)]
fn yt_dlp_download_args(
    url: &str,
    format_selector: &str,
    maximum_bytes: u64,
) -> Vec<std::ffi::OsString> {
    use std::ffi::OsString;
    [
        OsString::from("--ignore-config"),
        OsString::from("--no-update"),
        OsString::from("--no-cache-dir"),
        OsString::from("--no-cookies"),
        OsString::from("--no-cookies-from-browser"),
        OsString::from("--no-playlist"),
        OsString::from("--socket-timeout"),
        OsString::from(SOCKET_TIMEOUT_SECS),
        OsString::from("--retries"),
        OsString::from("0"),
        OsString::from("--fragment-retries"),
        OsString::from("0"),
        OsString::from("--continue"),
        OsString::from("--part"),
        OsString::from("--no-write-info-json"),
        OsString::from("--no-write-thumbnail"),
        OsString::from("--no-write-subs"),
        OsString::from("--no-write-auto-subs"),
        OsString::from("--no-write-description"),
        OsString::from("--no-write-playlist-metafiles"),
        OsString::from("--newline"),
        OsString::from("--progress"),
        OsString::from("--progress-template"),
        OsString::from(format!(
            "download:{PROGRESS_PREFIX}\t%(progress.status)s\t%(progress.downloaded_bytes)s\t%(progress.total_bytes)s\t%(progress.total_bytes_estimate)s\t%(progress.speed)s\t%(progress.eta)s"
        )),
        OsString::from("--max-filesize"),
        OsString::from(maximum_bytes.to_string()),
        OsString::from("--format"),
        OsString::from(format_selector),
        OsString::from("--output"),
        OsString::from("fluxnet-media.%(ext)s"),
        OsString::from("--"),
        OsString::from(url),
    ]
    .into_iter()
    .collect()
}

fn parse_download_progress_line(line: &str) -> Option<VideoDownloadProgress> {
    let fields = line
        .strip_prefix(PROGRESS_PREFIX)?
        .strip_prefix('\t')?
        .split('\t')
        .collect::<Vec<_>>();
    if fields.len() != 6 {
        return None;
    }
    let downloaded_bytes = fields[1].parse::<u64>().ok()?;
    let total_bytes = fields[2]
        .parse::<u64>()
        .ok()
        .or_else(|| fields[3].parse::<u64>().ok())
        .filter(|value| *value > 0);
    let speed_bytes_per_second = parse_finite_nonnegative(fields[4]);
    let eta_seconds = parse_finite_nonnegative(fields[5]);
    let percent = total_bytes
        .map(|total| ((downloaded_bytes as f64 / total as f64) * 100.0).clamp(0.0, 99.9));
    Some(VideoDownloadProgress {
        downloaded_bytes,
        total_bytes,
        speed_bytes_per_second,
        eta_seconds,
        percent,
    })
}

fn parse_finite_nonnegative(value: &str) -> Option<f64> {
    value
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite() && *value >= 0.0)
}

#[cfg(windows)]
#[derive(Default)]
struct ProgressLineBuffers {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

#[cfg(windows)]
impl ProgressLineBuffers {
    fn feed(&mut self, stream: crate::process::ProcessStream, bytes: &[u8]) -> Vec<String> {
        let buffer = match stream {
            crate::process::ProcessStream::Stdout => &mut self.stdout,
            crate::process::ProcessStream::Stderr => &mut self.stderr,
        };
        let mut lines = Vec::new();
        for byte in bytes {
            if *byte == b'\n' {
                let line = String::from_utf8_lossy(buffer)
                    .trim_end_matches('\r')
                    .to_owned();
                if line.starts_with(PROGRESS_PREFIX) {
                    lines.push(line);
                }
                buffer.clear();
            } else if buffer.len() < 4096 {
                buffer.push(*byte);
            } else {
                buffer.clear();
            }
        }
        lines
    }
}

#[cfg(windows)]
fn prepare_download_file(
    outcome: crate::process::ProcessOutcome,
    stage: &Path,
    maximum_bytes: u64,
    expected_extension: &str,
) -> Result<(PathBuf, u64), VideoDownloadError> {
    use crate::process::ProcessOutcome;
    match outcome {
        ProcessOutcome::Exited { code: 0, .. } => {}
        ProcessOutcome::Exited { .. } => return Err(VideoDownloadError::ProcessFailed),
        ProcessOutcome::Cancelled { .. } | ProcessOutcome::Paused { .. } => {
            return Err(VideoDownloadError::Cancelled);
        }
        ProcessOutcome::TimedOut { .. } => return Err(VideoDownloadError::TimedOut),
        ProcessOutcome::IdleTimedOut { .. } => return Err(VideoDownloadError::IdleTimedOut),
        ProcessOutcome::OutputLimitExceeded { .. } => {
            return Err(VideoDownloadError::OutputTooLarge);
        }
    }
    let entries = std::fs::read_dir(stage).map_err(|_| VideoDownloadError::Filesystem)?;
    let files = entries
        .map(|entry| entry.map_err(|_| VideoDownloadError::Filesystem))
        .collect::<Result<Vec<_>, _>>()?;
    if files.len() != 1 {
        return Err(VideoDownloadError::InvalidMedia);
    }
    let file = &files[0];
    if !file
        .file_name()
        .to_string_lossy()
        .starts_with("fluxnet-media.")
    {
        return Err(VideoDownloadError::InvalidMedia);
    }
    let metadata =
        std::fs::symlink_metadata(file.path()).map_err(|_| VideoDownloadError::Filesystem)?;
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if metadata.file_attributes() & 0x0400 != 0 {
            return Err(VideoDownloadError::InvalidMedia);
        }
    }
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > maximum_bytes {
        return Err(VideoDownloadError::OutputTooLarge);
    }
    if file
        .path()
        .extension()
        .and_then(|value| value.to_str())
        .is_none_or(|ext| !ext.eq_ignore_ascii_case(expected_extension))
    {
        return Err(VideoDownloadError::InvalidMedia);
    }
    crate::download_manager::validate_ytdlp_media(&file.path())
        .map_err(|_| VideoDownloadError::InvalidMedia)?;
    Ok((file.path(), metadata.len()))
}

#[cfg(windows)]
fn create_transfer_link(source: &Path, parent: &Path) -> Result<PathBuf, VideoDownloadError> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    for _ in 0..32 {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let name = format!(".fluxnet-video-transfer-{}-{id}.tmp", std::process::id());
        let path = parent.join(name);
        match std::fs::hard_link(source, &path) {
            Ok(()) => return Ok(path),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(_) => {
                if !path.exists() && std::fs::rename(source, &path).is_ok() {
                    return Ok(path);
                }
                return Err(VideoDownloadError::Filesystem);
            }
        }
    }
    Err(VideoDownloadError::Filesystem)
}

#[cfg(windows)]
fn validate_regular_non_reparse_file(
    path: &Path,
    maximum_bytes: u64,
) -> Result<u64, VideoDownloadError> {
    use std::os::windows::fs::MetadataExt;
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;

    let metadata = std::fs::symlink_metadata(path).map_err(|_| VideoDownloadError::Filesystem)?;
    if !metadata.is_file() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(VideoDownloadError::InvalidMedia);
    }
    if metadata.len() > maximum_bytes {
        return Err(VideoDownloadError::OutputTooLarge);
    }
    Ok(metadata.len())
}

#[cfg(windows)]
fn move_file_replacing(source: &Path, target: &Path) -> Result<(), VideoDownloadError> {
    use std::os::windows::fs::MetadataExt;
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;

    if let Ok(metadata) = std::fs::symlink_metadata(target) {
        if !metadata.is_file() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(VideoDownloadError::InvalidMedia);
        }
        std::fs::remove_file(target).map_err(|_| VideoDownloadError::Filesystem)?;
    }
    std::fs::rename(source, target).map_err(|_| VideoDownloadError::Filesystem)
}

#[cfg(windows)]
fn restore_partial_into_stage(
    partial_destination: &Path,
    staged_part_path: &Path,
    maximum_bytes: u64,
    restored_bytes: &mut u64,
) -> Result<(), VideoDownloadError> {
    match std::fs::symlink_metadata(partial_destination) {
        Ok(_) => {
            let bytes = validate_regular_non_reparse_file(partial_destination, maximum_bytes)?;
            if bytes == 0 {
                std::fs::remove_file(partial_destination)
                    .map_err(|_| VideoDownloadError::Filesystem)?;
                *restored_bytes = 0;
                return Ok(());
            }
            std::fs::rename(partial_destination, staged_part_path)
                .map_err(|_| VideoDownloadError::Filesystem)?;
            *restored_bytes = bytes;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            *restored_bytes = 0;
            Ok(())
        }
        Err(_) => Err(VideoDownloadError::Filesystem),
    }
}

#[cfg(windows)]
fn persist_paused_partial_file(
    stage: &Path,
    partial_destination: &Path,
    maximum_bytes: u64,
    expected_extension: &str,
) -> Result<u64, VideoDownloadError> {
    let expected_part_name = format!("fluxnet-media.{expected_extension}.part");
    let expected_full_name = format!("fluxnet-media.{expected_extension}");
    let mut candidate: Option<PathBuf> = None;

    let entries = std::fs::read_dir(stage).map_err(|_| VideoDownloadError::Filesystem)?;
    for entry in entries {
        let entry = entry.map_err(|_| VideoDownloadError::Filesystem)?;
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if file_name.eq_ignore_ascii_case(&expected_part_name)
            || file_name.eq_ignore_ascii_case(&expected_full_name)
        {
            if candidate.is_some() {
                return Err(VideoDownloadError::InvalidMedia);
            }
            candidate = Some(entry.path());
        } else {
            return Err(VideoDownloadError::InvalidMedia);
        }
    }

    let Some(staged_file) = candidate else {
        return Ok(0);
    };
    let bytes = validate_regular_non_reparse_file(&staged_file, maximum_bytes)?;
    if bytes == 0 {
        std::fs::remove_file(&staged_file).map_err(|_| VideoDownloadError::Filesystem)?;
        return Ok(0);
    }
    move_file_replacing(&staged_file, partial_destination)?;
    Ok(bytes)
}

#[cfg(windows)]
fn cleanup_stage_with_error(
    stage: RuntimeTempDir,
    error: VideoDownloadError,
) -> Result<VideoDownloadResult, VideoDownloadError> {
    if stage.cleanup().is_err() {
        Err(VideoDownloadError::TemporaryCleanupFailed)
    } else {
        Err(error)
    }
}

pub(crate) fn validate_public_page_url(raw: &str) -> Result<String, DiscoveryError> {
    let parsed = reqwest::Url::parse(raw).map_err(|_| DiscoveryError::InvalidUrl)?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
    {
        return Err(DiscoveryError::InvalidUrl);
    }
    let host = parsed
        .host_str()
        .unwrap_or_default()
        .trim_end_matches('.')
        .to_ascii_lowercase();
    if host == "localhost"
        || host.ends_with(".localhost")
        || host.ends_with(".local")
        || host.ends_with(".internal")
    {
        return Err(DiscoveryError::InvalidUrl);
    }
    if let Ok(address) = host.parse::<IpAddr>() {
        let private = match address {
            IpAddr::V4(ip) => {
                ip.is_private()
                    || ip.is_loopback()
                    || ip.is_link_local()
                    || ip.is_unspecified()
                    || ip.is_multicast()
                    || ip.is_broadcast()
            }
            IpAddr::V6(ip) => {
                ip.is_loopback()
                    || ip.is_unspecified()
                    || ip.is_unique_local()
                    || ip.is_unicast_link_local()
                    || ip.is_multicast()
            }
        };
        if private {
            return Err(DiscoveryError::InvalidUrl);
        }
    }
    Ok(parsed.to_string())
}

pub(crate) fn parse_discovery_json(bytes: &[u8]) -> Result<VideoDiscovery, DiscoveryError> {
    if bytes.len() > MAX_METADATA_BYTES {
        return Err(DiscoveryError::OutputTooLarge);
    }
    let root: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| DiscoveryError::InvalidJson)?;
    let object = root.as_object().ok_or(DiscoveryError::InvalidJson)?;
    if matches!(
        object.get("_type").and_then(|v| v.as_str()),
        Some("playlist" | "multi_video")
    ) {
        return Err(DiscoveryError::PlaylistNotSupported);
    }
    let raw_formats = object
        .get("formats")
        .and_then(|v| v.as_array())
        .ok_or(DiscoveryError::NoFormats)?;
    if raw_formats.is_empty() || raw_formats.len() > MAX_FORMATS {
        return Err(DiscoveryError::NoFormats);
    }
    let audio_exists = raw_formats.iter().any(|format| {
        codec(format.get("vcodec")) == Some("none")
            && codec(format.get("acodec")).is_some_and(|v| v != "none")
    });
    let mut formats = Vec::with_capacity(raw_formats.len());
    let mut ids = HashSet::with_capacity(raw_formats.len());
    for format in raw_formats {
        let Some(id) = format
            .get("format_id")
            .and_then(|v| v.as_str())
            .filter(|s| valid_format_id(s))
        else {
            continue;
        };
        if !ids.insert(id.to_owned()) {
            continue;
        }
        let width = format
            .get("width")
            .and_then(|v| v.as_u64())
            .and_then(|v| u32::try_from(v).ok());
        let height = format
            .get("height")
            .and_then(|v| v.as_u64())
            .and_then(|v| u32::try_from(v).ok());
        let video_codec = codec(format.get("vcodec"));
        let audio_codec = codec(format.get("acodec"));
        let has_video = video_codec.is_some_and(|v| v != "none");
        let has_audio = audio_codec.is_some_and(|v| v != "none");
        formats.push(DiscoveredFormat {
            format_id: id.chars().take(MAX_TEXT_CHARS).collect(),
            resolution: match (width, height) {
                (Some(w), Some(h)) => Some(format!("{w}x{h}")),
                _ => format
                    .get("resolution")
                    .and_then(|v| v.as_str())
                    .map(safe_text),
            },
            height,
            fps: format
                .get("fps")
                .and_then(|v| v.as_f64())
                .filter(|v| v.is_finite() && *v > 0.0),
            video_codec: video_codec.map(safe_text),
            audio_codec: audio_codec.map(safe_text),
            container: format.get("ext").and_then(|v| v.as_str()).map(safe_text),
            file_size: format
                .get("filesize")
                .or_else(|| format.get("filesize_approx"))
                .and_then(|v| v.as_u64()),
            has_video,
            has_audio,
            requires_separate_audio: has_video && !has_audio && audio_exists,
        });
    }
    if formats.is_empty() {
        return Err(DiscoveryError::NoFormats);
    }
    let title = object
        .get("title")
        .and_then(|v| v.as_str())
        .map(safe_title)
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "Untitled video".into());
    let extractor = object
        .get("extractor_key")
        .or_else(|| object.get("extractor"))
        .and_then(|v| v.as_str())
        .map(safe_text)
        .unwrap_or_else(|| "Unknown site".into());
    let duration_seconds = object
        .get("duration")
        .and_then(|v| v.as_f64())
        .filter(|v| v.is_finite() && *v >= 0.0);
    Ok(VideoDiscovery {
        title,
        duration_seconds,
        extractor,
        formats,
        best_available: true,
    })
}

fn codec(value: Option<&serde_json::Value>) -> Option<&str> {
    value
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty())
}
fn valid_format_id(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('-')
        && value.len() <= MAX_TEXT_CHARS
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._+-".contains(&b))
}
fn safe_text(value: &str) -> String {
    redact_metadata_text(value, MAX_TEXT_CHARS)
}
fn safe_title(value: &str) -> String {
    redact_metadata_text(value, MAX_TITLE_CHARS)
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
pub(crate) fn verify_ytdlp_executable(candidate: &Path) -> Result<PathBuf, String> {
    use std::os::windows::fs::MetadataExt;
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;

    let initial_meta = std::fs::symlink_metadata(candidate)
        .map_err(|_| VERIFIED_YTDLP_UNAVAILABLE_MSG.to_owned())?;
    if !initial_meta.is_file()
        || initial_meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
        || initial_meta.len() != VERIFIED_YTDLP_SIZE_BYTES
    {
        return Err(VERIFIED_YTDLP_UNAVAILABLE_MSG.into());
    }
    let canonical =
        std::fs::canonicalize(candidate).map_err(|_| VERIFIED_YTDLP_UNAVAILABLE_MSG.to_owned())?;
    let normalized = strip_verbatim_drive_prefix(&canonical);
    if !normalized.is_absolute() {
        return Err(VERIFIED_YTDLP_UNAVAILABLE_MSG.into());
    }
    let canonical_meta = std::fs::symlink_metadata(&normalized)
        .map_err(|_| VERIFIED_YTDLP_UNAVAILABLE_MSG.to_owned())?;
    if !canonical_meta.is_file()
        || canonical_meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
        || canonical_meta.len() != VERIFIED_YTDLP_SIZE_BYTES
    {
        return Err(VERIFIED_YTDLP_UNAVAILABLE_MSG.into());
    }
    let digest = sha256_file(&normalized, VERIFIED_YTDLP_SIZE_BYTES)
        .map_err(|_| VERIFIED_YTDLP_UNAVAILABLE_MSG.to_owned())?;
    if digest != VERIFIED_YTDLP_SHA256 {
        return Err(VERIFIED_YTDLP_UNAVAILABLE_MSG.into());
    }
    Ok(normalized)
}

#[cfg(windows)]
fn collect_trusted_release_candidates(app: Option<&tauri::AppHandle>) -> Vec<PathBuf> {
    use tauri::Manager;

    let mut candidates = Vec::new();
    if let Some(app) = app {
        if let Ok(resource_dir) = app.path().resource_dir() {
            candidates.push(resource_dir.join("yt-dlp.exe"));
            candidates.push(resource_dir.join("binaries").join("yt-dlp.exe"));
            candidates.push(resource_dir.join("tools").join("yt-dlp.exe"));
        }
        if let Ok(app_data_dir) = app.path().app_data_dir() {
            candidates.push(app_data_dir.join("tools").join("yt-dlp.exe"));
            candidates.push(app_data_dir.join("yt-dlp.exe"));
        }
    }
    if let Ok(current_exe) = std::env::current_exe() {
        if let Some(exe_dir) = current_exe.parent() {
            candidates.push(exe_dir.join("yt-dlp.exe"));
            candidates.push(exe_dir.join("tools").join("yt-dlp.exe"));
        }
    }
    candidates
}

#[cfg(windows)]
fn collect_executable_candidates_for_profile(
    app: Option<&tauri::AppHandle>,
    allow_dev_candidates: bool,
) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    #[cfg(any(test, debug_assertions))]
    if allow_dev_candidates {
        for env_name in ["FLUXNET_YTDLP_REAL_EXE", "FLUXNET_YTDLP_EXE"] {
            if let Some(value) = std::env::var_os(env_name).filter(|v| !v.is_empty()) {
                candidates.push(PathBuf::from(value));
            }
        }
    }
    candidates.extend(collect_trusted_release_candidates(app));
    #[cfg(any(test, debug_assertions))]
    if allow_dev_candidates {
        if let Some(repo_root) = Path::new(env!("CARGO_MANIFEST_DIR")).parent() {
            candidates.push(
                repo_root
                    .join("experiments")
                    .join("media-engine-poc")
                    .join("test-tools")
                    .join("yt-dlp-2026.10.26")
                    .join("yt-dlp.exe"),
            );
            candidates.push(
                repo_root
                    .join("experiments")
                    .join("media-engine-poc")
                    .join("yt-dlp-2026.10.26.exe"),
            );
        }
    }
    #[cfg(not(any(test, debug_assertions)))]
    let _ = allow_dev_candidates;
    candidates
}

#[cfg(windows)]
pub(crate) fn resolve_verified_executable(
    app: Option<&tauri::AppHandle>,
) -> Result<PathBuf, String> {
    let candidates =
        collect_executable_candidates_for_profile(app, cfg!(any(test, debug_assertions)));
    resolve_verified_executable_from_candidates(&candidates)
}

#[cfg(windows)]
pub(crate) fn resolve_verified_executable_from_candidates(
    candidates: &[PathBuf],
) -> Result<PathBuf, String> {
    for candidate in candidates {
        if let Ok(verified) = verify_ytdlp_executable(candidate) {
            return Ok(verified);
        }
    }
    Err(VERIFIED_YTDLP_UNAVAILABLE_MSG.into())
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

fn redact_metadata_text(value: &str, max_chars: usize) -> String {
    const SECRET_KEYS: [&str; 9] = [
        "token=",
        "signature=",
        "sig=",
        "auth=",
        "authorization=",
        "expires=",
        "policy=",
        "jwt=",
        "key=",
    ];
    value
        .chars()
        .filter(|c| !c.is_control())
        .collect::<String>()
        .split_whitespace()
        .map(|word| {
            let lower = word.to_ascii_lowercase();
            if lower.contains("http://") || lower.contains("https://") {
                return "[URL redacted]".to_owned();
            }
            if let Some((key, _)) = word.split_once('=') {
                let normalized = key
                    .trim_matches(|c: char| matches!(c, '?' | '&' | ';' | ':'))
                    .to_ascii_lowercase();
                if SECRET_KEYS
                    .iter()
                    .any(|secret| secret.trim_end_matches('=') == normalized)
                {
                    return format!("{key}=[redacted]");
                }
            }
            word.to_owned()
        })
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(max_chars)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(windows)]
    fn test_task_workspace(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "fluxnet-runtime-test-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        path
    }

    #[cfg(windows)]
    #[test]
    fn runtime_cleanup_is_scoped_to_the_exclusively_created_task_directory() {
        let task_workspace = test_task_workspace("scope");
        let unrelated = task_workspace.join("_MEI123456");
        std::fs::create_dir(&unrelated).unwrap();
        std::fs::write(unrelated.join("keep.txt"), b"owned elsewhere").unwrap();

        let runtime = RuntimeTempDir::create(&task_workspace).unwrap();
        assert!(
            runtime
                .path()
                .starts_with(std::fs::canonicalize(&task_workspace).unwrap())
        );
        let mei = runtime.path().join("_MEI654321");
        std::fs::create_dir(&mei).unwrap();
        std::fs::write(mei.join("payload.bin"), b"fixture extraction").unwrap();
        runtime.cleanup().unwrap();

        assert!(unrelated.join("keep.txt").is_file());
        assert_eq!(std::fs::read_dir(&task_workspace).unwrap().count(), 1);
        std::fs::remove_dir_all(task_workspace).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn runtime_cleanup_fails_closed_and_retains_safe_evidence_if_path_changes() {
        let task_workspace = test_task_workspace("evidence");
        let runtime = RuntimeTempDir::create(&task_workspace).unwrap();
        let path = runtime.path().to_path_buf();
        std::fs::remove_dir(&path).unwrap();
        std::fs::write(&path, b"replacement owned by test").unwrap();

        assert!(runtime.cleanup().is_err());
        let entries = std::fs::read_dir(&task_workspace)
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>();
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().any(|entry| {
            std::fs::canonicalize(entry.path()).unwrap_or_else(|_| entry.path()) == path
        }));
        let marker = entries
            .iter()
            .find(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".cleanup-warning.txt")
            })
            .expect("cleanup warning evidence should be retained");
        let content = std::fs::read_to_string(marker.path()).unwrap();
        assert!(content.contains("runtime_workspace=.fluxnet-yt-dlp-runtime-"));
        assert!(content.contains("cleanup_failed=InvalidInput"));
        std::fs::remove_dir_all(task_workspace).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn unconfirmed_process_tree_shutdown_preserves_runtime_and_warning_evidence() {
        let task_workspace = test_task_workspace("unconfirmed");
        let mut runtime = RuntimeTempDir::create(&task_workspace).unwrap();
        let mei = runtime.path().join("_MEI123456");
        std::fs::create_dir(&mei).unwrap();
        std::fs::write(mei.join("payload.bin"), b"fixture extraction").unwrap();
        runtime.preserve_with_evidence("process_tree_shutdown_unconfirmed");

        assert!(mei.join("payload.bin").is_file());
        let marker = std::fs::read_dir(&task_workspace)
            .unwrap()
            .map(Result::unwrap)
            .find(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".cleanup-warning.txt")
            })
            .expect("shutdown uncertainty evidence should be retained");
        let content = std::fs::read_to_string(marker.path()).unwrap();
        assert!(content.contains("process_tree_shutdown_unconfirmed"));
        std::fs::remove_dir_all(task_workspace).unwrap();
    }

    #[test]
    fn discovery_and_format_selection_types_round_trip_through_tauri_ipc_json() {
        let catalog = parse_discovery_json(SAMPLE).unwrap();
        let muxed = retain_muxed_formats(catalog).unwrap();
        assert_eq!(muxed.formats.len(), 1);
        assert_eq!(muxed.formats[0].format_id, "muxed");
        assert!(muxed.best_available);

        let serialized_catalog = serde_json::to_string(&muxed).unwrap();
        assert!(serialized_catalog.contains("\"formatId\":\"muxed\""));
        assert!(serialized_catalog.contains("\"bestAvailable\":true"));
        assert!(!serialized_catalog.contains("yt-dlp"));
        let deserialized_catalog: VideoDiscovery =
            serde_json::from_str(&serialized_catalog).unwrap();
        assert_eq!(deserialized_catalog, muxed);

        let best_json = serde_json::to_string(&FormatSelection::BestAvailable).unwrap();
        assert_eq!(best_json, "\"bestAvailable\"");
        assert_eq!(
            serde_json::from_str::<FormatSelection>(&best_json).unwrap(),
            FormatSelection::BestAvailable
        );
        assert_eq!(
            serde_json::from_str::<FormatSelection>("\"BestAvailable\"").unwrap(),
            FormatSelection::BestAvailable
        );

        let exact = FormatSelection::Exact("muxed".into());
        let exact_json = serde_json::to_string(&exact).unwrap();
        assert_eq!(exact_json, "{\"exact\":\"muxed\"}");
        assert_eq!(
            serde_json::from_str::<FormatSelection>(&exact_json).unwrap(),
            exact
        );
        assert_eq!(
            serde_json::from_str::<FormatSelection>("{\"Exact\":\"muxed\"}").unwrap(),
            exact
        );
    }

    #[cfg(windows)]
    #[test]
    fn discover_muxed_in_workspace_filters_split_streams_and_cleans_private_task_dir() {
        let exe = std::env::current_exe().unwrap();
        let base_dir = unique_workspace("muxed-command");
        let prefix = [
            "--exact",
            "process::tests::job_object_test_child_fixture",
            "--nocapture",
        ]
        .into_iter()
        .map(std::ffi::OsString::from)
        .collect::<Vec<_>>();

        let split_only_err = discover_muxed_in_workspace_with_test_hooks(
            &exe,
            &base_dir,
            "https://example.test/video",
            &DownloadTaskControl::default(),
            Duration::from_secs(5),
            MAX_METADATA_BYTES,
            prefix.clone(),
            Some("yt-dlp-json"),
        )
        .unwrap_err();
        assert_eq!(split_only_err, VideoDownloadError::NoMuxedMp4.to_string());
        assert_eq!(std::fs::read_dir(&base_dir).unwrap().count(), 0);

        let discovered = discover_muxed_in_workspace_with_test_hooks(
            &exe,
            &base_dir,
            "https://example.test/video",
            &DownloadTaskControl::default(),
            Duration::from_secs(5),
            MAX_METADATA_BYTES,
            prefix,
            Some("yt-dlp-json-muxed"),
        )
        .expect("muxed workspace discovery should succeed");

        assert_eq!(discovered.title, "Fixture video");
        assert_eq!(discovered.formats.len(), 1);
        assert_eq!(discovered.formats[0].format_id, "muxed720");
        assert!(discovered.formats.iter().all(is_muxed_mp4_or_webm));
        assert!(!discovered.formats.iter().any(|f| f.requires_separate_audio));
        assert_eq!(
            std::fs::read_dir(&base_dir).unwrap().count(),
            0,
            "private per-discovery task workspace must be cleaned up"
        );
        std::fs::remove_dir(base_dir).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn verified_executable_resolver_rejects_unverified_binaries_and_never_leaks_paths() {
        let dir = unique_workspace("verify-exe");
        let fake_exe = dir.join("yt-dlp.exe");
        std::fs::write(&fake_exe, b"unverified binary bytes").unwrap();

        let err = verify_ytdlp_executable(&fake_exe).unwrap_err();
        assert_eq!(err, VERIFIED_YTDLP_UNAVAILABLE_MSG);
        assert!(!err.contains("yt-dlp.exe"));
        assert!(
            !err.contains(&dir.to_string_lossy().as_ref()),
            "error must not expose filesystem paths"
        );

        let missing_err =
            resolve_verified_executable_from_candidates(&[fake_exe, dir.join("missing.exe")])
                .unwrap_err();
        assert_eq!(missing_err, VERIFIED_YTDLP_UNAVAILABLE_MSG);
        std::fs::remove_dir_all(dir).unwrap();

        if let Ok(resolved) = resolve_verified_executable(None) {
            assert!(resolved.is_absolute());
            assert!(resolved.is_file());
            assert_eq!(
                std::fs::metadata(&resolved).unwrap().len(),
                VERIFIED_YTDLP_SIZE_BYTES
            );
        }

        // Gap 3: Release-profile candidate list excludes experiments/PoC and env overrides,
        // whereas test/debug candidate list retains them.
        let release_candidates = collect_executable_candidates_for_profile(None, false);
        assert!(
            !release_candidates.is_empty(),
            "release profile should still check exe-adjacent trusted paths"
        );
        assert!(
            release_candidates.iter().all(|path| {
                let text = path.to_string_lossy().to_ascii_lowercase();
                !text.contains("experiments") && !text.contains("media-engine-poc")
            }),
            "release candidates must not contain dev/PoC experiments paths: {release_candidates:?}"
        );

        let dev_candidates = collect_executable_candidates_for_profile(None, true);
        assert!(
            dev_candidates.iter().any(|path| {
                let text = path.to_string_lossy();
                text.contains("experiments") && text.contains("media-engine-poc")
            }),
            "test/debug candidates must retain experiments/media-engine-poc paths"
        );
    }

    const SAMPLE: &[u8] = br#"{"title":"Demo","duration":12.5,"extractor":"Example","extractor_key":"ExampleSite","webpage_url":"https://example.test/v?token=SECRET","formats":[{"format_id":"v1080","width":1920,"height":1080,"fps":60,"vcodec":"avc1.640028","acodec":"none","ext":"mp4","filesize":1234,"url":"https://cdn.test/a?signature=SECRET"},{"format_id":"a","vcodec":"none","acodec":"mp4a.40.2","ext":"m4a"},{"format_id":"muxed","height":720,"vcodec":"vp9","acodec":"opus","ext":"webm"}]}"#;

    #[test]
    fn parses_dynamic_formats_and_does_not_expose_signed_urls() {
        let result = parse_discovery_json(SAMPLE).unwrap();
        assert_eq!(result.title, "Demo");
        assert_eq!(result.extractor, "ExampleSite");
        assert!(result.best_available);
        assert!(
            result
                .validate_selection(&FormatSelection::BestAvailable)
                .is_ok()
        );
        assert!(
            result
                .validate_selection(&FormatSelection::Exact("v1080".into()))
                .is_ok()
        );
        assert!(
            result
                .validate_selection(&FormatSelection::Exact("missing".into()))
                .is_err()
        );
        assert_eq!(result.formats.len(), 3);
        assert_eq!(result.formats[0].height, Some(1080));
        assert_eq!(result.formats[0].fps, Some(60.0));
        assert_eq!(result.formats[0].file_size, Some(1234));
        assert!(result.formats[0].requires_separate_audio);
        assert!(!result.formats[2].requires_separate_audio);
        let public_json = serde_json::to_string(&result).unwrap();
        assert!(!public_json.contains("SECRET"));
        assert!(!public_json.contains("signature"));
        assert!(!public_json.contains("webpage_url"));
    }

    #[test]
    fn metadata_text_redacts_embedded_urls_and_signature_parameters() {
        let input = br#"{"title":"Clip https://cdn.test/v?signature=SECRET token=ABC","formats":[{"format_id":"1","vcodec":"avc1","acodec":"aac"}]}"#;
        let parsed = parse_discovery_json(input).unwrap();
        assert!(!parsed.title.contains("SECRET"));
        assert!(!parsed.title.contains("ABC"));
        assert!(parsed.title.contains("[URL redacted]"));
    }

    #[test]
    fn parses_machine_readable_progress_and_normalizes_unknown_estimates() {
        let parsed =
            parse_download_progress_line("FLUXNET_PROGRESS\tdownloading\t25\t100\t120\t12.5\t6")
                .unwrap();
        assert_eq!(parsed.downloaded_bytes, 25);
        assert_eq!(parsed.total_bytes, Some(100));
        assert_eq!(parsed.speed_bytes_per_second, Some(12.5));
        assert_eq!(parsed.eta_seconds, Some(6.0));
        assert_eq!(parsed.percent, Some(25.0));

        let unknown =
            parse_download_progress_line("FLUXNET_PROGRESS\tdownloading\t7\tNA\tNA\tNA\tNA")
                .unwrap();
        assert_eq!(unknown.downloaded_bytes, 7);
        assert_eq!(unknown.total_bytes, None);
        assert_eq!(unknown.percent, None);
        assert!(parse_download_progress_line("ordinary yt-dlp output").is_none());
        assert!(parse_download_progress_line("FLUXNET_PROGRESS\tbad").is_none());
    }

    #[cfg(windows)]
    #[test]
    fn media_download_selection_never_substitutes_or_uses_split_streams() {
        let catalog = parse_discovery_json(SAMPLE).unwrap();
        let mut without_muxed = catalog.clone();
        without_muxed
            .formats
            .retain(|format| format.format_id != "muxed");
        assert_eq!(
            select_muxed_format(&without_muxed, &FormatSelection::BestAvailable),
            Err(VideoDownloadError::NoMuxedMp4)
        );
        assert_eq!(
            select_muxed_format(&catalog, &FormatSelection::Exact("v1080".into())),
            Err(VideoDownloadError::SplitStreamsUnsupported)
        );
        assert_eq!(
            select_muxed_format(&catalog, &FormatSelection::Exact("missing".into())),
            Err(VideoDownloadError::InvalidSelection)
        );
        let mut with_muxed_mp4 = catalog.clone();
        with_muxed_mp4.formats.push(DiscoveredFormat {
            format_id: "muxed-mp4".into(),
            resolution: Some("640x360".into()),
            height: Some(360),
            fps: Some(30.0),
            video_codec: Some("avc1".into()),
            audio_codec: Some("mp4a".into()),
            container: Some("mp4".into()),
            file_size: None,
            has_video: true,
            has_audio: true,
            requires_separate_audio: false,
        });
        assert_eq!(
            select_muxed_format(&with_muxed_mp4, &FormatSelection::BestAvailable)
                .unwrap()
                .selector,
            "best[ext=mp4]"
        );
        assert_eq!(
            select_muxed_format(&catalog, &FormatSelection::BestAvailable)
                .unwrap()
                .selector,
            "best[ext=webm]"
        );
        assert_eq!(
            select_muxed_format(&with_muxed_mp4, &FormatSelection::Exact("muxed-mp4".into()))
                .unwrap(),
            SelectedFormat {
                selector: "muxed-mp4".into(),
                extension: "mp4"
            }
        );

        // Gap 4: Reject any format_id starting with '-' (option-like strings) across all entry points.
        for bad_id in ["-best", "--help", "--cookies", "-f", "-"] {
            assert!(
                !valid_format_id(bad_id),
                "format_id starting with '-' must be rejected: {bad_id}"
            );
            assert_eq!(
                selected_format_from_info(bad_id, "mp4"),
                Err(VideoDownloadError::InvalidSelection)
            );
            let mut injected = with_muxed_mp4.clone();
            injected.formats.push(DiscoveredFormat {
                format_id: bad_id.into(),
                resolution: Some("1280x720".into()),
                height: Some(720),
                fps: Some(30.0),
                video_codec: Some("avc1".into()),
                audio_codec: Some("mp4a".into()),
                container: Some("mp4".into()),
                file_size: None,
                has_video: true,
                has_audio: true,
                requires_separate_audio: false,
            });
            assert_eq!(
                select_muxed_format(&injected, &FormatSelection::Exact(bad_id.into())),
                Err(VideoDownloadError::InvalidSelection)
            );
        }
        let dash_json = br#"{"title":"OptionId","formats":[{"format_id":"-best","vcodec":"avc1","acodec":"mp4a","ext":"mp4"},{"format_id":"--help","vcodec":"avc1","acodec":"mp4a","ext":"mp4"}]}"#;
        assert_eq!(
            parse_discovery_json(dash_json).unwrap_err(),
            DiscoveryError::NoFormats
        );
    }

    #[cfg(windows)]
    #[test]
    fn split_format_selection_falls_back_when_no_muxed_and_rejects_option_ids() {
        let split_only_json = br#"{"title":"DASH Reel","duration":18.0,"extractor_key":"Instagram","formats":[
            {"format_id":"-evil","width":3840,"height":2160,"vcodec":"avc1","acodec":"none","ext":"mp4","filesize":99999},
            {"format_id":"--help","vcodec":"none","acodec":"mp4a.40.2","ext":"m4a","filesize":99999},
            {"format_id":"136","width":1280,"height":720,"fps":30,"vcodec":"avc1.64001f","acodec":"none","ext":"mp4","filesize":2000},
            {"format_id":"137","width":1920,"height":1080,"fps":60,"vcodec":"avc1.640028","acodec":"none","ext":"mp4","filesize":5000},
            {"format_id":"140","vcodec":"none","acodec":"mp4a.40.2","ext":"m4a","filesize":1200}
        ]}"#;
        let catalog = parse_discovery_json(split_only_json).unwrap();
        assert_eq!(
            select_muxed_format(&catalog, &FormatSelection::BestAvailable),
            Err(VideoDownloadError::NoMuxedMp4)
        );
        assert_eq!(
            select_download_plan(&catalog, &FormatSelection::BestAvailable).unwrap(),
            DownloadPlan::Split(SplitFormatSelection {
                video_format_id: "137".into(),
                audio_format_id: "140".into(),
                container: "mp4",
            })
        );
        assert_eq!(
            select_download_plan(&catalog, &FormatSelection::Exact("136".into())).unwrap(),
            DownloadPlan::Split(SplitFormatSelection {
                video_format_id: "136".into(),
                audio_format_id: "140".into(),
                container: "mp4",
            })
        );
        assert_eq!(
            select_download_plan(&catalog, &FormatSelection::Exact("-evil".into())),
            Err(VideoDownloadError::InvalidSelection)
        );
        assert_eq!(
            select_download_plan(&catalog, &FormatSelection::Exact("--help".into())),
            Err(VideoDownloadError::InvalidSelection)
        );

        // When a muxed format exists, muxed remains preferred over split fallback.
        let mut with_muxed = catalog;
        with_muxed.formats.push(DiscoveredFormat {
            format_id: "18".into(),
            resolution: Some("640x360".into()),
            height: Some(360),
            fps: Some(30.0),
            video_codec: Some("avc1".into()),
            audio_codec: Some("mp4a".into()),
            container: Some("mp4".into()),
            file_size: Some(1500),
            has_video: true,
            has_audio: true,
            requires_separate_audio: false,
        });
        assert_eq!(
            select_download_plan(&with_muxed, &FormatSelection::BestAvailable).unwrap(),
            DownloadPlan::Muxed(SelectedFormat {
                selector: "best[ext=mp4]".into(),
                extension: "mp4",
            })
        );
    }

    #[test]
    fn rejects_malformed_missing_formats_and_playlists() {
        assert_eq!(
            parse_discovery_json(b"{").unwrap_err(),
            DiscoveryError::InvalidJson
        );
        assert_eq!(
            parse_discovery_json(br#"{"title":"x"}"#).unwrap_err(),
            DiscoveryError::NoFormats
        );
        assert_eq!(
            parse_discovery_json(br#"{"_type":"playlist","formats":[{}]}"#).unwrap_err(),
            DiscoveryError::PlaylistNotSupported
        );
        assert_eq!(
            parse_discovery_json(&vec![b'x'; MAX_METADATA_BYTES + 1]).unwrap_err(),
            DiscoveryError::OutputTooLarge
        );
    }

    #[test]
    fn rejects_non_public_url_forms_without_echoing_input() {
        for url in [
            "file:///video.mp4",
            "https://user:pass@example.test/v",
            "http://127.0.0.1/video",
            "http://localhost/v",
            "http://192.168.1.2/v",
        ] {
            assert_eq!(
                validate_public_page_url(url).unwrap_err(),
                DiscoveryError::InvalidUrl
            );
        }
        assert!(validate_public_page_url("https://example.test/watch?v=123&sig=secret").is_ok());
    }

    #[test]
    fn yt_dlp_invocation_is_explicitly_metadata_only_and_uncredentialed() {
        let args = yt_dlp_metadata_args("https://example.test/watch?v=1");
        let args = args
            .iter()
            .map(|arg| arg.to_string_lossy())
            .collect::<Vec<_>>();
        for required in [
            "--ignore-config",
            "--no-update",
            "--no-cache-dir",
            "--no-cookies",
            "--no-cookies-from-browser",
            "--no-playlist",
            "--socket-timeout",
            "30",
            "--retries",
            "--fragment-retries",
            "--skip-download",
            "--dump-single-json",
            "--no-write-info-json",
            "--no-write-thumbnail",
            "--no-write-subs",
            "--no-write-auto-subs",
            "--no-write-description",
            "--no-write-playlist-metafiles",
            "--",
        ] {
            assert!(
                args.iter().any(|arg| arg == required),
                "missing safety option {required}"
            );
        }
        assert!(
            args.windows(2)
                .any(|pair| pair[0] == "--retries" && pair[1] == "0"),
            "metadata args must pass --retries 0"
        );
        assert!(
            args.windows(2)
                .any(|pair| pair[0] == "--fragment-retries" && pair[1] == "0"),
            "metadata args must pass --fragment-retries 0"
        );
        assert_eq!(
            args.last().map(|s| s.as_ref()),
            Some("https://example.test/watch?v=1")
        );
        assert_eq!(args.iter().filter(|arg| arg.as_ref() == "--").count(), 1);
        assert!(!args.iter().any(|arg| arg.starts_with("--cookies")));
    }

    #[cfg(windows)]
    #[test]
    fn yt_dlp_download_args_enable_continue_and_part_without_sidecars() {
        let args = yt_dlp_download_args("https://example.test/watch?v=1", "best[ext=mp4]", 1024);
        let args = args
            .iter()
            .map(|arg| arg.to_string_lossy())
            .collect::<Vec<_>>();
        for required in [
            "--ignore-config",
            "--no-update",
            "--no-cache-dir",
            "--no-cookies",
            "--no-cookies-from-browser",
            "--no-playlist",
            "--socket-timeout",
            "30",
            "--retries",
            "--fragment-retries",
            "--continue",
            "--part",
            "--no-write-info-json",
            "--no-write-thumbnail",
            "--no-write-subs",
            "--no-write-auto-subs",
            "--no-write-description",
            "--no-write-playlist-metafiles",
            "--",
        ] {
            assert!(
                args.iter().any(|arg| arg == required),
                "missing required download option {required}"
            );
        }
        assert!(
            args.windows(2)
                .any(|pair| pair[0] == "--retries" && pair[1] == "0"),
            "download args must pass --retries 0"
        );
        assert!(
            args.windows(2)
                .any(|pair| pair[0] == "--fragment-retries" && pair[1] == "0"),
            "download args must pass --fragment-retries 0"
        );
        assert_eq!(
            args.last().map(|s| s.as_ref()),
            Some("https://example.test/watch?v=1")
        );
    }

    #[cfg(windows)]
    fn muxed_mp4_catalog() -> VideoDiscovery {
        let mut catalog = parse_discovery_json(SAMPLE).unwrap();
        catalog.formats.push(DiscoveredFormat {
            format_id: "muxed-mp4".into(),
            resolution: Some("1280x720".into()),
            height: Some(720),
            fps: Some(30.0),
            video_codec: Some("avc1".into()),
            audio_codec: Some("mp4a".into()),
            container: Some("mp4".into()),
            file_size: Some(1024),
            has_video: true,
            has_audio: true,
            requires_separate_audio: false,
        });
        catalog
    }

    #[cfg(windows)]
    #[test]
    fn pause_persists_partial_file_cleans_mei_runtime_and_resumes_to_valid_mp4() {
        use std::sync::{Arc, Mutex};

        let exe = std::env::current_exe().unwrap();
        let workspace = unique_workspace("pause-resume");
        let destination = workspace.join("video.mp4");
        let partial = crate::download_manager::partial_path(&destination);
        let catalog = muxed_mp4_catalog();
        let prefix = vec![
            std::ffi::OsString::from("--exact"),
            std::ffi::OsString::from("process::tests::job_object_test_child_fixture"),
            std::ffi::OsString::from("--nocapture"),
        ];

        // 1. Run download until first progress callback (after child writes 32-byte .part and _MEI dir), then request pause.
        let control = Arc::new(DownloadTaskControl::default());
        let control_for_progress = Arc::clone(&control);
        let seen_progress = Arc::new(Mutex::new(Vec::new()));
        let seen_for_cb = Arc::clone(&seen_progress);
        let pause_prefix = prefix.clone();
        let pause_result = download_video_with_test_hooks(
            &exe,
            "https://example.test/video",
            &destination,
            &catalog,
            &FormatSelection::Exact("muxed-mp4".into()),
            &control,
            1024 * 1024,
            Duration::from_secs(10),
            pause_prefix,
            Some("yt-dlp-download-pause"),
            Arc::new(move |progress| {
                seen_for_cb.lock().unwrap().push(progress);
                control_for_progress.request_pause();
            }),
        );

        assert_eq!(
            pause_result,
            Err(VideoDownloadError::Paused {
                downloaded_bytes: 32
            })
        );
        assert!(!destination.exists());
        assert!(partial.is_file());
        assert_eq!(
            std::fs::read(&partial).unwrap(),
            b"PARTIAL_MP4_PREFIX_BYTES_32_____"
        );

        // Verify no stage or _MEI runtime directory remains in workspace—only the partial file.
        let remaining_entries = std::fs::read_dir(&workspace)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(remaining_entries, vec![partial.clone()]);

        // 2. Resume download: retained partial file is moved into fresh stage as fluxnet-media.mp4.part,
        // completed by child, strictly validated as a muxed MP4, and committed to destination.
        let resume_control = DownloadTaskControl::default();
        let resume_result = download_video_with_test_hooks(
            &exe,
            "https://example.test/video",
            &destination,
            &catalog,
            &FormatSelection::Exact("muxed-mp4".into()),
            &resume_control,
            1024 * 1024,
            Duration::from_secs(10),
            prefix,
            Some("yt-dlp-download-resume"),
            Arc::new(|_| {}),
        )
        .expect("resumed download should complete and pass strict MP4 validation");

        assert!(destination.is_file());
        assert!(!partial.exists());
        assert_eq!(
            resume_result.bytes,
            std::fs::metadata(&destination).unwrap().len()
        );
        crate::download_manager::validate_ytdlp_media(&destination).unwrap();

        let final_entries = std::fs::read_dir(&workspace)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(final_entries, vec!["video.mp4".to_owned()]);

        std::fs::remove_dir_all(workspace).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn rejects_invalid_or_oversized_partial_files_and_corrupted_range_resumes() {
        use std::sync::Arc;

        let exe = std::env::current_exe().unwrap();
        let workspace = unique_workspace("invalid-partial");
        let destination = workspace.join("video.mp4");
        let partial = crate::download_manager::partial_path(&destination);
        let catalog = muxed_mp4_catalog();
        let prefix = vec![
            std::ffi::OsString::from("--exact"),
            std::ffi::OsString::from("process::tests::job_object_test_child_fixture"),
            std::ffi::OsString::from("--nocapture"),
        ];

        // 1. Oversized partial file on resume is rejected before spawning yt-dlp.
        std::fs::write(&partial, [b'A'; 65]).unwrap();
        let err = download_video_with_test_hooks(
            &exe,
            "https://example.test/video",
            &destination,
            &catalog,
            &FormatSelection::Exact("muxed-mp4".into()),
            &DownloadTaskControl::default(),
            64,
            Duration::from_secs(5),
            prefix.clone(),
            Some("yt-dlp-download-resume"),
            Arc::new(|_| {}),
        )
        .unwrap_err();
        assert_eq!(err, VideoDownloadError::OutputTooLarge);
        std::fs::remove_file(&partial).unwrap();

        // 2. Directory at partial_path is rejected as InvalidMedia.
        std::fs::create_dir(&partial).unwrap();
        let err = download_video_with_test_hooks(
            &exe,
            "https://example.test/video",
            &destination,
            &catalog,
            &FormatSelection::Exact("muxed-mp4".into()),
            &DownloadTaskControl::default(),
            1024 * 1024,
            Duration::from_secs(5),
            prefix.clone(),
            Some("yt-dlp-download-resume"),
            Arc::new(|_| {}),
        )
        .unwrap_err();
        assert_eq!(err, VideoDownloadError::InvalidMedia);
        std::fs::remove_dir(&partial).unwrap();

        // 3. Corrupted range resume (incompatible bytes appended) fails strict MP4 validation
        // and never commits a corrupted file to destination.
        std::fs::write(&partial, b"PARTIAL_MP4_PREFIX_BYTES_32_____").unwrap();
        let err = download_video_with_test_hooks(
            &exe,
            "https://example.test/video",
            &destination,
            &catalog,
            &FormatSelection::Exact("muxed-mp4".into()),
            &DownloadTaskControl::default(),
            1024 * 1024,
            Duration::from_secs(10),
            prefix,
            Some("yt-dlp-download-corrupt-resume"),
            Arc::new(|_| {}),
        )
        .unwrap_err();
        assert_eq!(err, VideoDownloadError::InvalidMedia);
        assert!(!destination.exists());
        assert_eq!(std::fs::read_dir(&workspace).unwrap().count(), 0);

        std::fs::remove_dir_all(workspace).unwrap();
    }

    #[cfg(windows)]
    fn fixture(
        mode: &str,
        limit: usize,
        timeout: Duration,
    ) -> Result<VideoDiscovery, DiscoveryError> {
        let exe = std::env::current_exe().unwrap();
        let cwd = std::env::temp_dir().join(format!(
            "fluxnet-ytdlp-poc-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&cwd).unwrap();
        let prefix = [
            "--exact",
            "process::tests::job_object_test_child_fixture",
            "--nocapture",
        ]
        .into_iter()
        .map(std::ffi::OsString::from)
        .collect();
        let control = DownloadTaskControl::default();
        let result = discover_with_limits(
            &exe,
            &cwd,
            "https://example.test/video",
            &control,
            timeout,
            limit,
            prefix,
            Some(mode),
        );
        let remaining = std::fs::read_dir(&cwd).unwrap().count();
        std::fs::remove_dir(&cwd).unwrap();
        assert_eq!(
            remaining, 0,
            "discovery must not create media or sidecar files"
        );
        result
    }

    #[cfg(windows)]
    #[test]
    fn supervisor_backed_discovery_handles_json_limits_and_timeout_without_files() {
        let discovered =
            fixture("yt-dlp-json", MAX_METADATA_BYTES, Duration::from_secs(5)).unwrap();
        assert_eq!(discovered.title, "Fixture video");
        assert!(
            discovered
                .formats
                .iter()
                .any(|format| format.requires_separate_audio)
        );
        assert_eq!(
            fixture(
                "yt-dlp-malformed",
                MAX_METADATA_BYTES,
                Duration::from_secs(5)
            )
            .unwrap_err(),
            DiscoveryError::InvalidJson
        );
        assert_eq!(
            fixture(
                "yt-dlp-no-formats",
                MAX_METADATA_BYTES,
                Duration::from_secs(5)
            )
            .unwrap_err(),
            DiscoveryError::NoFormats
        );
        assert_eq!(
            fixture("yt-dlp-large", 1024, Duration::from_secs(5)).unwrap_err(),
            DiscoveryError::OutputTooLarge
        );
        assert_eq!(
            fixture(
                "yt-dlp-wait",
                MAX_METADATA_BYTES,
                Duration::from_millis(100)
            )
            .unwrap_err(),
            DiscoveryError::TimedOut
        );
    }

    #[cfg(windows)]
    #[test]
    fn supervisor_backed_discovery_observes_manager_cancellation() {
        let exe = std::env::current_exe().unwrap();
        let cwd = std::env::temp_dir().join(format!("fluxnet-ytdlp-cancel-{}", std::process::id()));
        std::fs::create_dir_all(&cwd).unwrap();
        let prefix = [
            "--exact",
            "process::tests::job_object_test_child_fixture",
            "--nocapture",
        ]
        .into_iter()
        .map(std::ffi::OsString::from)
        .collect();
        let control = std::sync::Arc::new(DownloadTaskControl::default());
        let worker_control = control.clone();
        let worker_cwd = cwd.clone();
        let worker = std::thread::spawn(move || {
            discover_with_limits(
                &exe,
                &worker_cwd,
                "https://example.test/video",
                &worker_control,
                Duration::from_secs(10),
                MAX_METADATA_BYTES,
                prefix,
                Some("yt-dlp-wait"),
            )
        });
        std::thread::sleep(Duration::from_millis(150));
        control.request_cancel();
        assert_eq!(
            worker.join().unwrap().unwrap_err(),
            DiscoveryError::Cancelled
        );
        assert_eq!(std::fs::read_dir(&cwd).unwrap().count(), 0);
        std::fs::remove_dir(&cwd).unwrap();
    }

    #[cfg(windows)]
    fn real_tool_path() -> std::path::PathBuf {
        std::env::var_os("FLUXNET_YTDLP_REAL_EXE")
            .expect("set FLUXNET_YTDLP_REAL_EXE to the verified local tool")
            .into()
    }

    #[cfg(windows)]
    fn unique_workspace(label: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "fluxnet-ytdlp-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[cfg(windows)]
    fn assert_no_media_outputs(workspace: &std::path::Path) {
        let entries = std::fs::read_dir(workspace).unwrap().count();
        assert_eq!(entries, 0, "task workspace retained files after discovery");
    }

    /// Opt-in real-site smoke test. It returns only allowlisted metadata to
    /// the test log and fails if discovery created a file in its task workspace.
    #[cfg(windows)]
    #[test]
    #[ignore = "manual network test; requires the verified yt-dlp release and FLUXNET_YTDLP_TEST_URL"]
    fn real_yt_dlp_public_url_discovery_smoke() {
        let executable = real_tool_path();
        let page_url = std::env::var("FLUXNET_YTDLP_TEST_URL").expect("set a public test URL");
        let workspace = unique_workspace("real-site");
        let result = discover(
            &executable,
            &workspace,
            &page_url,
            &DownloadTaskControl::default(),
        )
        .unwrap();
        assert!(!result.formats.is_empty());
        result
            .validate_selection(&FormatSelection::BestAvailable)
            .unwrap();
        result
            .validate_selection(&FormatSelection::Exact(result.formats[0].format_id.clone()))
            .unwrap();
        let serialized = serde_json::to_string(&result).unwrap();
        assert!(!serialized.contains("http://") && !serialized.contains("https://"));
        assert_no_media_outputs(&workspace);
        eprintln!(
            "yt-dlp discovery: site={}, title={}, formats={}, heights={:?}, separate_av={}",
            result.extractor,
            result.title,
            result.formats.len(),
            result
                .formats
                .iter()
                .filter_map(|format| format.height)
                .collect::<Vec<_>>(),
            result
                .formats
                .iter()
                .any(|format| format.requires_separate_audio),
        );
        std::fs::remove_dir(workspace).unwrap();
    }

    #[cfg(windows)]
    fn decode_one_video_frame(path: &std::path::Path) {
        use windows::{
            Win32::Media::MediaFoundation::{
                MF_MT_MAJOR_TYPE, MF_MT_SUBTYPE, MF_SOURCE_READER_FIRST_VIDEO_STREAM, MF_VERSION,
                MFCreateMediaType, MFCreateSourceReaderFromURL, MFMediaType_Video, MFSTARTUP_FULL,
                MFShutdown, MFStartup, MFVideoFormat_RGB32,
            },
            core::PCWSTR,
        };

        unsafe { MFStartup(MF_VERSION, MFSTARTUP_FULL) }.expect("Media Foundation should start");
        struct Shutdown;
        impl Drop for Shutdown {
            fn drop(&mut self) {
                unsafe { MFShutdown() }.expect("Media Foundation should shut down");
            }
        }
        let _shutdown = Shutdown;
        let file_url = reqwest::Url::from_file_path(path)
            .expect("local test media path should form a file URL")
            .to_string();
        let path = file_url.encode_utf16().chain([0]).collect::<Vec<_>>();
        let reader = unsafe { MFCreateSourceReaderFromURL(PCWSTR(path.as_ptr()), None) }
            .expect("Windows Media Foundation should open the downloaded video");
        let video_type = unsafe { MFCreateMediaType() }.expect("media type should be created");
        unsafe { video_type.SetGUID(&MF_MT_MAJOR_TYPE, &MFMediaType_Video) }
            .expect("video major type should be set");
        unsafe { video_type.SetGUID(&MF_MT_SUBTYPE, &MFVideoFormat_RGB32) }
            .expect("uncompressed RGB output type should be set");
        unsafe {
            reader.SetCurrentMediaType(
                MF_SOURCE_READER_FIRST_VIDEO_STREAM.0 as u32,
                None,
                &video_type,
            )
        }
        .expect("Media Foundation should configure an RGB video decoder");
        let mut flags = 0;
        let mut sample = None;
        unsafe {
            reader.ReadSample(
                MF_SOURCE_READER_FIRST_VIDEO_STREAM.0 as u32,
                0,
                None,
                Some(&mut flags),
                None,
                Some(&mut sample),
            )
        }
        .expect("Media Foundation should decode a video sample");
        assert!(
            sample.is_some(),
            "decoder returned no video sample (flags={flags})"
        );
    }

    #[cfg(windows)]
    #[test]
    #[ignore = "manual public-network test; requires verified yt-dlp executable and Media Foundation codecs"]
    fn real_yt_dlp_download_exact_and_best_is_validated_decodable_and_atomic() {
        use std::sync::{Arc, Mutex};

        let executable = real_tool_path();
        let url = "https://commons.wikimedia.org/wiki/File:Big_buck_bunny_720p_5mb.webm";
        let workspace = unique_workspace("real-download");
        let catalog = discover(
            &executable,
            &workspace,
            url,
            &DownloadTaskControl::default(),
        )
        .expect("public Wikimedia test-media discovery should succeed");
        eprintln!(
            "Wikimedia test-media format metadata: {:?}",
            catalog
                .formats
                .iter()
                .map(|format| (
                    &format.format_id,
                    &format.container,
                    format.height,
                    &format.video_codec,
                    &format.audio_codec,
                    format.has_video,
                    format.has_audio,
                ))
                .collect::<Vec<_>>()
        );
        let exact_id = catalog
            .formats
            .iter()
            .find(|format| {
                matches!(format.container.as_deref(), Some("mp4" | "webm"))
                    && format.video_codec.as_deref() != Some("none")
                    && format.audio_codec.as_deref() != Some("none")
            })
            .expect("test media should provide a muxed MP4 or WebM format");
        let exact_extension = exact_id.container.as_deref().unwrap().to_ascii_lowercase();
        let exact_id = exact_id.format_id.clone();
        let control = DownloadTaskControl::default();
        let exact_progress = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&exact_progress);
        let exact = download_video_with_limits(
            &executable,
            url,
            &workspace.join(format!("exact.{exact_extension}")),
            &catalog,
            &FormatSelection::Exact(exact_id),
            &control,
            50 * 1024 * 1024,
            Duration::from_secs(180),
            Arc::new(move |progress| captured.lock().unwrap().push(progress)),
        )
        .expect("exact muxed format download should succeed");
        assert_eq!(
            exact.bytes,
            std::fs::metadata(&exact.destination).unwrap().len()
        );
        assert!(exact.bytes > 0 && exact.bytes <= 50 * 1024 * 1024);
        crate::download_manager::validate_ytdlp_media(&exact.destination).unwrap();
        decode_one_video_frame(&exact.destination);
        assert!(
            exact_progress
                .lock()
                .unwrap()
                .iter()
                .any(|progress| progress.downloaded_bytes > 0)
        );
        assert_eq!(
            exact_progress.lock().unwrap().last().unwrap().percent,
            Some(100.0)
        );

        let best = download_video_with_limits(
            &executable,
            url,
            &workspace.join(format!("best.{exact_extension}")),
            &catalog,
            &FormatSelection::BestAvailable,
            &DownloadTaskControl::default(),
            50 * 1024 * 1024,
            Duration::from_secs(180),
            Arc::new(|_| {}),
        )
        .expect("Best Available muxed MP4 download should succeed");
        crate::download_manager::validate_ytdlp_media(&best.destination).unwrap();
        decode_one_video_frame(&best.destination);
        assert!(
            std::fs::read_dir(&workspace).unwrap().all(|entry| entry
                .unwrap()
                .file_type()
                .unwrap()
                .is_file())
        );
        std::fs::remove_file(exact.destination).unwrap();
        std::fs::remove_file(best.destination).unwrap();
        std::fs::remove_dir(workspace).unwrap();
    }

    #[cfg(windows)]
    #[test]
    #[ignore = "manual network/process test; requires the verified yt-dlp release"]
    fn real_yt_dlp_timeout_and_cancel_kill_the_contained_process() {
        let executable = real_tool_path();
        let url = "https://example.invalid/fluxnet-timeout-test";
        let workspace = unique_workspace("real-timeout");
        assert_eq!(
            discover_with_limits(
                &executable,
                &workspace,
                url,
                &DownloadTaskControl::default(),
                Duration::from_millis(1),
                MAX_METADATA_BYTES,
                Vec::new(),
                None,
            )
            .unwrap_err(),
            DiscoveryError::TimedOut
        );
        assert_no_media_outputs(&workspace);
        std::fs::remove_dir(&workspace).unwrap();

        let workspace = unique_workspace("real-cancel");
        let control = std::sync::Arc::new(DownloadTaskControl::default());
        let child_control = std::sync::Arc::clone(&control);
        let child_workspace = workspace.clone();
        let child_executable = executable.clone();
        let worker = std::thread::spawn(move || {
            discover_with_limits(
                &child_executable,
                &child_workspace,
                url,
                &child_control,
                Duration::from_secs(20),
                MAX_METADATA_BYTES,
                Vec::new(),
                None,
            )
        });
        std::thread::sleep(Duration::from_millis(10));
        control.request_cancel();
        assert_eq!(
            worker.join().unwrap().unwrap_err(),
            DiscoveryError::Cancelled
        );
        assert_no_media_outputs(&workspace);
        std::fs::remove_dir(workspace).unwrap();
    }
}
