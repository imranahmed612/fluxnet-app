use crate::browser_bridge::{
    HlsPlaylist, HlsResource, MAX_HLS_EXPECTED_BYTES, validate_hls_playlist,
};
use futures_util::StreamExt;
use reqwest::{
    Client, StatusCode,
    header::{ACCEPT_RANGES, CONTENT_RANGE, CONTENT_TYPE, ETAG, LAST_MODIFIED, RANGE},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    error::Error as StdError,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tauri::{Emitter, Manager};
use tauri_plugin_notification::NotificationExt;
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
    sync::{Notify, Semaphore},
    time::sleep,
};

const DOWNLOAD_EVENT: &str = "download-manager-update";
const DOWNLOAD_HISTORY_FILE: &str = "download-history.json";
const MAX_CONCURRENT_DOWNLOADS: usize = 4;
const DEFAULT_CONNECTIONS_PER_DOWNLOAD: usize = 4;
const MAX_CONNECTIONS_PER_DOWNLOAD: usize = 16;
const MIN_PARALLEL_SEGMENT_BYTES: u64 = 16;
const MIN_PARALLEL_FILE_BYTES: u64 = 64;
const MAX_SEGMENT_ATTEMPTS: usize = 3;
const MAX_AUTO_RESUME_ATTEMPTS: u32 = 5;
const MAX_URL_LENGTH: usize = 8192;
const MAX_FILENAME_STEM_CHARS: usize = 80;
const MAX_MP4_ATOM_COUNT: usize = 100_000;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const READ_TIMEOUT: Duration = Duration::from_secs(30);
const PROGRESS_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum DownloadStatus {
    Queued,
    Downloading,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

impl DownloadStatus {
    fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    fn can_retry(&self) -> bool {
        matches!(self, Self::Failed | Self::Cancelled)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WebsiteMediaInfo {
    pub format_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audio_format_id: Option<String>,
    pub container: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadSegment {
    pub index: usize,
    pub start: u64,
    pub end: u64,
    pub downloaded_bytes: u64,
    pub completed: bool,
}

fn default_connections_per_download() -> usize {
    DEFAULT_CONNECTIONS_PER_DOWNLOAD
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadEntry {
    pub id: String,
    pub url: String,
    pub destination: String,
    pub filename: String,
    pub status: DownloadStatus,
    pub total_bytes: Option<u64>,
    pub downloaded_bytes: u64,
    pub speed_bytes_per_second: f64,
    pub elapsed_ms: u64,
    pub remaining_seconds: Option<f64>,
    pub error: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hls_playlist: Option<HlsPlaylist>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    hls_completed_parts: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub website_media: Option<WebsiteMediaInfo>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub segments: Vec<DownloadSegment>,
    etag: Option<String>,
    last_modified: Option<String>,
    pub notifications_enabled: bool,
    #[serde(default)]
    pub notify_on_complete: bool,
    #[serde(default)]
    pub notify_on_failure: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedDownloads {
    version: u8,
    entries: Vec<DownloadEntry>,
    max_concurrent: usize,
    #[serde(default = "default_connections_per_download")]
    max_connections_per_download: usize,
    bandwidth_limit_bytes_per_second: Option<u64>,
    #[serde(default)]
    notifications_enabled: bool,
    #[serde(default)]
    notify_on_complete: bool,
    #[serde(default)]
    notify_on_failure: bool,
}

fn persisted_notification_preferences(
    notifications_enabled: bool,
    notify_on_complete: bool,
    notify_on_failure: bool,
) -> (bool, bool) {
    if notify_on_complete || notify_on_failure {
        (notify_on_complete, notify_on_failure)
    } else {
        (notifications_enabled, notifications_enabled)
    }
}

pub(crate) struct DownloadTaskControl {
    cancelled: AtomicBool,
    pause_requested: AtomicBool,
    shutdown_requested: AtomicBool,
    wake: Notify,
}

/// Runs the isolated yt-dlp download inside the DownloadManager's existing
/// task slot and lifecycle.
#[cfg(windows)]
async fn run_ytdlp_download_worker(
    app: Option<&tauri::AppHandle>,
    state: &Arc<Mutex<DownloadState>>,
    concurrency: &Arc<ConcurrencyLimiter>,
    id: &str,
    executable: PathBuf,
    download_plan: crate::media::yt_dlp::DownloadPlan,
    control: &Arc<DownloadTaskControl>,
) -> Result<(), String> {
    let Some(_slot) = concurrency.acquire(state, control).await? else {
        remove_website_partial_if_present(state, id);
        return finish_cancelled_optional(app, state, id);
    };
    if control.is_cancelled() {
        remove_website_partial_if_present(state, id);
        return finish_cancelled_optional(app, state, id);
    }
    let (url, destination, base_elapsed_ms, initial_downloaded_bytes) = {
        let locked = state
            .lock()
            .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
        let entry = locked
            .entries
            .iter()
            .find(|entry| entry.id == id)
            .ok_or_else(|| "Download record was not found.".to_owned())?;
        if entry.status == DownloadStatus::Cancelled || control.is_cancelled() {
            let dest = PathBuf::from(&entry.destination);
            drop(locked);
            let _ = std::fs::remove_file(partial_path(&dest));
            return finish_cancelled_optional(app, state, id);
        }
        if control.is_pause_requested() {
            let dest = PathBuf::from(&entry.destination);
            let retained_bytes = std::fs::metadata(partial_path(&dest))
                .map(|m| m.len())
                .unwrap_or(entry.downloaded_bytes);
            drop(locked);
            update_entry_optional(app, state, id, |entry| {
                if entry.status == DownloadStatus::Queued
                    || entry.status == DownloadStatus::Downloading
                {
                    if entry.status == DownloadStatus::Queued {
                        transition(entry, DownloadStatus::Downloading)?;
                    }
                    transition(entry, DownloadStatus::Paused)?;
                    entry.downloaded_bytes = retained_bytes;
                    entry.speed_bytes_per_second = 0.0;
                    entry.remaining_seconds = None;
                }
                Ok(())
            })?;
            return Ok(());
        }
        if entry.status != DownloadStatus::Queued && entry.status != DownloadStatus::Paused {
            return Err("The media task is not queued.".into());
        }
        (
            entry.url.clone(),
            PathBuf::from(&entry.destination),
            entry.elapsed_ms,
            entry.downloaded_bytes,
        )
    };
    update_entry_optional(app, state, id, |entry| {
        if entry.status == DownloadStatus::Cancelled || control.is_cancelled() {
            return Ok(());
        }
        if entry.status == DownloadStatus::Queued || entry.status == DownloadStatus::Paused {
            transition(entry, DownloadStatus::Downloading)?;
        }
        Ok(())
    })?;
    if control.is_cancelled() {
        let _ = std::fs::remove_file(partial_path(&destination));
        return finish_cancelled_optional(app, state, id);
    }

    let started = Instant::now();
    let app_for_progress = app.cloned();
    let state_for_progress = Arc::clone(state);
    let id_for_progress = id.to_owned();
    let control_for_progress = Arc::clone(control);
    let process_control = Arc::clone(control);
    let destination_for_worker = destination.clone();
    let progress =
        std::sync::Arc::new(move |value: crate::media::yt_dlp::VideoDownloadProgress| {
            if control_for_progress.is_cancelled() || control_for_progress.is_pause_requested() {
                return;
            }
            let elapsed_ms = base_elapsed_ms
                .saturating_add(started.elapsed().as_millis().min(u64::MAX as u128) as u64);
            let _ = update_entry_optional(
                app_for_progress.as_ref(),
                &state_for_progress,
                &id_for_progress,
                |entry| {
                    if entry.status != DownloadStatus::Downloading {
                        return Ok(());
                    }
                    entry.downloaded_bytes = value.downloaded_bytes;
                    entry.total_bytes = value.total_bytes;
                    entry.speed_bytes_per_second = value.speed_bytes_per_second.unwrap_or(0.0);
                    entry.elapsed_ms = elapsed_ms;
                    entry.remaining_seconds = value.eta_seconds;
                    Ok(())
                },
            );
        });
    let app_for_split = app.cloned();
    let result = match download_plan {
        crate::media::yt_dlp::DownloadPlan::Muxed(selected_format) => {
            tauri::async_runtime::spawn_blocking(move || {
                crate::media::yt_dlp::download_selected_video(
                    &executable,
                    &url,
                    &destination_for_worker,
                    &selected_format,
                    &process_control,
                    progress,
                )
            })
            .await
            .map_err(|_| "The video download worker could not finish.".to_owned())?
        }
        crate::media::yt_dlp::DownloadPlan::Split(split) => {
            let ffmpeg_exe = crate::media::ffmpeg::resolve_verified_ffmpeg_executable(
                app_for_split.as_ref(),
            )?;
            let ffprobe_exe = crate::media::ffmpeg::resolve_verified_ffprobe_executable(
                app_for_split.as_ref(),
            )?;
            tauri::async_runtime::spawn_blocking(move || {
                crate::media::yt_dlp::download_split_video(
                    &executable,
                    &ffmpeg_exe,
                    &ffprobe_exe,
                    &url,
                    &destination_for_worker,
                    &split,
                    &process_control,
                    progress,
                )
            })
            .await
            .map_err(|_| "The video download worker could not finish.".to_owned())?
        }
    };

    let elapsed_ms =
        base_elapsed_ms.saturating_add(started.elapsed().as_millis().min(u64::MAX as u128) as u64);
    match result {
        Ok(download) => {
            if control.is_cancelled() {
                let _ = std::fs::remove_file(&download.destination);
                let _ = std::fs::remove_file(partial_path(&destination));
                finish_cancelled_optional(app, state, id)?;
                return Ok(());
            }
            let completed = update_entry_optional(app, state, id, |entry| {
                if control.is_cancelled() || entry.status == DownloadStatus::Cancelled {
                    return Err("The media task was cancelled.".into());
                }
                transition(entry, DownloadStatus::Completed)?;
                entry.downloaded_bytes = download.bytes;
                entry.total_bytes = Some(download.bytes);
                entry.speed_bytes_per_second = 0.0;
                entry.elapsed_ms = elapsed_ms;
                entry.remaining_seconds = Some(0.0);
                entry.error = None;
                Ok(())
            });
            match completed {
                Ok(entry) => {
                    if entry.notify_on_complete {
                        send_download_notification_optional(app, &entry, true);
                    }
                    Ok(())
                }
                Err(_error) if control.is_cancelled() => {
                    let _ = std::fs::remove_file(download.destination);
                    let _ = std::fs::remove_file(partial_path(&destination));
                    finish_cancelled_optional(app, state, id)?;
                    Ok(())
                }
                Err(error) => Err(error),
            }
        }
        Err(_error) if control.is_cancelled() => {
            let _ = std::fs::remove_file(partial_path(&destination));
            finish_cancelled_optional(app, state, id)
        }
        Err(crate::media::yt_dlp::VideoDownloadError::Paused { downloaded_bytes }) => {
            let retained_bytes = if downloaded_bytes > 0 {
                downloaded_bytes
            } else {
                initial_downloaded_bytes
            };
            match update_entry_optional(app, state, id, |entry| {
                if control.is_cancelled() || entry.status == DownloadStatus::Cancelled {
                    return Err("The media task was cancelled.".into());
                }
                transition(entry, DownloadStatus::Paused)?;
                entry.downloaded_bytes = retained_bytes;
                entry.elapsed_ms = elapsed_ms;
                entry.speed_bytes_per_second = 0.0;
                entry.remaining_seconds = None;
                entry.error = None;
                Ok(())
            }) {
                Ok(_) => Ok(()),
                Err(_error) if control.is_cancelled() => {
                    let _ = std::fs::remove_file(partial_path(&destination));
                    finish_cancelled_optional(app, state, id)
                }
                Err(error) => Err(error),
            }
        }
        Err(crate::media::yt_dlp::VideoDownloadError::Cancelled) => {
            let _ = std::fs::remove_file(partial_path(&destination));
            finish_cancelled_optional(app, state, id)
        }
        Err(error) => fail_entry_optional(app, state, id, error.to_string()).await,
    }
}

#[cfg(windows)]
fn remove_website_partial_if_present(state: &Mutex<DownloadState>, id: &str) {
    let destination = state.lock().ok().and_then(|locked| {
        locked
            .entries
            .iter()
            .find(|entry| entry.id == id && entry.website_media.is_some())
            .map(|entry| PathBuf::from(&entry.destination))
    });
    if let Some(destination) = destination {
        let _ = std::fs::remove_file(partial_path(&destination));
    }
}

#[cfg(windows)]
fn verify_website_executable_for_resume(candidate: &Path, url: &str) -> Result<PathBuf, String> {
    #[cfg(test)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
        if url.starts_with("https://example.test/fixture-") {
            if let (Ok(current_exe), Ok(meta)) =
                (std::env::current_exe(), std::fs::symlink_metadata(candidate))
            {
                if meta.is_file() && meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT == 0 {
                    if let (Ok(canon_candidate), Ok(canon_self)) = (
                        std::fs::canonicalize(candidate),
                        std::fs::canonicalize(current_exe),
                    ) {
                        if canon_candidate == canon_self {
                            return Ok(candidate.to_path_buf());
                        }
                    }
                }
            }
        }
    }
    #[cfg(not(test))]
    let _ = url;
    crate::media::yt_dlp::verify_ytdlp_executable(candidate)
}

#[cfg(windows)]
fn spawn_website_worker(
    app: Option<tauri::AppHandle>,
    state: Arc<Mutex<DownloadState>>,
    concurrency: Arc<ConcurrencyLimiter>,
    worker_count: Arc<AtomicUsize>,
    id: String,
    executable: PathBuf,
    download_plan: crate::media::yt_dlp::DownloadPlan,
    control: Arc<DownloadTaskControl>,
) {
    worker_count.fetch_add(1, Ordering::AcqRel);
    tauri::async_runtime::spawn(async move {
        let _worker_guard = WorkerGuard(worker_count);
        if let Err(error) = run_ytdlp_download_worker(
            app.as_ref(),
            &state,
            &concurrency,
            &id,
            executable,
            download_plan,
            &control,
        )
        .await
        {
            if let Err(update_error) = fail_entry_optional(app.as_ref(), &state, &id, error).await {
                eprintln!("Could not record website download failure: {update_error}");
            }
        }
    });
}

impl Default for DownloadTaskControl {
    fn default() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            pause_requested: AtomicBool::new(false),
            shutdown_requested: AtomicBool::new(false),
            wake: Notify::new(),
        }
    }
}

impl DownloadTaskControl {
    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub(crate) fn request_cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.wake.notify_waiters();
        self.wake.notify_one();
    }

    pub(crate) fn is_pause_requested(&self) -> bool {
        self.pause_requested.load(Ordering::Acquire)
    }

    #[allow(dead_code)]
    pub(crate) fn request_pause(&self) {
        self.pause_requested.store(true, Ordering::Release);
        self.wake.notify_waiters();
        self.wake.notify_one();
    }

    pub(crate) fn is_shutdown_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::Acquire)
    }

    pub(crate) fn request_shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::Release);
        self.wake.notify_waiters();
        self.wake.notify_one();
    }
}

struct DownloadState {
    entries: Vec<DownloadEntry>,
    controls: HashMap<String, Arc<DownloadTaskControl>>,
    #[cfg(windows)]
    website_executables: HashMap<String, PathBuf>,
    #[cfg(windows)]
    verified_ytdlp_executable: Option<(PathBuf, Option<SystemTime>)>,
    max_concurrent: usize,
    max_connections_per_download: usize,
    bandwidth_limit_bytes_per_second: Option<u64>,
    history_path: Option<PathBuf>,
    notifications_enabled: bool,
    notify_on_complete: bool,
    notify_on_failure: bool,
}

impl Default for DownloadState {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            controls: HashMap::new(),
            #[cfg(windows)]
            website_executables: HashMap::new(),
            #[cfg(windows)]
            verified_ytdlp_executable: None,
            max_concurrent: 2,
            max_connections_per_download: DEFAULT_CONNECTIONS_PER_DOWNLOAD,
            bandwidth_limit_bytes_per_second: None,
            history_path: None,
            notifications_enabled: false,
            notify_on_complete: false,
            notify_on_failure: false,
        }
    }
}

#[derive(Default)]
struct ConcurrencyLimiter {
    active: AtomicUsize,
    notify: Notify,
}

struct DownloadSlot(Arc<ConcurrencyLimiter>);

impl Drop for DownloadSlot {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
        self.0.notify.notify_waiters();
    }
}

impl ConcurrencyLimiter {
    async fn acquire(
        self: &Arc<Self>,
        state: &Mutex<DownloadState>,
        control: &DownloadTaskControl,
    ) -> Result<Option<DownloadSlot>, String> {
        loop {
            if control.cancelled.load(Ordering::Acquire)
                || control.shutdown_requested.load(Ordering::Acquire)
            {
                return Ok(None);
            }
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let wake = control.wake.notified();
            tokio::pin!(wake);
            wake.as_mut().enable();
            let limit = state
                .lock()
                .map_err(|_| "Download manager state lock was poisoned.".to_owned())?
                .max_concurrent;
            let active = self.active.load(Ordering::Acquire);
            if active < limit
                && self
                    .active
                    .compare_exchange(active, active + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                return Ok(Some(DownloadSlot(Arc::clone(self))));
            }
            tokio::select! {
                _ = &mut notified => {},
                _ = &mut wake => {},
            }
        }
    }
}

#[derive(Default)]
struct BandwidthLimiter {
    next_available: Mutex<Option<Instant>>,
}

impl BandwidthLimiter {
    async fn wait_for_turn(&self, bytes: usize, limit: Option<u64>) -> Result<(), String> {
        let Some(limit) = limit.filter(|limit| *limit > 0) else {
            return Ok(());
        };
        let scheduled = {
            let mut next = self
                .next_available
                .lock()
                .map_err(|_| "Download bandwidth limiter lock was poisoned.".to_owned())?;
            let now = Instant::now();
            let scheduled = next.map_or(now, |next| next.max(now));
            *next = Some(scheduled + Duration::from_secs_f64(bytes as f64 / limit as f64));
            scheduled
        };
        if scheduled > Instant::now() {
            sleep(scheduled - Instant::now()).await;
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct DownloadManager {
    state: Arc<Mutex<DownloadState>>,
    concurrency: Arc<ConcurrencyLimiter>,
    bandwidth: Arc<BandwidthLimiter>,
    worker_count: Arc<AtomicUsize>,
}

impl DownloadManager {
    pub(crate) fn pause_download(&self, id: &str) -> Result<(), String> {
        let state = self
            .state
            .lock()
            .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
        let entry = state
            .entries
            .iter()
            .find(|entry| entry.id == id)
            .ok_or_else(|| "Download record was not found.".to_owned())?;
        if entry.status != DownloadStatus::Downloading {
            return Err("Only a running download can be paused.".into());
        }
        let control = state
            .controls
            .get(id)
            .ok_or_else(|| "The download worker is no longer available.".to_owned())?;
        control.request_pause();
        Ok(())
    }

    pub(crate) fn resume_download(
        &self,
        app: Option<&tauri::AppHandle>,
        id: &str,
    ) -> Result<(), String> {
        enum ResumeTarget {
            Standard,
            #[cfg(windows)]
            Website {
                executable: PathBuf,
                download_plan: crate::media::yt_dlp::DownloadPlan,
            },
        }

        let (entry, control, target) = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
            let index = state
                .entries
                .iter()
                .position(|entry| entry.id == id)
                .ok_or_else(|| "Download record was not found.".to_owned())?;
            let original = state.entries[index].clone();
            if original.status != DownloadStatus::Paused {
                return Err("Only a paused download can be resumed.".into());
            }
            if state.controls.contains_key(id) {
                return Err("A download worker is already active for this item.".into());
            }
            let target = if let Some(website_media) = &original.website_media {
                #[cfg(windows)]
                {
                    let cached_executable = state
                        .website_executables
                        .get(id)
                        .cloned()
                        .ok_or_else(|| {
                            "The yt-dlp executable for this paused website download is no longer available; start a new download to retry.".to_owned()
                        })?;
                    let executable =
                        verify_website_executable_for_resume(&cached_executable, &original.url)?;
                    let download_plan = crate::media::yt_dlp::download_plan_from_info(
                        &website_media.format_id,
                        website_media.audio_format_id.as_deref(),
                        &website_media.container,
                    )
                    .map_err(|error| error.to_string())?;
                    let (validated_dest, _) =
                        validate_resumable_download_destination(&original.destination)?;
                    if validated_dest
                        .extension()
                        .and_then(|ext| ext.to_str())
                        .is_none_or(|ext| !ext.eq_ignore_ascii_case(download_plan.extension()))
                    {
                        return Err(
                            "The paused website download destination extension no longer matches its format container."
                                .into(),
                        );
                    }
                    ResumeTarget::Website {
                        executable,
                        download_plan,
                    }
                }
                #[cfg(not(windows))]
                {
                    let _ = website_media;
                    return Err("Website video downloads are supported on Windows only.".into());
                }
            } else {
                ResumeTarget::Standard
            };

            let mut entry = original.clone();
            transition(&mut entry, DownloadStatus::Queued)?;
            entry.attempts = entry.attempts.saturating_add(1);
            entry.error = None;
            let control = Arc::new(DownloadTaskControl::default());
            state.entries[index] = entry.clone();
            let old_control = state.controls.insert(id.to_owned(), Arc::clone(&control));
            if let Err(error) = persist_locked(&state) {
                state.entries[index] = original;
                if let Some(old_control) = old_control {
                    state.controls.insert(id.to_owned(), old_control);
                } else {
                    state.controls.remove(id);
                }
                return Err(error);
            }
            (entry, control, target)
        };

        emit_entry_optional(app, entry);
        match target {
            ResumeTarget::Standard => {
                spawn_worker(
                    app.cloned(),
                    Arc::clone(&self.state),
                    Arc::clone(&self.concurrency),
                    Arc::clone(&self.bandwidth),
                    Arc::clone(&self.worker_count),
                    id.to_owned(),
                    control,
                    false,
                );
            }
            #[cfg(windows)]
            ResumeTarget::Website {
                executable,
                download_plan,
            } => {
                spawn_website_worker(
                    app.cloned(),
                    Arc::clone(&self.state),
                    Arc::clone(&self.concurrency),
                    Arc::clone(&self.worker_count),
                    id.to_owned(),
                    executable,
                    download_plan,
                    control,
                );
            }
        }
        Ok(())
    }

    pub(crate) fn cancel_download(
        &self,
        app: Option<&tauri::AppHandle>,
        id: &str,
    ) -> Result<(), String> {
        let (cancelled_entry, cleanup_website_partial, cleanup_segments) = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
            let index = state
                .entries
                .iter()
                .position(|entry| entry.id == id)
                .ok_or_else(|| "Download record was not found.".to_owned())?;
            if state.entries[index].status.is_terminal() {
                return Err("This download is already finished.".into());
            }
            let control = state.controls.get(id).cloned();
            if state.entries[index].status == DownloadStatus::Paused
                || state.entries[index].status == DownloadStatus::Queued
            {
                let original = state.entries[index].clone();
                let mut cancelled = original.clone();
                transition(&mut cancelled, DownloadStatus::Cancelled)?;
                cancelled.speed_bytes_per_second = 0.0;
                cancelled.remaining_seconds = None;
                let has_segments = !cancelled.segments.is_empty();
                cancelled.segments.clear();
                state.entries[index] = cancelled.clone();
                state.controls.remove(id);
                #[cfg(windows)]
                let removed_exe = state.website_executables.remove(id);
                if let Err(error) = persist_locked(&state) {
                    state.entries[index] = original;
                    if let Some(control) = control {
                        state.controls.insert(id.to_owned(), control);
                    }
                    #[cfg(windows)]
                    if let Some(exe) = removed_exe {
                        state.website_executables.insert(id.to_owned(), exe);
                    }
                    return Err(error);
                }
                if let Some(control) = control {
                    control.request_cancel();
                }
                let cleanup_partial = Some(PathBuf::from(&cancelled.destination));
                let seg_cleanup =
                    has_segments.then(|| (PathBuf::from(&cancelled.destination), cancelled.id.clone()));
                (Some(cancelled), cleanup_partial, seg_cleanup)
            } else {
                let control =
                    control.ok_or_else(|| "The download worker was not found.".to_owned())?;
                // Set the signal while holding the same state lock used by the
                // worker's Completed and Paused transitions. Cancellation always
                // wins over both pause and completion races.
                control.request_cancel();
                (None, None, None)
            }
        };
        if let Some(dest) = cleanup_website_partial {
            let _ = std::fs::remove_file(partial_path(&dest));
        }
        if let Some((dest, cancel_id)) = cleanup_segments {
            remove_all_segment_parts_sync(&dest, &cancel_id);
            let _ = std::fs::remove_file(partial_path(&dest));
        }
        if let Some(entry) = cancelled_entry {
            emit_entry_optional(app, entry);
        }
        Ok(())
    }
    #[cfg(windows)]
    #[allow(dead_code)]
    pub(crate) async fn run_ytdlp_download_worker(
        &self,
        app: Option<&tauri::AppHandle>,
        id: &str,
        executable: PathBuf,
        catalog: crate::media::yt_dlp::VideoDiscovery,
        selection: crate::media::yt_dlp::FormatSelection,
        control: &Arc<DownloadTaskControl>,
    ) -> Result<(), String> {
        let download_plan = crate::media::yt_dlp::select_download_plan(&catalog, &selection)
            .map_err(|error| error.to_string())?;
        run_ytdlp_download_worker(
            app,
            &self.state,
            &self.concurrency,
            id,
            executable,
            download_plan,
            control,
        )
        .await
    }

    #[cfg(windows)]
    pub(crate) fn resolve_verified_ytdlp_executable(
        &self,
        app: Option<&tauri::AppHandle>,
    ) -> Result<PathBuf, String> {
        {
            let cached = self
                .state
                .lock()
                .map_err(|_| "Download manager state lock was poisoned.".to_owned())?
                .verified_ytdlp_executable
                .as_ref()
                .map(|(cached_path, _)| cached_path.clone());
            if let Some(cached_path) = cached {
                if let Ok(verified) = crate::media::yt_dlp::verify_ytdlp_executable(&cached_path) {
                    return Ok(verified);
                }
            }
        }

        let verified = crate::media::yt_dlp::resolve_verified_executable(app)?;
        let modified = std::fs::symlink_metadata(&verified)
            .ok()
            .and_then(|meta| meta.modified().ok());
        let mut state = self
            .state
            .lock()
            .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
        state.verified_ytdlp_executable = Some((verified.clone(), modified));
        Ok(verified)
    }

    #[cfg(windows)]
    pub(crate) async fn discover_website_media(
        &self,
        app: Option<&tauri::AppHandle>,
        url: String,
    ) -> Result<crate::media::yt_dlp::VideoDiscovery, String> {
        if url.len() > MAX_URL_LENGTH {
            return Err("Download URL is too long.".into());
        }
        let normalized_url = crate::media::yt_dlp::validate_public_page_url(&url)
            .map_err(|error| error.to_string())?;
        let executable = self.resolve_verified_ytdlp_executable(app)?;
        let base_dir = app
            .and_then(|app| app.path().app_cache_dir().ok())
            .filter(|dir| std::fs::create_dir_all(dir).is_ok())
            .unwrap_or_else(std::env::temp_dir);
        self.run_website_discovery(executable, base_dir, normalized_url)
            .await
    }

    #[cfg(windows)]
    async fn run_website_discovery(
        &self,
        executable: PathBuf,
        base_dir: PathBuf,
        normalized_url: String,
    ) -> Result<crate::media::yt_dlp::VideoDiscovery, String> {
        struct DiscoveryControlGuard {
            state: Arc<Mutex<DownloadState>>,
            id: String,
        }

        impl Drop for DiscoveryControlGuard {
            fn drop(&mut self) {
                if let Ok(mut state) = self.state.lock() {
                    state.controls.remove(&self.id);
                }
            }
        }

        let control = Arc::new(DownloadTaskControl::default());
        let control_id = format!("discovery-{}-{}", current_timestamp_ms(), rand_id());
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
            state
                .controls
                .insert(control_id.clone(), Arc::clone(&control));
        }
        self.worker_count.fetch_add(1, Ordering::AcqRel);
        let _worker_guard = WorkerGuard(Arc::clone(&self.worker_count));
        let _control_guard = DiscoveryControlGuard {
            state: Arc::clone(&self.state),
            id: control_id,
        };

        tauri::async_runtime::spawn_blocking(move || {
            crate::media::yt_dlp::discover_in_workspace(
                &executable,
                &base_dir,
                &normalized_url,
                &control,
            )
        })
        .await
        .map_err(|_| "The website media discovery worker could not finish.".to_owned())?
    }

    #[cfg(windows)]
    pub(crate) fn start_website_download_verified(
        &self,
        app: Option<&tauri::AppHandle>,
        url: String,
        destination: String,
        catalog: crate::media::yt_dlp::VideoDiscovery,
        selection: crate::media::yt_dlp::FormatSelection,
    ) -> Result<DownloadEntry, String> {
        if url.len() > MAX_URL_LENGTH {
            return Err("Download URL is too long.".into());
        }
        let normalized_url = crate::media::yt_dlp::validate_public_page_url(&url)
            .map_err(|error| format!("Invalid public website URL: {error}"))?;
        let _ = crate::media::yt_dlp::select_download_plan(&catalog, &selection)
            .map_err(|error| error.to_string())?;
        let executable = self.resolve_verified_ytdlp_executable(app)?;
        self.start_website_download(
            app,
            normalized_url,
            destination,
            executable,
            catalog,
            selection,
        )
    }

    #[cfg(windows)]
    pub(crate) fn start_website_download(
        &self,
        app: Option<&tauri::AppHandle>,
        url: String,
        destination: String,
        executable: PathBuf,
        catalog: crate::media::yt_dlp::VideoDiscovery,
        selection: crate::media::yt_dlp::FormatSelection,
    ) -> Result<DownloadEntry, String> {
        if url.len() > MAX_URL_LENGTH {
            return Err("Download URL is too long.".into());
        }
        {
            use std::os::windows::fs::MetadataExt;
            const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
            let meta = std::fs::symlink_metadata(&executable)
                .map_err(|_| "The yt-dlp executable path is invalid or does not exist.".to_owned())?;
            if !meta.is_file() || meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
                return Err("The yt-dlp executable path is invalid or does not exist.".into());
            }
        }
        let normalized_url = crate::media::yt_dlp::validate_public_page_url(&url)
            .map_err(|error| format!("Invalid public website URL: {error}"))?;
        let download_plan = crate::media::yt_dlp::select_download_plan(&catalog, &selection)
            .map_err(|error| error.to_string())?;

        let id = format!("{}-{}", current_timestamp_ms(), rand_id());
        let now = current_timestamp_ms();

        let (entry, control) = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
            let destination = unique_destination(Path::new(&destination), &state.entries)?;
            let (destination, filename) =
                validate_download_destination(destination.to_string_lossy().as_ref())?;

            let dest_ext = destination
                .extension()
                .and_then(|value| value.to_str())
                .map(str::to_ascii_lowercase);
            if dest_ext.as_deref() != Some(download_plan.extension()) {
                return Err(format!(
                    "The destination file extension (.{:?}) does not match the selected format container (.{}). FluxNet never renames WebM to MP4 or vice versa.",
                    dest_ext.unwrap_or_default(),
                    download_plan.extension()
                ));
            }

            let height = match &download_plan {
                crate::media::yt_dlp::DownloadPlan::Muxed(selected_format) => match &selection {
                    crate::media::yt_dlp::FormatSelection::Exact(id) => catalog
                        .formats
                        .iter()
                        .find(|f| &f.format_id == id)
                        .and_then(|f| f.height),
                    crate::media::yt_dlp::FormatSelection::BestAvailable => catalog
                        .formats
                        .iter()
                        .filter(|f| {
                            f.container
                                .as_deref()
                                .is_some_and(|c| c.eq_ignore_ascii_case(selected_format.extension))
                                && f.video_codec.as_deref() != Some("none")
                                && f.audio_codec.as_deref() != Some("none")
                        })
                        .filter_map(|f| f.height)
                        .max(),
                },
                crate::media::yt_dlp::DownloadPlan::Split(split) => catalog
                    .formats
                    .iter()
                    .find(|f| f.format_id == split.video_format_id)
                    .and_then(|f| f.height),
            };

            let control = Arc::new(DownloadTaskControl::default());
            let entry = DownloadEntry {
                id: id.clone(),
                url: normalized_url.clone(),
                destination: destination.to_string_lossy().to_string(),
                filename,
                status: DownloadStatus::Queued,
                total_bytes: None,
                downloaded_bytes: 0,
                speed_bytes_per_second: 0.0,
                elapsed_ms: 0,
                remaining_seconds: None,
                error: None,
                created_at_ms: now,
                updated_at_ms: now,
                attempts: 1,
                hls_playlist: None,
                hls_completed_parts: None,
                website_media: Some(WebsiteMediaInfo {
                    format_id: download_plan.primary_format_id().to_owned(),
                    audio_format_id: download_plan.audio_format_id().map(str::to_owned),
                    container: download_plan.extension().to_string(),
                    height,
                }),
                segments: Vec::new(),
                etag: None,
                last_modified: None,
                notifications_enabled: state.notifications_enabled,
                notify_on_complete: state.notify_on_complete,
                notify_on_failure: state.notify_on_failure,
            };
            state.entries.push(entry.clone());
            state.controls.insert(id.clone(), Arc::clone(&control));
            state
                .website_executables
                .insert(id.clone(), executable.clone());
            if let Err(error) = persist_locked(&state) {
                state.entries.pop();
                state.controls.remove(&id);
                state.website_executables.remove(&id);
                return Err(error);
            }
            (entry, control)
        };

        emit_entry_optional(app, entry.clone());
        spawn_website_worker(
            app.cloned(),
            Arc::clone(&self.state),
            Arc::clone(&self.concurrency),
            Arc::clone(&self.worker_count),
            id,
            executable,
            download_plan,
            control,
        );
        Ok(entry)
    }

    #[cfg(not(windows))]
    pub(crate) fn start_website_download(
        &self,
        _app: Option<&tauri::AppHandle>,
        _url: String,
        _destination: String,
        _executable: PathBuf,
        _catalog: (),
        _selection: (),
    ) -> Result<DownloadEntry, String> {
        Err("Website video downloads are supported on Windows only.".into())
    }

    pub fn initialize(&self, app: &tauri::AppHandle) -> Result<(), String> {
        let directory = app.path().app_data_dir().map_err(|error| {
            filesystem_error("could not locate FluxNet's app-data directory", error)
        })?;
        std::fs::create_dir_all(&directory).map_err(|error| {
            filesystem_error("could not create FluxNet's app-data directory", error)
        })?;
        let path = directory.join(DOWNLOAD_HISTORY_FILE);
        self.initialize_with_history_path(Some(app), path)
    }

    pub(crate) fn initialize_with_history_path(
        &self,
        app: Option<&tauri::AppHandle>,
        path: PathBuf,
    ) -> Result<(), String> {
        let persisted = match std::fs::read(&path) {
            Ok(contents) => Some(
                serde_json::from_slice::<PersistedDownloads>(&contents).map_err(|error| {
                    filesystem_error("could not parse saved download history", error)
                })?,
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => {
                return Err(filesystem_error(
                    "could not read saved download history",
                    error,
                ));
            }
        };
        let mut to_resume: Vec<(String, Arc<DownloadTaskControl>)> = Vec::new();
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
            state.history_path = Some(path);
            if let Some(mut persisted) = persisted {
                if persisted.version != 1 {
                    return Err("Saved download history uses an unsupported version.".into());
                }
                state.max_concurrent = persisted.max_concurrent.clamp(1, MAX_CONCURRENT_DOWNLOADS);
                state.max_connections_per_download = persisted
                    .max_connections_per_download
                    .clamp(1, MAX_CONNECTIONS_PER_DOWNLOAD);
                state.bandwidth_limit_bytes_per_second =
                    persisted.bandwidth_limit_bytes_per_second;
                state.notifications_enabled = persisted.notifications_enabled;
                (state.notify_on_complete, state.notify_on_failure) =
                    persisted_notification_preferences(
                        persisted.notifications_enabled,
                        persisted.notify_on_complete,
                        persisted.notify_on_failure,
                    );

                let now = current_timestamp_ms();
                for entry in &mut persisted.entries {
                    if entry.notifications_enabled
                        && !entry.notify_on_complete
                        && !entry.notify_on_failure
                    {
                        entry.notify_on_complete = true;
                        entry.notify_on_failure = true;
                    }
                    entry.speed_bytes_per_second = 0.0;
                    entry.remaining_seconds = None;

                    match entry.status {
                        DownloadStatus::Completed
                        | DownloadStatus::Cancelled
                        | DownloadStatus::Failed => {
                            // Never automatically restart completed, cancelled, or permanently failed entries.
                        }
                        DownloadStatus::Paused => {
                            // Respect Paused state while verifying partial/segment data on disk.
                            verify_and_repair_partial_state_on_disk(entry);
                        }
                        DownloadStatus::Queued | DownloadStatus::Downloading => {
                            // Prevent duplicate workers if one is already active for this ID.
                            if state.controls.contains_key(&entry.id) {
                                continue;
                            }
                            if entry.website_media.is_some() {
                                entry.status = DownloadStatus::Failed;
                                entry.error = Some(
                                    "Interrupted when FluxNet exited. Start a new website download to retry."
                                        .into(),
                                );
                                entry.updated_at_ms = now;
                                continue;
                            }
                            if entry.attempts >= MAX_AUTO_RESUME_ATTEMPTS {
                                entry.status = DownloadStatus::Failed;
                                entry.error = Some(
                                    "Automatic resume limit reached after repeated interruptions. Retry explicitly to continue or restart."
                                        .into(),
                                );
                                entry.updated_at_ms = now;
                                continue;
                            }
                            let validated_url = validated_download_url(&entry.url).and_then(|u| {
                                if entry.hls_playlist.is_none() {
                                    normalize_instagram_media_url(&u)
                                } else {
                                    Ok(u)
                                }
                            });
                            let parsed_url = match validated_url {
                                Ok(parsed) => parsed,
                                Err(error) => {
                                    entry.status = DownloadStatus::Failed;
                                    entry.error = Some(error);
                                    entry.updated_at_ms = now;
                                    continue;
                                }
                            };
                            if is_signed_url_expired(&parsed_url) {
                                entry.status = DownloadStatus::Failed;
                                entry.error = Some(
                                    "The signed download URL has expired and cannot be automatically resumed. Obtain a fresh download link to retry."
                                        .into(),
                                );
                                entry.updated_at_ms = now;
                                continue;
                            }
                            entry.url = parsed_url.to_string();
                            let dest_path = Path::new(&entry.destination);
                            if dest_path.exists() {
                                entry.status = DownloadStatus::Failed;
                                entry.error = Some(
                                    "The destination file already exists; choose a different filename."
                                        .into(),
                                );
                                entry.updated_at_ms = now;
                                continue;
                            }
                            verify_and_repair_partial_state_on_disk(entry);
                            entry.status = DownloadStatus::Queued;
                            entry.attempts = entry.attempts.saturating_add(1);
                            entry.error = None;
                            entry.updated_at_ms = now;
                            let control = Arc::new(DownloadTaskControl::default());
                            state
                                .controls
                                .insert(entry.id.clone(), Arc::clone(&control));
                            to_resume.push((entry.id.clone(), control));
                        }
                    }
                }
                // If any existing in-memory entry had an active control not present in persisted, preserve its control lock.
                state.entries = persisted.entries;
            }
            persist_locked(&state)?;
        }
        for (id, control) in to_resume {
            spawn_worker(
                app.cloned(),
                Arc::clone(&self.state),
                Arc::clone(&self.concurrency),
                Arc::clone(&self.bandwidth),
                Arc::clone(&self.worker_count),
                id,
                control,
                false,
            );
        }
        Ok(())
    }

    pub fn configure_connections_per_download(
        &self,
        max_connections: usize,
    ) -> Result<(), String> {
        if !(1..=MAX_CONNECTIONS_PER_DOWNLOAD).contains(&max_connections) {
            return Err(format!(
                "Choose between 1 and {MAX_CONNECTIONS_PER_DOWNLOAD} parallel connections per download."
            ));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
        let previous = state.max_connections_per_download;
        state.max_connections_per_download = max_connections;
        if let Err(error) = persist_locked(&state) {
            state.max_connections_per_download = previous;
            return Err(error);
        }
        Ok(())
    }

    fn snapshot(&self) -> Result<Vec<DownloadEntry>, String> {
        let state = self
            .state
            .lock()
            .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
        Ok(state.entries.clone())
    }

    fn settings(&self) -> Result<DownloadSettings, String> {
        let state = self
            .state
            .lock()
            .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
        Ok(DownloadSettings {
            max_concurrent: state.max_concurrent,
            max_connections_per_download: state.max_connections_per_download,
            bandwidth_limit_mbps: state
                .bandwidth_limit_bytes_per_second
                .map_or(0.0, |limit| limit as f64 * 8.0 / 1_000_000.0),
            notifications_enabled: state.notifications_enabled,
            notify_on_complete: state.notify_on_complete,
            notify_on_failure: state.notify_on_failure,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadSettings {
    max_concurrent: usize,
    max_connections_per_download: usize,
    bandwidth_limit_mbps: f64,
    notifications_enabled: bool,
    notify_on_complete: bool,
    notify_on_failure: bool,
}

fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn sanitize_filename(input: &str) -> String {
    let tail = input.rsplit(['/', '\\']).next().unwrap_or_default();
    let sanitized = tail
        .chars()
        .map(|character| {
            if character.is_control()
                || matches!(
                    character,
                    '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*'
                )
            {
                '_'
            } else {
                character
            }
        })
        .collect::<String>()
        .trim_matches([' ', '.'])
        .to_owned();
    let sanitized = if sanitized.is_empty() || sanitized == "." || sanitized == ".." {
        "download".to_owned()
    } else {
        sanitized
    };
    let (stem, extension) = sanitized
        .rsplit_once('.')
        .filter(|(stem, extension)| !stem.is_empty() && !extension.is_empty())
        .unwrap_or((&sanitized, ""));
    let stem = truncate_filename_stem(stem, MAX_FILENAME_STEM_CHARS);
    let stem = stem.to_owned();
    let stem = if stem.is_empty() {
        "download".to_owned()
    } else {
        stem
    };
    let sanitized = if extension.is_empty() {
        stem
    } else {
        format!("{stem}.{extension}")
    };
    let reserved_stem = sanitized
        .split('.')
        .next()
        .unwrap_or_default()
        .trim_end_matches([' ', '.'])
        .to_ascii_uppercase();
    if matches!(
        reserved_stem.as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    ) {
        format!("_{sanitized}")
    } else {
        sanitized
    }
}

fn truncate_filename_stem(stem: &str, max_chars: usize) -> String {
    stem.chars().take(max_chars).collect()
}

fn video_extension(media_type: Option<&str>, path_extension: &str) -> Option<String> {
    let extension = match media_type.map(str::to_ascii_lowercase).as_deref() {
        Some("hls") => "mp4",
        Some("mp4" | "webm" | "mov" | "m4v" | "mkv" | "avi" | "ogv" | "mpeg" | "mpg" | "3gp") => {
            media_type.unwrap_or_default()
        }
        _ => path_extension,
    };
    matches!(
        extension.to_ascii_lowercase().as_str(),
        "mp4" | "webm" | "mov" | "m4v" | "mkv" | "avi" | "ogv" | "mpeg" | "mpg" | "3gp"
    )
    .then(|| extension.to_ascii_lowercase())
}

fn fallback_video_title(page_domain: Option<&str>) -> &'static str {
    let domain = page_domain
        .unwrap_or_default()
        .trim()
        .trim_start_matches("www.");
    if domain.eq_ignore_ascii_case("instagram.com")
        || domain.to_ascii_lowercase().ends_with(".instagram.com")
    {
        "Instagram_Video"
    } else if domain.eq_ignore_ascii_case("facebook.com")
        || domain.to_ascii_lowercase().ends_with(".facebook.com")
        || domain.eq_ignore_ascii_case("fb.watch")
        || domain.to_ascii_lowercase().ends_with(".fb.watch")
    {
        "Facebook_Video"
    } else {
        "Browser_Video"
    }
}

fn short_url_media_hash(parsed: &reqwest::Url) -> String {
    let mut canonical = parsed.clone();
    canonical.set_fragment(None);
    if let Some(host) = canonical.host_str().map(str::to_ascii_lowercase) {
        if host == "cdninstagram.com"
            || host.ends_with(".cdninstagram.com")
            || host == "fbcdn.net"
            || host.ends_with(".fbcdn.net")
        {
            canonical.set_query(None);
        }
    }
    let mut hash: u32 = 0x811c_9dc5;
    for byte in canonical.as_str().as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    format!("{hash:08x}")
}

fn is_generic_video_title(title: &str) -> bool {
    let trimmed = title.trim();
    let without_count = trimmed
        .strip_prefix('(')
        .and_then(|rest| rest.split_once(')'))
        .filter(|(digits, _)| !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
        .map(|(_, after)| after.trim())
        .unwrap_or(trimmed);
    matches!(
        without_count.to_ascii_lowercase().as_str(),
        "" | "instagram"
            | "instagram video"
            | "instagram reels"
            | "reels"
            | "facebook"
            | "facebook video"
    )
}

fn looks_like_url_or_media_filename(title: &str) -> bool {
    let lowercase_title = title.to_ascii_lowercase();
    lowercase_title.starts_with("http://")
        || lowercase_title.starts_with("https://")
        || lowercase_title.starts_with("www.")
        || (!title.chars().any(char::is_whitespace)
            && [
                ".mp4", ".webm", ".mov", ".m4v", ".mkv", ".avi", ".ogv", ".mpeg", ".mpg", ".3gp",
            ]
            .iter()
            .any(|extension| {
                lowercase_title
                    .split(['?', '#'])
                    .next()
                    .is_some_and(|path| path.ends_with(extension))
            }))
}

fn preferred_video_title(page_title: Option<&str>, page_domain: Option<&str>) -> String {
    let title = page_title.unwrap_or_default().trim();
    if !is_generic_video_title(title) && !looks_like_url_or_media_filename(title) {
        title.to_owned()
    } else {
        fallback_video_title(page_domain).to_owned()
    }
}

fn is_usable_preferred_video_filename(
    preferred: &str,
    path_filename: &str,
    raw_title: &str,
) -> bool {
    let trimmed = preferred.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case(path_filename) {
        return false;
    }
    if !raw_title.is_empty()
        && looks_like_url_or_media_filename(raw_title)
        && raw_title.to_ascii_lowercase().contains(&trimmed.to_ascii_lowercase())
    {
        return false;
    }
    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("http://") || lower.starts_with("https://") || lower.starts_with("www.") {
        return false;
    }
    let stem = trimmed
        .rsplit_once('.')
        .map(|(stem, _)| stem)
        .unwrap_or(trimmed);
    !is_generic_video_title(stem)
}

pub(crate) fn suggested_filename_with_metadata(
    url: &str,
    page_title: Option<&str>,
    page_domain: Option<&str>,
    media_type: Option<&str>,
    preferred_filename: Option<&str>,
) -> Result<String, String> {
    let parsed = validated_download_url(url)?;
    let path_filename = parsed
        .path_segments()
        .and_then(Iterator::last)
        .filter(|segment| !segment.is_empty())
        .unwrap_or("download");
    let path_extension = path_filename
        .rsplit_once('.')
        .map(|(_, extension)| extension)
        .unwrap_or_default();
    if let Some(extension) = video_extension(media_type, path_extension) {
        let raw_title = page_title.unwrap_or_default().trim();
        let has_meaningful_title =
            !is_generic_video_title(raw_title) && !looks_like_url_or_media_filename(raw_title);
        if has_meaningful_title {
            let title = preferred_video_title(page_title, page_domain).replace(['/', '\\'], "_");
            return Ok(sanitize_filename(&format!("{title}.{extension}")));
        }
        if let Some(preferred) = preferred_filename
            .map(str::trim)
            .filter(|name| is_usable_preferred_video_filename(name, path_filename, raw_title))
        {
            let sanitized = sanitize_filename(preferred);
            if sanitized
                .rsplit_once('.')
                .is_some_and(|(_, ext)| ext.eq_ignore_ascii_case(&extension))
            {
                return Ok(sanitized);
            }
            let stem = sanitized
                .rsplit_once('.')
                .map(|(stem, _)| stem)
                .unwrap_or(&sanitized);
            return Ok(sanitize_filename(&format!("{stem}.{extension}")));
        }
        let mut title = preferred_video_title(page_title, page_domain).replace(['/', '\\'], "_");
        let host = parsed
            .host_str()
            .map(str::to_ascii_lowercase)
            .unwrap_or_default();
        let is_social_cdn = host == "cdninstagram.com"
            || host.ends_with(".cdninstagram.com")
            || host == "fbcdn.net"
            || host.ends_with(".fbcdn.net");
        if is_social_cdn {
            title = format!("{title}_{}", short_url_media_hash(&parsed));
        }
        return Ok(sanitize_filename(&format!("{title}.{extension}")));
    }
    Ok(sanitize_filename(
        preferred_filename.unwrap_or(path_filename),
    ))
}

pub(crate) fn suggested_filename(url: &str) -> Result<String, String> {
    suggested_filename_with_metadata(url, None, None, None, None)
}

fn filename_with_suffix(filename: &str, suffix: usize) -> String {
    if suffix == 0 {
        return filename.to_owned();
    }
    let (stem, extension) = filename
        .rsplit_once('.')
        .filter(|(stem, extension)| !stem.is_empty() && !extension.is_empty())
        .unwrap_or((filename, ""));
    let (stem, previous_suffix) = stem
        .strip_suffix(')')
        .and_then(|stem| {
            let (base, number) = stem.rsplit_once(" (")?;
            Some((base, number.parse::<usize>().ok()?))
        })
        .unwrap_or((stem, 0));
    let suffix = format!(" ({})", previous_suffix.saturating_add(suffix));
    let available_stem_chars = MAX_FILENAME_STEM_CHARS.saturating_sub(suffix.chars().count());
    let stem = truncate_filename_stem(stem, available_stem_chars)
        .trim_end_matches([' ', '.'])
        .to_owned();
    if extension.is_empty() {
        format!("{stem}{suffix}")
    } else {
        format!("{stem}{suffix}.{extension}")
    }
}

fn unique_filename(
    filename: &str,
    mut is_available: impl FnMut(&str) -> bool,
) -> Result<String, String> {
    for suffix in 0..=10_000 {
        let candidate = filename_with_suffix(filename, suffix);
        if is_available(&candidate) {
            return Ok(candidate);
        }
    }
    Err("Could not choose an unused filename; select a different destination.".into())
}

fn destination_in_use(directory: &Path, filename: &str, entries: &[DownloadEntry]) -> bool {
    let destination = directory.join(filename);
    let destination_string = destination.to_string_lossy();
    destination.exists()
        || partial_path(&destination).exists()
        || entries.iter().any(|entry| {
            matches!(
                entry.status,
                DownloadStatus::Queued | DownloadStatus::Downloading | DownloadStatus::Paused
            ) && entry
                .destination
                .eq_ignore_ascii_case(destination_string.as_ref())
        })
}

fn unique_destination(destination: &Path, entries: &[DownloadEntry]) -> Result<PathBuf, String> {
    let parent = destination.parent().ok_or_else(|| {
        "Filesystem error: the destination must be inside an existing folder.".to_owned()
    })?;
    let filename = destination
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            "Filesystem error: choose a destination with a valid filename.".to_owned()
        })?;
    let filename = sanitize_filename(filename);
    unique_filename(&filename, |candidate| {
        !destination_in_use(parent, candidate, entries)
    })
    .map(|filename| parent.join(filename))
}

fn validated_download_url(url: &str) -> Result<reqwest::Url, String> {
    let parsed =
        reqwest::Url::parse(url).map_err(|error| format!("Invalid download URL: {error}"))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || parsed.username() != ""
        || parsed.password().is_some()
    {
        return Err("Enter a valid HTTP or HTTPS URL without embedded credentials.".into());
    }
    Ok(parsed)
}

#[cfg(test)]
fn validate_unranged_media_response(
    status: StatusCode,
    content_range: Option<&str>,
    content_length: Option<u64>,
) -> Result<(), String> {
    if status == StatusCode::PARTIAL_CONTENT {
        let complete_representation =
            content_range
                .and_then(parse_content_range)
                .is_some_and(|(start, end, total)| {
                    start == 0 && end.saturating_add(1) == total && content_length == Some(total)
                });
        if complete_representation {
            return Ok(());
        }
        return Err(
            "The server returned only a partial byte-range response. FluxNet cannot save it as a complete media file; choose a complete source instead."
                .into(),
        );
    }
    if content_range.is_some() {
        return Err(
            "The server returned only a partial byte-range response. FluxNet cannot save it as a complete media file; choose a complete source instead."
                .into(),
        );
    }
    Ok(())
}

pub(crate) fn is_instagram_cdn_url(url: &reqwest::Url) -> bool {
    let Some(host) = url.host_str().map(str::to_ascii_lowercase) else {
        return false;
    };
    host == "cdninstagram.com"
        || host.ends_with(".cdninstagram.com")
        || host == "fbcdn.net"
        || host.ends_with(".fbcdn.net")
}

fn decode_base64_bytes(input: &str) -> Option<Vec<u8>> {
    let trimmed = input.trim().trim_end_matches('=');
    if trimmed.is_empty() {
        return None;
    }
    let mut sextets = Vec::with_capacity(trimmed.len());
    for byte in trimmed.bytes() {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' | b'-' | b' ' => 62,
            b'/' | b'_' => 63,
            _ => return None,
        };
        sextets.push(value);
    }
    if sextets.len() % 4 == 1 {
        return None;
    }
    let mut output = Vec::with_capacity(sextets.len() * 3 / 4);
    for chunk in sextets.chunks(4) {
        let b0 = (chunk[0] << 2) | (chunk[1] >> 4);
        output.push(b0);
        if chunk.len() > 2 {
            let b1 = (chunk[1] << 4) | (chunk[2] >> 2);
            output.push(b1);
        }
        if chunk.len() > 3 {
            let b2 = (chunk[2] << 6) | chunk[3];
            output.push(b2);
        }
    }
    Some(output)
}

fn decode_instagram_vencode_tag(url: &reqwest::Url) -> Option<String> {
    let efg_value = url
        .query_pairs()
        .find_map(|(name, value)| name.eq_ignore_ascii_case("efg").then(|| value.into_owned()))?;
    let decoded = decode_base64_bytes(&efg_value)?;
    let json = serde_json::from_slice::<serde_json::Value>(&decoded).ok()?;
    json.get("vencode_tag")
        .and_then(serde_json::Value::as_str)
        .map(str::to_ascii_lowercase)
}

pub(crate) fn is_instagram_audio_only_url(url: &reqwest::Url) -> bool {
    if !is_instagram_cdn_url(url) {
        return false;
    }
    let is_m78_audio_path = url.path().to_ascii_lowercase().contains("/m78/");
    if let Some(tag) = decode_instagram_vencode_tag(url) {
        let has_video_marker = [
            "vp9", "vp8", "vp09", "vp08", "avc1", "avc3", "h264", "hev1", "hvc1", "hevc", "av01",
            "progressive", "144p", "240p", "360p", "480p", "540p", "720p", "1080p", "1440p",
            "2160p", "_q90", "_q80", "_q70", "_q60", "_q50", "_q40", "_q30",
        ]
        .iter()
        .any(|marker| tag.contains(marker));
        let has_audio_marker = tag == "audio"
            || [
                "_audio",
                ".audio",
                "-audio",
                "audio_",
                "audio.",
                "audio-",
                "audio_only",
                "audioonly",
                "heaac",
                "aac",
                "opus",
                "mp4a",
                "vorbis",
            ]
            .iter()
            .any(|marker| tag.contains(marker));
        if has_audio_marker && !has_video_marker {
            return true;
        }
        if has_video_marker && !has_audio_marker && !is_m78_audio_path {
            return false;
        }
    }
    is_m78_audio_path
}

#[allow(dead_code)]
pub(crate) fn is_instagram_byte_range_url(url: &reqwest::Url) -> bool {
    is_instagram_cdn_url(url)
        && url.query_pairs().any(|(name, _)| {
            name.eq_ignore_ascii_case("bytestart") || name.eq_ignore_ascii_case("byteend")
        })
}

pub(crate) fn normalize_instagram_media_url(url: &reqwest::Url) -> Result<reqwest::Url, String> {
    if !is_instagram_cdn_url(url) {
        return Ok(url.clone());
    }
    if is_instagram_audio_only_url(url) {
        return Err(
            "Instagram supplied an audio-only DASH track, not a complete video source.".into(),
        );
    }
    let mut byte_start: Option<u64> = None;
    let mut byte_end: Option<u64> = None;
    let mut has_range_param = false;
    for (name, value) in url.query_pairs() {
        if name.eq_ignore_ascii_case("bytestart") {
            has_range_param = true;
            if byte_start.is_some() {
                return Err("Instagram supplied duplicate bytestart parameters.".into());
            }
            byte_start = Some(
                value
                    .parse::<u64>()
                    .map_err(|_| "Instagram supplied a malformed bytestart parameter.".to_owned())?,
            );
        } else if name.eq_ignore_ascii_case("byteend") {
            has_range_param = true;
            if byte_end.is_some() {
                return Err("Instagram supplied duplicate byteend parameters.".into());
            }
            byte_end = Some(
                value
                    .parse::<u64>()
                    .map_err(|_| "Instagram supplied a malformed byteend parameter.".to_owned())?,
            );
        }
    }
    if !has_range_param {
        return Ok(url.clone());
    }
    if let (Some(start), Some(end)) = (byte_start, byte_end) {
        if start > end {
            return Err("Instagram supplied an invalid byte range (bytestart > byteend).".into());
        }
    }
    let mut normalized = url.clone();
    if let Some(raw_query) = url.query() {
        let preserved: Vec<&str> = raw_query
            .split('&')
            .filter(|segment| !segment.is_empty())
            .filter(|segment| {
                let raw_key = segment.split('=').next().unwrap_or_default();
                !raw_key.eq_ignore_ascii_case("bytestart")
                    && !raw_key.eq_ignore_ascii_case("byteend")
            })
            .collect();
        if preserved.is_empty() {
            normalized.set_query(None);
        } else {
            let joined = preserved.join("&");
            normalized.set_query(Some(&joined));
        }
    }
    Ok(normalized)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RequestFailureKind {
    Dns,
    Tls,
    Timeout,
    Connection,
    Redirect,
    Request,
}

impl RequestFailureKind {
    fn label(self) -> &'static str {
        match self {
            Self::Dns => "DNS resolution error",
            Self::Tls => "TLS/certificate error",
            Self::Timeout => "Request timed out",
            Self::Connection => "Connection error",
            Self::Redirect => "Redirect error",
            Self::Request => "HTTP request error",
        }
    }
}

fn classify_request_failure(
    is_timeout: bool,
    is_connect: bool,
    is_redirect: bool,
    source_chain: &str,
) -> RequestFailureKind {
    let chain = source_chain.to_ascii_lowercase();
    if [
        "dns error",
        "failed to lookup",
        "failed to resolve",
        "name or service not known",
        "no such host",
        "host not found",
        "name resolution",
        "wshost_not_found",
        "11001",
    ]
    .iter()
    .any(|needle| chain.contains(needle))
    {
        return RequestFailureKind::Dns;
    }
    if [
        "certificate",
        "cert error",
        "tls",
        "rustls",
        "handshake",
        "unknown issuer",
        "invalid peer",
    ]
    .iter()
    .any(|needle| chain.contains(needle))
    {
        return RequestFailureKind::Tls;
    }
    if is_timeout || chain.contains("timed out") || chain.contains("timeout") {
        return RequestFailureKind::Timeout;
    }
    if is_redirect {
        return RequestFailureKind::Redirect;
    }
    if is_connect {
        return RequestFailureKind::Connection;
    }
    RequestFailureKind::Request
}

fn safe_request_error_detail(value: &str, request_url: Option<&reqwest::Url>) -> String {
    let mut detail = value.to_owned();
    if let Some(url) = request_url {
        detail = detail.replace(url.as_str(), "requested URL");
        if let Some(query) = url.query().filter(|query| !query.is_empty()) {
            detail = detail.replace(query, "[query redacted]");
        }
    }
    detail
        .chars()
        .filter(|character| !character.is_control())
        .take(240)
        .collect::<String>()
}

fn reqwest_source_chain(error: &reqwest::Error, requested_url: &str) -> String {
    let parsed_url = reqwest::Url::parse(requested_url).ok();
    let request_url = parsed_url.as_ref().or_else(|| error.url());
    let mut parts = vec!["request failed".to_owned()];
    let mut source = error.source();
    while let Some(cause) = source {
        let detail = safe_request_error_detail(&cause.to_string(), request_url);
        if !detail.is_empty() && parts.last() != Some(&detail) {
            parts.push(detail);
        }
        source = cause.source();
    }
    parts.join(" → ")
}

fn describe_reqwest_error(error: &reqwest::Error, requested_url: &str) -> String {
    let source_chain = reqwest_source_chain(error, requested_url);
    let kind = classify_request_failure(
        error.is_timeout(),
        error.is_connect(),
        error.is_redirect(),
        &source_chain,
    );
    format!("{}: {source_chain}", kind.label())
}

fn filesystem_error(context: &str, error: impl std::fmt::Display) -> String {
    format!("Filesystem error: {context}: {error}")
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Mp4Validation {
    Standalone,
    FragmentedWithInitialization,
    FragmentedMissingInitialization,
    Invalid(String),
}

#[derive(Clone, Copy)]
struct Mp4Atom {
    kind: [u8; 4],
    payload_start: u64,
    end: u64,
}

fn read_mp4_atoms<R: Read + Seek>(
    file: &mut R,
    start: u64,
    end: u64,
) -> Result<Vec<Mp4Atom>, String> {
    let mut atoms = Vec::new();
    let mut position = start;
    while position < end {
        if atoms.len() >= MAX_MP4_ATOM_COUNT {
            return Err("The MP4 contains too many boxes to validate safely.".into());
        }
        let remaining = end - position;
        if remaining < 8 {
            return Err("The MP4 ends with a truncated atom header.".into());
        }
        file.seek(SeekFrom::Start(position))
            .map_err(|error| filesystem_error("could not inspect MP4 atoms", error))?;
        let mut header = [0_u8; 16];
        file.read_exact(&mut header[..8]).map_err(|error| {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                "The MP4 ends with a truncated atom header.".to_owned()
            } else {
                filesystem_error("could not read MP4 atoms", error)
            }
        })?;
        let size32 = u32::from_be_bytes(header[..4].try_into().unwrap());
        let kind: [u8; 4] = header[4..8].try_into().unwrap();
        let mut header_size = 8_u64;
        let atom_size = match size32 {
            0 => remaining,
            1 => {
                if remaining < 16 {
                    return Err("The MP4 ends with a truncated extended-size atom header.".into());
                }
                file.read_exact(&mut header[8..16]).map_err(|error| {
                    if error.kind() == std::io::ErrorKind::UnexpectedEof {
                        "The MP4 ends with a truncated extended-size atom header.".to_owned()
                    } else {
                        filesystem_error("could not read MP4 atoms", error)
                    }
                })?;
                header_size = 16;
                u64::from_be_bytes(header[8..16].try_into().unwrap())
            }
            size => u64::from(size),
        };
        if kind == *b"uuid" {
            header_size = header_size.saturating_add(16);
        }
        if atom_size < header_size {
            return Err("The MP4 contains an atom smaller than its header.".into());
        }
        if atom_size > remaining {
            return Err("The MP4 contains an atom that extends beyond the downloaded file.".into());
        }
        let atom_end = position
            .checked_add(atom_size)
            .ok_or_else(|| "The MP4 contains an atom with an invalid size.".to_owned())?;
        atoms.push(Mp4Atom {
            kind,
            payload_start: position + header_size,
            end: atom_end,
        });
        position = atom_end;
    }
    Ok(atoms)
}

fn has_mp4_atom(atoms: &[Mp4Atom], kind: &[u8; 4]) -> bool {
    atoms.iter().any(|atom| &atom.kind == kind)
}

fn has_mp4_atom_with_payload(atoms: &[Mp4Atom], kind: &[u8; 4], minimum: u64) -> bool {
    atoms
        .iter()
        .any(|atom| &atom.kind == kind && atom.end - atom.payload_start >= minimum)
}

fn mp4_atom_children<R: Read + Seek>(file: &mut R, atom: Mp4Atom) -> Result<Vec<Mp4Atom>, String> {
    read_mp4_atoms(file, atom.payload_start, atom.end)
}

fn validate_mp4_initialization<R: Read + Seek>(
    file: &mut R,
    moov: Mp4Atom,
    fragmented: bool,
) -> Result<(), String> {
    let moov_children = mp4_atom_children(file, moov)
        .map_err(|error| format!("The MP4 initialization metadata is invalid: {error}"))?;
    if !has_mp4_atom_with_payload(&moov_children, b"mvhd", 4) {
        return Err("The MP4 initialization metadata is missing its movie header.".into());
    }
    let tracks: Vec<_> = moov_children
        .iter()
        .copied()
        .filter(|atom| &atom.kind == b"trak")
        .collect();
    if tracks.is_empty() {
        return Err("The MP4 initialization metadata does not contain a media track.".into());
    }
    if fragmented {
        let movie_extends = moov_children
            .iter()
            .copied()
            .find(|atom| &atom.kind == b"mvex")
            .ok_or_else(|| {
                "The fragmented MP4 initialization metadata is missing its track defaults."
                    .to_owned()
            })?;
        let extends_children = mp4_atom_children(file, movie_extends)
            .map_err(|error| format!("The MP4 track defaults are invalid: {error}"))?;
        if !has_mp4_atom_with_payload(&extends_children, b"trex", 8) {
            return Err(
                "The fragmented MP4 initialization metadata is missing its track defaults.".into(),
            );
        }
    }

    for track in tracks {
        let track_children = mp4_atom_children(file, track)
            .map_err(|error| format!("The MP4 track metadata is invalid: {error}"))?;
        let media = track_children
            .iter()
            .copied()
            .find(|atom| &atom.kind == b"mdia")
            .ok_or_else(|| "The MP4 track is missing its media metadata.".to_owned())?;
        let media_children = mp4_atom_children(file, media)
            .map_err(|error| format!("The MP4 media metadata is invalid: {error}"))?;
        if !has_mp4_atom_with_payload(&media_children, b"mdhd", 4)
            || !has_mp4_atom_with_payload(&media_children, b"hdlr", 4)
        {
            return Err("The MP4 track is missing required media metadata.".into());
        }
        let media_info = media_children
            .iter()
            .copied()
            .find(|atom| &atom.kind == b"minf")
            .ok_or_else(|| "The MP4 track is missing its media information.".to_owned())?;
        let info_children = mp4_atom_children(file, media_info)
            .map_err(|error| format!("The MP4 media information is invalid: {error}"))?;
        let sample_table = info_children
            .iter()
            .copied()
            .find(|atom| &atom.kind == b"stbl")
            .ok_or_else(|| "The MP4 track is missing its sample table.".to_owned())?;
        let sample_children = mp4_atom_children(file, sample_table)
            .map_err(|error| format!("The MP4 sample table is invalid: {error}"))?;
        if !has_mp4_atom_with_payload(&sample_children, b"stsd", 8) {
            return Err("The MP4 track is missing its sample descriptions.".into());
        }
        if !fragmented
            && (!has_mp4_atom(&sample_children, b"stts")
                || !has_mp4_atom(&sample_children, b"stsc")
                || (!has_mp4_atom(&sample_children, b"stsz")
                    && !has_mp4_atom(&sample_children, b"stz2"))
                || (!has_mp4_atom(&sample_children, b"stco")
                    && !has_mp4_atom(&sample_children, b"co64")))
        {
            return Err("The MP4 track is missing required sample-index tables.".into());
        }
    }
    Ok(())
}

fn validate_mp4_fragments<R: Read + Seek>(file: &mut R, atoms: &[Mp4Atom]) -> Result<(), String> {
    for moof in atoms.iter().copied().filter(|atom| &atom.kind == b"moof") {
        let children = mp4_atom_children(file, moof)
            .map_err(|error| format!("The MP4 movie fragment is invalid: {error}"))?;
        if !has_mp4_atom_with_payload(&children, b"mfhd", 8) {
            return Err("The fragmented MP4 is missing a valid fragment header.".into());
        }
        let track_fragments: Vec<_> = children
            .iter()
            .copied()
            .filter(|atom| &atom.kind == b"traf")
            .collect();
        if track_fragments.is_empty() {
            return Err("The fragmented MP4 does not contain a track fragment.".into());
        }
        for traf in track_fragments {
            let track_fragment = mp4_atom_children(file, traf)
                .map_err(|error| format!("The MP4 track fragment is invalid: {error}"))?;
            if !has_mp4_atom_with_payload(&track_fragment, b"tfhd", 8)
                || !has_mp4_atom_with_payload(&track_fragment, b"trun", 8)
            {
                return Err("The fragmented MP4 contains an incomplete track fragment.".into());
            }
        }
    }
    Ok(())
}

fn validate_mp4_file(path: &Path) -> Result<Mp4Validation, String> {
    let mut file = std::fs::File::open(path).map_err(|error| {
        filesystem_error("could not open the downloaded MP4 for validation", error)
    })?;
    let file_length = file
        .metadata()
        .map_err(|error| filesystem_error("could not inspect the downloaded MP4", error))?
        .len();
    validate_mp4_reader(&mut file, file_length)
}

pub(crate) fn validate_ytdlp_mp4(path: &Path) -> Result<(), String> {
    match validate_mp4_file(path)? {
        Mp4Validation::Standalone | Mp4Validation::FragmentedWithInitialization => {
            let (has_video, has_audio) = mp4_track_kinds(path)?;
            if has_video && has_audio {
                Ok(())
            } else {
                Err("The downloaded MP4 does not contain both audio and video tracks.".into())
            }
        }
        Mp4Validation::FragmentedMissingInitialization => {
            Err("The downloaded fragmented MP4 is missing its initialization data.".into())
        }
        Mp4Validation::Invalid(_) => Err("The downloaded MP4 is incomplete or invalid.".into()),
    }
}

pub(crate) fn validate_ytdlp_media(path: &Path) -> Result<(), String> {
    match path
        .extension()
        .and_then(|value| value.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("mp4") => validate_ytdlp_mp4(path),
        Some("webm") => validate_ytdlp_webm(path),
        _ => Err("The downloaded media container is unsupported.".into()),
    }
}

#[derive(Clone, Copy)]
struct EbmlElement {
    id: u64,
    payload_start: u64,
    end: u64,
}

const EBML_HEADER_ID: u64 = 0x1A45_DFA3;
const EBML_SEGMENT_ID: u64 = 0x1853_8067;
const EBML_DOC_TYPE_ID: u64 = 0x4282;
const EBML_INFO_ID: u64 = 0x1549_A966;
const EBML_TRACKS_ID: u64 = 0x1654_AE6B;
const EBML_TRACK_ENTRY_ID: u64 = 0xAE;
const EBML_TRACK_TYPE_ID: u64 = 0x83;
const EBML_CLUSTER_ID: u64 = 0x1F43_B675;
const EBML_SIMPLE_BLOCK_ID: u64 = 0xA3;
const EBML_BLOCK_GROUP_ID: u64 = 0xA0;
const EBML_BLOCK_ID: u64 = 0xA1;
const MAX_EBML_ELEMENTS: usize = 100_000;

fn validate_ytdlp_webm(path: &Path) -> Result<(), String> {
    let mut file = std::fs::File::open(path)
        .map_err(|error| filesystem_error("could not inspect the downloaded WebM", error))?;
    let length = file
        .metadata()
        .map_err(|error| filesystem_error("could not inspect the downloaded WebM", error))?
        .len();
    if length < 16 {
        return Err("The downloaded WebM is too small to contain a media segment.".into());
    }
    let header = read_ebml_element(&mut file, 0, length)?;
    if header.id != EBML_HEADER_ID {
        return Err("The downloaded file is not an EBML/WebM container.".into());
    }
    let header_children = read_ebml_children(&mut file, header.payload_start, header.end)?;
    let is_webm = header_children.iter().any(|element| {
        if element.id != EBML_DOC_TYPE_ID || element.end - element.payload_start > 16 {
            return false;
        }
        let size = (element.end - element.payload_start) as usize;
        let mut bytes = vec![0; size];
        file.seek(SeekFrom::Start(element.payload_start))
            .and_then(|_| file.read_exact(&mut bytes))
            .is_ok()
            && bytes == b"webm"
    });
    if !is_webm {
        return Err("The downloaded EBML file does not declare the WebM document type.".into());
    }

    let mut position = header.end;
    let mut segment = None;
    while position < length {
        let element = read_ebml_element(&mut file, position, length)?;
        if element.id == EBML_SEGMENT_ID {
            segment = Some(element);
            break;
        }
        position = element.end;
    }
    let segment = segment.ok_or_else(|| "The downloaded WebM has no segment.".to_owned())?;
    let segment_children = read_ebml_children(&mut file, segment.payload_start, segment.end)?;
    if !segment_children
        .iter()
        .any(|element| element.id == EBML_INFO_ID)
    {
        return Err("The downloaded WebM has no segment information.".into());
    }
    let tracks = segment_children
        .iter()
        .copied()
        .find(|element| element.id == EBML_TRACKS_ID)
        .ok_or_else(|| "The downloaded WebM has no track metadata.".to_owned())?;
    let track_entries = read_ebml_children(&mut file, tracks.payload_start, tracks.end)?;
    let mut has_video = false;
    let mut has_audio = false;
    for track in track_entries
        .into_iter()
        .filter(|element| element.id == EBML_TRACK_ENTRY_ID)
    {
        for field in read_ebml_children(&mut file, track.payload_start, track.end)?
            .into_iter()
            .filter(|element| element.id == EBML_TRACK_TYPE_ID)
        {
            let byte_count = field.end.saturating_sub(field.payload_start);
            if byte_count == 0 || byte_count > 8 {
                return Err("The downloaded WebM contains an invalid track type.".into());
            }
            let mut bytes = vec![0; byte_count as usize];
            file.seek(SeekFrom::Start(field.payload_start))
                .and_then(|_| file.read_exact(&mut bytes))
                .map_err(|error| filesystem_error("could not inspect WebM tracks", error))?;
            let kind = bytes
                .iter()
                .fold(0u64, |value, byte| (value << 8) | u64::from(*byte));
            has_video |= kind == 1;
            has_audio |= kind == 2;
        }
    }
    if !has_video || !has_audio {
        return Err("The downloaded WebM does not contain both audio and video tracks.".into());
    }

    let mut has_media_block = false;
    for cluster in segment_children
        .iter()
        .copied()
        .filter(|element| element.id == EBML_CLUSTER_ID)
    {
        for child in read_ebml_children(&mut file, cluster.payload_start, cluster.end)? {
            if child.id == EBML_SIMPLE_BLOCK_ID && child.end - child.payload_start > 4 {
                has_media_block = true;
            }
            if child.id == EBML_BLOCK_GROUP_ID {
                has_media_block |= read_ebml_children(&mut file, child.payload_start, child.end)?
                    .iter()
                    .any(|block| block.id == EBML_BLOCK_ID && block.end - block.payload_start > 4);
            }
        }
    }
    if !has_media_block {
        return Err("The downloaded WebM has no nonempty audio/video block.".into());
    }
    Ok(())
}

fn read_ebml_vint<R: Read + Seek>(
    file: &mut R,
    position: u64,
    parent_end: u64,
    id: bool,
) -> Result<(u64, usize, bool), String> {
    if position >= parent_end {
        return Err("The downloaded WebM contains a truncated element.".into());
    }
    let mut first = [0u8; 1];
    file.seek(SeekFrom::Start(position))
        .and_then(|_| file.read_exact(&mut first))
        .map_err(|error| filesystem_error("could not inspect WebM elements", error))?;
    if first[0] == 0 {
        return Err("The downloaded WebM contains an invalid variable-length integer.".into());
    }
    let length = first[0].leading_zeros() as usize + 1;
    let maximum = if id { 4 } else { 8 };
    if length > maximum || position.saturating_add(length as u64) > parent_end {
        return Err("The downloaded WebM contains an invalid element header.".into());
    }
    let mut bytes = vec![0; length];
    file.seek(SeekFrom::Start(position))
        .and_then(|_| file.read_exact(&mut bytes))
        .map_err(|error| filesystem_error("could not inspect WebM elements", error))?;
    let mut value = u64::from(bytes[0]);
    if !id {
        let marker_mask = if length == 8 { 0 } else { 0xFFu8 >> length };
        value &= u64::from(marker_mask);
    }
    for byte in bytes.iter().skip(1) {
        value = (value << 8) | u64::from(*byte);
    }
    let unknown = !id && value == ((1u64 << (7 * length)) - 1);
    Ok((value, length, unknown))
}

fn read_ebml_element<R: Read + Seek>(
    file: &mut R,
    position: u64,
    parent_end: u64,
) -> Result<EbmlElement, String> {
    let (id, id_length, _) = read_ebml_vint(file, position, parent_end, true)?;
    let (size, size_length, unknown) =
        read_ebml_vint(file, position + id_length as u64, parent_end, false)?;
    let payload_start = position + (id_length + size_length) as u64;
    let end = if unknown {
        parent_end
    } else {
        payload_start
            .checked_add(size)
            .filter(|end| *end <= parent_end)
            .ok_or_else(|| {
                "The downloaded WebM contains an element past the file end.".to_owned()
            })?
    };
    Ok(EbmlElement {
        id,
        payload_start,
        end,
    })
}

fn read_ebml_children<R: Read + Seek>(
    file: &mut R,
    start: u64,
    end: u64,
) -> Result<Vec<EbmlElement>, String> {
    let mut children = Vec::new();
    let mut position = start;
    while position < end {
        if children.len() >= MAX_EBML_ELEMENTS {
            return Err("The downloaded WebM contains too many elements.".into());
        }
        let element = read_ebml_element(file, position, end)?;
        if element.end <= position {
            return Err("The downloaded WebM contains an invalid element size.".into());
        }
        position = element.end;
        children.push(element);
    }
    Ok(children)
}

fn mp4_track_kinds(path: &Path) -> Result<(bool, bool), String> {
    let mut file = std::fs::File::open(path)
        .map_err(|error| filesystem_error("could not inspect downloaded MP4 tracks", error))?;
    let length = file
        .metadata()
        .map_err(|error| filesystem_error("could not inspect downloaded MP4 tracks", error))?
        .len();
    let top = read_mp4_atoms(&mut file, 0, length)?;
    let moov = top
        .iter()
        .copied()
        .find(|atom| &atom.kind == b"moov")
        .ok_or_else(|| "The downloaded MP4 has no movie metadata.".to_owned())?;
    let children = mp4_atom_children(&mut file, moov)?;
    let mut has_video = false;
    let mut has_audio = false;
    for track in children
        .iter()
        .copied()
        .filter(|atom| &atom.kind == b"trak")
    {
        let track_children = mp4_atom_children(&mut file, track)?;
        let media = track_children
            .iter()
            .copied()
            .find(|atom| &atom.kind == b"mdia")
            .ok_or_else(|| "The downloaded MP4 track has no media metadata.".to_owned())?;
        let media_children = mp4_atom_children(&mut file, media)?;
        let handler = media_children
            .iter()
            .copied()
            .find(|atom| &atom.kind == b"hdlr")
            .ok_or_else(|| "The downloaded MP4 track has no handler metadata.".to_owned())?;
        if handler.end - handler.payload_start < 12 {
            return Err("The downloaded MP4 track handler is truncated.".into());
        }
        let mut payload = [0u8; 12];
        file.seek(SeekFrom::Start(handler.payload_start))
            .and_then(|_| file.read_exact(&mut payload))
            .map_err(|error| filesystem_error("could not inspect downloaded MP4 tracks", error))?;
        match &payload[8..12] {
            b"vide" => has_video = true,
            b"soun" => has_audio = true,
            _ => {}
        }
    }
    Ok((has_video, has_audio))
}

fn parse_mp4_sidx_expected_end<R: Read + Seek>(
    file: &mut R,
    sidx: Mp4Atom,
) -> Result<u64, String> {
    let payload_len = sidx.end.saturating_sub(sidx.payload_start);
    if payload_len < 24 {
        return Err("The MP4 segment index (sidx) box is truncated.".into());
    }
    if payload_len > 1_048_576 {
        return Err("The MP4 segment index (sidx) box exceeds the 1 MiB safety limit.".into());
    }
    let mut payload = vec![0_u8; payload_len as usize];
    file.seek(SeekFrom::Start(sidx.payload_start))
        .and_then(|_| file.read_exact(&mut payload))
        .map_err(|error| filesystem_error("could not read MP4 segment index (sidx)", error))?;
    let version = payload[0];
    let (first_offset, reference_count, entries_start) = match version {
        0 => {
            let first_offset = u64::from(u32::from_be_bytes(payload[16..20].try_into().unwrap()));
            let reference_count = usize::from(u16::from_be_bytes(payload[22..24].try_into().unwrap()));
            (first_offset, reference_count, 24_usize)
        }
        1 => {
            if payload.len() < 32 {
                return Err("The MP4 v1 segment index (sidx) box is truncated.".into());
            }
            let first_offset = u64::from_be_bytes(payload[20..28].try_into().unwrap());
            let reference_count = usize::from(u16::from_be_bytes(payload[30..32].try_into().unwrap()));
            (first_offset, reference_count, 32_usize)
        }
        _ => {
            return Err("The MP4 segment index (sidx) uses an unsupported version.".into());
        }
    };
    let required_len = entries_start
        .checked_add(reference_count.checked_mul(12).ok_or_else(|| {
            "The MP4 segment index (sidx) reference count overflows.".to_owned()
        })?)
        .ok_or_else(|| "The MP4 segment index (sidx) size overflows.".to_owned())?;
    if payload.len() < required_len {
        return Err("The MP4 segment index (sidx) entries are truncated.".into());
    }
    let mut total_referenced_size = 0_u64;
    for index in 0..reference_count {
        let entry_offset = entries_start + index * 12;
        let word0 = u32::from_be_bytes(payload[entry_offset..entry_offset + 4].try_into().unwrap());
        let referenced_size = u64::from(word0 & 0x7FFF_FFFF);
        total_referenced_size = total_referenced_size
            .checked_add(referenced_size)
            .ok_or_else(|| "The MP4 segment index (sidx) byte total overflows.".to_owned())?;
    }
    sidx.end
        .checked_add(first_offset)
        .and_then(|anchor| anchor.checked_add(total_referenced_size))
        .ok_or_else(|| "The MP4 segment index (sidx) extent overflows.".to_owned())
}

fn inspect_mp4_sidx_expected_bytes(path: &Path) -> Result<Option<u64>, String> {
    let mut file = std::fs::File::open(path)
        .map_err(|error| filesystem_error("could not inspect MP4 segment index", error))?;
    let file_length = file
        .metadata()
        .map_err(|error| filesystem_error("could not inspect MP4 segment index", error))?
        .len();
    if file_length < 8 {
        return Ok(None);
    }
    let Ok(atoms) = read_mp4_atoms(&mut file, 0, file_length) else {
        return Ok(None);
    };
    if !has_mp4_atom(&atoms, b"ftyp") || !has_mp4_atom(&atoms, b"moov") {
        return Ok(None);
    }
    let mut max_expected_end: Option<u64> = None;
    for sidx in atoms.into_iter().filter(|atom| &atom.kind == b"sidx") {
        let expected_end = parse_mp4_sidx_expected_end(&mut file, sidx)?;
        max_expected_end = Some(max_expected_end.map_or(expected_end, |prev| prev.max(expected_end)));
    }
    Ok(max_expected_end)
}

fn validate_mp4_reader<R: Read + Seek>(
    mut file: &mut R,
    file_length: u64,
) -> Result<Mp4Validation, String> {
    if file_length < 8 {
        return Ok(Mp4Validation::Invalid(
            "The downloaded file is too small to contain an MP4 atom.".into(),
        ));
    }
    let prefix_length = usize::try_from(file_length.min(512)).unwrap_or(512);
    let mut prefix = vec![0_u8; prefix_length];
    file.seek(SeekFrom::Start(0))
        .and_then(|_| file.read_exact(&mut prefix))
        .map_err(|error| filesystem_error("could not inspect the downloaded MP4", error))?;
    let text_prefix = prefix
        .strip_prefix(&[0xEF, 0xBB, 0xBF])
        .unwrap_or(&prefix)
        .iter()
        .copied()
        .skip_while(u8::is_ascii_whitespace)
        .collect::<Vec<_>>();
    if text_prefix.starts_with(b"<")
        || text_prefix.starts_with(b"{")
        || text_prefix.starts_with(b"[")
    {
        return Ok(Mp4Validation::Invalid(
            "The server returned a text, HTML, or JSON response instead of MP4 media.".into(),
        ));
    }

    let atoms = match read_mp4_atoms(&mut file, 0, file_length) {
        Ok(atoms) => atoms,
        Err(error) => return Ok(Mp4Validation::Invalid(error)),
    };
    for sidx in atoms.iter().copied().filter(|atom| &atom.kind == b"sidx") {
        let expected_end = match parse_mp4_sidx_expected_end(&mut file, sidx) {
            Ok(expected_end) => expected_end,
            Err(error) => return Ok(Mp4Validation::Invalid(error)),
        };
        if file_length < expected_end {
            return Ok(Mp4Validation::Invalid(format!(
                "The fragmented MP4 is incomplete: downloaded {file_length} bytes, but its segment index (sidx) declares {expected_end} bytes."
            )));
        }
    }
    let moof_count = atoms.iter().filter(|atom| &atom.kind == b"moof").count();
    if !atoms.iter().any(|atom| &atom.kind == b"mdat")
        || atoms
            .iter()
            .any(|atom| &atom.kind == b"mdat" && atom.payload_start == atom.end)
    {
        return Ok(Mp4Validation::Invalid(
            "The MP4 does not contain media data.".into(),
        ));
    }
    let mut fragment_has_media_data = None;
    for atom in &atoms {
        if &atom.kind == b"moof" {
            if fragment_has_media_data == Some(false) {
                return Ok(Mp4Validation::Invalid(
                    "The fragmented MP4 is incomplete: a media fragment has no following media data.".into(),
                ));
            }
            fragment_has_media_data = Some(false);
        } else if &atom.kind == b"mdat" && fragment_has_media_data.is_some() {
            fragment_has_media_data = Some(true);
        }
    }
    if fragment_has_media_data == Some(false) {
        return Ok(Mp4Validation::Invalid(
            "The fragmented MP4 is incomplete: a media fragment has no following media data."
                .into(),
        ));
    }
    if let Err(error) = validate_mp4_fragments(&mut file, &atoms) {
        return Ok(Mp4Validation::Invalid(error));
    }

    let ftyp = atoms.iter().find(|atom| &atom.kind == b"ftyp").copied();
    let moov = atoms.iter().find(|atom| &atom.kind == b"moov").copied();
    if moof_count > 0 && (ftyp.is_none() || moov.is_none()) {
        return Ok(Mp4Validation::FragmentedMissingInitialization);
    }
    let (Some(ftyp), Some(moov)) = (ftyp, moov) else {
        return Ok(Mp4Validation::Invalid(
            "The MP4 is missing its file-type or movie initialization atom.".into(),
        ));
    };
    if ftyp.end - ftyp.payload_start < 8 {
        return Ok(Mp4Validation::Invalid(
            "The MP4 file-type atom is incomplete.".into(),
        ));
    }
    if let Err(error) = validate_mp4_initialization(&mut file, moov, moof_count > 0) {
        return Ok(Mp4Validation::Invalid(error));
    }
    if moof_count > 0 {
        Ok(Mp4Validation::FragmentedWithInitialization)
    } else {
        Ok(Mp4Validation::Standalone)
    }
}

fn media_format_hint(value: &str) -> Option<&'static str> {
    match value
        .rsplit_once('.')
        .map(|(_, extension)| extension)?
        .to_ascii_lowercase()
        .as_str()
    {
        "mp4" => Some("mp4"),
        "webm" => Some("webm"),
        _ => None,
    }
}

fn has_mp4_signature(path: &Path) -> Result<bool, String> {
    let mut file = std::fs::File::open(path)
        .map_err(|error| filesystem_error("could not inspect the downloaded file type", error))?;
    let mut header = [0_u8; 8];
    match file.read_exact(&mut header) {
        Ok(()) => Ok(matches!(&header[4..8], b"ftyp" | b"moof" | b"styp")),
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => Err(filesystem_error(
            "could not inspect the downloaded file type",
            error,
        )),
    }
}

fn validate_mp4_completion(
    file_path: &Path,
    destination_path: &Path,
    url: &str,
    content_type: Option<&str>,
) -> Result<(), String> {
    let destination_format = destination_path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase);
    let url_format = reqwest::Url::parse(url)
        .ok()
        .and_then(|url| {
            url.path_segments()
                .and_then(Iterator::last)
                .map(str::to_owned)
        })
        .and_then(|name| media_format_hint(&name));
    let mime_format = content_type
        .and_then(|mime| mime.split(';').next())
        .map(str::trim)
        .map(str::to_ascii_lowercase);
    let mime_format = match mime_format.as_deref() {
        Some("video/mp4") => Some("mp4"),
        Some("video/webm") => Some("webm"),
        _ => None,
    };
    let mp4_signature = has_mp4_signature(file_path)?;
    let mp4_declared = mp4_signature
        || destination_format.as_deref() == Some("mp4")
        || url_format == Some("mp4")
        || mime_format == Some("mp4");
    let webm_declared = destination_format.as_deref() == Some("webm")
        || url_format == Some("webm")
        || mime_format == Some("webm");
    if mp4_declared && webm_declared {
        return Err(
            "The source format and destination filename disagree (MP4 and WebM); choose a matching filename extension."
                .into(),
        );
    }
    if !mp4_declared {
        return Ok(());
    }
    if destination_format.as_deref() != Some("mp4") {
        return Err(
            "The source is MP4 media, but the destination filename does not end in .mp4. Choose an .mp4 filename; FluxNet will not silently change the extension."
                .into(),
        );
    }
    match validate_mp4_file(file_path)? {
        Mp4Validation::Standalone | Mp4Validation::FragmentedWithInitialization => Ok(()),
        Mp4Validation::FragmentedMissingInitialization => Err(
            "The source is a fragmented MP4 without its required initialization segment (ftyp/moov). FluxNet cannot assemble it from the available source data, so it was not saved as a completed MP4."
                .into(),
        ),
        Mp4Validation::Invalid(reason) => Err(format!(
            "MP4 validation failed; the downloaded data is incomplete or invalid and was not saved as a completed MP4. {reason}"
        )),
    }
}

fn build_http_client() -> Result<Client, String> {
    Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .read_timeout(READ_TIMEOUT)
        .timeout(Duration::from_secs(60 * 60))
        .redirect(reqwest::redirect::Policy::limited(10))
        .user_agent(concat!(
            env!("CARGO_PKG_NAME"),
            "/",
            env!("CARGO_PKG_VERSION")
        ))
        .build()
        .map_err(|error| format!("HTTP client configuration error: {error}"))
}

pub(crate) fn validate_download_destination(path: &str) -> Result<(PathBuf, String), String> {
    validate_download_destination_with_options(path, false)
}

#[cfg(windows)]
pub(crate) fn validate_resumable_download_destination(
    path: &str,
) -> Result<(PathBuf, String), String> {
    validate_download_destination_with_options(path, true)
}

fn validate_download_destination_with_options(
    path: &str,
    allow_existing_partial: bool,
) -> Result<(PathBuf, String), String> {
    let destination = PathBuf::from(path);
    if !destination.is_absolute() {
        return Err(
            "Filesystem error: choose an absolute destination using the save-file dialog.".into(),
        );
    }
    let filename = destination
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            "Filesystem error: choose a destination with a valid filename.".to_owned()
        })?;
    if filename != sanitize_filename(filename) {
        return Err(
            "Filesystem error: the destination filename contains unsafe characters.".into(),
        );
    }
    let parent = destination.parent().ok_or_else(|| {
        "Filesystem error: the destination must be inside an existing folder.".to_owned()
    })?;
    if !parent.is_dir() {
        return Err("Filesystem error: the selected destination folder does not exist.".into());
    }
    if destination.exists() {
        return Err(
            "Filesystem error: the destination file already exists. Choose a different filename."
                .into(),
        );
    }
    let partial = partial_path(&destination);
    if !allow_existing_partial && partial.exists() {
        return Err("Filesystem error: a partial download already exists at this destination. Retry its history entry or choose another filename.".into());
    }
    let filename = filename.to_owned();
    Ok((destination, filename))
}

fn parse_content_range(value: &str) -> Option<(u64, u64, u64)> {
    let value = value.strip_prefix("bytes ")?;
    let (range, total) = value.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    let start = start.parse().ok()?;
    let end = end.parse().ok()?;
    let total = total.parse().ok()?;
    (start <= end && end < total).then_some((start, end, total))
}

fn resume_validator<'a>(etag: Option<&'a str>, last_modified: Option<&'a str>) -> Option<&'a str> {
    etag.filter(|value| !value.trim_start().starts_with("W/"))
        .or(last_modified)
}

#[cfg(test)]
fn validate_range_response(
    status: StatusCode,
    content_range: Option<&str>,
    offset: u64,
    expected_validator: Option<&str>,
    response_validator: Option<&str>,
) -> Result<u64, String> {
    if status != StatusCode::PARTIAL_CONTENT {
        return Err(
            "The server did not honor the Range request. The partial file was not appended; use Retry to safely restart from zero."
                .into(),
        );
    }
    if expected_validator.is_none() || response_validator != expected_validator {
        return Err(
            "The server's file validator changed or is missing. The partial file was not appended; use Retry to restart safely."
                .into(),
        );
    }
    let range = content_range
        .and_then(parse_content_range)
        .ok_or_else(|| "The server returned an invalid Content-Range.".to_owned())?;
    let (start, end, total) = range;
    if start != offset {
        return Err("The server returned a Content-Range with the wrong starting offset.".into());
    }
    if end + 1 != total {
        return Err(
            "The server returned an incomplete byte range. The partial file was not appended; use Retry to restart safely."
                .into(),
        );
    }
    Ok(total)
}

#[cfg(test)]
fn progress_percent(downloaded: u64, total: Option<u64>) -> Option<f64> {
    total
        .filter(|total| *total > 0)
        .map(|total| (downloaded as f64 / total as f64 * 100.0).clamp(0.0, 100.0))
}

fn bandwidth_bytes_per_second(mbps: f64) -> Option<u64> {
    (mbps > 0.0).then_some((mbps * 1_000_000.0 / 8.0) as u64)
}

fn validate_transition(from: &DownloadStatus, to: &DownloadStatus) -> Result<(), String> {
    let valid = matches!(
        (from, to),
        (DownloadStatus::Queued, DownloadStatus::Downloading)
            | (DownloadStatus::Queued, DownloadStatus::Cancelled)
            | (DownloadStatus::Queued, DownloadStatus::Failed)
            | (DownloadStatus::Downloading, DownloadStatus::Paused)
            | (DownloadStatus::Downloading, DownloadStatus::Completed)
            | (DownloadStatus::Downloading, DownloadStatus::Failed)
            | (DownloadStatus::Downloading, DownloadStatus::Cancelled)
            | (DownloadStatus::Paused, DownloadStatus::Queued)
            | (DownloadStatus::Paused, DownloadStatus::Downloading)
            | (DownloadStatus::Paused, DownloadStatus::Cancelled)
            | (DownloadStatus::Failed, DownloadStatus::Queued)
            | (DownloadStatus::Cancelled, DownloadStatus::Queued)
    );
    if valid {
        Ok(())
    } else {
        Err(format!(
            "Invalid download state transition: {from:?} → {to:?}."
        ))
    }
}

fn transition(entry: &mut DownloadEntry, status: DownloadStatus) -> Result<(), String> {
    validate_transition(&entry.status, &status)?;
    entry.status = status;
    entry.updated_at_ms = current_timestamp_ms();
    Ok(())
}

fn persist_locked(state: &DownloadState) -> Result<(), String> {
    let Some(path) = &state.history_path else {
        return Ok(());
    };
    let data = PersistedDownloads {
        version: 1,
        entries: state.entries.clone(),
        max_concurrent: state.max_concurrent,
        max_connections_per_download: state.max_connections_per_download,
        bandwidth_limit_bytes_per_second: state.bandwidth_limit_bytes_per_second,
        notifications_enabled: state.notifications_enabled,
        notify_on_complete: state.notify_on_complete,
        notify_on_failure: state.notify_on_failure,
    };
    let serialized = serde_json::to_vec(&data).map_err(|error| {
        format!("Filesystem error: could not serialize download history: {error}")
    })?;
    let temporary_path = path.with_extension("json.tmp");
    std::fs::write(&temporary_path, serialized)
        .map_err(|error| filesystem_error("could not write download history", error))?;
    std::fs::rename(&temporary_path, path)
        .map_err(|error| filesystem_error("could not commit download history", error))
}

#[cfg(test)]
type TestCommitHook = Box<dyn Fn(&str, &str, &DownloadTaskControl) + Send + Sync>;

#[cfg(test)]
static TEST_COMMIT_HOOK: std::sync::LazyLock<Mutex<Option<TestCommitHook>>> =
    std::sync::LazyLock::new(|| Mutex::new(None));

#[cfg(test)]
static TEST_EMITTED_EVENTS: std::sync::LazyLock<Mutex<Vec<(String, DownloadStatus, u32)>>> =
    std::sync::LazyLock::new(|| Mutex::new(Vec::new()));

#[cfg(test)]
fn invoke_test_commit_hook(stage: &str, id: &str, control: &DownloadTaskControl) {
    if let Ok(guard) = TEST_COMMIT_HOOK.lock() {
        if let Some(hook) = guard.as_ref() {
            hook(stage, id, control);
        }
    }
}

fn emit_entry_optional(app: Option<&tauri::AppHandle>, entry: DownloadEntry) {
    #[cfg(test)]
    if let Ok(mut events) = TEST_EMITTED_EVENTS.lock() {
        events.push((entry.id.clone(), entry.status.clone(), entry.attempts));
    }
    if let Some(app) = app {
        if let Err(error) = app.emit(DOWNLOAD_EVENT, entry) {
            eprintln!("Could not publish Download Manager update: {error}");
        }
    }
}

#[allow(dead_code)]
fn emit_entry(app: &tauri::AppHandle, entry: DownloadEntry) {
    emit_entry_optional(Some(app), entry);
}

fn update_entry_optional<F>(
    app: Option<&tauri::AppHandle>,
    state: &Mutex<DownloadState>,
    id: &str,
    update: F,
) -> Result<DownloadEntry, String>
where
    F: FnOnce(&mut DownloadEntry) -> Result<(), String>,
{
    let entry = {
        let mut state = state
            .lock()
            .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
        let index = state
            .entries
            .iter()
            .position(|entry| entry.id == id)
            .ok_or_else(|| "Download record was not found.".to_owned())?;
        let original = state.entries[index].clone();
        let mut updated = original.clone();
        update(&mut updated)?;
        state.entries[index] = updated.clone();
        if let Err(error) = persist_locked(&state) {
            state.entries[index] = original;
            return Err(error);
        }
        if updated.status.is_terminal() || updated.status == DownloadStatus::Paused {
            state.controls.remove(id);
        }
        #[cfg(windows)]
        if updated.status.is_terminal() {
            state.website_executables.remove(id);
        }
        updated
    };
    emit_entry_optional(app, entry.clone());
    Ok(entry)
}

#[allow(dead_code)]
fn update_entry<F>(
    app: &tauri::AppHandle,
    state: &Mutex<DownloadState>,
    id: &str,
    update: F,
) -> Result<DownloadEntry, String>
where
    F: FnOnce(&mut DownloadEntry) -> Result<(), String>,
{
    update_entry_optional(Some(app), state, id, update)
}

#[tauri::command]
pub fn get_downloads(
    manager: tauri::State<'_, DownloadManager>,
) -> Result<Vec<DownloadEntry>, String> {
    manager.snapshot()
}

#[tauri::command]
pub fn get_download_settings(
    manager: tauri::State<'_, DownloadManager>,
) -> Result<DownloadSettings, String> {
    manager.settings()
}

#[tauri::command]
pub fn get_download_filename(
    manager: tauri::State<'_, DownloadManager>,
    url: String,
    page_title: Option<String>,
    page_domain: Option<String>,
    media_type: Option<String>,
    preferred_filename: Option<String>,
    directory: Option<String>,
) -> Result<String, String> {
    if url.len() > MAX_URL_LENGTH {
        return Err("Download URL is too long.".into());
    }
    let filename = suggested_filename_with_metadata(
        &url,
        page_title.as_deref(),
        page_domain.as_deref(),
        media_type.as_deref(),
        preferred_filename.as_deref(),
    )?;
    let Some(directory) = directory else {
        return Ok(filename);
    };
    let entries = manager
        .state
        .lock()
        .map_err(|_| "Download manager state lock was poisoned.".to_owned())?
        .entries
        .clone();
    unique_filename(&filename, |candidate| {
        !destination_in_use(Path::new(&directory), candidate, &entries)
    })
}

#[tauri::command]
pub fn configure_download_limits(
    manager: tauri::State<'_, DownloadManager>,
    max_concurrent: usize,
    bandwidth_limit_mbps: f64,
) -> Result<(), String> {
    if !(1..=MAX_CONCURRENT_DOWNLOADS).contains(&max_concurrent) {
        return Err(format!(
            "Choose between 1 and {MAX_CONCURRENT_DOWNLOADS} concurrent downloads."
        ));
    }
    if ![0.0, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0].contains(&bandwidth_limit_mbps) {
        return Err("Choose a supported bandwidth limit.".into());
    }
    let mut state = manager
        .state
        .lock()
        .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
    let previous_concurrent = state.max_concurrent;
    let previous_bandwidth = state.bandwidth_limit_bytes_per_second;
    state.max_concurrent = max_concurrent;
    state.bandwidth_limit_bytes_per_second = bandwidth_bytes_per_second(bandwidth_limit_mbps);
    if let Err(error) = persist_locked(&state) {
        state.max_concurrent = previous_concurrent;
        state.bandwidth_limit_bytes_per_second = previous_bandwidth;
        return Err(error);
    }
    drop(state);
    manager.concurrency.notify.notify_waiters();
    Ok(())
}

#[tauri::command]
pub fn configure_download_notifications(
    manager: tauri::State<'_, DownloadManager>,
    enabled: bool,
) -> Result<(), String> {
    let mut state = manager
        .state
        .lock()
        .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
    let previous_notifications = state.notifications_enabled;
    let previous_complete = state.notify_on_complete;
    let previous_failed = state.notify_on_failure;
    let previous_entries = state.entries.clone();
    state.notifications_enabled = enabled;
    state.notify_on_complete = enabled;
    state.notify_on_failure = enabled;
    for entry in &mut state.entries {
        if !entry.status.is_terminal() {
            entry.notifications_enabled = enabled;
            entry.notify_on_complete = enabled;
            entry.notify_on_failure = enabled;
        }
    }
    if let Err(error) = persist_locked(&state) {
        state.notifications_enabled = previous_notifications;
        state.notify_on_complete = previous_complete;
        state.notify_on_failure = previous_failed;
        state.entries = previous_entries;
        return Err(error);
    }
    Ok(())
}

#[tauri::command]
pub fn configure_download_notification_preferences(
    manager: tauri::State<'_, DownloadManager>,
    notify_on_complete: bool,
    notify_on_failure: bool,
) -> Result<(), String> {
    let mut state = manager
        .state
        .lock()
        .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
    let previous_complete = state.notify_on_complete;
    let previous_failed = state.notify_on_failure;
    let previous_enabled = state.notifications_enabled;
    let previous_entries = state.entries.clone();
    state.notify_on_complete = notify_on_complete;
    state.notify_on_failure = notify_on_failure;
    state.notifications_enabled = notify_on_complete || notify_on_failure;
    for entry in &mut state.entries {
        if !entry.status.is_terminal() {
            entry.notify_on_complete = notify_on_complete;
            entry.notify_on_failure = notify_on_failure;
            entry.notifications_enabled = notify_on_complete || notify_on_failure;
        }
    }
    if let Err(error) = persist_locked(&state) {
        state.notify_on_complete = previous_complete;
        state.notify_on_failure = previous_failed;
        state.notifications_enabled = previous_enabled;
        state.entries = previous_entries;
        return Err(error);
    }
    Ok(())
}

#[cfg(windows)]
#[tauri::command]
pub async fn discover_website_media(
    app: tauri::AppHandle,
    manager: tauri::State<'_, DownloadManager>,
    url: String,
) -> Result<crate::media::yt_dlp::VideoDiscovery, String> {
    manager.discover_website_media(Some(&app), url).await
}

#[cfg(not(windows))]
#[tauri::command]
pub async fn discover_website_media(
    _app: tauri::AppHandle,
    _manager: tauri::State<'_, DownloadManager>,
    _url: String,
) -> Result<crate::media::yt_dlp::VideoDiscovery, String> {
    Err("Website video discovery is supported on Windows only.".into())
}

#[cfg(windows)]
#[tauri::command]
pub fn start_website_download(
    app: tauri::AppHandle,
    manager: tauri::State<'_, DownloadManager>,
    url: String,
    destination: String,
    catalog: crate::media::yt_dlp::VideoDiscovery,
    selection: crate::media::yt_dlp::FormatSelection,
) -> Result<DownloadEntry, String> {
    manager.start_website_download_verified(Some(&app), url, destination, catalog, selection)
}

#[cfg(not(windows))]
#[tauri::command]
pub fn start_website_download(
    _app: tauri::AppHandle,
    _manager: tauri::State<'_, DownloadManager>,
    _url: String,
    _destination: String,
    _catalog: crate::media::yt_dlp::VideoDiscovery,
    _selection: crate::media::yt_dlp::FormatSelection,
) -> Result<DownloadEntry, String> {
    Err("Website video downloads are supported on Windows only.".into())
}

impl DownloadManager {
    pub(crate) fn start_download_internal(
        &self,
        app: Option<&tauri::AppHandle>,
        url: String,
        destination: String,
        hls_playlist: Option<HlsPlaylist>,
    ) -> Result<DownloadEntry, String> {
        if url.len() > MAX_URL_LENGTH {
            return Err("Download URL is too long.".into());
        }
        let parsed_url = validated_download_url(&url)?;
        let parsed_url = if hls_playlist.is_none() {
            normalize_instagram_media_url(&parsed_url)?
        } else {
            parsed_url
        };
        let hls_playlist = hls_playlist
            .map(|playlist| validate_hls_playlist(playlist, &parsed_url))
            .transpose()?;
        let _suggested = suggested_filename(parsed_url.as_str())?;
        let id = format!("{}-{}", current_timestamp_ms(), rand_id());
        let now = current_timestamp_ms();
        let (entry, control) = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
            let control = Arc::new(DownloadTaskControl::default());
            let destination = unique_destination(Path::new(&destination), &state.entries)?;
            let (destination, filename) =
                validate_download_destination(destination.to_string_lossy().as_ref())?;
            if hls_playlist.is_some() {
                validate_hls_destination(&destination)?;
            }
            let entry = DownloadEntry {
                id: id.clone(),
                url: parsed_url.to_string(),
                destination: destination.to_string_lossy().to_string(),
                filename,
                status: DownloadStatus::Queued,
                total_bytes: hls_playlist
                    .as_ref()
                    .and_then(|playlist| playlist.expected_bytes),
                downloaded_bytes: 0,
                speed_bytes_per_second: 0.0,
                elapsed_ms: 0,
                remaining_seconds: None,
                error: None,
                created_at_ms: now,
                updated_at_ms: now,
                attempts: 1,
                hls_playlist: hls_playlist.clone(),
                hls_completed_parts: hls_playlist.as_ref().map(|_| 0),
                website_media: None,
                segments: Vec::new(),
                etag: None,
                last_modified: None,
                notifications_enabled: state.notifications_enabled,
                notify_on_complete: state.notify_on_complete,
                notify_on_failure: state.notify_on_failure,
            };
            state.entries.push(entry.clone());
            state.controls.insert(id.clone(), Arc::clone(&control));
            if let Err(error) = persist_locked(&state) {
                state.entries.pop();
                state.controls.remove(&id);
                return Err(error);
            }
            (entry, control)
        };
        emit_entry_optional(app, entry.clone());
        spawn_worker(
            app.cloned(),
            Arc::clone(&self.state),
            Arc::clone(&self.concurrency),
            Arc::clone(&self.bandwidth),
            Arc::clone(&self.worker_count),
            id,
            control,
            false,
        );
        Ok(entry)
    }

    pub(crate) fn retry_download_internal(
        &self,
        app: Option<&tauri::AppHandle>,
        id: String,
        restart: bool,
    ) -> Result<(), String> {
        let control = Arc::new(DownloadTaskControl::default());
        let cleanup_dest = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
            let index = state
                .entries
                .iter()
                .position(|entry| entry.id == id)
                .ok_or_else(|| "Download record was not found.".to_owned())?;
            if state.controls.contains_key(&id) {
                return Err("A download worker is already active for this item.".into());
            }
            let original = state.entries[index].clone();
            if original.website_media.is_some() {
                return Err("Website video downloads must be re-queued with a format selection; start a new download to retry.".into());
            }
            let mut entry = original.clone();
            if !entry.status.can_retry() {
                return Err("Only failed or cancelled downloads can be retried.".into());
            }
            transition(&mut entry, DownloadStatus::Queued)?;
            entry.attempts = entry.attempts.saturating_add(1);
            entry.error = None;
            let dest_for_cleanup = if restart {
                entry.downloaded_bytes = 0;
                entry.total_bytes = None;
                entry.speed_bytes_per_second = 0.0;
                entry.elapsed_ms = 0;
                entry.remaining_seconds = None;
                entry.segments.clear();
                if entry.hls_playlist.is_some() {
                    entry.hls_completed_parts = Some(0);
                }
                Some(PathBuf::from(&entry.destination))
            } else {
                verify_and_repair_partial_state_on_disk(&mut entry);
                None
            };
            state.entries[index] = entry.clone();
            let old_control = state.controls.insert(id.clone(), Arc::clone(&control));
            if let Err(error) = persist_locked(&state) {
                state.entries[index] = original;
                if let Some(old_control) = old_control {
                    state.controls.insert(id.clone(), old_control);
                } else {
                    state.controls.remove(&id);
                }
                return Err(error);
            }
            (entry, dest_for_cleanup)
        };
        if let Some(dest) = cleanup_dest.1 {
            remove_all_segment_parts_sync(&dest, &id);
        }
        emit_entry_optional(app, cleanup_dest.0);
        spawn_worker(
            app.cloned(),
            Arc::clone(&self.state),
            Arc::clone(&self.concurrency),
            Arc::clone(&self.bandwidth),
            Arc::clone(&self.worker_count),
            id,
            control,
            restart,
        );
        Ok(())
    }
}

#[tauri::command]
pub fn start_download(
    app: tauri::AppHandle,
    manager: tauri::State<'_, DownloadManager>,
    url: String,
    destination: String,
    hls_playlist: Option<HlsPlaylist>,
) -> Result<DownloadEntry, String> {
    manager.start_download_internal(Some(&app), url, destination, hls_playlist)
}

fn rand_id() -> String {
    format!(
        "{:016x}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    )
}

fn spawn_worker(
    app: Option<tauri::AppHandle>,
    state: Arc<Mutex<DownloadState>>,
    concurrency: Arc<ConcurrencyLimiter>,
    bandwidth: Arc<BandwidthLimiter>,
    worker_count: Arc<AtomicUsize>,
    id: String,
    control: Arc<DownloadTaskControl>,
    restart: bool,
) {
    worker_count.fetch_add(1, Ordering::AcqRel);
    tauri::async_runtime::spawn(async move {
        let _worker_guard = WorkerGuard(worker_count);
        if let Err(error) = download_worker(
            app.as_ref(),
            &state,
            &concurrency,
            &bandwidth,
            &id,
            &control,
            restart,
        )
        .await
        {
            if let Err(update_error) = fail_entry_optional(app.as_ref(), &state, &id, error).await {
                eprintln!("Could not record download failure: {update_error}");
            }
        }
    });
}

pub(crate) fn segment_part_path(destination: &Path, id: &str, index: usize) -> PathBuf {
    let mut name = destination.file_name().unwrap_or_default().to_os_string();
    name.push(format!(".{id}.seg-{index}.fluxnet-part"));
    destination.with_file_name(name)
}

fn remove_all_segment_parts_sync(destination: &Path, id: &str) {
    for index in 0..MAX_CONNECTIONS_PER_DOWNLOAD {
        let _ = std::fs::remove_file(segment_part_path(destination, id, index));
    }
}

async fn remove_all_segment_parts(destination: &Path, id: &str) {
    for index in 0..MAX_CONNECTIONS_PER_DOWNLOAD {
        let _ = fs::remove_file(segment_part_path(destination, id, index)).await;
    }
}

pub(crate) fn plan_parallel_segments(
    total_bytes: u64,
    max_connections: usize,
) -> Vec<DownloadSegment> {
    if total_bytes < MIN_PARALLEL_FILE_BYTES || max_connections <= 1 {
        return Vec::new();
    }
    let clamped = max_connections.clamp(1, MAX_CONNECTIONS_PER_DOWNLOAD);
    let max_by_size =
        usize::try_from(total_bytes / MIN_PARALLEL_SEGMENT_BYTES).unwrap_or(clamped);
    let count = clamped.min(max_by_size).max(1);
    if count <= 1 {
        return Vec::new();
    }
    let base_len = total_bytes / (count as u64);
    let remainder = total_bytes % (count as u64);
    let mut cursor = 0_u64;
    let mut segments = Vec::with_capacity(count);
    for index in 0..count {
        let seg_len = base_len + u64::from((index as u64) < remainder);
        let start = cursor;
        let end = start + seg_len - 1;
        cursor = end + 1;
        segments.push(DownloadSegment {
            index,
            start,
            end,
            downloaded_bytes: 0,
            completed: false,
        });
    }
    segments
}

pub(crate) fn validate_segment_plan(
    segments: &[DownloadSegment],
    total_bytes: u64,
) -> Result<(), String> {
    if segments.is_empty() || total_bytes == 0 {
        return Err("The parallel segment plan is empty.".into());
    }
    if segments.len() > MAX_CONNECTIONS_PER_DOWNLOAD {
        return Err("The parallel segment plan exceeds the connection limit.".into());
    }
    let mut expected_start = 0_u64;
    for (expected_index, seg) in segments.iter().enumerate() {
        if seg.index != expected_index {
            return Err(format!(
                "Parallel segment index mismatch or duplicated segment at index {}.",
                seg.index
            ));
        }
        if seg.start > seg.end {
            return Err(format!(
                "Parallel segment {} has an invalid byte range ({}-{}).",
                seg.index, seg.start, seg.end
            ));
        }
        if seg.start < expected_start {
            return Err(format!(
                "Parallel segment {} overlaps with a previous segment at byte {}.",
                seg.index, seg.start
            ));
        }
        if seg.start > expected_start {
            return Err(format!(
                "Parallel segment {} leaves a missing byte gap between {} and {}.",
                seg.index, expected_start, seg.start
            ));
        }
        let capacity = seg.end - seg.start + 1;
        if seg.downloaded_bytes > capacity {
            return Err(format!(
                "Parallel segment {} downloaded bytes ({}) exceed its capacity ({capacity}).",
                seg.index, seg.downloaded_bytes
            ));
        }
        if seg.completed && seg.downloaded_bytes != capacity {
            return Err(format!(
                "Parallel segment {} is marked complete but has {} of {capacity} bytes.",
                seg.index, seg.downloaded_bytes
            ));
        }
        expected_start = seg
            .end
            .checked_add(1)
            .ok_or_else(|| "Parallel segment range overflow.".to_owned())?;
    }
    if expected_start != total_bytes {
        return Err(format!(
            "Parallel segments end at byte {expected_start}, but total file size is {total_bytes} bytes."
        ));
    }
    Ok(())
}

fn response_advertises_byte_ranges(headers: &reqwest::header::HeaderMap) -> bool {
    headers
        .get_all(ACCEPT_RANGES)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .any(|v| {
            v.split(',')
                .any(|token| token.trim().eq_ignore_ascii_case("bytes"))
        })
}

fn current_unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(crate) fn is_signed_url_expired(url: &reqwest::Url) -> bool {
    let now_secs = current_unix_timestamp_secs();
    if is_instagram_cdn_url(url) {
        for (name, value) in url.query_pairs() {
            if name.eq_ignore_ascii_case("oe") {
                if let Ok(expiry) = u64::from_str_radix(value.trim(), 16) {
                    if expiry > 0 && expiry < now_secs {
                        return true;
                    }
                }
            }
        }
    }
    for (name, value) in url.query_pairs() {
        if name.eq_ignore_ascii_case("expires") {
            if let Ok(expiry) = value.trim().parse::<u64>() {
                if expiry >= 946_684_800 && expiry < now_secs {
                    return true;
                }
            }
        }
    }
    false
}

fn verify_and_repair_partial_state_on_disk(entry: &mut DownloadEntry) {
    let destination = PathBuf::from(&entry.destination);
    if entry.hls_playlist.is_some() {
        let staging = hls_staging_path(&destination, &entry.id);
        let _ = std::fs::remove_file(staging);
        let partial = partial_path(&destination);
        match std::fs::symlink_metadata(&partial) {
            Ok(meta) if meta.file_type().is_file() => {
                if meta.len() < entry.downloaded_bytes {
                    entry.downloaded_bytes = 0;
                    entry.hls_completed_parts = Some(0);
                    let _ = std::fs::remove_file(&partial);
                } else if meta.len() > entry.downloaded_bytes {
                    if let Ok(file) = std::fs::OpenOptions::new().write(true).open(&partial) {
                        let _ = file.set_len(entry.downloaded_bytes);
                    }
                }
            }
            _ => {
                entry.downloaded_bytes = 0;
                entry.hls_completed_parts = Some(0);
            }
        }
        return;
    }

    if !entry.segments.is_empty() {
        let Some(total_bytes) = entry.total_bytes else {
            remove_all_segment_parts_sync(&destination, &entry.id);
            let _ = std::fs::remove_file(partial_path(&destination));
            entry.segments.clear();
            entry.downloaded_bytes = 0;
            return;
        };
        if validate_segment_plan(&entry.segments, total_bytes).is_err() {
            remove_all_segment_parts_sync(&destination, &entry.id);
            let _ = std::fs::remove_file(partial_path(&destination));
            entry.segments.clear();
            entry.downloaded_bytes = 0;
            return;
        }
        let mut verified_total = 0_u64;
        for seg in &mut entry.segments {
            let seg_path = segment_part_path(&destination, &entry.id, seg.index);
            let capacity = seg.end - seg.start + 1;
            match std::fs::symlink_metadata(&seg_path) {
                Ok(meta) if meta.file_type().is_file() => {
                    let actual_len = meta.len();
                    if seg.completed {
                        if actual_len == capacity && seg.downloaded_bytes == capacity {
                            verified_total = verified_total.saturating_add(actual_len);
                        } else {
                            let _ = std::fs::remove_file(&seg_path);
                            seg.downloaded_bytes = 0;
                            seg.completed = false;
                        }
                    } else if seg.downloaded_bytes > 0
                        && actual_len == seg.downloaded_bytes
                        && actual_len <= capacity
                    {
                        verified_total = verified_total.saturating_add(actual_len);
                    } else if seg.downloaded_bytes > 0
                        && actual_len > seg.downloaded_bytes
                        && actual_len <= capacity
                    {
                        if let Ok(file) = std::fs::OpenOptions::new().write(true).open(&seg_path) {
                            if file.set_len(seg.downloaded_bytes).is_ok() {
                                verified_total =
                                    verified_total.saturating_add(seg.downloaded_bytes);
                            } else {
                                drop(file);
                                let _ = std::fs::remove_file(&seg_path);
                                seg.downloaded_bytes = 0;
                                seg.completed = false;
                            }
                        } else {
                            let _ = std::fs::remove_file(&seg_path);
                            seg.downloaded_bytes = 0;
                            seg.completed = false;
                        }
                    } else {
                        let _ = std::fs::remove_file(&seg_path);
                        seg.downloaded_bytes = 0;
                        seg.completed = false;
                    }
                }
                _ => {
                    let _ = std::fs::remove_file(&seg_path);
                    seg.downloaded_bytes = 0;
                    seg.completed = false;
                }
            }
        }
        entry.downloaded_bytes = verified_total;
        return;
    }

    let partial = partial_path(&destination);
    match std::fs::symlink_metadata(&partial) {
        Ok(meta) if meta.file_type().is_file() => {
            let actual_len = meta.len();
            if entry.total_bytes.is_some_and(|total| actual_len > total) {
                let _ = std::fs::remove_file(&partial);
                entry.downloaded_bytes = 0;
            } else if entry.downloaded_bytes == 0 {
                entry.downloaded_bytes = actual_len;
            } else if actual_len < entry.downloaded_bytes {
                entry.downloaded_bytes = actual_len;
            } else if actual_len > entry.downloaded_bytes {
                if let Ok(file) = std::fs::OpenOptions::new().write(true).open(&partial) {
                    if file.set_len(entry.downloaded_bytes).is_err() {
                        drop(file);
                        let _ = std::fs::remove_file(&partial);
                        entry.downloaded_bytes = 0;
                    }
                } else {
                    let _ = std::fs::remove_file(&partial);
                    entry.downloaded_bytes = 0;
                }
            }
        }
        _ => {
            entry.downloaded_bytes = 0;
        }
    }
}

enum ParallelDownloadOutcome {
    Handled,
    FallbackToSequential,
}

enum ParallelSegmentResult {
    Completed,
    Paused,
    Shutdown,
    Cancelled,
    RangeUnsupported(String),
    FatalError(String),
}

struct ParallelProgressTracker {
    last_update: Instant,
    last_bytes: u64,
}

#[allow(clippy::too_many_arguments)]
async fn download_parallel_segments_worker(
    app: Option<&tauri::AppHandle>,
    state: &Mutex<DownloadState>,
    bandwidth: &Arc<BandwidthLimiter>,
    client: &Client,
    id: &str,
    control: &Arc<DownloadTaskControl>,
    url: &str,
    destination: &Path,
    total_bytes: u64,
    mut segments: Vec<DownloadSegment>,
    etag: Option<String>,
    last_modified: Option<String>,
    response_content_type: Option<String>,
    base_elapsed_ms: u64,
    max_connections: usize,
    initial_fresh_download: bool,
) -> Result<ParallelDownloadOutcome, String> {
    if let Err(error) = validate_segment_plan(&segments, total_bytes) {
        remove_all_segment_parts(destination, id).await;
        return Err(error);
    }

    let mut initial_verified_bytes = 0_u64;
    for seg in &mut segments {
        let seg_path = segment_part_path(destination, id, seg.index);
        let capacity = seg.end - seg.start + 1;
        match fs::symlink_metadata(&seg_path).await {
            Ok(meta) if meta.file_type().is_file() => {
                let actual_len = meta.len();
                if seg.completed {
                    if actual_len == capacity && seg.downloaded_bytes == capacity {
                        initial_verified_bytes = initial_verified_bytes.saturating_add(actual_len);
                    } else {
                        let _ = fs::remove_file(&seg_path).await;
                        seg.downloaded_bytes = 0;
                        seg.completed = false;
                    }
                } else if seg.downloaded_bytes > 0
                    && actual_len == seg.downloaded_bytes
                    && actual_len <= capacity
                {
                    initial_verified_bytes = initial_verified_bytes.saturating_add(actual_len);
                } else if seg.downloaded_bytes > 0
                    && actual_len > seg.downloaded_bytes
                    && actual_len <= capacity
                {
                    if let Ok(file) = OpenOptions::new().write(true).open(&seg_path).await {
                        if file.set_len(seg.downloaded_bytes).await.is_ok() {
                            initial_verified_bytes =
                                initial_verified_bytes.saturating_add(seg.downloaded_bytes);
                        } else {
                            drop(file);
                            let _ = fs::remove_file(&seg_path).await;
                            seg.downloaded_bytes = 0;
                            seg.completed = false;
                        }
                    } else {
                        let _ = fs::remove_file(&seg_path).await;
                        seg.downloaded_bytes = 0;
                        seg.completed = false;
                    }
                } else {
                    let _ = fs::remove_file(&seg_path).await;
                    seg.downloaded_bytes = 0;
                    seg.completed = false;
                }
            }
            _ => {
                let _ = fs::remove_file(&seg_path).await;
                seg.downloaded_bytes = 0;
                seg.completed = false;
            }
        }
    }

    let initial_segments_snapshot = segments.clone();
    update_entry_optional(app, state, id, |entry| {
        entry.total_bytes = Some(total_bytes);
        entry.downloaded_bytes = initial_verified_bytes;
        entry.segments = initial_segments_snapshot;
        if etag.is_some() {
            entry.etag = etag.clone();
        }
        if last_modified.is_some() {
            entry.last_modified = last_modified.clone();
        }
        entry.error = None;
        Ok(())
    })?;

    let start_instant = Instant::now();
    let incomplete_indices: Vec<usize> = segments
        .iter()
        .filter(|s| !s.completed)
        .map(|s| s.index)
        .collect();

    let shared_segments = Arc::new(Mutex::new(segments));
    let shared_content_type = Arc::new(Mutex::new(response_content_type));
    let shared_validator = Arc::new(Mutex::new(
        resume_validator(etag.as_deref(), last_modified.as_deref()).map(str::to_owned),
    ));
    let progress_tracker = Arc::new(Mutex::new(ParallelProgressTracker {
        last_update: Instant::now(),
        last_bytes: initial_verified_bytes,
    }));
    let connection_limit = max_connections.clamp(1, MAX_CONNECTIONS_PER_DOWNLOAD);
    let semaphore = Arc::new(Semaphore::new(connection_limit));
    let abort_all = Arc::new(AtomicBool::new(false));

    let mut tasks = Vec::with_capacity(incomplete_indices.len());
    for seg_idx in incomplete_indices {
        let sem = Arc::clone(&semaphore);
        let segs_lock = Arc::clone(&shared_segments);
        let ct_lock = Arc::clone(&shared_content_type);
        let val_lock = Arc::clone(&shared_validator);
        let prog_lock = Arc::clone(&progress_tracker);
        let abort_flag = Arc::clone(&abort_all);
        let ctrl = Arc::clone(control);
        let bw = Arc::clone(bandwidth);
        let http = client.clone();
        let req_url = url.to_owned();
        let dest_path = destination.to_path_buf();
        let entry_id = id.to_owned();

        tasks.push(async move {
            let stop_check = || {
                if ctrl.is_cancelled() {
                    Some(ParallelSegmentResult::Cancelled)
                } else if ctrl.is_pause_requested() {
                    Some(ParallelSegmentResult::Paused)
                } else if ctrl.is_shutdown_requested() {
                    Some(ParallelSegmentResult::Shutdown)
                } else if abort_flag.load(Ordering::Acquire) {
                    Some(ParallelSegmentResult::Cancelled)
                } else {
                    None
                }
            };
            if let Some(early) = stop_check() {
                return early;
            }
            let permit = loop {
                let notified = ctrl.wake.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if let Some(early) = stop_check() {
                    return early;
                }
                tokio::select! {
                    permit = sem.clone().acquire_owned() => match permit {
                        Ok(p) => break p,
                        Err(_) => return ParallelSegmentResult::FatalError("Parallel connection limiter closed.".into()),
                    },
                    _ = &mut notified => {
                        if let Some(early) = stop_check() {
                            return early;
                        }
                    }
                }
            };
            let _permit = permit;

            for attempt in 0..MAX_SEGMENT_ATTEMPTS {
                if let Some(early) = stop_check() {
                    return early;
                }

                let (seg_start, seg_end, already_downloaded) = {
                    let locked = segs_lock.lock().unwrap();
                    let s = &locked[seg_idx];
                    (s.start, s.end, s.downloaded_bytes)
                };
                let capacity = seg_end - seg_start + 1;
                if already_downloaded >= capacity {
                    let mut locked = segs_lock.lock().unwrap();
                    locked[seg_idx].downloaded_bytes = capacity;
                    locked[seg_idx].completed = true;
                    return ParallelSegmentResult::Completed;
                }

                let req_start = seg_start + already_downloaded;
                let expected_remaining = seg_end - req_start + 1;
                let seg_path = segment_part_path(&dest_path, &entry_id, seg_idx);

                let mut seg_file = match OpenOptions::new()
                    .create(already_downloaded == 0)
                    .write(true)
                    .append(already_downloaded > 0)
                    .truncate(already_downloaded == 0)
                    .open(&seg_path)
                    .await
                {
                    Ok(f) => f,
                    Err(e) => {
                        abort_flag.store(true, Ordering::Release);
                        ctrl.wake.notify_waiters();
                        return ParallelSegmentResult::FatalError(filesystem_error(
                            "could not open temporary parallel segment file",
                            e,
                        ));
                    }
                };
                if let Ok(meta) = seg_file.metadata().await {
                    if meta.len() != already_downloaded {
                        drop(seg_file);
                        let _ = fs::remove_file(&seg_path).await;
                        {
                            let mut locked = segs_lock.lock().unwrap();
                            locked[seg_idx].downloaded_bytes = 0;
                            locked[seg_idx].completed = false;
                        }
                        continue;
                    }
                }

                let request = http
                    .get(&req_url)
                    .header(RANGE, format!("bytes={req_start}-{seg_end}"));
                let notified = ctrl.wake.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if let Some(early) = stop_check() {
                    let _ = seg_file.flush().await;
                    return early;
                }
                let response = tokio::select! {
                    res = request.send() => match res {
                        Ok(r) => r,
                        Err(e) => {
                            let _ = seg_file.flush().await;
                            if attempt + 1 < MAX_SEGMENT_ATTEMPTS && stop_check().is_none() {
                                sleep(Duration::from_millis(40)).await;
                                continue;
                            }
                            abort_flag.store(true, Ordering::Release);
                            ctrl.wake.notify_waiters();
                            return ParallelSegmentResult::FatalError(describe_reqwest_error(&e, &req_url));
                        }
                    },
                    _ = &mut notified => {
                        let _ = seg_file.flush().await;
                        if let Some(early) = stop_check() {
                            return early;
                        }
                        return ParallelSegmentResult::FatalError("Parallel segment request was interrupted.".into());
                    }
                };

                let final_url = response.url();
                if !matches!(final_url.scheme(), "http" | "https")
                    || final_url.host_str().is_none()
                    || !final_url.username().is_empty()
                    || final_url.password().is_some()
                {
                    abort_flag.store(true, Ordering::Release);
                    ctrl.wake.notify_waiters();
                    return ParallelSegmentResult::FatalError(
                        "The download redirected to an unsupported or credential-bearing URL."
                            .into(),
                    );
                }

                let status = response.status();
                if status == StatusCode::OK
                    || status == StatusCode::RANGE_NOT_SATISFIABLE
                    || status == StatusCode::NOT_IMPLEMENTED
                {
                    abort_flag.store(true, Ordering::Release);
                    ctrl.wake.notify_waiters();
                    return ParallelSegmentResult::RangeUnsupported(format!(
                        "The server did not honor parallel Range request bytes={req_start}-{seg_end} (returned {status})."
                    ));
                }
                if matches!(
                    status,
                    StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED | StatusCode::GONE
                ) {
                    abort_flag.store(true, Ordering::Release);
                    ctrl.wake.notify_waiters();
                    return ParallelSegmentResult::FatalError(format!(
                        "HTTP status error: the server returned {status} (the video source URL is expired, unauthorized, or no longer available)."
                    ));
                }
                if status != StatusCode::PARTIAL_CONTENT {
                    if status.is_server_error()
                        && attempt + 1 < MAX_SEGMENT_ATTEMPTS
                        && stop_check().is_none()
                    {
                        sleep(Duration::from_millis(40)).await;
                        continue;
                    }
                    abort_flag.store(true, Ordering::Release);
                    ctrl.wake.notify_waiters();
                    return ParallelSegmentResult::FatalError(format!(
                        "HTTP status error: parallel segment {seg_idx} expected HTTP 206 Partial Content but received {status}."
                    ));
                }

                if let Some(ct) = response
                    .headers()
                    .get(CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                {
                    let mut ct_guard = ct_lock.lock().unwrap();
                    if ct_guard.is_none() {
                        *ct_guard = Some(ct.to_owned());
                    }
                }

                let raw_cr = response
                    .headers()
                    .get(CONTENT_RANGE)
                    .and_then(|v| v.to_str().ok());
                let Some(raw_cr) = raw_cr else {
                    abort_flag.store(true, Ordering::Release);
                    ctrl.wake.notify_waiters();
                    return ParallelSegmentResult::FatalError(format!(
                        "Parallel segment {seg_idx} received HTTP 206 Partial Content without a Content-Range header."
                    ));
                };
                let Some((cr_start, cr_end, cr_total)) = parse_content_range(raw_cr) else {
                    abort_flag.store(true, Ordering::Release);
                    ctrl.wake.notify_waiters();
                    return ParallelSegmentResult::FatalError(format!(
                        "Parallel segment {seg_idx} received an invalid Content-Range header ({raw_cr})."
                    ));
                };
                if cr_start != req_start || cr_end != seg_end || cr_total != total_bytes {
                    abort_flag.store(true, Ordering::Release);
                    ctrl.wake.notify_waiters();
                    return ParallelSegmentResult::FatalError(format!(
                        "Parallel segment {seg_idx} received mismatched Content-Range (expected bytes {req_start}-{seg_end}/{total_bytes}, received {raw_cr})."
                    ));
                }
                if let Some(raw_cl) = response.headers().get(reqwest::header::CONTENT_LENGTH) {
                    let parsed_cl = raw_cl
                        .to_str()
                        .ok()
                        .and_then(|v| v.trim().parse::<u64>().ok());
                    if parsed_cl != Some(expected_remaining) {
                        abort_flag.store(true, Ordering::Release);
                        ctrl.wake.notify_waiters();
                        return ParallelSegmentResult::FatalError(format!(
                            "Parallel segment {seg_idx} Content-Length ({:?}) does not match expected byte count ({expected_remaining}).",
                            raw_cl
                        ));
                    }
                }

                let resp_etag = response
                    .headers()
                    .get(ETAG)
                    .and_then(|v| v.to_str().ok());
                let resp_lm = response
                    .headers()
                    .get(LAST_MODIFIED)
                    .and_then(|v| v.to_str().ok());
                let has_weak_etag = resp_etag.is_some_and(|v| v.trim_start().starts_with("W/"));
                let resp_val = resume_validator(resp_etag, resp_lm);
                {
                    let mut val_guard = val_lock.lock().unwrap();
                    if has_weak_etag
                        && resp_val.is_none()
                        && (val_guard.is_some()
                            || !initial_fresh_download
                            || already_downloaded > 0)
                    {
                        abort_flag.store(true, Ordering::Release);
                        ctrl.wake.notify_waiters();
                        return ParallelSegmentResult::FatalError(
                            "The server returned a weak file validator during the parallel range transfer."
                                .into(),
                        );
                    }
                    if let Some(actual_val) = resp_val {
                        if let Some(expected_val) = val_guard.as_deref() {
                            if expected_val != actual_val {
                                abort_flag.store(true, Ordering::Release);
                                ctrl.wake.notify_waiters();
                                return ParallelSegmentResult::FatalError(
                                    "The server's file validator changed during the parallel range transfer."
                                        .into(),
                                );
                            }
                        } else {
                            *val_guard = Some(actual_val.to_owned());
                        }
                    }
                }

                let mut stream = response.bytes_stream();
                let mut attempt_received = 0_u64;
                let mut stream_interrupted_error: Option<String> = None;

                loop {
                    if let Some(early) = stop_check() {
                        let _ = seg_file.flush().await;
                        return early;
                    }
                    let notified = ctrl.wake.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    if let Some(early) = stop_check() {
                        let _ = seg_file.flush().await;
                        return early;
                    }
                    let next = tokio::select! {
                        _ = &mut notified => continue,
                        chunk = stream.next() => chunk,
                    };
                    let Some(chunk) = next else {
                        break;
                    };
                    let chunk = match chunk {
                        Ok(c) => c,
                        Err(e) => {
                            stream_interrupted_error =
                                Some(format!("Transfer body error: {}", describe_reqwest_error(&e, &req_url)));
                            break;
                        }
                    };
                    let chunk_len = chunk.len() as u64;
                    if attempt_received.saturating_add(chunk_len) > expected_remaining {
                        let _ = seg_file.flush().await;
                        abort_flag.store(true, Ordering::Release);
                        ctrl.wake.notify_waiters();
                        return ParallelSegmentResult::FatalError(format!(
                            "Parallel segment {seg_idx} sent more data than its declared Content-Range."
                        ));
                    }
                    let limit = state
                        .lock()
                        .ok()
                        .and_then(|s| s.bandwidth_limit_bytes_per_second);
                    if let Err(e) = bw.wait_for_turn(chunk.len(), limit).await {
                        abort_flag.store(true, Ordering::Release);
                        ctrl.wake.notify_waiters();
                        return ParallelSegmentResult::FatalError(e);
                    }
                    if let Err(e) = seg_file.write_all(&chunk).await {
                        abort_flag.store(true, Ordering::Release);
                        ctrl.wake.notify_waiters();
                        return ParallelSegmentResult::FatalError(filesystem_error(
                            "could not write parallel segment data",
                            e,
                        ));
                    }
                    attempt_received = attempt_received.saturating_add(chunk_len);
                    let maybe_progress_update = {
                        let (snapshot_segs, sum_bytes) = {
                            let mut locked = segs_lock.lock().unwrap();
                            locked[seg_idx].downloaded_bytes =
                                already_downloaded.saturating_add(attempt_received);
                            let sum: u64 = locked.iter().map(|s| s.downloaded_bytes).sum();
                            (locked.clone(), sum)
                        };
                        let mut prog = prog_lock.lock().unwrap();
                        let dt = prog.last_update.elapsed();
                        if dt >= PROGRESS_INTERVAL {
                            let speed = sum_bytes.saturating_sub(prog.last_bytes) as f64
                                / dt.as_secs_f64().max(0.001);
                            prog.last_bytes = sum_bytes;
                            prog.last_update = Instant::now();
                            let elapsed_ms = base_elapsed_ms.saturating_add(
                                start_instant.elapsed().as_millis().min(u64::MAX as u128) as u64,
                            );
                            let remaining_seconds = (speed > 0.0)
                                .then(|| total_bytes.saturating_sub(sum_bytes) as f64 / speed);
                            Some((snapshot_segs, sum_bytes, speed, elapsed_ms, remaining_seconds))
                        } else {
                            None
                        }
                    };
                    if let Some((snapshot_segs, sum_bytes, speed, elapsed_ms, remaining_seconds)) =
                        maybe_progress_update
                    {
                        let _ = update_entry_optional(app, state, &entry_id, |entry| {
                            if entry.status == DownloadStatus::Downloading {
                                entry.downloaded_bytes = sum_bytes;
                                entry.segments = snapshot_segs;
                                entry.speed_bytes_per_second = speed;
                                entry.elapsed_ms = elapsed_ms;
                                entry.remaining_seconds = remaining_seconds;
                            }
                            Ok(())
                        });
                    }
                }

                let _ = seg_file.flush().await;
                let _ = seg_file.sync_data().await;
                drop(seg_file);

                if ctrl.is_cancelled() {
                    return ParallelSegmentResult::Cancelled;
                }
                if ctrl.is_pause_requested() {
                    return ParallelSegmentResult::Paused;
                }
                if ctrl.is_shutdown_requested() {
                    return ParallelSegmentResult::Shutdown;
                }

                if let Some(err) = stream_interrupted_error {
                    if attempt + 1 < MAX_SEGMENT_ATTEMPTS && stop_check().is_none() {
                        sleep(Duration::from_millis(40)).await;
                        continue;
                    }
                    abort_flag.store(true, Ordering::Release);
                    ctrl.wake.notify_waiters();
                    return ParallelSegmentResult::FatalError(err);
                }

                if attempt_received != expected_remaining {
                    if attempt + 1 < MAX_SEGMENT_ATTEMPTS && stop_check().is_none() {
                        sleep(Duration::from_millis(40)).await;
                        continue;
                    }
                    abort_flag.store(true, Ordering::Release);
                    ctrl.wake.notify_waiters();
                    return ParallelSegmentResult::FatalError(format!(
                        "Parallel segment {seg_idx} transfer ended after {attempt_received} bytes, but {expected_remaining} bytes were expected."
                    ));
                }

                match fs::metadata(&seg_path).await {
                    Ok(meta) if meta.len() == capacity => {
                        let (snapshot_segs, sum_bytes) = {
                            let mut locked = segs_lock.lock().unwrap();
                            locked[seg_idx].downloaded_bytes = capacity;
                            locked[seg_idx].completed = true;
                            let sum: u64 = locked.iter().map(|s| s.downloaded_bytes).sum();
                            (locked.clone(), sum)
                        };
                        let elapsed_ms = base_elapsed_ms.saturating_add(
                            start_instant.elapsed().as_millis().min(u64::MAX as u128) as u64,
                        );
                        let _ = update_entry_optional(app, state, &entry_id, |entry| {
                            entry.downloaded_bytes = sum_bytes;
                            entry.segments = snapshot_segs;
                            entry.elapsed_ms = elapsed_ms;
                            Ok(())
                        });
                        return ParallelSegmentResult::Completed;
                    }
                    _ => {
                        abort_flag.store(true, Ordering::Release);
                        ctrl.wake.notify_waiters();
                        return ParallelSegmentResult::FatalError(format!(
                            "Parallel segment {seg_idx} size on disk did not match expected capacity ({capacity} bytes)."
                        ));
                    }
                }
            }

            ParallelSegmentResult::FatalError(format!(
                "Parallel segment {seg_idx} exhausted its retry attempts."
            ))
        });
    }

    let results = futures_util::future::join_all(tasks).await;
    let elapsed_ms = base_elapsed_ms
        .saturating_add(start_instant.elapsed().as_millis().min(u64::MAX as u128) as u64);
    let (latest_segments, total_downloaded_now) = {
        let locked = shared_segments.lock().unwrap();
        let sum: u64 = locked.iter().map(|s| s.downloaded_bytes).sum();
        (locked.clone(), sum)
    };

    if control.is_cancelled()
        || (results
            .iter()
            .any(|r| matches!(r, ParallelSegmentResult::Cancelled))
            && !results.iter().any(|r| {
                matches!(
                    r,
                    ParallelSegmentResult::FatalError(_)
                        | ParallelSegmentResult::RangeUnsupported(_)
                )
            })
            && !control.is_pause_requested()
            && !control.is_shutdown_requested())
    {
        remove_all_segment_parts(destination, id).await;
        let _ = fs::remove_file(partial_path(destination)).await;
        update_entry_optional(app, state, id, |entry| {
            entry.downloaded_bytes = total_downloaded_now;
            entry.segments.clear();
            entry.elapsed_ms = elapsed_ms;
            Ok(())
        })?;
        finish_cancelled_optional(app, state, id)?;
        return Ok(ParallelDownloadOutcome::Handled);
    }

    if control.is_shutdown_requested()
        || results
            .iter()
            .any(|r| matches!(r, ParallelSegmentResult::Shutdown))
    {
        finish_shutdown_optional(state, id, |entry| {
            entry.downloaded_bytes = total_downloaded_now;
            entry.segments = latest_segments;
            entry.elapsed_ms = elapsed_ms;
            entry.speed_bytes_per_second = 0.0;
            entry.remaining_seconds = None;
        })?;
        return Ok(ParallelDownloadOutcome::Handled);
    }

    if control.is_pause_requested()
        || results
            .iter()
            .any(|r| matches!(r, ParallelSegmentResult::Paused))
    {
        update_entry_optional(app, state, id, |entry| {
            transition(entry, DownloadStatus::Paused)?;
            entry.downloaded_bytes = total_downloaded_now;
            entry.segments = latest_segments;
            entry.elapsed_ms = elapsed_ms;
            entry.speed_bytes_per_second = 0.0;
            entry.remaining_seconds = None;
            Ok(())
        })?;
        return Ok(ParallelDownloadOutcome::Handled);
    }

    if let Some(ParallelSegmentResult::RangeUnsupported(reason)) = results
        .iter()
        .find(|r| matches!(r, ParallelSegmentResult::RangeUnsupported(_)))
    {
        if initial_verified_bytes == 0 {
            remove_all_segment_parts(destination, id).await;
            let _ = fs::remove_file(partial_path(destination)).await;
            update_entry_optional(app, state, id, |entry| {
                entry.downloaded_bytes = 0;
                entry.segments.clear();
                Ok(())
            })?;
            return Ok(ParallelDownloadOutcome::FallbackToSequential);
        }
        update_entry_optional(app, state, id, |entry| {
            entry.downloaded_bytes = total_downloaded_now;
            entry.segments = latest_segments;
            entry.elapsed_ms = elapsed_ms;
            Ok(())
        })?;
        fail_entry_optional(app, state, id, reason.clone()).await?;
        return Ok(ParallelDownloadOutcome::Handled);
    }

    if let Some(ParallelSegmentResult::FatalError(error)) = results
        .iter()
        .find(|r| matches!(r, ParallelSegmentResult::FatalError(_)))
    {
        update_entry_optional(app, state, id, |entry| {
            entry.downloaded_bytes = total_downloaded_now;
            entry.segments = latest_segments;
            entry.elapsed_ms = elapsed_ms;
            Ok(())
        })?;
        fail_entry_optional(app, state, id, error.clone()).await?;
        return Ok(ParallelDownloadOutcome::Handled);
    }

    if let Err(error) = validate_segment_plan(&latest_segments, total_bytes) {
        fail_entry_optional(app, state, id, error).await?;
        return Ok(ParallelDownloadOutcome::Handled);
    }
    if latest_segments.iter().any(|s| !s.completed) || total_downloaded_now != total_bytes {
        fail_entry_optional(
            app,
            state,
            id,
            format!(
                "Parallel segment transfer incomplete: received {total_downloaded_now} of {total_bytes} bytes."
            ),
        )
        .await?;
        return Ok(ParallelDownloadOutcome::Handled);
    }

    let partial = partial_path(destination);
    let mut assembled = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&partial)
        .await
        .map_err(|e| filesystem_error("could not create assembled partial download file", e))?;

    let mut write_cursor = 0_u64;
    for seg in &latest_segments {
        if seg.start != write_cursor {
            drop(assembled);
            let _ = fs::remove_file(&partial).await;
            fail_entry_optional(
                app,
                state,
                id,
                format!(
                    "Parallel segment assembly offset mismatch at segment {} (expected {}, got {}).",
                    seg.index, write_cursor, seg.start
                ),
            )
            .await?;
            return Ok(ParallelDownloadOutcome::Handled);
        }
        let seg_path = segment_part_path(destination, id, seg.index);
        let expected_seg_len = seg.end - seg.start + 1;
        let meta = match fs::metadata(&seg_path).await {
            Ok(m) => m,
            Err(e) => {
                drop(assembled);
                let _ = fs::remove_file(&partial).await;
                return Err(filesystem_error(
                    "could not inspect completed parallel segment file",
                    e,
                ));
            }
        };
        if meta.len() != expected_seg_len {
            drop(assembled);
            let _ = fs::remove_file(&partial).await;
            fail_entry_optional(
                app,
                state,
                id,
                format!(
                    "Parallel segment {} file size ({}) does not match expected length ({expected_seg_len}).",
                    seg.index,
                    meta.len()
                ),
            )
            .await?;
            return Ok(ParallelDownloadOutcome::Handled);
        }
        let mut seg_reader = match fs::File::open(&seg_path).await {
            Ok(f) => f,
            Err(e) => {
                drop(assembled);
                let _ = fs::remove_file(&partial).await;
                return Err(filesystem_error(
                    "could not open completed parallel segment file",
                    e,
                ));
            }
        };
        let copied = match tokio::io::copy(&mut seg_reader, &mut assembled).await {
            Ok(c) => c,
            Err(e) => {
                drop(assembled);
                let _ = fs::remove_file(&partial).await;
                return Err(filesystem_error(
                    "could not assemble parallel segment data",
                    e,
                ));
            }
        };
        if copied != expected_seg_len {
            drop(assembled);
            let _ = fs::remove_file(&partial).await;
            fail_entry_optional(
                app,
                state,
                id,
                format!(
                    "Parallel segment {} copied {copied} bytes instead of {expected_seg_len} bytes.",
                    seg.index
                ),
            )
            .await?;
            return Ok(ParallelDownloadOutcome::Handled);
        }
        write_cursor = write_cursor.saturating_add(copied);
    }

    assembled
        .flush()
        .await
        .map_err(|e| filesystem_error("could not flush assembled download file", e))?;
    assembled
        .sync_data()
        .await
        .map_err(|e| filesystem_error("could not sync assembled download file", e))?;
    drop(assembled);

    if write_cursor != total_bytes {
        let _ = fs::remove_file(&partial).await;
        fail_entry_optional(
            app,
            state,
            id,
            format!(
                "Assembled parallel download size ({write_cursor}) does not match {total_bytes}."
            ),
        )
        .await?;
        return Ok(ParallelDownloadOutcome::Handled);
    }

    let validation_path = partial.clone();
    let validation_destination = destination.to_path_buf();
    let validation_url = url.to_owned();
    let validation_content_type = shared_content_type.lock().unwrap().clone();
    if let Err(validation_error) = tokio::task::spawn_blocking(move || {
        validate_mp4_completion(
            &validation_path,
            &validation_destination,
            &validation_url,
            validation_content_type.as_deref(),
        )
    })
    .await
    .map_err(|error| format!("The media validation worker could not finish: {error}"))?
    {
        remove_all_segment_parts(destination, id).await;
        let _ = update_entry_optional(app, state, id, |entry| {
            entry.segments.clear();
            Ok(())
        });
        return Err(format!(
            "{validation_error} The downloaded bytes remain in the temporary partial file; retry or remove the failed item."
        ));
    }

    remove_all_segment_parts(destination, id).await;
    #[cfg(test)]
    invoke_test_commit_hook("parallel_after_validation", id, control);
    if control.is_cancelled() {
        let _ = fs::remove_file(&partial).await;
        update_entry_optional(app, state, id, |entry| {
            entry.segments.clear();
            entry.elapsed_ms = elapsed_ms;
            Ok(())
        })?;
        finish_cancelled_optional(app, state, id)?;
        return Ok(ParallelDownloadOutcome::Handled);
    }
    if destination.exists() {
        fail_entry_optional(
            app,
            state,
            id,
            "The destination file appeared during the download; it was not overwritten.".into(),
        )
        .await?;
        return Ok(ParallelDownloadOutcome::Handled);
    }
    fs::rename(&partial, destination)
        .await
        .map_err(|error| filesystem_error("could not commit the completed download", error))?;
    #[cfg(test)]
    invoke_test_commit_hook("parallel_after_rename", id, control);
    let average_speed = if elapsed_ms > 0 {
        total_bytes as f64 / (elapsed_ms as f64 / 1000.0)
    } else {
        0.0
    };
    let completed = update_entry_optional(app, state, id, |entry| {
        if control.is_cancelled() || entry.status == DownloadStatus::Cancelled {
            return Err("The download was cancelled.".into());
        }
        transition(entry, DownloadStatus::Completed)?;
        entry.downloaded_bytes = total_bytes;
        entry.total_bytes = Some(total_bytes);
        entry.segments.clear();
        entry.speed_bytes_per_second = average_speed;
        entry.elapsed_ms = elapsed_ms;
        entry.remaining_seconds = Some(0.0);
        entry.error = None;
        Ok(())
    });
    match completed {
        Ok(completed) => {
            if completed.notify_on_complete {
                send_download_notification_optional(app, &completed, true);
            }
            Ok(ParallelDownloadOutcome::Handled)
        }
        Err(_) if control.is_cancelled() => {
            let _ = fs::remove_file(destination).await;
            finish_cancelled_optional(app, state, id)?;
            Ok(ParallelDownloadOutcome::Handled)
        }
        Err(error) => Err(error),
    }
}

struct WorkerGuard(Arc<AtomicUsize>);

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

async fn download_worker(
    app: Option<&tauri::AppHandle>,
    state: &Mutex<DownloadState>,
    concurrency: &Arc<ConcurrencyLimiter>,
    bandwidth: &Arc<BandwidthLimiter>,
    id: &str,
    control: &Arc<DownloadTaskControl>,
    restart: bool,
) -> Result<(), String> {
    let Some(_slot) = concurrency.acquire(state, control).await? else {
        if control.is_shutdown_requested() && !control.is_cancelled() {
            return finish_shutdown_optional(state, id, |_| {});
        }
        return finish_cancelled_optional(app, state, id);
    };
    update_entry_optional(app, state, id, |entry| {
        if entry.status == DownloadStatus::Queued || entry.status == DownloadStatus::Paused {
            transition(entry, DownloadStatus::Downloading)?;
        }
        Ok(())
    })?;
    let (
        mut url,
        destination,
        mut offset,
        mut total_bytes,
        mut etag,
        mut last_modified,
        base_elapsed_ms,
        hls_playlist,
        hls_completed_parts,
        mut persisted_segments,
        max_connections_per_download,
    ) = {
        let state = state
            .lock()
            .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
        let entry = state
            .entries
            .iter()
            .find(|entry| entry.id == id)
            .ok_or_else(|| "Download record was not found.".to_owned())?;
        (
            entry.url.clone(),
            PathBuf::from(&entry.destination),
            if restart { 0 } else { entry.downloaded_bytes },
            if restart { None } else { entry.total_bytes },
            if restart { None } else { entry.etag.clone() },
            if restart {
                None
            } else {
                entry.last_modified.clone()
            },
            if restart { 0 } else { entry.elapsed_ms },
            entry.hls_playlist.clone(),
            if restart {
                0
            } else {
                entry.hls_completed_parts.unwrap_or(0)
            },
            if restart {
                Vec::new()
            } else {
                entry.segments.clone()
            },
            state
                .max_connections_per_download
                .clamp(1, MAX_CONNECTIONS_PER_DOWNLOAD),
        )
    };
    if let Some(playlist) = hls_playlist {
        return download_hls_worker(
            app,
            state,
            bandwidth,
            id,
            control,
            url,
            destination,
            playlist,
            if restart { 0 } else { offset },
            hls_completed_parts,
            if restart { 0 } else { base_elapsed_ms },
            restart,
        )
        .await;
    }
    let parsed_url = match validated_download_url(&url).and_then(|u| normalize_instagram_media_url(&u)) {
        Ok(normalized) => normalized,
        Err(message) => return fail_entry_optional(app, state, id, message).await,
    };
    if is_signed_url_expired(&parsed_url) {
        return fail_entry_optional(
            app,
            state,
            id,
            "The signed download URL has expired and cannot be resumed. Obtain a fresh download link to retry.".into(),
        )
        .await;
    }
    if parsed_url.as_str() != url {
        url = parsed_url.to_string();
        update_entry_optional(app, state, id, |entry| {
            entry.url = url.clone();
            Ok(())
        })?;
    }
    let is_instagram_cdn = is_instagram_cdn_url(&parsed_url);
    let partial = partial_path(&destination);
    if restart {
        remove_all_segment_parts(&destination, id).await;
        persisted_segments.clear();
        match fs::metadata(&partial).await {
            Ok(_) => fs::remove_file(&partial).await.map_err(|error| {
                filesystem_error("could not remove the previous partial file", error)
            })?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(filesystem_error(
                    "could not inspect the previous partial file",
                    error,
                ));
            }
        }
    }
    if destination.exists() {
        return fail_entry_optional(
            app,
            state,
            id,
            "The destination file already exists; choose a different filename.".into(),
        )
        .await;
    }

    let client = build_http_client()?;
    let mut allow_parallel_upgrade =
        !is_instagram_cdn && max_connections_per_download > 1 && offset == 0;

    // If resuming an already-segmented parallel download, dispatch directly to parallel segment worker
    if !persisted_segments.is_empty() {
        if let Some(total) = total_bytes {
            match download_parallel_segments_worker(
                app,
                state,
                bandwidth,
                &client,
                id,
                control,
                &url,
                &destination,
                total,
                persisted_segments,
                etag.clone(),
                last_modified.clone(),
                None,
                base_elapsed_ms,
                max_connections_per_download,
                false,
            )
            .await?
            {
                ParallelDownloadOutcome::Handled => return Ok(()),
                ParallelDownloadOutcome::FallbackToSequential => {
                    allow_parallel_upgrade = false;
                    offset = 0;
                    total_bytes = None;
                }
            }
        } else {
            remove_all_segment_parts(&destination, id).await;
            persisted_segments.clear();
            offset = 0;
        }
    }

    if offset > 0 {
        match fs::symlink_metadata(&partial).await {
            Ok(metadata) if metadata.file_type().is_file() && metadata.len() == offset => {}
            Ok(_) => {
                return fail_entry_optional(
                    app,
                    state,
                    id,
                    "The partial file size does not match the recorded offset. Use Retry and choose restart to avoid corrupting the file.".into(),
                )
                .await;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return fail_entry_optional(
                    app,
                    state,
                    id,
                    "The partial file is missing. Use Retry and choose restart to download from zero.".into(),
                )
                .await;
            }
            Err(error) => {
                return Err(filesystem_error(
                    "could not inspect the partial file",
                    error,
                ));
            }
        }
        if total_bytes.is_some_and(|total| offset > total) {
            return fail_entry_optional(
                app,
                state,
                id,
                "The server's range metadata conflicts with the partial file. Use Retry to restart safely.".into(),
            )
            .await;
        }
    }

    if offset == 0 {
        if let Ok(meta) = fs::metadata(&partial).await {
            if meta.len() == 0 {
                let _ = fs::remove_file(&partial).await;
            }
        }
    }

    let mut output = OpenOptions::new()
        .create(offset == 0)
        .create_new(offset == 0)
        .write(true)
        .append(offset > 0)
        .truncate(offset == 0)
        .open(&partial)
        .await
        .map_err(|error| filesystem_error("could not open the partial download file", error))?;
    let file_length = output
        .metadata()
        .await
        .map_err(|error| filesystem_error("could not inspect the partial download file", error))?
        .len();
    if file_length != offset {
        return fail_entry_optional(
            app,
            state,
            id,
            "The partial file changed while the download was starting. Use Retry and choose restart to avoid corrupting the file."
                .into(),
        )
        .await;
    }

    let start = Instant::now();
    let mut last_update = Instant::now();
    let mut last_bytes = offset;
    let mut speed = 0.0;
    let mut response_content_type: Option<String> = None;
    let mut force_range_request = offset > 0;
    let mut explicit_range_end: Option<u64> = None;
    let mut range_requests_count = 0_usize;
    const MAX_RANGE_REQUESTS: usize = 4096;

    while total_bytes.is_none() || offset < total_bytes.unwrap_or(0) {
        if control.cancelled.load(Ordering::Acquire) {
            drop(output);
            let _ = fs::remove_file(&partial).await;
            remove_all_segment_parts(&destination, id).await;
            let elapsed_ms = base_elapsed_ms
                .saturating_add(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
            update_entry_optional(app, state, id, |entry| {
                entry.downloaded_bytes = 0;
                entry.elapsed_ms = elapsed_ms;
                Ok(())
            })?;
            return finish_cancelled_optional(app, state, id);
        }
        if control.shutdown_requested.load(Ordering::Acquire) {
            output
                .flush()
                .await
                .map_err(|error| filesystem_error("could not flush the partial file", error))?;
            let elapsed_ms = base_elapsed_ms
                .saturating_add(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
            return finish_shutdown_optional(state, id, |entry| {
                entry.downloaded_bytes = offset;
                entry.elapsed_ms = elapsed_ms;
                entry.speed_bytes_per_second = 0.0;
                entry.remaining_seconds = None;
            });
        }
        if control.pause_requested.load(Ordering::Acquire) {
            output
                .flush()
                .await
                .map_err(|error| filesystem_error("could not flush the partial file", error))?;
            let elapsed_ms = base_elapsed_ms
                .saturating_add(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
            update_entry_optional(app, state, id, |entry| {
                transition(entry, DownloadStatus::Paused)?;
                entry.downloaded_bytes = offset;
                entry.elapsed_ms = elapsed_ms;
                entry.speed_bytes_per_second = 0.0;
                entry.remaining_seconds = None;
                Ok(())
            })?;
            return Ok(());
        }
        range_requests_count = range_requests_count.saturating_add(1);
        if range_requests_count > MAX_RANGE_REQUESTS {
            return fail_entry_optional(
                app,
                state,
                id,
                "The download required too many sequential byte-range requests to complete safely.".into(),
            )
            .await;
        }

        let mut request = client.get(&url);
        if is_instagram_cdn {
            request = request
                .header(reqwest::header::REFERER, "https://www.instagram.com/")
                .header(reqwest::header::ORIGIN, "https://www.instagram.com");
        }
        let requested_range = offset > 0 || force_range_request;
        if requested_range {
            let range_header_value = match explicit_range_end {
                Some(end) if end >= offset => format!("bytes={offset}-{end}"),
                _ => format!("bytes={offset}-"),
            };
            request = request.header(RANGE, range_header_value);
        }

        let response = tokio::select! {
            response = request.send() => response
                .map_err(|error| describe_reqwest_error(&error, &url))?,
            _ = control.wake.notified() => {
                output
                    .flush()
                    .await
                    .map_err(|error| filesystem_error("could not flush the partial file", error))?;
                let elapsed_ms = base_elapsed_ms
                    .saturating_add(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
                if control.cancelled.load(Ordering::Acquire) {
                    drop(output);
                    let _ = fs::remove_file(&partial).await;
                    remove_all_segment_parts(&destination, id).await;
                    update_entry_optional(app, state, id, |entry| {
                        entry.downloaded_bytes = 0;
                        entry.elapsed_ms = elapsed_ms;
                        Ok(())
                    })?;
                    return finish_cancelled_optional(app, state, id);
                }
                if control.shutdown_requested.load(Ordering::Acquire) {
                    return finish_shutdown_optional(state, id, |entry| {
                        entry.downloaded_bytes = offset;
                        entry.elapsed_ms = elapsed_ms;
                        entry.speed_bytes_per_second = 0.0;
                        entry.remaining_seconds = None;
                    });
                }
                if control.pause_requested.load(Ordering::Acquire) {
                    update_entry_optional(app, state, id, |entry| {
                        transition(entry, DownloadStatus::Paused)?;
                        entry.downloaded_bytes = offset;
                        entry.elapsed_ms = elapsed_ms;
                        entry.speed_bytes_per_second = 0.0;
                        entry.remaining_seconds = None;
                        Ok(())
                    })?;
                    return Ok(());
                }
                return Err("The download worker was interrupted before the request completed.".into());
            }
        };

        let final_url = response.url();
        if !matches!(final_url.scheme(), "http" | "https")
            || final_url.host_str().is_none()
            || !final_url.username().is_empty()
            || final_url.password().is_some()
        {
            return fail_entry_optional(
                app,
                state,
                id,
                "The download redirected to an unsupported or credential-bearing URL.".into(),
            )
            .await;
        }

        let status = response.status();
        if matches!(
            status,
            StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED | StatusCode::GONE
        ) {
            return fail_entry_optional(
                app,
                state,
                id,
                format!(
                    "HTTP status error: the server returned {status} (the video source URL is expired, unauthorized, or no longer available)."
                ),
            )
            .await;
        }
        if !status.is_success() {
            return fail_entry_optional(
                app,
                state,
                id,
                format!("HTTP status error: the server returned {status}."),
            )
            .await;
        }

        let content_range_header = response
            .headers()
            .get(CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let response_content_length = response.content_length();
        if let Some(ct) = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
        {
            response_content_type = Some(ct.to_owned());
        }
        let response_etag = response
            .headers()
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let response_last_modified = response
            .headers()
            .get(LAST_MODIFIED)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);

        let advertises_ranges = response_advertises_byte_ranges(response.headers());
        let expected_chunk_bytes: Option<u64>;
        if status == StatusCode::OK {
            if requested_range {
                return fail_entry_optional(
                    app,
                    state,
                    id,
                    "The server did not honor the Range request (returned HTTP 200 instead of HTTP 206 Partial Content). The partial file was not appended; use Retry to safely restart from zero.".into(),
                )
                .await;
            }
            if content_range_header.is_some() {
                return fail_entry_optional(
                    app,
                    state,
                    id,
                    "The server returned only a partial byte-range response with HTTP 200. FluxNet cannot save it as a complete media file.".into(),
                )
                .await;
            }
            expected_chunk_bytes = response_content_length;
            total_bytes = response_content_length;
            if etag.is_none() {
                etag = response_etag.clone();
            }
            if last_modified.is_none() {
                last_modified = response_last_modified.clone();
            }
            if allow_parallel_upgrade && advertises_ranges {
                if let Some(total) = total_bytes.filter(|t| *t >= MIN_PARALLEL_FILE_BYTES) {
                    let planned = plan_parallel_segments(total, max_connections_per_download);
                    if !planned.is_empty() {
                        drop(response);
                        drop(output);
                        let _ = fs::remove_file(&partial).await;
                        match download_parallel_segments_worker(
                            app,
                            state,
                            bandwidth,
                            &client,
                            id,
                            control,
                            &url,
                            &destination,
                            total,
                            planned,
                            etag.clone(),
                            last_modified.clone(),
                            response_content_type.clone(),
                            base_elapsed_ms,
                            max_connections_per_download,
                            true,
                        )
                        .await?
                        {
                            ParallelDownloadOutcome::Handled => return Ok(()),
                            ParallelDownloadOutcome::FallbackToSequential => {
                                allow_parallel_upgrade = false;
                                offset = 0;
                                total_bytes = None;
                                output = OpenOptions::new()
                                    .create(true)
                                    .write(true)
                                    .truncate(true)
                                    .open(&partial)
                                    .await
                                    .map_err(|error| {
                                        filesystem_error(
                                            "could not reopen partial download file after parallel fallback",
                                            error,
                                        )
                                    })?;
                                continue;
                            }
                        }
                    }
                }
            }
        } else if status == StatusCode::PARTIAL_CONTENT {
            let Some(raw_cr) = content_range_header.as_deref() else {
                return fail_entry_optional(
                    app,
                    state,
                    id,
                    "The server returned HTTP 206 Partial Content without a Content-Range header.".into(),
                )
                .await;
            };
            let Some((range_start, range_end, range_total)) = parse_content_range(raw_cr) else {
                return fail_entry_optional(
                    app,
                    state,
                    id,
                    "The server returned HTTP 206 Partial Content with an invalid Content-Range header.".into(),
                )
                .await;
            };
            if range_start != offset {
                return fail_entry_optional(
                    app,
                    state,
                    id,
                    format!(
                        "The server returned a Content-Range with the wrong starting offset (expected byte {offset}, received byte {range_start})."
                    ),
                )
                .await;
            }
            if let Some(expected_total) = total_bytes {
                if range_total != expected_total {
                    return fail_entry_optional(
                        app,
                        state,
                        id,
                        format!(
                            "The server's Content-Range total size ({range_total}) conflicts with the previously declared size ({expected_total})."
                        ),
                    )
                    .await;
                }
            }
            let expected_validator = resume_validator(etag.as_deref(), last_modified.as_deref());
            let response_validator = resume_validator(
                response_etag.as_deref(),
                response_last_modified.as_deref(),
            );
            if offset > 0
                && expected_validator.is_some()
                && response_validator.is_some()
                && expected_validator != response_validator
            {
                return fail_entry_optional(
                    app,
                    state,
                    id,
                    "The server's file validator changed during the range transfer. The partial file was not appended; use Retry to restart safely.".into(),
                )
                .await;
            }
            let chunk_len = range_end - range_start + 1;
            if let Some(declared_len) = response_content_length {
                if declared_len != chunk_len {
                    return fail_entry_optional(
                        app,
                        state,
                        id,
                        format!(
                            "The server's Content-Length ({declared_len}) does not match the Content-Range byte count ({chunk_len})."
                        ),
                    )
                    .await;
                }
            }
            total_bytes = Some(range_total);
            expected_chunk_bytes = Some(chunk_len);
            if etag.is_none() {
                etag = response_etag.clone();
            }
            if last_modified.is_none() {
                last_modified = response_last_modified.clone();
            }
        } else {
            return fail_entry_optional(
                app,
                state,
                id,
                format!(
                    "HTTP status error: expected HTTP 200 or 206 Partial Content but received {status}."
                ),
            )
            .await;
        }

        let current_etag = etag.clone();
        let current_last_modified = last_modified.clone();
        update_entry_optional(app, state, id, |entry| {
            entry.total_bytes = total_bytes;
            entry.error = None;
            entry.etag = current_etag;
            entry.last_modified = current_last_modified;
            Ok(())
        })?;

        let mut stream = response.bytes_stream();
        let mut chunk_received = 0_u64;

        loop {
            if control.cancelled.load(Ordering::Acquire) {
                drop(output);
                let _ = fs::remove_file(&partial).await;
                remove_all_segment_parts(&destination, id).await;
                let elapsed_ms = base_elapsed_ms
                    .saturating_add(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
                update_entry_optional(app, state, id, |entry| {
                    entry.downloaded_bytes = 0;
                    entry.elapsed_ms = elapsed_ms;
                    Ok(())
                })?;
                return finish_cancelled_optional(app, state, id);
            }
            if control.shutdown_requested.load(Ordering::Acquire) {
                output
                    .flush()
                    .await
                    .map_err(|error| filesystem_error("could not flush the partial file", error))?;
                let elapsed_ms = base_elapsed_ms
                    .saturating_add(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
                return finish_shutdown_optional(state, id, |entry| {
                    entry.downloaded_bytes = offset;
                    entry.elapsed_ms = elapsed_ms;
                    entry.speed_bytes_per_second = 0.0;
                    entry.remaining_seconds = None;
                });
            }
            if control.pause_requested.load(Ordering::Acquire) {
                output
                    .flush()
                    .await
                    .map_err(|error| filesystem_error("could not flush the partial file", error))?;
                let elapsed_ms = base_elapsed_ms
                    .saturating_add(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
                update_entry_optional(app, state, id, |entry| {
                    transition(entry, DownloadStatus::Paused)?;
                    entry.downloaded_bytes = offset;
                    entry.elapsed_ms = elapsed_ms;
                    entry.speed_bytes_per_second = 0.0;
                    entry.remaining_seconds = None;
                    Ok(())
                })?;
                return Ok(());
            }
            let next = tokio::select! {
                _ = control.wake.notified() => continue,
                chunk = stream.next() => chunk,
            };
            let Some(chunk) = next else {
                break;
            };
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    let _ = output.flush().await;
                    let elapsed_ms = base_elapsed_ms
                        .saturating_add(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
                    update_entry_optional(app, state, id, |entry| {
                        entry.downloaded_bytes = offset;
                        entry.elapsed_ms = elapsed_ms;
                        Ok(())
                    })?;
                    return Err(format!(
                        "Transfer body error: {}",
                        describe_reqwest_error(&error, &url)
                    ));
                }
            };
            let chunk_len = chunk.len() as u64;
            let next_chunk_received = chunk_received.saturating_add(chunk_len);
            let next_offset = offset.saturating_add(chunk_len);
            if expected_chunk_bytes.is_some_and(|expected| next_chunk_received > expected)
                || total_bytes.is_some_and(|total| next_offset > total)
            {
                let _ = output.flush().await;
                let elapsed_ms = base_elapsed_ms
                    .saturating_add(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
                update_entry_optional(app, state, id, |entry| {
                    entry.downloaded_bytes = offset;
                    entry.elapsed_ms = elapsed_ms;
                    Ok(())
                })?;
                return fail_entry_optional(
                    app,
                    state,
                    id,
                    "The server sent more data than its declared byte range or file size.".into(),
                )
                .await;
            }
            let limit = state
                .lock()
                .map_err(|_| "Download manager state lock was poisoned.".to_owned())?
                .bandwidth_limit_bytes_per_second;
            bandwidth.wait_for_turn(chunk.len(), limit).await?;
            if let Err(error) = output.write_all(&chunk).await {
                let elapsed_ms = base_elapsed_ms
                    .saturating_add(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
                update_entry_optional(app, state, id, |entry| {
                    entry.downloaded_bytes = offset;
                    entry.elapsed_ms = elapsed_ms;
                    Ok(())
                })?;
                return Err(filesystem_error("could not write downloaded data", error));
            }
            chunk_received = next_chunk_received;
            offset = next_offset;
            if last_update.elapsed() >= PROGRESS_INTERVAL {
                let elapsed = start.elapsed().as_secs_f64();
                speed =
                    offset.saturating_sub(last_bytes) as f64 / last_update.elapsed().as_secs_f64();
                last_bytes = offset;
                last_update = Instant::now();
                let elapsed_ms =
                    base_elapsed_ms.saturating_add(elapsed.mul_add(1000.0, 0.0) as u64);
                let remaining_seconds = total_bytes
                    .filter(|_| speed > 0.0)
                    .map(|total| total.saturating_sub(offset) as f64 / speed);
                update_entry_optional(app, state, id, |entry| {
                    entry.downloaded_bytes = offset;
                    entry.speed_bytes_per_second = speed;
                    entry.elapsed_ms = elapsed_ms;
                    entry.remaining_seconds = remaining_seconds;
                    entry.total_bytes = total_bytes;
                    Ok(())
                })?;
            }
        }

        output
            .flush()
            .await
            .map_err(|error| filesystem_error("could not flush the downloaded file", error))?;

        if control.cancelled.load(Ordering::Acquire) {
            drop(output);
            let _ = fs::remove_file(&partial).await;
            remove_all_segment_parts(&destination, id).await;
            let elapsed_ms = base_elapsed_ms
                .saturating_add(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
            update_entry_optional(app, state, id, |entry| {
                entry.downloaded_bytes = 0;
                entry.elapsed_ms = elapsed_ms;
                Ok(())
            })?;
            return finish_cancelled_optional(app, state, id);
        }
        if let Some(expected) = expected_chunk_bytes {
            if chunk_received != expected {
                let elapsed_ms = base_elapsed_ms
                    .saturating_add(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
                update_entry_optional(app, state, id, |entry| {
                    entry.downloaded_bytes = offset;
                    entry.elapsed_ms = elapsed_ms;
                    Ok(())
                })?;
                return fail_entry_optional(
                    app,
                    state,
                    id,
                    format!(
                        "Transfer ended after {chunk_received} bytes, but the server declared {expected} bytes for the current response."
                    ),
                )
                .await;
            }
        }
        if chunk_received == 0 {
            return fail_entry_optional(
                app,
                state,
                id,
                "The server returned an empty media response body.".into(),
            )
            .await;
        }

        if status == StatusCode::PARTIAL_CONTENT {
            let declared_total = total_bytes.unwrap_or(offset);
            if offset < declared_total {
                force_range_request = true;
                explicit_range_end = None;
                update_entry_optional(app, state, id, |entry| {
                    entry.downloaded_bytes = offset;
                    entry.total_bytes = total_bytes;
                    Ok(())
                })?;
                continue;
            }
            break;
        }

        // HTTP 200 response completed. Do not assume stripping bytestart/byteend always
        // returned the complete representation: inspect the MP4 segment index (sidx) if present.
        if let Ok(Some(sidx_total)) = inspect_mp4_sidx_expected_bytes(&partial) {
            if offset < sidx_total {
                total_bytes = Some(sidx_total);
                force_range_request = true;
                explicit_range_end = Some(sidx_total - 1);
                update_entry_optional(app, state, id, |entry| {
                    entry.downloaded_bytes = offset;
                    entry.total_bytes = total_bytes;
                    Ok(())
                })?;
                continue;
            }
        }
        break;
    }

    output
        .flush()
        .await
        .map_err(|error| filesystem_error("could not flush the downloaded file", error))?;
    output
        .sync_data()
        .await
        .map_err(|error| filesystem_error("could not sync the downloaded file", error))?;
    drop(output);
    if control.cancelled.load(Ordering::Acquire) {
        let elapsed_ms = base_elapsed_ms
            .saturating_add(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
        update_entry_optional(app, state, id, |entry| {
            entry.downloaded_bytes = offset;
            entry.elapsed_ms = elapsed_ms;
            Ok(())
        })?;
        return finish_cancelled_optional(app, state, id);
    }
    if control.shutdown_requested.load(Ordering::Acquire) {
        let elapsed_ms = base_elapsed_ms
            .saturating_add(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
        return finish_shutdown_optional(state, id, |entry| {
            entry.downloaded_bytes = offset;
            entry.elapsed_ms = elapsed_ms;
            entry.speed_bytes_per_second = 0.0;
            entry.remaining_seconds = None;
        });
    }
    if control.pause_requested.load(Ordering::Acquire) {
        let elapsed_ms = base_elapsed_ms
            .saturating_add(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
        update_entry_optional(app, state, id, |entry| {
            transition(entry, DownloadStatus::Paused)?;
            entry.downloaded_bytes = offset;
            entry.elapsed_ms = elapsed_ms;
            entry.speed_bytes_per_second = 0.0;
            entry.remaining_seconds = None;
            Ok(())
        })?;
        return Ok(());
    }
    if offset == 0 || total_bytes.is_some_and(|total| offset != total) {
        let elapsed_ms = base_elapsed_ms
            .saturating_add(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
        update_entry_optional(app, state, id, |entry| {
            entry.downloaded_bytes = offset;
            entry.elapsed_ms = elapsed_ms;
            Ok(())
        })?;
        return fail_entry_optional(
            app,
            state,
            id,
            format!(
                "Transfer ended at {offset} bytes, but the server declared {} bytes.",
                total_bytes.unwrap_or_default()
            ),
        )
        .await;
    }
    let actual_bytes = fs::metadata(&partial)
        .await
        .map_err(|error| filesystem_error("could not inspect the downloaded partial file", error))?
        .len();
    if actual_bytes != offset {
        return fail_entry_optional(
            app,
            state,
            id,
            "The downloaded file size on disk does not match the verified byte count.".into(),
        )
        .await;
    }
    let elapsed_ms =
        base_elapsed_ms.saturating_add(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
    update_entry_optional(app, state, id, |entry| {
        entry.downloaded_bytes = offset;
        entry.elapsed_ms = elapsed_ms;
        Ok(())
    })?;
    let validation_path = partial.clone();
    let validation_destination = destination.clone();
    let validation_url = url.clone();
    let validation_content_type = response_content_type.clone();
    tokio::task::spawn_blocking(move || {
        validate_mp4_completion(
            &validation_path,
            &validation_destination,
            &validation_url,
            validation_content_type.as_deref(),
        )
    })
    .await
    .map_err(|error| format!("The media validation worker could not finish: {error}"))?
    .map_err(|error| {
        format!(
            "{error} The downloaded bytes remain in the temporary partial file; retry or remove the failed item."
        )
    })?;
    #[cfg(test)]
    invoke_test_commit_hook("sequential_after_validation", id, control);
    if control.cancelled.load(Ordering::Acquire) {
        return finish_cancelled_optional(app, state, id);
    }
    if destination.exists() {
        return fail_entry_optional(
            app,
            state,
            id,
            "The destination file appeared during the download; it was not overwritten.".into(),
        )
        .await;
    }
    fs::rename(&partial, &destination)
        .await
        .map_err(|error| filesystem_error("could not commit the completed download", error))?;
    #[cfg(test)]
    invoke_test_commit_hook("sequential_after_rename", id, control);
    let average_speed = if elapsed_ms > 0 {
        offset as f64 / (elapsed_ms as f64 / 1000.0)
    } else {
        speed
    };
    let completed = update_entry_optional(app, state, id, |entry| {
        if control.cancelled.load(Ordering::Acquire) || entry.status == DownloadStatus::Cancelled {
            return Err("The download was cancelled.".into());
        }
        transition(entry, DownloadStatus::Completed)?;
        entry.downloaded_bytes = offset;
        entry.total_bytes = Some(offset);
        entry.speed_bytes_per_second = average_speed;
        entry.elapsed_ms = elapsed_ms;
        entry.remaining_seconds = Some(0.0);
        entry.error = None;
        Ok(())
    });
    match completed {
        Ok(completed) => {
            if completed.notify_on_complete {
                send_download_notification_optional(app, &completed, true);
            }
            Ok(())
        }
        Err(_) if control.cancelled.load(Ordering::Acquire) => {
            let _ = fs::remove_file(&destination).await;
            finish_cancelled_optional(app, state, id)
        }
        Err(error) => Err(error),
    }
}

enum HlsPartOutcome {
    Complete(u64),
    Paused,
    Shutdown,
    Cancelled,
}

fn hls_stop_reason(control: &DownloadTaskControl) -> Option<HlsPartOutcome> {
    if control.cancelled.load(Ordering::Acquire) {
        Some(HlsPartOutcome::Cancelled)
    } else if control.pause_requested.load(Ordering::Acquire) {
        Some(HlsPartOutcome::Paused)
    } else if control.shutdown_requested.load(Ordering::Acquire) {
        Some(HlsPartOutcome::Shutdown)
    } else {
        None
    }
}

fn hls_staging_path(destination: &Path, id: &str) -> PathBuf {
    let mut name = destination.file_name().unwrap_or_default().to_os_string();
    name.push(format!(".{id}.fluxnet-segment-part"));
    destination.with_file_name(name)
}

fn ordered_hls_resources(playlist: &HlsPlaylist) -> Vec<&HlsResource> {
    let mut resources = Vec::with_capacity(playlist.segments.len() + 1);
    resources.push(&playlist.init);
    resources.extend(playlist.segments.iter());
    resources
}

fn validate_hls_destination(destination: &Path) -> Result<(), String> {
    if destination
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("mp4"))
    {
        Ok(())
    } else {
        Err(
            "The selected HLS playlist contains fMP4 segments and requires an .mp4 destination. FluxNet will not change the selected extension."
                .into(),
        )
    }
}

fn validate_hls_part_response(
    resource: &HlsResource,
    status: StatusCode,
    content_range: Option<&str>,
    content_length: Option<u64>,
) -> Result<(), String> {
    if let Some(range) = &resource.byte_range {
        let content_range = content_range.and_then(parse_content_range);
        let last_byte = range
            .length
            .checked_sub(1)
            .ok_or_else(|| "The HLS byte range has an invalid length.".to_owned())?;
        let expected_end = range
            .offset
            .checked_add(last_byte)
            .ok_or_else(|| "The HLS byte range exceeds the supported size.".to_owned())?;
        if status != StatusCode::PARTIAL_CONTENT
            || !content_range.is_some_and(|(start, end, total)| {
                start == range.offset && end == expected_end && total > expected_end
            })
            || content_length.is_some_and(|length| length != range.length)
        {
            return Err(
                "The HLS server did not return the exact declared byte range (HTTP 206 and matching Content-Range are required)."
                    .into(),
            );
        }
    } else if status != StatusCode::OK {
        return Err(format!(
            "The HLS server returned HTTP {status}; a full segment response (HTTP 200) is required."
        ));
    }
    if content_length.is_some_and(|length| length > MAX_HLS_EXPECTED_BYTES) {
        return Err("An HLS segment exceeds the 2 GiB safety limit.".into());
    }
    Ok(())
}

fn validate_hls_part_length(
    received: u64,
    expected_part_bytes: Option<u64>,
    content_length: Option<u64>,
) -> Result<(), String> {
    if received == 0
        || expected_part_bytes.is_some_and(|expected| received != expected)
        || content_length.is_some_and(|expected| received != expected)
    {
        Err("The HLS segment is empty or truncated.".into())
    } else {
        Ok(())
    }
}

async fn remove_hls_staging(path: &Path) -> Result<(), String> {
    match fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_file() || metadata.file_type().is_symlink() => {
            fs::remove_file(path).await.map_err(|error| {
                filesystem_error("could not remove an incomplete HLS segment", error)
            })
        }
        Ok(_) => Err("The temporary HLS segment path is not a regular file.".into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(filesystem_error(
            "could not inspect an incomplete HLS segment",
            error,
        )),
    }
}

async fn download_hls_part(
    app: Option<&tauri::AppHandle>,
    state: &Mutex<DownloadState>,
    bandwidth: &BandwidthLimiter,
    client: &Client,
    id: &str,
    control: &DownloadTaskControl,
    resource: &HlsResource,
    staging: &Path,
    committed_bytes: u64,
    total_bytes: Option<u64>,
    base_elapsed_ms: u64,
    transfer_started: Instant,
) -> Result<HlsPartOutcome, String> {
    remove_hls_staging(staging).await?;
    if let Some(reason) = hls_stop_reason(control) {
        return Ok(reason);
    }

    let mut request = client.get(&resource.url);
    let expected_part_bytes = resource.byte_range.as_ref().map(|range| range.length);
    if let Some(range) = &resource.byte_range {
        let last_byte = range
            .length
            .checked_sub(1)
            .ok_or_else(|| "The HLS byte range has an invalid length.".to_owned())?;
        let end = range
            .offset
            .checked_add(last_byte)
            .ok_or_else(|| "The HLS byte range exceeds the supported size.".to_owned())?;
        request = request.header(RANGE, format!("bytes={}-{}", range.offset, end));
    }
    let response = tokio::select! {
        response = request.send() => response.map_err(|error| describe_reqwest_error(&error, &resource.url))?,
        _ = control.wake.notified() => {
            if let Some(reason) = hls_stop_reason(control) {
                return Ok(reason);
            }
            return Err("The HLS segment request was interrupted.".into());
        }
    };
    let status = response.status();
    let final_url = response.url();
    if !matches!(final_url.scheme(), "http" | "https")
        || final_url.host_str().is_none()
        || !final_url.username().is_empty()
        || final_url.password().is_some()
    {
        return Err("The HLS segment redirected to an unsupported URL.".into());
    }
    let response_content_length = response.content_length();
    let content_range = response
        .headers()
        .get(CONTENT_RANGE)
        .and_then(|value| value.to_str().ok());
    validate_hls_part_response(resource, status, content_range, response_content_length)?;

    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(staging)
        .await
        .map_err(|error| filesystem_error("could not create the temporary HLS segment", error))?;
    let mut received = 0_u64;
    let mut stream = response.bytes_stream();
    let mut last_update = Instant::now();
    loop {
        if let Some(reason) = hls_stop_reason(control) {
            output.flush().await.map_err(|error| {
                filesystem_error("could not flush the temporary HLS segment", error)
            })?;
            drop(output);
            remove_hls_staging(staging).await?;
            return Ok(reason);
        }
        let next = tokio::select! {
            _ = control.wake.notified() => continue,
            chunk = stream.next() => chunk,
        };
        let Some(chunk) = next else {
            break;
        };
        let chunk = chunk.map_err(|error| {
            format!(
                "HLS segment transfer failed: {}",
                describe_reqwest_error(&error, &resource.url)
            )
        })?;
        let next_received = received
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| "The HLS segment size overflowed.".to_owned())?;
        let cumulative = committed_bytes
            .checked_add(next_received)
            .ok_or_else(|| "The HLS download size overflowed.".to_owned())?;
        if expected_part_bytes.is_some_and(|expected| next_received > expected)
            || cumulative > MAX_HLS_EXPECTED_BYTES
            || total_bytes.is_some_and(|total| cumulative > total)
        {
            return Err("The HLS server sent more data than the playlist permits.".into());
        }
        let limit = state
            .lock()
            .map_err(|_| "Download manager state lock was poisoned.".to_owned())?
            .bandwidth_limit_bytes_per_second;
        bandwidth.wait_for_turn(chunk.len(), limit).await?;
        output
            .write_all(&chunk)
            .await
            .map_err(|error| filesystem_error("could not write an HLS segment", error))?;
        received = next_received;
        if last_update.elapsed() >= PROGRESS_INTERVAL {
            let elapsed_ms =
                base_elapsed_ms.saturating_add(
                    transfer_started.elapsed().as_millis().min(u64::MAX as u128) as u64,
                );
            update_entry_optional(app, state, id, |entry| {
                entry.downloaded_bytes = committed_bytes;
                entry.total_bytes = total_bytes;
                entry.elapsed_ms = elapsed_ms;
                entry.remaining_seconds = None;
                entry.speed_bytes_per_second =
                    received as f64 / transfer_started.elapsed().as_secs_f64().max(0.001);
                Ok(())
            })?;
            last_update = Instant::now();
        }
    }
    output
        .flush()
        .await
        .map_err(|error| filesystem_error("could not flush a downloaded HLS segment", error))?;
    drop(output);
    validate_hls_part_length(received, expected_part_bytes, response_content_length)?;
    Ok(HlsPartOutcome::Complete(received))
}

async fn download_hls_worker(
    app: Option<&tauri::AppHandle>,
    state: &Mutex<DownloadState>,
    bandwidth: &Arc<BandwidthLimiter>,
    id: &str,
    control: &Arc<DownloadTaskControl>,
    url: String,
    destination: PathBuf,
    playlist: HlsPlaylist,
    mut committed_bytes: u64,
    mut completed_parts: usize,
    base_elapsed_ms: u64,
    restart: bool,
) -> Result<(), String> {
    let partial = partial_path(&destination);
    let staging = hls_staging_path(&destination, id);
    if restart {
        for path in [&partial, &staging] {
            match fs::symlink_metadata(path).await {
                Ok(metadata)
                    if metadata.file_type().is_file() || metadata.file_type().is_symlink() =>
                {
                    fs::remove_file(path).await.map_err(|error| {
                        filesystem_error("could not remove an earlier HLS partial file", error)
                    })?;
                }
                Ok(_) => return Err("An earlier HLS partial path is not a regular file.".into()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(filesystem_error(
                        "could not inspect an earlier HLS partial file",
                        error,
                    ));
                }
            }
        }
        committed_bytes = 0;
        completed_parts = 0;
    } else {
        remove_hls_staging(&staging).await?;
    }
    if destination.exists() {
        return fail_entry_optional(
            app,
            state,
            id,
            "The destination file already exists; choose a different filename.".into(),
        )
        .await;
    }

    let resources = ordered_hls_resources(&playlist);
    if completed_parts > resources.len() || committed_bytes > MAX_HLS_EXPECTED_BYTES {
        return fail_entry_optional(
            app,
            state,
            id,
            "The saved HLS resume position is invalid. Retry from the beginning.".into(),
        )
        .await;
    }
    match fs::symlink_metadata(&partial).await {
        Ok(metadata) if metadata.file_type().is_file() => {
            if metadata.len() < committed_bytes {
                return fail_entry_optional(
                    app,
                    state,
                    id,
                    "The assembled HLS partial file is shorter than its saved resume position. Retry from the beginning.".into(),
                )
                .await;
            }
            if metadata.len() > committed_bytes {
                let file = OpenOptions::new()
                    .write(true)
                    .open(&partial)
                    .await
                    .map_err(|error| {
                        filesystem_error("could not repair the HLS partial file", error)
                    })?;
                file.set_len(committed_bytes).await.map_err(|error| {
                    filesystem_error("could not repair the HLS partial file", error)
                })?;
            }
        }
        Ok(_) => {
            return fail_entry_optional(
                app,
                state,
                id,
                "The assembled HLS partial path is not a regular file.".into(),
            )
            .await;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && committed_bytes == 0 => {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&partial)
                .await
                .map_err(|error| {
                    filesystem_error("could not create the assembled HLS partial file", error)
                })?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return fail_entry_optional(
                app,
                state,
                id,
                "The assembled HLS partial file is missing. Retry from the beginning.".into(),
            )
            .await;
        }
        Err(error) => {
            return Err(filesystem_error(
                "could not inspect the assembled HLS partial file",
                error,
            ));
        }
    }

    let client = build_http_client()?;
    let started = Instant::now();
    for (index, resource) in resources.iter().enumerate().skip(completed_parts) {
        if let Some(reason) = hls_stop_reason(control) {
            match reason {
                HlsPartOutcome::Cancelled => return finish_cancelled_optional(app, state, id),
                HlsPartOutcome::Shutdown => {
                    return finish_shutdown_optional(state, id, |entry| {
                        entry.downloaded_bytes = committed_bytes;
                        entry.hls_completed_parts = Some(completed_parts);
                        entry.speed_bytes_per_second = 0.0;
                        entry.remaining_seconds = None;
                    });
                }
                HlsPartOutcome::Paused | HlsPartOutcome::Complete(_) => {
                    update_entry_optional(app, state, id, |entry| {
                        transition(entry, DownloadStatus::Paused)?;
                        entry.downloaded_bytes = committed_bytes;
                        entry.speed_bytes_per_second = 0.0;
                        entry.remaining_seconds = None;
                        Ok(())
                    })?;
                    return Ok(());
                }
            }
        }
        let outcome = download_hls_part(
            app,
            state,
            bandwidth,
            &client,
            id,
            control,
            resource,
            &staging,
            committed_bytes,
            playlist.expected_bytes,
            base_elapsed_ms,
            started,
        )
        .await?;
        let received = match outcome {
            HlsPartOutcome::Complete(received) => received,
            HlsPartOutcome::Cancelled => return finish_cancelled_optional(app, state, id),
            HlsPartOutcome::Shutdown => {
                let elapsed_ms = base_elapsed_ms
                    .saturating_add(started.elapsed().as_millis().min(u64::MAX as u128) as u64);
                return finish_shutdown_optional(state, id, |entry| {
                    entry.downloaded_bytes = committed_bytes;
                    entry.hls_completed_parts = Some(completed_parts);
                    entry.elapsed_ms = elapsed_ms;
                    entry.speed_bytes_per_second = 0.0;
                    entry.remaining_seconds = None;
                });
            }
            HlsPartOutcome::Paused => {
                update_entry_optional(app, state, id, |entry| {
                    transition(entry, DownloadStatus::Paused)?;
                    entry.downloaded_bytes = committed_bytes;
                    entry.elapsed_ms = base_elapsed_ms
                        .saturating_add(started.elapsed().as_millis().min(u64::MAX as u128) as u64);
                    entry.speed_bytes_per_second = 0.0;
                    entry.remaining_seconds = None;
                    Ok(())
                })?;
                return Ok(());
            }
        };
        let next_committed = committed_bytes
            .checked_add(received)
            .filter(|total| *total <= MAX_HLS_EXPECTED_BYTES)
            .ok_or_else(|| {
                "The assembled HLS download exceeds the 2 GiB safety limit.".to_owned()
            })?;
        if playlist
            .expected_bytes
            .is_some_and(|expected| next_committed > expected)
        {
            return Err("The assembled HLS download exceeds its declared byte ranges.".into());
        }
        let mut input = fs::File::open(&staging)
            .await
            .map_err(|error| filesystem_error("could not open a completed HLS segment", error))?;
        let mut output = OpenOptions::new()
            .append(true)
            .open(&partial)
            .await
            .map_err(|error| filesystem_error("could not append an HLS segment", error))?;
        let copied = tokio::io::copy(&mut input, &mut output)
            .await
            .map_err(|error| filesystem_error("could not assemble an HLS segment", error))?;
        output
            .flush()
            .await
            .map_err(|error| filesystem_error("could not flush the assembled HLS file", error))?;
        if copied != received {
            return Err("An HLS segment changed before it could be assembled.".into());
        }
        drop(output);
        remove_hls_staging(&staging).await?;
        committed_bytes = next_committed;
        completed_parts = index + 1;
        let elapsed_ms = base_elapsed_ms
            .saturating_add(started.elapsed().as_millis().min(u64::MAX as u128) as u64);
        update_entry_optional(app, state, id, |entry| {
            entry.downloaded_bytes = committed_bytes;
            entry.hls_completed_parts = Some(completed_parts);
            entry.total_bytes = playlist.expected_bytes;
            entry.elapsed_ms = elapsed_ms;
            entry.speed_bytes_per_second = 0.0;
            entry.remaining_seconds = None;
            Ok(())
        })?;
    }
    if playlist
        .expected_bytes
        .is_some_and(|expected| committed_bytes != expected)
    {
        return Err(
            "The assembled HLS file does not match the playlist's expected byte count.".into(),
        );
    }
    let actual_bytes = fs::metadata(&partial)
        .await
        .map_err(|error| filesystem_error("could not inspect the assembled HLS file", error))?
        .len();
    if actual_bytes != committed_bytes {
        return Err(
            "The assembled HLS file size does not match its saved download position.".into(),
        );
    }
    let validation_path = partial.clone();
    let validation_destination = destination.clone();
    let validation_url = url.clone();
    tokio::task::spawn_blocking(move || {
        validate_mp4_completion(
            &validation_path,
            &validation_destination,
            &validation_url,
            Some("video/mp4"),
        )
    })
    .await
    .map_err(|error| format!("The HLS media validation worker could not finish: {error}"))?
    .map_err(|error| {
        format!(
            "The assembled HLS data is incomplete or invalid and was not saved as a completed MP4. {error} The bytes remain in the temporary partial file; retry from the beginning or remove the failed item."
        )
    })?;
    #[cfg(test)]
    invoke_test_commit_hook("hls_after_validation", id, control);
    if let Some(reason) = hls_stop_reason(control) {
        match reason {
            HlsPartOutcome::Cancelled => return finish_cancelled_optional(app, state, id),
            HlsPartOutcome::Shutdown => {
                let elapsed_ms = base_elapsed_ms
                    .saturating_add(started.elapsed().as_millis().min(u64::MAX as u128) as u64);
                return finish_shutdown_optional(state, id, |entry| {
                    entry.downloaded_bytes = committed_bytes;
                    entry.hls_completed_parts = Some(completed_parts);
                    entry.elapsed_ms = elapsed_ms;
                    entry.speed_bytes_per_second = 0.0;
                    entry.remaining_seconds = None;
                });
            }
            HlsPartOutcome::Paused | HlsPartOutcome::Complete(_) => {
                update_entry_optional(app, state, id, |entry| {
                    transition(entry, DownloadStatus::Paused)?;
                    entry.speed_bytes_per_second = 0.0;
                    entry.remaining_seconds = None;
                    Ok(())
                })?;
                return Ok(());
            }
        }
    }
    if destination.exists() {
        return fail_entry_optional(
            app,
            state,
            id,
            "The destination file appeared during the HLS download; it was not overwritten.".into(),
        )
        .await;
    }
    fs::rename(&partial, &destination)
        .await
        .map_err(|error| filesystem_error("could not commit the completed HLS download", error))?;
    #[cfg(test)]
    invoke_test_commit_hook("hls_after_rename", id, control);
    let elapsed_ms =
        base_elapsed_ms.saturating_add(started.elapsed().as_millis().min(u64::MAX as u128) as u64);
    let average_speed = if elapsed_ms > 0 {
        committed_bytes as f64 / (elapsed_ms as f64 / 1000.0)
    } else {
        0.0
    };
    let completed = update_entry_optional(app, state, id, |entry| {
        if control.is_cancelled() || entry.status == DownloadStatus::Cancelled {
            return Err("The HLS download was cancelled.".into());
        }
        transition(entry, DownloadStatus::Completed)?;
        entry.downloaded_bytes = committed_bytes;
        entry.total_bytes = Some(committed_bytes);
        entry.hls_completed_parts = Some(resources.len());
        entry.speed_bytes_per_second = average_speed;
        entry.elapsed_ms = elapsed_ms;
        entry.remaining_seconds = Some(0.0);
        entry.error = None;
        Ok(())
    });
    match completed {
        Ok(completed) => {
            if completed.notify_on_complete {
                send_download_notification_optional(app, &completed, true);
            }
            Ok(())
        }
        Err(_) if control.is_cancelled() => {
            let _ = fs::remove_file(&destination).await;
            finish_cancelled_optional(app, state, id)
        }
        Err(error) => Err(error),
    }
}

pub(crate) fn partial_path(destination: &Path) -> PathBuf {
    let mut name = destination.file_name().unwrap_or_default().to_os_string();
    name.push(".fluxnet-part");
    destination.with_file_name(name)
}

#[allow(dead_code)]
fn send_download_notification(app: &tauri::AppHandle, entry: &DownloadEntry, completed: bool) {
    send_download_notification_optional(Some(app), entry, completed);
}

fn send_download_notification_optional(
    app: Option<&tauri::AppHandle>,
    entry: &DownloadEntry,
    completed: bool,
) {
    let Some(app) = app else {
        return;
    };
    let (title, body) = if completed {
        (
            "Download complete",
            format!("{} was downloaded.", entry.filename),
        )
    } else {
        (
            "Download failed",
            format!(
                "{} failed: {}",
                entry.filename,
                entry.error.as_deref().unwrap_or("unknown error")
            ),
        )
    };
    if let Err(error) = app.notification().builder().title(title).body(body).show() {
        eprintln!("Could not show download notification: {error}");
    }
}

#[allow(dead_code)]
async fn fail_entry(
    app: &tauri::AppHandle,
    state: &Mutex<DownloadState>,
    id: &str,
    message: String,
) -> Result<(), String> {
    fail_entry_optional(Some(app), state, id, message).await
}

async fn fail_entry_optional(
    app: Option<&tauri::AppHandle>,
    state: &Mutex<DownloadState>,
    id: &str,
    message: String,
) -> Result<(), String> {
    let entry = update_entry_optional(app, state, id, |entry| {
        if entry.status.is_terminal() {
            return Ok(());
        }
        transition(entry, DownloadStatus::Failed)?;
        entry.error = Some(message);
        entry.speed_bytes_per_second = 0.0;
        entry.remaining_seconds = None;
        Ok(())
    })?;
    if entry.status == DownloadStatus::Failed && entry.notify_on_failure {
        send_download_notification_optional(app, &entry, false);
    }
    Ok(())
}

#[allow(dead_code)]
fn finish_cancelled(
    app: &tauri::AppHandle,
    state: &Mutex<DownloadState>,
    id: &str,
) -> Result<(), String> {
    finish_cancelled_optional(Some(app), state, id)
}

fn finish_cancelled_optional(
    app: Option<&tauri::AppHandle>,
    state: &Mutex<DownloadState>,
    id: &str,
) -> Result<(), String> {
    update_entry_optional(app, state, id, |entry| {
        if !entry.status.is_terminal() {
            transition(entry, DownloadStatus::Cancelled)?;
        }
        entry.speed_bytes_per_second = 0.0;
        entry.remaining_seconds = None;
        Ok(())
    })?;
    Ok(())
}

fn finish_shutdown_optional<F>(
    state: &Mutex<DownloadState>,
    id: &str,
    update: F,
) -> Result<(), String>
where
    F: FnOnce(&mut DownloadEntry),
{
    let mut state = state
        .lock()
        .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
    if let Some(entry) = state.entries.iter_mut().find(|entry| entry.id == id) {
        update(entry);
        entry.updated_at_ms = current_timestamp_ms();
    }
    state.controls.remove(id);
    persist_locked(&state)
}

#[tauri::command]
pub fn pause_download(
    manager: tauri::State<'_, DownloadManager>,
    id: String,
) -> Result<(), String> {
    manager.pause_download(&id)
}

#[tauri::command]
pub fn resume_download(
    app: tauri::AppHandle,
    manager: tauri::State<'_, DownloadManager>,
    id: String,
) -> Result<(), String> {
    manager.resume_download(Some(&app), &id)
}

#[tauri::command]
pub fn cancel_download(
    app: tauri::AppHandle,
    manager: tauri::State<'_, DownloadManager>,
    id: String,
) -> Result<(), String> {
    manager.cancel_download(Some(&app), &id)
}

#[tauri::command]
pub fn retry_download(
    app: tauri::AppHandle,
    manager: tauri::State<'_, DownloadManager>,
    id: String,
    restart: bool,
) -> Result<(), String> {
    manager.retry_download_internal(Some(&app), id, restart)
}

#[tauri::command]
pub fn open_download_containing_folder(
    manager: tauri::State<'_, DownloadManager>,
    id: String,
) -> Result<(), String> {
    let state = manager
        .state
        .lock()
        .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
    let entry = state
        .entries
        .iter()
        .find(|entry| entry.id == id)
        .ok_or_else(|| "Download record was not found.".to_owned())?;
    if entry.status != DownloadStatus::Completed {
        return Err("The containing folder is available after the download completes.".into());
    }
    let parent = Path::new(&entry.destination)
        .parent()
        .ok_or_else(|| "Could not resolve the download's containing folder.".to_owned())?;
    std::process::Command::new("explorer.exe")
        .arg(parent)
        .spawn()
        .map_err(|error| filesystem_error("could not open the containing folder", error))?;
    Ok(())
}

#[tauri::command]
pub fn clear_completed_downloads(manager: tauri::State<'_, DownloadManager>) -> Result<(), String> {
    let mut state = manager
        .state
        .lock()
        .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
    let previous_entries = state.entries.clone();
    state
        .entries
        .retain(|entry| entry.status != DownloadStatus::Completed);
    if let Err(error) = persist_locked(&state) {
        state.entries = previous_entries;
        return Err(error);
    }
    Ok(())
}

pub fn cancel_all_downloads(
    manager: &DownloadManager,
) -> Result<(Arc<AtomicUsize>, usize), String> {
    let mut state = manager
        .state
        .lock()
        .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
    let active_controls: Vec<(String, Arc<DownloadTaskControl>)> = state
        .controls
        .iter()
        .map(|(id, ctrl)| (id.clone(), Arc::clone(ctrl)))
        .collect();
    for (id, control) in &active_controls {
        let is_resumable_http_or_hls = state
            .entries
            .iter()
            .any(|entry| &entry.id == id && entry.website_media.is_none());
        if is_resumable_http_or_hls {
            control.request_shutdown();
        } else {
            control.request_cancel();
        }
    }
    let mut website_partials_to_remove = Vec::new();
    let mut cancelled_paused_ids = Vec::new();
    for entry in &mut state.entries {
        if entry.status == DownloadStatus::Paused && entry.website_media.is_some() {
            transition(entry, DownloadStatus::Cancelled)?;
            cancelled_paused_ids.push(entry.id.clone());
            website_partials_to_remove.push(PathBuf::from(&entry.destination));
        }
    }
    for id in &cancelled_paused_ids {
        state.controls.remove(id);
        #[cfg(windows)]
        state.website_executables.remove(id);
    }
    if let Err(error) = persist_locked(&state) {
        eprintln!(
            "Could not checkpoint download history during shutdown; partial files will be validated on the next launch: {error}"
        );
    }
    drop(state);
    for destination in website_partials_to_remove {
        let _ = std::fs::remove_file(partial_path(&destination));
    }
    Ok((Arc::clone(&manager.worker_count), active_controls.len()))
}

#[cfg(all(test, windows))]
#[allow(non_upper_case_globals)]
#[unsafe(no_mangle)]
#[used]
#[unsafe(link_section = ".drectve")]
static _FLUXNET_TEST_COMCTL6_MANIFEST: [u8; 167] = *b" /manifestdependency:\"type='win32' name='Microsoft.Windows.Common-Controls' version='6.0.0.0' processorArchitecture='*' publicKeyToken='6595b64144ccf1df' language='*'\"";

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[cfg(windows)]
    #[test]
    fn process_worker_uses_manager_slot_and_shutdown_cancels_whole_tree() {
        use crate::process::{
            ProcessOutcome, assert_process_terminated, process_fixture_spec_for_integration,
            run_supervised,
        };

        let manager = DownloadManager::default();
        let control = Arc::new(DownloadTaskControl::default());
        {
            let mut state = manager.state.lock().unwrap();
            state.max_concurrent = 1;
            state
                .controls
                .insert("process-test".into(), Arc::clone(&control));
        }
        let (spec, ready_path) = process_fixture_spec_for_integration();
        let descendant_pid_path = ready_path.with_extension("grandchild.pid");
        let queued_control = Arc::new(DownloadTaskControl::default());
        let slot = tauri::async_runtime::block_on(async {
            manager
                .concurrency
                .acquire(&manager.state, &control)
                .await
                .unwrap()
                .expect("process worker should use the manager's only slot")
        });
        manager.worker_count.fetch_add(1, Ordering::AcqRel);
        let worker_control = Arc::clone(&control);
        let worker_count = Arc::clone(&manager.worker_count);
        let worker = std::thread::spawn(move || {
            let _worker_guard = WorkerGuard(worker_count);
            let outcome = run_supervised(&spec, &worker_control);
            drop(slot);
            outcome
        });

        let deadline = Instant::now() + Duration::from_secs(5);
        while !ready_path.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(ready_path.exists(), "local child did not start");
        assert_eq!(manager.concurrency.active.load(Ordering::Acquire), 1);
        let waiting = tauri::async_runtime::block_on(async {
            tokio::time::timeout(
                Duration::from_millis(50),
                manager.concurrency.acquire(&manager.state, &queued_control),
            )
            .await
        });
        assert!(
            waiting.is_err(),
            "a second task bypassed the existing slot limit"
        );

        let (workers, cancelled_count) = cancel_all_downloads(&manager).unwrap();
        assert_eq!(cancelled_count, 1);
        let outcome = worker
            .join()
            .unwrap()
            .expect("supervisor should return cancellation");
        assert!(matches!(outcome, ProcessOutcome::Cancelled { .. }));
        assert_eq!(
            workers.load(Ordering::Acquire),
            0,
            "shutdown must wait for process worker cleanup"
        );
        assert_eq!(manager.concurrency.active.load(Ordering::Acquire), 0);

        let descendant = std::fs::read_to_string(&descendant_pid_path)
            .expect("fixture should record descendant PID")
            .parse::<u32>()
            .unwrap();
        assert_process_terminated(descendant);
        let _ = std::fs::remove_file(&ready_path);
        let _ = std::fs::remove_file(descendant_pid_path);
        let _ = std::fs::remove_file(ready_path.with_extension("grandchild.ready"));
    }

    #[cfg(windows)]
    #[test]
    fn paused_process_worker_terminates_tree_and_releases_download_slot() {
        use crate::process::{
            ProcessOutcome, assert_process_terminated, process_fixture_spec_for_integration,
            run_supervised,
        };

        let manager = DownloadManager::default();
        let control = Arc::new(DownloadTaskControl::default());
        {
            let mut state = manager.state.lock().unwrap();
            state.max_concurrent = 1;
        }
        let (spec, ready_path) = process_fixture_spec_for_integration();
        let descendant_pid_path = ready_path.with_extension("grandchild.pid");
        let descendant_ready_path = ready_path.with_extension("grandchild.ready");
        let queued_control = Arc::new(DownloadTaskControl::default());

        let slot = tauri::async_runtime::block_on(async {
            manager
                .concurrency
                .acquire(&manager.state, &control)
                .await
                .unwrap()
                .expect("process worker should acquire the only slot")
        });
        let worker_control = Arc::clone(&control);
        let worker = std::thread::spawn(move || {
            let outcome = run_supervised(&spec, &worker_control);
            drop(slot);
            outcome
        });

        let deadline = Instant::now() + Duration::from_secs(5);
        while (!ready_path.exists()
            || !descendant_pid_path.exists()
            || !descendant_ready_path.exists())
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            ready_path.exists() && descendant_pid_path.exists() && descendant_ready_path.exists(),
            "local child and descendant did not start"
        );
        assert_eq!(manager.concurrency.active.load(Ordering::Acquire), 1);

        control.request_pause();
        let outcome = worker
            .join()
            .unwrap()
            .expect("supervisor should return pause");
        assert!(matches!(outcome, ProcessOutcome::Paused { .. }));
        assert_eq!(manager.concurrency.active.load(Ordering::Acquire), 0);

        let acquired_by_waiting = tauri::async_runtime::block_on(async {
            tokio::time::timeout(
                Duration::from_millis(200),
                manager.concurrency.acquire(&manager.state, &queued_control),
            )
            .await
        })
        .expect("waiting worker should acquire the released slot immediately")
        .unwrap();
        assert!(acquired_by_waiting.is_some());

        let descendant = std::fs::read_to_string(&descendant_pid_path)
            .expect("fixture should record descendant PID")
            .parse::<u32>()
            .unwrap();
        assert_process_terminated(descendant);
        let _ = std::fs::remove_file(&ready_path);
        let _ = std::fs::remove_file(descendant_pid_path);
        let _ = std::fs::remove_file(descendant_ready_path);
    }

    #[test]
    fn cancelled_entry_cannot_transition_to_completed() {
        let mut entry = DownloadEntry {
            id: "cancelled-race".into(),
            url: "https://example.test/video.mp4".into(),
            destination: "video.mp4".into(),
            filename: "video.mp4".into(),
            status: DownloadStatus::Cancelled,
            total_bytes: None,
            downloaded_bytes: 0,
            speed_bytes_per_second: 0.0,
            elapsed_ms: 0,
            remaining_seconds: None,
            error: None,
            created_at_ms: 0,
            updated_at_ms: 0,
            attempts: 1,
            hls_playlist: None,
            hls_completed_parts: None,
            website_media: None,
            segments: Vec::new(),
            etag: None,
            last_modified: None,
            notifications_enabled: false,
            notify_on_complete: false,
            notify_on_failure: false,
        };
        assert!(transition(&mut entry, DownloadStatus::Completed).is_err());
        assert_eq!(entry.status, DownloadStatus::Cancelled);
    }

    fn mp4_atom(kind: &[u8; 4], payload: &[u8]) -> Vec<u8> {
        let mut atom = Vec::with_capacity(payload.len() + 8);
        atom.extend_from_slice(&u32::try_from(payload.len() + 8).unwrap().to_be_bytes());
        atom.extend_from_slice(kind);
        atom.extend_from_slice(payload);
        atom
    }

    fn valid_mp4_initialization(fragmented: bool) -> Vec<u8> {
        let mut sample_table = mp4_atom(b"stsd", &[0; 8]);
        if !fragmented {
            sample_table.extend(mp4_atom(b"stts", &[0; 4]));
            sample_table.extend(mp4_atom(b"stsc", &[0; 4]));
            sample_table.extend(mp4_atom(b"stsz", &[0; 8]));
            sample_table.extend(mp4_atom(b"stco", &[0; 8]));
        }
        let sample_table = mp4_atom(b"stbl", &sample_table);
        let media_info = mp4_atom(b"minf", &sample_table);
        let mut media = mp4_atom(b"mdhd", &[0; 8]);
        media.extend(mp4_atom(b"hdlr", &[0; 8]));
        media.extend(media_info);
        let track = mp4_atom(b"trak", &mp4_atom(b"mdia", &media));
        let mut movie = mp4_atom(b"mvhd", &[0; 8]);
        movie.extend(track);
        if fragmented {
            movie.extend(mp4_atom(b"mvex", &mp4_atom(b"trex", &[0; 8])));
        }
        mp4_atom(b"moov", &movie)
    }

    fn valid_mp4(fragmented: bool) -> Vec<u8> {
        let mut bytes = mp4_atom(b"ftyp", b"isom\0\0\0\0isom");
        bytes.extend(valid_mp4_initialization(fragmented));
        if fragmented {
            let mut track_fragment = mp4_atom(b"tfhd", &[0; 8]);
            track_fragment.extend(mp4_atom(b"trun", &[0; 8]));
            let mut movie_fragment = mp4_atom(b"mfhd", &[0; 8]);
            movie_fragment.extend(mp4_atom(b"traf", &track_fragment));
            bytes.extend(mp4_atom(b"moof", &movie_fragment));
        }
        bytes.extend(mp4_atom(b"mdat", &[0; 16]));
        bytes
    }

    fn validate_mp4_bytes(bytes: Vec<u8>) -> Mp4Validation {
        let length = bytes.len() as u64;
        validate_mp4_reader(&mut Cursor::new(bytes), length).unwrap()
    }

    #[test]
    fn rejects_unranged_media_responses_that_are_http_partial_content() {
        assert!(validate_unranged_media_response(StatusCode::OK, None, None).is_ok());
        assert!(
            validate_unranged_media_response(StatusCode::PARTIAL_CONTENT, None, Some(10400))
                .is_err()
        );
        assert!(
            validate_unranged_media_response(
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 0-10399/847392"),
                Some(10400),
            )
            .is_err()
        );
        assert!(
            validate_unranged_media_response(
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 0-99/100"),
                Some(100),
            )
            .is_ok()
        );
        assert!(
            validate_unranged_media_response(
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 0-99/100"),
                Some(99),
            )
            .is_err()
        );
        assert!(
            validate_unranged_media_response(StatusCode::OK, Some("bytes 0-99/100"), Some(100),)
                .is_err()
        );
    }

    #[test]
    fn normalizes_instagram_byte_range_urls_and_prevents_audio_video_dash_track_collision() {
        for (raw, expected) in [
            (
                "https://scontent.cdninstagram.com/v/t51/reel.mp4?bytestart=0&byteend=10399&token=private",
                "https://scontent.cdninstagram.com/v/t51/reel.mp4?token=private",
            ),
            (
                "https://instagram.fdac5-1.fna.fbcdn.net/v/t51/story.mp4?signature=private&bytestart=0&byteend=10399",
                "https://instagram.fdac5-1.fna.fbcdn.net/v/t51/story.mp4?signature=private",
            ),
            (
                "https://instagram.fdac5-2.fna.fbcdn.net/o1/v/t2/f2/m367/clip.mp4?_nc_cat=110&efg=eyJ2ZW5jb2RlX3RhZyI6ImlnLXhwdmRzLmNsaXBzLmMyLUMzLmRhc2hfdnA5LWJhc2ljLWdlbjItaGZyXzEwODBwIiwieHB2X2Fzc2V0X2lkIjoxODE3Mjk3NzUyODAzMzU3fQ%3D%3D&ccb=17-1&oh=00_AQR_sig&oe=6AF5453F&bytestart=5248832&byteend=8727821",
                "https://instagram.fdac5-2.fna.fbcdn.net/o1/v/t2/f2/m367/clip.mp4?_nc_cat=110&efg=eyJ2ZW5jb2RlX3RhZyI6ImlnLXhwdmRzLmNsaXBzLmMyLUMzLmRhc2hfdnA5LWJhc2ljLWdlbjItaGZyXzEwODBwIiwieHB2X2Fzc2V0X2lkIjoxODE3Mjk3NzUyODAzMzU3fQ%3D%3D&ccb=17-1&oh=00_AQR_sig&oe=6AF5453F",
            ),
        ] {
            let parsed = validated_download_url(raw).unwrap();
            assert!(is_instagram_byte_range_url(&parsed));
            assert!(!is_instagram_audio_only_url(&parsed));
            let normalized = normalize_instagram_media_url(&parsed).unwrap();
            assert_eq!(normalized.as_str(), expected);
            assert!(!is_instagram_byte_range_url(&normalized));
        }

        // Audio-only DASH track with same xpv_asset_id must be distinguished and rejected
        for audio_url in [
            "https://instagram.fdac5-1.fna.fbcdn.net/o1/v/t2/f2/m78/audio.mp4?_nc_cat=1&efg=eyJ2ZW5jb2RlX3RhZyI6ImlnLXhwdmRzLmNsaXBzLmMxLUMzLmRhc2hfbG5faGVhYWNfdmJyM19hdWRpbyIsInhwdl9hc3NldF9pZCI6MTgxNzI5Nzc1MjgwMzM1N30%3D&oh=00_AQT_sig&oe=6AF1457A&bytestart=179584&byteend=198777",
            "https://instagram.fdac5-1.fna.fbcdn.net/o1/v/t2/f2/m78/audio_no_efg.mp4?oh=00_AQT_sig&oe=6AF1457A&bytestart=0&byteend=1024",
        ] {
            let parsed = validated_download_url(audio_url).unwrap();
            assert!(is_instagram_audio_only_url(&parsed));
            let err = normalize_instagram_media_url(&parsed).unwrap_err();
            assert!(err.contains("audio-only DASH track"));
        }

        // Malformed byte-range parameters must be rejected
        for malformed in [
            "https://scontent.cdninstagram.com/v/t51/reel.mp4?bytestart=500&byteend=100&token=private",
            "https://scontent.cdninstagram.com/v/t51/reel.mp4?bytestart=abc&byteend=100&token=private",
            "https://scontent.cdninstagram.com/v/t51/reel.mp4?bytestart=0&bytestart=10&byteend=100",
        ] {
            let parsed = validated_download_url(malformed).unwrap();
            assert!(normalize_instagram_media_url(&parsed).is_err());
        }

        assert!(!is_instagram_byte_range_url(
            &validated_download_url("https://cdn.example/video.mp4?bytestart=0&byteend=10399")
                .unwrap()
        ));
    }

    #[test]
    fn rejects_observed_10400_byte_instagram_reel_and_story_fragments() {
        let mut fragment_header = mp4_atom(b"mfhd", &[0; 8]);
        fragment_header.extend(mp4_atom(b"traf", &{
            let mut track = mp4_atom(b"tfhd", &[0; 8]);
            track.extend(mp4_atom(b"trun", &[0; 8]));
            track
        }));
        let moof = mp4_atom(b"moof", &fragment_header);
        let mdat_payload_len = 10_400 - moof.len() - 8;
        let mut reels_fragment = moof.clone();
        reels_fragment.extend(mp4_atom(b"mdat", &vec![0; mdat_payload_len]));
        assert_eq!(reels_fragment.len(), 10_400);

        for (name, url) in [
            (
                "Instagram_Reel.mp4.fluxnet-part",
                "https://scontent.cdninstagram.com/v/t51/reel.mp4?signature=private",
            ),
            (
                "Instagram_Story.mp4.fluxnet-part",
                "https://instagram.fdac5-1.fna.fbcdn.net/v/t51/story.mp4?signature=private",
            ),
        ] {
            let partial = write_temp_download_file(name, &reels_fragment);
            let error = validate_mp4_completion(
                &partial,
                Path::new("Instagram_Video.mp4"),
                url,
                Some("video/mp4"),
            )
            .unwrap_err();
            assert!(error.contains("fragmented MP4 without its required initialization segment"));
            assert_eq!(std::fs::metadata(&partial).unwrap().len(), 10_400);
            std::fs::remove_file(partial).unwrap();
        }
    }

    #[test]
    fn hls_stop_checks_prioritize_cancel_over_pause() {
        let control = DownloadTaskControl::default();
        assert!(hls_stop_reason(&control).is_none());
        control.request_shutdown();
        assert!(matches!(
            hls_stop_reason(&control),
            Some(HlsPartOutcome::Shutdown)
        ));
        control.pause_requested.store(true, Ordering::Release);
        assert!(matches!(
            hls_stop_reason(&control),
            Some(HlsPartOutcome::Paused)
        ));
        control.cancelled.store(true, Ordering::Release);
        assert!(matches!(
            hls_stop_reason(&control),
            Some(HlsPartOutcome::Cancelled)
        ));
    }

    #[test]
    fn validates_hls_response_ranges_and_truncated_bodies() {
        let ranged = HlsResource {
            url: "https://cdn.example/media.mp4".into(),
            byte_range: Some(crate::browser_bridge::HlsByteRange {
                length: 4,
                offset: 8,
            }),
        };
        assert!(
            validate_hls_part_response(
                &ranged,
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 8-11/100"),
                Some(4),
            )
            .is_ok()
        );
        assert!(
            validate_hls_part_response(
                &ranged,
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 8-10/100"),
                Some(3),
            )
            .is_err()
        );
        assert!(validate_hls_part_length(3, Some(4), Some(4)).is_err());
        assert!(validate_hls_part_length(4, Some(4), Some(4)).is_ok());
        assert!(validate_hls_part_response(&ranged, StatusCode::OK, None, Some(4)).is_err());
    }

    #[test]
    fn validates_full_hls_responses_and_empty_parts() {
        let full = HlsResource {
            url: "https://cdn.example/init.mp4".into(),
            byte_range: None,
        };
        assert!(validate_hls_part_response(&full, StatusCode::OK, None, Some(4)).is_ok());
        assert!(
            validate_hls_part_response(
                &full,
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 0-3/100"),
                Some(4),
            )
            .is_err()
        );
        assert!(
            validate_hls_part_response(
                &full,
                StatusCode::OK,
                None,
                Some(MAX_HLS_EXPECTED_BYTES + 1),
            )
            .is_err()
        );
        assert!(validate_hls_part_length(0, None, None).is_err());
        assert!(validate_hls_part_length(4, None, Some(4)).is_ok());
    }

    #[test]
    fn hls_assembly_order_keeps_initialization_first_and_segments_declared() {
        let playlist = HlsPlaylist {
            version: 1,
            container: "fmp4".into(),
            manifest_size_bytes: 64,
            init: HlsResource {
                url: "https://cdn.example/init.mp4".into(),
                byte_range: None,
            },
            segments: [
                "https://cdn.example/segment-1.m4s",
                "https://cdn.example/segment-2.m4s",
            ]
            .into_iter()
            .map(|url| HlsResource {
                url: url.into(),
                byte_range: None,
            })
            .collect(),
            expected_bytes: None,
        };

        assert_eq!(
            ordered_hls_resources(&playlist)
                .iter()
                .map(|resource| resource.url.as_str())
                .collect::<Vec<_>>(),
            [
                "https://cdn.example/init.mp4",
                "https://cdn.example/segment-1.m4s",
                "https://cdn.example/segment-2.m4s",
            ]
        );
    }

    #[test]
    fn hls_destinations_must_keep_the_selected_mp4_extension() {
        assert!(validate_hls_destination(Path::new("Instagram_Video.MP4")).is_ok());
        assert!(validate_hls_destination(Path::new("Instagram_Video.webm")).is_err());
        assert!(validate_hls_destination(Path::new("Instagram_Video.mp4 ")).is_err());
    }

    fn write_temp_download_file(name: &str, bytes: &[u8]) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "fluxnet-{}-{:?}-{name}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::write(&path, bytes).unwrap();
        path
    }

    #[test]
    fn validates_standalone_and_initialized_fragmented_mp4_containers() {
        assert_eq!(
            validate_mp4_bytes(valid_mp4(false)),
            Mp4Validation::Standalone
        );
        assert_eq!(
            validate_mp4_bytes(valid_mp4(true)),
            Mp4Validation::FragmentedWithInitialization
        );
    }

    #[test]
    fn identifies_fragmented_mp4_that_lacks_initialization_data() {
        let mut fragment = mp4_atom(b"mfhd", &[0; 8]);
        fragment.extend(mp4_atom(b"traf", &{
            let mut track = mp4_atom(b"tfhd", &[0; 8]);
            track.extend(mp4_atom(b"trun", &[0; 8]));
            track
        }));
        let mut bytes = mp4_atom(b"moof", &fragment);
        bytes.extend(mp4_atom(b"mdat", &[0; 16]));
        assert_eq!(
            validate_mp4_bytes(bytes),
            Mp4Validation::FragmentedMissingInitialization
        );

        assert!(matches!(
            validate_mp4_bytes({
                let mut incomplete = mp4_atom(b"moof", &fragment);
                incomplete.extend(mp4_atom(b"mdat", &[0; 8]));
                let truncated_length = incomplete.len() - 3;
                incomplete.truncate(truncated_length);
                incomplete
            }),
            Mp4Validation::Invalid(message) if message.contains("extends beyond")
        ));

        assert!(matches!(
            validate_mp4_bytes({
                let mut incomplete = mp4_atom(b"mdat", &[0; 8]);
                incomplete.extend(mp4_atom(b"moof", &fragment));
                incomplete
            }),
            Mp4Validation::Invalid(message) if message.contains("following media data")
        ));
    }

    #[test]
    fn rejects_truncated_malformed_and_non_media_mp4_responses() {
        let mut truncated = valid_mp4(false);
        truncated.truncate(truncated.len() - 3);
        assert!(matches!(
            validate_mp4_bytes(truncated),
            Mp4Validation::Invalid(_)
        ));
        assert!(matches!(
            validate_mp4_bytes(b"<html>not a video</html>".to_vec()),
            Mp4Validation::Invalid(message) if message.contains("HTML")
        ));
        assert!(matches!(
            validate_mp4_bytes(br#"{"error":"unavailable"}"#.to_vec()),
            Mp4Validation::Invalid(message) if message.contains("JSON")
        ));
        assert!(matches!(
            validate_mp4_bytes(mp4_atom(b"ftyp", b"isom\0\0\0\0isom")),
            Mp4Validation::Invalid(_)
        ));
    }

    #[test]
    fn mp4_completion_requires_matching_extension_and_preserves_non_mp4_downloads() {
        let archive = write_temp_download_file("archive.zip", b"PK\x03\x04");
        assert!(
            validate_mp4_completion(
                &archive,
                Path::new("archive.zip"),
                "https://cdn.example/archive.zip",
                Some("application/zip"),
            )
            .is_ok()
        );
        let partial =
            write_temp_download_file("Instagram_Video4.mp4.fluxnet-part", &valid_mp4(false));
        let destination = Path::new("Instagram_Video4.mp4");
        assert!(
            validate_mp4_completion(
                &partial,
                destination,
                "https://cdn.example/video.mp4",
                Some("video/mp4"),
            )
            .is_ok()
        );
        let uppercase_destination = Path::new("Instagram_Video4.MP4");
        assert!(
            validate_mp4_completion(
                &partial,
                uppercase_destination,
                "https://cdn.example/video.mp4",
                Some("video/mp4"),
            )
            .is_ok()
        );

        let webm = Path::new("video.webm");
        let mismatch = validate_mp4_completion(
            &partial,
            webm,
            "https://cdn.example/video.mp4",
            Some("video/mp4"),
        )
        .unwrap_err();
        assert!(mismatch.contains("format and destination filename disagree"));
        let missing_extension = validate_mp4_completion(
            &partial,
            Path::new("video.bin"),
            "https://cdn.example/stream?id=1",
            Some("application/octet-stream"),
        )
        .unwrap_err();
        assert!(missing_extension.contains("does not end in .mp4"));
        std::fs::remove_file(archive).unwrap();
        std::fs::remove_file(partial).unwrap();
    }

    #[test]
    fn releasing_a_failed_download_slot_wakes_a_queued_worker() {
        tauri::async_runtime::block_on(async {
            let concurrency = Arc::new(ConcurrencyLimiter::default());
            let mut download_state = DownloadState::default();
            download_state.max_concurrent = 1;
            let state = Arc::new(Mutex::new(download_state));
            let active_control = DownloadTaskControl::default();
            let queued_control = Arc::new(DownloadTaskControl::default());
            let active_slot = concurrency
                .acquire(&state, &active_control)
                .await
                .unwrap()
                .expect("first worker should acquire the only slot");
            let waiting_concurrency = Arc::clone(&concurrency);
            let waiting_state = Arc::clone(&state);
            let waiting_control = Arc::clone(&queued_control);
            let waiting = tauri::async_runtime::spawn(async move {
                waiting_concurrency
                    .acquire(&waiting_state, &waiting_control)
                    .await
            });
            tokio::task::yield_now().await;

            drop(active_slot);

            let released_slot = tokio::time::timeout(Duration::from_secs(1), waiting)
                .await
                .expect("queued worker should wake when a slot is released")
                .expect("queued worker should not panic")
                .expect("queued worker should acquire the released slot");
            assert!(released_slot.is_some());
        });
    }

    #[test]
    fn preserves_separate_download_notification_preferences_and_migrates_legacy_setting() {
        assert_eq!(
            persisted_notification_preferences(true, false, false),
            (true, true)
        );
        assert_eq!(
            persisted_notification_preferences(true, false, true),
            (false, true)
        );
        assert_eq!(
            persisted_notification_preferences(false, true, false),
            (true, false)
        );
    }

    #[test]
    fn sanitizes_path_traversal_and_windows_reserved_names() {
        assert_eq!(sanitize_filename("../../CON.txt"), "_CON.txt");
        assert_eq!(sanitize_filename("CON .txt"), "_CON .txt");
        assert_eq!(sanitize_filename("..\\folder\\report?.pdf"), "report_.pdf");
        assert_eq!(sanitize_filename("..."), "download");
    }

    #[test]
    fn validates_http_and_https_url_schemes() {
        assert!(suggested_filename("https://example.test/download/a.bin").is_ok());
        assert!(suggested_filename("http://example.test/a.bin").is_ok());
        assert!(suggested_filename("file:///C:/Windows/win.ini").is_err());
        assert!(suggested_filename("https://user:pass@example.test/file").is_err());
    }

    #[test]
    fn preserves_query_parameters_and_derives_filename_only_from_path() {
        let url = validated_download_url(
            "https://example.test/files/archive%20name.zip?token=private&download=1",
        )
        .unwrap();
        assert_eq!(url.query(), Some("token=private&download=1"));
        assert_eq!(
            suggested_filename(url.as_str()).unwrap(),
            "archive%20name.zip"
        );
    }

    #[test]
    fn uses_webpage_title_for_long_social_cdn_video_urls_without_changing_the_url() {
        let url = "https://scontent.cdninstagram.com/v/t51/long-cdn-generated-filename.mp4?token=private&tracking=1#player";
        let parsed = validated_download_url(url).unwrap();
        let filename = suggested_filename_with_metadata(
            url,
            Some("My actual video title"),
            Some("www.instagram.com"),
            Some("mp4"),
            Some("long-cdn-generated-filename.mp4"),
        )
        .unwrap();

        assert_eq!(filename, "My actual video title.mp4");
        assert_eq!(parsed.query(), Some("token=private&tracking=1"));
        assert_eq!(parsed.fragment(), Some("player"));
    }

    #[test]
    fn uses_platform_fallbacks_when_the_page_title_is_generic_or_missing() {
        let ig1 = suggested_filename_with_metadata(
            "https://scontent.cdninstagram.com/clip.mp4?token=1",
            Some("Instagram"),
            Some("instagram.com"),
            Some("mp4"),
            None,
        )
        .unwrap();
        assert!(ig1.starts_with("Instagram_Video_") && ig1.ends_with(".mp4"));
        assert!(!ig1.contains("clip"));
        assert!(!ig1.contains("token"));

        let ig2 = suggested_filename_with_metadata(
            "https://scontent.cdninstagram.com/clip.mp4?token=1",
            Some("https://scontent.cdninstagram.com/very-long-cdn-hash.mp4?token=1"),
            Some("instagram.com"),
            Some("mp4"),
            Some("very-long-cdn-hash.mp4"),
        )
        .unwrap();
        assert_eq!(ig2, ig1);

        let ig_other = suggested_filename_with_metadata(
            "https://scontent.cdninstagram.com/other_clip.mp4?token=2",
            Some("(3) Instagram"),
            Some("www.instagram.com"),
            Some("mp4"),
            None,
        )
        .unwrap();
        assert!(ig_other.starts_with("Instagram_Video_") && ig_other.ends_with(".mp4"));
        assert_ne!(ig_other, ig1);

        let preferred = suggested_filename_with_metadata(
            "https://scontent.cdninstagram.com/v/t50/raw_cdn_999.mp4?token=1",
            Some("Instagram"),
            Some("www.instagram.com"),
            Some("mp4"),
            Some("creator_C8reelXYZ.mp4"),
        )
        .unwrap();
        assert_eq!(preferred, "creator_C8reelXYZ.mp4");

        assert_eq!(
            suggested_filename_with_metadata(
                "https://video.example/clip.webm",
                None,
                Some("m.facebook.com"),
                Some("webm"),
                None,
            )
            .unwrap(),
            "Facebook_Video.webm"
        );
        assert_eq!(
            suggested_filename_with_metadata(
                "https://video.example/clip.mp4",
                None,
                None,
                Some("mp4"),
                None,
            )
            .unwrap(),
            "Browser_Video.mp4"
        );
    }

    #[test]
    fn instagram_filenames_are_unique_across_videos_and_never_overwrite_existing_files() {
        let dir = create_test_temp_dir("instagram-unique-filenames");
        let name_a = suggested_filename_with_metadata(
            "https://scontent.cdninstagram.com/v/t50.2886-16/video_a.mp4?efg=video_a&oe=1",
            Some("Instagram"),
            Some("www.instagram.com"),
            Some("mp4"),
            Some("alice_C8reelAAA.mp4"),
        )
        .unwrap();
        let name_b = suggested_filename_with_metadata(
            "https://scontent.cdninstagram.com/v/t50.2886-16/video_b.mp4?efg=video_b&oe=2",
            Some("Instagram"),
            Some("www.instagram.com"),
            Some("mp4"),
            Some("bob_C8reelBBB.mp4"),
        )
        .unwrap();
        assert_eq!(name_a, "alice_C8reelAAA.mp4");
        assert_eq!(name_b, "bob_C8reelBBB.mp4");
        assert_ne!(name_a, name_b);

        // Simulate Video A already downloaded on disk, then re-downloaded.
        let path_a = dir.join(&name_a);
        std::fs::write(&path_a, b"existing-video-a-bytes").unwrap();
        let redownload_a = unique_destination(&path_a, &[]).unwrap();
        assert_ne!(redownload_a, path_a);
        assert_eq!(
            redownload_a.file_name().unwrap().to_str().unwrap(),
            "alice_C8reelAAA (1).mp4"
        );
        // Existing file is untouched.
        assert_eq!(std::fs::read(&path_a).unwrap(), b"existing-video-a-bytes");

        // Unicode and Windows-invalid characters are sanitized while preserving extension,
        // and path traversal prefixes are stripped.
        let unicode = suggested_filename_with_metadata(
            "https://scontent.cdninstagram.com/v/t50/clip.mp4?oe=1",
            Some("Instagram"),
            Some("www.instagram.com"),
            Some("mp4"),
            Some("..\\..\\夏の旅: Final Clip?_C8uni123.mp4"),
        )
        .unwrap();
        assert_eq!(unicode, "夏の旅_ Final Clip__C8uni123.mp4");

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn suggests_mp4_for_hls_fmp4_playlists() {
        assert_eq!(
            suggested_filename_with_metadata(
                "https://cdn.example/video/index.m3u8?token=short-lived",
                Some("Clip title"),
                Some("instagram.com"),
                Some("hls"),
                None,
            )
            .unwrap(),
            "Clip title.mp4"
        );
    }

    #[test]
    fn sanitizes_unicode_video_titles_and_limits_only_the_stem() {
        let filename = suggested_filename_with_metadata(
            "https://cdn.example/clip.mp4",
            Some("夏の動画: \"Final\"/Clip?*<x>|"),
            Some("example.com"),
            Some("mp4"),
            None,
        )
        .unwrap();
        let (stem, extension) = filename.rsplit_once('.').unwrap();
        assert_eq!(extension, "mp4");
        assert!(stem.starts_with("夏の動画"));
        assert!(stem.chars().count() <= MAX_FILENAME_STEM_CHARS);
        assert!(!stem.chars().any(|character| {
            character.is_control()
                || matches!(
                    character,
                    '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*'
                )
        }));

        let long = suggested_filename_with_metadata(
            "https://cdn.example/clip.webm",
            Some(&"é".repeat(100)),
            Some("example.com"),
            Some("webm"),
            None,
        )
        .unwrap();
        let (stem, extension) = long.rsplit_once('.').unwrap();
        assert_eq!(stem.chars().count(), 80);
        assert_eq!(extension, "webm");
    }

    #[test]
    fn chooses_duplicate_filenames_with_incrementing_suffixes() {
        let existing = ["Video.mp4", "Video (1).mp4"];
        let unique =
            unique_filename("Video.mp4", |candidate| !existing.contains(&candidate)).unwrap();
        assert_eq!(unique, "Video (2).mp4");
        let unique =
            unique_filename("Video (1).mp4", |candidate| candidate != "Video (1).mp4").unwrap();
        assert_eq!(unique, "Video (2).mp4");

        let long = unique_filename(&format!("{}.mp4", "x".repeat(80)), |candidate| {
            candidate.starts_with(&"x".repeat(76)) && candidate.contains("(1)")
        })
        .unwrap();
        let (stem, extension) = long.rsplit_once('.').unwrap();
        assert_eq!(extension, "mp4");
        assert!(stem.chars().count() <= MAX_FILENAME_STEM_CHARS);
        assert!(stem.ends_with("(1)"));
    }

    #[test]
    fn sanitizes_browser_filenames_without_losing_their_extension() {
        assert_eq!(
            suggested_filename_with_metadata(
                "https://files.example/download",
                None,
                None,
                Some("zip"),
                Some("C:\\Downloads\\Report: final?.zip"),
            )
            .unwrap(),
            "Report_ final_.zip"
        );
    }

    #[test]
    fn classifies_request_failures_by_their_underlying_cause() {
        assert_eq!(
            classify_request_failure(
                true,
                true,
                false,
                "error sending request: operation timed out",
            ),
            RequestFailureKind::Timeout
        );
        assert_eq!(
            classify_request_failure(
                false,
                true,
                false,
                "dns error: failed to lookup address: no such host (os error 11001)",
            ),
            RequestFailureKind::Dns
        );
        assert_eq!(
            classify_request_failure(
                false,
                true,
                false,
                "invalid peer certificate: unknown issuer",
            ),
            RequestFailureKind::Tls
        );
        assert_eq!(
            classify_request_failure(false, true, false, "connection refused"),
            RequestFailureKind::Connection
        );
        assert_eq!(
            classify_request_failure(false, false, true, "too many redirects"),
            RequestFailureKind::Redirect
        );
        assert_eq!(
            classify_request_failure(false, false, false, "protocol error"),
            RequestFailureKind::Request
        );
    }

    #[test]
    fn redacts_query_parameters_from_transport_diagnostics() {
        let url =
            reqwest::Url::parse("https://example.test/download?token=secret&key=private").unwrap();
        let message = safe_request_error_detail(
            "connection failed for https://example.test/download?token=secret&key=private",
            Some(&url),
        );
        assert!(message.contains("requested URL"));
        assert!(!message.contains("secret"));
        assert!(!message.contains("private"));
        let query_only = safe_request_error_detail(
            "connection failed at /download?token=secret&key=private",
            Some(&url),
        );
        assert!(query_only.contains("[query redacted]"));
        assert!(!query_only.contains("secret"));
        assert!(!query_only.contains("private"));
    }

    #[test]
    #[ignore = "requires live internet access"]
    fn downloads_small_public_https_file_with_application_client() {
        tauri::async_runtime::block_on(async {
            let response = build_http_client()
                .unwrap()
                .get("https://speed.cloudflare.com/__down?bytes=1024")
                .send()
                .await
                .unwrap_or_else(|error| {
                    panic!(
                        "{}",
                        describe_reqwest_error(
                            &error,
                            "https://speed.cloudflare.com/__down?bytes=1024"
                        )
                    )
                });
            assert_eq!(response.status(), StatusCode::OK);
            let mut stream = response.bytes_stream();
            let mut received = 0usize;
            while let Some(chunk) = stream.next().await {
                received += chunk.unwrap().len();
            }
            assert_eq!(received, 1024);
        });
    }

    #[test]
    fn calculates_progress_for_known_unknown_and_excess_bytes() {
        assert_eq!(progress_percent(25, Some(100)), Some(25.0));
        assert_eq!(progress_percent(20, None), None);
        assert_eq!(progress_percent(120, Some(100)), Some(100.0));
    }

    #[test]
    fn converts_bandwidth_limits_from_megabits_to_bytes_per_second() {
        assert_eq!(bandwidth_bytes_per_second(1.0), Some(125_000));
        assert_eq!(bandwidth_bytes_per_second(0.25), Some(31_250));
        assert_eq!(bandwidth_bytes_per_second(0.0), None);
    }

    #[test]
    fn validates_download_state_transitions() {
        assert!(validate_transition(&DownloadStatus::Queued, &DownloadStatus::Downloading).is_ok());
        assert!(validate_transition(&DownloadStatus::Downloading, &DownloadStatus::Paused).is_ok());
        assert!(validate_transition(&DownloadStatus::Paused, &DownloadStatus::Queued).is_ok());
        assert!(validate_transition(&DownloadStatus::Paused, &DownloadStatus::Downloading).is_ok());
        assert!(validate_transition(&DownloadStatus::Failed, &DownloadStatus::Queued).is_ok());
        assert!(validate_transition(&DownloadStatus::Cancelled, &DownloadStatus::Queued).is_ok());
        assert!(DownloadStatus::Failed.can_retry());
        assert!(DownloadStatus::Cancelled.can_retry());
        assert!(!DownloadStatus::Completed.can_retry());
        assert!(validate_transition(&DownloadStatus::Completed, &DownloadStatus::Queued).is_err());
    }

    #[test]
    fn validates_http_range_response_values() {
        assert_eq!(
            validate_range_response(
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 100-999/1000"),
                100,
                Some("\"v1\""),
                Some("\"v1\""),
            ),
            Ok(1000)
        );
        assert!(
            validate_range_response(
                StatusCode::OK,
                Some("bytes 100-999/1000"),
                100,
                Some("\"v1\""),
                Some("\"v1\""),
            )
            .is_err()
        );
        assert!(
            validate_range_response(
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 100-999/1000"),
                100,
                None,
                None,
            )
            .is_err()
        );
        assert!(
            validate_range_response(
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 99-999/1000"),
                100,
                Some("\"v1\""),
                Some("\"v1\""),
            )
            .is_err()
        );
        assert!(
            validate_range_response(
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 100-199/1000"),
                100,
                Some("\"v1\""),
                Some("\"v1\""),
            )
            .is_err()
        );
        assert!(
            validate_range_response(
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 100-999/1000"),
                100,
                Some("\"v1\""),
                Some("\"v2\""),
            )
            .is_err()
        );
        assert_eq!(
            parse_content_range("bytes 100-199/1000"),
            Some((100, 199, 1000))
        );
        assert_eq!(
            parse_content_range("bytes 99-199/1000"),
            Some((99, 199, 1000))
        );
        assert_eq!(parse_content_range("bytes 100-100/100"), None);
        assert_eq!(parse_content_range("items 100-199/1000"), None);
        assert_eq!(parse_content_range("bytes */1000"), None);
    }

    fn sample_website_catalog() -> crate::media::yt_dlp::VideoDiscovery {
        crate::media::yt_dlp::VideoDiscovery {
            title: "Website Clip".into(),
            duration_seconds: Some(15.0),
            extractor: "ExampleSite".into(),
            formats: vec![
                crate::media::yt_dlp::DiscoveredFormat {
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
                },
                crate::media::yt_dlp::DiscoveredFormat {
                    format_id: "split-1080".into(),
                    resolution: Some("1920x1080".into()),
                    height: Some(1080),
                    fps: Some(60.0),
                    video_codec: Some("avc1".into()),
                    audio_codec: Some("none".into()),
                    container: Some("mp4".into()),
                    file_size: Some(2048),
                    has_video: true,
                    has_audio: false,
                    requires_separate_audio: true,
                },
            ],
            best_available: true,
        }
    }

    fn valid_mp4_with_handlers(handlers: &[&[u8; 4]]) -> Vec<u8> {
        let sample_table = {
            let mut stbl = mp4_atom(b"stsd", &[0; 8]);
            stbl.extend(mp4_atom(b"stts", &[0; 4]));
            stbl.extend(mp4_atom(b"stsc", &[0; 4]));
            stbl.extend(mp4_atom(b"stsz", &[0; 8]));
            stbl.extend(mp4_atom(b"stco", &[0; 8]));
            mp4_atom(b"stbl", &stbl)
        };
        let minf = mp4_atom(b"minf", &sample_table);
        let track_with_handler = |handler_type: &[u8; 4]| {
            let mut hdlr_payload = vec![0u8; 8];
            hdlr_payload.extend_from_slice(handler_type);
            let mut mdia = mp4_atom(b"mdhd", &[0; 8]);
            mdia.extend(mp4_atom(b"hdlr", &hdlr_payload));
            mdia.extend(&minf);
            mp4_atom(b"trak", &mp4_atom(b"mdia", &mdia))
        };
        let mut moov = mp4_atom(b"mvhd", &[0; 8]);
        for handler in handlers {
            moov.extend(track_with_handler(handler));
        }
        let mut bytes = mp4_atom(b"ftyp", b"isom\0\0\0\0isom");
        bytes.extend(mp4_atom(b"moov", &moov));
        bytes.extend(mp4_atom(b"mdat", &[0; 16]));
        bytes
    }

    #[cfg(windows)]
    #[test]
    fn website_download_validates_strict_muxed_mp4_tracks() {
        let valid_path = write_temp_download_file(
            "website-muxed.mp4",
            &valid_mp4_with_handlers(&[b"vide", b"soun"]),
        );
        assert!(validate_ytdlp_mp4(&valid_path).is_ok());
        assert!(validate_ytdlp_media(&valid_path).is_ok());
        std::fs::remove_file(valid_path).unwrap();

        let single_track_path =
            write_temp_download_file("website-single.mp4", &valid_mp4_with_handlers(&[b"vide"]));
        let err = validate_ytdlp_mp4(&single_track_path).unwrap_err();
        assert!(err.contains("both audio and video tracks"));
        std::fs::remove_file(single_track_path).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn website_download_integrates_with_queue_concurrency_and_failure_lifecycle() {
        let manager = DownloadManager::default();
        let hold_control = Arc::new(DownloadTaskControl::default());
        {
            let mut state = manager.state.lock().unwrap();
            state.max_concurrent = 1;
        }
        let held_slot = tauri::async_runtime::block_on(async {
            manager
                .concurrency
                .acquire(&manager.state, &hold_control)
                .await
                .unwrap()
                .expect("should hold the single concurrency slot")
        });

        let dir = std::env::temp_dir().join(format!(
            "fluxnet-website-queue-test-{}-{}",
            std::process::id(),
            current_timestamp_ms()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let dest = dir.join("clip.mp4");
        let exe = std::env::var_os("ComSpec")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(r"C:\Windows\System32\cmd.exe"));
        let catalog = sample_website_catalog();

        let mismatch = manager.start_website_download(
            None,
            "https://example.test/watch?v=1".into(),
            dir.join("clip.webm").to_string_lossy().into_owned(),
            exe.clone(),
            catalog.clone(),
            crate::media::yt_dlp::FormatSelection::BestAvailable,
        );
        assert!(mismatch.is_err());

        let entry = manager
            .start_website_download(
                None,
                "https://example.test/watch?v=1".into(),
                dest.to_string_lossy().into_owned(),
                exe,
                catalog,
                crate::media::yt_dlp::FormatSelection::BestAvailable,
            )
            .expect("website download should queue inside DownloadManager");

        assert_eq!(entry.status, DownloadStatus::Queued);
        assert_eq!(
            entry.website_media,
            Some(WebsiteMediaInfo {
                format_id: "best[ext=mp4]".into(),
                audio_format_id: None,
                container: "mp4".into(),
                height: Some(720),
            })
        );

        std::thread::sleep(Duration::from_millis(40));
        let snapshot = manager.snapshot().unwrap();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(
            snapshot[0].status,
            DownloadStatus::Queued,
            "website worker must wait for the DownloadManager concurrency slot"
        );

        drop(held_slot);

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let current = manager.snapshot().unwrap();
            if current[0].status.is_terminal() {
                assert_eq!(current[0].status, DownloadStatus::Failed);
                assert!(current[0].error.is_some());
                break;
            }
            assert!(
                Instant::now() < deadline,
                "website worker did not transition to terminal state"
            );
            std::thread::sleep(Duration::from_millis(25));
        }

        assert_eq!(
            manager.concurrency.active.load(Ordering::Acquire),
            0,
            "terminal website task must release its concurrency slot"
        );
        assert_eq!(
            manager.worker_count.load(Ordering::Acquire),
            0,
            "terminal website task must decrement worker_count"
        );
        assert!(
            !manager
                .state
                .lock()
                .unwrap()
                .controls
                .contains_key(&entry.id),
            "terminal website task must remove its control handle"
        );
        assert_eq!(
            std::fs::read_dir(&dir).unwrap().count(),
            0,
            "failed website task must clean up temporary staging directories"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn website_download_cancelled_while_queued_never_becomes_completed_and_releases_slot() {
        let manager = DownloadManager::default();
        let hold_control = Arc::new(DownloadTaskControl::default());
        {
            let mut state = manager.state.lock().unwrap();
            state.max_concurrent = 1;
        }
        let held_slot = tauri::async_runtime::block_on(async {
            manager
                .concurrency
                .acquire(&manager.state, &hold_control)
                .await
                .unwrap()
                .expect("should hold the single concurrency slot")
        });

        let dir = std::env::temp_dir().join(format!(
            "fluxnet-website-cancel-test-{}-{}",
            std::process::id(),
            current_timestamp_ms()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let dest = dir.join("cancel.mp4");
        let exe = std::env::var_os("ComSpec")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(r"C:\Windows\System32\cmd.exe"));
        let catalog = sample_website_catalog();

        let entry = manager
            .start_website_download(
                None,
                "https://example.test/watch?v=cancel".into(),
                dest.to_string_lossy().into_owned(),
                exe,
                catalog,
                crate::media::yt_dlp::FormatSelection::Exact("muxed-mp4".into()),
            )
            .unwrap();

        let (workers, cancelled_count) = cancel_all_downloads(&manager).unwrap();
        assert_eq!(cancelled_count, 1);
        drop(held_slot);

        let deadline = Instant::now() + Duration::from_secs(5);
        while workers.load(Ordering::Acquire) > 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(workers.load(Ordering::Acquire), 0);
        assert_eq!(manager.concurrency.active.load(Ordering::Acquire), 0);

        let snapshot = manager.snapshot().unwrap();
        assert_eq!(snapshot[0].id, entry.id);
        assert_eq!(snapshot[0].status, DownloadStatus::Cancelled);
        assert!(!dest.exists());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn website_download_rejects_pause_resume_and_retry_and_supports_direct_cancellation() {
        let manager = DownloadManager::default();
        let hold_control = Arc::new(DownloadTaskControl::default());
        {
            let mut state = manager.state.lock().unwrap();
            state.max_concurrent = 1;
        }
        let held_slot = tauri::async_runtime::block_on(async {
            manager
                .concurrency
                .acquire(&manager.state, &hold_control)
                .await
                .unwrap()
                .expect("should hold the single concurrency slot")
        });

        let dir = std::env::temp_dir().join(format!(
            "fluxnet-website-lifecycle-test-{}-{}",
            std::process::id(),
            current_timestamp_ms()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let dest = dir.join("lifecycle.mp4");
        let exe = std::env::var_os("ComSpec")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(r"C:\Windows\System32\cmd.exe"));
        let catalog = sample_website_catalog();

        let entry = manager
            .start_website_download(
                None,
                "https://example.test/watch?v=lifecycle".into(),
                dest.to_string_lossy().into_owned(),
                exe.clone(),
                catalog.clone(),
                crate::media::yt_dlp::FormatSelection::Exact("muxed-mp4".into()),
            )
            .unwrap();

        let control = {
            let state = manager.state.lock().unwrap();
            let stored = state.entries.iter().find(|e| e.id == entry.id).unwrap();
            assert!(stored.website_media.is_some());
            Arc::clone(state.controls.get(&entry.id).unwrap())
        };

        control.request_cancel();
        drop(held_slot);

        let deadline = Instant::now() + Duration::from_secs(5);
        while manager.worker_count.load(Ordering::Acquire) > 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(manager.worker_count.load(Ordering::Acquire), 0);
        assert_eq!(manager.concurrency.active.load(Ordering::Acquire), 0);

        let snapshot = manager.snapshot().unwrap();
        assert_eq!(snapshot[0].status, DownloadStatus::Cancelled);

        // Verify direct run_ytdlp_download_worker helper also respects pre-cancelled control
        let second_entry = {
            let mut state = manager.state.lock().unwrap();
            let second_id = "website-direct-helper-cancel".to_owned();
            let second_control = Arc::new(DownloadTaskControl::default());
            second_control.request_cancel();
            let e = DownloadEntry {
                id: second_id.clone(),
                url: "https://example.test/watch?v=helper".into(),
                destination: dir.join("helper.mp4").to_string_lossy().into_owned(),
                filename: "helper.mp4".into(),
                status: DownloadStatus::Queued,
                total_bytes: None,
                downloaded_bytes: 0,
                speed_bytes_per_second: 0.0,
                elapsed_ms: 0,
                remaining_seconds: None,
                error: None,
                created_at_ms: 0,
                updated_at_ms: 0,
                attempts: 1,
                hls_playlist: None,
                hls_completed_parts: None,
                website_media: Some(WebsiteMediaInfo {
                    format_id: "muxed-mp4".into(),
                    audio_format_id: None,
                    container: "mp4".into(),
                    height: Some(720),
                }),
                segments: Vec::new(),
                etag: None,
                last_modified: None,
                notifications_enabled: false,
                notify_on_complete: false,
                notify_on_failure: false,
            };
            state.entries.push(e.clone());
            state
                .controls
                .insert(second_id, Arc::clone(&second_control));
            (e, second_control)
        };
        tauri::async_runtime::block_on(async {
            manager
                .run_ytdlp_download_worker(
                    None,
                    &second_entry.0.id,
                    exe,
                    catalog,
                    crate::media::yt_dlp::FormatSelection::Exact("muxed-mp4".into()),
                    &second_entry.1,
                )
                .await
                .unwrap();
        });
        let snapshot = manager.snapshot().unwrap();
        let helper_entry = snapshot.iter().find(|e| e.id == second_entry.0.id).unwrap();
        assert_eq!(helper_entry.status, DownloadStatus::Cancelled);
        assert_eq!(manager.concurrency.active.load(Ordering::Acquire), 0);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn website_download_supports_multiple_pause_resume_cycles_slot_release_and_elapsed_preservation()
     {
        let manager = DownloadManager::default();
        {
            let mut state = manager.state.lock().unwrap();
            state.max_concurrent = 1;
        }
        let dir = std::env::temp_dir().join(format!(
            "fluxnet-website-pause-resume-cycles-{}-{}",
            std::process::id(),
            current_timestamp_ms()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let dest = dir.join("cycles.mp4");
        let partial = partial_path(&dest);
        let exe = std::env::current_exe().unwrap();
        let catalog = sample_website_catalog();

        let entry = manager
            .start_website_download(
                None,
                "https://example.test/fixture-pause-resume".into(),
                dest.to_string_lossy().into_owned(),
                exe,
                catalog,
                crate::media::yt_dlp::FormatSelection::Exact("muxed-mp4".into()),
            )
            .unwrap();

        // Wait for first progress report (downloaded_bytes == 32, status == Downloading)
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let snap = manager.snapshot().unwrap();
            if snap[0].status == DownloadStatus::Downloading && snap[0].downloaded_bytes == 32 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "did not reach first 32-byte downloading checkpoint"
            );
            std::thread::sleep(Duration::from_millis(15));
        }

        // Cycle 1: Downloading -> Paused
        manager.pause_download(&entry.id).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let first_elapsed = loop {
            let snap = manager.snapshot().unwrap();
            if snap[0].status == DownloadStatus::Paused
                && manager.worker_count.load(Ordering::Acquire) == 0
            {
                assert_eq!(snap[0].downloaded_bytes, 32);
                assert_eq!(snap[0].speed_bytes_per_second, 0.0);
                assert!(snap[0].remaining_seconds.is_none());
                assert!(snap[0].elapsed_ms > 0);
                break snap[0].elapsed_ms;
            }
            assert!(Instant::now() < deadline, "did not transition to Paused");
            std::thread::sleep(Duration::from_millis(15));
        };
        assert_eq!(
            manager.concurrency.active.load(Ordering::Acquire),
            0,
            "paused website download must release its concurrency slot"
        );
        assert!(partial.is_file());
        assert_eq!(std::fs::metadata(&partial).unwrap().len(), 32);

        // Hold the slot before resuming to verify Paused -> Queued transition waits for the slot
        let hold_control = Arc::new(DownloadTaskControl::default());
        let held_slot = tauri::async_runtime::block_on(async {
            manager
                .concurrency
                .acquire(&manager.state, &hold_control)
                .await
                .unwrap()
                .unwrap()
        });

        manager.resume_download(None, &entry.id).unwrap();
        std::thread::sleep(Duration::from_millis(30));
        let snap = manager.snapshot().unwrap();
        assert_eq!(snap[0].status, DownloadStatus::Queued);
        assert_eq!(snap[0].attempts, 2);
        assert_eq!(snap[0].downloaded_bytes, 32);
        assert_eq!(snap[0].elapsed_ms, first_elapsed);

        // Release slot so worker transitions Queued -> Downloading and reaches 64 bytes
        drop(held_slot);
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let snap = manager.snapshot().unwrap();
            if snap[0].status == DownloadStatus::Downloading && snap[0].downloaded_bytes == 64 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "did not reach second 64-byte downloading checkpoint"
            );
            std::thread::sleep(Duration::from_millis(15));
        }

        // Cycle 2: Downloading -> Paused
        manager.pause_download(&entry.id).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let second_elapsed = loop {
            let snap = manager.snapshot().unwrap();
            if snap[0].status == DownloadStatus::Paused
                && manager.worker_count.load(Ordering::Acquire) == 0
            {
                assert_eq!(snap[0].downloaded_bytes, 64);
                assert_eq!(snap[0].speed_bytes_per_second, 0.0);
                assert!(snap[0].remaining_seconds.is_none());
                assert!(
                    snap[0].elapsed_ms > first_elapsed,
                    "elapsed_ms must accumulate across pause/resume cycles"
                );
                break snap[0].elapsed_ms;
            }
            assert!(
                Instant::now() < deadline,
                "did not transition to Paused on second cycle"
            );
            std::thread::sleep(Duration::from_millis(15));
        };
        assert_eq!(manager.concurrency.active.load(Ordering::Acquire), 0);
        assert_eq!(std::fs::metadata(&partial).unwrap().len(), 64);

        // Cycle 3: Paused -> Queued -> Downloading -> Completed
        manager.resume_download(None, &entry.id).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let snap = manager.snapshot().unwrap();
            if snap[0].status == DownloadStatus::Completed
                && manager.worker_count.load(Ordering::Acquire) == 0
            {
                assert_eq!(snap[0].attempts, 3);
                assert!(snap[0].elapsed_ms > second_elapsed);
                assert!(snap[0].downloaded_bytes > 64);
                assert_eq!(snap[0].total_bytes, Some(snap[0].downloaded_bytes));
                break;
            }
            assert!(
                Instant::now() < deadline,
                "did not complete after second resume: {:?}",
                snap[0]
            );
            std::thread::sleep(Duration::from_millis(15));
        }
        assert!(dest.is_file());
        assert!(!partial.exists());
        validate_ytdlp_mp4(&dest).unwrap();
        assert_eq!(manager.concurrency.active.load(Ordering::Acquire), 0);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn website_download_cancel_while_paused_and_pause_followed_by_cancel_clean_up_partial_files() {
        let manager = DownloadManager::default();
        let dir = std::env::temp_dir().join(format!(
            "fluxnet-website-pause-cancel-{}-{}",
            std::process::id(),
            current_timestamp_ms()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let exe = std::env::current_exe().unwrap();
        let catalog = sample_website_catalog();

        // 1. Cancel while Paused via cancel_download removes .fluxnet-part
        let dest1 = dir.join("cancel-paused.mp4");
        let part1 = partial_path(&dest1);
        let entry1 = manager
            .start_website_download(
                None,
                "https://example.test/fixture-pause-resume".into(),
                dest1.to_string_lossy().into_owned(),
                exe.clone(),
                catalog.clone(),
                crate::media::yt_dlp::FormatSelection::Exact("muxed-mp4".into()),
            )
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while manager.snapshot().unwrap()[0].downloaded_bytes != 32 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(15));
        }
        manager.pause_download(&entry1.id).unwrap();
        while (manager.snapshot().unwrap()[0].status != DownloadStatus::Paused
            || manager.worker_count.load(Ordering::Acquire) > 0)
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(15));
        }
        assert!(part1.is_file());

        manager.cancel_download(None, &entry1.id).unwrap();
        let snap = manager.snapshot().unwrap();
        assert_eq!(snap[0].status, DownloadStatus::Cancelled);
        assert!(
            !part1.exists(),
            "cancelling a paused website download must remove its .fluxnet-part file"
        );

        // 2. Pause followed immediately by cancel while still Downloading: cancellation wins and cleans up .fluxnet-part
        let dest2 = dir.join("pause-then-cancel.mp4");
        let part2 = partial_path(&dest2);
        let entry2 = manager
            .start_website_download(
                None,
                "https://example.test/fixture-pause-resume".into(),
                dest2.to_string_lossy().into_owned(),
                exe.clone(),
                catalog.clone(),
                crate::media::yt_dlp::FormatSelection::Exact("muxed-mp4".into()),
            )
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while manager
            .snapshot()
            .unwrap()
            .iter()
            .find(|e| e.id == entry2.id)
            .unwrap()
            .downloaded_bytes
            != 32
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(15));
        }
        manager.pause_download(&entry2.id).unwrap();
        manager.cancel_download(None, &entry2.id).unwrap();

        while manager.worker_count.load(Ordering::Acquire) > 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(15));
        }
        let e2 = manager
            .snapshot()
            .unwrap()
            .into_iter()
            .find(|e| e.id == entry2.id)
            .unwrap();
        assert_eq!(e2.status, DownloadStatus::Cancelled);
        assert!(!dest2.exists());
        assert!(
            !part2.exists(),
            "pause followed by cancel must clean up .fluxnet-part"
        );

        // 3. Cancel while Paused via cancel_all_downloads removes .fluxnet-part
        let dest3 = dir.join("cancel-all-paused.mp4");
        let part3 = partial_path(&dest3);
        let entry3 = manager
            .start_website_download(
                None,
                "https://example.test/fixture-pause-resume".into(),
                dest3.to_string_lossy().into_owned(),
                exe,
                catalog,
                crate::media::yt_dlp::FormatSelection::Exact("muxed-mp4".into()),
            )
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while manager
            .snapshot()
            .unwrap()
            .iter()
            .find(|e| e.id == entry3.id)
            .unwrap()
            .downloaded_bytes
            != 32
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(15));
        }
        manager.pause_download(&entry3.id).unwrap();
        while (manager
            .snapshot()
            .unwrap()
            .iter()
            .find(|e| e.id == entry3.id)
            .unwrap()
            .status
            != DownloadStatus::Paused
            || manager.worker_count.load(Ordering::Acquire) > 0)
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(15));
        }
        assert!(part3.is_file());

        cancel_all_downloads(&manager).unwrap();
        let e3 = manager
            .snapshot()
            .unwrap()
            .into_iter()
            .find(|e| e.id == entry3.id)
            .unwrap();
        assert_eq!(e3.status, DownloadStatus::Cancelled);
        assert!(
            !part3.exists(),
            "cancel_all_downloads must remove paused website .fluxnet-part file"
        );

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn website_download_cancel_racing_with_completion_never_becomes_completed_and_cleans_up() {
        let manager = DownloadManager::default();
        let dir = std::env::temp_dir().join(format!(
            "fluxnet-website-cancel-complete-race-{}-{}",
            std::process::id(),
            current_timestamp_ms()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let dest = dir.join("race.mp4");
        let part = partial_path(&dest);
        let exe = std::env::current_exe().unwrap();
        let catalog = sample_website_catalog();

        let entry = manager
            .start_website_download(
                None,
                "https://example.test/fixture-fast-complete".into(),
                dest.to_string_lossy().into_owned(),
                exe,
                catalog,
                crate::media::yt_dlp::FormatSelection::Exact("muxed-mp4".into()),
            )
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        while manager.snapshot().unwrap()[0].downloaded_bytes != 100 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        manager.cancel_download(None, &entry.id).unwrap();

        while manager.worker_count.load(Ordering::Acquire) > 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let snap = manager.snapshot().unwrap();
        assert_eq!(snap[0].status, DownloadStatus::Cancelled);
        assert!(
            !dest.exists(),
            "cancelled race must not leave final destination"
        );
        assert!(!part.exists(), "cancelled race must not leave partial file");
        assert_eq!(manager.concurrency.active.load(Ordering::Acquire), 0);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn tauri_website_download_wiring_resolves_verified_executable_and_serializes_ipc_types() {
        let manager = DownloadManager::default();
        let catalog = crate::media::yt_dlp::retain_muxed_formats(sample_website_catalog()).unwrap();
        assert_eq!(catalog.formats.len(), 1);
        assert_eq!(catalog.formats[0].format_id, "muxed-mp4");

        // Verify IPC JSON round-trip for catalog and format selections
        let catalog_json = serde_json::to_string(&catalog).unwrap();
        let parsed_catalog: crate::media::yt_dlp::VideoDiscovery =
            serde_json::from_str(&catalog_json).unwrap();
        assert_eq!(parsed_catalog, catalog);

        let selection: crate::media::yt_dlp::FormatSelection =
            serde_json::from_str("{\"exact\":\"muxed-mp4\"}").unwrap();
        assert_eq!(
            selection,
            crate::media::yt_dlp::FormatSelection::Exact("muxed-mp4".into())
        );

        // Reject invalid URL, invalid selection, and extension mismatch before queueing
        let dir = std::env::temp_dir().join(format!(
            "fluxnet-phase4a-wiring-{}-{}",
            std::process::id(),
            current_timestamp_ms()
        ));
        std::fs::create_dir_all(&dir).unwrap();

        let bad_url_err = manager
            .start_website_download_verified(
                None,
                "http://127.0.0.1/private".into(),
                dir.join("clip.mp4").to_string_lossy().into_owned(),
                parsed_catalog.clone(),
                selection.clone(),
            )
            .unwrap_err();
        assert!(bad_url_err.contains("Invalid public website URL"));
        assert!(!bad_url_err.contains("yt-dlp.exe"));

        let bad_sel_err = manager
            .start_website_download_verified(
                None,
                "https://example.test/watch?v=1".into(),
                dir.join("clip.mp4").to_string_lossy().into_owned(),
                parsed_catalog.clone(),
                crate::media::yt_dlp::FormatSelection::Exact("missing-id".into()),
            )
            .unwrap_err();
        assert!(bad_sel_err.contains("not returned by discovery"));

        if let Ok(verified_exe) = manager.resolve_verified_ytdlp_executable(None) {
            let reused_exe = manager.resolve_verified_ytdlp_executable(None).unwrap();
            assert_eq!(verified_exe, reused_exe);

            let ext_mismatch = manager
                .start_website_download_verified(
                    None,
                    "https://example.test/watch?v=1".into(),
                    dir.join("clip.webm").to_string_lossy().into_owned(),
                    parsed_catalog,
                    selection,
                )
                .unwrap_err();
            assert!(ext_mismatch.contains("does not match the selected format container"));
            assert!(
                !ext_mismatch.contains(&verified_exe.to_string_lossy().as_ref()),
                "error message must never leak the executable path"
            );
        }

        let entry_info = WebsiteMediaInfo {
            format_id: "muxed-mp4".into(),
            audio_format_id: None,
            container: "mp4".into(),
            height: Some(720),
        };
        let info_json = serde_json::to_string(&entry_info).unwrap();
        assert_eq!(
            serde_json::from_str::<WebsiteMediaInfo>(&info_json).unwrap(),
            entry_info
        );

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn website_download_stalled_stream_fails_fast_on_idle_timeout_retains_partial_and_frees_slot() {
        let manager = DownloadManager::default();
        {
            let mut state = manager.state.lock().unwrap();
            state.max_concurrent = 1;
        }
        let dir = std::env::temp_dir().join(format!(
            "fluxnet-website-idle-stall-{}-{}",
            std::process::id(),
            current_timestamp_ms()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let dest_stalled = dir.join("stalled.mp4");
        let part_stalled = partial_path(&dest_stalled);
        let dest_next = dir.join("queued-next.mp4");
        let exe = std::env::current_exe().unwrap();
        let catalog = sample_website_catalog();

        let started = Instant::now();
        let stalled_entry = manager
            .start_website_download(
                None,
                "https://example.test/fixture-stall-idle".into(),
                dest_stalled.to_string_lossy().into_owned(),
                exe.clone(),
                catalog.clone(),
                crate::media::yt_dlp::FormatSelection::Exact("muxed-mp4".into()),
            )
            .unwrap();
        let next_entry = manager
            .start_website_download(
                None,
                "https://example.test/fixture-fast-complete".into(),
                dest_next.to_string_lossy().into_owned(),
                exe,
                catalog,
                crate::media::yt_dlp::FormatSelection::Exact("muxed-mp4".into()),
            )
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let snap = manager.snapshot().unwrap();
            let s1 = snap.iter().find(|e| e.id == stalled_entry.id).unwrap();
            let s2 = snap.iter().find(|e| e.id == next_entry.id).unwrap();
            if s1.status.is_terminal()
                && s2.status.is_terminal()
                && manager.worker_count.load(Ordering::Acquire) == 0
            {
                assert_eq!(
                    s1.status,
                    DownloadStatus::Failed,
                    "stalled job must be marked Failed, never Completed"
                );
                assert!(
                    s1.error
                        .as_deref()
                        .is_some_and(|msg| msg.contains("stalled without progress and timed out")),
                    "unexpected idle timeout error: {:?}",
                    s1.error
                );
                assert_eq!(s1.downloaded_bytes, 32);
                assert_eq!(
                    s2.status,
                    DownloadStatus::Completed,
                    "queued download behind stalled download must acquire freed slot and complete"
                );
                break;
            }
            assert!(
                Instant::now() < deadline,
                "stalled download did not fail fast on idle timeout"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "idle timeout must fail fast instead of waiting 45 minutes"
        );
        assert!(!dest_stalled.exists());
        assert!(
            part_stalled.is_file(),
            "partial output must be retained per resume policy on idle timeout"
        );
        assert_eq!(std::fs::metadata(&part_stalled).unwrap().len(), 32);
        assert!(dest_next.is_file());
        assert_eq!(manager.concurrency.active.load(Ordering::Acquire), 0);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn website_download_resume_re_verifies_ytdlp_sha256_and_fails_closed_on_tampering() {
        let manager = DownloadManager::default();
        let dir = std::env::temp_dir().join(format!(
            "fluxnet-website-resume-toctou-{}-{}",
            std::process::id(),
            current_timestamp_ms()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let dest = dir.join("resume-verify.mp4");
        let part = partial_path(&dest);
        let exe = std::env::current_exe().unwrap();
        let catalog = sample_website_catalog();

        let entry = manager
            .start_website_download(
                None,
                "https://example.test/fixture-pause-resume".into(),
                dest.to_string_lossy().into_owned(),
                exe,
                catalog,
                crate::media::yt_dlp::FormatSelection::Exact("muxed-mp4".into()),
            )
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while manager.snapshot().unwrap()[0].downloaded_bytes != 32 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(15));
        }
        manager.pause_download(&entry.id).unwrap();
        while (manager.snapshot().unwrap()[0].status != DownloadStatus::Paused
            || manager.worker_count.load(Ordering::Acquire) > 0)
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(15));
        }
        assert!(part.is_file());

        // Switch URL to non-fixture URL so resume exercises the unconditional production SHA-256 verifier.
        {
            let mut state = manager.state.lock().unwrap();
            state.entries[0].url = "https://example.test/watch?v=resume-verify".into();
        }

        // 1. Missing executable -> fails closed without spawning.
        {
            let mut state = manager.state.lock().unwrap();
            state
                .website_executables
                .insert(entry.id.clone(), dir.join("missing-yt-dlp.exe"));
        }
        let missing_err = manager.resume_download(None, &entry.id).unwrap_err();
        assert_eq!(
            missing_err,
            "The verified yt-dlp media engine is not available."
        );
        assert_eq!(manager.snapshot().unwrap()[0].status, DownloadStatus::Paused);
        assert_eq!(manager.snapshot().unwrap()[0].attempts, 1);
        assert_eq!(manager.worker_count.load(Ordering::Acquire), 0);

        // 2. Non-regular file (directory) -> fails closed without spawning.
        let fake_dir_exe = dir.join("dir-as-exe.exe");
        std::fs::create_dir_all(&fake_dir_exe).unwrap();
        {
            let mut state = manager.state.lock().unwrap();
            state
                .website_executables
                .insert(entry.id.clone(), fake_dir_exe);
        }
        assert!(manager.resume_download(None, &entry.id).is_err());
        assert_eq!(manager.snapshot().unwrap()[0].status, DownloadStatus::Paused);
        assert_eq!(manager.worker_count.load(Ordering::Acquire), 0);

        // 3. Wrong-size executable -> fails closed without spawning.
        let wrong_size_exe = dir.join("wrong-size-yt-dlp.exe");
        std::fs::write(&wrong_size_exe, b"tampered small binary").unwrap();
        {
            let mut state = manager.state.lock().unwrap();
            state
                .website_executables
                .insert(entry.id.clone(), wrong_size_exe);
        }
        assert!(manager.resume_download(None, &entry.id).is_err());
        assert_eq!(manager.snapshot().unwrap()[0].status, DownloadStatus::Paused);
        assert_eq!(manager.worker_count.load(Ordering::Acquire), 0);

        // 4. Exact pinned size (23,575,744 bytes) but tampered SHA-256 -> fails closed without spawning!
        let tampered_exact_size_exe = dir.join("tampered-exact-size-yt-dlp.exe");
        {
            let f = std::fs::File::create(&tampered_exact_size_exe).unwrap();
            f.set_len(crate::media::yt_dlp::VERIFIED_YTDLP_SIZE_BYTES)
                .unwrap();
        }
        {
            let mut state = manager.state.lock().unwrap();
            state
                .website_executables
                .insert(entry.id.clone(), tampered_exact_size_exe.clone());
        }
        let hash_mismatch_err = manager.resume_download(None, &entry.id).unwrap_err();
        assert_eq!(
            hash_mismatch_err,
            "The verified yt-dlp media engine is not available."
        );
        assert_eq!(manager.snapshot().unwrap()[0].status, DownloadStatus::Paused);
        assert_eq!(manager.snapshot().unwrap()[0].attempts, 1);
        assert_eq!(manager.worker_count.load(Ordering::Acquire), 0);
        let _ = std::fs::remove_file(&tampered_exact_size_exe);

        // 5. Valid pinned yt-dlp executable passes full SHA-256 verification and resumes cleanly!
        let verified_exe = manager
            .resolve_verified_ytdlp_executable(None)
            .expect("pinned yt-dlp-2026.10.26.exe should resolve and pass SHA-256 verification");
        let hold_control = Arc::new(DownloadTaskControl::default());
        {
            let mut state = manager.state.lock().unwrap();
            state.max_concurrent = 1;
            state
                .website_executables
                .insert(entry.id.clone(), verified_exe);
        }
        let held_slot = tauri::async_runtime::block_on(async {
            manager
                .concurrency
                .acquire(&manager.state, &hold_control)
                .await
                .unwrap()
                .unwrap()
        });
        manager
            .resume_download(None, &entry.id)
            .expect("valid pinned yt-dlp executable must pass resume SHA-256 verification");
        let snap = manager.snapshot().unwrap();
        assert_eq!(snap[0].status, DownloadStatus::Queued);
        assert_eq!(snap[0].attempts, 2);

        manager.cancel_download(None, &entry.id).unwrap();
        drop(held_slot);
        while manager.worker_count.load(Ordering::Acquire) > 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(15));
        }
        assert_eq!(manager.snapshot().unwrap()[0].status, DownloadStatus::Cancelled);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn website_download_aggregate_stage_and_runtime_over_1gib_terminates_and_marks_failed() {
        let manager = DownloadManager::default();
        let dir = std::env::temp_dir().join(format!(
            "fluxnet-website-agg-1gib-{}-{}",
            std::process::id(),
            current_timestamp_ms()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let dest = dir.join("overflow.mp4");
        let exe = std::env::current_exe().unwrap();
        let catalog = sample_website_catalog();

        let entry = manager
            .start_website_download(
                None,
                "https://example.test/fixture-aggregate-overflow".into(),
                dest.to_string_lossy().into_owned(),
                exe,
                catalog,
                crate::media::yt_dlp::FormatSelection::Exact("muxed-mp4".into()),
            )
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let snap = manager.snapshot().unwrap();
            if snap[0].status.is_terminal() && manager.worker_count.load(Ordering::Acquire) == 0 {
                assert_eq!(
                    snap[0].status,
                    DownloadStatus::Failed,
                    "aggregate >1 GiB job must fail, never complete"
                );
                assert!(
                    snap[0]
                        .error
                        .as_deref()
                        .is_some_and(|msg| msg.contains("exceeded the configured size limit")),
                    "unexpected error: {:?}",
                    snap[0].error
                );
                break;
            }
            assert!(
                Instant::now() < deadline,
                "aggregate >1 GiB download did not terminate promptly: {:?}",
                snap[0]
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(!dest.exists());
        assert_eq!(
            std::fs::read_dir(&dir).unwrap().count(),
            0,
            "stage and runtime files must be completely cleaned up after aggregate 1 GiB breach"
        );
        assert_eq!(manager.concurrency.active.load(Ordering::Acquire), 0);
        let _ = entry;
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn website_download_split_streams_merge_validates_and_cleans_up_on_merge_failure() {
        let manager = DownloadManager::default();
        let dir = std::env::temp_dir().join(format!(
            "fluxnet-website-split-merge-{}-{}",
            std::process::id(),
            current_timestamp_ms()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let exe = std::env::current_exe().unwrap();

        // Split-only catalog (NO muxed format): video-only 1080p MP4 + audio-only M4A.
        let split_catalog = crate::media::yt_dlp::VideoDiscovery {
            title: "Split DASH Video".into(),
            duration_seconds: Some(0.5),
            extractor: "FixtureSite".into(),
            best_available: true,
            formats: vec![
                crate::media::yt_dlp::DiscoveredFormat {
                    format_id: "137".into(),
                    resolution: Some("1920x1080".into()),
                    height: Some(1080),
                    fps: Some(30.0),
                    video_codec: Some("avc1.640028".into()),
                    audio_codec: Some("none".into()),
                    container: Some("mp4".into()),
                    file_size: Some(8192),
                    has_video: true,
                    has_audio: false,
                    requires_separate_audio: true,
                },
                crate::media::yt_dlp::DiscoveredFormat {
                    format_id: "140".into(),
                    resolution: None,
                    height: None,
                    fps: None,
                    video_codec: Some("none".into()),
                    audio_codec: Some("mp4a.40.2".into()),
                    container: Some("m4a".into()),
                    file_size: Some(4096),
                    has_video: false,
                    has_audio: true,
                    requires_separate_audio: false,
                },
            ],
        };

        // 1. Success case: downloads split video + audio, merges via real LGPL ffmpeg.exe,
        // validates with ffprobe (1 video + 1 audio, duration > 0) + full decode, commits single .mp4.
        let ok_dir = dir.join("ok");
        std::fs::create_dir_all(&ok_dir).unwrap();
        let ok_dest = ok_dir.join("split-merged.mp4");
        let ok_entry = manager
            .start_website_download(
                None,
                "https://example.test/fixture-split-ok".into(),
                ok_dest.to_string_lossy().into_owned(),
                exe.clone(),
                split_catalog.clone(),
                crate::media::yt_dlp::FormatSelection::BestAvailable,
            )
            .expect("split-only catalog should fall back to split video+audio plan");
        assert_eq!(
            ok_entry.website_media,
            Some(WebsiteMediaInfo {
                format_id: "137".into(),
                audio_format_id: Some("140".into()),
                container: "mp4".into(),
                height: Some(1080),
            })
        );

        let deadline = Instant::now() + Duration::from_secs(180);
        loop {
            let snap = manager.snapshot().unwrap();
            let current = snap.iter().find(|e| e.id == ok_entry.id).unwrap();
            if current.status.is_terminal() && manager.worker_count.load(Ordering::Acquire) == 0 {
                assert_eq!(
                    current.status,
                    DownloadStatus::Completed,
                    "valid split merge must complete: {:?}",
                    current.error
                );
                assert!(current.downloaded_bytes > 0);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "split merge did not complete (status={:?}, error={:?})",
                current.status,
                current.error
            );
            std::thread::sleep(Duration::from_millis(25));
        }
        assert!(ok_dest.is_file());
        let ok_files = std::fs::read_dir(&ok_dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(ok_files, vec!["split-merged.mp4".to_owned()]);

        // Confirm final committed .mp4 has exactly 1 video + 1 audio stream and duration > 0.
        let ffprobe = crate::media::ffmpeg::resolve_verified_ffprobe_executable(None).unwrap();
        let probe_dir = dir.join("probe-check");
        std::fs::create_dir_all(&probe_dir).unwrap();
        let probed = crate::media::ffmpeg::probe_local_media(
            &ffprobe,
            &ok_dest,
            &probe_dir,
            &DownloadTaskControl::default(),
        )
        .unwrap();
        assert_eq!(probed.video_stream_count, 1);
        assert_eq!(probed.audio_stream_count, 1);
        assert!(probed.duration_seconds.is_some_and(|d| d > 0.0));

        // 2. Merge-failure case: corrupt audio stream fails merge, marks Failed (never Completed),
        // cleans up both staged streams and any partial merged output, and frees slot.
        let fail_dir = dir.join("fail");
        std::fs::create_dir_all(&fail_dir).unwrap();
        let fail_dest = fail_dir.join("split-fail.mp4");
        let fail_entry = manager
            .start_website_download(
                None,
                "https://example.test/fixture-split-merge-fail".into(),
                fail_dest.to_string_lossy().into_owned(),
                exe,
                split_catalog,
                crate::media::yt_dlp::FormatSelection::BestAvailable,
            )
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(180);
        loop {
            let snap = manager.snapshot().unwrap();
            let current = snap.iter().find(|e| e.id == fail_entry.id).unwrap();
            if current.status.is_terminal() && manager.worker_count.load(Ordering::Acquire) == 0 {
                assert_eq!(
                    current.status,
                    DownloadStatus::Failed,
                    "merge failure must mark job Failed, never Completed"
                );
                assert!(current.error.is_some());
                break;
            }
            assert!(
                Instant::now() < deadline,
                "failed split merge did not terminate (status={:?}, error={:?})",
                current.status,
                current.error
            );
            std::thread::sleep(Duration::from_millis(25));
        }
        assert!(!fail_dest.exists());
        assert_eq!(
            std::fs::read_dir(&fail_dir).unwrap().count(),
            0,
            "failed split merge must clean up both staged streams and partial merged output"
        );
        assert_eq!(manager.concurrency.active.load(Ordering::Acquire), 0);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn resume_prefers_strong_etags_or_last_modified_validators() {
        assert_eq!(
            resume_validator(Some("\"strong\""), Some("Wed, 21 Oct 2015 07:28:00 GMT")),
            Some("\"strong\"")
        );
        assert_eq!(
            resume_validator(Some("W/\"weak\""), Some("Wed, 21 Oct 2015 07:28:00 GMT")),
            Some("Wed, 21 Oct 2015 07:28:00 GMT")
        );
        assert_eq!(resume_validator(Some("W/\"weak\""), None), None);
    }

    struct SyntheticHttpResponse {
        status_line: &'static str,
        headers: Vec<(String, String)>,
        content_length_override: Option<usize>,
        body: Vec<u8>,
        delay_before_body_ms: u64,
    }

    impl SyntheticHttpResponse {
        fn ok_mp4(body: Vec<u8>) -> Self {
            Self {
                status_line: "200 OK",
                headers: vec![("Content-Type".into(), "video/mp4".into())],
                content_length_override: None,
                body,
                delay_before_body_ms: 0,
            }
        }

        fn partial_mp4(start: usize, end: usize, total: usize, body: Vec<u8>) -> Self {
            Self {
                status_line: "206 Partial Content",
                headers: vec![
                    ("Content-Type".into(), "video/mp4".into()),
                    (
                        "Content-Range".into(),
                        format!("bytes {start}-{end}/{total}"),
                    ),
                    ("ETag".into(), "\"synthetic-v1\"".into()),
                ],
                content_length_override: None,
                body,
                delay_before_body_ms: 0,
            }
        }
    }

    struct SyntheticServerHandle {
        stop_flag: Arc<AtomicBool>,
        join_handle: Option<std::thread::JoinHandle<Vec<Option<String>>>>,
    }

    impl SyntheticServerHandle {
        fn join(mut self) -> Vec<Option<String>> {
            if let Some(handle) = self.join_handle.take() {
                handle.join().unwrap()
            } else {
                Vec::new()
            }
        }

        fn stop_and_join(mut self) -> Vec<Option<String>> {
            self.stop_flag.store(true, Ordering::Release);
            if let Some(handle) = self.join_handle.take() {
                handle.join().unwrap()
            } else {
                Vec::new()
            }
        }
    }

    impl Drop for SyntheticServerHandle {
        fn drop(&mut self) {
            self.stop_flag.store(true, Ordering::Release);
            if let Some(handle) = self.join_handle.take() {
                let _ = handle.join();
            }
        }
    }

    fn spawn_synthetic_http_server<F>(
        expected_requests: usize,
        handler: F,
    ) -> (String, SyntheticServerHandle)
    where
        F: FnMut(usize, Option<String>) -> SyntheticHttpResponse + Send + 'static,
    {
        spawn_synthetic_http_server_with_timeout(
            expected_requests,
            Duration::from_secs(5),
            handler,
        )
    }

    fn spawn_synthetic_http_server_with_timeout<F>(
        expected_requests: usize,
        accept_timeout: Duration,
        mut handler: F,
    ) -> (String, SyntheticServerHandle)
    where
        F: FnMut(usize, Option<String>) -> SyntheticHttpResponse + Send + 'static,
    {
        use std::io::{Read as _, Write as _};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://127.0.0.1:{}/video.mp4", addr.port());
        let stop_flag = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop_flag);
        let handle = std::thread::spawn(move || {
            let mut observed_ranges = Vec::new();
            for req_idx in 0..expected_requests {
                let accept_deadline = Instant::now() + accept_timeout;
                let mut stream = loop {
                    if worker_stop.load(Ordering::Acquire) || Instant::now() >= accept_deadline {
                        return observed_ranges;
                    }
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(_) => return observed_ranges,
                    }
                };
                let _ = stream.set_nonblocking(false);
                let _ = stream.set_read_timeout(Some(Duration::from_secs(3)));
                let _ = stream.set_write_timeout(Some(Duration::from_secs(3)));
                let mut req_buf = Vec::new();
                let mut temp = [0_u8; 1024];
                while !req_buf.windows(4).any(|w| w == b"\r\n\r\n") && req_buf.len() < 16_384 {
                    match stream.read(&mut temp) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => req_buf.extend_from_slice(&temp[..n]),
                    }
                }
                let req_text = String::from_utf8_lossy(&req_buf);
                let range_header = req_text.lines().find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.trim()
                        .eq_ignore_ascii_case("range")
                        .then(|| value.trim().to_owned())
                });
                observed_ranges.push(range_header.clone());
                let response = handler(req_idx, range_header);
                let declared_len = response
                    .content_length_override
                    .unwrap_or(response.body.len());
                let mut head = format!(
                    "HTTP/1.1 {}\r\nConnection: close\r\nContent-Length: {}\r\n",
                    response.status_line, declared_len
                );
                for (k, v) in &response.headers {
                    head.push_str(&format!("{k}: {v}\r\n"));
                }
                head.push_str("\r\n");
                let _ = stream.write_all(head.as_bytes());
                let _ = stream.flush();
                if response.delay_before_body_ms > 0 {
                    std::thread::sleep(Duration::from_millis(response.delay_before_body_ms));
                }
                let _ = stream.write_all(&response.body);
                let _ = stream.flush();
            }
            observed_ranges
        });
        (
            url,
            SyntheticServerHandle {
                stop_flag,
                join_handle: Some(handle),
            },
        )
    }

    #[test]
    fn synthetic_http_server_terminates_cleanly_when_no_client_connects() {
        let started = Instant::now();
        let (_url, server) = spawn_synthetic_http_server_with_timeout(
            2,
            Duration::from_millis(100),
            |_, _| SyntheticHttpResponse::ok_mp4(vec![0; 16]),
        );
        let observed = server.join();
        assert!(
            observed.is_empty(),
            "no requests should be observed when no client connects"
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "synthetic HTTP server must terminate cleanly within its accept timeout"
        );
    }

    fn create_test_temp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "fluxnet-ig-range-test-{label}-{}-{}",
            std::process::id(),
            rand_id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn wait_for_download_settlement(manager: &DownloadManager, id: &str) -> DownloadEntry {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let snap = manager.snapshot().unwrap();
            let entry = snap.into_iter().find(|e| e.id == id).unwrap();
            if (entry.status.is_terminal() || entry.status == DownloadStatus::Paused)
                && manager.worker_count.load(Ordering::Acquire) == 0
            {
                return entry;
            }
            assert!(
                Instant::now() < deadline,
                "download {id} did not settle before timeout; current state: {entry:?}"
            );
            std::thread::sleep(Duration::from_millis(15));
        }
    }

    fn valid_fragmented_mp4_with_sidx(fragment_count: usize) -> (Vec<u8>, usize) {
        let mut init_prefix = mp4_atom(b"ftyp", b"isom\0\0\0\0isom");
        init_prefix.extend(valid_mp4_initialization(true));

        let mut fragments: Vec<Vec<u8>> = Vec::with_capacity(fragment_count);
        for index in 0..fragment_count {
            let mut track_fragment = mp4_atom(b"tfhd", &[0; 8]);
            track_fragment.extend(mp4_atom(b"trun", &[0; 8]));
            let mut movie_fragment = mp4_atom(b"mfhd", &[0; 8]);
            movie_fragment.extend(mp4_atom(b"traf", &track_fragment));
            let mut fragment = mp4_atom(b"moof", &movie_fragment);
            fragment.extend(mp4_atom(b"mdat", &vec![(index as u8) + 1; 24]));
            fragments.push(fragment);
        }

        // Build ISO BMFF v0 sidx box indexing each subsegment
        let mut sidx_payload = Vec::with_capacity(24 + fragments.len() * 12);
        sidx_payload.extend_from_slice(&[0, 0, 0, 0]); // version 0, flags 0
        sidx_payload.extend_from_slice(&1_u32.to_be_bytes()); // reference_ID
        sidx_payload.extend_from_slice(&1000_u32.to_be_bytes()); // timescale
        sidx_payload.extend_from_slice(&0_u32.to_be_bytes()); // earliest_presentation_time
        sidx_payload.extend_from_slice(&0_u32.to_be_bytes()); // first_offset = 0 from sidx.end
        sidx_payload.extend_from_slice(&0_u16.to_be_bytes()); // reserved
        sidx_payload.extend_from_slice(&(fragments.len() as u16).to_be_bytes()); // reference_count
        for fragment in &fragments {
            let ref_size = u32::try_from(fragment.len()).unwrap() & 0x7FFF_FFFF;
            sidx_payload.extend_from_slice(&ref_size.to_be_bytes());
            sidx_payload.extend_from_slice(&1000_u32.to_be_bytes()); // subsegment_duration
            sidx_payload.extend_from_slice(&0x8000_0000_u32.to_be_bytes()); // starts_with_SAP
        }
        init_prefix.extend(mp4_atom(b"sidx", &sidx_payload));
        let init_len = init_prefix.len();
        let mut full = init_prefix;
        for fragment in fragments {
            full.extend(fragment);
        }
        (full, init_len)
    }

    #[test]
    fn synthetic_complete_mp4_http_200_download_succeeds() {
        let mp4 = valid_mp4(false);
        let expected_mp4 = mp4.clone();
        let (url, server) =
            spawn_synthetic_http_server(1, move |_, _| SyntheticHttpResponse::ok_mp4(mp4.clone()));
        let dir = create_test_temp_dir("complete-200");
        let dest = dir.join("complete.mp4");
        let manager = DownloadManager::default();
        let entry = manager
            .start_download_internal(None, url, dest.to_string_lossy().into_owned(), None)
            .unwrap();
        let settled = wait_for_download_settlement(&manager, &entry.id);
        let ranges = server.stop_and_join();

        assert_eq!(settled.status, DownloadStatus::Completed, "error: {:?}", settled.error);
        assert_eq!(ranges, vec![None]);
        assert_eq!(settled.downloaded_bytes, expected_mp4.len() as u64);
        assert_eq!(std::fs::read(&dest).unwrap(), expected_mp4);
        assert!(!partial_path(&dest).exists());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn synthetic_single_http_206_byte_range_response_succeeds() {
        let mp4 = valid_mp4(false);
        let total = mp4.len();
        let expected_mp4 = mp4.clone();
        let (url, server) = spawn_synthetic_http_server(1, move |_, _| {
            SyntheticHttpResponse::partial_mp4(0, total - 1, total, mp4.clone())
        });
        let dir = create_test_temp_dir("single-206");
        let dest = dir.join("single206.mp4");
        let manager = DownloadManager::default();
        let entry = manager
            .start_download_internal(None, url, dest.to_string_lossy().into_owned(), None)
            .unwrap();
        let settled = wait_for_download_settlement(&manager, &entry.id);
        let ranges = server.stop_and_join();

        assert_eq!(settled.status, DownloadStatus::Completed, "error: {:?}", settled.error);
        assert_eq!(ranges, vec![None]);
        assert_eq!(settled.total_bytes, Some(total as u64));
        assert_eq!(std::fs::read(&dest).unwrap(), expected_mp4);
        assert!(!partial_path(&dest).exists());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn synthetic_multiple_http_206_byte_range_responses_assemble_complete_mp4() {
        let mp4 = valid_mp4(false);
        let total = mp4.len();
        let c1 = total / 3;
        let c2 = (total * 2) / 3;
        let expected_mp4 = mp4.clone();
        let (url, server) = spawn_synthetic_http_server(3, move |idx, _| match idx {
            0 => SyntheticHttpResponse::partial_mp4(0, c1 - 1, total, mp4[0..c1].to_vec()),
            1 => SyntheticHttpResponse::partial_mp4(c1, c2 - 1, total, mp4[c1..c2].to_vec()),
            _ => SyntheticHttpResponse::partial_mp4(c2, total - 1, total, mp4[c2..total].to_vec()),
        });
        let dir = create_test_temp_dir("multi-206");
        let dest = dir.join("multi206.mp4");
        let manager = DownloadManager::default();
        let entry = manager
            .start_download_internal(None, url, dest.to_string_lossy().into_owned(), None)
            .unwrap();
        let settled = wait_for_download_settlement(&manager, &entry.id);
        let ranges = server.stop_and_join();

        assert_eq!(settled.status, DownloadStatus::Completed, "error: {:?}", settled.error);
        assert_eq!(
            ranges,
            vec![
                None,
                Some(format!("bytes={c1}-")),
                Some(format!("bytes={c2}-")),
            ]
        );
        assert_eq!(settled.downloaded_bytes, total as u64);
        assert_eq!(std::fs::read(&dest).unwrap(), expected_mp4);
        assert!(!partial_path(&dest).exists());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn synthetic_http_206_missing_or_invalid_content_range_fails_without_creating_mp4() {
        let dir = create_test_temp_dir("invalid-content-range");
        for (index, headers) in [
            vec![("Content-Type".to_owned(), "video/mp4".to_owned())],
            vec![
                ("Content-Type".to_owned(), "video/mp4".to_owned()),
                ("Content-Range".to_owned(), "bytes invalid/200".to_owned()),
            ],
            vec![
                ("Content-Type".to_owned(), "video/mp4".to_owned()),
                ("Content-Range".to_owned(), "bytes 0-99/200".to_owned()),
            ],
        ]
        .into_iter()
        .enumerate()
        {
            let mp4 = valid_mp4(false);
            let (url, server) = spawn_synthetic_http_server(1, move |_, _| SyntheticHttpResponse {
                status_line: "206 Partial Content",
                headers: headers.clone(),
                content_length_override: None,
                body: mp4.clone(),
                delay_before_body_ms: 0,
            });
            let dest = dir.join(format!("bad-cr-{index}.mp4"));
            let manager = DownloadManager::default();
            let entry = manager
                .start_download_internal(None, url, dest.to_string_lossy().into_owned(), None)
                .unwrap();
            let settled = wait_for_download_settlement(&manager, &entry.id);
            let ranges = server.stop_and_join();

            assert_eq!(ranges.len(), 1);
            assert_eq!(settled.status, DownloadStatus::Failed);
            assert!(
                settled.error.as_deref().unwrap_or_default().contains("Content-Range"),
                "unexpected error: {:?}",
                settled.error
            );
            assert!(!dest.exists());
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn synthetic_fragmented_mp4_with_initialization_and_sidx_reconstructs_across_ranges() {
        let (fmp4, init_len) = valid_fragmented_mp4_with_sidx(2);
        let total = fmp4.len();
        assert_eq!(
            validate_mp4_bytes(fmp4.clone()),
            Mp4Validation::FragmentedWithInitialization
        );
        // Verify truncated fMP4 missing second subsegment declared in sidx is rejected by validate_mp4_reader
        let truncated_at_first_frag = fmp4[..total - 20].to_vec();
        assert!(matches!(
            validate_mp4_bytes(truncated_at_first_frag),
            Mp4Validation::Invalid(_)
        ));

        // Server returns HTTP 200 with ONLY initialization segment (ftyp + moov + sidx) on first request,
        // and DownloadManager detects incomplete representation via sidx and fetches remaining fragments via HTTP 206!
        let expected_fmp4 = fmp4.clone();
        let mid = init_len + (total - init_len) / 2;
        let (url, server) = spawn_synthetic_http_server(3, move |idx, _| match idx {
            0 => SyntheticHttpResponse::ok_mp4(fmp4[0..init_len].to_vec()),
            1 => SyntheticHttpResponse::partial_mp4(
                init_len,
                mid - 1,
                total,
                fmp4[init_len..mid].to_vec(),
            ),
            _ => SyntheticHttpResponse::partial_mp4(
                mid,
                total - 1,
                total,
                fmp4[mid..total].to_vec(),
            ),
        });
        let dir = create_test_temp_dir("fmp4-sidx");
        let dest = dir.join("fmp4_initialized.mp4");
        let manager = DownloadManager::default();
        let entry = manager
            .start_download_internal(None, url, dest.to_string_lossy().into_owned(), None)
            .unwrap();
        let settled = wait_for_download_settlement(&manager, &entry.id);
        let ranges = server.stop_and_join();

        assert_eq!(settled.status, DownloadStatus::Completed, "error: {:?}", settled.error);
        assert_eq!(
            ranges,
            vec![
                None,
                Some(format!("bytes={init_len}-{}", total - 1)),
                Some(format!("bytes={mid}-")),
            ]
        );
        assert_eq!(std::fs::read(&dest).unwrap(), expected_fmp4);
        assert!(!partial_path(&dest).exists());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn synthetic_fragmented_mp4_missing_initialization_segment_is_rejected() {
        let mut track_fragment = mp4_atom(b"tfhd", &[0; 8]);
        track_fragment.extend(mp4_atom(b"trun", &[0; 8]));
        let mut movie_fragment = mp4_atom(b"mfhd", &[0; 8]);
        movie_fragment.extend(mp4_atom(b"traf", &track_fragment));
        let mut uninitialized = mp4_atom(b"moof", &movie_fragment);
        uninitialized.extend(mp4_atom(b"mdat", &[7_u8; 64]));
        let total = uninitialized.len();
        let split = total / 2;

        let (url, server) = spawn_synthetic_http_server(2, move |idx, _| match idx {
            0 => SyntheticHttpResponse::partial_mp4(
                0,
                split - 1,
                total,
                uninitialized[0..split].to_vec(),
            ),
            _ => SyntheticHttpResponse::partial_mp4(
                split,
                total - 1,
                total,
                uninitialized[split..total].to_vec(),
            ),
        });
        let dir = create_test_temp_dir("fmp4-missing-init");
        let dest = dir.join("uninitialized.mp4");
        let manager = DownloadManager::default();
        let entry = manager
            .start_download_internal(None, url, dest.to_string_lossy().into_owned(), None)
            .unwrap();
        let settled = wait_for_download_settlement(&manager, &entry.id);
        let ranges = server.stop_and_join();

        assert_eq!(ranges.len(), 2);
        assert_eq!(settled.status, DownloadStatus::Failed);
        assert!(
            settled
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("fragmented MP4 without its required initialization segment"),
            "unexpected error: {:?}",
            settled.error
        );
        assert!(!dest.exists());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn synthetic_interrupted_range_download_resumes_from_exact_offset_on_retry() {
        let mp4 = valid_mp4(false);
        let total = mp4.len();
        let c1 = 48_usize;
        let dropped_after = 24_usize;
        let resume_offset = c1 + dropped_after;
        let expected_mp4 = mp4.clone();

        let (url, server) = spawn_synthetic_http_server(3, move |idx, _| match idx {
            0 => SyntheticHttpResponse::partial_mp4(0, c1 - 1, total, mp4[0..c1].to_vec()),
            1 => {
                // Declare remaining range c1..total, but drop connection after `dropped_after` bytes
                let mut resp = SyntheticHttpResponse::partial_mp4(
                    c1,
                    total - 1,
                    total,
                    mp4[c1..resume_offset].to_vec(),
                );
                resp.content_length_override = Some(total - c1);
                resp
            }
            _ => SyntheticHttpResponse::partial_mp4(
                resume_offset,
                total - 1,
                total,
                mp4[resume_offset..total].to_vec(),
            ),
        });
        let dir = create_test_temp_dir("interrupted-retry");
        let dest = dir.join("interrupted.mp4");
        let part = partial_path(&dest);
        let manager = DownloadManager::default();
        let entry = manager
            .start_download_internal(None, url, dest.to_string_lossy().into_owned(), None)
            .unwrap();
        let failed = wait_for_download_settlement(&manager, &entry.id);

        assert_eq!(failed.status, DownloadStatus::Failed);
        assert_eq!(
            failed.downloaded_bytes, resume_offset as u64,
            "interrupted download should preserve partial bytes; error was: {:?}",
            failed.error
        );
        assert!(!dest.exists());
        assert_eq!(std::fs::metadata(&part).unwrap().len(), resume_offset as u64);

        manager
            .retry_download_internal(None, entry.id.clone(), false)
            .unwrap();
        let completed = wait_for_download_settlement(&manager, &entry.id);
        let ranges = server.stop_and_join();

        assert_eq!(completed.status, DownloadStatus::Completed, "error: {:?}", completed.error);
        assert_eq!(
            ranges,
            vec![
                None,
                Some(format!("bytes={c1}-")),
                Some(format!("bytes={resume_offset}-")),
            ]
        );
        assert_eq!(std::fs::read(&dest).unwrap(), expected_mp4);
        assert!(!part.exists());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn synthetic_expired_instagram_signed_url_http_403_fails_cleanly() {
        let mp4 = valid_mp4(false);
        let total = mp4.len();
        let split = total / 2;
        let (url, server) = spawn_synthetic_http_server(2, move |idx, _| match idx {
            0 => SyntheticHttpResponse::partial_mp4(0, split - 1, total, mp4[0..split].to_vec()),
            _ => SyntheticHttpResponse {
                status_line: "403 Forbidden",
                headers: vec![("Content-Type".into(), "text/plain".into())],
                content_length_override: None,
                body: b"URL signature expired".to_vec(),
                delay_before_body_ms: 0,
            },
        });
        let dir = create_test_temp_dir("expired-403");
        let dest = dir.join("expired.mp4");
        let manager = DownloadManager::default();
        let entry = manager
            .start_download_internal(None, url, dest.to_string_lossy().into_owned(), None)
            .unwrap();
        let settled = wait_for_download_settlement(&manager, &entry.id);
        let ranges = server.stop_and_join();

        assert_eq!(ranges.len(), 2);
        assert_eq!(settled.status, DownloadStatus::Failed);
        let err = settled.error.unwrap_or_default();
        assert!(err.contains("403") && err.contains("expired"), "unexpected error: {err}");
        assert!(!dest.exists());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn synthetic_range_request_ignored_with_http_200_is_rejected_without_duplicating_bytes() {
        let mp4 = valid_mp4(false);
        let total = mp4.len();
        let first_len = 50_usize;
        let (url, server) = spawn_synthetic_http_server(2, move |idx, _| match idx {
            0 => SyntheticHttpResponse::partial_mp4(
                0,
                first_len - 1,
                total,
                mp4[0..first_len].to_vec(),
            ),
            _ => SyntheticHttpResponse::ok_mp4(mp4.clone()),
        });
        let dir = create_test_temp_dir("range-ignored-200");
        let dest = dir.join("ignored200.mp4");
        let part = partial_path(&dest);
        let manager = DownloadManager::default();
        let entry = manager
            .start_download_internal(None, url, dest.to_string_lossy().into_owned(), None)
            .unwrap();
        let settled = wait_for_download_settlement(&manager, &entry.id);
        let ranges = server.stop_and_join();

        assert_eq!(ranges.len(), 2);
        assert_eq!(settled.status, DownloadStatus::Failed);
        assert!(
            settled
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("did not honor the Range request"),
            "unexpected error: {:?}",
            settled.error
        );
        assert_eq!(settled.downloaded_bytes, first_len as u64);
        assert_eq!(std::fs::metadata(&part).unwrap().len(), first_len as u64);
        assert!(!dest.exists());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn synthetic_incorrect_content_range_offset_is_rejected() {
        let mp4 = valid_mp4(false);
        let total = mp4.len();
        let first_len = 50_usize;
        let wrong_start = 40_usize;
        let (url, server) = spawn_synthetic_http_server(2, move |idx, _| match idx {
            0 => SyntheticHttpResponse::partial_mp4(
                0,
                first_len - 1,
                total,
                mp4[0..first_len].to_vec(),
            ),
            _ => SyntheticHttpResponse::partial_mp4(
                wrong_start,
                total - 1,
                total,
                mp4[wrong_start..total].to_vec(),
            ),
        });
        let dir = create_test_temp_dir("wrong-range-offset");
        let dest = dir.join("wrong_offset.mp4");
        let part = partial_path(&dest);
        let manager = DownloadManager::default();
        let entry = manager
            .start_download_internal(None, url, dest.to_string_lossy().into_owned(), None)
            .unwrap();
        let settled = wait_for_download_settlement(&manager, &entry.id);
        let ranges = server.stop_and_join();

        assert_eq!(ranges.len(), 2);
        assert_eq!(settled.status, DownloadStatus::Failed);
        assert!(
            settled
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("wrong starting offset"),
            "unexpected error: {:?}",
            settled.error
        );
        assert_eq!(settled.downloaded_bytes, first_len as u64);
        assert_eq!(std::fs::metadata(&part).unwrap().len(), first_len as u64);
        assert!(!dest.exists());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn synthetic_application_interruption_during_transfer_recovers_and_resumes_on_retry() {
        let mp4 = valid_mp4(false);
        let total = mp4.len();
        let saved_offset = 64_usize;
        let expected_mp4 = mp4.clone();

        let dir = create_test_temp_dir("app-interruption");
        let dest = dir.join("app_interrupt.mp4");
        let part = partial_path(&dest);
        std::fs::write(&part, &mp4[0..saved_offset]).unwrap();

        let (url, server) = spawn_synthetic_http_server(1, move |_, _| {
            SyntheticHttpResponse::partial_mp4(
                saved_offset,
                total - 1,
                total,
                mp4[saved_offset..total].to_vec(),
            )
        });

        let manager = DownloadManager::default();
        let entry_id = "interrupted-on-exit-entry".to_owned();
        {
            let mut state = manager.state.lock().unwrap();
            let mut entry = DownloadEntry {
                id: entry_id.clone(),
                url,
                destination: dest.to_string_lossy().into_owned(),
                filename: "app_interrupt.mp4".into(),
                status: DownloadStatus::Downloading,
                total_bytes: Some(total as u64),
                downloaded_bytes: saved_offset as u64,
                speed_bytes_per_second: 1024.0,
                elapsed_ms: 150,
                remaining_seconds: Some(1.0),
                error: None,
                created_at_ms: 1000,
                updated_at_ms: 1150,
                attempts: 1,
                hls_playlist: None,
                hls_completed_parts: None,
                website_media: None,
                segments: Vec::new(),
                etag: Some("\"synthetic-v1\"".into()),
                last_modified: None,
                notifications_enabled: false,
                notify_on_complete: false,
                notify_on_failure: false,
            };
            // Simulate DownloadManager::initialize startup recovery of an interrupted in-flight transfer
            if matches!(
                &entry.status,
                DownloadStatus::Queued | DownloadStatus::Downloading | DownloadStatus::Paused
            ) {
                entry.status = DownloadStatus::Failed;
                entry.error = Some(
                    "Interrupted when FluxNet exited. The partial file was retained; retry explicitly to continue or restart."
                        .into(),
                );
            }
            state.entries.push(entry);
        }

        manager
            .retry_download_internal(None, entry_id.clone(), false)
            .unwrap();
        let settled = wait_for_download_settlement(&manager, &entry_id);
        let ranges = server.stop_and_join();

        assert_eq!(settled.status, DownloadStatus::Completed, "error: {:?}", settled.error);
        assert_eq!(ranges, vec![Some(format!("bytes={saved_offset}-"))]);
        assert_eq!(settled.downloaded_bytes, total as u64);
        assert_eq!(std::fs::read(&dest).unwrap(), expected_mp4);
        assert!(!part.exists());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn synthetic_multi_range_pause_resume_and_cancel_preserve_byte_integrity() {
        let mp4 = valid_mp4(false);
        let total = mp4.len();
        let split = total / 2;
        let expected_mp4 = mp4.clone();

        // 1. Range 0 (0..split) completes immediately; Range 1 pauses before body so pause_download
        // deterministically catches the active download at exact offset `split`, then resume completes Range 2.
        let mp4_for_pause = mp4.clone();
        let (url, server) = spawn_synthetic_http_server(3, move |idx, _| match idx {
            0 => SyntheticHttpResponse::partial_mp4(
                0,
                split - 1,
                total,
                mp4_for_pause[0..split].to_vec(),
            ),
            1 => {
                let mut r = SyntheticHttpResponse::partial_mp4(
                    split,
                    total - 1,
                    total,
                    mp4_for_pause[split..total].to_vec(),
                );
                r.delay_before_body_ms = 300;
                r
            }
            _ => SyntheticHttpResponse::partial_mp4(
                split,
                total - 1,
                total,
                mp4_for_pause[split..total].to_vec(),
            ),
        });
        let dir = create_test_temp_dir("range-pause-resume");
        let dest = dir.join("paused_range.mp4");
        let part = partial_path(&dest);
        let manager = DownloadManager::default();
        let entry = manager
            .start_download_internal(None, url, dest.to_string_lossy().into_owned(), None)
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let snap = manager.snapshot().unwrap();
            let current = &snap[0];
            if current.status == DownloadStatus::Downloading
                && current.downloaded_bytes == split as u64
            {
                break;
            }
            assert!(
                !current.status.is_terminal() && Instant::now() < deadline,
                "download did not reach pause checkpoint at {split} bytes; state: {current:?}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        manager.pause_download(&entry.id).unwrap();
        let paused = wait_for_download_settlement(&manager, &entry.id);
        assert_eq!(paused.status, DownloadStatus::Paused);
        assert_eq!(paused.downloaded_bytes, split as u64);
        assert_eq!(std::fs::metadata(&part).unwrap().len(), split as u64);
        assert!(!dest.exists());

        manager.resume_download(None, &entry.id).unwrap();
        let completed = wait_for_download_settlement(&manager, &entry.id);
        let ranges = server.stop_and_join();

        assert_eq!(completed.status, DownloadStatus::Completed, "error: {:?}", completed.error);
        assert_eq!(
            ranges,
            vec![
                None,
                Some(format!("bytes={split}-")),
                Some(format!("bytes={split}-")),
            ]
        );
        assert_eq!(std::fs::read(&dest).unwrap(), expected_mp4);
        assert!(!part.exists());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn parallel_segment_plan_divides_ranges_and_rejects_missing_overlapping_or_duplicated_segments()
    {
        assert_eq!(DEFAULT_CONNECTIONS_PER_DOWNLOAD, 4);
        let manager = DownloadManager::default();
        assert_eq!(
            manager.settings().unwrap().max_connections_per_download,
            DEFAULT_CONNECTIONS_PER_DOWNLOAD
        );
        manager.configure_connections_per_download(6).unwrap();
        assert_eq!(manager.settings().unwrap().max_connections_per_download, 6);
        assert!(manager.configure_connections_per_download(0).is_err());
        assert!(
            manager
                .configure_connections_per_download(MAX_CONNECTIONS_PER_DOWNLOAD + 1)
                .is_err()
        );

        let segments = plan_parallel_segments(212, 4);
        assert_eq!(segments.len(), 4);
        assert_eq!(
            segments
                .iter()
                .map(|s| (s.index, s.start, s.end))
                .collect::<Vec<_>>(),
            vec![(0, 0, 52), (1, 53, 105), (2, 106, 158), (3, 159, 211)]
        );
        assert!(validate_segment_plan(&segments, 212).is_ok());

        // Missing byte gap between segment 0 and segment 1
        let mut with_gap = segments.clone();
        with_gap[1].start = 54;
        assert!(
            validate_segment_plan(&with_gap, 212)
                .unwrap_err()
                .contains("missing byte gap")
        );

        // Overlapping segments
        let mut overlapping = segments.clone();
        overlapping[2].start = 105;
        assert!(
            validate_segment_plan(&overlapping, 212)
                .unwrap_err()
                .contains("overlaps")
        );

        // Duplicated segment index
        let mut duplicated = segments.clone();
        duplicated[2].index = 1;
        assert!(
            validate_segment_plan(&duplicated, 212)
                .unwrap_err()
                .contains("duplicated segment")
        );

        // Truncated total coverage
        assert!(
            validate_segment_plan(&segments, 250)
                .unwrap_err()
                .contains("total file size is 250")
        );
    }

    #[test]
    fn startup_auto_resume_rejects_expired_signed_url_and_prevents_completed_paused_cancelled_or_duplicate_restarts()
     {
        let dir = create_test_temp_dir("startup-eligibility");
        let history_path = dir.join("download-history.json");

        let completed_dest = dir.join("completed.mp4");
        std::fs::write(&completed_dest, valid_mp4(false)).unwrap();

        let paused_dest = dir.join("paused.mp4");
        std::fs::write(partial_path(&paused_dest), vec![1_u8; 40]).unwrap();

        let expired_ig_url = "https://instagram.fdac5-1.fna.fbcdn.net/o1/v/t2/f2/m367/video.mp4?oh=00_AQR&oe=5F000000";
        let persisted = PersistedDownloads {
            version: 1,
            max_concurrent: 2,
            max_connections_per_download: 4,
            bandwidth_limit_bytes_per_second: None,
            notifications_enabled: false,
            notify_on_complete: false,
            notify_on_failure: false,
            entries: vec![
                DownloadEntry {
                    id: "done-1".into(),
                    url: "http://127.0.0.1:9/never-called.mp4".into(),
                    destination: completed_dest.to_string_lossy().into_owned(),
                    filename: "completed.mp4".into(),
                    status: DownloadStatus::Completed,
                    total_bytes: Some(212),
                    downloaded_bytes: 212,
                    speed_bytes_per_second: 0.0,
                    elapsed_ms: 50,
                    remaining_seconds: Some(0.0),
                    error: None,
                    created_at_ms: 100,
                    updated_at_ms: 150,
                    attempts: 1,
                    hls_playlist: None,
                    hls_completed_parts: None,
                    website_media: None,
                    segments: Vec::new(),
                    etag: None,
                    last_modified: None,
                    notifications_enabled: false,
                    notify_on_complete: false,
                    notify_on_failure: false,
                },
                DownloadEntry {
                    id: "paused-1".into(),
                    url: "http://127.0.0.1:9/paused.mp4".into(),
                    destination: paused_dest.to_string_lossy().into_owned(),
                    filename: "paused.mp4".into(),
                    status: DownloadStatus::Paused,
                    total_bytes: Some(212),
                    downloaded_bytes: 40,
                    speed_bytes_per_second: 0.0,
                    elapsed_ms: 20,
                    remaining_seconds: None,
                    error: None,
                    created_at_ms: 100,
                    updated_at_ms: 120,
                    attempts: 1,
                    hls_playlist: None,
                    hls_completed_parts: None,
                    website_media: None,
                    segments: Vec::new(),
                    etag: Some("\"v1\"".into()),
                    last_modified: None,
                    notifications_enabled: false,
                    notify_on_complete: false,
                    notify_on_failure: false,
                },
                DownloadEntry {
                    id: "cancelled-1".into(),
                    url: "http://127.0.0.1:9/cancelled.mp4".into(),
                    destination: dir.join("cancelled.mp4").to_string_lossy().into_owned(),
                    filename: "cancelled.mp4".into(),
                    status: DownloadStatus::Cancelled,
                    total_bytes: Some(212),
                    downloaded_bytes: 0,
                    speed_bytes_per_second: 0.0,
                    elapsed_ms: 5,
                    remaining_seconds: None,
                    error: None,
                    created_at_ms: 100,
                    updated_at_ms: 105,
                    attempts: 1,
                    hls_playlist: None,
                    hls_completed_parts: None,
                    website_media: None,
                    segments: Vec::new(),
                    etag: None,
                    last_modified: None,
                    notifications_enabled: false,
                    notify_on_complete: false,
                    notify_on_failure: false,
                },
                DownloadEntry {
                    id: "expired-ig-1".into(),
                    url: expired_ig_url.into(),
                    destination: dir.join("expired_ig.mp4").to_string_lossy().into_owned(),
                    filename: "expired_ig.mp4".into(),
                    status: DownloadStatus::Downloading,
                    total_bytes: Some(212),
                    downloaded_bytes: 0,
                    speed_bytes_per_second: 500.0,
                    elapsed_ms: 10,
                    remaining_seconds: Some(1.0),
                    error: None,
                    created_at_ms: 100,
                    updated_at_ms: 110,
                    attempts: 1,
                    hls_playlist: None,
                    hls_completed_parts: None,
                    website_media: None,
                    segments: Vec::new(),
                    etag: None,
                    last_modified: None,
                    notifications_enabled: false,
                    notify_on_complete: false,
                    notify_on_failure: false,
                },
                DownloadEntry {
                    id: "duplicate-guarded-1".into(),
                    url: "http://127.0.0.1:9/active.mp4".into(),
                    destination: dir.join("active.mp4").to_string_lossy().into_owned(),
                    filename: "active.mp4".into(),
                    status: DownloadStatus::Downloading,
                    total_bytes: Some(212),
                    downloaded_bytes: 0,
                    speed_bytes_per_second: 0.0,
                    elapsed_ms: 0,
                    remaining_seconds: None,
                    error: None,
                    created_at_ms: 100,
                    updated_at_ms: 110,
                    attempts: 1,
                    hls_playlist: None,
                    hls_completed_parts: None,
                    website_media: None,
                    segments: Vec::new(),
                    etag: None,
                    last_modified: None,
                    notifications_enabled: false,
                    notify_on_complete: false,
                    notify_on_failure: false,
                },
            ],
        };
        std::fs::write(&history_path, serde_json::to_vec(&persisted).unwrap()).unwrap();

        let manager = DownloadManager::default();
        // Pre-insert an active control for `duplicate-guarded-1` to verify duplicate worker prevention
        {
            let mut state = manager.state.lock().unwrap();
            state.controls.insert(
                "duplicate-guarded-1".into(),
                Arc::new(DownloadTaskControl::default()),
            );
        }

        manager
            .initialize_with_history_path(None, history_path)
            .unwrap();
        // Zero background workers should have been spawned!
        assert_eq!(manager.worker_count.load(Ordering::Acquire), 0);

        let snap = manager.snapshot().unwrap();
        let done = snap.iter().find(|e| e.id == "done-1").unwrap();
        assert_eq!(done.status, DownloadStatus::Completed);
        assert_eq!(done.attempts, 1);

        let paused = snap.iter().find(|e| e.id == "paused-1").unwrap();
        assert_eq!(paused.status, DownloadStatus::Paused);
        assert_eq!(paused.downloaded_bytes, 40);
        assert_eq!(paused.attempts, 1);

        let cancelled = snap.iter().find(|e| e.id == "cancelled-1").unwrap();
        assert_eq!(cancelled.status, DownloadStatus::Cancelled);

        let expired = snap.iter().find(|e| e.id == "expired-ig-1").unwrap();
        assert_eq!(expired.status, DownloadStatus::Failed);
        assert!(
            expired
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("expired")
        );

        std::fs::remove_dir_all(dir).unwrap();
    }

    fn spawn_concurrent_range_http_server(
        payload: Vec<u8>,
        expected_requests: usize,
        range_hold_ms: u64,
        ignore_ranges_with_200: bool,
    ) -> (
        String,
        Arc<AtomicUsize>,
        Arc<Mutex<Vec<Option<String>>>>,
        SyntheticServerHandle,
    ) {
        use std::io::{Read as _, Write as _};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://127.0.0.1:{}/parallel.mp4", addr.port());
        let stop_flag = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop_flag);
        let active_range_requests = Arc::new(AtomicUsize::new(0));
        let max_concurrent_range_requests = Arc::new(AtomicUsize::new(0));
        let max_concurrent_for_server = Arc::clone(&max_concurrent_range_requests);
        let observed_ranges = Arc::new(Mutex::new(Vec::new()));
        let observed_for_server = Arc::clone(&observed_ranges);

        let handle = std::thread::spawn(move || {
            let mut conn_threads: Vec<std::thread::JoinHandle<()>> = Vec::new();
            for _ in 0..expected_requests {
                let accept_deadline = Instant::now() + Duration::from_secs(5);
                let mut stream = loop {
                    if worker_stop.load(Ordering::Acquire) || Instant::now() >= accept_deadline {
                        for t in conn_threads {
                            let _ = t.join();
                        }
                        return Vec::new();
                    }
                    match listener.accept() {
                        Ok((s, _)) => break s,
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(2));
                        }
                        Err(_) => {
                            for t in conn_threads {
                                let _ = t.join();
                            }
                            return Vec::new();
                        }
                    }
                };
                let data = payload.clone();
                let active = Arc::clone(&active_range_requests);
                let max_conc = Arc::clone(&max_concurrent_for_server);
                let obs = Arc::clone(&observed_for_server);

                conn_threads.push(std::thread::spawn(move || {
                    let _ = stream.set_nonblocking(false);
                    let _ = stream.set_read_timeout(Some(Duration::from_secs(3)));
                    let _ = stream.set_write_timeout(Some(Duration::from_secs(3)));
                    let mut req_buf = Vec::new();
                    let mut temp = [0_u8; 1024];
                    while !req_buf.windows(4).any(|w| w == b"\r\n\r\n") && req_buf.len() < 16_384 {
                        match stream.read(&mut temp) {
                            Ok(0) | Err(_) => break,
                            Ok(n) => req_buf.extend_from_slice(&temp[..n]),
                        }
                    }
                    let req_text = String::from_utf8_lossy(&req_buf);
                    let range_header = req_text.lines().find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.trim()
                            .eq_ignore_ascii_case("range")
                            .then(|| value.trim().to_owned())
                    });
                    obs.lock().unwrap().push(range_header.clone());

                    let total = data.len();
                    if let Some(raw_range) = range_header.filter(|_| !ignore_ranges_with_200) {
                        let current_active = active.fetch_add(1, Ordering::AcqRel) + 1;
                        let mut prev_max = max_conc.load(Ordering::Acquire);
                        while current_active > prev_max {
                            match max_conc.compare_exchange_weak(
                                prev_max,
                                current_active,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            ) {
                                Ok(_) => break,
                                Err(actual) => prev_max = actual,
                            }
                        }

                        if range_hold_ms > 0 {
                            std::thread::sleep(Duration::from_millis(range_hold_ms));
                        }

                        let range_spec = raw_range.strip_prefix("bytes=").unwrap_or("");
                        let (s_str, e_str) = range_spec.split_once('-').unwrap_or(("0", ""));
                        let start = s_str.parse::<usize>().unwrap_or(0);
                        let end = if e_str.is_empty() {
                            total - 1
                        } else {
                            e_str.parse::<usize>().unwrap_or(total - 1).min(total - 1)
                        };
                        let slice = &data[start..=end];
                        let head = format!(
                            "HTTP/1.1 206 Partial Content\r\nConnection: close\r\nContent-Type: video/mp4\r\nAccept-Ranges: bytes\r\nETag: \"parallel-v1\"\r\nContent-Range: bytes {start}-{end}/{total}\r\nContent-Length: {}\r\n\r\n",
                            slice.len()
                        );
                        let _ = stream.write_all(head.as_bytes());
                        let _ = stream.write_all(slice);
                        let _ = stream.flush();
                        active.fetch_sub(1, Ordering::AcqRel);
                    } else {
                        let head = format!(
                            "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: video/mp4\r\nAccept-Ranges: bytes\r\nETag: \"parallel-v1\"\r\nContent-Length: {total}\r\n\r\n"
                        );
                        let _ = stream.write_all(head.as_bytes());
                        let _ = stream.write_all(&data);
                        let _ = stream.flush();
                    }
                }));
            }
            for t in conn_threads {
                let _ = t.join();
            }
            Vec::new()
        });

        (
            url,
            max_concurrent_range_requests,
            observed_ranges,
            SyntheticServerHandle {
                stop_flag,
                join_handle: Some(handle),
            },
        )
    }

    #[test]
    fn synthetic_parallel_multi_segment_http_download_overlaps_requests_and_assembles_exact_file() {
        // Make the MP4 payload larger (~2 KiB) so 4 segments each carry substantial distinct data
        let mut large_mdat = Vec::with_capacity(2048);
        for i in 0..2048_usize {
            large_mdat.push((i % 251) as u8);
        }
        let mut mp4 = mp4_atom(b"ftyp", b"isom\0\0\0\0isom");
        mp4.extend(valid_mp4_initialization(false));
        mp4.extend(mp4_atom(b"mdat", &large_mdat));
        let expected_mp4 = mp4.clone();

        // 1 initial probe GET + 4 concurrent Range GETs = 5 requests
        let (url, max_concurrent_observed, observed_ranges, server) =
            spawn_concurrent_range_http_server(mp4, 5, 90, false);
        let dir = create_test_temp_dir("parallel-4-segments");
        let dest = dir.join("parallel.mp4");
        let manager = DownloadManager::default();
        let entry = manager
            .start_download_internal(None, url, dest.to_string_lossy().into_owned(), None)
            .unwrap();
        let settled = wait_for_download_settlement(&manager, &entry.id);
        server.stop_and_join();

        assert_eq!(
            settled.status,
            DownloadStatus::Completed,
            "parallel download failed: {:?}",
            settled.error
        );
        let max_conc = max_concurrent_observed.load(Ordering::Acquire);
        assert!(
            max_conc >= 2,
            "expected parallel range requests to overlap in time (max concurrent observed = {max_conc})"
        );
        let ranges = observed_ranges.lock().unwrap().clone();
        assert_eq!(ranges.len(), 5);
        assert_eq!(ranges.iter().filter(|r| r.is_some()).count(), 4);
        assert_eq!(std::fs::read(&dest).unwrap(), expected_mp4);
        assert!(!partial_path(&dest).exists());
        for idx in 0..4 {
            assert!(!segment_part_path(&dest, &entry.id, idx).exists());
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn synthetic_parallel_download_falls_back_to_sequential_when_ranges_are_ignored() {
        let mp4 = valid_mp4(false);
        let expected_mp4 = mp4.clone();
        // Server advertises `Accept-Ranges: bytes` on initial 200 OK, but ignores `Range` headers with 200 OK.
        // 1 initial probe + up to 4 parallel attempts + 1 sequential fallback = 6 max connections
        let (url, _max_conc, _observed, server) =
            spawn_concurrent_range_http_server(mp4, 6, 0, true);
        let dir = create_test_temp_dir("parallel-fallback-seq");
        let dest = dir.join("fallback.mp4");
        let manager = DownloadManager::default();
        manager.configure_connections_per_download(2).unwrap();
        let entry = manager
            .start_download_internal(None, url, dest.to_string_lossy().into_owned(), None)
            .unwrap();
        let settled = wait_for_download_settlement(&manager, &entry.id);
        server.stop_and_join();

        assert_eq!(
            settled.status,
            DownloadStatus::Completed,
            "fallback download failed: {:?}",
            settled.error
        );
        assert_eq!(std::fs::read(&dest).unwrap(), expected_mp4);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn synthetic_app_restart_automatically_resumes_single_and_multiple_unfinished_downloads() {
        let mp4 = valid_mp4(false);
        let total = mp4.len();
        let offset1 = 60_usize;
        let offset2 = 96_usize;
        let expected_mp4 = mp4.clone();

        let dir = create_test_temp_dir("auto-resume-multiple");
        let history_path = dir.join("download-history.json");
        let dest1 = dir.join("resume_one.mp4");
        let dest2 = dir.join("resume_two.mp4");
        std::fs::write(partial_path(&dest1), &mp4[0..offset1]).unwrap();
        std::fs::write(partial_path(&dest2), &mp4[0..offset2]).unwrap();

        let mp4_for_s1 = mp4.clone();
        let (url1, server1) = spawn_synthetic_http_server(1, move |_, _| {
            SyntheticHttpResponse::partial_mp4(
                offset1,
                total - 1,
                total,
                mp4_for_s1[offset1..total].to_vec(),
            )
        });
        let mp4_for_s2 = mp4.clone();
        let (url2, server2) = spawn_synthetic_http_server(1, move |_, _| {
            SyntheticHttpResponse::partial_mp4(
                offset2,
                total - 1,
                total,
                mp4_for_s2[offset2..total].to_vec(),
            )
        });

        let persisted = PersistedDownloads {
            version: 1,
            max_concurrent: 2,
            max_connections_per_download: 4,
            bandwidth_limit_bytes_per_second: None,
            notifications_enabled: false,
            notify_on_complete: false,
            notify_on_failure: false,
            entries: vec![
                DownloadEntry {
                    id: "auto-resume-1".into(),
                    url: url1,
                    destination: dest1.to_string_lossy().into_owned(),
                    filename: "resume_one.mp4".into(),
                    status: DownloadStatus::Downloading,
                    total_bytes: Some(total as u64),
                    downloaded_bytes: offset1 as u64,
                    speed_bytes_per_second: 100.0,
                    elapsed_ms: 50,
                    remaining_seconds: None,
                    error: None,
                    created_at_ms: 100,
                    updated_at_ms: 150,
                    attempts: 1,
                    hls_playlist: None,
                    hls_completed_parts: None,
                    website_media: None,
                    segments: Vec::new(),
                    etag: Some("\"synthetic-v1\"".into()),
                    last_modified: None,
                    notifications_enabled: false,
                    notify_on_complete: false,
                    notify_on_failure: false,
                },
                DownloadEntry {
                    id: "auto-resume-2".into(),
                    url: url2,
                    destination: dest2.to_string_lossy().into_owned(),
                    filename: "resume_two.mp4".into(),
                    status: DownloadStatus::Queued,
                    total_bytes: Some(total as u64),
                    downloaded_bytes: offset2 as u64,
                    speed_bytes_per_second: 0.0,
                    elapsed_ms: 30,
                    remaining_seconds: None,
                    error: None,
                    created_at_ms: 100,
                    updated_at_ms: 130,
                    attempts: 1,
                    hls_playlist: None,
                    hls_completed_parts: None,
                    website_media: None,
                    segments: Vec::new(),
                    etag: Some("\"synthetic-v1\"".into()),
                    last_modified: None,
                    notifications_enabled: false,
                    notify_on_complete: false,
                    notify_on_failure: false,
                },
            ],
        };
        std::fs::write(&history_path, serde_json::to_vec(&persisted).unwrap()).unwrap();

        let manager = DownloadManager::default();
        manager
            .initialize_with_history_path(None, history_path)
            .unwrap();

        let settled1 = wait_for_download_settlement(&manager, "auto-resume-1");
        let settled2 = wait_for_download_settlement(&manager, "auto-resume-2");
        let ranges1 = server1.stop_and_join();
        let ranges2 = server2.stop_and_join();

        assert_eq!(settled1.status, DownloadStatus::Completed, "err1: {:?}", settled1.error);
        assert_eq!(settled2.status, DownloadStatus::Completed, "err2: {:?}", settled2.error);
        assert_eq!(ranges1, vec![Some(format!("bytes={offset1}-"))]);
        assert_eq!(ranges2, vec![Some(format!("bytes={offset2}-"))]);
        assert_eq!(std::fs::read(&dest1).unwrap(), expected_mp4);
        assert_eq!(std::fs::read(&dest2).unwrap(), expected_mp4);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn synthetic_app_restart_resumes_partially_completed_parallel_segments_and_repairs_corrupted_segment()
     {
        let mp4 = valid_mp4(false);
        let total = mp4.len();
        let expected_mp4 = mp4.clone();
        let mut segments = plan_parallel_segments(total as u64, 4);
        assert_eq!(segments.len(), 4);

        let dir = create_test_temp_dir("auto-resume-parallel-segments");
        let history_path = dir.join("download-history.json");
        let dest = dir.join("segmented_resume.mp4");
        let entry_id = "seg-resume-id";

        // Segment 0: valid completed on disk
        let s0_len = (segments[0].end - segments[0].start + 1) as usize;
        std::fs::write(
            segment_part_path(&dest, entry_id, 0),
            &mp4[segments[0].start as usize..=segments[0].end as usize],
        )
        .unwrap();
        segments[0].downloaded_bytes = s0_len as u64;
        segments[0].completed = true;

        // Segment 1: partially completed (20 bytes verified on disk)
        let s1_partial = 20_usize;
        let s1_start = segments[1].start as usize;
        std::fs::write(
            segment_part_path(&dest, entry_id, 1),
            &mp4[s1_start..s1_start + s1_partial],
        )
        .unwrap();
        segments[1].downloaded_bytes = s1_partial as u64;
        segments[1].completed = false;

        // Segment 2: marked completed in metadata, but CORRUPTED (truncated to 7 bytes) on disk!
        let s2_len = (segments[2].end - segments[2].start + 1) as usize;
        std::fs::write(segment_part_path(&dest, entry_id, 2), &[0xFF_u8; 7]).unwrap();
        segments[2].downloaded_bytes = s2_len as u64;
        segments[2].completed = true;

        // Segment 3: valid completed on disk
        let s3_len = (segments[3].end - segments[3].start + 1) as usize;
        std::fs::write(
            segment_part_path(&dest, entry_id, 3),
            &mp4[segments[3].start as usize..=segments[3].end as usize],
        )
        .unwrap();
        segments[3].downloaded_bytes = s3_len as u64;
        segments[3].completed = true;

        // Only 2 HTTP range requests should be made:
        // - Segment 1 remaining range: (segments[1].start + 20)..=segments[1].end
        // - Segment 2 full range (repaired from corruption): segments[2].start..=segments[2].end
        let (url, _max_conc, observed_ranges, server) =
            spawn_concurrent_range_http_server(mp4, 2, 0, false);

        let persisted = PersistedDownloads {
            version: 1,
            max_concurrent: 2,
            max_connections_per_download: 4,
            bandwidth_limit_bytes_per_second: None,
            notifications_enabled: false,
            notify_on_complete: false,
            notify_on_failure: false,
            entries: vec![DownloadEntry {
                id: entry_id.into(),
                url,
                destination: dest.to_string_lossy().into_owned(),
                filename: "segmented_resume.mp4".into(),
                status: DownloadStatus::Downloading,
                total_bytes: Some(total as u64),
                downloaded_bytes: (s0_len + s1_partial + s2_len + s3_len) as u64,
                speed_bytes_per_second: 0.0,
                elapsed_ms: 40,
                remaining_seconds: None,
                error: None,
                created_at_ms: 100,
                updated_at_ms: 140,
                attempts: 1,
                hls_playlist: None,
                hls_completed_parts: None,
                website_media: None,
                segments: segments.clone(),
                etag: Some("\"parallel-v1\"".into()),
                last_modified: None,
                notifications_enabled: false,
                notify_on_complete: false,
                notify_on_failure: false,
            }],
        };
        std::fs::write(&history_path, serde_json::to_vec(&persisted).unwrap()).unwrap();

        let manager = DownloadManager::default();
        manager
            .initialize_with_history_path(None, history_path)
            .unwrap();
        let settled = wait_for_download_settlement(&manager, entry_id);
        server.stop_and_join();

        assert_eq!(
            settled.status,
            DownloadStatus::Completed,
            "segmented auto-resume failed: {:?}",
            settled.error
        );
        let mut ranges = observed_ranges
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .flatten()
            .collect::<Vec<_>>();
        ranges.sort();
        let mut expected_ranges = vec![
            format!(
                "bytes={}-{}",
                segments[1].start + (s1_partial as u64),
                segments[1].end
            ),
            format!("bytes={}-{}", segments[2].start, segments[2].end),
        ];
        expected_ranges.sort();
        assert_eq!(ranges, expected_ranges);
        assert_eq!(std::fs::read(&dest).unwrap(), expected_mp4);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn synthetic_app_restart_with_server_expired_403_url_fails_once_and_does_not_retry_indefinitely()
     {
        let (url, server) = spawn_synthetic_http_server(1, move |_, _| SyntheticHttpResponse {
            status_line: "403 Forbidden",
            headers: vec![("Content-Type".into(), "text/plain".into())],
            content_length_override: None,
            body: b"Signed URL expired".to_vec(),
            delay_before_body_ms: 0,
        });

        let dir = create_test_temp_dir("auto-resume-expired-403");
        let history_path = dir.join("download-history.json");
        let dest = dir.join("expired_on_restart.mp4");
        let persisted = PersistedDownloads {
            version: 1,
            max_concurrent: 2,
            max_connections_per_download: 4,
            bandwidth_limit_bytes_per_second: None,
            notifications_enabled: false,
            notify_on_complete: false,
            notify_on_failure: false,
            entries: vec![DownloadEntry {
                id: "expired-403-restart".into(),
                url,
                destination: dest.to_string_lossy().into_owned(),
                filename: "expired_on_restart.mp4".into(),
                status: DownloadStatus::Downloading,
                total_bytes: Some(212),
                downloaded_bytes: 0,
                speed_bytes_per_second: 0.0,
                elapsed_ms: 10,
                remaining_seconds: None,
                error: None,
                created_at_ms: 100,
                updated_at_ms: 110,
                attempts: 1,
                hls_playlist: None,
                hls_completed_parts: None,
                website_media: None,
                segments: Vec::new(),
                etag: None,
                last_modified: None,
                notifications_enabled: false,
                notify_on_complete: false,
                notify_on_failure: false,
            }],
        };
        std::fs::write(&history_path, serde_json::to_vec(&persisted).unwrap()).unwrap();

        let manager = DownloadManager::default();
        manager
            .initialize_with_history_path(None, history_path.clone())
            .unwrap();
        let settled = wait_for_download_settlement(&manager, "expired-403-restart");
        let ranges = server.stop_and_join();

        assert_eq!(ranges.len(), 1);
        assert_eq!(settled.status, DownloadStatus::Failed);
        assert!(
            settled
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("403 Forbidden")
        );
        assert!(!dest.exists());

        // Second app restart with the persisted Failed status must NOT re-trigger any download worker!
        let second_manager = DownloadManager::default();
        second_manager
            .initialize_with_history_path(None, history_path)
            .unwrap();
        assert_eq!(second_manager.worker_count.load(Ordering::Acquire), 0);
        let second_snap = second_manager.snapshot().unwrap();
        assert_eq!(second_snap[0].status, DownloadStatus::Failed);

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn startup_auto_resume_handles_website_entries_existing_destinations_and_retry_cap_safely() {
        let dir = create_test_temp_dir("startup-edge-cases");
        let history_path = dir.join("download-history.json");

        let pre_existing_dest = dir.join("already_exists.mp4");
        let original_bytes = b"DO_NOT_OVERWRITE_EXISTING_DESTINATION".to_vec();
        std::fs::write(&pre_existing_dest, &original_bytes).unwrap();

        let persisted = PersistedDownloads {
            version: 1,
            max_concurrent: 2,
            max_connections_per_download: 4,
            bandwidth_limit_bytes_per_second: None,
            notifications_enabled: false,
            notify_on_complete: false,
            notify_on_failure: false,
            entries: vec![
                DownloadEntry {
                    id: "website-interrupted".into(),
                    url: "https://example.com/watch?v=123".into(),
                    destination: dir.join("website.mp4").to_string_lossy().into_owned(),
                    filename: "website.mp4".into(),
                    status: DownloadStatus::Downloading,
                    total_bytes: Some(1000),
                    downloaded_bytes: 200,
                    speed_bytes_per_second: 50.0,
                    elapsed_ms: 100,
                    remaining_seconds: Some(10.0),
                    error: None,
                    created_at_ms: 100,
                    updated_at_ms: 200,
                    attempts: 1,
                    hls_playlist: None,
                    hls_completed_parts: None,
                    website_media: Some(WebsiteMediaInfo {
                        format_id: "18".into(),
                        audio_format_id: None,
                        container: "mp4".into(),
                        height: Some(360),
                    }),
                    segments: Vec::new(),
                    etag: None,
                    last_modified: None,
                    notifications_enabled: false,
                    notify_on_complete: false,
                    notify_on_failure: false,
                },
                DownloadEntry {
                    id: "existing-dest-conflict".into(),
                    url: "http://127.0.0.1:9/conflict.mp4".into(),
                    destination: pre_existing_dest.to_string_lossy().into_owned(),
                    filename: "already_exists.mp4".into(),
                    status: DownloadStatus::Downloading,
                    total_bytes: Some(212),
                    downloaded_bytes: 50,
                    speed_bytes_per_second: 0.0,
                    elapsed_ms: 10,
                    remaining_seconds: None,
                    error: None,
                    created_at_ms: 100,
                    updated_at_ms: 110,
                    attempts: 1,
                    hls_playlist: None,
                    hls_completed_parts: None,
                    website_media: None,
                    segments: Vec::new(),
                    etag: None,
                    last_modified: None,
                    notifications_enabled: false,
                    notify_on_complete: false,
                    notify_on_failure: false,
                },
                DownloadEntry {
                    id: "retry-cap-reached".into(),
                    url: "http://127.0.0.1:9/capped.mp4".into(),
                    destination: dir.join("capped.mp4").to_string_lossy().into_owned(),
                    filename: "capped.mp4".into(),
                    status: DownloadStatus::Queued,
                    total_bytes: Some(212),
                    downloaded_bytes: 20,
                    speed_bytes_per_second: 0.0,
                    elapsed_ms: 50,
                    remaining_seconds: None,
                    error: None,
                    created_at_ms: 100,
                    updated_at_ms: 150,
                    attempts: MAX_AUTO_RESUME_ATTEMPTS,
                    hls_playlist: None,
                    hls_completed_parts: None,
                    website_media: None,
                    segments: Vec::new(),
                    etag: None,
                    last_modified: None,
                    notifications_enabled: false,
                    notify_on_complete: false,
                    notify_on_failure: false,
                },
                DownloadEntry {
                    id: "already-failed".into(),
                    url: "http://127.0.0.1:9/failed.mp4".into(),
                    destination: dir.join("failed.mp4").to_string_lossy().into_owned(),
                    filename: "failed.mp4".into(),
                    status: DownloadStatus::Failed,
                    total_bytes: Some(212),
                    downloaded_bytes: 0,
                    speed_bytes_per_second: 0.0,
                    elapsed_ms: 10,
                    remaining_seconds: None,
                    error: Some("Previous permanent failure.".into()),
                    created_at_ms: 100,
                    updated_at_ms: 110,
                    attempts: 1,
                    hls_playlist: None,
                    hls_completed_parts: None,
                    website_media: None,
                    segments: Vec::new(),
                    etag: None,
                    last_modified: None,
                    notifications_enabled: false,
                    notify_on_complete: false,
                    notify_on_failure: false,
                },
            ],
        };
        std::fs::write(&history_path, serde_json::to_vec(&persisted).unwrap()).unwrap();

        let manager = DownloadManager::default();
        manager
            .initialize_with_history_path(None, history_path)
            .unwrap();
        assert_eq!(manager.worker_count.load(Ordering::Acquire), 0);

        let snap = manager.snapshot().unwrap();
        let website = snap.iter().find(|e| e.id == "website-interrupted").unwrap();
        assert_eq!(website.status, DownloadStatus::Failed);
        assert!(
            website
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("Start a new website download")
        );

        let conflict = snap
            .iter()
            .find(|e| e.id == "existing-dest-conflict")
            .unwrap();
        assert_eq!(conflict.status, DownloadStatus::Failed);
        assert!(
            conflict
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("already exists")
        );
        assert_eq!(std::fs::read(&pre_existing_dest).unwrap(), original_bytes);

        let capped = snap.iter().find(|e| e.id == "retry-cap-reached").unwrap();
        assert_eq!(capped.status, DownloadStatus::Failed);
        assert!(
            capped
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("Automatic resume limit reached")
        );

        let failed = snap.iter().find(|e| e.id == "already-failed").unwrap();
        assert_eq!(failed.status, DownloadStatus::Failed);
        assert_eq!(
            failed.error.as_deref(),
            Some("Previous permanent failure.")
        );

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn synthetic_parallel_pause_resume_and_cancel_wake_active_and_semaphore_waiting_segments() {
        let mp4 = valid_mp4(false);
        let expected_mp4 = mp4.clone();

        // 1. Pause while 2 segments hold semaphore permits (delayed 250ms) and 2 segments wait on semaphore,
        // then resume to completion.
        let (url1, _max_conc1, _obs1, server1) =
            spawn_concurrent_range_http_server(mp4.clone(), 9, 250, false);
        let dir = create_test_temp_dir("parallel-pause-resume-cancel");
        let dest1 = dir.join("pause_resume_parallel.mp4");
        let manager = DownloadManager::default();
        manager.configure_connections_per_download(2).unwrap();

        let entry1 = manager
            .start_download_internal(None, url1, dest1.to_string_lossy().into_owned(), None)
            .unwrap();

        // Wait until the parallel segments are planned and the download is active
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let snap = manager.snapshot().unwrap();
            let e = snap.iter().find(|x| x.id == entry1.id).unwrap();
            if e.status == DownloadStatus::Downloading && e.segments.len() == 2 {
                break;
            }
            assert!(Instant::now() < deadline, "did not enter parallel downloading");
            std::thread::sleep(Duration::from_millis(10));
        }

        manager.pause_download(&entry1.id).unwrap();
        let paused = wait_for_download_settlement(&manager, &entry1.id);
        assert_eq!(paused.status, DownloadStatus::Paused);
        assert!(!paused.segments.is_empty());

        manager.resume_download(None, &entry1.id).unwrap();
        let completed = wait_for_download_settlement(&manager, &entry1.id);
        server1.stop_and_join();
        assert_eq!(
            completed.status,
            DownloadStatus::Completed,
            "resume failed: {:?}",
            completed.error
        );
        assert_eq!(std::fs::read(&dest1).unwrap(), expected_mp4);

        // 2. Cancel while 2 segments are active (delayed 300ms) and 2 segments are waiting on the semaphore (4 segments total, connection limit 2)
        let (url2, _max_conc2, _obs2, server2) =
            spawn_concurrent_range_http_server(mp4.clone(), 6, 300, false);
        let dest2 = dir.join("cancel_parallel.mp4");
        let mut preset_segments = plan_parallel_segments(mp4.len() as u64, 4);
        assert_eq!(preset_segments.len(), 4);
        let cancel_id = "cancel-4-seg-with-2-permits".to_owned();
        let control = Arc::new(DownloadTaskControl::default());
        {
            let mut state = manager.state.lock().unwrap();
            state.max_connections_per_download = 2;
            state.entries.push(DownloadEntry {
                id: cancel_id.clone(),
                url: url2,
                destination: dest2.to_string_lossy().into_owned(),
                filename: "cancel_parallel.mp4".into(),
                status: DownloadStatus::Queued,
                total_bytes: Some(mp4.len() as u64),
                downloaded_bytes: 0,
                speed_bytes_per_second: 0.0,
                elapsed_ms: 0,
                remaining_seconds: None,
                error: None,
                created_at_ms: 100,
                updated_at_ms: 100,
                attempts: 1,
                hls_playlist: None,
                hls_completed_parts: None,
                website_media: None,
                segments: preset_segments.clone(),
                etag: Some("\"parallel-v1\"".into()),
                last_modified: None,
                notifications_enabled: false,
                notify_on_complete: false,
                notify_on_failure: false,
            });
            state
                .controls
                .insert(cancel_id.clone(), Arc::clone(&control));
        }
        spawn_worker(
            None,
            Arc::clone(&manager.state),
            Arc::clone(&manager.concurrency),
            Arc::clone(&manager.bandwidth),
            Arc::clone(&manager.worker_count),
            cancel_id.clone(),
            control,
            false,
        );

        std::thread::sleep(Duration::from_millis(40));
        manager.cancel_download(None, &cancel_id).unwrap();
        let cancelled = wait_for_download_settlement(&manager, &cancel_id);
        server2.stop_and_join();

        assert_eq!(cancelled.status, DownloadStatus::Cancelled);
        assert!(cancelled.segments.is_empty());
        assert!(!dest2.exists());
        assert!(!partial_path(&dest2).exists());
        for seg in &mut preset_segments {
            assert!(!segment_part_path(&dest2, &cancel_id, seg.index).exists());
        }

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn synthetic_parallel_segment_retries_from_exact_offset_and_rejects_changed_validator() {
        let mp4 = valid_mp4(false);
        let total = mp4.len();
        let expected_mp4 = mp4.clone();
        let segments = plan_parallel_segments(total as u64, 2);
        assert_eq!(segments.len(), 2);
        let s0_end = segments[0].end as usize;
        let s1_start = segments[1].start as usize;
        let s1_end = segments[1].end as usize;
        let s1_partial_len = 18_usize;

        // Request 0:Segment 0 full range (0..=s0_end)
        // Request 1: Segment 1 range (s1_start..=s1_end), but server truncates body after 18 bytes while declaring full Content-Length!
        // Request 2: Segment 1 retry range ((s1_start + 18)..=s1_end) -> returns remaining bytes!
        let mp4_for_retry = mp4.clone();
        let (url1, server1) = spawn_synthetic_http_server(3, move |req_idx, range_hdr| {
            let range_str = range_hdr.unwrap_or_default();
            if range_str == format!("bytes=0-{s0_end}") {
                SyntheticHttpResponse::partial_mp4(
                    0,
                    s0_end,
                    total,
                    mp4_for_retry[0..=s0_end].to_vec(),
                )
            } else if range_str == format!("bytes={s1_start}-{s1_end}") && req_idx <= 1 {
                let mut r = SyntheticHttpResponse::partial_mp4(
                    s1_start,
                    s1_end,
                    total,
                    mp4_for_retry[s1_start..s1_start + s1_partial_len].to_vec(),
                );
                r.content_length_override = Some(s1_end - s1_start + 1);
                r
            } else {
                let expected_retry_start = s1_start + s1_partial_len;
                assert_eq!(
                    range_str,
                    format!("bytes={expected_retry_start}-{s1_end}"),
                    "segment retry must request from exact already-written offset"
                );
                SyntheticHttpResponse::partial_mp4(
                    expected_retry_start,
                    s1_end,
                    total,
                    mp4_for_retry[expected_retry_start..=s1_end].to_vec(),
                )
            }
        });

        let dir = create_test_temp_dir("parallel-retry-and-validator");
        let dest1 = dir.join("retry_offset.mp4");
        let history_path = dir.join("download-history.json");

        // Also test validator mismatch on a second segmented entry
        let mp4_for_val = mp4.clone();
        let (url2, server2) = spawn_synthetic_http_server(2, move |_, range_hdr| {
            let range_str = range_hdr.unwrap_or_default();
            if range_str == format!("bytes=0-{s0_end}") {
                SyntheticHttpResponse::partial_mp4(
                    0,
                    s0_end,
                    total,
                    mp4_for_val[0..=s0_end].to_vec(),
                )
            } else {
                let mut r = SyntheticHttpResponse::partial_mp4(
                    s1_start,
                    s1_end,
                    total,
                    mp4_for_val[s1_start..=s1_end].to_vec(),
                );
                r.headers = vec![
                    ("Content-Type".into(), "video/mp4".into()),
                    (
                        "Content-Range".into(),
                        format!("bytes {s1_start}-{s1_end}/{total}"),
                    ),
                    ("ETag".into(), "\"changed-etag-v2\"".into()),
                ];
                r
            }
        });
        let dest2 = dir.join("validator_changed.mp4");

        let persisted = PersistedDownloads {
            version: 1,
            max_concurrent: 2,
            max_connections_per_download: 1, // serialize requests to our deterministic single-thread server
            bandwidth_limit_bytes_per_second: None,
            notifications_enabled: false,
            notify_on_complete: false,
            notify_on_failure: false,
            entries: vec![
                DownloadEntry {
                    id: "seg-exact-retry".into(),
                    url: url1,
                    destination: dest1.to_string_lossy().into_owned(),
                    filename: "retry_offset.mp4".into(),
                    status: DownloadStatus::Downloading,
                    total_bytes: Some(total as u64),
                    downloaded_bytes: 0,
                    speed_bytes_per_second: 0.0,
                    elapsed_ms: 0,
                    remaining_seconds: None,
                    error: None,
                    created_at_ms: 100,
                    updated_at_ms: 100,
                    attempts: 1,
                    hls_playlist: None,
                    hls_completed_parts: None,
                    website_media: None,
                    segments: segments.clone(),
                    etag: Some("\"synthetic-v1\"".into()),
                    last_modified: None,
                    notifications_enabled: false,
                    notify_on_complete: false,
                    notify_on_failure: false,
                },
                DownloadEntry {
                    id: "seg-validator-mismatch".into(),
                    url: url2,
                    destination: dest2.to_string_lossy().into_owned(),
                    filename: "validator_changed.mp4".into(),
                    status: DownloadStatus::Downloading,
                    total_bytes: Some(total as u64),
                    downloaded_bytes: 0,
                    speed_bytes_per_second: 0.0,
                    elapsed_ms: 0,
                    remaining_seconds: None,
                    error: None,
                    created_at_ms: 100,
                    updated_at_ms: 100,
                    attempts: 1,
                    hls_playlist: None,
                    hls_completed_parts: None,
                    website_media: None,
                    segments,
                    etag: Some("\"synthetic-v1\"".into()),
                    last_modified: None,
                    notifications_enabled: false,
                    notify_on_complete: false,
                    notify_on_failure: false,
                },
            ],
        };
        std::fs::write(&history_path, serde_json::to_vec(&persisted).unwrap()).unwrap();

        let manager = DownloadManager::default();
        manager
            .initialize_with_history_path(None, history_path)
            .unwrap();

        let settled1 = wait_for_download_settlement(&manager, "seg-exact-retry");
        let settled2 = wait_for_download_settlement(&manager, "seg-validator-mismatch");
        let ranges1 = server1.stop_and_join();
        let _ranges2 = server2.stop_and_join();

        assert_eq!(
            settled1.status,
            DownloadStatus::Completed,
            "retry download failed: {:?}",
            settled1.error
        );
        assert_eq!(
            ranges1,
            vec![
                Some(format!("bytes=0-{s0_end}")),
                Some(format!("bytes={s1_start}-{s1_end}")),
                Some(format!("bytes={}-{s1_end}", s1_start + s1_partial_len)),
            ]
        );
        assert_eq!(std::fs::read(&dest1).unwrap(), expected_mp4);

        assert_eq!(settled2.status, DownloadStatus::Failed);
        assert!(
            settled2
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("file validator changed")
        );
        assert!(!dest2.exists());

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn synthetic_clean_shutdown_checkpoints_active_and_paused_downloads_for_automatic_startup_resume()
     {
        let mp4 = valid_mp4(false);
        let expected_mp4 = mp4.clone();

        // Server holds range responses for 300ms so cancel_all_downloads catches the active parallel download mid-flight,
        // and then serves the 4 range requests when the new DownloadManager instance starts up!
        let (url, _max_conc, _obs, server) =
            spawn_concurrent_range_http_server(mp4.clone(), 9, 200, false);
        let dir = create_test_temp_dir("clean-shutdown-auto-resume");
        let history_path = dir.join("download-history.json");
        let active_dest = dir.join("active_on_exit.mp4");
        let paused_dest = dir.join("paused_on_exit.mp4");
        std::fs::write(partial_path(&paused_dest), &mp4[0..32]).unwrap();

        let manager1 = DownloadManager::default();
        manager1
            .initialize_with_history_path(None, history_path.clone())
            .unwrap();
        {
            let mut state = manager1.state.lock().unwrap();
            state.entries.push(DownloadEntry {
                id: "stay-paused-across-exit".into(),
                url: "http://127.0.0.1:9/paused.mp4".into(),
                destination: paused_dest.to_string_lossy().into_owned(),
                filename: "paused_on_exit.mp4".into(),
                status: DownloadStatus::Paused,
                total_bytes: Some(mp4.len() as u64),
                downloaded_bytes: 32,
                speed_bytes_per_second: 0.0,
                elapsed_ms: 25,
                remaining_seconds: None,
                error: None,
                created_at_ms: 100,
                updated_at_ms: 125,
                attempts: 1,
                hls_playlist: None,
                hls_completed_parts: None,
                website_media: None,
                segments: Vec::new(),
                etag: Some("\"parallel-v1\"".into()),
                last_modified: None,
                notifications_enabled: false,
                notify_on_complete: false,
                notify_on_failure: false,
            });
            persist_locked(&state).unwrap();
        }

        let active_entry = manager1
            .start_download_internal(None, url, active_dest.to_string_lossy().into_owned(), None)
            .unwrap();

        // Wait until parallel segments are planned and active
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let snap = manager1.snapshot().unwrap();
            let e = snap.iter().find(|x| x.id == active_entry.id).unwrap();
            if e.status == DownloadStatus::Downloading && e.segments.len() == 4 {
                break;
            }
            assert!(Instant::now() < deadline, "did not enter parallel download");
            std::thread::sleep(Duration::from_millis(10));
        }

        // Simulate clean application exit (`request_exit` -> `cancel_all_downloads`)
        let (workers, count) = cancel_all_downloads(&manager1).unwrap();
        assert_eq!(count, 1);
        while workers.load(Ordering::Acquire) > 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(workers.load(Ordering::Acquire), 0);

        // Verify persisted history kept active download in Downloading and paused download in Paused
        let manager2 = DownloadManager::default();
        manager2
            .initialize_with_history_path(None, history_path)
            .unwrap();

        let settled = wait_for_download_settlement(&manager2, &active_entry.id);
        server.stop_and_join();

        assert_eq!(
            settled.status,
            DownloadStatus::Completed,
            "auto-resume after clean shutdown failed: {:?}",
            settled.error
        );
        assert_eq!(std::fs::read(&active_dest).unwrap(), expected_mp4);

        let snap2 = manager2.snapshot().unwrap();
        let paused_after = snap2
            .iter()
            .find(|x| x.id == "stay-paused-across-exit")
            .unwrap();
        assert_eq!(paused_after.status, DownloadStatus::Paused);
        assert_eq!(paused_after.downloaded_bytes, 32);

        std::fs::remove_dir_all(dir).unwrap();
    }

    fn build_synthetic_hls_fmp4_parts() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let mut init = mp4_atom(b"ftyp", b"isom\0\0\0\0isom");
        init.extend(valid_mp4_initialization(true));

        let make_fragment = |marker: u8| {
            let mut track_fragment = mp4_atom(b"tfhd", &[0; 8]);
            track_fragment.extend(mp4_atom(b"trun", &[0; 8]));
            let mut movie_fragment = mp4_atom(b"mfhd", &[0; 8]);
            movie_fragment.extend(mp4_atom(b"traf", &track_fragment));
            let mut frag = mp4_atom(b"moof", &movie_fragment);
            frag.extend(mp4_atom(b"mdat", &[marker; 24]));
            frag
        };

        (init, make_fragment(0x11), make_fragment(0x22))
    }

    #[test]
    fn synthetic_hls_clean_shutdown_checkpoints_parts_and_resumes_automatically_on_restart() {
        let (init, seg1, seg2) = build_synthetic_hls_fmp4_parts();
        let mut expected_mp4 = Vec::new();
        expected_mp4.extend_from_slice(&init);
        expected_mp4.extend_from_slice(&seg1);
        expected_mp4.extend_from_slice(&seg2);

        let init_len = init.len();
        let seg1_len = seg1.len();
        let seg2_len = seg2.len();
        let init_for_srv = init.clone();
        let seg1_for_srv = seg1.clone();
        let seg2_for_srv = seg2.clone();

        // Request 0: init.mp4 (fast)
        // Request 1: seg1.m4s (fast)
        // Request 2: seg2.m4s (delayed 350ms so clean shutdown catches HLS mid-segment)
        // Request 3: seg2.m4s (fast, requested on startup automatic resume!)
        let (base_url, server) = spawn_synthetic_http_server(4, move |idx, _| match idx {
            0 => SyntheticHttpResponse::ok_mp4(init_for_srv.clone()),
            1 => SyntheticHttpResponse::ok_mp4(seg1_for_srv.clone()),
            2 => {
                let mut r = SyntheticHttpResponse::ok_mp4(seg2_for_srv.clone());
                r.delay_before_body_ms = 350;
                r
            }
            _ => SyntheticHttpResponse::ok_mp4(seg2_for_srv.clone()),
        });
        let origin = base_url.trim_end_matches("/video.mp4");
        let manifest_url = format!("{origin}/playlist.m3u8");
        let playlist = HlsPlaylist {
            version: 1,
            container: "fmp4".into(),
            manifest_size_bytes: 256,
            init: HlsResource {
                url: format!("{origin}/init.mp4"),
                byte_range: None,
            },
            segments: vec![
                HlsResource {
                    url: format!("{origin}/seg1.m4s"),
                    byte_range: None,
                },
                HlsResource {
                    url: format!("{origin}/seg2.m4s"),
                    byte_range: None,
                },
            ],
            expected_bytes: None,
        };

        let dir = create_test_temp_dir("hls-clean-shutdown-resume");
        let history_path = dir.join("download-history.json");
        let dest = dir.join("hls_resume.mp4");

        let manager1 = DownloadManager::default();
        manager1
            .initialize_with_history_path(None, history_path.clone())
            .unwrap();
        let entry = manager1
            .start_download_internal(
                None,
                manifest_url,
                dest.to_string_lossy().into_owned(),
                Some(playlist),
            )
            .unwrap();

        // Wait until init + seg1 (2 parts) are committed and seg2 is in-flight
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let snap = manager1.snapshot().unwrap();
            let current = snap.iter().find(|e| e.id == entry.id).unwrap();
            if current.hls_completed_parts == Some(2) {
                break;
            }
            assert!(
                !current.status.is_terminal() && Instant::now() < deadline,
                "HLS download did not reach 2 committed parts; state: {current:?}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }

        // Trigger clean application shutdown
        let (workers, active_count) = cancel_all_downloads(&manager1).unwrap();
        assert_eq!(active_count, 1);
        while workers.load(Ordering::Acquire) > 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            workers.load(Ordering::Acquire),
            0,
            "HLS worker must exit promptly on clean shutdown"
        );

        let snap_after_shutdown = manager1.snapshot().unwrap();
        let checkpointed = snap_after_shutdown
            .iter()
            .find(|e| e.id == entry.id)
            .unwrap();
        assert_eq!(checkpointed.status, DownloadStatus::Downloading);
        assert_eq!(checkpointed.hls_completed_parts, Some(2));
        assert_eq!(
            checkpointed.downloaded_bytes,
            (init_len + seg1_len) as u64
        );
        assert!(
            !hls_staging_path(&dest, &entry.id).exists(),
            "incomplete HLS temporary staging file must be cleaned up on shutdown"
        );
        assert_eq!(
            std::fs::metadata(partial_path(&dest)).unwrap().len(),
            (init_len + seg1_len) as u64
        );
        assert!(!dest.exists());

        // Reload history in a fresh DownloadManager and verify automatic resume completes HLS
        let manager2 = DownloadManager::default();
        manager2
            .initialize_with_history_path(None, history_path)
            .unwrap();
        let settled = wait_for_download_settlement(&manager2, &entry.id);
        let observed_requests = server.stop_and_join();

        assert_eq!(
            settled.status,
            DownloadStatus::Completed,
            "HLS auto-resume failed: {:?}",
            settled.error
        );
        assert_eq!(settled.hls_completed_parts, Some(3));
        assert_eq!(
            settled.downloaded_bytes,
            (init_len + seg1_len + seg2_len) as u64
        );
        assert_eq!(observed_requests.len(), 4);
        assert_eq!(std::fs::read(&dest).unwrap(), expected_mp4);
        assert!(!partial_path(&dest).exists());
        assert!(!hls_staging_path(&dest, &entry.id).exists());

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn synthetic_cancel_vs_completion_race_across_parallel_sequential_and_hls_workers() {
        struct CommitHookGuard;
        impl Drop for CommitHookGuard {
            fn drop(&mut self) {
                if let Ok(mut guard) = TEST_COMMIT_HOOK.lock() {
                    *guard = None;
                }
            }
        }
        let _hook_guard = CommitHookGuard;

        let dir = create_test_temp_dir("cancel-vs-completion-races");
        let mp4 = valid_mp4(false);

        // 1. Parallel worker: cancel immediately after MP4 validation AND immediately after fs::rename
        for trigger_stage in ["parallel_after_validation", "parallel_after_rename"] {
            {
                let mut guard = TEST_COMMIT_HOOK.lock().unwrap();
                let stage_target = trigger_stage.to_owned();
                *guard = Some(Box::new(move |stage, _id, ctrl| {
                    if stage == stage_target {
                        ctrl.request_cancel();
                    }
                }));
            }
            let (url, _max_c, _obs, server) =
                spawn_concurrent_range_http_server(mp4.clone(), 5, 0, false);
            let dest = dir.join(format!("{trigger_stage}.mp4"));
            let manager = DownloadManager::default();
            let entry = manager
                .start_download_internal(None, url, dest.to_string_lossy().into_owned(), None)
                .unwrap();
            let settled = wait_for_download_settlement(&manager, &entry.id);
            server.stop_and_join();

            assert_eq!(
                settled.status,
                DownloadStatus::Cancelled,
                "parallel {trigger_stage} must end Cancelled, not Completed"
            );
            assert!(
                !dest.exists(),
                "parallel {trigger_stage} must not leave newly created destination file"
            );
            assert!(!partial_path(&dest).exists());
        }

        // 2. Sequential worker: cancel immediately after MP4 validation AND immediately after fs::rename
        for trigger_stage in ["sequential_after_validation", "sequential_after_rename"] {
            {
                let mut guard = TEST_COMMIT_HOOK.lock().unwrap();
                let stage_target = trigger_stage.to_owned();
                *guard = Some(Box::new(move |stage, _id, ctrl| {
                    if stage == stage_target {
                        ctrl.request_cancel();
                    }
                }));
            }
            let mp4_srv = mp4.clone();
            let (url, server) = spawn_synthetic_http_server(1, move |_, _| {
                SyntheticHttpResponse::ok_mp4(mp4_srv.clone())
            });
            let dest = dir.join(format!("{trigger_stage}.mp4"));
            let manager = DownloadManager::default();
            let entry = manager
                .start_download_internal(None, url, dest.to_string_lossy().into_owned(), None)
                .unwrap();
            let settled = wait_for_download_settlement(&manager, &entry.id);
            server.stop_and_join();

            assert_eq!(
                settled.status,
                DownloadStatus::Cancelled,
                "sequential {trigger_stage} must end Cancelled, not Completed"
            );
            assert!(
                !dest.exists(),
                "sequential {trigger_stage} must not leave newly created destination file"
            );
        }

        // 3. HLS worker: cancel immediately after MP4 validation AND immediately after fs::rename
        let (init, seg1, seg2) = build_synthetic_hls_fmp4_parts();
        for trigger_stage in ["hls_after_validation", "hls_after_rename"] {
            {
                let mut guard = TEST_COMMIT_HOOK.lock().unwrap();
                let stage_target = trigger_stage.to_owned();
                *guard = Some(Box::new(move |stage, _id, ctrl| {
                    if stage == stage_target {
                        ctrl.request_cancel();
                    }
                }));
            }
            let i_srv = init.clone();
            let s1_srv = seg1.clone();
            let s2_srv = seg2.clone();
            let (base_url, server) = spawn_synthetic_http_server(3, move |idx, _| match idx {
                0 => SyntheticHttpResponse::ok_mp4(i_srv.clone()),
                1 => SyntheticHttpResponse::ok_mp4(s1_srv.clone()),
                _ => SyntheticHttpResponse::ok_mp4(s2_srv.clone()),
            });
            let origin = base_url.trim_end_matches("/video.mp4");
            let playlist = HlsPlaylist {
                version: 1,
                container: "fmp4".into(),
                manifest_size_bytes: 256,
                init: HlsResource {
                    url: format!("{origin}/init.mp4"),
                    byte_range: None,
                },
                segments: vec![
                    HlsResource {
                        url: format!("{origin}/seg1.m4s"),
                        byte_range: None,
                    },
                    HlsResource {
                        url: format!("{origin}/seg2.m4s"),
                        byte_range: None,
                    },
                ],
                expected_bytes: None,
            };
            let dest = dir.join(format!("{trigger_stage}.mp4"));
            let manager = DownloadManager::default();
            let entry = manager
                .start_download_internal(
                    None,
                    format!("{origin}/playlist.m3u8"),
                    dest.to_string_lossy().into_owned(),
                    Some(playlist),
                )
                .unwrap();
            let settled = wait_for_download_settlement(&manager, &entry.id);
            server.stop_and_join();

            assert_eq!(
                settled.status,
                DownloadStatus::Cancelled,
                "HLS {trigger_stage} must end Cancelled, not Completed"
            );
            assert!(
                !dest.exists(),
                "HLS {trigger_stage} must not leave newly created destination file"
            );
        }

        // 4. Verify pre-existing destination appearing before rename is NEVER overwritten or deleted
        let existing_dest = dir.join("preexisting_protected.mp4");
        let existing_bytes = b"UNRELATED_EXISTING_USER_FILE".to_vec();
        {
            let existing_path_for_hook = existing_dest.clone();
            let existing_bytes_for_hook = existing_bytes.clone();
            let mut guard = TEST_COMMIT_HOOK.lock().unwrap();
            *guard = Some(Box::new(move |stage, _id, _ctrl| {
                if stage == "hls_after_validation" {
                    let _ = std::fs::write(&existing_path_for_hook, &existing_bytes_for_hook);
                }
            }));
        }
        let i_srv = init.clone();
        let s1_srv = seg1.clone();
        let s2_srv = seg2.clone();
        let (base_url, server) = spawn_synthetic_http_server(3, move |idx, _| match idx {
            0 => SyntheticHttpResponse::ok_mp4(i_srv.clone()),
            1 => SyntheticHttpResponse::ok_mp4(s1_srv.clone()),
            _ => SyntheticHttpResponse::ok_mp4(s2_srv.clone()),
        });
        let origin = base_url.trim_end_matches("/video.mp4");
        let playlist = HlsPlaylist {
            version: 1,
            container: "fmp4".into(),
            manifest_size_bytes: 256,
            init: HlsResource {
                url: format!("{origin}/init.mp4"),
                byte_range: None,
            },
            segments: vec![
                HlsResource {
                    url: format!("{origin}/seg1.m4s"),
                    byte_range: None,
                },
                HlsResource {
                    url: format!("{origin}/seg2.m4s"),
                    byte_range: None,
                },
            ],
            expected_bytes: None,
        };
        let manager = DownloadManager::default();
        let entry = manager
            .start_download_internal(
                None,
                format!("{origin}/playlist.m3u8"),
                existing_dest.to_string_lossy().into_owned(),
                Some(playlist),
            )
            .unwrap();
        let settled = wait_for_download_settlement(&manager, &entry.id);
        server.stop_and_join();

        assert_eq!(settled.status, DownloadStatus::Failed);
        assert!(
            settled
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("appeared during the HLS download")
        );
        assert_eq!(
            std::fs::read(&existing_dest).unwrap(),
            existing_bytes,
            "pre-existing destination file must never be overwritten or deleted"
        );

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn synthetic_retry_emits_queued_immediately_when_concurrency_slots_are_full_and_completes_on_release()
     {
        let mp4 = valid_mp4(false);
        let expected_mp4 = mp4.clone();
        let (url, server) = spawn_synthetic_http_server(1, move |_, _| {
            SyntheticHttpResponse::ok_mp4(mp4.clone())
        });
        let dir = create_test_temp_dir("retry-full-slots");
        let dest = dir.join("retried_queued.mp4");
        let manager = DownloadManager::default();
        let retry_id = "failed-entry-for-full-slot-retry".to_owned();

        // Occupy the only concurrency slot (`max_concurrent = 1`)
        let hold_control = Arc::new(DownloadTaskControl::default());
        {
            let mut state = manager.state.lock().unwrap();
            state.max_concurrent = 1;
            state.entries.push(DownloadEntry {
                id: retry_id.clone(),
                url,
                destination: dest.to_string_lossy().into_owned(),
                filename: "retried_queued.mp4".into(),
                status: DownloadStatus::Failed,
                total_bytes: None,
                downloaded_bytes: 0,
                speed_bytes_per_second: 0.0,
                elapsed_ms: 10,
                remaining_seconds: None,
                error: Some("Transient network failure".into()),
                created_at_ms: 100,
                updated_at_ms: 110,
                attempts: 1,
                hls_playlist: None,
                hls_completed_parts: None,
                website_media: None,
                segments: Vec::new(),
                etag: None,
                last_modified: None,
                notifications_enabled: false,
                notify_on_complete: false,
                notify_on_failure: false,
            });
        }
        let held_slot = tauri::async_runtime::block_on(async {
            manager
                .concurrency
                .acquire(&manager.state, &hold_control)
                .await
                .unwrap()
                .expect("should occupy the only concurrency slot")
        });

        TEST_EMITTED_EVENTS.lock().unwrap().clear();
        manager
            .retry_download_internal(None, retry_id.clone(), false)
            .unwrap();

        // Verify Queued event was emitted synchronously before worker could execute
        let emitted_immediately = TEST_EMITTED_EVENTS
            .lock()
            .unwrap()
            .iter()
            .filter(|(id, _, _)| id == &retry_id)
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            emitted_immediately,
            vec![(retry_id.clone(), DownloadStatus::Queued, 2)],
            "retry_download_internal must emit Queued with incremented attempts before worker execution"
        );

        // Confirm entry remains Queued while concurrency slot is held
        std::thread::sleep(Duration::from_millis(40));
        let snap_while_full = manager.snapshot().unwrap();
        let queued_entry = snap_while_full.iter().find(|e| e.id == retry_id).unwrap();
        assert_eq!(queued_entry.status, DownloadStatus::Queued);
        assert_eq!(queued_entry.attempts, 2);
        assert!(queued_entry.error.is_none());

        // Release slot and verify the retry proceeds to Completed
        drop(held_slot);
        let settled = wait_for_download_settlement(&manager, &retry_id);
        server.stop_and_join();

        assert_eq!(
            settled.status,
            DownloadStatus::Completed,
            "retry failed after slot release: {:?}",
            settled.error
        );
        assert_eq!(settled.attempts, 2);
        assert_eq!(std::fs::read(&dest).unwrap(), expected_mp4);

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn synthetic_pause_at_zero_bytes_resumes_and_completes_with_exact_size() {
        let mp4 = valid_mp4(false);
        let expected_mp4 = mp4.clone();
        let (url, server) = spawn_synthetic_http_server(2, move |idx, _| {
            let mut r = SyntheticHttpResponse::ok_mp4(mp4.clone());
            if idx == 0 {
                // Delay body bytes so pause occurs while status is Downloading with 0 bytes
                // written and a 0-byte .fluxnet-part already created on disk.
                r.delay_before_body_ms = 600;
            }
            r
        });
        let dir = create_test_temp_dir("pause-zero-bytes");
        let dest = dir.join("paused_at_zero.mp4");
        let part = partial_path(&dest);
        let manager = DownloadManager::default();
        let entry = manager
            .start_download_internal(None, url, dest.to_string_lossy().into_owned(), None)
            .unwrap();

        // Wait until the worker has created the 0-byte .fluxnet-part and entered Downloading.
        let wait_start = Instant::now();
        loop {
            let snap = manager.snapshot().unwrap();
            let current = snap.iter().find(|e| e.id == entry.id).unwrap();
            if part.exists() && current.status == DownloadStatus::Downloading {
                break;
            }
            assert!(
                !current.status.is_terminal() && wait_start.elapsed() < Duration::from_secs(5),
                "download did not reach Downloading at 0 bytes (state={current:?})"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(std::fs::metadata(&part).unwrap().len(), 0);

        manager.pause_download(&entry.id).unwrap();
        let paused = wait_for_download_settlement(&manager, &entry.id);
        assert_eq!(paused.status, DownloadStatus::Paused);
        assert_eq!(paused.downloaded_bytes, 0);
        assert_eq!(std::fs::metadata(&part).unwrap().len(), 0);

        // Resume from the 0-byte .fluxnet-part — must NOT fail with os error 80.
        manager.resume_download(None, &entry.id).unwrap();
        let settled = wait_for_download_settlement(&manager, &entry.id);
        server.stop_and_join();

        assert_eq!(
            settled.status,
            DownloadStatus::Completed,
            "resuming from 0-byte partial must complete cleanly: {:?}",
            settled.error
        );
        assert_eq!(settled.downloaded_bytes, expected_mp4.len() as u64);
        assert_eq!(std::fs::read(&dest).unwrap(), expected_mp4);
        assert!(!part.exists());

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn synthetic_sequential_cancel_deletes_partial_file_and_frees_slot() {
        let mp4 = valid_mp4(false);
        let (url, server) = spawn_synthetic_http_server(1, move |_, _| {
            // Delay the single non-Range response body so cancel fires while .fluxnet-part is open.
            let mut r = SyntheticHttpResponse::ok_mp4(mp4.clone());
            r.delay_before_body_ms = 600;
            r
        });
        let dir = create_test_temp_dir("seq-cancel-cleanup");
        let dest = dir.join("cancelled_seq.mp4");
        let part = partial_path(&dest);
        let manager = DownloadManager::default();
        let entry = manager
            .start_download_internal(None, url, dest.to_string_lossy().into_owned(), None)
            .unwrap();

        let wait_start = Instant::now();
        loop {
            let snap = manager.snapshot().unwrap();
            let current = snap.iter().find(|e| e.id == entry.id).unwrap();
            if part.exists() && current.status == DownloadStatus::Downloading {
                break;
            }
            assert!(
                !current.status.is_terminal() && wait_start.elapsed() < Duration::from_secs(5),
                "download did not reach Downloading before cancel (state={current:?})"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(part.exists(), "partial file should exist before cancelling");

        manager.cancel_download(None, &entry.id).unwrap();
        let settled = wait_for_download_settlement(&manager, &entry.id);
        server.stop_and_join();

        assert_eq!(settled.status, DownloadStatus::Cancelled);
        assert!(!dest.exists());
        assert!(
            !part.exists(),
            "cancelling a sequential direct download must delete .fluxnet-part"
        );
        assert_eq!(
            std::fs::read_dir(&dir).unwrap().count(),
            0,
            "staging directory must be empty after cancelling sequential direct download"
        );
        assert_eq!(
            manager.concurrency.active.load(Ordering::Acquire),
            0,
            "concurrency slot must be freed after cancel"
        );

        std::fs::remove_dir_all(dir).unwrap();
    }
}
