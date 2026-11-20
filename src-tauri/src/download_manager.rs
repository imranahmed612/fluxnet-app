use crate::browser_bridge::{
    HlsPlaylist, HlsResource, MAX_HLS_EXPECTED_BYTES, validate_hls_playlist,
};
use futures_util::StreamExt;
use reqwest::{
    Client, StatusCode,
    header::{CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_RANGE, LAST_MODIFIED, RANGE},
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
    sync::Notify,
    time::sleep,
};

const DOWNLOAD_EVENT: &str = "download-manager-update";
const DOWNLOAD_HISTORY_FILE: &str = "download-history.json";
const MAX_CONCURRENT_DOWNLOADS: usize = 4;
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
    pub container: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,
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
    selected_format: crate::media::yt_dlp::SelectedFormat,
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
    let result = tauri::async_runtime::spawn_blocking(move || {
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
    .map_err(|_| "The video download worker could not finish.".to_owned())?;

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
fn spawn_website_worker(
    app: Option<tauri::AppHandle>,
    state: Arc<Mutex<DownloadState>>,
    concurrency: Arc<ConcurrencyLimiter>,
    worker_count: Arc<AtomicUsize>,
    id: String,
    executable: PathBuf,
    selected_format: crate::media::yt_dlp::SelectedFormat,
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
            selected_format,
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
        self.wake.notify_one();
    }

    pub(crate) fn is_pause_requested(&self) -> bool {
        self.pause_requested.load(Ordering::Acquire)
    }

    #[allow(dead_code)]
    pub(crate) fn request_pause(&self) {
        self.pause_requested.store(true, Ordering::Release);
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
            if control.cancelled.load(Ordering::Acquire) {
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
                selected_format: crate::media::yt_dlp::SelectedFormat,
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
                    let executable = state
                        .website_executables
                        .get(id)
                        .cloned()
                        .filter(|path| path.is_file())
                        .ok_or_else(|| {
                            "The yt-dlp executable for this paused website download is no longer available; start a new download to retry.".to_owned()
                        })?;
                    let selected_format = crate::media::yt_dlp::selected_format_from_info(
                        &website_media.format_id,
                        &website_media.container,
                    )
                    .map_err(|error| error.to_string())?;
                    let (validated_dest, _) =
                        validate_resumable_download_destination(&original.destination)?;
                    if validated_dest
                        .extension()
                        .and_then(|ext| ext.to_str())
                        .is_none_or(|ext| !ext.eq_ignore_ascii_case(selected_format.extension))
                    {
                        return Err(
                            "The paused website download destination extension no longer matches its format container."
                                .into(),
                        );
                    }
                    ResumeTarget::Website {
                        executable,
                        selected_format,
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
                let Some(app) = app.cloned() else {
                    return Err(
                        "An application handle is required to resume standard HTTP/HLS downloads."
                            .into(),
                    );
                };
                spawn_worker(
                    app,
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
                selected_format,
            } => {
                spawn_website_worker(
                    app.cloned(),
                    Arc::clone(&self.state),
                    Arc::clone(&self.concurrency),
                    Arc::clone(&self.worker_count),
                    id.to_owned(),
                    executable,
                    selected_format,
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
        let (cancelled_entry, cleanup_website_partial) = {
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
                let cleanup_partial = cancelled
                    .website_media
                    .as_ref()
                    .map(|_| PathBuf::from(&cancelled.destination));
                (Some(cancelled), cleanup_partial)
            } else {
                let control =
                    control.ok_or_else(|| "The download worker was not found.".to_owned())?;
                // Set the signal while holding the same state lock used by the
                // worker's Completed and Paused transitions. Cancellation always
                // wins over both pause and completion races.
                control.request_cancel();
                (None, None)
            }
        };
        if let Some(dest) = cleanup_website_partial {
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
        let selected_format = crate::media::yt_dlp::select_muxed_format(&catalog, &selection)
            .map_err(|error| error.to_string())?;
        run_ytdlp_download_worker(
            app,
            &self.state,
            &self.concurrency,
            id,
            executable,
            selected_format,
            control,
        )
        .await
    }

    #[cfg(windows)]
    pub(crate) fn resolve_verified_ytdlp_executable(
        &self,
        app: Option<&tauri::AppHandle>,
    ) -> Result<PathBuf, String> {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;

        {
            let state = self
                .state
                .lock()
                .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
            if let Some((cached_path, cached_modified)) = &state.verified_ytdlp_executable {
                if let Ok(meta) = std::fs::symlink_metadata(cached_path) {
                    if meta.is_file()
                        && meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT == 0
                        && meta.len() == crate::media::yt_dlp::VERIFIED_YTDLP_SIZE_BYTES
                        && &meta.modified().ok() == cached_modified
                    {
                        return Ok(cached_path.clone());
                    }
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
            crate::media::yt_dlp::discover_muxed_in_workspace(
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
        let _ = crate::media::yt_dlp::select_muxed_format(&catalog, &selection)
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
        if !executable.is_file() {
            return Err("The yt-dlp executable path is invalid or does not exist.".into());
        }
        let normalized_url = crate::media::yt_dlp::validate_public_page_url(&url)
            .map_err(|error| format!("Invalid public website URL: {error}"))?;
        let selected_format = crate::media::yt_dlp::select_muxed_format(&catalog, &selection)
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
            if dest_ext.as_deref() != Some(selected_format.extension) {
                return Err(format!(
                    "The destination file extension (.{:?}) does not match the selected format container (.{}). FluxNet never renames WebM to MP4 or vice versa.",
                    dest_ext.unwrap_or_default(),
                    selected_format.extension
                ));
            }

            let height = match &selection {
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
                    format_id: selected_format.selector.clone(),
                    container: selected_format.extension.to_string(),
                    height,
                }),
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
            selected_format,
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
        let mut state = self
            .state
            .lock()
            .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
        state.history_path = Some(path);
        if let Some(mut persisted) = persisted {
            if persisted.version != 1 {
                return Err("Saved download history uses an unsupported version.".into());
            }
            for entry in &mut persisted.entries {
                if matches!(
                    &entry.status,
                    DownloadStatus::Queued | DownloadStatus::Downloading | DownloadStatus::Paused
                ) {
                    entry.status = DownloadStatus::Failed;
                    entry.error = Some(
                        "Interrupted when FluxNet exited. The partial file was retained; retry explicitly to continue or restart."
                            .into(),
                    );
                    entry.updated_at_ms = current_timestamp_ms();
                }
            }
            state.entries = persisted.entries;
            state.max_concurrent = persisted.max_concurrent.clamp(1, MAX_CONCURRENT_DOWNLOADS);
            state.bandwidth_limit_bytes_per_second = persisted.bandwidth_limit_bytes_per_second;
            state.notifications_enabled = persisted.notifications_enabled;
            (state.notify_on_complete, state.notify_on_failure) =
                persisted_notification_preferences(
                    persisted.notifications_enabled,
                    persisted.notify_on_complete,
                    persisted.notify_on_failure,
                );
            for entry in &mut state.entries {
                if entry.notifications_enabled
                    && !entry.notify_on_complete
                    && !entry.notify_on_failure
                {
                    entry.notify_on_complete = true;
                    entry.notify_on_failure = true;
                }
            }
        }
        persist_locked(&state)?;
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

fn preferred_video_title(page_title: Option<&str>, page_domain: Option<&str>) -> String {
    let title = page_title.unwrap_or_default().trim();
    let generic_title = matches!(
        title.to_ascii_lowercase().as_str(),
        "" | "instagram" | "instagram video" | "facebook" | "facebook video"
    );
    let lowercase_title = title.to_ascii_lowercase();
    let looks_like_url_or_media_filename = lowercase_title.starts_with("http://")
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
            }));
    if !generic_title && !looks_like_url_or_media_filename {
        title.to_owned()
    } else {
        fallback_video_title(page_domain).to_owned()
    }
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
        let title = preferred_video_title(page_title, page_domain).replace(['/', '\\'], "_");
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

fn is_instagram_byte_range_url(url: &reqwest::Url) -> bool {
    let Some(host) = url.host_str().map(str::to_ascii_lowercase) else {
        return false;
    };
    let instagram_cdn = host == "cdninstagram.com"
        || host.ends_with(".cdninstagram.com")
        || host == "fbcdn.net"
        || host.ends_with(".fbcdn.net");
    instagram_cdn
        && url
            .query_pairs()
            .any(|(name, _)| name == "bytestart" || name == "byteend")
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

fn emit_entry_optional(app: Option<&tauri::AppHandle>, entry: DownloadEntry) {
    if let Some(app) = app {
        if let Err(error) = app.emit(DOWNLOAD_EVENT, entry) {
            eprintln!("Could not publish Download Manager update: {error}");
        }
    }
}

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

#[tauri::command]
pub fn start_download(
    app: tauri::AppHandle,
    manager: tauri::State<'_, DownloadManager>,
    url: String,
    destination: String,
    hls_playlist: Option<HlsPlaylist>,
) -> Result<DownloadEntry, String> {
    if url.len() > MAX_URL_LENGTH {
        return Err("Download URL is too long.".into());
    }
    let parsed_url = validated_download_url(&url)?;
    if hls_playlist.is_none() && is_instagram_byte_range_url(&parsed_url) {
        return Err(
            "Instagram supplied a byte-range media URL, not a complete video source. FluxNet did not queue it."
                .into(),
        );
    }
    let hls_playlist = hls_playlist
        .map(|playlist| validate_hls_playlist(playlist, &parsed_url))
        .transpose()?;
    let _suggested = suggested_filename(parsed_url.as_str())?;
    let id = format!("{}-{}", current_timestamp_ms(), rand_id());
    let now = current_timestamp_ms();
    let (entry, control) = {
        let mut state = manager
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
    emit_entry(&app, entry.clone());
    spawn_worker(
        app,
        Arc::clone(&manager.state),
        Arc::clone(&manager.concurrency),
        Arc::clone(&manager.bandwidth),
        Arc::clone(&manager.worker_count),
        id,
        control,
        false,
    );
    Ok(entry)
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
    app: tauri::AppHandle,
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
            &app,
            &state,
            &concurrency,
            &bandwidth,
            &id,
            &control,
            restart,
        )
        .await
        {
            if let Err(update_error) = fail_entry(&app, &state, &id, error).await {
                eprintln!("Could not record download failure: {update_error}");
            }
        }
    });
}

struct WorkerGuard(Arc<AtomicUsize>);

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

async fn download_worker(
    app: &tauri::AppHandle,
    state: &Mutex<DownloadState>,
    concurrency: &Arc<ConcurrencyLimiter>,
    bandwidth: &Arc<BandwidthLimiter>,
    id: &str,
    control: &Arc<DownloadTaskControl>,
    restart: bool,
) -> Result<(), String> {
    let Some(_slot) = concurrency.acquire(state, control).await? else {
        return finish_cancelled(app, state, id);
    };
    update_entry(app, state, id, |entry| {
        if entry.status == DownloadStatus::Queued || entry.status == DownloadStatus::Paused {
            transition(entry, DownloadStatus::Downloading)?;
        }
        Ok(())
    })?;
    let (
        url,
        destination,
        mut offset,
        etag,
        last_modified,
        base_elapsed_ms,
        hls_playlist,
        hls_completed_parts,
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
            entry.etag.clone(),
            entry.last_modified.clone(),
            if restart { 0 } else { entry.elapsed_ms },
            entry.hls_playlist.clone(),
            if restart {
                0
            } else {
                entry.hls_completed_parts.unwrap_or(0)
            },
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
    let partial = partial_path(&destination);
    if restart {
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
    if offset == 0 && destination.exists() {
        return fail_entry(
            app,
            state,
            id,
            "The destination file already exists; choose a different filename.".into(),
        )
        .await;
    }
    if offset > 0 {
        match fs::symlink_metadata(&partial).await {
            Ok(metadata) if metadata.file_type().is_file() && metadata.len() == offset => {}
            Ok(_) => {
                return fail_entry(
                    app,
                    state,
                    id,
                    "The partial file size does not match the recorded offset. Use Retry and choose restart to avoid corrupting the file.".into(),
                )
                .await;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return fail_entry(
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
    }

    let client = build_http_client()?;
    let mut request = client.get(&url);
    if offset > 0 {
        let validator = resume_validator(etag.as_deref(), last_modified.as_deref());
        if validator.is_none() {
            return fail_entry(
                app,
                state,
                id,
                "The server did not provide a validator for safe resume. Use Retry to restart from zero.".into(),
            )
            .await;
        }
        request = request.header(RANGE, format!("bytes={offset}-"));
        if let Some(validator) = validator {
            request = request.header(IF_RANGE, validator);
        }
    }
    let response = tokio::select! {
        response = request.send() => response
            .map_err(|error| describe_reqwest_error(&error, &url))?,
        _ = control.wake.notified() => {
            if control.cancelled.load(Ordering::Acquire) {
                return finish_cancelled(app, state, id);
            }
            if control.pause_requested.load(Ordering::Acquire) {
                update_entry(app, state, id, |entry| {
                    transition(entry, DownloadStatus::Paused)?;
                    entry.speed_bytes_per_second = 0.0;
                    entry.remaining_seconds = None;
                    Ok(())
                })?;
                return Ok(());
            }
            return Err("The download worker was interrupted before the request started.".into());
        }
    };
    let status = response.status();
    let content_range = response
        .headers()
        .get(CONTENT_RANGE)
        .and_then(|value| value.to_str().ok());
    let mut total_bytes = response.content_length();
    if offset == 0 {
        if let Err(message) = validate_unranged_media_response(status, content_range, total_bytes) {
            return fail_entry(app, state, id, message).await;
        }
    }
    if !status.is_success() {
        return fail_entry(
            app,
            state,
            id,
            format!("HTTP status error: the server returned {status}."),
        )
        .await;
    }

    let response_content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    if offset > 0 {
        let range_header = response
            .headers()
            .get(CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
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
        let expected_validator = resume_validator(etag.as_deref(), last_modified.as_deref());
        let response_validator = if response_etag.as_deref() == expected_validator {
            response_etag.as_deref()
        } else {
            response_last_modified.as_deref()
        };
        let total = match validate_range_response(
            status,
            range_header.as_deref(),
            offset,
            expected_validator,
            response_validator,
        ) {
            Ok(total) => total,
            Err(message) => return fail_entry(app, state, id, message).await,
        };
        total_bytes = Some(total);
    } else if status != StatusCode::OK {
        return fail_entry(
            app,
            state,
            id,
            format!(
                "HTTP status error: expected HTTP 200 for a new download but received {status}."
            ),
        )
        .await;
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
    if offset > 0 && total_bytes.is_some_and(|total| offset > total) {
        return fail_entry(
            app,
            state,
            id,
            "The server's range metadata conflicts with the partial file. Use Retry to restart safely.".into(),
        )
        .await;
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
        return fail_entry(
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
    let mut stream = response.bytes_stream();

    update_entry(app, state, id, |entry| {
        entry.total_bytes = total_bytes;
        entry.error = None;
        entry.etag = response_etag;
        entry.last_modified = response_last_modified;
        Ok(())
    })?;

    loop {
        if control.cancelled.load(Ordering::Acquire) {
            output
                .flush()
                .await
                .map_err(|error| filesystem_error("could not flush the partial file", error))?;
            let elapsed_ms = base_elapsed_ms
                .saturating_add(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
            update_entry(app, state, id, |entry| {
                entry.downloaded_bytes = offset;
                entry.elapsed_ms = elapsed_ms;
                Ok(())
            })?;
            return finish_cancelled(app, state, id);
        }
        if control.pause_requested.load(Ordering::Acquire) {
            output
                .flush()
                .await
                .map_err(|error| filesystem_error("could not flush the partial file", error))?;
            let elapsed_ms = base_elapsed_ms
                .saturating_add(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
            update_entry(app, state, id, |entry| {
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
                let elapsed_ms = base_elapsed_ms
                    .saturating_add(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
                update_entry(app, state, id, |entry| {
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
        let limit = state
            .lock()
            .map_err(|_| "Download manager state lock was poisoned.".to_owned())?
            .bandwidth_limit_bytes_per_second;
        bandwidth.wait_for_turn(chunk.len(), limit).await?;
        if let Err(error) = output.write_all(&chunk).await {
            let elapsed_ms = base_elapsed_ms
                .saturating_add(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
            update_entry(app, state, id, |entry| {
                entry.downloaded_bytes = offset;
                entry.elapsed_ms = elapsed_ms;
                Ok(())
            })?;
            return Err(filesystem_error("could not write downloaded data", error));
        }
        offset = offset.saturating_add(chunk.len() as u64);
        if total_bytes.is_some_and(|total| offset > total) {
            return fail_entry(
                app,
                state,
                id,
                "The server sent more data than its declared file size.".into(),
            )
            .await;
        }
        if last_update.elapsed() >= PROGRESS_INTERVAL {
            let elapsed = start.elapsed().as_secs_f64();
            speed = offset.saturating_sub(last_bytes) as f64 / last_update.elapsed().as_secs_f64();
            last_bytes = offset;
            last_update = Instant::now();
            let elapsed_ms = base_elapsed_ms.saturating_add(elapsed.mul_add(1000.0, 0.0) as u64);
            let remaining_seconds = total_bytes
                .filter(|_| speed > 0.0)
                .map(|total| total.saturating_sub(offset) as f64 / speed);
            update_entry(app, state, id, |entry| {
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
    drop(output);
    if control.cancelled.load(Ordering::Acquire) {
        let elapsed_ms = base_elapsed_ms
            .saturating_add(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
        update_entry(app, state, id, |entry| {
            entry.downloaded_bytes = offset;
            entry.elapsed_ms = elapsed_ms;
            Ok(())
        })?;
        return finish_cancelled(app, state, id);
    }
    if total_bytes.is_some_and(|total| offset != total) {
        let elapsed_ms = base_elapsed_ms
            .saturating_add(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
        update_entry(app, state, id, |entry| {
            entry.downloaded_bytes = offset;
            entry.elapsed_ms = elapsed_ms;
            Ok(())
        })?;
        return fail_entry(
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
    let elapsed_ms =
        base_elapsed_ms.saturating_add(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
    update_entry(app, state, id, |entry| {
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
    if destination.exists() {
        return fail_entry(
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
    let average_speed = if elapsed_ms > 0 {
        offset as f64 / (elapsed_ms as f64 / 1000.0)
    } else {
        speed
    };
    let completed = update_entry(app, state, id, |entry| {
        transition(entry, DownloadStatus::Completed)?;
        entry.downloaded_bytes = offset;
        entry.total_bytes = Some(offset);
        entry.speed_bytes_per_second = average_speed;
        entry.elapsed_ms = elapsed_ms;
        entry.remaining_seconds = Some(0.0);
        entry.error = None;
        Ok(())
    })?;
    if completed.notify_on_complete {
        send_download_notification(app, &completed, true);
    }
    Ok(())
}

enum HlsPartOutcome {
    Complete(u64),
    Paused,
    Cancelled,
}

fn hls_stop_reason(control: &DownloadTaskControl) -> Option<HlsPartOutcome> {
    if control.cancelled.load(Ordering::Acquire) {
        Some(HlsPartOutcome::Cancelled)
    } else if control.pause_requested.load(Ordering::Acquire) {
        Some(HlsPartOutcome::Paused)
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
    app: &tauri::AppHandle,
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
        if matches!(hls_stop_reason(control), Some(HlsPartOutcome::Cancelled)) {
            output.flush().await.map_err(|error| {
                filesystem_error("could not flush the temporary HLS segment", error)
            })?;
            drop(output);
            remove_hls_staging(staging).await?;
            return Ok(HlsPartOutcome::Cancelled);
        }
        if matches!(hls_stop_reason(control), Some(HlsPartOutcome::Paused)) {
            output.flush().await.map_err(|error| {
                filesystem_error("could not flush the temporary HLS segment", error)
            })?;
            drop(output);
            remove_hls_staging(staging).await?;
            return Ok(HlsPartOutcome::Paused);
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
            update_entry(app, state, id, |entry| {
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
    app: &tauri::AppHandle,
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
        return fail_entry(
            app,
            state,
            id,
            "The destination file already exists; choose a different filename.".into(),
        )
        .await;
    }

    let resources = ordered_hls_resources(&playlist);
    if completed_parts > resources.len() || committed_bytes > MAX_HLS_EXPECTED_BYTES {
        return fail_entry(
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
                return fail_entry(
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
            return fail_entry(
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
            return fail_entry(
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
            if matches!(reason, HlsPartOutcome::Cancelled) {
                return finish_cancelled(app, state, id);
            }
            update_entry(app, state, id, |entry| {
                transition(entry, DownloadStatus::Paused)?;
                entry.downloaded_bytes = committed_bytes;
                entry.speed_bytes_per_second = 0.0;
                entry.remaining_seconds = None;
                Ok(())
            })?;
            return Ok(());
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
            HlsPartOutcome::Cancelled => return finish_cancelled(app, state, id),
            HlsPartOutcome::Paused => {
                update_entry(app, state, id, |entry| {
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
        update_entry(app, state, id, |entry| {
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
    if let Some(reason) = hls_stop_reason(control) {
        if matches!(reason, HlsPartOutcome::Cancelled) {
            return finish_cancelled(app, state, id);
        }
        update_entry(app, state, id, |entry| {
            transition(entry, DownloadStatus::Paused)?;
            entry.speed_bytes_per_second = 0.0;
            entry.remaining_seconds = None;
            Ok(())
        })?;
        return Ok(());
    }
    if destination.exists() {
        return fail_entry(
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
    let elapsed_ms =
        base_elapsed_ms.saturating_add(started.elapsed().as_millis().min(u64::MAX as u128) as u64);
    let average_speed = if elapsed_ms > 0 {
        committed_bytes as f64 / (elapsed_ms as f64 / 1000.0)
    } else {
        0.0
    };
    let completed = update_entry(app, state, id, |entry| {
        transition(entry, DownloadStatus::Completed)?;
        entry.downloaded_bytes = committed_bytes;
        entry.total_bytes = Some(committed_bytes);
        entry.hls_completed_parts = Some(resources.len());
        entry.speed_bytes_per_second = average_speed;
        entry.elapsed_ms = elapsed_ms;
        entry.remaining_seconds = Some(0.0);
        entry.error = None;
        Ok(())
    })?;
    if completed.notify_on_complete {
        send_download_notification(app, &completed, true);
    }
    Ok(())
}

pub(crate) fn partial_path(destination: &Path) -> PathBuf {
    let mut name = destination.file_name().unwrap_or_default().to_os_string();
    name.push(".fluxnet-part");
    destination.with_file_name(name)
}

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
    let control = Arc::new(DownloadTaskControl::default());
    {
        let mut state = manager
            .state
            .lock()
            .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
        let index = state
            .entries
            .iter()
            .position(|entry| entry.id == id)
            .ok_or_else(|| "Download record was not found.".to_owned())?;
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
        if restart {
            entry.downloaded_bytes = 0;
            entry.total_bytes = None;
            entry.speed_bytes_per_second = 0.0;
            entry.elapsed_ms = 0;
            entry.remaining_seconds = None;
            if entry.hls_playlist.is_some() {
                entry.hls_completed_parts = Some(0);
            }
        }
        state.entries[index] = entry;
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
    }
    spawn_worker(
        app,
        Arc::clone(&manager.state),
        Arc::clone(&manager.concurrency),
        Arc::clone(&manager.bandwidth),
        Arc::clone(&manager.worker_count),
        id,
        control,
        restart,
    );
    Ok(())
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
    let controls = state.controls.values().cloned().collect::<Vec<_>>();
    for control in &controls {
        control.request_cancel();
    }
    let mut website_partials_to_remove = Vec::new();
    let mut cancelled_paused_ids = Vec::new();
    for entry in &mut state.entries {
        if entry.status == DownloadStatus::Paused {
            transition(entry, DownloadStatus::Cancelled)?;
            cancelled_paused_ids.push(entry.id.clone());
            if entry.website_media.is_some() {
                website_partials_to_remove.push(PathBuf::from(&entry.destination));
            }
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
    Ok((Arc::clone(&manager.worker_count), controls.len()))
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
    fn rejects_instagram_reel_and_story_byte_range_urls_before_queueing() {
        for url in [
            "https://scontent.cdninstagram.com/v/t51/reel.mp4?bytestart=0&byteend=10399&token=private",
            "https://instagram.fdac5-1.fna.fbcdn.net/v/t51/story.mp4?signature=private&bytestart=0&byteend=10399",
        ] {
            let parsed = validated_download_url(url).unwrap();
            assert!(is_instagram_byte_range_url(&parsed));
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
        assert_eq!(
            suggested_filename_with_metadata(
                "https://scontent.cdninstagram.com/clip.mp4?token=1",
                Some("Instagram"),
                Some("instagram.com"),
                Some("mp4"),
                None,
            )
            .unwrap(),
            "Instagram_Video.mp4"
        );
        assert_eq!(
            suggested_filename_with_metadata(
                "https://scontent.cdninstagram.com/clip.mp4?token=1",
                Some("https://scontent.cdninstagram.com/very-long-cdn-hash.mp4?token=1"),
                Some("instagram.com"),
                Some("mp4"),
                Some("very-long-cdn-hash.mp4"),
            )
            .unwrap(),
            "Instagram_Video.mp4"
        );
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
                    container: "mp4".into(),
                    height: Some(720),
                }),
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
}
