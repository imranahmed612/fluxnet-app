use futures_util::StreamExt;
use reqwest::{
    Client, StatusCode,
    header::{CONTENT_RANGE, ETAG, IF_RANGE, LAST_MODIFIED, RANGE},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    error::Error as StdError,
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

struct DownloadTaskControl {
    cancelled: AtomicBool,
    pause_requested: AtomicBool,
    wake: Notify,
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

struct DownloadState {
    entries: Vec<DownloadEntry>,
    controls: HashMap<String, Arc<DownloadTaskControl>>,
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
                    entry.status,
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
    let stem = sanitized
        .split('.')
        .next()
        .unwrap_or_default()
        .trim_end_matches([' ', '.'])
        .to_ascii_uppercase();
    if matches!(
        stem.as_str(),
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

fn suggested_filename(url: &str) -> Result<String, String> {
    let parsed = validated_download_url(url)?;
    Ok(sanitize_filename(
        parsed
            .path_segments()
            .and_then(Iterator::last)
            .filter(|segment| !segment.is_empty())
            .unwrap_or("download"),
    ))
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

fn validate_download_destination(path: &str) -> Result<(PathBuf, String), String> {
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
    if partial.exists() {
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

fn emit_entry(app: &tauri::AppHandle, entry: DownloadEntry) {
    if let Err(error) = app.emit(DOWNLOAD_EVENT, entry) {
        eprintln!("Could not publish Download Manager update: {error}");
    }
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
        if updated.status.is_terminal() {
            state.controls.remove(id);
        }
        updated
    };
    emit_entry(app, entry.clone());
    Ok(entry)
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
pub fn get_download_filename(url: String) -> Result<String, String> {
    if url.len() > MAX_URL_LENGTH {
        return Err("Download URL is too long.".into());
    }
    suggested_filename(&url)
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

#[tauri::command]
pub fn start_download(
    app: tauri::AppHandle,
    manager: tauri::State<'_, DownloadManager>,
    url: String,
    destination: String,
) -> Result<DownloadEntry, String> {
    if url.len() > MAX_URL_LENGTH {
        return Err("Download URL is too long.".into());
    }
    let parsed_url = validated_download_url(&url)?;
    let _suggested = suggested_filename(parsed_url.as_str())?;
    let (destination, selected_filename) = validate_download_destination(&destination)?;
    let filename = selected_filename;
    let id = format!("{}-{}", current_timestamp_ms(), rand_id());
    let now = current_timestamp_ms();
    let (entry, control) = {
        let mut state = manager
            .state
            .lock()
            .map_err(|_| "Download manager state lock was poisoned.".to_owned())?;
        let control = Arc::new(DownloadTaskControl::default());
        if state.entries.iter().any(|entry| {
            entry
                .destination
                .eq_ignore_ascii_case(destination.to_string_lossy().as_ref())
        }) {
            return Err("A download already uses this destination.".into());
        }
        let entry = DownloadEntry {
            id: id.clone(),
            url: parsed_url.to_string(),
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
    let (url, destination, mut offset, etag, last_modified, base_elapsed_ms) = {
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
        )
    };
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
            return Err("The download worker was interrupted before the request started.".into());
        }
    };
    let status = response.status();
    if !status.is_success() {
        return fail_entry(
            app,
            state,
            id,
            format!("HTTP status error: the server returned {status}."),
        )
        .await;
    }

    let mut total_bytes = response.content_length();
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
        if entry.status == DownloadStatus::Queued || entry.status == DownloadStatus::Paused {
            transition(entry, DownloadStatus::Downloading)?;
        }
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
    let elapsed_ms =
        base_elapsed_ms.saturating_add(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
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

fn partial_path(destination: &Path) -> PathBuf {
    let mut name = destination.file_name().unwrap_or_default().to_os_string();
    name.push(".fluxnet-part");
    destination.with_file_name(name)
}

fn send_download_notification(app: &tauri::AppHandle, entry: &DownloadEntry, completed: bool) {
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
    let entry = update_entry(app, state, id, |entry| {
        if !entry.status.is_terminal() {
            transition(entry, DownloadStatus::Failed)?;
        }
        entry.error = Some(message);
        entry.speed_bytes_per_second = 0.0;
        entry.remaining_seconds = None;
        Ok(())
    })?;
    if entry.notify_on_failure {
        send_download_notification(app, &entry, false);
    }
    Ok(())
}

fn finish_cancelled(
    app: &tauri::AppHandle,
    state: &Mutex<DownloadState>,
    id: &str,
) -> Result<(), String> {
    update_entry(app, state, id, |entry| {
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
    let state = manager
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
        .get(&id)
        .ok_or_else(|| "The download worker is no longer available.".to_owned())?;
    control.pause_requested.store(true, Ordering::Release);
    control.wake.notify_one();
    Ok(())
}

#[tauri::command]
pub fn resume_download(
    app: tauri::AppHandle,
    manager: tauri::State<'_, DownloadManager>,
    id: String,
) -> Result<(), String> {
    let control = {
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
        let mut entry = original.clone();
        if entry.status != DownloadStatus::Paused {
            return Err("Only a paused download can be resumed.".into());
        }
        transition(&mut entry, DownloadStatus::Queued)?;
        entry.attempts = entry.attempts.saturating_add(1);
        let control = Arc::new(DownloadTaskControl::default());
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
        control
    };
    let state = Arc::clone(&manager.state);
    let concurrency = Arc::clone(&manager.concurrency);
    let bandwidth = Arc::clone(&manager.bandwidth);
    let worker_count = Arc::clone(&manager.worker_count);
    spawn_worker(
        app,
        state,
        concurrency,
        bandwidth,
        worker_count,
        id,
        control,
        false,
    );
    Ok(())
}

#[tauri::command]
pub fn cancel_download(
    app: tauri::AppHandle,
    manager: tauri::State<'_, DownloadManager>,
    id: String,
) -> Result<(), String> {
    let (control, cancelled_entry) = {
        let mut state = manager
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
        let control = state
            .controls
            .get(&id)
            .cloned()
            .ok_or_else(|| "The download worker was not found.".to_owned())?;
        if state.entries[index].status == DownloadStatus::Paused {
            let original = state.entries[index].clone();
            let mut cancelled = original.clone();
            transition(&mut cancelled, DownloadStatus::Cancelled)?;
            cancelled.speed_bytes_per_second = 0.0;
            cancelled.remaining_seconds = None;
            state.entries[index] = cancelled.clone();
            state.controls.remove(&id);
            if let Err(error) = persist_locked(&state) {
                state.entries[index] = original;
                state.controls.insert(id, control);
                return Err(error);
            }
            (None, Some(cancelled))
        } else {
            (Some(control), None)
        }
    };
    if let Some(control) = control {
        control.cancelled.store(true, Ordering::Release);
        control.wake.notify_one();
    }
    if let Some(entry) = cancelled_entry {
        emit_entry(&app, entry);
    }
    Ok(())
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
        control.cancelled.store(true, Ordering::Release);
        control.wake.notify_one();
    }
    for entry in &mut state.entries {
        if entry.status == DownloadStatus::Paused {
            transition(entry, DownloadStatus::Cancelled)?;
        }
    }
    if let Err(error) = persist_locked(&state) {
        eprintln!(
            "Could not checkpoint download history during shutdown; partial files will be validated on the next launch: {error}"
        );
    }
    Ok((Arc::clone(&manager.worker_count), controls.len()))
}

#[cfg(test)]
mod tests {
    use super::*;

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
