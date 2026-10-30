use std::{
    io::{self, Read},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use reqwest::{StatusCode, Url, blocking::Client};
use serde::Serialize;
use tauri::{AppHandle, Emitter, State};

const TEST_BYTES: u64 = 5 * 1024 * 1024;
const MAX_TRANSFER_BYTES: u64 = 5 * 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const READ_CHUNK_BYTES: usize = 64 * 1024;
const CLOUDFLARE_DOWNLOAD_URL: &str = "https://speed.cloudflare.com/__down?bytes=5242880";
const CLOUDFLARE_UPLOAD_URL: &str = "https://speed.cloudflare.com/__up";

#[derive(Default)]
pub struct SpeedTestControl {
    running: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SpeedTestProgress {
    pub phase: String,
    pub bytes_transferred: u64,
    pub total_bytes: u64,
    pub percent: f64,
    pub elapsed_ms: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SpeedTestResult {
    pub endpoint: String,
    pub download_mbps: f64,
    pub upload_mbps: f64,
    pub download_elapsed_ms: u64,
    pub upload_elapsed_ms: u64,
    pub elapsed_ms: u64,
    pub download_bytes: u64,
    pub upload_bytes: u64,
}

struct RunningGuard(Arc<AtomicBool>);

impl Drop for RunningGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

#[tauri::command]
pub async fn run_speed_test(
    app: AppHandle,
    control: State<'_, SpeedTestControl>,
    provider: String,
    endpoint: String,
) -> Result<SpeedTestResult, String> {
    if control
        .running
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err("A speed test is already running.".into());
    }
    control.cancelled.store(false, Ordering::Release);

    let running = Arc::clone(&control.running);
    let cancelled = Arc::clone(&control.cancelled);
    tauri::async_runtime::spawn_blocking(move || {
        let _running_guard = RunningGuard(running);
        run_speed_test_blocking(app, provider, endpoint, cancelled)
    })
    .await
    .map_err(|error| format!("The speed test task failed: {error}"))?
}

#[tauri::command]
pub fn cancel_speed_test(control: State<'_, SpeedTestControl>) -> Result<(), String> {
    if !control.running.load(Ordering::Acquire) {
        return Err("No speed test is currently running.".into());
    }
    control.cancelled.store(true, Ordering::Release);
    Ok(())
}

fn run_speed_test_blocking(
    app: AppHandle,
    provider: String,
    endpoint: String,
    cancelled: Arc<AtomicBool>,
) -> Result<SpeedTestResult, String> {
    let (download_url, upload_url, result_endpoint) = resolve_endpoints(&provider, &endpoint)?;
    let client = Client::builder()
        .connect_timeout(REQUEST_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| format!("Could not configure the speed test client: {error}"))?;

    let total_started = Instant::now();
    check_cancelled(&cancelled)?;
    emit_progress(&app, "download", 0, TEST_BYTES, total_started)?;

    let download_started = Instant::now();
    let mut response = client
        .get(download_url)
        .send()
        .map_err(|error| request_error(&cancelled, "Download request failed", error))?;
    check_cancelled(&cancelled)?;
    require_success(response.status(), "download")?;
    if response
        .content_length()
        .is_some_and(|length| length > MAX_TRANSFER_BYTES)
    {
        return Err("The test endpoint offered more than the 5 MiB download limit.".into());
    }

    let mut download_bytes = 0u64;
    let mut buffer = vec![0u8; READ_CHUNK_BYTES];
    loop {
        check_cancelled(&cancelled)?;
        let bytes_read = response.read(&mut buffer).map_err(|error| {
            request_error(&cancelled, "Download response could not be read", error)
        })?;
        if bytes_read == 0 {
            break;
        }
        download_bytes += bytes_read as u64;
        if download_bytes > MAX_TRANSFER_BYTES {
            return Err("The test endpoint sent more than the 5 MiB download limit.".into());
        }
        emit_progress(&app, "download", download_bytes, TEST_BYTES, total_started)?;
    }
    let download_elapsed = download_started.elapsed();
    if download_bytes != TEST_BYTES {
        return Err(format!(
            "The download endpoint returned {download_bytes} bytes; the required test size is {TEST_BYTES} bytes. Verify that the configured server supports the FluxNet speed-test contract."
        ));
    }

    check_cancelled(&cancelled)?;
    emit_progress(&app, "upload", 0, TEST_BYTES, total_started)?;
    let upload_started = Instant::now();
    let uploaded_bytes = Arc::new(AtomicU64::new(0));
    let upload_body = TestPayloadReader {
        remaining: TEST_BYTES,
        transferred: 0,
        uploaded_bytes: Arc::clone(&uploaded_bytes),
        app: app.clone(),
        cancelled: Arc::clone(&cancelled),
        started: total_started,
    };
    let response = client
        .post(upload_url)
        .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
        .header(reqwest::header::CONTENT_LENGTH, TEST_BYTES)
        .body(reqwest::blocking::Body::new(upload_body))
        .send()
        .map_err(|error| request_error(&cancelled, "Upload request failed", error))?;
    check_cancelled(&cancelled)?;
    require_success(response.status(), "upload")?;
    let upload_bytes = uploaded_bytes.load(Ordering::Acquire);
    require_complete_upload(upload_bytes)?;
    let upload_elapsed = upload_started.elapsed();
    let total_elapsed = total_started.elapsed();

    Ok(SpeedTestResult {
        endpoint: result_endpoint,
        download_mbps: throughput_mbps(download_bytes, download_elapsed),
        upload_mbps: throughput_mbps(upload_bytes, upload_elapsed),
        download_elapsed_ms: elapsed_ms(download_elapsed),
        upload_elapsed_ms: elapsed_ms(upload_elapsed),
        elapsed_ms: elapsed_ms(total_elapsed),
        download_bytes,
        upload_bytes,
    })
}

fn resolve_endpoints(provider: &str, endpoint: &str) -> Result<(Url, Url, String), String> {
    match provider {
        "cloudflare" => Ok((
            Url::parse(CLOUDFLARE_DOWNLOAD_URL)
                .map_err(|error| format!("Invalid Cloudflare download URL: {error}"))?,
            Url::parse(CLOUDFLARE_UPLOAD_URL)
                .map_err(|error| format!("Invalid Cloudflare upload URL: {error}"))?,
            "speed.cloudflare.com".into(),
        )),
        "custom" => {
            let origin = validate_endpoint(endpoint)?;
            let download_url = endpoint_url(&origin, "download")?;
            let upload_url = endpoint_url(&origin, "upload")?;
            Ok((
                download_url,
                upload_url,
                origin.origin().ascii_serialization(),
            ))
        }
        _ => Err("Choose Cloudflare or a custom speed-test server.".into()),
    }
}

fn validate_endpoint(value: &str) -> Result<Url, String> {
    let endpoint = Url::parse(value.trim())
        .map_err(|error| format!("Enter a valid HTTPS speed-test endpoint: {error}"))?;
    if endpoint.scheme() != "https" {
        return Err("The speed-test endpoint must use HTTPS.".into());
    }
    if endpoint.host_str().is_none() {
        return Err("The speed-test endpoint must include a host name.".into());
    }
    if !endpoint.username().is_empty() || endpoint.password().is_some() {
        return Err("Do not include credentials in the speed-test endpoint URL.".into());
    }
    if endpoint.path() != "/" || endpoint.query().is_some() || endpoint.fragment().is_some() {
        return Err(
            "Enter the HTTPS server origin only, for example https://speed.example.com.".into(),
        );
    }
    Ok(endpoint)
}

fn endpoint_url(endpoint: &Url, path: &str) -> Result<Url, String> {
    endpoint
        .join(path)
        .map_err(|error| format!("Could not construct the {path} endpoint URL: {error}"))
        .map(|mut url| {
            if path == "download" {
                url.query_pairs_mut()
                    .append_pair("bytes", &TEST_BYTES.to_string());
            }
            url
        })
}

fn require_success(status: StatusCode, phase: &str) -> Result<(), String> {
    if status.is_success() {
        Ok(())
    } else {
        Err(format!(
            "The {phase} endpoint returned HTTP {status}. Confirm the configured server supports the FluxNet speed-test contract."
        ))
    }
}

fn check_cancelled(cancelled: &AtomicBool) -> Result<(), String> {
    if cancelled.load(Ordering::Acquire) {
        Err("Speed test stopped by user.".into())
    } else {
        Ok(())
    }
}

fn request_error(cancelled: &AtomicBool, context: &str, error: impl std::fmt::Display) -> String {
    if cancelled.load(Ordering::Acquire) {
        "Speed test stopped by user.".into()
    } else {
        format!("{context}: {error}")
    }
}

fn require_complete_upload(upload_bytes: u64) -> Result<(), String> {
    if upload_bytes == TEST_BYTES {
        Ok(())
    } else {
        Err(format!(
            "The upload endpoint responded after FluxNet provided {upload_bytes} of {TEST_BYTES} bytes. Verify that the server consumes the complete upload before responding."
        ))
    }
}

fn emit_progress(
    app: &AppHandle,
    phase: &str,
    bytes_transferred: u64,
    total_bytes: u64,
    started: Instant,
) -> Result<(), String> {
    let progress = SpeedTestProgress {
        phase: phase.into(),
        bytes_transferred,
        total_bytes,
        percent: bytes_transferred as f64 / total_bytes as f64 * 100.0,
        elapsed_ms: elapsed_ms(started.elapsed()),
    };
    app.emit("speed-test-progress", progress)
        .map_err(|error| format!("Could not update speed-test progress: {error}"))
}

fn throughput_mbps(bytes: u64, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        return 0.0;
    }
    bytes as f64 * 8.0 / elapsed.as_secs_f64() / 1_000_000.0
}

fn elapsed_ms(elapsed: Duration) -> u64 {
    elapsed.as_millis().min(u64::MAX as u128) as u64
}

struct TestPayloadReader {
    remaining: u64,
    transferred: u64,
    uploaded_bytes: Arc<AtomicU64>,
    app: AppHandle,
    cancelled: Arc<AtomicBool>,
    started: Instant,
}

impl Read for TestPayloadReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "Speed test stopped by user.",
            ));
        }
        if self.remaining == 0 {
            return Ok(0);
        }

        let bytes_read = buffer
            .len()
            .min(self.remaining.min(READ_CHUNK_BYTES as u64) as usize);
        buffer[..bytes_read].fill(0);
        self.remaining -= bytes_read as u64;
        self.transferred += bytes_read as u64;
        self.uploaded_bytes
            .store(self.transferred, Ordering::Release);
        emit_progress(
            &self.app,
            "upload",
            self.transferred,
            TEST_BYTES,
            self.started,
        )
        .map_err(io::Error::other)?;
        Ok(bytes_read)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_requires_https_root_origin_without_credentials_or_query() {
        assert!(validate_endpoint("https://speed.example.com").is_ok());
        assert!(validate_endpoint("http://speed.example.com").is_err());
        assert!(validate_endpoint("https://user:pass@speed.example.com").is_err());
        assert!(validate_endpoint("https://speed.example.com/path").is_err());
        assert!(validate_endpoint("https://speed.example.com?token=secret").is_err());
    }

    #[test]
    fn endpoint_paths_match_documented_speed_test_contract() {
        let (download_url, upload_url, result_endpoint) =
            resolve_endpoints("custom", "https://speed.example.com").unwrap();
        assert_eq!(
            download_url.as_str(),
            "https://speed.example.com/download?bytes=5242880"
        );
        assert_eq!(upload_url.as_str(), "https://speed.example.com/upload");
        assert_eq!(result_endpoint, "https://speed.example.com");
    }

    #[test]
    fn cloudflare_preset_uses_its_documented_download_and_upload_urls() {
        let (download_url, upload_url, result_endpoint) =
            resolve_endpoints("cloudflare", "").unwrap();
        assert_eq!(download_url.as_str(), CLOUDFLARE_DOWNLOAD_URL);
        assert_eq!(upload_url.as_str(), CLOUDFLARE_UPLOAD_URL);
        assert_eq!(result_endpoint, "speed.cloudflare.com");
    }

    #[test]
    fn unknown_provider_is_rejected() {
        assert!(resolve_endpoints("unknown", "").is_err());
    }

    #[test]
    fn throughput_is_based_on_bytes_actually_transferred_and_elapsed_time() {
        assert_eq!(throughput_mbps(1_000_000, Duration::from_secs(1)), 8.0);
        assert_eq!(throughput_mbps(0, Duration::from_secs(1)), 0.0);
        assert_eq!(throughput_mbps(100, Duration::ZERO), 0.0);
    }

    #[test]
    fn upload_must_be_fully_provided_before_reporting_a_result() {
        assert!(require_complete_upload(TEST_BYTES).is_ok());
        assert!(require_complete_upload(TEST_BYTES - 1).is_err());
        assert!(require_complete_upload(0).is_err());
    }

    #[test]
    fn cancelled_requests_report_a_stopped_state() {
        let cancelled = AtomicBool::new(true);
        assert_eq!(
            request_error(&cancelled, "Download request failed", "timed out"),
            "Speed test stopped by user."
        );
    }

    #[test]
    fn upload_payload_is_bounded_and_contains_no_user_data() {
        let payload = vec![0u8; TEST_BYTES as usize];
        assert_eq!(payload.len() as u64, MAX_TRANSFER_BYTES);
        assert!(payload.iter().all(|byte| *byte == 0));
    }
}
