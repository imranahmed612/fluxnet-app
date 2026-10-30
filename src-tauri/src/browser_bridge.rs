use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use tauri::{AppHandle, Emitter, Manager};

const MAX_MESSAGE_BYTES: usize = 64 * 1024;
const MAX_URL_LENGTH: usize = 8 * 1024;
const MAX_TEXT_LENGTH: usize = 512;
const CONNECTION_FRESHNESS_MS: u64 = 30_000;

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BrowserBridgeStatus {
    pub pairing_code: String,
    pub connection_status: String,
    pub last_handshake_at_ms: Option<u64>,
    pub native_host_status: String,
    pub registered_browsers: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BrowserMediaResource {
    pub url: String,
    pub media_type: String,
    pub page_title: String,
    pub page_domain: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tab_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_bytes: Option<u64>,
    pub observed_bytes: Option<u64>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    #[serde(default)]
    pub received_at_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NativeMessage {
    action: String,
    pairing_code: String,
    resource: Option<BrowserMediaResource>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct NativeResponse {
    ok: bool,
    message: String,
}

pub struct BrowserBridge {
    pairing_code: String,
    connected: AtomicBool,
    connection_attempted: AtomicBool,
    last_handshake_at_ms: AtomicU64,
    resources: Mutex<Vec<BrowserMediaResource>>,
}

impl Default for BrowserBridge {
    fn default() -> Self {
        let mut bytes = [0_u8; 16];
        rand::rng().fill_bytes(&mut bytes);
        Self {
            pairing_code: bytes.iter().map(|byte| format!("{byte:02x}")).collect(),
            connected: AtomicBool::new(false),
            connection_attempted: AtomicBool::new(false),
            last_handshake_at_ms: AtomicU64::new(0),
            resources: Mutex::new(Vec::new()),
        }
    }
}

impl BrowserBridge {
    pub fn initialize(&self, app: AppHandle) -> Result<(), String> {
        #[cfg(windows)]
        {
            let config_dir = app.path().app_config_dir().map_err(|error| {
                format!("Could not find FluxNet's configuration directory: {error}")
            })?;
            std::fs::create_dir_all(&config_dir).map_err(|error| {
                format!("Could not create FluxNet's configuration directory: {error}")
            })?;
            let mut pipe_bytes = [0_u8; 16];
            rand::rng().fill_bytes(&mut pipe_bytes);
            let pipe_name = format!(
                r"\\.\pipe\fluxnet-browser-{}",
                pipe_bytes
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            );
            let host_config = config_dir.join("browser-bridge.json");
            let config_json = serde_json::json!({ "pipeName": pipe_name });
            std::fs::write(
                &host_config,
                serde_json::to_vec(&config_json).map_err(|error| error.to_string())?,
            )
            .map_err(|error| {
                format!("Could not write the browser bridge configuration: {error}")
            })?;
            let app_for_thread = app.clone();
            std::thread::Builder::new()
                .name("fluxnet-browser-bridge".into())
                .spawn(move || {
                    if let Err(error) = run_pipe_server(&pipe_name, app_for_thread) {
                        eprintln!("FluxNet browser bridge stopped: {error}");
                    }
                })
                .map_err(|error| format!("Could not start the browser bridge: {error}"))?;
            app.emit("browser-bridge-status", self.status())
                .map_err(|error| format!("Could not publish browser bridge status: {error}"))?;
        }
        #[cfg(not(windows))]
        let _ = app;
        Ok(())
    }

    fn status(&self) -> BrowserBridgeStatus {
        let (native_host_status, registered_browsers) = Self::native_host_installation();
        let last_handshake_at_ms = self.last_handshake_at_ms.load(Ordering::Acquire);
        let now_ms = current_timestamp_ms();
        let connected = connection_status(
            self.connection_attempted.load(Ordering::Acquire),
            self.connected.load(Ordering::Acquire),
            last_handshake_at_ms,
            now_ms,
        ) == "connected";
        self.connected.store(connected, Ordering::Release);
        BrowserBridgeStatus {
            pairing_code: self.pairing_code.clone(),
            connection_status: connection_status(
                self.connection_attempted.load(Ordering::Acquire),
                connected,
                last_handshake_at_ms,
                now_ms,
            )
            .into(),
            last_handshake_at_ms: (last_handshake_at_ms > 0).then_some(last_handshake_at_ms),
            native_host_status,
            registered_browsers,
        }
    }

    #[cfg(windows)]
    fn native_host_installation() -> (String, Vec<String>) {
        use std::{ffi::OsStr, os::windows::ffi::OsStrExt};
        use windows_sys::Win32::{
            Foundation::ERROR_SUCCESS,
            System::Registry::{
                HKEY, HKEY_CURRENT_USER, KEY_QUERY_VALUE, RegCloseKey, RegOpenKeyExW,
            },
        };

        let host = "com.fluxnet.browser";
        let roots = [
            ("Chrome", "Software\\Google\\Chrome\\NativeMessagingHosts"),
            ("Edge", "Software\\Microsoft\\Edge\\NativeMessagingHosts"),
        ];
        let mut installed = Vec::new();
        for (browser, root) in roots {
            let key_path = format!("{root}\\{host}");
            let wide = OsStr::new(&key_path)
                .encode_wide()
                .chain(Some(0))
                .collect::<Vec<_>>();
            let mut key: HKEY = std::ptr::null_mut();
            let result = unsafe {
                RegOpenKeyExW(
                    HKEY_CURRENT_USER,
                    wide.as_ptr(),
                    0,
                    KEY_QUERY_VALUE,
                    &mut key,
                )
            };
            if result != ERROR_SUCCESS {
                continue;
            }
            let manifest = Self::read_native_host_manifest(key);
            unsafe { RegCloseKey(key) };
            if manifest.is_some_and(|path| Self::native_host_manifest_is_usable(&path)) {
                installed.push(browser.to_owned());
            }
        }
        (
            if installed.is_empty() {
                "notInstalled"
            } else {
                "installed"
            }
            .into(),
            installed,
        )
    }

    #[cfg(windows)]
    fn read_native_host_manifest(
        key: windows_sys::Win32::System::Registry::HKEY,
    ) -> Option<String> {
        use windows_sys::Win32::{
            Foundation::ERROR_SUCCESS,
            System::Registry::{REG_SZ, RegQueryValueExW},
        };

        let mut value_type = 0u32;
        let mut byte_count = 0u32;
        let result = unsafe {
            RegQueryValueExW(
                key,
                std::ptr::null(),
                std::ptr::null(),
                &mut value_type,
                std::ptr::null_mut(),
                &mut byte_count,
            )
        };
        if result != ERROR_SUCCESS || value_type != REG_SZ || byte_count < 2 {
            return None;
        }
        let mut buffer = vec![0u16; byte_count as usize / 2];
        let result = unsafe {
            RegQueryValueExW(
                key,
                std::ptr::null(),
                std::ptr::null(),
                &mut value_type,
                buffer.as_mut_ptr().cast(),
                &mut byte_count,
            )
        };
        if result != ERROR_SUCCESS {
            return None;
        }
        let length = buffer
            .iter()
            .position(|unit| *unit == 0)
            .unwrap_or(buffer.len());
        String::from_utf16(&buffer[..length]).ok()
    }

    #[cfg(windows)]
    fn native_host_manifest_is_usable(path: &str) -> bool {
        let Ok(contents) = std::fs::read_to_string(path) else {
            return false;
        };
        let Ok(manifest) = serde_json::from_str::<serde_json::Value>(&contents) else {
            return false;
        };
        let valid_origins = manifest
            .get("allowed_origins")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|origins| {
                !origins.is_empty()
                    && origins.iter().all(|origin| {
                        origin.as_str().is_some_and(|origin| {
                            let Some(id) = origin
                                .strip_prefix("chrome-extension://")
                                .and_then(|value| value.strip_suffix('/'))
                            else {
                                return false;
                            };
                            id.len() == 32 && id.bytes().all(|byte| (b'a'..=b'p').contains(&byte))
                        })
                    })
            });
        manifest.get("name").and_then(serde_json::Value::as_str) == Some("com.fluxnet.browser")
            && manifest.get("type").and_then(serde_json::Value::as_str) == Some("stdio")
            && manifest
                .get("path")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|executable| std::path::Path::new(executable).is_file())
            && valid_origins
    }

    #[cfg(not(windows))]
    fn native_host_installation() -> (String, Vec<String>) {
        ("unsupported".into(), Vec::new())
    }

    fn add_resource(&self, resource: BrowserMediaResource) -> Result<(), String> {
        let mut resources = self
            .resources
            .lock()
            .map_err(|_| "Browser media history state lock was poisoned.".to_owned())?;
        if resources.iter().any(|entry| entry.url == resource.url) {
            return Ok(());
        }
        resources.push(resource);
        if resources.len() > 100 {
            resources.remove(0);
        }
        Ok(())
    }

    fn resources(&self) -> Result<Vec<BrowserMediaResource>, String> {
        self.resources
            .lock()
            .map(|resources| resources.clone())
            .map_err(|_| "Browser media history state lock was poisoned.".to_owned())
    }

    fn clear_resources(&self) -> Result<(), String> {
        self.resources
            .lock()
            .map(|mut resources| resources.clear())
            .map_err(|_| "Browser media history state lock was poisoned.".to_owned())
    }
}

fn connection_status(
    connection_attempted: bool,
    authenticated: bool,
    last_handshake_at_ms: u64,
    now_ms: u64,
) -> &'static str {
    if authenticated
        && last_handshake_at_ms > 0
        && now_ms.saturating_sub(last_handshake_at_ms) <= CONNECTION_FRESHNESS_MS
    {
        "connected"
    } else if connection_attempted {
        "disconnected"
    } else {
        "awaitingPairing"
    }
}

#[cfg(windows)]
fn copy_windows_clipboard_text(
    value: &str,
    owner: windows_sys::Win32::Foundation::HWND,
) -> Result<(), String> {
    use std::{mem::size_of, ptr};
    use windows_sys::Win32::{
        Foundation::GlobalFree,
        System::{
            DataExchange::{CloseClipboard, EmptyClipboard, OpenClipboard, SetClipboardData},
            Memory::{GMEM_MOVEABLE, GlobalAlloc, GlobalLock, GlobalUnlock},
        },
    };

    let wide = value.encode_utf16().chain(Some(0)).collect::<Vec<_>>();
    let bytes = wide
        .len()
        .checked_mul(size_of::<u16>())
        .ok_or_else(|| "Pairing code is too large for the Windows clipboard.".to_owned())?;
    let memory = unsafe { GlobalAlloc(GMEM_MOVEABLE, bytes) };
    if memory.is_null() {
        return Err(format!(
            "Could not allocate Windows clipboard memory: {}",
            std::io::Error::last_os_error()
        ));
    }
    let buffer = unsafe { GlobalLock(memory) }.cast::<u16>();
    if buffer.is_null() {
        unsafe { GlobalFree(memory) };
        return Err(format!(
            "Could not access Windows clipboard memory: {}",
            std::io::Error::last_os_error()
        ));
    }
    unsafe {
        ptr::copy_nonoverlapping(wide.as_ptr(), buffer, wide.len());
        GlobalUnlock(memory);
    }
    if unsafe { OpenClipboard(owner) } == 0 {
        unsafe { GlobalFree(memory) };
        return Err(format!(
            "Could not open the Windows clipboard: {}",
            std::io::Error::last_os_error()
        ));
    }
    struct ClipboardGuard;
    impl Drop for ClipboardGuard {
        fn drop(&mut self) {
            unsafe { CloseClipboard() };
        }
    }
    let _clipboard_guard = ClipboardGuard;
    if unsafe { EmptyClipboard() } == 0 {
        unsafe { GlobalFree(memory) };
        return Err(format!(
            "Could not clear the Windows clipboard: {}",
            std::io::Error::last_os_error()
        ));
    }
    if unsafe { SetClipboardData(13, memory) }.is_null() {
        unsafe { GlobalFree(memory) };
        return Err(format!(
            "Could not set the Windows clipboard text: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(())
}

#[tauri::command]
pub fn copy_browser_pairing_code(
    app: tauri::AppHandle,
    bridge: tauri::State<'_, BrowserBridge>,
) -> Result<(), String> {
    #[cfg(windows)]
    {
        let window = app.get_webview_window("main").ok_or_else(|| {
            "FluxNet's main window is unavailable for clipboard access.".to_owned()
        })?;
        let owner = window
            .hwnd()
            .map_err(|error| format!("Could not access FluxNet's Windows window: {error}"))?
            .0;
        copy_windows_clipboard_text(&bridge.pairing_code, owner)
    }
    #[cfg(not(windows))]
    {
        let _ = app;
        let _ = bridge;
        Err("Pairing-code clipboard support is available on Windows only.".into())
    }
}

#[tauri::command]
pub fn get_browser_bridge_status(bridge: tauri::State<'_, BrowserBridge>) -> BrowserBridgeStatus {
    bridge.status()
}

#[tauri::command]
pub fn get_browser_media_resources(
    bridge: tauri::State<'_, BrowserBridge>,
) -> Result<Vec<BrowserMediaResource>, String> {
    bridge.resources()
}

#[tauri::command]
pub fn clear_browser_media_resources(
    bridge: tauri::State<'_, BrowserBridge>,
) -> Result<(), String> {
    bridge.clear_resources()
}

fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn validate_resource(resource: BrowserMediaResource) -> Result<BrowserMediaResource, String> {
    if resource.url.len() > MAX_URL_LENGTH {
        return Err("The media URL is too long.".into());
    }
    let url = reqwest::Url::parse(&resource.url).map_err(|_| "The media URL is invalid.")?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err("Only credential-free HTTP or HTTPS media URLs are accepted.".into());
    }
    let path = url.path().to_ascii_lowercase();
    if path.ends_with(".m3u8") || path.ends_with(".mpd") {
        return Err("HLS and DASH manifests are not supported as direct downloads.".into());
    }
    let supported_video_type = matches!(
        resource.media_type.as_str(),
        "mp4" | "webm" | "mov" | "m4v" | "mkv" | "avi" | "ogv" | "mpeg" | "mpg" | "3gp"
    );
    if resource.media_type != "video" && !supported_video_type {
        return Err("The media type is not supported.".into());
    }
    if (supported_video_type && !path.ends_with(&format!(".{}", resource.media_type)))
        || (resource.media_type == "video"
            && !resource
                .mime_type
                .as_deref()
                .is_some_and(|mime_type| mime_type.to_ascii_lowercase().starts_with("video/")))
    {
        return Err("The media type does not match the URL.".into());
    }
    validate_resource_metadata(resource, url)
}

fn validate_download_resource(
    resource: BrowserMediaResource,
) -> Result<BrowserMediaResource, String> {
    if resource.url.len() > MAX_URL_LENGTH {
        return Err("The download URL is too long.".into());
    }
    let url = reqwest::Url::parse(&resource.url).map_err(|_| "The download URL is invalid.")?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err("Only credential-free HTTP or HTTPS download URLs are accepted.".into());
    }
    if resource.media_type.is_empty()
        || resource.media_type.len() > 32
        || !resource.media_type.as_bytes()[0].is_ascii_alphanumeric()
        || !resource
            .media_type
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
    {
        return Err("The browser download file type is invalid.".into());
    }
    if resource
        .mime_type
        .as_ref()
        .is_some_and(|value| value.len() > 128)
        || resource
            .filename
            .as_ref()
            .is_some_and(|value| value.len() > MAX_TEXT_LENGTH)
    {
        return Err("The browser download metadata is too long.".into());
    }
    validate_resource_metadata(resource, url)
}

fn validate_resource_metadata(
    resource: BrowserMediaResource,
    url: reqwest::Url,
) -> Result<BrowserMediaResource, String> {
    if resource.page_title.len() > MAX_TEXT_LENGTH || resource.page_domain.len() > MAX_TEXT_LENGTH {
        return Err("The source page information is too long.".into());
    }
    if resource.filename.as_ref().is_some_and(|filename| {
        filename.len() > MAX_TEXT_LENGTH
            || filename
                .chars()
                .any(|character| matches!(character, '/' | '\\' | '\0'))
            || filename == "."
            || filename == ".."
    }) || resource
        .mime_type
        .as_ref()
        .is_some_and(|mime_type| mime_type.len() > 128)
    {
        return Err("The browser resource metadata is invalid or too long.".into());
    }
    if resource.width.is_some_and(|width| width > 16_384)
        || resource.height.is_some_and(|height| height > 16_384)
        || resource.tab_id.is_some_and(|tab_id| tab_id < 0)
    {
        return Err("The browser resource metadata is outside the accepted range.".into());
    }
    Ok(BrowserMediaResource {
        url: url.to_string(),
        page_title: resource.page_title.trim().to_owned(),
        page_domain: resource.page_domain.trim().to_owned(),
        received_at_ms: current_timestamp_ms(),
        ..resource
    })
}

fn handle_native_message(
    bytes: &[u8],
    bridge: &BrowserBridge,
    app: &AppHandle,
) -> Result<NativeResponse, String> {
    let message = match parse_native_message(bytes, &bridge.pairing_code) {
        Ok(message) => message,
        Err(error) => {
            record_bridge_connection(bridge, app, false)?;
            return Err(error);
        }
    };
    let response = if message.action == "ping" {
        Ok(NativeResponse {
            ok: true,
            message: "FluxNet is connected.".into(),
        })
    } else {
        handle_resource_message(message, bridge, app)
    };
    match response {
        Ok(response) => {
            record_bridge_connection(bridge, app, true)?;
            Ok(response)
        }
        Err(error) => {
            record_bridge_connection(bridge, app, false)?;
            Err(error)
        }
    }
}

fn record_bridge_connection(
    bridge: &BrowserBridge,
    app: &AppHandle,
    authenticated: bool,
) -> Result<(), String> {
    bridge.connection_attempted.store(true, Ordering::Release);
    bridge.connected.store(authenticated, Ordering::Release);
    if authenticated {
        bridge
            .last_handshake_at_ms
            .store(current_timestamp_ms(), Ordering::Release);
    }
    app.emit("browser-bridge-status", bridge.status())
        .map_err(|error| error.to_string())
}

fn handle_resource_message(
    message: NativeMessage,
    bridge: &BrowserBridge,
    app: &AppHandle,
) -> Result<NativeResponse, String> {
    let is_download = message.action == "sendDownload";
    let resource =
        if message.action == "sendMedia" {
            validate_resource(message.resource.ok_or_else(|| {
                "The browser message did not include a media resource.".to_owned()
            })?)?
        } else if is_download {
            validate_download_resource(message.resource.ok_or_else(|| {
                "The browser message did not include a download resource.".to_owned()
            })?)?
        } else if message.action == "reportUnsupported" {
            validate_unsupported_resource(message.resource.ok_or_else(|| {
                "The browser message did not include a media resource.".to_owned()
            })?)?
        } else {
            return Err("The browser action is not supported.".into());
        };
    bridge.add_resource(resource.clone())?;
    app.emit("browser-media-detected", &resource)
        .map_err(|error| format!("Could not send media to FluxNet: {error}"))?;
    if is_download {
        app.emit("browser-download-handoff", &resource)
            .map_err(|error| format!("Could not hand the browser download to FluxNet: {error}"))?;
    }
    Ok(NativeResponse {
        ok: true,
        message: if is_download {
            "Browser download accepted by FluxNet.".into()
        } else if resource.media_type == "hls" || resource.media_type == "dash" {
            "Unsupported manifest reported to FluxNet.".into()
        } else {
            "Media resource sent to FluxNet for review.".into()
        },
    })
}

fn validate_unsupported_resource(
    resource: BrowserMediaResource,
) -> Result<BrowserMediaResource, String> {
    if resource.url.len() > MAX_URL_LENGTH {
        return Err("The media URL is too long.".into());
    }
    let url = reqwest::Url::parse(&resource.url).map_err(|_| "The media URL is invalid.")?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err("Only credential-free HTTP or HTTPS media URLs are accepted.".into());
    }
    let path = url.path().to_ascii_lowercase();
    if !((path.ends_with(".m3u8") && resource.media_type == "hls")
        || (path.ends_with(".mpd") && resource.media_type == "dash"))
    {
        return Err("Only recognized HLS or DASH manifests can be reported as unsupported.".into());
    }
    if resource.page_title.len() > MAX_TEXT_LENGTH || resource.page_domain.len() > MAX_TEXT_LENGTH {
        return Err("The source page information is too long.".into());
    }
    if resource.width.is_some_and(|width| width > 16_384)
        || resource.height.is_some_and(|height| height > 16_384)
    {
        return Err("The media dimensions are outside the accepted range.".into());
    }
    Ok(BrowserMediaResource {
        url: url.to_string(),
        page_title: resource.page_title.trim().to_owned(),
        page_domain: resource.page_domain.trim().to_owned(),
        received_at_ms: current_timestamp_ms(),
        ..resource
    })
}

fn parse_native_message(bytes: &[u8], pairing_code: &str) -> Result<NativeMessage, String> {
    if bytes.len() > MAX_MESSAGE_BYTES {
        return Err("The browser message exceeded the size limit.".into());
    }
    let message: NativeMessage =
        serde_json::from_slice(bytes).map_err(|_| "The browser message is invalid.")?;
    if message.pairing_code != pairing_code {
        return Err(
            "Pairing code did not match. Check the code in FluxNet and the extension.".into(),
        );
    }
    Ok(message)
}

#[cfg(windows)]
fn run_pipe_server(pipe_name: &str, app: AppHandle) -> Result<(), String> {
    use std::{ffi::OsStr, os::windows::ffi::OsStrExt, os::windows::io::FromRawHandle};
    use windows_sys::Win32::{
        Foundation::{ERROR_PIPE_CONNECTED, GetLastError, INVALID_HANDLE_VALUE},
        Storage::FileSystem::FILE_FLAGS_AND_ATTRIBUTES,
        System::Pipes::{
            ConnectNamedPipe, CreateNamedPipeW, DisconnectNamedPipe, PIPE_READMODE_BYTE,
            PIPE_REJECT_REMOTE_CLIENTS, PIPE_TYPE_BYTE, PIPE_WAIT,
        },
    };

    let bridge = app.state::<BrowserBridge>();
    let name: Vec<u16> = OsStr::new(pipe_name).encode_wide().chain(Some(0)).collect();
    loop {
        let handle = unsafe {
            CreateNamedPipeW(
                name.as_ptr(),
                0x00000003 as FILE_FLAGS_AND_ATTRIBUTES,
                PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS,
                1,
                MAX_MESSAGE_BYTES as u32,
                MAX_MESSAGE_BYTES as u32,
                0,
                std::ptr::null(),
            )
        };
        if handle == INVALID_HANDLE_VALUE {
            return Err(std::io::Error::last_os_error().to_string());
        }
        let connected = unsafe { ConnectNamedPipe(handle, std::ptr::null_mut()) } != 0
            || unsafe { GetLastError() } == ERROR_PIPE_CONNECTED;
        if connected {
            let mut stream = unsafe { std::fs::File::from_raw_handle(handle as _) };
            if let Err(error) = process_pipe_message(&mut stream, &bridge, &app) {
                eprintln!("Rejected browser bridge message: {error}");
            }
            drop(stream);
        } else {
            unsafe {
                DisconnectNamedPipe(handle);
                windows_sys::Win32::Foundation::CloseHandle(handle);
            }
        }
    }
}

#[cfg(windows)]
fn process_pipe_message(
    stream: &mut std::fs::File,
    bridge: &BrowserBridge,
    app: &AppHandle,
) -> Result<(), String> {
    use std::io::{Read, Write};
    let mut length_bytes = [0_u8; 4];
    stream
        .read_exact(&mut length_bytes)
        .map_err(|error| error.to_string())?;
    let length = u32::from_le_bytes(length_bytes) as usize;
    if length > MAX_MESSAGE_BYTES {
        return Err("The browser message exceeded the size limit.".into());
    }
    let mut payload = vec![0; length];
    stream
        .read_exact(&mut payload)
        .map_err(|error| error.to_string())?;
    let response = match handle_native_message(&payload, bridge, app) {
        Ok(response) => response,
        Err(message) => NativeResponse { ok: false, message },
    };
    let response = serde_json::to_vec(&response).map_err(|error| error.to_string())?;
    stream
        .write_all(&(response.len() as u32).to_le_bytes())
        .and_then(|_| stream.write_all(&response))
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_state_requires_a_recent_authenticated_request() {
        assert_eq!(
            connection_status(false, false, 0, 100_000),
            "awaitingPairing"
        );
        assert_eq!(connection_status(true, false, 0, 100_000), "disconnected");
        assert_eq!(connection_status(true, true, 80_001, 100_000), "connected");
        assert_eq!(
            connection_status(true, true, 70_000, 100_001),
            "disconnected"
        );
        assert_eq!(connection_status(true, true, 100_001, 100_000), "connected");
    }

    fn resource(url: &str, media_type: &str) -> BrowserMediaResource {
        BrowserMediaResource {
            url: url.into(),
            media_type: media_type.into(),
            page_title: "Example".into(),
            page_domain: "example.com".into(),
            tab_id: None,
            filename: None,
            mime_type: None,
            total_bytes: None,
            observed_bytes: None,
            width: None,
            height: None,
            received_at_ms: 0,
        }
    }

    #[test]
    fn accepts_direct_media_and_preserves_query_parameters() {
        let valid = validate_resource(resource(
            "https://cdn.example/video.mp4?token=short-lived",
            "mp4",
        ))
        .unwrap();
        assert!(valid.url.contains("?token=short-lived"));
    }

    #[test]
    fn accepts_additional_direct_video_formats_and_video_mime_urls() {
        assert!(validate_resource(resource("https://cdn.example/clip.mkv", "mkv")).is_ok());
        let mut extensionless = resource("https://cdn.example/stream?id=8", "video");
        extensionless.mime_type = Some("video/mp4".into());
        assert!(validate_resource(extensionless).is_ok());
    }

    #[test]
    fn validated_media_uses_the_existing_download_filename_path() {
        let valid = validate_resource(resource(
            "https://cdn.example/path/clip.webm?token=short-lived",
            "webm",
        ))
        .unwrap();
        let filename = crate::download_manager::get_download_filename(valid.url).unwrap();
        assert_eq!(filename, "clip.webm");
    }

    #[test]
    fn rejects_credentials_and_manifests() {
        assert!(
            validate_resource(resource("https://user:pass@cdn.example/video.mp4", "mp4")).is_err()
        );
        assert!(validate_resource(resource("https://cdn.example/list.m3u8", "mp4")).is_err());
    }

    #[test]
    fn rejects_non_media_paths() {
        assert!(validate_resource(resource("https://cdn.example/file.zip", "mp4")).is_err());
    }

    #[test]
    fn validates_browser_download_handoffs_without_credentials() {
        let mut download = resource("https://files.example/archive.zip?token=short-lived", "zip");
        download.filename = Some("archive.zip".into());
        download.mime_type = Some("application/zip".into());
        download.total_bytes = Some(4096);
        assert!(
            validate_download_resource(download.clone())
                .unwrap()
                .url
                .contains("?token=short-lived")
        );

        download.url = "https://user:pass@files.example/archive.zip".into();
        assert!(validate_download_resource(download).is_err());

        let mut invalid_name = resource("https://files.example/archive.zip", "zip");
        invalid_name.filename = Some("..\\archive.zip".into());
        assert!(validate_download_resource(invalid_name).is_err());
    }

    #[test]
    fn accepts_manifests_only_as_unsupported_resources() {
        assert!(
            validate_unsupported_resource(resource("https://cdn.example/list.m3u8", "hls")).is_ok()
        );
        assert!(validate_resource(resource("https://cdn.example/list.m3u8", "hls")).is_err());
        assert!(
            validate_unsupported_resource(resource("https://cdn.example/list.mpd", "mp4")).is_err()
        );
    }

    #[test]
    fn validates_pairing_code_and_message_shape() {
        let payload = br#"{"action":"ping","pairingCode":"secret"}"#;
        assert!(parse_native_message(payload, "secret").is_ok());
        assert!(parse_native_message(payload, "other").is_err());
        assert!(parse_native_message(b"not json", "secret").is_err());
    }

    #[test]
    fn retains_a_bounded_deduplicated_resource_snapshot() {
        let bridge = BrowserBridge::default();
        for index in 0..101 {
            bridge
                .add_resource(resource(&format!("https://cdn.example/{index}.mp4"), "mp4"))
                .unwrap();
        }
        bridge
            .add_resource(resource("https://cdn.example/100.mp4", "mp4"))
            .unwrap();
        let resources = bridge.resources().unwrap();
        assert_eq!(resources.len(), 100);
        assert_eq!(resources[0].url, "https://cdn.example/1.mp4");
    }

    #[cfg(windows)]
    #[test]
    fn copies_pairing_code_to_the_windows_clipboard() {
        use windows_sys::Win32::{
            System::DataExchange::{
                CloseClipboard, EmptyClipboard, EnumClipboardFormats, GetClipboardData,
                OpenClipboard,
            },
            System::Memory::{GlobalLock, GlobalSize, GlobalUnlock},
            UI::WindowsAndMessaging::GetDesktopWindow,
        };

        fn clipboard_formats() -> Result<Vec<u32>, String> {
            if unsafe { OpenClipboard(std::ptr::null_mut()) } == 0 {
                return Err(std::io::Error::last_os_error().to_string());
            }
            struct Guard;
            impl Drop for Guard {
                fn drop(&mut self) {
                    unsafe { CloseClipboard() };
                }
            }
            let _guard = Guard;
            let mut formats = Vec::new();
            let mut current = 0;
            loop {
                current = unsafe { EnumClipboardFormats(current) };
                if current == 0 {
                    break;
                }
                formats.push(current);
            }
            Ok(formats)
        }

        fn clipboard_text() -> Option<String> {
            if unsafe { OpenClipboard(std::ptr::null_mut()) } == 0 {
                return None;
            }
            struct Guard;
            impl Drop for Guard {
                fn drop(&mut self) {
                    unsafe { CloseClipboard() };
                }
            }
            let _guard = Guard;
            let memory = unsafe { GetClipboardData(13) };
            if memory.is_null() {
                return None;
            }
            let bytes = unsafe { GlobalSize(memory) };
            let pointer = unsafe { GlobalLock(memory) }.cast::<u16>();
            if pointer.is_null() {
                return None;
            }
            let units = bytes / std::mem::size_of::<u16>();
            let text = unsafe { std::slice::from_raw_parts(pointer, units) };
            let length = text.iter().position(|unit| *unit == 0).unwrap_or(units);
            let result = String::from_utf16(&text[..length]).ok();
            unsafe { GlobalUnlock(memory) };
            result
        }

        let formats = match clipboard_formats() {
            Ok(formats) if formats.iter().all(|format| *format == 13) => formats,
            Ok(_) => {
                eprintln!("Skipping clipboard test to preserve non-text clipboard data.");
                return;
            }
            Err(error) => {
                eprintln!("Skipping clipboard test because the clipboard is unavailable: {error}");
                return;
            }
        };
        let previous_text = clipboard_text();
        if formats.contains(&13) && previous_text.is_none() {
            eprintln!("Skipping clipboard test because existing text could not be preserved.");
            return;
        }
        struct RestoreClipboard(Option<String>);
        impl Drop for RestoreClipboard {
            fn drop(&mut self) {
                let owner = unsafe { GetDesktopWindow() };
                if let Some(text) = &self.0 {
                    if let Err(error) = copy_windows_clipboard_text(text, owner) {
                        eprintln!("Could not restore previous Windows clipboard text: {error}");
                    }
                } else if unsafe { OpenClipboard(owner) } != 0 {
                    unsafe {
                        EmptyClipboard();
                        CloseClipboard();
                    }
                }
            }
        }
        let _restore = RestoreClipboard(previous_text);
        let owner = unsafe { GetDesktopWindow() };
        let value = "fluxnet-clipboard-integration-test";
        copy_windows_clipboard_text(value, owner)
            .expect("text should copy to the Windows clipboard");
        assert_eq!(
            clipboard_text().as_deref(),
            Some(value),
            "the Windows clipboard should contain the copied text"
        );
    }
}
