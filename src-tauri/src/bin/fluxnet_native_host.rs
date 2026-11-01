#[cfg(windows)]
fn main() {
    use serde::Deserialize;
    use std::io::{Read, Write};

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct HostConfig {
        pipe_name: String,
    }

    const MAX_NATIVE_MESSAGE: usize = 64 * 1024;
    const TRUSTED_EXTENSION_ID: &str = "clgiddogjgpjllmjgpffopnmighkmhae";
    let result = (|| -> Result<(), String> {
        let origin = std::env::args_os()
            .nth(1)
            .ok_or_else(|| "The browser extension origin was not supplied.".to_owned())?;
        let origin = origin.to_string_lossy();
        let extension_id = origin
            .strip_prefix("chrome-extension://")
            .and_then(|value| value.strip_suffix('/'))
            .ok_or_else(|| "The Native Messaging caller origin is invalid.".to_owned())?;
        if extension_id != TRUSTED_EXTENSION_ID {
            return Err("The Native Messaging caller is not the FluxNet extension.".into());
        }
        let config_dir = std::env::var_os("APPDATA").ok_or_else(|| {
            "The Windows roaming application data path is unavailable.".to_owned()
        })?;
        let config_path = std::path::PathBuf::from(config_dir)
            .join("com.administrator.fluxnet")
            .join("browser-bridge.json");
        let config_bytes = std::fs::read(config_path).map_err(|error| error.to_string())?;
        if config_bytes.len() > 4096 {
            return Err("FluxNet's browser bridge configuration is invalid.".into());
        }
        let config: HostConfig =
            serde_json::from_slice(&config_bytes).map_err(|error| error.to_string())?;
        let pipe_suffix = config
            .pipe_name
            .strip_prefix(r"\\.\pipe\fluxnet-browser-")
            .ok_or_else(|| "FluxNet's browser bridge pipe name is invalid.".to_owned())?;
        if pipe_suffix.len() != 32
            || !pipe_suffix
                .chars()
                .all(|character| character.is_ascii_hexdigit())
        {
            return Err("FluxNet's browser bridge pipe name is invalid.".into());
        }
        let mut input = std::io::stdin().lock();
        let mut length = [0_u8; 4];
        input
            .read_exact(&mut length)
            .map_err(|error| error.to_string())?;
        let length = u32::from_le_bytes(length) as usize;
        if length == 0 || length > MAX_NATIVE_MESSAGE {
            return Err("Native message length is outside the permitted range.".into());
        }
        let mut browser_message = vec![0; length];
        input
            .read_exact(&mut browser_message)
            .map_err(|error| error.to_string())?;
        let mut message: serde_json::Value = serde_json::from_slice(&browser_message)
            .map_err(|_| "The browser message is invalid.".to_owned())?;
        let message_object = message
            .as_object_mut()
            .ok_or_else(|| "The browser message must be a JSON object.".to_owned())?;
        if !matches!(
            message_object
                .get("action")
                .and_then(serde_json::Value::as_str),
            Some("ping" | "sendMedia" | "sendDownload" | "reportUnsupported")
        ) {
            return Err("The browser action is not supported.".into());
        }
        message_object.insert(
            "extensionId".into(),
            serde_json::Value::String(extension_id.to_owned()),
        );
        let payload = serde_json::to_vec(&message).map_err(|error| error.to_string())?;
        let mut pipe = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(config.pipe_name)
            .map_err(|error| {
                format!("FluxNet is not available. Open FluxNet and try again: {error}")
            })?;
        pipe.write_all(&(payload.len() as u32).to_le_bytes())
            .and_then(|_| pipe.write_all(&payload))
            .map_err(|error| error.to_string())?;
        let mut response_length_bytes = [0_u8; 4];
        pipe.read_exact(&mut response_length_bytes)
            .map_err(|error| error.to_string())?;
        let response_length = u32::from_le_bytes(response_length_bytes) as usize;
        if response_length > MAX_NATIVE_MESSAGE {
            return Err("FluxNet returned an invalid response.".into());
        }
        let mut response = vec![0; response_length];
        pipe.read_exact(&mut response)
            .map_err(|error| error.to_string())?;
        let mut output = std::io::stdout().lock();
        output
            .write_all(&(response.len() as u32).to_le_bytes())
            .and_then(|_| output.write_all(&response))
            .and_then(|_| output.flush())
            .map_err(|error| error.to_string())
    })();
    if let Err(error) = result {
        eprintln!("{error}");
    }
}

#[cfg(not(windows))]
fn main() {
    eprintln!("FluxNet browser Native Messaging is available on Windows only.");
}
