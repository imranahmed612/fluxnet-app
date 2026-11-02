use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use tauri::Manager;

const SETTINGS_FILE: &str = "settings.json";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct AppSettings {
    pub launch_on_startup: bool,
    pub minimize_to_tray: bool,
    pub start_minimized: bool,
    pub theme: String,
    pub default_download_directory: Option<String>,
    pub network_interval_seconds: u64,
    pub ping_timeout_ms: u64,
    pub network_alerts_enabled: bool,
    pub network_alert_failure_threshold: u32,
    pub network_alert_cooldown_seconds: u64,
    pub auto_check_updates: bool,
}

impl Default for AppSettings {
    fn default() -> Self {
        Self {
            launch_on_startup: false,
            minimize_to_tray: true,
            start_minimized: false,
            theme: "dark".into(),
            default_download_directory: None,
            network_interval_seconds: 1,
            ping_timeout_ms: 5000,
            network_alerts_enabled: false,
            network_alert_failure_threshold: 3,
            network_alert_cooldown_seconds: 300,
            auto_check_updates: false,
        }
    }
}

#[derive(Default)]
pub struct AppSettingsState {
    settings: Mutex<AppSettings>,
    path: Mutex<Option<std::path::PathBuf>>,
}

impl AppSettingsState {
    pub fn initialize(&self, app: &tauri::AppHandle) -> Result<(), String> {
        let directory = app
            .path()
            .app_config_dir()
            .map_err(|error| format!("Could not find FluxNet's settings directory: {error}"))?;
        self.initialize_from_path(directory.join(SETTINGS_FILE))
    }

    fn initialize_from_path(&self, path: std::path::PathBuf) -> Result<(), String> {
        let directory = path
            .parent()
            .ok_or_else(|| "FluxNet settings file path has no parent directory.".to_owned())?;
        std::fs::create_dir_all(directory)
            .map_err(|error| format!("Could not create FluxNet's settings directory: {error}"))?;
        let settings = match std::fs::read(&path) {
            Ok(contents) => serde_json::from_slice::<AppSettings>(&contents)
                .map_err(|error| format!("Could not parse saved FluxNet settings: {error}"))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => AppSettings::default(),
            Err(error) => return Err(format!("Could not read saved FluxNet settings: {error}")),
        };
        validate_settings(&settings)?;
        *self
            .settings
            .lock()
            .map_err(|_| "FluxNet settings lock was poisoned.".to_owned())? = settings;
        *self
            .path
            .lock()
            .map_err(|_| "FluxNet settings path lock was poisoned.".to_owned())? = Some(path);
        Ok(())
    }

    pub fn get(&self) -> Result<AppSettings, String> {
        self.settings
            .lock()
            .map(|settings| settings.clone())
            .map_err(|_| "FluxNet settings lock was poisoned.".to_owned())
    }

    fn save(&self, settings: AppSettings) -> Result<(), String> {
        validate_app_settings_value(&settings)?;
        let path = self
            .path
            .lock()
            .map_err(|_| "FluxNet settings path lock was poisoned.".to_owned())?
            .clone()
            .ok_or_else(|| "FluxNet settings storage has not been initialized.".to_owned())?;
        let mut current = self
            .settings
            .lock()
            .map_err(|_| "FluxNet settings lock was poisoned.".to_owned())?;

        #[cfg(windows)]
        let startup_changed = current.launch_on_startup != settings.launch_on_startup;
        #[cfg(windows)]
        if startup_changed {
            set_windows_startup(settings.launch_on_startup)?;
        }

        let contents = serde_json::to_vec_pretty(&settings)
            .map_err(|error| format!("Could not encode FluxNet settings: {error}"))?;
        let temporary = path.with_extension("json.tmp");
        if let Err(error) = std::fs::write(&temporary, contents) {
            let message = format!("Could not write FluxNet settings: {error}");
            #[cfg(windows)]
            if startup_changed {
                return Err(rollback_startup(current.launch_on_startup, message));
            }
            return Err(message);
        }
        if let Err(error) = std::fs::rename(&temporary, &path) {
            let _ = std::fs::remove_file(&temporary);
            let message = format!("Could not commit FluxNet settings: {error}");
            #[cfg(windows)]
            if startup_changed {
                return Err(rollback_startup(current.launch_on_startup, message));
            }
            return Err(message);
        }
        *current = settings;
        Ok(())
    }
}

fn validate_app_settings_value(settings: &AppSettings) -> Result<(), String> {
    validate_settings(settings)?;
    if let Some(directory) = &settings.default_download_directory {
        let path = std::path::Path::new(directory);
        if !path.is_absolute() || !path.is_dir() {
            return Err("Choose an existing folder for the default download directory.".into());
        }
    }
    #[cfg(not(windows))]
    if settings.launch_on_startup {
        return Err("Launching FluxNet on startup is only supported on Windows.".into());
    }
    Ok(())
}

#[cfg(windows)]
fn rollback_startup(previous_value: bool, error: String) -> String {
    match set_windows_startup(previous_value) {
        Ok(()) => error,
        Err(rollback_error) => {
            format!("{error} Startup setting rollback also failed: {rollback_error}")
        }
    }
}

fn validate_settings(settings: &AppSettings) -> Result<(), String> {
    if !["dark", "light", "system"].contains(&settings.theme.as_str()) {
        return Err("Choose Dark, Light, or System for the theme.".into());
    }
    if ![1, 2, 5].contains(&settings.network_interval_seconds) {
        return Err("Choose a network monitoring interval of 1, 2, or 5 seconds.".into());
    }
    if !(500..=5000).contains(&settings.ping_timeout_ms) {
        return Err("Choose a ping timeout from 500 to 5000 milliseconds.".into());
    }
    if !(2..=10).contains(&settings.network_alert_failure_threshold) {
        return Err("Choose a network alert threshold between 2 and 10 failed probes.".into());
    }
    if ![60, 300, 900].contains(&settings.network_alert_cooldown_seconds) {
        return Err("Choose a network alert cooldown of 1, 5, or 15 minutes.".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_app_settings_ranges_and_theme_values() {
        assert!(validate_settings(&AppSettings::default()).is_ok());

        let mut settings = AppSettings::default();
        settings.theme = "system".into();
        settings.network_interval_seconds = 5;
        settings.ping_timeout_ms = 500;
        settings.network_alert_failure_threshold = 10;
        settings.network_alert_cooldown_seconds = 900;
        assert!(validate_settings(&settings).is_ok());

        settings.theme = "auto".into();
        assert!(validate_settings(&settings).is_err());
        settings.theme = "dark".into();
        settings.network_interval_seconds = 3;
        assert!(validate_settings(&settings).is_err());
        settings.network_interval_seconds = 1;
        settings.ping_timeout_ms = 499;
        assert!(validate_settings(&settings).is_err());
    }

    #[test]
    fn older_settings_default_automatic_update_checks_to_off() {
        let settings: AppSettings =
            serde_json::from_str(r#"{"theme":"light"}"#).expect("legacy settings should load");
        assert!(!settings.auto_check_updates);
    }

    #[test]
    fn validates_default_download_directory_before_applying_settings() {
        let mut settings = AppSettings::default();
        settings.default_download_directory = Some(
            std::env::temp_dir()
                .join(format!(
                    "fluxnet-missing-settings-directory-{}-{}",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("system clock should be available")
                        .as_nanos()
                ))
                .join("downloads")
                .display()
                .to_string(),
        );
        assert!(validate_app_settings_value(&settings).is_err());
        settings.default_download_directory = Some(std::env::temp_dir().display().to_string());
        assert!(validate_app_settings_value(&settings).is_ok());
    }

    #[test]
    fn app_settings_serialize_using_stable_frontend_field_names() {
        let value =
            serde_json::to_value(AppSettings::default()).expect("app settings should serialize");
        assert_eq!(value["launchOnStartup"], false);
        assert_eq!(value["defaultDownloadDirectory"], serde_json::Value::Null);
        assert_eq!(value["pingTimeoutMs"], 5000);
    }

    #[test]
    fn app_settings_survive_reinitializing_from_persistent_storage() {
        let directory = std::env::temp_dir().join(format!(
            "fluxnet-settings-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock should be available")
                .as_nanos()
        ));
        let path = directory.join(SETTINGS_FILE);
        let first = AppSettingsState::default();
        first
            .initialize_from_path(path.clone())
            .expect("settings storage should initialize");

        let expected = AppSettings {
            minimize_to_tray: false,
            start_minimized: true,
            theme: "light".into(),
            default_download_directory: Some(std::env::temp_dir().display().to_string()),
            network_interval_seconds: 5,
            ping_timeout_ms: 1_500,
            network_alerts_enabled: true,
            network_alert_failure_threshold: 5,
            network_alert_cooldown_seconds: 900,
            ..AppSettings::default()
        };
        first
            .save(expected.clone())
            .expect("settings should save to persistent storage");

        let restarted = AppSettingsState::default();
        restarted
            .initialize_from_path(path)
            .expect("settings should load after reinitialization");
        assert_eq!(
            restarted.get().expect("settings should be readable"),
            expected
        );

        std::fs::remove_dir_all(directory).expect("temporary settings directory should be removed");
    }

    #[cfg(windows)]
    fn windows_startup_value() -> Result<Option<String>, String> {
        use std::{ffi::OsStr, os::windows::ffi::OsStrExt};
        use windows_sys::Win32::{
            Foundation::{ERROR_FILE_NOT_FOUND, ERROR_SUCCESS},
            System::Registry::{
                HKEY, HKEY_CURRENT_USER, KEY_QUERY_VALUE, REG_SZ, RegCloseKey, RegOpenKeyExW,
                RegQueryValueExW,
            },
        };

        let key_path = OsStr::new("Software\\Microsoft\\Windows\\CurrentVersion\\Run")
            .encode_wide()
            .chain(Some(0))
            .collect::<Vec<_>>();
        let value_name = OsStr::new("FluxNet")
            .encode_wide()
            .chain(Some(0))
            .collect::<Vec<_>>();
        let mut key: HKEY = std::ptr::null_mut();
        let open_result = unsafe {
            RegOpenKeyExW(
                HKEY_CURRENT_USER,
                key_path.as_ptr(),
                0,
                KEY_QUERY_VALUE,
                &mut key,
            )
        };
        if open_result == ERROR_FILE_NOT_FOUND {
            return Ok(None);
        }
        if open_result != ERROR_SUCCESS {
            return Err(format!(
                "Could not inspect FluxNet startup registry value ({open_result})."
            ));
        }

        let mut value_type = 0u32;
        let mut byte_count = 0u32;
        let result = unsafe {
            RegQueryValueExW(
                key,
                value_name.as_ptr(),
                std::ptr::null(),
                &mut value_type,
                std::ptr::null_mut(),
                &mut byte_count,
            )
        };
        if result == ERROR_FILE_NOT_FOUND {
            unsafe { RegCloseKey(key) };
            return Ok(None);
        }
        if result != ERROR_SUCCESS {
            unsafe { RegCloseKey(key) };
            return Err(format!(
                "Could not query FluxNet startup registry value ({result})."
            ));
        }
        if value_type != REG_SZ || byte_count < 2 {
            unsafe { RegCloseKey(key) };
            return Err("FluxNet startup registry value has an unsupported type.".into());
        }
        let mut buffer = vec![0u16; byte_count as usize / 2];
        let result = unsafe {
            RegQueryValueExW(
                key,
                value_name.as_ptr(),
                std::ptr::null(),
                &mut value_type,
                buffer.as_mut_ptr().cast(),
                &mut byte_count,
            )
        };
        unsafe { RegCloseKey(key) };
        if result != ERROR_SUCCESS {
            return Err(format!(
                "Could not read FluxNet startup registry value ({result})."
            ));
        }
        let length = buffer
            .iter()
            .position(|unit| *unit == 0)
            .unwrap_or(buffer.len());
        String::from_utf16(&buffer[..length])
            .map(Some)
            .map_err(|error| format!("FluxNet startup registry value is not valid UTF-16: {error}"))
    }

    #[cfg(windows)]
    #[test]
    fn windows_startup_registration_can_be_added_and_removed() {
        if windows_startup_value()
            .expect("startup registry state should be readable")
            .is_some()
        {
            eprintln!("Skipping startup registry test to preserve the existing FluxNet Run value.");
            return;
        }

        struct RemoveTemporaryStartupRegistration(String);
        impl Drop for RemoveTemporaryStartupRegistration {
            fn drop(&mut self) {
                if windows_startup_value().ok().flatten().as_deref() == Some(&self.0) {
                    let _ = set_windows_startup(false);
                }
            }
        }
        let expected = format!(
            "\"{}\"",
            std::env::current_exe()
                .expect("test executable path should be available")
                .display()
        );
        let cleanup = RemoveTemporaryStartupRegistration(expected.clone());

        set_windows_startup(true).expect("startup registration should be added");
        assert!(
            windows_startup_value()
                .expect("startup registry should be readable")
                .as_deref()
                == Some(expected.as_str()),
            "FluxNet Run value should point to the current executable after registration"
        );
        if windows_startup_value()
            .expect("startup registry should be readable")
            .as_deref()
            == Some(expected.as_str())
        {
            set_windows_startup(false).expect("startup registration should be removed");
        }
        assert!(
            windows_startup_value()
                .expect("startup registry should be readable")
                .is_none(),
            "FluxNet Run value should be absent after removal"
        );
        drop(cleanup);
    }
}

#[tauri::command]
pub fn get_app_settings(
    settings: tauri::State<'_, AppSettingsState>,
) -> Result<AppSettings, String> {
    settings.get()
}

#[tauri::command]
pub fn validate_app_settings(value: AppSettings) -> Result<(), String> {
    validate_app_settings_value(&value)
}

#[tauri::command]
pub fn save_app_settings(
    settings: tauri::State<'_, AppSettingsState>,
    value: AppSettings,
) -> Result<(), String> {
    settings.save(value)
}

#[cfg(windows)]
fn set_windows_startup(enabled: bool) -> Result<(), String> {
    use std::{ffi::OsStr, mem::size_of, os::windows::ffi::OsStrExt};
    use windows_sys::Win32::{
        Foundation::{ERROR_FILE_NOT_FOUND, ERROR_SUCCESS},
        System::Registry::{
            HKEY, HKEY_CURRENT_USER, KEY_SET_VALUE, REG_OPTION_NON_VOLATILE, REG_SZ, RegCloseKey,
            RegCreateKeyExW, RegDeleteValueW, RegSetValueExW,
        },
    };

    const RUN_KEY: &str = "Software\\Microsoft\\Windows\\CurrentVersion\\Run";
    const VALUE_NAME: &str = "FluxNet";
    let startup_command = if enabled {
        let executable = std::env::current_exe()
            .map_err(|error| format!("Could not locate FluxNet's executable: {error}"))?;
        Some(format!("\"{}\"", executable.display()))
    } else {
        None
    };
    let key_path = OsStr::new(RUN_KEY)
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();
    let value_name = OsStr::new(VALUE_NAME)
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();
    let mut key: HKEY = std::ptr::null_mut();
    let result = unsafe {
        RegCreateKeyExW(
            HKEY_CURRENT_USER,
            key_path.as_ptr(),
            0,
            std::ptr::null(),
            REG_OPTION_NON_VOLATILE,
            KEY_SET_VALUE,
            std::ptr::null(),
            &mut key,
            std::ptr::null_mut(),
        )
    };
    if result != ERROR_SUCCESS {
        return Err(format!(
            "Could not open the Windows startup registry key (error {result})."
        ));
    }
    let operation = if enabled {
        let data = OsStr::new(startup_command.as_deref().unwrap_or_default())
            .encode_wide()
            .chain(Some(0))
            .collect::<Vec<_>>();
        unsafe {
            RegSetValueExW(
                key,
                value_name.as_ptr(),
                0,
                REG_SZ,
                data.as_ptr().cast(),
                (data.len() * size_of::<u16>()) as u32,
            )
        }
    } else {
        unsafe { RegDeleteValueW(key, value_name.as_ptr()) }
    };
    unsafe { RegCloseKey(key) };
    if operation != ERROR_SUCCESS && !(operation == ERROR_FILE_NOT_FOUND && !enabled) {
        return Err(format!(
            "Could not update FluxNet's Windows startup setting (error {operation})."
        ));
    }
    Ok(())
}
