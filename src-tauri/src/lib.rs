mod app_settings;
mod browser_bridge;
mod download_manager;
mod network;
mod speed_test;

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tauri::{
    Emitter, Manager, RunEvent,
    menu::{Menu, MenuItem, PredefinedMenuItem},
    tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent},
};

#[cfg(windows)]
fn show_main_window(app: &tauri::AppHandle) {
    let Some(window) = app.get_webview_window("main") else {
        eprintln!("Could not restore FluxNet: the main window was not found.");
        return;
    };
    if let Err(error) = window.unminimize() {
        eprintln!("Could not unminimize the FluxNet window: {error}");
    }
    if let Err(error) = window.show() {
        eprintln!("Could not show the FluxNet window: {error}");
        return;
    }
    if let Err(error) = window.set_focus() {
        eprintln!("Could not focus the FluxNet window: {error}");
    }
}

fn request_exit(app: &tauri::AppHandle, exit_started: &AtomicBool) {
    if exit_started.swap(true, Ordering::AcqRel) {
        return;
    }
    let monitor_running =
        network::request_quality_monitor_stop(&app.state::<network::QualityMonitorControl>());
    let (download_workers, _) = match download_manager::cancel_all_downloads(
        &app.state::<download_manager::DownloadManager>(),
    ) {
        Ok(result) => result,
        Err(error) => {
            eprintln!("Could not checkpoint downloads during exit: {error}");
            app.exit(1);
            return;
        }
    };
    if !monitor_running.load(Ordering::Acquire) && download_workers.load(Ordering::Acquire) == 0 {
        app.exit(0);
        return;
    }

    let app = app.clone();
    std::thread::spawn(move || {
        while monitor_running.load(Ordering::Acquire)
            || download_workers.load(Ordering::Acquire) > 0
        {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        app.exit(0);
    });
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let exit_started = Arc::new(AtomicBool::new(false));
    let exit_started_for_setup = Arc::clone(&exit_started);
    let exit_started_for_run = Arc::clone(&exit_started);

    let builder = tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_notification::init())
        .manage(speed_test::SpeedTestControl::default())
        .manage(network::BdixAnalysisControl::default())
        .manage(network::QualityMonitorControl::default())
        .manage(download_manager::DownloadManager::default())
        .manage(browser_bridge::BrowserBridge::default())
        .manage(app_settings::AppSettingsState::default())
        .setup(move |app| {
            app.state::<app_settings::AppSettingsState>()
                .initialize(app.handle())
                .map_err(std::io::Error::other)?;
            let saved_settings = app
                .state::<app_settings::AppSettingsState>()
                .get()
                .map_err(std::io::Error::other)?;
            network::apply_saved_quality_preferences(
                &app.state::<network::QualityMonitorControl>(),
                saved_settings.network_interval_seconds,
                saved_settings.ping_timeout_ms,
                network::QualityAlertSettings {
                    enabled: saved_settings.network_alerts_enabled,
                    failure_threshold: saved_settings.network_alert_failure_threshold,
                    cooldown_seconds: saved_settings.network_alert_cooldown_seconds,
                },
            )
            .map_err(std::io::Error::other)?;
            app.state::<download_manager::DownloadManager>()
                .initialize(app.handle())
                .map_err(std::io::Error::other)?;
            app.state::<browser_bridge::BrowserBridge>()
                .initialize(app.handle().clone())
                .map_err(std::io::Error::other)?;
            #[cfg(windows)]
            {
                let open = MenuItem::with_id(app, "tray_open", "Open FluxNet", true, None::<&str>)?;
                let start = MenuItem::with_id(
                    app,
                    "tray_start_monitoring",
                    "Start Monitoring",
                    true,
                    None::<&str>,
                )?;
                let stop = MenuItem::with_id(
                    app,
                    "tray_stop_monitoring",
                    "Stop Monitoring",
                    true,
                    None::<&str>,
                )?;
                let downloads = MenuItem::with_id(
                    app,
                    "tray_downloads",
                    "Open Download Manager",
                    true,
                    None::<&str>,
                )?;
                let separator = PredefinedMenuItem::separator(app)?;
                let exit = MenuItem::with_id(app, "tray_exit", "Exit FluxNet", true, None::<&str>)?;
                let menu =
                    Menu::with_items(app, &[&open, &downloads, &start, &stop, &separator, &exit])?;

                let app_for_click = app.handle().clone();
                let exit_for_menu = Arc::clone(&exit_started_for_setup);
                let tray = TrayIconBuilder::new()
                    .icon(
                        app.default_window_icon()
                            .cloned()
                            .ok_or("FluxNet has no default window icon for its system tray.")?,
                    )
                    .tooltip("FluxNet — Network Quality Monitor")
                    .menu(&menu)
                    .show_menu_on_left_click(false)
                    .on_menu_event(move |app, event| match event.id().as_ref() {
                        "tray_open" => show_main_window(app),
                        "tray_downloads" => {
                            show_main_window(app);
                            if let Err(error) = app.emit("navigate-to-download-manager", ()) {
                                eprintln!("Could not open the Download Manager page: {error}");
                            }
                        }
                        "tray_start_monitoring" => {
                            if let Err(error) = network::start_saved_quality_monitor(app.clone()) {
                                eprintln!("Could not start monitoring from the tray: {error}");
                            }
                        }
                        "tray_stop_monitoring" => {
                            if let Err(error) = network::stop_saved_quality_monitor(app) {
                                eprintln!("Could not stop monitoring from the tray: {error}");
                            }
                        }
                        "tray_exit" => request_exit(app, &exit_for_menu),
                        _ => {}
                    })
                    .on_tray_icon_event(move |_tray, event| {
                        if matches!(
                            event,
                            TrayIconEvent::Click {
                                button: MouseButton::Left,
                                button_state: MouseButtonState::Up,
                                ..
                            }
                        ) {
                            show_main_window(&app_for_click);
                        }
                    })
                    .build(app)?;
                if !app.manage(tray) {
                    return Err("Could not retain the FluxNet system tray icon.".into());
                }

                let window = app
                    .get_webview_window("main")
                    .ok_or("FluxNet's main window was not found during startup.")?;
                let window_to_hide = window.clone();
                let app_handle = app.handle().clone();
                let exit_for_close = Arc::clone(&exit_started_for_setup);
                window.on_window_event(move |event| {
                    if let tauri::WindowEvent::CloseRequested { api, .. } = event {
                        match app_handle.state::<app_settings::AppSettingsState>().get() {
                            Ok(settings) if settings.minimize_to_tray => {
                                api.prevent_close();
                                if let Err(error) = window_to_hide.hide() {
                                    eprintln!("Could not hide FluxNet to the system tray: {error}");
                                }
                            }
                            Ok(_) => {
                                api.prevent_close();
                                if let Err(error) = window_to_hide.hide() {
                                    eprintln!("Could not hide FluxNet before exiting: {error}");
                                }
                                request_exit(&app_handle, &exit_for_close);
                            }
                            Err(error) => {
                                api.prevent_close();
                                eprintln!("Could not read the close behavior setting: {error}");
                            }
                        }
                    }
                });
                let settings = app
                    .state::<app_settings::AppSettingsState>()
                    .get()
                    .map_err(std::io::Error::other)?;
                if settings.start_minimized {
                    window.hide().map_err(std::io::Error::other)?;
                }
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            app_settings::get_app_settings,
            app_settings::validate_app_settings,
            app_settings::save_app_settings,
            network::get_network_interfaces,
            network::test_connectivity,
            network::analyze_bdix_target,
            network::cancel_bdix_analysis,
            network::start_quality_monitor,
            network::stop_quality_monitor,
            network::clear_quality_monitor_history,
            network::export_quality_monitor_history,
            network::configure_quality_alerts,
            network::configure_quality_monitor_options,
            network::configure_quality_monitor_preferences,
            network::get_quality_monitor_snapshot,
            speed_test::run_speed_test,
            speed_test::cancel_speed_test,
            download_manager::get_downloads,
            download_manager::get_download_settings,
            download_manager::get_download_filename,
            download_manager::configure_download_limits,
            download_manager::configure_download_notifications,
            download_manager::configure_download_notification_preferences,
            download_manager::start_download,
            download_manager::pause_download,
            download_manager::resume_download,
            download_manager::cancel_download,
            download_manager::retry_download,
            download_manager::open_download_containing_folder,
            download_manager::clear_completed_downloads,
            browser_bridge::get_browser_bridge_status,
            browser_bridge::copy_browser_pairing_code,
            browser_bridge::get_browser_media_resources,
            browser_bridge::clear_browser_media_resources
        ]);
    let app = match builder.build(tauri::generate_context!()) {
        Ok(app) => app,
        Err(error) => {
            eprintln!("Could not start FluxNet: {error}");
            return;
        }
    };
    app.run(move |app_handle, event| {
        if let RunEvent::ExitRequested { api, .. } = event {
            if !exit_started_for_run.load(Ordering::Acquire) {
                api.prevent_exit();
                request_exit(app_handle, &exit_started_for_run);
            }
        }
    });
}
