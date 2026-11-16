# Technology Stack

**Analysis Date:** 2026-11-17

## Overview
FluxNet is a high-performance desktop network utility and download manager built with a **Tauri 2** (Rust) core, a **React 19** frontend, and a companion **Manifest V3 Browser Extension** communicating via Windows Native Messaging and Named Pipes.

---

## Languages & Runtimes

| Language / Runtime | Version / Target | Primary Location | Purpose |
| :--- | :--- | :--- | :--- |
| **Rust** | Edition 2024 (MSVC x86_64) | `src-tauri/` | Desktop backend, Windows OS integration, process supervision, download engine, native host |
| **TypeScript** | `~6.0.3` | `src/` | Frontend desktop application logic, state, and type definitions |
| **JavaScript (ESM / MJS)** | Node.js 20+ / Chrome MV3 | `browser-extension/`, `tests/` | Browser extension content/background scripts, extension build pipeline, handoff tests |
| **C++** | MSVC (C++17) | `experiments/media-engine-poc/` | Test harnesses for Windows Job Object validation (`job_object_harness.cpp`) |
| **Python** | 3.12+ (virtualenv) | `experiments/media-engine-poc/` | Media engine discovery proof-of-concept (`poc.py`, `local_fixture_test.py`) |
| **PowerShell** | Windows PowerShell 5.1 / 7+ | `browser-extension/*.ps1` | Native host registry installation and validation scripts |

---

## Core Frameworks & Libraries

### Desktop Frontend
- **React**: `19.1.0` (`react`, `react-dom`)
- **Vite**: `^8.0.16` (`vite`, `@vitejs/plugin-react` `^6.0.2`)
- **Tauri Frontend API**: `@tauri-apps/api` `^2`, `@tauri-apps/plugin-dialog` `^2`, `@tauri-apps/plugin-opener` `^2`, `@tauri-apps/plugin-process` `^2.4.0`, `@tauri-apps/plugin-updater` `^2.14.1`
- **Styling**: Vanilla scoped CSS / Flexbox / Grid (`src/App.css`)

### Desktop Backend (Rust / Tauri Core)
- **Tauri Core**: `2` (with `tray-icon` feature)
- **Async Runtime**: `tokio` `1` (`features = ["fs", "io-util", "macros", "sync", "time"]`)
- **HTTP Client**: `reqwest` `0.12` (`features = ["blocking", "rustls-tls-native-roots", "stream", "system-proxy"]`)
- **Futures & Concurrency**: `futures-util` `0.3`
- **Serialization**: `serde` `1` (with `derive`), `serde_json` `1`
- **Time & Utilities**: `chrono` `0.4`, `rand` `0.9`

### Windows Platform Bindings
- **`windows-sys`**: `0.61`
  - `Win32_Foundation`
  - `Win32_Security`
  - `Win32_NetworkManagement_IpHelper` (network interfaces, metrics, routing tables)
  - `Win32_NetworkManagement_Ndis`
  - `Win32_NetworkManagement_WiFi` (WLAN SSID, RSSI signal quality, channel frequencies)
  - `Win32_Storage_FileSystem`
  - `Win32_System_IO`
  - `Win32_System_JobObjects` (process tree isolation and leak-proof cleanup)
  - `Win32_System_Pipes` (named pipe IPC for browser extension)
  - `Win32_System_Threading`
  - `Win32_System_Registry`
  - `Win32_System_DataExchange`
  - `Win32_System_Memory`
  - `Win32_Networking_WinSock`
  - `Win32_UI_WindowsAndMessaging`
- **`windows` (dev dependency)**: `0.62.2` (`Win32_Media_MediaFoundation`)

---

## Browser Extension (Manifest V3)
- **Specification**: Chrome Manifest V3 (`browser-extension/manifest.json`)
- **ID**: `clgiddogjgpjllmjgpffopnmighkmhae` (allowlisted for native messaging)
- **Native Host**: `com.administrator.fluxnet` (`fluxnet_native_host.exe`)
- **Modules**:
  - `browser-extension/background.js` — Service worker, native messaging client, tab event orchestration
  - `browser-extension/media.js` — HLS/DASH manifest parsing, direct media sniffers, CDN sanitizers
  - `browser-extension/content.js` & `browser-extension/download-gesture.js` — DOM click listeners and user intent tracking
  - `browser-extension/video-sources.js` — HTML5 video player inspection

---

## Configuration Files

| File | Purpose |
| :--- | :--- |
| `package.json` | Root scripts, Node test runners, npm dependencies |
| `vite.config.ts` | Frontend bundle configuration, React plugin, dev server |
| `tsconfig.json` & `tsconfig.node.json` | TypeScript compiler configuration (strict mode, Node target) |
| `src-tauri/Cargo.toml` | Rust crate manifest, dependencies, Windows sys feature flags, release profile |
| `src-tauri/tauri.conf.json` | Tauri application configuration (window sizing, updater endpoints, tray icon) |
| `browser-extension/manifest.json` | Extension permissions, content script declarations, background service worker |
| `browser-extension/build.mjs` | Packaging and bundling script for the Chrome extension |

---

## Build & Release Pipeline

- **Frontend Dev**: `npm run dev` (`vite`)
- **Frontend Build**: `npm run build` (`tsc && vite build`)
- **Extension Build**: `npm run build:extension` (`node browser-extension/build.mjs`)
- **Native Host Build**: `cargo build --manifest-path src-tauri/Cargo.toml --release --bin fluxnet_native_host`
- **Desktop Application Build**: `npm run tauri:build`
- **Cargo Release Optimizations**:
  - `codegen-units = 1`
  - `lto = true`
  - `opt-level = 3`
  - `panic = "abort"`
  - `strip = true`
