# External & System Integrations

**Analysis Date:** 2026-11-17

## Overview
FluxNet relies on multi-layer system integrations across native Windows APIs, inter-process communication pipelines (Tauri IPC, Native Messaging, Named Pipes), and external CLI execution engines.

---

## 1. Operating System APIs (Windows Win32)

### IP Helper API (`Win32_NetworkManagement_IpHelper`)
- **Files:** `src-tauri/src/network.rs`
- **Purpose:** Discovers physical and virtual network adapters, IP addresses (IPv4 / IPv6), default gateways, DNS servers, operational status, interface metrics, and real-time octet throughput.
- **Key Functions:**
  - `GetAdaptersAddresses` — enumerates interface addresses and adapter configuration.
  - `GetIfEntry2` / `GetIfTable2` — polls cumulative bytes transmitted and received for real-time speed calculation.

### WLAN API (`Win32_NetworkManagement_WiFi`)
- **Files:** `src-tauri/src/network.rs`
- **Purpose:** Extracts wireless network telemetry on Windows without external tools.
- **Key Functions:**
  - `WlanOpenHandle`, `WlanEnumInterfaces`, `WlanQueryInterface`
  - Fetches SSID, link signal quality percentage (0-100%), channel frequency in MHz, and link speeds (RX/TX Mbps).

### Windows Job Objects (`Win32_System_JobObjects`)
- **Files:** `src-tauri/src/process/windows_job.rs`, `src-tauri/src/process/supervisor.rs`
- **Purpose:** Guarantees leak-proof child process lifecycle management.
- **Implementation:**
  - Creates a dedicated Win32 Job Object per process task with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`.
  - When the FluxNet supervisor drops or the application closes/crashes, the Windows kernel automatically terminates all child and grandchild processes immediately.
  - Verified by C++ harness (`experiments/media-engine-poc/job_object_harness.cpp`) and Rust tests (`src-tauri/src/process/tests.rs`).

---

## 2. Inter-Process Communication (IPC)

### A. Tauri IPC (Frontend <-> Backend)
- **Frontend File:** `src/App.tsx`, `src/browserHandoff.mjs`
- **Backend File:** `src-tauri/src/lib.rs`
- **Mechanisms:**
  - `invoke('<command_name>', args)` — Strong-typed command invocation for settings, network tests, download actions, and monitor controls.
  - `listen('<event_name>', callback)` — Push events emitted from Rust background threads (e.g. `quality-monitor-sample`, `download-progress`, `browser-bridge-status`, `navigate-to-download-manager`).

### B. Chrome Native Messaging Host
- **Host Binary:** `fluxnet_native_host.exe` (`src-tauri/src/bin/fluxnet_native_host.rs`)
- **Extension Script:** `browser-extension/background.js`
- **Registry Config:** Registered via `browser-extension/install-native-host.ps1` under `HKCU:\Software\Google\Chrome\NativeMessagingHosts\com.administrator.fluxnet`.
- **Protocol:**
  - Reads 4-byte little-endian length prefix followed by JSON payload over `stdin`.
  - Enforces origin verification: caller MUST match extension ID `clgiddogjgpjllmjgpffopnmighkmhae`.
  - Reads pipe name from `%APPDATA%\com.administrator.fluxnet\browser-bridge.json`.
  - Relays payload to the Windows Named Pipe.

### C. Windows Named Pipe Bridge
- **Backend File:** `src-tauri/src/browser_bridge.rs`
- **Pipe Address:** `\\.\pipe\fluxnet-browser-<32-hex-characters>`
- **Security:**
  - Uses randomly generated pipe name suffix per session.
  - Requires pairing code / security token validation for browser bridge commands.
  - Relays captured media resources (HLS playlists, direct video URLs, audio tracks) into FluxNet's state.

---

## 3. External CLI Engines & Tools

### `yt-dlp` Media Discovery & Download
- **Files:** `src-tauri/src/media/yt_dlp.rs`, `experiments/media-engine-poc/`
- **Execution Pattern:**
  - Executed via `std::process::Command` with Windows Job Object attachment.
  - Runs in a strict sandbox temporary runtime folder (`RuntimeTempDir`) to prevent workspace pollution.
  - Flags enforced: `--dump-single-json`, `--no-download`, `--flat-playlist`, `--socket-timeout 15`.
  - Limits: 60-second discovery timeout, 2MB max metadata size, strict URL scheme validation (`http://`, `https://` only, rejects non-HTTP and credential-bearing URLs).

### `ffmpeg` Media Demuxing & Remuxing
- **Files:** `experiments/media-engine-poc/phase05-output/ffmpeg-audit/`, `experiments/media-engine-poc/phase05_local_media_validation.py`
- **Purpose:** Audio extraction (MP3 / AAC / M4A), stream copy remuxing (`-c copy`), and HLS/DASH segment recombination.

---

## 4. Network Services & External Endpoints

### Connectivity & BDIX Diagnostics
- **Files:** `src-tauri/src/network.rs`
- **Targets:**
  - Default BDIX gateway and DNS probe targets.
  - Configurable custom target hosts and fallback DNS probes (`1.1.1.1`, `8.8.8.8`).

### Speed Testing Engine
- **Files:** `src-tauri/src/speed_test.rs`
- **Protocol:** Multi-stream HTTP chunked transfer and latency ping measuring bandwidth and jitter.

### Application Auto-Updater
- **Files:** `src-tauri/src/updater.rs`, `latest.json`
- **Integration:** `@tauri-apps/plugin-updater`
- **Endpoint:** Reads updater metadata from configured remote manifest endpoint with cryptographic signature validation.
