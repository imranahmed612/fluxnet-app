# FluxNet v0.1.2 Release Notes

**Release Date:** November 21, 2026  
**Build Target:** Windows 10 / 11 (x64)  
**Git Checkpoint:** [`8f73668`](file:///e:/Fluxnet)  
**Installer:** `FluxNet_0.1.2_x64-setup.exe` (2,955,875 bytes, SHA256: `699ED64ADF4F628A4D504EB6C6ECC4DA1D410FD3B853193399A5010838DFDE06`)  
**Application Binary:** `fluxnet.exe` (8,429,056 bytes, FileVersion: `0.1.2`, ProductVersion: `0.1.2`)  
**Native Messaging Host:** `fluxnet_native_host.exe` (298,496 bytes, FileVersion: `0.1.2`, SHA256: `B19CF7BA3B4E9394942C07B814FAF46B30D04BCDF9CDABB0A3C1ACD95132D7FD`)

---

## 1. Release Overview

FluxNet v0.1.2 is an official maintenance and release-hardening update for the FluxNet desktop application and browser integration suite. This release standardizes product branding across the desktop user interface and Windows installation metadata, finalizes the removal of prerelease branding badges, updates the core version manifest, and validates seamless silent in-place upgrades from installed v0.1.1 deployments while preserving user preferences, download history, and Native Messaging Host registrations.

---

## 2. Version and Branding Changes

- **Product Branding Standardization:** Application branding across the user interface, sidebar, Settings page, window title, and Windows setup executable is unified strictly under the name **FluxNet**.
- **Removal of Prerelease Labels:** The legacy `BETA` indicator badge has been completely removed from the sidebar brand component in `src/App.tsx`. In accordance with product requirements, no "Beta", "Stable", or auxiliary release status labels appear in the application or packaging.
- **Synchronized Version Manifests:** Updated version metadata across all project layers to `0.1.2`:
  - `package.json` and `package-lock.json`
  - `src-tauri/Cargo.toml` and `src-tauri/Cargo.lock`
  - `src-tauri/tauri.conf.json`
  - `latest.json` updater release manifest
- **Windows Executable and Installer Metadata:** Compiled release binaries report `FileVersion: 0.1.2`, `ProductVersion: 0.1.2`, and `ProductName: FluxNet`. The Windows uninstall entry in `HKCU\Software\Microsoft\Windows\CurrentVersion\Uninstall\FluxNet` reflects `DisplayVersion: 0.1.2` and `DisplayName: FluxNet`.

---

## 3. Download Manager Improvements (Inherited & Hardened)

FluxNet v0.1.2 retains and verifies the robust download management core established in v0.1.1:

- **Concurrency Pool Management:** Global queue scheduling with configurable concurrent transfer limits (default: 2 active downloads, adjustable up to 4).
- **Bandwidth Regulation:** Timestamp-scheduled bandwidth rate limiting supporting global limits (Mbps) or unthrottled transfers for direct HTTP/HTTPS downloads and segmented streams.
- **Atomic File Staging:** Downloads write into temporary `.part` files and are atomically renamed upon completion and integrity validation, preventing incomplete or corrupted downloads from cluttering the destination directory.
- **Crash-Resilient State:** Persistent state and download history stored in `%APPDATA%\com.administrator.fluxnet\download-history.json` remain fully intact across in-place application updates.
- **Pause, Resume, and Cancellation:** Reliable HTTP byte-range resumption (`Range: bytes=X-`) with `ETag` and `Last-Modified` validation, instant thread termination via atomic task tokens, and safe cleanup of partial artifacts upon cancellation.

---

## 4. Browser Video Detection and Native Messaging

FluxNet connects with Chromium-based browsers (Google Chrome and Microsoft Edge) via a dedicated, hardened Windows Native Messaging Host (`fluxnet_native_host.exe`):

- **Local Named Pipe IPC:** Communication between the browser extension and the desktop application occurs over a per-session randomized named pipe (`\\.\pipe\fluxnet-browser-<token>`), registered via `%APPDATA%\com.administrator.fluxnet\browser-bridge.json`.
- **Extension Allowlisting:** Native Messaging host enforces strict validation of caller origins against the trusted extension identifier (`chrome-extension://clgiddogjgpjllmjgpffopnmighkmhae/`).
- **Two-Way Media Handoff:**
  - `sendMedia`: Detected media resources are transferred to FluxNet's **Video Detection** interface (`VideoDetectionPage`) for review, ensuring no downloads start automatically.
  - `sendDownload`: User-initiated downloads from the extension route directly to FluxNet, prompting for destination selection and queuing into the Download Manager.
  - `ping`: Lightweight IPC connection probe confirming desktop application availability and pipe health.

---

## 5. Security and Reliability

FluxNet enforces defense-in-depth security policies across all subsystems:

- **URL Protocol Allowlisting:** Only credential-free `http://` and `https://` URLs are accepted. Embedded credentials (e.g., `user:pass@`), non-standard URL schemes, and invalid URIs are strictly rejected.
- **Directory Traversal Protection:** Download targets, user-specified destinations, and suggested filenames are sanitized to prevent directory traversal (`..`, null bytes, absolute root overrides, and reserved Windows device names).
- **Process Supervision:** External media discovery helper processes run under a dedicated Windows Job Object (`KillOnJobClose`), guaranteeing immediate termination of worker child processes when tasks finish or cancel.
- **Payload & Manifest Bounds:** Native Messaging message size is strictly capped at 64 KB (`MAX_MESSAGE_BYTES`), HLS playlist manifests are limited to 1 MB (`MAX_HLS_MANIFEST_BYTES`) and 64 segments, and direct buffering is restricted to 2 GB.
- **Data Isolation:** Extractor helper processes run without ambient credential inheritance and operate in isolated scratch directories.

---

## 6. Testing Results

The v0.1.2 release packaging and runtime functionality have passed comprehensive verification:

| Test Area | Target / Check | Result | Evidence / Details |
| :--- | :--- | :---: | :--- |
| **Release Build** | NSIS Installer Packaging | **PASS** | `FluxNet_0.1.2_x64-setup.exe` (2,955,875 B, SHA256: `699ED64ADF...`) |
| **Binary Metadata** | PE Header & Version Verification | **PASS** | `fluxnet.exe` and `fluxnet_native_host.exe` report `FileVersion: 0.1.2`, `ProductVersion: 0.1.2` |
| **In-Place Upgrade** | Silent Upgrade (`/S`) from v0.1.1 | **PASS** | Successfully updated `%LOCALAPPDATA%\FluxNet` without file locks or process errors |
| **Data Preservation** | Roaming Profile Integrity | **PASS** | 100% data preservation: 16 download history items and all user settings preserved intact |
| **Registry Continuity** | Chrome & Edge Native Host Keys | **PASS** | `HKCU\Software\Google\Chrome\NativeMessagingHosts\com.fluxnet.browser` maintained |
| **UI Branding Verification** | Desktop UI Automation Tree | **PASS** | Verified absence of `BETA` badge; sidebar and Settings/About display `v0.1.2` |
| **Native Host IPC** | Host Ping & Media Handoff | **PASS** | Ping returned `{"ok": true, "message": "FluxNet is connected."}`; `sendMedia` accepted |
| **Regression Suites** | Frontend & Rust Test Suites | **PASS** | 46 / 46 frontend tests passed; 142 / 142 Rust tests passed (`cargo test`) |

---

## 7. Installation and Upgrade Information

### System Requirements
- Windows 10 or Windows 11 (64-bit AMD64)
- Google Chrome or Microsoft Edge (for browser extension capabilities)
- Microsoft Visual C++ 2015–2022 Redistributable (x64)

### Upgrading from v0.1.1
1. Close running instances of FluxNet.
2. Run the installer silently or interactively:
   ```cmd
   FluxNet_0.1.2_x64-setup.exe /S
   ```
3. The installer automatically upgrades application executables in `%LOCALAPPDATA%\FluxNet\`.
4. Existing user preferences, download history, and native messaging configurations in `%APPDATA%\com.administrator.fluxnet\` are preserved without manual migration.
5. Launch FluxNet from the Windows Start Menu or `%LOCALAPPDATA%\FluxNet\fluxnet.exe`.

### Browser Extension Configuration
1. Open `chrome://extensions` or `edge://extensions` and enable **Developer mode**.
2. Click **Load unpacked** and select the `browser-extension\` folder.
3. The extension automatically detects the registered native host at:
   `HKCU\Software\Google\Chrome\NativeMessagingHosts\com.fluxnet.browser`

---

## 8. Known Limitations

The following operational constraints and boundaries continue to apply:

- **No Universal Website Compatibility:** Extraction relies on supported media extractors and site structures. Sites that change their DOM structure or deploy anti-bot challenges (e.g., Cloudflare Turnstile, CAPTCHAs) may not permit automated format discovery.
- **DRM-Protected Content:** Media protected by Digital Rights Management (Widevine, PlayReady, FairPlay) is strictly **unsupported**. FluxNet does not circumvent encryption or CDM protection.
- **Authenticated Sessions & Paywalls:** FluxNet does not read or extract private browser session cookies, tokens, or saved credentials. Private, subscriber-only, or paywalled videos cannot be downloaded.
- **Unsupported Manifests & Split-Stream Formats:** Direct DASH manifests (`.mpd`) and elementary video/audio streams requiring external multiplexing are filtered out; only pre-muxed MP4 and WebM progressive streams are supported.
- **Platform Scope:** Native Messaging host registration, Windows Job Object sandboxing, and background monitor services are currently supported on **Windows x64**.
