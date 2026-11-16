# Codebase Concerns & Technical Debt

**Analysis Date:** 2026-11-17

## Technical Debt

### 1. Monolithic Frontend View Component
- **Files:** `src/App.tsx` (3,968 lines), `src/App.css`
- **Issue:** All application views (Dashboard, Network Interfaces, Speed Test, BDIX Analysis, Quality Monitor, Download Manager, Settings, and Browser Pairing) are contained within a single monolithic file.
- **Impact:**
  - High risk of regression when modifying independent tabs.
  - Frequent re-renders of the root component tree during real-time network sampling and download progress ticks.
  - Poor maintainability and developer onboarding friction.
- **Fix Approach:**
  - Decompose `src/App.tsx` into modular feature directories under `src/views/` (e.g. `DashboardView`, `SpeedTestView`, `DownloadsView`, `SettingsView`).
  - Move shared UI widgets (stat cards, icon renderers, progress bars) into `src/components/common/`.
  - Introduce custom React hooks for IPC subscriptions (e.g., `useQualityMonitor`, `useDownloadManager`).

### 2. Large Binary Assets in Source Repository
- **Files:** `experiments/media-engine-poc/` (`yt-dlp-2026.10.26.exe`, `.venv/`, `downloads/mp3/*.mp3`, `phase05-output/*.mp4`, `job_object_harness.exe`)
- **Issue:** Heavy binary executables, virtual environments, and multimedia audio/video fixtures are committed directly into the workspace.
- **Impact:** Inflates repository clone size and violates separation of source code from generated/binary test data.
- **Fix Approach:** Add `experiments/media-engine-poc/.venv/` and large media test outputs to `.gitignore`. Distribute test binaries or fixtures via external artifact storage or automated fetch scripts.

### 3. Isolated Process Supervisor Wiring
- **Files:** `src-tauri/src/process/mod.rs`, `src-tauri/src/media/yt_dlp.rs`, `src-tauri/src/download_manager.rs`
- **Issue:** The process supervisor and Windows Job Object primitives (`src-tauri/src/process/`) are fully tested and functional, but are not yet integrated into `DownloadManager` workers for live media extraction downloads.
- **Impact:** Media streaming downloads remain in phase-spiked status rather than production pipeline.
- **Fix Approach:** Connect `DownloadManager` task execution to `media::yt_dlp` through `process::run_supervised`.

---

## Known Bugs & Environmental Edge Cases

### 1. Windows Startup Registry Access in Restricted Contexts
- **Files:** `src-tauri/src/app_settings.rs`
- **Symptoms:** Unit test `app_settings::tests::windows_startup_registration_can_be_added_and_removed` fails with `Access is denied (os error 5)` when running under non-elevated or restricted sandboxed accounts.
- **Fix Approach:** Mock or stub the registry handle in unit tests, or wrap registry modification in a test fixture that gracefully skips when write access is prohibited.

### 2. PowerShell Script Execution Restrictions
- **Symptoms:** Running `npm` from PowerShell fails if `npm.ps1` is blocked by the Windows execution policy (`Restricted`).
- **Workaround:** Run scripts using `cmd /c npm ...` or direct Node execution (`node --test ...`).

---

## Security Considerations

### 1. Native Messaging Bridge Security
- **Files:** `src-tauri/src/bin/fluxnet_native_host.rs`, `src-tauri/src/browser_bridge.rs`
- **Current State:** The native host verifies that the origin is `chrome-extension://clgiddogjgpjllmjgpffopnmighkmhae/`, and the named pipe address is loaded from `%APPDATA%\com.administrator.fluxnet\browser-bridge.json`.
- **Recommendations:**
  - Maintain strict Windows file ACLs on `%APPDATA%\com.administrator.fluxnet\browser-bridge.json` to prevent local non-admin tampering.
  - Keep the pairing code handshake active for sensitive bridge actions.

### 2. URL & Input Sanitization for Subprocesses
- **Files:** `src-tauri/src/media/yt_dlp.rs`, `browser-extension/media.js`
- **Risk:** Command injection or arbitrary URL execution when passing user-provided or web-intercepted media links to `yt-dlp`.
- **Mitigation:**
  - All arguments are passed as explicit elements in `ProcessSpec.args` without invoking `cmd.exe` or `sh`.
  - URLs are strictly verified: HTTP/HTTPS scheme required, non-ASCII hostnames and credentials rejected.
  - Discovery output is capped at 2 MB to prevent memory exhaustion.

---

## Performance Considerations

### 1. High-Frequency Network Polling
- **Files:** `src-tauri/src/network.rs`
- **Observation:** `GetIfTable2` queries all system adapters every interval. On systems with dozens of virtual adapter interfaces (VPNs, WSL, Docker, Hyper-V), enumerating all adapters can incur measurable CPU overhead.
- **Recommendation:** Cache adapter identifiers and only poll byte counters for the active default interface and connected adapters.
