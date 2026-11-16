# Repository Structure & Directory Organization

**Analysis Date:** 2026-11-17

## Directory Tree

```
E:\Fluxnet\
├── .planning/                              # GSD planning and codebase intelligence
│   └── codebase/                           # 7 structured codebase map documents
├── .vscode/                                # IDE workspace settings
├── artifacts/                              # Build artifacts and diagnostic outputs
├── browser-extension/                      # Chrome / Edge Manifest V3 companion extension
│   ├── icons/                              # Extension action icons (16, 32, 48, 128 px)
│   ├── tests/                              # Extension test suite
│   │   ├── extension-build.test.mjs        # Packaging & manifest integrity tests
│   │   └── media.test.mjs                  # HLS/DASH/video detection unit tests
│   ├── background.js                       # Service worker & native messaging client
│   ├── build.mjs                           # Reproducible extension build pipeline
│   ├── content.js                          # Content script injecting DOM observers
│   ├── download-gesture.js                 # User click & interaction gesture tracker
│   ├── install-native-host.ps1             # PowerShell script to register Native Host in registry
│   ├── manifest.json                       # Extension manifest specification (MV3)
│   ├── media.js                            # Media URL, HLS, DASH, and streaming sniffer
│   ├── popup.css / popup.html / popup.js   # Extension popup interface
│   ├── prompt.html / prompt.js             # Extension download confirmation modal
│   ├── uninstall-native-host.ps1           # Native Host registry cleanup script
│   └── video-sources.js                    # HTML5 video element inspect logic
├── dist/                                   # Compiled Vite frontend distribution bundle
├── experiments/                            # Experimental PoCs and research spikes
│   └── media-engine-poc/                   # yt-dlp / ffmpeg / Windows Job Object spikes
│       ├── acquired/                       # Scripts & validation assets
│       ├── downloads/                      # Sample downloaded streams
│       ├── phase05-output/                 # ffmpeg audit & media validation artifacts
│       ├── test-tools/                     # Pinned standalone binaries (yt-dlp)
│       ├── job_object_harness.cpp          # C++ Win32 Job Object verification harness
│       ├── local_fixture_test.py           # Python local media fixture runner
│       ├── phase05_feature_audit.py        # Feature & format support scanner
│       ├── poc.py                          # Initial Python media extraction PoC
│       ├── process_lifecycle_test.py       # Child process termination audit
│       └── requirements.txt                # Python virtualenv dependencies
├── public/                                 # Static assets served by Vite
├── src/                                    # Desktop frontend application source (React 19)
│   ├── assets/                             # Logos, branding icons, symbols
│   ├── App.css                             # Primary stylesheet (layout, tabs, themes, animations)
│   ├── App.tsx                             # Main monolithic React desktop view component (~3,970 lines)
│   ├── browserHandoff.d.mts                # Type declarations for browser handoff helpers
│   ├── browserHandoff.mjs                  # Window activation & download dialog coordinator
│   ├── main.tsx                            # React application entry point (DOM mount)
│   └── vite-env.d.ts                       # Vite client ambient types
├── src-tauri/                              # Desktop native backend (Tauri 2 / Rust)
│   ├── src/                                # Rust source code
│   │   ├── bin/
│   │   │   └── fluxnet_native_host.rs      # Native Messaging Host binary for browser extension
│   │   ├── media/                          # Media extraction & parsing
│   │   │   ├── mod.rs                      # Media module exports
│   │   │   └── yt_dlp.rs                   # yt-dlp metadata runner with sandbox temp dir
│   │   ├── process/                        # Windows Process Supervision & Job Objects
│   │   │   ├── mod.rs                      # Module declarations & public re-exports
│   │   │   ├── supervisor.rs               # Stream monitoring & outcome classification
│   │   │   ├── tests.rs                    # Unit & integration tests for job objects
│   │   │   ├── windows_job.rs              # Win32 Job Object wrapper with kill-on-close
│   │   │   └── windows_spawn.rs            # Suspended process spawn adapter
│   │   ├── app_settings.rs                 # User preferences & registry startup manager
│   │   ├── browser_bridge.rs               # Windows Named Pipe server & pairing security
│   │   ├── download_manager.rs             # Segmented multi-connection download engine
│   │   ├── lib.rs                          # Tauri setup, command handlers, tray & window events
│   │   ├── main.rs                         # Rust executable entry point (`fluxnet_lib::run()`)
│   │   ├── network.rs                      # Win32 IP Helper & WLAN monitoring implementation
│   │   ├── speed_test.rs                   # Throughput & latency benchmark engine
│   │   └── updater.rs                      # Cryptographic update validator
│   ├── Cargo.toml                          # Rust dependencies & Windows sys feature flags
│   ├── Cargo.lock                          # Dependency lockfile
│   └── tauri.conf.json                     # Tauri application configuration
├── tests/                                  # Integration & unit test suites
│   └── browserHandoff.test.mjs             # Node test runner suite for browser download handoff
├── index.html                              # Vite HTML shell
├── latest.json                             # Release updater endpoint metadata
├── package.json                            # Node scripts, frontend dependencies, test runners
├── tsconfig.json                           # TypeScript configuration for frontend
├── tsconfig.node.json                      # TypeScript configuration for Vite/Node scripts
└── vite.config.ts                          # Vite build & plugin configuration
```

---

## Key Locations by Domain

| Feature Domain | Primary Location | Relevant Files |
| :--- | :--- | :--- |
| **Network Interfaces & WLAN** | `src-tauri/src/network.rs` | Uses Win32 `GetAdaptersAddresses` and `WlanQueryInterface`. |
| **Download Engine** | `src-tauri/src/download_manager.rs` | Chunked downloads, pause/resume, state management. |
| **Process Supervision** | `src-tauri/src/process/` | `windows_job.rs`, `windows_spawn.rs`, `supervisor.rs`. |
| **Media Extraction** | `src-tauri/src/media/` | `yt_dlp.rs` metadata isolation runner. |
| **Browser Extension Bridge** | `src-tauri/src/browser_bridge.rs`, `src-tauri/src/bin/fluxnet_native_host.rs` | Windows Named Pipe server and Native Messaging Host binary. |
| **Browser Media Sniffing** | `browser-extension/` | `media.js`, `content.js`, `background.js`. |
| **Desktop User Interface** | `src/App.tsx`, `src/App.css` | Tab navigation, charts, download list, alerts. |
| **Application Settings** | `src-tauri/src/app_settings.rs` | JSON configuration and Windows registry Run key. |

---

## Naming Conventions

### Rust (`src-tauri/src/`)
- **Modules & Files:** `snake_case` (e.g., `download_manager.rs`, `windows_job.rs`).
- **Structs & Enums:** `PascalCase` (e.g., `DownloadManager`, `ProcessOutcome`, `QualityAlertSettings`).
- **Functions & Methods:** `snake_case` (e.g., `start_quality_monitor`, `cancel_all_downloads`).
- **Constants:** `SCREAMING_SNAKE_CASE` (e.g., `MAX_METADATA_BYTES`, `DISCOVERY_TIMEOUT`).

### TypeScript / React (`src/`)
- **Components:** `PascalCase` (e.g., `App`).
- **Types & Interfaces:** `PascalCase` (e.g., `NetworkInterface`, `ConnectivityProbe`, `DownloadItem`).
- **Files:** `PascalCase` for React components (`App.tsx`), `camelCase` for utilities (`browserHandoff.mjs`).

### Browser Extension (`browser-extension/`)
- **Script Files:** `kebab-case` (e.g., `download-gesture.js`, `video-sources.js`).
- **Functions & Variables:** `camelCase` (e.g., `isDirectMediaUrl`, `sendNativeMessage`).

---

## Guidance for Adding New Code

### 1. Adding a New Tauri Backend Command
1. Define the command function in the relevant module (e.g., `src-tauri/src/network.rs`):
   ```rust
   #[tauri::command]
   pub fn my_new_command(state: tauri::State<MyState>) -> Result<MyResponse, String> { ... }
   ```
2. Re-export and register it inside `src-tauri/src/lib.rs` in the `invoke_handler(tauri::generate_handler![ ... ])` macro.
3. In `src/App.tsx` (or a dedicated component), call it via:
   ```typescript
   import { invoke } from "@tauri-apps/api/core";
   const result = await invoke<MyResponse>("my_new_command", { ... });
   ```

### 2. Adding a New Subsystem or Module
1. Place the new file in `src-tauri/src/<module_name>.rs` or `src-tauri/src/<module_name>/mod.rs`.
2. Declare `mod <module_name>;` in `src-tauri/src/lib.rs`.
3. If it maintains long-lived state, wrap it in a struct, implement `Default`, and register via `.manage(MyModuleState::default())` in `lib.rs`.

### 3. Adding a New Browser Extension Detection Rule
1. Locate `browser-extension/media.js`.
2. Add the classification regex or parser function.
3. Write an accompanying unit test in `browser-extension/tests/media.test.mjs`.
4. Run tests with `cmd /c npm run test:extension` to ensure 100% pass rate.
