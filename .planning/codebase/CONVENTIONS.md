# Coding Conventions & Development Patterns

**Analysis Date:** 2026-11-17

## Code Style & Formatting

### 1. Rust Conventions (`src-tauri/`)
- **Edition:** Rust 2024 Edition.
- **Naming:**
  - `snake_case` for functions, methods, modules, and local variables.
  - `PascalCase` for structs, enums, traits, and type aliases.
  - `SCREAMING_SNAKE_CASE` for global static constants and limits.
- **Imports:** Grouped standard library first (`std::...`), third-party crates second (`tauri::...`, `serde::...`), internal crate modules third (`crate::...`).
- **Derives:** Data transfer objects (DTOs) and models consistently derive `Debug, Clone, Serialize, Deserialize`.
- **Platform Guards:** Windows-specific APIs are strictly guarded using `#[cfg(windows)]` or `#[cfg(all(test, windows))]`. Cross-platform fallbacks are provided where necessary.

### 2. TypeScript & React Conventions (`src/`)
- **Strict Mode:** TypeScript `strict: true` is enforced in `tsconfig.json`.
- **Components:** Functional components using React 19 standard hooks (`useState`, `useEffect`, `useCallback`, `useRef`).
- **Type Annotations:** Explicit type definitions for all state, props, and Tauri IPC responses (avoiding bare `any`).
- **Icons:** Strongly typed `IconName` union mapped to SVG render helpers.
- **CSS Architecture:** Scoped styles defined in `src/App.css` utilizing CSS custom variables for dark/light themes, card elevation, and layout flex/grid utilities.

### 3. Browser Extension Conventions (`browser-extension/`)
- **Module Format:** Vanilla ES modules / MJS for test runners, standard scripts for MV3 background service workers and content scripts.
- **Global Scope Safety:** Content scripts avoid declaring duplicate variables in the shared tab global scope (verified by `browser-extension/tests/extension-build.test.mjs`).
- **Zero-Dependency Core:** The extension is built without external heavy dependencies to maximize loading speed and maintain Chrome Web Store compliance.

---

## Error Handling Patterns

### Rust Backend
1. **Command Error Propagation:** Tauri commands return `Result<T, String>`:
   ```rust
   #[tauri::command]
   fn my_action() -> Result<Data, String> {
       operation().map_err(|error| format!("Could not complete operation: {error}"))
   }
   ```
2. **Contextual Error Messages:** Errors describe the failure and preserve the underlying error cause:
   - Example: `"Could not inspect FluxNet startup registry value: {error}"`.
3. **Fail-Closed Security Checks:** Path traversal and symlink validation fail closed:
   - Reparse points, symlinks, or directories that fail validation return immediate `InvalidInput` errors.

### Frontend Error Handling
1. **Async IPC Calls:** All `invoke()` calls are wrapped in `try...catch` blocks.
2. **State Error Reporting:** Errors populate component alert banners or notification toasts, preventing silent failures.
3. **Retry Boundaries:** Failed downloads and handoffs release locks, allowing user retry.

---

## Security & Boundary Conventions

1. **Origin Verification in Native Host:**
   - `fluxnet_native_host.rs` strictly validates `args[1]` against `chrome-extension://clgiddogjgpjllmjgpffopnmighkmhae/`.
   - Any other origin terminates the process immediately.
2. **Buffer Limits:**
   - Native message size is hard-capped at 64 KB (`MAX_NATIVE_MESSAGE`).
   - Browser bridge config read is capped at 4 KB.
   - Metadata extraction from yt-dlp is capped at 2 MB (`MAX_METADATA_BYTES`).
3. **Credential & URL Sanitization:**
   - All URLs stripped of user credentials (`user:pass@host` rejected).
   - Rotating CDN query strings are scrubbed before computing resource hashes.
   - Instagram and Facebook byte-range partial stream URLs are filtered out to prevent broken downloads.
4. **Child Process Containment:**
   - All external CLI subprocesses (`yt-dlp`, test fixtures) MUST be spawned inside a Win32 Job Object with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`.

---

## Concurrency & Lifecycle Rules

1. **Non-Blocking Main Thread:**
   - Long-running network or disk operations are executed inside `tokio::spawn` or dedicated OS threads.
2. **Graceful Exit Handshake:**
   - Window close and tray exit route through `request_exit()` in `src-tauri/src/lib.rs`.
   - An `AtomicBool` exit flag prevents duplicate exit triggers.
   - Exit waits for the network quality monitor loop and download workers to gracefully finish or checkpoint before invoking `app.exit(0)`.
