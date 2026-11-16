# Testing Strategy & Patterns

**Analysis Date:** 2026-11-17

## Test Frameworks & Tools

| Subsystem | Framework / Harness | Test Location | Execution Command |
| :--- | :--- | :--- | :--- |
| **Browser Extension** | Node.js Built-in Runner (`node --test`) | `browser-extension/tests/` | `cmd /c npm run test:extension` |
| **Browser Handoff Coordinator** | Node.js Built-in Runner (`node --test`) | `tests/browserHandoff.test.mjs` | `cmd /c npm run test:browser-handoff` |
| **Tauri Core & Native Modules** | Rust Native Test Harness (`cargo test`) | `src-tauri/src/` | `cargo test --manifest-path src-tauri/Cargo.toml` |
| **Process Supervision (Windows)** | Rust Integration Tests & C++ Harness | `src-tauri/src/process/tests.rs` | `cargo test --manifest-path src-tauri/Cargo.toml process::tests` |
| **Media Engine PoC** | Python scripts & acquired test suites | `experiments/media-engine-poc/` | `python local_fixture_test.py` |

---

## Test Suites & Coverage Breakdown

### 1. Browser Extension Suite (39 Tests)
- **Path:** `browser-extension/tests/media.test.mjs`, `browser-extension/tests/extension-build.test.mjs`
- **Scope:**
  - Manifest validation and extension ID verification (`clgiddogjgpjllmjgpffopnmighkmhae`).
  - Shared scope isolation (verifying no duplicate variable leaks in content scripts).
  - Native Messaging reconnect backoff and envelope sanitization.
  - Media sniffing: HLS variant playlists, fMP4 playlists, DASH manifests.
  - Byte-range Instagram Reel / Story filtering and duplicate URL suppression.
  - Download gesture verification (ensuring trusted user clicks trigger handoffs).

### 2. Browser Handoff Suite (7 Tests)
- **Path:** `tests/browserHandoff.test.mjs`
- **Scope:**
  - Tests window unminimize/restore lifecycle before prompting user save dialogs.
  - Release of URL lock upon user cancellation or network error.
  - Deduplication of simultaneous handoff requests for the same media URL.

### 3. Rust Native Suite (129 Tests)
- **Path:** Co-located in `src-tauri/src/`:
  - `app_settings::tests` — settings serialization, boundary validation, and migration.
  - `browser_bridge::tests` — named pipe security, pairing code validation, media resource deduplication.
  - `download_manager::tests` — progress calculation, pause/resume validators, sanitized file naming, path traversal defenses.
  - `media::yt_dlp::tests` — temp directory sandboxing, failsafe cleanup, timeout handling.
  - `network::windows::tests` — adapter enumeration, socket byte ordering, IPv4/IPv6 classification, BDIX probe limits.
  - `process::tests` — Win32 Job Object creation, assignment, suspended process resumption, tree kill on exit.
  - `speed_test::tests` — payload bounds, throughput calculations, provider validation.
  - `updater::tests` — updater endpoint requirements and cryptographic signature verification.

---

## Test Execution Commands & Environment Notes

### Windows Terminal Execution
> [!NOTE]
> On Windows systems with restricted PowerShell script execution policies, run npm test scripts via `cmd /c`:

```bash
# Run extension unit and build tests:
cmd /c npm run test:extension

# Run browser handoff tests:
cmd /c npm run test:browser-handoff

# Run Rust unit tests:
cargo test --manifest-path src-tauri/Cargo.toml
```

### Known Test Behaviors & Exclusions
1. **Registry Access (`app_settings::tests`):**
   - The test `app_settings::tests::windows_startup_registration_can_be_added_and_removed` attempts to inspect and modify `HKCU\Software\Microsoft\Windows\CurrentVersion\Run`. In sandboxed or non-elevated CI environments, this test may report error code 5 (Access is denied).
2. **Network Tests Ignored:**
   - Tests requiring active WAN connection (e.g., `downloads_small_public_https_file_with_application_client`) are decorated with `#[ignore]` to ensure offline test suite determinism.
3. **Child Fixtures:**
   - Process supervision tests launch small local executable fixtures to simulate hung child processes, verifying that the Windows Job Object kills them upon parent handle drop.
