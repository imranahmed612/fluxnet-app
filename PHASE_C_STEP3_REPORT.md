# FluxNet Phase C Step 3 — yt-dlp Temporary Runtime Cleanup and Reliability

**Date:** 2026-11-16  
**Status:** task-scoped PyInstaller runtime cleanup implemented and verified on Windows x64.

## Root cause and process ownership

The tested official Windows `yt-dlp.exe` is a PyInstaller one-file executable. PyInstaller documents that its bootloader creates a random `_MEIxxxxxx` extraction directory, launches the frozen child process, and that the **one-file parent bootloader process** deletes the extraction directory after the child exits. If that parent is forcefully terminated, its cleanup code cannot run and the `_MEI*` directory remains. [PyInstaller one-file operation](https://pyinstaller.org/en/latest/operating-mode.html) · [PyInstaller one-file cleanup behavior](https://pyinstaller.org/en/stable/feature-notes.html#onefile-mode-and-temporary-directory-cleanup)

FluxNet starts the executable suspended, assigns it to the existing kill-on-close Job Object, and only then resumes it. Its spawned PyInstaller child remains in that job. Before this change the supervisor reaped the root process and closed the job, but did not wait until all descendants had actually exited. Thus the adapter could not safely tell when cleanup of PyInstaller's extraction directory was safe.

## Changes

- Added Job Object active-process accounting and a bounded wait after termination. The supervisor now terminates and waits for the complete contained process tree even after an otherwise successful root exit. This also prevents descendants from outliving normal process completion.
- The yt-dlp adapter now reserves a unique runtime temp directory inside the caller's task workspace and sets both `TEMP` and `TMP` to it. Existing environment isolation remains in place.
- The temp parent must be an existing non-reparse directory. The reserved runtime directory is created exclusively, canonicalized, and verified as a direct child of the canonical task workspace.
- Cleanup runs only after `run_supervised` returns a successful terminal outcome, which now proves the Job Object reached zero active processes. It removes the exact uniquely created task runtime directory; it does not search for or delete global `_MEI*` directories.
- If process-tree shutdown cannot be confirmed, the adapter preserves the runtime directory. If cleanup or path ownership validation fails, it preserves evidence and returns a generic safe cleanup error. The marker includes only the generated runtime directory basename and an allowlisted failure reason; it contains no URL, title, or absolute path.
- Existing task cancellation is unchanged. Normal exit, timeout, cancellation, and DownloadManager shutdown cancellation use the same process supervisor path. Discovery remains isolated and is not connected to production dispatch or UI.
- Added no dependencies and did not modify HTTP/HLS behavior.

## Test evidence

### Cleanup and process lifecycle

- Runtime cleanup removes a fixture `_MEI*` below its exact reserved task directory and preserves an unrelated sibling `_MEI*` directory: **PASS**.
- Ownership/path replacement fails closed, preserves the replacement, and creates sanitized cleanup evidence: **PASS**.
- Unconfirmed process-tree shutdown preserves the runtime directory and warning evidence: **PASS**.
- Existing fixture test where the root exits while a descendant survives: **PASS**; supervisor returns only after the descendant is terminated.
- Existing timeout and cancellation process fixture: **PASS**.
- Existing DownloadManager worker/concurrency/shutdown test: **PASS**; shutdown signal terminates the complete process tree.

### Real yt-dlp 2026.10.26

The same official binary and public URLs recorded in [Phase C Step 2](PHASE_C_STEP2_REPORT.md) were used. Discovery remained metadata-only, without cookies, browser credentials, or media downloads.

| Scenario | Result | Cleanup evidence |
|---|---|---|
| YouTube `https://www.youtube.com/watch?v=dQw4w9WgXcQ` | **PASS** — 48 formats, heights through 2160, separate audio/video present | Adapter's task workspace was empty after return; real smoke test passed |
| Instagram `https://instagram.com/p/aye83DjauH/` | **PASS** — 4 formats, reported height 640 | Adapter's task workspace was empty after return; real smoke test passed |
| Real executable, 1 ms timeout | **PASS** — returned `TimedOut` | Task workspace empty; no owned `_MEI*` remained in it |
| Real executable, existing task cancellation | **PASS** — returned `Cancelled` | Task workspace empty; no owned `_MEI*` remained in it |

The timeout and cancellation tests force termination of the executable's Job Object tree, then assert the exact task workspace contains no files before removing that test workspace. This verifies cleanup for the current run without inspecting or deleting other tasks' temporary directories.

## Validation results

- `cargo check --offline --target x86_64-pc-windows-msvc --lib`: **PASS**.
- `cargo fmt --check`: **PASS**.
- `git diff --check`: **PASS**. Git emitted existing LF/CRLF working-copy warnings for unrelated files.
- Full Windows library suite: **121 passed, 1 failed, 3 ignored**. The unchanged failure is `app_settings::tests::windows_startup_registration_can_be_added_and_removed`, where the current environment denies access to the HKCU Run registry key (`RegCreateKeyExW`, error 5). The test was not skipped or weakened. The three ignored tests include live-network/manual tests.
- The real YouTube and Instagram tests, plus real timeout/cancellation tests, were explicitly invoked after the cleanup implementation and passed.

## Remaining limitations and blockers

1. **Legacy Step 2 residue:** three old `_MEI*` directories from prior Phase C Step 2 runs remain in those runs' `%TEMP%\fluxnet-ytdlp-real-timeout-*` and `%TEMP%\fluxnet-ytdlp-real-cancel-*` workspaces. They were left untouched: Phase C Step 3 only removes a temp directory it created exclusively for its own task, and must not delete another FluxNet task's workspace. Current Step 3 test workspaces contained no `_MEI*` after each test.
2. If Job Object termination/accounting fails, the adapter intentionally retains the private runtime directory and a diagnostic marker instead of risking deletion while a process may still use it. This is an explicit cleanup warning path, not silent success.
3. Task-workspace privacy is a caller precondition: the current internal prototype requires the caller to provide a private task directory. This adapter is not exposed to UI or wired to the production DownloadManager, so future integration must preserve unique per-task workspace ownership and pass the manager's existing cancellation control.
4. Shutdown handling is covered through the existing DownloadManager cancellation/shutdown test and the adapter's same `DownloadTaskControl` path. A user-facing application shutdown test with yt-dlp itself is unavailable because the discovery adapter is not yet production-integrated.
5. Real website results remain limited to the exact YouTube and Instagram URLs and test date. No general site compatibility is claimed.

## Files changed

- `src-tauri/src/process/windows_job.rs` — bounded wait for zero active Job Object processes.
- `src-tauri/src/process/supervisor.rs` — terminate and reap the full process tree before returning.
- `src-tauri/src/media/yt_dlp.rs` — exclusive task-scoped runtime temp directory, safe cleanup/evidence handling, and cleanup tests.
- `PHASE_C_STEP3_REPORT.md` — this report.
