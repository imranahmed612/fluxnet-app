# FluxNet Phase C, Step 1 — yt-dlp Discovery Prototype

**Status:** isolated Rust prototype complete; no download dispatch or UI/queue integration.

## Architecture review

- `DownloadManager` remains the only queue, history, concurrency, cancellation, retry, and completion owner. The browser extension/Native Messaging handoff remains unchanged.
- The new `media::yt_dlp` adapter takes an executable path, task working directory, page URL, and the existing `DownloadTaskControl`. It invokes the Phase B Windows supervisor, so child creation remains suspended → Job Object assignment → resume.
- No adapter path was added to `download_worker`, `start_download`, or browser handoff. Direct HTTP and HLS execution are unchanged.
- The adapter is callable from Rust but is not yet exposed as a Tauri command or React flow. A user-visible discovery flow needs a later, explicit manager worker/API integration that acquires the existing queue slot.

## Prototype behavior

- Accepts HTTP/HTTPS URLs and rejects credentials, local hostnames, and private/local IP literals. It does not include the submitted URL in its result or errors.
- Runs yt-dlp with an explicit argument vector and no shell. Options disable configuration, update checks, filesystem cache, cookie files/browser cookies, playlists, media download, info JSON, thumbnails, subtitles, descriptions, playlist sidecars, progress, and warnings. The current upstream option reference supports these controls: [yt-dlp README](https://github.com/yt-dlp/yt-dlp/blob/master/README.md) and [yt-dlp options source](https://github.com/yt-dlp/yt-dlp/blob/master/yt_dlp/options.py).
- Caps JSON output at 2 MiB, stderr capture at 16 KiB, and accepted format entries at 2,048. Truncated output is rejected. No maximum resolution is set.
- Parses only allowlisted metadata: title, duration, extractor, format IDs, resolution/height, FPS, codecs, extension, approximate/known size, audio/video presence, and whether a video-only format has a separate audio format available.
- Format catalogs expose `BestAvailable` plus exact IDs returned by the extractor. Exact selection validation rejects unavailable IDs; it does not substitute another format. This step does not download or merge selected formats.
- URL fields from yt-dlp JSON are discarded. Metadata text is bounded, control characters are removed, URL tokens and common signed/auth query values are redacted. Raw stdout/stderr are never logged or returned to the UI.
- The child receives a replacement environment with only Windows system variables, task-local TEMP/TMP, and Python isolation flags. No browser credentials or cookies are read.

## Files changed

- `src-tauri/src/lib.rs` — declares the isolated media module.
- `src-tauri/src/media/mod.rs` — media adapter module boundary.
- `src-tauri/src/media/yt_dlp.rs` — supervised discovery, URL checks, dynamic catalog model, safe JSON parsing, and unit tests.
- `src-tauri/src/process/mod.rs` — crate-internal supervisor API exports for the adapter.
- `src-tauri/src/process/tests.rs` — harmless local fixture modes for metadata, malformed/empty/oversized output, and indefinite wait.
- `PHASE_C_STEP1_REPORT.md` — this report.

No Cargo dependencies were added. `serde_json`, `reqwest::Url`, the existing task control, and the existing supervisor are reused.

## Verification

- Windows-target `cargo check --offline --target x86_64-pc-windows-msvc --lib`: passed.
- Focused yt-dlp adapter tests: **7 passed**. Coverage includes dynamic separate audio/video metadata, malformed JSON, missing formats, playlist rejection, oversized output, timeout, cancellation, URL validation/redaction, and no fixture-created media/sidecar files.
- Full Windows-target Rust suite, including direct HTTP and HLS regressions: **118 passed, 1 failed, 1 ignored**. Existing HTTP/HLS tests passed. The live public HTTPS test remains ignored because it requires internet access.
- The one failure is the existing `app_settings::tests::windows_startup_registration_can_be_added_and_removed`: `RegCreateKeyExW` for the current-user Run key returns error 5 (access denied). The test was not altered or suppressed.
- `cargo fmt --check` and `git diff --check`: passed.

The local fixture exercises supervisor lifecycle and JSON parsing; a separate unit test checks the metadata-only argument set. The fixture is not the yt-dlp binary and does not establish compatibility with any website.

## Limitations and blockers

1. No yt-dlp executable exists in the workspace. No binary was acquired or built, so real yt-dlp option compatibility, extraction, website coverage, and public URL tests remain unverified. Pin and verify a legally approved artifact before doing those tests.
2. There is no Tauri command or UI yet. Discovery currently has an internal Rust API and must later run within a real DownloadManager worker/slot using its task control.
3. Hostname checks reject common local names and private IP literals but do not resolve and pin DNS answers or police redirect/manifest destinations. A dedicated network isolation/SSRF policy is still needed before exposing arbitrary URL discovery to the UI.
4. yt-dlp extractors may use public website APIs or encounter unavailable, private, login-required, geo-restricted, age-restricted, or DRM-protected content. This prototype supplies no credentials and does not bypass those controls; such pages may fail.
5. `BestAvailable` and exact format choices are catalog-level contracts only. Actual format expression, separate stream transfer, merge/remux, media validation, atomic completion, and manager-visible progress belong to later steps.
6. Format catalog size and JSON size are bounded by explicit safety limits. Extremely large extractor catalogs fail closed instead of being partially presented.

**Compatibility statement:** no website compatibility is claimed until the exact pinned yt-dlp artifact passes real, authorized tests.
