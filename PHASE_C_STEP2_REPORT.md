# Phase C Step 2 — Real yt-dlp Artifact Verification and Discovery Testing

**Test date:** 2026-11-16 (Windows x64, local development machine)  
**Scope:** Real yt-dlp metadata discovery through FluxNet's isolated Rust adapter and existing Windows process supervisor. No media downloading, FFmpeg/aria2 use, UI wiring, authentication, or production dispatch was performed.

## Executive result

The official yt-dlp Windows x64 executable completed metadata-only extraction for one public YouTube video and one public Instagram post. The adapter returned dynamic formats, including separate audio/video streams for YouTube. A real-executable timeout and cancellation both returned the expected errors. No video or yt-dlp sidecar output was created in the task workspaces. Forced termination of this PyInstaller executable did leave its private `_MEI*` runtime extraction directory under the task's `TEMP`/`TMP` workspace; see the blocker below.

Facebook's selected public test URL failed extraction, and the selected Vimeo URL required login. These results do not establish universal compatibility with any site.

**Artifact integrity is partially verified:** the executable SHA-256 matches both the official release checksum file and GitHub's release asset digest. The detached GPG signature could not be verified locally because no GPG verifier was installed. The executable is not Authenticode-signed. Do not treat this run as complete signature verification.

## Adapter and process boundary inspected

- `src-tauri/src/media/yt_dlp.rs` invokes the tool through the existing contained process supervisor; it does not create a queue or UI entry point.
- `src-tauri/src/process/` preserves suspended creation, Job Object assignment, and resume ordering; cancellation and timeout terminate the job tree and reap the root process.
- The adapter uses explicit argv with no shell, disables config, update, cache, cookies, browser-cookie lookup, playlists, media download, and sidecar writes, and imposes a 60-second default timeout and 2 MiB stdout cap.
- The adapter parses metadata into a URL-free catalog and validates exact format IDs against the returned catalog. `BestAvailable` is accepted as a distinct selection. It does not substitute an unavailable exact format.
- During inspection, the argv builder was found to contain a duplicate `--` URL terminator. That was corrected and covered by an argument test; without the correction yt-dlp interpreted an extra token as a URL. Existing HTTP/HLS implementation and dispatch were not changed.
- Error output is not logged in normal operation. The opt-in test diagnostic emits only a bounded, sanitized stderr excerpt. Site smoke output includes only extractor, sanitized title, format count, heights, and separate-stream presence. The test checks serialized discovery results contain no HTTP URLs.

## Artifact provenance and verification

| Item | Result |
|---|---|
| Official release | [yt-dlp 2026.10.26](https://github.com/yt-dlp/yt-dlp/releases/tag/2026.10.26) |
| Asset | `yt-dlp.exe`, official Windows standalone x64 release asset |
| Exact source URL | `https://github.com/yt-dlp/yt-dlp/releases/download/2026.10.26/yt-dlp.exe` |
| Isolated location | `experiments/media-engine-poc/test-tools/yt-dlp-2026.10.26/yt-dlp.exe` |
| Size | 23,575,744 bytes (22.5 MiB as displayed by GitHub) |
| Runtime version | `2026.10.26` (`yt-dlp.exe --version`) |
| SHA-256 | `f5389d3698ecde4ef06f4e6b1ce0294fe9ac1396228c8990b8d3416d6d177a7d` |
| Local checksum comparison | PASS — `Get-FileHash` matched the `yt-dlp.exe` line in official `SHA2-256SUMS` |
| Release asset digest comparison | PASS — GitHub release metadata reports the same SHA-256 for `yt-dlp.exe` |
| Authenticode | NOT SIGNED — `Get-AuthenticodeSignature` returned `NotSigned` |
| Detached signature | PRESENT but NOT VERIFIED — official `SHA2-256SUMS.sig` and `public.key` were acquired alongside the executable; no `gpg` verifier was available and none was installed |
| Release metadata | GitHub identifies the 2026-10-26 release as published by `github-actions`; release assets include SHA-256 sums, detached signature, and a release attestation asset |

The pinned checksum was not invented: it came from the official release's `SHA2-256SUMS` and matched GitHub's asset digest. The upstream README documents `yt-dlp.exe` as the recommended Windows 10+ standalone x64 binary and gives GPG verification instructions for the checksum files. Those are useful provenance signals, but without verifying the detached signature, authenticity is not fully established. [Official release assets](https://github.com/yt-dlp/yt-dlp/releases/tag/2026.10.26) · [Official installation and verification instructions](https://github.com/yt-dlp/yt-dlp#release-files)

### License and notices

The yt-dlp project source is licensed under the Unlicense. The official documentation cautions that release binaries contain code under other licenses; the PyInstaller Windows executable has bundled third-party components and corresponding upstream third-party license information. A future redistributed build must retain the applicable third-party notices and satisfy each included component's terms. This audit does not make a new legal determination for redistribution of this particular binary; consult the exact release's third-party license material before packaging it. [Upstream licensing guidance](https://github.com/yt-dlp/yt-dlp#licensing) · [Upstream third-party license list](https://github.com/yt-dlp/yt-dlp/blob/master/THIRD_PARTY_LICENSES.txt)

## Site-by-site discovery results

All invocations used the adapter's discovery-only argv and no cookies, browser credentials, or login session. URLs are listed because they are public test page URLs; signed media URLs were neither logged nor persisted in the returned model.

| Site and exact page URL | Result on 2026-11-16 | Observations |
|---|---|---|
| YouTube — `https://www.youtube.com/watch?v=dQw4w9WgXcQ` | **PASS** | Title: “Rick Astley - Never Gonna Give You Up (Official Video) (4K Remaster)”; 48 returned formats; heights ranged from 27 through 2160; separate audio/video formats were present. Exact selection of a returned format ID and `BestAvailable` validation both passed. |
| Instagram — `https://instagram.com/p/aye83DjauH/` | **PASS** | Extractor `Instagram`; title `Video by naomipq`; 4 returned formats; reported height 640; separate audio/video was not indicated. Exact returned format ID and `BestAvailable` validation passed. This only verifies this post at this time. |
| Facebook — `https://www.facebook.com/radiokicksfm/videos/3676516585958356/` | **FAIL** | Extractor reported it could not parse the page data and suggested checking for an updated yt-dlp version. No authentication was attempted. Official yt-dlp extractor test reference: [Facebook test case](https://github.com/yt-dlp/yt-dlp/blob/2026.10.26/yt_dlp/extractor/facebook.py). |
| Vimeo — `https://vimeo.com/76979871` | **NOT AVAILABLE WITHOUT LOGIN** | The extractor reported that its web client only works when logged in and suggested cookies. No cookies were supplied and no login/access restriction was bypassed. Official test reference: [Vimeo test case](https://github.com/yt-dlp/yt-dlp/blob/2026.10.26/yt_dlp/extractor/vimeo.py). |
| YouTube — `https://www.youtube.com/watch?v=BaW_jenozKc` | **FAIL** | yt-dlp reported the video unavailable during this run. No compatibility claim is based on this URL. |

The YouTube and Instagram successes were repeated through the actual Rust adapter. The adapter returned format metadata only. The YouTube result demonstrated a broad, dynamic resolution catalog, separate audio/video availability, and successful exact-ID/Best Available validation; no resolution cap was applied by the adapter.

## Side effects, privacy, timeout, and cancellation

- **Normal discovery:** both successful real-site runs found no files in their per-test working directory after completion. The returned JSON model was serialized in the smoke test and asserted to contain no `http://` or `https://` URL. This verifies URL fields are excluded from that returned result, not every possible secret string in every website title or future extractor response.
- **Sidecar suppression:** invocation included `--skip-download`, `--no-write-info-json`, `--no-write-thumbnail`, `--no-write-subs`, `--no-write-auto-subs`, `--no-write-description`, and `--no-write-playlist-metafiles`; no media file or sidecar was observed.
- **Timeout:** real executable with a 1 ms limit returned `DiscoveryError::TimedOut`.
- **Cancellation:** real executable with the existing `DownloadTaskControl` returned `DiscoveryError::Cancelled` after cancellation; the call uses the existing process supervisor and Job Object.
- **Forced-stop runtime residue:** both timeout and cancellation workspaces contained a `_MEI*` directory after the bundled PyInstaller executable was terminated. These are the executable's private extracted runtime files, not media or yt-dlp sidecars. The directory could not be enumerated/deleted by the current process due to access denied. This means abrupt cancellation currently leaves temporary runtime residue and task-workspace cleanup is not yet demonstrated for this executable form. This is a real cleanup blocker for product integration; investigate a suitable no-extraction/unpackaged official build or safe cleanup strategy before production use.
- No authentication, DRM, private-content access, or access-control bypass was tested or attempted.

## Rust checks and regression results

- Real YouTube smoke through Rust adapter: **PASS**.
- Real Instagram smoke through Rust adapter: **PASS**.
- Real yt-dlp timeout and cancellation test: **PASS** for expected error results and no media outputs; `_MEI*` runtime residue remains as described above.
- `cargo test --offline --target x86_64-pc-windows-msvc --lib`: **118 passed, 1 failed, 3 ignored**. The failure is the previously observed unrelated `app_settings::tests::windows_startup_registration_can_be_added_and_removed`; `RegCreateKeyExW` returned Windows error 5 (access denied) for the current environment. It was not suppressed or altered. The ignored tests include the two manually invoked real-tool tests and an existing live-network download test.
- Adapter parser, redaction, argument, selection, and process tests passed in that suite.
- `cargo check --offline --target x86_64-pc-windows-msvc --lib`: **PASS** after addressing an unused-value warning in the inspected adapter branch.
- `cargo fmt --check`: **PASS**.
- `git diff --check`: **PASS**; Git printed existing line-ending conversion warnings for unrelated modified files.

## Blockers and limitations

1. Verify `SHA2-256SUMS.sig` with the official public key on a trusted machine before treating the release as fully authenticated or pinning it for distribution. Hash agreement alone is not a signature check.
2. The executable has no Authenticode signature; Windows publisher identity is therefore not available from Authenticode.
3. Forced process termination leaves PyInstaller `_MEI*` runtime data in the task workspace and access denied prevented cleanup. Do not integrate cancellation without resolving this residue behavior.
4. Facebook extraction failed on the selected public test page; Instagram and YouTube results are limited to the exact URLs and date above. Extractor changes, geographic restrictions, page changes, and rate limits can alter results.
5. Vimeo test access required login. The product policy intentionally supplies no credentials or browser cookies, so this case remains unsupported under the tested configuration.
6. Redistribution licensing for the complete standalone executable requires reviewing the exact bundled third-party notices and release metadata, not only yt-dlp's Unlicense.
7. The existing registry permission test failure remains an environment issue and is still visible in the full test result.

No production UI, download dispatch, HTTP/HLS behavior, repository license, FFmpeg/aria2 integration, or authentication handling was changed.
