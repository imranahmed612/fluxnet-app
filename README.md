# FluxNet

FluxNet is a Tauri 2 desktop network utility built with React, TypeScript, and Rust.

## Development

Install the JavaScript dependencies, then run the desktop application:

```powershell
npm.cmd install
npm.cmd run tauri dev
```

The frontend can also be checked independently with `npm.cmd run build`. Rust tests run with:

```powershell
cargo test --manifest-path E:\Fluxnet\src-tauri\Cargo.toml
```

## Speed test

On the **Speed Test** page, select **Cloudflare public speed test** or **Custom server** and confirm before starting.

The Cloudflare preset uses the documented public endpoints:

- `GET https://speed.cloudflare.com/__down?bytes=5242880`
- `POST https://speed.cloudflare.com/__up` with a 5 MiB `application/octet-stream` body.

The custom-server option accepts an HTTPS origin and uses the FluxNet contract:

- `GET /download?bytes=5242880` returns exactly 5 MiB of response data.
- `POST /upload` consumes the complete 5 MiB request body and returns a successful HTTP status only after it has consumed the body.

Both options require valid HTTPS; redirects are not followed. Each run is limited to 5 MiB per direction, with a 20-second request timeout per phase. Results use the bytes actually transferred and elapsed time, and are not shown for failed or incomplete transfers. Results depend on the selected server, its load, and the route between the device and that server. Tests consume network data. The Cloudflare preset is configured from its documented URL contract; it is not treated as live-verified by these tests.

## BDIX analysis

The **BDIX Analysis** page resolves a user-entered hostname or IP address, displays the returned IPv4/IPv6 addresses, and sends a configurable series of Windows ICMP echo probes to the first resolved address. DNS resolution is bounded to 8 seconds; each probe has a configurable 0.5–5 second timeout. Analysis can be cancelled, and results include actual per-probe responses, latency statistics, packet loss, and a timestamp. IPv6 uses Windows ICMPv6; the existing Dashboard connectivity diagnostic remains unchanged.

ICMP reachability does not establish that a server is on BDIX, and ping latency is not throughput. The **BDIX Server List** starts empty; add servers supplied by your ISP or another trusted source. Entries are stored in the app's local browser storage. Marking an entry as BDIX verified requires a verification source and notes and is a user-recorded claim, not an automatic FluxNet classification. No server addresses are preloaded or inferred.

## Network Quality Monitor

On Windows, FluxNet starts one shared background monitor when the app launches. It repeatedly sends one ICMP echo probe at a time to the configured target (initially `1.1.1.1`) and samples Windows adapter byte counters. The **Network Quality** page can enable quality monitoring for a selected hostname/IP and 1-, 2-, or 5-second interval; each interval is a wait after the probe and associated sampling complete. DNS resolution is bounded to 8 seconds and is retried after a failed resolution; a DNS failure is recorded as a failed outcome, not a permanent stop. ICMP calls time out after at most 5 seconds.

The page's **Stop Monitoring** (and the tray stop action) stops quality-page monitoring and its quality-probe events, but the shared background monitor continues so Dashboard telemetry remains available. Leaving the page or hiding the window does not stop either monitor. Explicitly exiting FluxNet cancels the shared monitor and waits for it to finish; an in-flight Windows ICMP call may take up to its timeout to return.

ICMP latency and packet loss measure reachability to the selected target; they are not download/upload throughput measurements. Dashboard download/upload Mbps are separately estimated from changes in Windows interface byte counters over elapsed time, aggregated over active physical Ethernet and Wi-Fi adapters. This is observed traffic on those interfaces, not an active speed test or a guarantee of internet throughput. Latency and packet-loss statistics use the most recent 60 probe outcomes (failures count as loss); retained chart and probe history are capped at 300 outcomes. Clearing history also resets the rolling statistics.

Connection ratings use average latency across successful probes and packet loss across all outcomes in that rolling window. Initial thresholds are configured in `src-tauri/src/network.rs`: Excellent (<= 50 ms, <= 1% loss), Good (<= 100 ms, <= 2%), Fair (<= 200 ms, <= 5%), and Poor above either Fair threshold. No outcomes show No data; a window with no successful probes shows Disconnected. These thresholds are heuristics, not universal internet-quality standards, and can be changed in the Rust constants. Ratings are based on ICMP reachability only; adapter traffic estimates are independent and are not used to rate connection throughput or confirm BDIX membership.

**Export CSV** opens the native Tauri save-file dialog and exports the complete retained history (up to 300 records) in chronological order. Each record includes its actual timestamp as an ISO 8601 UTC value, the hostname/IP used for that probe, success/failure, latency (up to two decimal places) when available, and failure reason. Failed-probe latency is blank. Empty history cannot be exported; canceling the dialog leaves the history unchanged.

On Windows, FluxNet remains in the system tray when its window is closed. Select **Open FluxNet** from the tray icon or click the icon to restore the existing window. Tray **Start Monitoring** enables quality monitoring using the last configured target and interval; tray **Stop Monitoring** disables quality monitoring, while shared ICMP and adapter sampling continue for Dashboard telemetry. **Exit FluxNet** cancels the shared probe loop, waits for it to finish, and exits. Monitoring does not continue after explicitly exiting FluxNet or Windows shuts down.

Network Quality settings provide optional native Windows connection alerts. Alerts are disabled by default; enable them and select 2, 3, or 5 consecutive failed probes and a 1, 5, or 15 minute cooldown. A successful probe resets the consecutive-failure count. Windows notification settings may suppress delivery. Notification availability depends on the installed Windows app identity and OS notification settings; tray and notification behavior should be checked in a packaged Windows build.

## Network Interfaces

The **Network Interfaces** page reads Windows adapter names, descriptions, types/status, physical addresses, IPv4/IPv6 addresses, gateways, DNS servers, link speeds, and byte counters using `GetAdaptersAddresses` and `GetIfTable2`. Adapter traffic is sampled every two seconds only while this page is open; rates are calculated from the difference between successive Windows byte counters and are not an independent speed test. Wi-Fi SSID, signal quality, and associated receive/transmit rates use the Windows WLAN API. Frequency is looked up for the connected access point in the WLAN BSS list, when the driver exposes that information. Windows or a network driver may omit individual fields; these are displayed as unavailable rather than estimated. Virtual adapter labeling uses the Windows interface type and recognizable adapter/driver names, so it is an informative classification, not a definitive virtualization inventory.

## Settings

The **Settings** page stores desktop preferences in the per-user FluxNet configuration directory. Launch on startup uses the current user's Windows Run registry entry; start minimized takes effect on the next launch, and disabling minimize-to-tray makes closing the window exit FluxNet after the existing shutdown handling. Download concurrency, bandwidth limits, and completion/failure notifications use the Download Manager's persisted settings. The selected default download folder is used as the initial location in the save dialog, which still lets the user choose a different destination.

Network monitoring interval, ping timeout, and alert threshold/cooldown are validated and applied to Windows ICMP monitoring. Packet loss is always derived from probe results. Native Messaging installation status is checked from the Chrome/Edge per-user registration and manifest; extension connection requires the extension to initiate a message, so reconnect from its popup. Automatic video/download detection, overlays, extension filters, and extension notification behavior remain owned by Chrome/Edge extension storage and must be changed in the extension popup; FluxNet does not claim to control them from desktop Settings. Browser-side download prompts do not pause or cancel the browser's own transfer.

### In-app updates

The **Updates** section displays the installed version, supports manual checks, and can optionally check once at startup (off by default). FluxNet never installs an update automatically. Update bundles are downloaded and installed through Tauri's updater plugin, which verifies the artifact signature; release metadata is required to bind the advertised version to the signed version. Before installing, FluxNet asks for confirmation and warns about queued, downloading, or paused FluxNet transfers that may be interrupted. Windows updates use the existing NSIS format.

Update checks are intentionally unavailable in this source configuration: `src-tauri/tauri.conf.json` has no public verification key or release endpoint. Configure `plugins.updater.pubkey` with the public key from Tauri's signer and `plugins.updater.endpoints` with the real HTTPS endpoint hosting signed update metadata before publishing a release. The application rejects empty or non-HTTPS endpoints, missing keys, downgrade-enabled settings, and configurations that disable signed-version or TLS verification. The frontend cannot provide or override update URLs.

Generate and store the signing key outside the repository, for example:

```powershell
New-Item -ItemType Directory -Force "$env:USERPROFILE\.tauri" | Out-Null
npm.cmd run tauri signer generate -- -w "$env:USERPROFILE\.tauri\fluxnet.key"
```

Keep the private key and its password in a protected local location or a release-system secret store. For release builds, provide the private-key path/content through `TAURI_SIGNING_PRIVATE_KEY` and, when applicable, the password through `TAURI_SIGNING_PRIVATE_KEY_PASSWORD`; do not put either in source control or a `.env` file. Put only the generated public key in the Tauri updater configuration. `bundle.createUpdaterArtifacts` is enabled to prepare signed updater artifacts, but real update distribution cannot work until a public key, a real signed release, and its HTTPS metadata endpoint are configured. The Chrome extension remains distributed separately through Chrome/Edge.

## Download Manager

The **Download Manager** accepts HTTP and HTTPS URLs and asks for a destination with the native save-file dialog. Files stream to a sibling `.fluxnet-part` file and are renamed into place only after a successful complete transfer. TLS certificate verification remains enabled and uses the Windows native certificate roots; the HTTP client also honors the Windows system proxy settings. Redirects are limited to 10 hops, and requests have a 15-second connect timeout, a 30-second read timeout, and a one-hour total timeout. Existing destination files are not overwritten. FluxNet does not open or execute downloaded files.

Connection and transfer failures include their classified cause (DNS, TLS/certificate, timeout, connection, redirect, or HTTP status) and the underlying error chain where available. Query values are redacted from transport diagnostics and hidden in the visible queue URL to avoid exposing URL tokens. Filesystem failures are labeled separately with the operation that failed.

Downloads can be paused, resumed, cancelled, and retried. Resume uses an HTTP `Range` request only when a partial file's recorded size and a server ETag or Last-Modified validator are available; FluxNet requires a matching `206 Partial Content` response, matching validator, and valid `Content-Range` before appending. If these conditions are not met, the partial file is left untouched and the user can explicitly choose **Restart**. Failed or cancelled downloads can also be retried from the recorded offset when the server supports safe range requests. Progress, transfer speed, elapsed time, and remaining time are based on bytes actually streamed.

The queue supports 1–4 concurrent downloads and an optional aggregate bandwidth cap. Download history and settings are stored in FluxNet's app-data directory. Active or paused transfers are not resumed automatically after an application restart; interrupted records are marked failed, and partial files are retained for an explicit retry or restart. The tray includes an **Open Download Manager** action, and downloads continue while the window is hidden. Explicit exit cancels active transfers and waits for their workers to stop. Optional native completion/failure notifications are controlled by the Download Manager setting and remain subject to Windows notification permissions.

Downloads use the selected server and network route and can consume substantial data and disk space. Only download files from sources you trust. To exercise the page, enter a legitimate public HTTP/HTTPS file URL, select a new destination in the save dialog, and observe the actual transfer; resume behavior additionally requires a server that provides validators and honors byte ranges.

## Browser video detection

FluxNet includes an unpacked Chromium Manifest V3 extension in `browser-extension\`. On Windows, build its Native Messaging helper and install the extension:

1. From the project root, run `cargo build --manifest-path src-tauri\Cargo.toml --release --bin fluxnet_native_host`. Cargo must be given the explicit `--bin` because the package has both the FluxNet app and the `fluxnet_native_host` binary; do not use an ambiguous `cargo run` command.
2. Open `chrome://extensions` or `edge://extensions`, enable Developer mode, and choose **Load unpacked** with the `browser-extension\` folder.
3. Run PowerShell from the project root:

   ```powershell
   .\browser-extension\install-native-host.ps1 -HostExePath .\src-tauri\target\release\fluxnet_native_host.exe -Browser Both
   ```

4. Start FluxNet. The extension connects automatically and retries when FluxNet becomes available; no pairing code is required. Video detection and download controls work on the page, while **Scan this tab** and the extension popup remain optional tools.

The player overlay appears only for supported video resources found in HTML video elements or confirmed as video responses. It is isolated in a Shadow DOM and offers only the detected original MP4/WebM format and known dimensions; it does not convert or rename formats. An accessible HLS/DASH manifest is parsed for actual representations, resolutions, and audio tracks. Because this build has no HLS/DASH segment downloader or merger, these variants are identified but shown as unavailable rather than being presented as complete downloads. Inaccessible manifests are reported as such. DRM, encrypted/protected content, and browser authentication are not bypassed.

Ordinary files are considered only after a plain user click on an eligible HTTP/HTTPS link; the click itself is not blocked. Chrome's `downloads.onCreated` event occurs after a browser transfer can start, so an extension prompt may appear after the transfer begins. **Download with FluxNet** sends the approved URL through Native Messaging to FluxNet's existing Download Manager and opens its save dialog. The Chrome transfer is not silently cancelled and remains the fallback if FluxNet is unavailable, the handoff fails, or the save dialog is cancelled. If FluxNet's queue is confirmed and both copies are running, cancel the browser copy yourself if desired. **Continue with browser** leaves the browser transfer alone. Remembered choices are optional and scoped to the file type.

The extension permissions are used as follows:

- `webRequest` observes HTTP/HTTPS request URLs and response `Content-Type` to recognize direct media and HLS/DASH manifests. It does not block or modify requests and does not collect request headers.
- `host_permissions` for HTTP/HTTPS pages allow event-driven observation and the content script on sites the user visits. This broad scope is necessary for detection without a per-site scan click; domains can be excluded in extension settings.
- `downloads` observes browser download metadata such as URL, filename, MIME type when Chrome reports one, and size when known. The event is not a pre-download interception hook.
- `tabs` associates requests with the source tab and obtains the page title/domain; `activeTab` and `scripting` retain the explicit manual-scan fallback.
- `storage` stores local settings, bounded detected-resource data, and short-lived user-click/prompt state. `nativeMessaging` carries only an explicitly selected resource and limited display metadata through the existing allowlisted native host.

The extension does not read cookies, passwords, authorization headers, or browsing history. It sends no browser authentication data to FluxNet and does not bypass DRM, encryption, login restrictions, or paywalls. Only MP4/WebM video and HLS/DASH manifests are treated as video; ordinary files are offered only after a user link click. Browser performance APIs may expose a transferred-byte estimate, not a reliable total size; Chrome may omit MIME type or file size. Signed URLs may expire, and resources requiring browser authentication may fail because FluxNet intentionally does not reuse browser credentials.

FluxNet validates each URL again. The native host is allowlisted to the stable FluxNet extension ID and forwards messages over a per-user Windows named pipe that rejects remote clients. The app retains at most 100 reported resources in memory for the current session; clearing the list or exiting FluxNet removes them. Run extension unit tests with `npm.cmd run test:extension`. Unit tests do not emulate Chrome event timing or verify an installed Chrome-to-FluxNet Native Messaging connection; confirm that flow manually in Chrome or Edge after installation.