const DIRECT_VIDEO_TYPES = new Set(["mp4", "webm"]);
const STREAM_TYPES = new Map([["m3u8", "hls"], ["mpd", "dash"]]);
let settings = { automaticVideoDetection: true, automaticDownloadDetection: true, videoOverlay: true, excludedDomains: [] };
const overlays = new Map();
const trackedVideos = new Set();
const watchedVideos = new WeakSet();
const sentCandidateUrls = new Set();
const candidateRegistrations = new Map();
const registrationFailures = new Map();
const { videoSourceLabel, canDownloadVideoSource } = globalThis.FluxNetVideoSources;
let refreshQueued = false;
let positionQueued = false;

function classifyMediaUrl(rawUrl, mimeType = "") {
  let url;
  try {
    url = new URL(rawUrl);
  } catch {
    return null;
  }
  if (!["http:", "https:"].includes(url.protocol) || url.username || url.password) return null;
  const extension = url.pathname.toLowerCase().split(".").pop();
  if (STREAM_TYPES.has(extension)) return { url: url.href, mediaType: STREAM_TYPES.get(extension), downloadable: false };
  if (DIRECT_VIDEO_TYPES.has(extension)) return { url: url.href, mediaType: extension, downloadable: true };
  const mimeVideoType = /^video\/(mp4|webm)(?:;|$)/i.exec(mimeType)?.[1]?.toLowerCase();
  if (mimeVideoType) return { url: url.href, mediaType: mimeVideoType, downloadable: true };
  return null;
}

function normalizeSettings(value = {}) {
  return {
    automaticVideoDetection: value.automaticVideoDetection !== false,
    automaticDownloadDetection: value.automaticDownloadDetection !== false,
    videoOverlay: value.videoOverlay !== false,
    excludedDomains: Array.isArray(value.excludedDomains)
      ? value.excludedDomains.map((domain) => String(domain).toLowerCase())
      : [],
  };
}

function isExcludedPage() {
  const hostname = location.hostname.toLowerCase().replace(/\.$/, "");
  return settings.excludedDomains.some((domain) => hostname === domain || hostname.endsWith(`.${domain}`));
}

function sendCandidate(url, metadata = {}) {
  if (!settings.automaticVideoDetection || isExcludedPage()) return Promise.resolve(false);
  const classified = classifyMediaUrl(url, metadata.mimeType || "");
  if (!classified) return Promise.resolve(false);
  if (sentCandidateUrls.has(classified.url)) return Promise.resolve(true);
  const pending = candidateRegistrations.get(classified.url);
  if (pending) return pending;

  const entry = performance.getEntriesByName(classified.url, "resource").at(-1);
  const resource = {
    url: classified.url,
    pageTitle: document.title.slice(0, 512),
    mimeType: metadata.mimeType || "",
    observedBytes: entry && Number.isFinite(entry.transferSize) && entry.transferSize > 0 ? entry.transferSize : null,
    width: metadata.width || null,
    height: metadata.height || null,
    fromPlayer: metadata.fromPlayer === true,
  };
  const registration = Promise.resolve()
    .then(() => chrome.runtime.sendMessage({ type: "fluxnet:media", resource }))
    .then((result) => {
      if (!result?.ok) throw new Error("FluxNet did not confirm this video source.");
      sentCandidateUrls.add(classified.url);
      registrationFailures.delete(classified.url);
      if (sentCandidateUrls.size > 500) sentCandidateUrls.delete(sentCandidateUrls.values().next().value);
      updateOverlaySourceRegistration(classified.url, true);
      return true;
    })
    .catch((error) => {
      registrationFailures.set(classified.url, error instanceof Error ? error.message : String(error));
      if (registrationFailures.size > 500) registrationFailures.delete(registrationFailures.keys().next().value);
      updateOverlaySourceRegistration(classified.url, false);
      console.debug("FluxNet could not confirm a detected video:", error);
      return false;
    })
    .finally(() => candidateRegistrations.delete(classified.url));
  candidateRegistrations.set(classified.url, registration);
  updateOverlaySourceRegistration(classified.url, false);
  return registration;
}

function qualityLabel(width, height) {
  if (!Number.isInteger(height) || height <= 0) return "Resolution unavailable";
  const prefix = `${height}p`;
  return Number.isInteger(width) && width > 0 ? `${prefix} (${width} × ${height})` : prefix;
}

function updateOverlaySourceRegistration(url, registered) {
  for (const overlay of overlays.values()) {
    const source = overlay.availableSources.find((item) => item.url === url);
    if (!source) continue;
    source.registered = registered;
    source.pending = registered ? false : candidateRegistrations.has(url);
    source.registrationError = registered ? "" : registrationFailures.get(url) || "";
    updateSelector(overlay);
  }
}

function registrationState(url) {
  return {
    registered: sentCandidateUrls.has(url),
    pending: candidateRegistrations.has(url),
    registrationError: registrationFailures.get(url) || "",
  };
}

function retrySelectedSourceRegistration(overlay) {
  const source = overlay.availableSources.find((item) => item.url === overlay.quality.value);
  if (!source || source.registered || source.pending) return;
  source.pending = true;
  source.registrationError = "";
  updateSelector(overlay);
  void sendCandidate(source.url, {
    mimeType: `video/${source.mediaType}`,
    width: source.width,
    height: source.height,
    fromPlayer: true,
  });
  Object.assign(source, registrationState(source.url));
  updateSelector(overlay);
}

function updateQualityStatus(overlay) {
  const source = overlay.availableSources.find((item) => item.url === overlay.quality.value);
  if (source?.pending) {
    overlay.status.textContent = "Waiting for FluxNet to confirm this source. Your selection is saved.";
    return;
  }
  if (source && !source.registered) {
    overlay.status.textContent = source.registrationError
      ? `FluxNet could not confirm this source: ${source.registrationError}. Reopen the selector to retry.`
      : "This video source must be confirmed by FluxNet before download.";
    return;
  }
  if (!source?.height) {
    overlay.status.textContent = "Resolution is unavailable; only the original source can be downloaded. No conversion.";
  } else if (source.height === 1080) {
    overlay.status.textContent = "1080p is available. The original source will be downloaded without conversion.";
  } else {
    overlay.status.textContent = `1080p is unavailable. The original ${source.height}p source is available; no conversion.`;
  }
}

function updateSelector(overlay) {
  const { quality, format, status, download } = overlay;
  const requestedFormat = format.value;
  const requestedUrl = quality.value;
  quality.replaceChildren();
  format.replaceChildren();
  const classified = classifyMediaUrl(overlay.url, overlay.mimeType);

  if (classified?.downloadable) {
    const sources = overlay.availableSources.length
      ? overlay.availableSources
      : [{ url: overlay.url, mediaType: classified.mediaType, width: overlay.width, height: overlay.height }];
    const availableFormats = [...new Set(sources.map((source) => source.mediaType))];
    for (const mediaType of availableFormats) {
      const option = document.createElement("option");
      option.value = mediaType;
      option.textContent = mediaType.toUpperCase();
      format.append(option);
    }
    format.value = availableFormats.includes(requestedFormat)
      ? requestedFormat
      : availableFormats.includes("mp4") ? "mp4" : availableFormats[0];
    const formatSources = sources.filter((source) => source.mediaType === format.value);
    const uniqueSources = [...new Map(formatSources.map((source) => [
      `${source.width || ""}x${source.height || ""}:${source.url}`,
      source,
    ])).values()];
    for (const [index, source] of uniqueSources.entries()) {
      const option = document.createElement("option");
      option.value = source.url;
      option.textContent = videoSourceLabel(source, index, uniqueSources.length);
      quality.append(option);
    }
    const selectedSource = uniqueSources.find((source) => source.url === requestedUrl)
      || uniqueSources.find((source) => source.height === 1080)
      || uniqueSources.find((source) => source.url === overlay.url)
      || uniqueSources[0];
    if (selectedSource) quality.value = selectedSource.url;
    const selected = uniqueSources.find((source) => source.url === quality.value);
    format.onchange = () => {
      updateSelector(overlay);
      updateQualityStatus(overlay);
    };
    quality.onchange = () => {
      const selected = uniqueSources.find((source) => source.url === quality.value);
      download.disabled = !canDownloadVideoSource(selected, overlay.downloadStarted);
      updateQualityStatus(overlay);
    };
    download.disabled = !canDownloadVideoSource(selected, overlay.downloadStarted);
    if (!overlay.message) updateQualityStatus(overlay);
    return;
  }

  const variants = Array.isArray(overlay.manifestVariants) ? overlay.manifestVariants : [];
  for (const variant of variants) {
    const option = document.createElement("option");
    option.value = "manifest";
    option.disabled = true;
    option.textContent = `${qualityLabel(variant.width, variant.height)} · ${classified?.mediaType.toUpperCase() || "STREAM"} (unavailable)`;
    quality.append(option);
  }
  if (!variants.length) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "No variant resolutions reported";
    option.disabled = true;
    quality.append(option);
  }
  const formatOption = document.createElement("option");
  formatOption.value = classified?.mediaType || "unsupported";
  formatOption.textContent = `${classified?.mediaType.toUpperCase() || "STREAM"} · unavailable`;
  formatOption.disabled = true;
  format.append(formatOption);
  download.disabled = true;
  if (!overlay.message) {
    status.textContent = variants.length
      ? `${variants.length} variant(s) found. Segment merging is unavailable; no download was started.`
      : "Manifest detected; no downloadable video variant is available.";
  }
}

function positionOverlays() {
  for (const video of trackedVideos) {
    if (!video.isConnected) trackedVideos.delete(video);
  }
  for (const [video, overlay] of overlays) {
    if (!video.isConnected || !settings.videoOverlay) {
      overlay.host.remove();
      overlays.delete(video);
      continue;
    }
    const rect = video.getBoundingClientRect();
    if (rect.width < 80 || rect.height < 50 || rect.bottom <= 0 || rect.top >= innerHeight || rect.right <= 0 || rect.left >= innerWidth) {
      overlay.host.hidden = true;
      continue;
    }
    overlay.host.hidden = false;
    const left = Math.min(innerWidth - 100, Math.max(8, rect.right - 96));
    const top = Math.min(innerHeight - 42, Math.max(8, rect.top + 8));
    overlay.host.style.left = `${left}px`;
    overlay.host.style.top = `${top}px`;
  }
}

function schedulePositionOverlays() {
  if (positionQueued) return;
  positionQueued = true;
  requestAnimationFrame(() => {
    positionQueued = false;
    positionOverlays();
  });
}

function addPlayerOverlay(video, url, mimeType = "", metadata = {}) {
  if (!settings.videoOverlay || !video?.isConnected) return;
  const classified = classifyMediaUrl(url, mimeType);
  if (!classified) return;
  let overlay = overlays.get(video);
  if (!overlay) {
    const host = document.createElement("div");
    host.dataset.fluxnetVideoOverlay = "true";
    host.style.cssText = "position:fixed;z-index:2147483646;pointer-events:none";
    const shadow = host.attachShadow({ mode: "closed" });
    shadow.innerHTML = `
      <style>
        :host{font:12px "Segoe UI",sans-serif;color:#e5f6f7}
        button,select{font:inherit}
        .toggle{display:flex;align-items:center;gap:6px;min-height:30px;padding:5px 8px;border:1px solid #45c5cf;border-radius:7px;background:#0b1521ed;color:#d9fbfc;box-shadow:0 2px 10px #0008;cursor:pointer;pointer-events:auto}
        .toggle:hover{background:#12313a}
        .toggle svg{width:15px;height:15px;fill:none;stroke:#67e3e5;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}
        .panel{position:absolute;top:36px;right:0;width:230px;padding:11px;border:1px solid #294452;border-radius:9px;background:#0b111cf5;box-shadow:0 8px 28px #000b;pointer-events:auto}
        .panel[hidden]{display:none}
        .brand{margin-bottom:8px;color:#67e3e5;font-size:11px;font-weight:700;letter-spacing:.04em}
        label{display:block;margin:8px 0 4px;color:#aab8c8;font-size:10px}
        select{width:100%;padding:6px;border:1px solid #35485b;border-radius:5px;background:#101b29;color:#e5f6f7}
        .status{margin:8px 0;color:#aab8c8;font-size:10px;line-height:1.4}
        .start{width:100%;padding:7px;border:0;border-radius:5px;background:#36b8c2;color:#04161a;font-weight:700;cursor:pointer}
        .start:disabled{opacity:.55;cursor:not-allowed}
      </style>
      <button class="toggle" type="button" aria-label="Download video with FluxNet" title="Download video with FluxNet">
        <svg viewBox="0 0 20 20" aria-hidden="true"><path d="M10 2v10m-4-4 4 4 4-4M3 14v3h14v-3"/></svg><span>FluxNet</span>
      </button>
      <section class="panel" hidden>
        <div class="brand">FLUXNET VIDEO</div>
        <label>Available source</label><select class="quality"></select>
        <label>Format</label><select class="format"></select>
        <div class="status" role="status">Video resource detected.</div>
        <button class="start" type="button">Download with FluxNet</button>
      </section>`;
    const toggle = shadow.querySelector(".toggle");
    const panel = shadow.querySelector(".panel");
    const status = shadow.querySelector(".status");
    const download = shadow.querySelector(".start");
    overlay = {
      host,
      url: classified.url,
      mimeType,
      width: metadata.width || video.videoWidth,
      height: metadata.height || video.videoHeight,
      manifestVariants: [],
      availableSources: [],
      message: "",
      downloadStarted: false,
      quality: shadow.querySelector(".quality"),
      format: shadow.querySelector(".format"),
      status,
      download,
    };
    toggle.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      panel.hidden = !panel.hidden;
      if (!panel.hidden) retrySelectedSourceRegistration(overlay);
    });
    download.addEventListener("click", async (event) => {
      event.preventDefault();
      event.stopPropagation();
      if (download.disabled) return;
      const selected = overlay.availableSources.find((item) => item.url === overlay.quality.value);
      if (!canDownloadVideoSource(selected, overlay.downloadStarted)) return;
      download.disabled = true;
      overlay.message = "Starting FluxNet handoff…";
      status.textContent = overlay.message;
      try {
        const result = await chrome.runtime.sendMessage({
          type: "fluxnet:video-download",
          url: overlay.quality.value,
          format: overlay.format.value,
        });
        if (!result?.ok) throw new Error(result?.error || "FluxNet did not accept the download.");
        overlay.downloadStarted = false;
        overlay.message = "Handoff sent to FluxNet. If you cancel the save dialog, click to retry.";
        status.textContent = overlay.message;
        download.disabled = !canDownloadVideoSource(selected, overlay.downloadStarted);
      } catch (error) {
        overlay.message = `FluxNet unavailable: ${error.message}. No download was started.`;
        status.textContent = overlay.message;
        overlay.downloadStarted = false;
        download.disabled = false;
      }
    });
    shadow.querySelectorAll("button,select").forEach((element) => {
      element.addEventListener("click", (event) => event.stopPropagation());
    });
    document.documentElement.append(host);
    overlays.set(video, overlay);
  }
  const urlChanged = overlay.url !== classified.url;
  if (urlChanged) {
    overlay.message = "";
    overlay.downloadStarted = false;
    overlay.availableSources = [];
  }
  overlay.url = classified.url;
  overlay.mimeType = mimeType;
  const isCurrentSource = classified.url === video.currentSrc;
  overlay.width = metadata.width || (isCurrentSource ? video.videoWidth : null);
  overlay.height = metadata.height || (isCurrentSource ? video.videoHeight : null);
  overlay.manifestVariants = Array.isArray(metadata.manifestVariants) ? metadata.manifestVariants : overlay.manifestVariants;
  if (Array.isArray(metadata.availableSources) && metadata.availableSources.length) {
    overlay.availableSources = metadata.availableSources.map((source) => ({
      ...source,
      ...registrationState(source.url),
    }));
  } else if (classified.downloadable && !overlay.availableSources.length) {
    overlay.availableSources = [{
      url: classified.url,
      mediaType: classified.mediaType,
      width: overlay.width,
      height: overlay.height,
      ...registrationState(classified.url),
    }];
  }
  if (metadata.manifestStatus) overlay.message = metadata.manifestStatus;
  else if (metadata.manifestLoading) overlay.message = "Checking manifest availability…";
  else if (metadata.manifestVariants?.length) overlay.message = "";
  updateSelector(overlay);
  schedulePositionOverlays();
}

function inspectVideo(video) {
  if (!(video instanceof HTMLVideoElement) || !video.isConnected) return;
  trackedVideos.add(video);
  if (!watchedVideos.has(video)) {
    watchedVideos.add(video);
    video.addEventListener("loadedmetadata", () => inspectVideo(video));
  }
  const sources = [...video.querySelectorAll("source")];
  const candidates = [...new Set([video.currentSrc, video.src, ...sources.map((source) => source.src)].filter(Boolean))];
  for (const url of candidates) {
    const source = sources.find((element) => element.src === url);
    const mimeType = source?.type || video.getAttribute("type") || "";
    void sendCandidate(url, {
      mimeType,
      width: url === video.currentSrc ? video.videoWidth : null,
      height: url === video.currentSrc ? video.videoHeight : null,
      fromPlayer: true,
    });
  }
  const selectedUrl = candidates.find((url) => {
    const source = sources.find((element) => element.src === url);
    const classified = classifyMediaUrl(url, source?.type || video.getAttribute("type") || "");
    return classified?.downloadable && classified.mediaType === "mp4";
  }) || candidates.find((url) => {
    const source = sources.find((element) => element.src === url);
    return classifyMediaUrl(url, source?.type || video.getAttribute("type") || "")?.downloadable;
  }) || candidates.find((url) => {
    const source = sources.find((element) => element.src === url);
    return classifyMediaUrl(url, source?.type || video.getAttribute("type") || "");
  });
  if (!selectedUrl) return;
  const selectedSource = sources.find((source) => source.src === selectedUrl);
  const selectedMimeType = selectedSource?.type || video.getAttribute("type") || "";
  const selected = classifyMediaUrl(selectedUrl, selectedMimeType);
  if (selected) {
    const availableSources = candidates
      .map((url) => {
        const source = sources.find((element) => element.src === url);
        const mimeType = source?.type || video.getAttribute("type") || "";
        const classified = classifyMediaUrl(url, mimeType);
        if (!classified?.downloadable) return null;
        return {
          url: classified.url,
          mediaType: classified.mediaType,
          width: url === video.currentSrc ? video.videoWidth || null : null,
          height: url === video.currentSrc ? video.videoHeight || null : null,
          ...registrationState(classified.url),
        };
      })
      .filter(Boolean);
    addPlayerOverlay(video, selectedUrl, selectedMimeType, {
      width: selectedUrl === video.currentSrc ? video.videoWidth : null,
      height: selectedUrl === video.currentSrc ? video.videoHeight : null,
      manifestLoading: !selected.downloadable,
      availableSources,
    });
  }
}

function scanResources() {
  for (const entry of performance.getEntriesByType("resource")) {
    const classified = classifyMediaUrl(entry.name);
    if (classified && !classified.downloadable) sendCandidate(entry.name);
  }
  document.querySelectorAll("video").forEach(inspectVideo);
  schedulePositionOverlays();
}

function inspectAddedNode(node) {
  if (!(node instanceof Element)) return;
  const videos = new Set();
  if (node.matches("video")) videos.add(node);
  node.querySelectorAll("video").forEach((video) => videos.add(video));
  if (node.matches("source")) {
    const video = node.closest("video");
    if (video) videos.add(video);
  }
  node.querySelectorAll("source").forEach((source) => {
    const video = source.closest("video");
    if (video) videos.add(video);
  });
  videos.forEach(inspectVideo);
}

function scheduleScan() {
  if (refreshQueued) return;
  refreshQueued = true;
  queueMicrotask(() => {
    refreshQueued = false;
    scanResources();
  });
}

const mutationObserver = new MutationObserver((mutations) => {
  for (const mutation of mutations) {
    if (mutation.type === "attributes") {
      if (mutation.target instanceof HTMLVideoElement) inspectVideo(mutation.target);
      else if (mutation.target instanceof HTMLSourceElement) inspectVideo(mutation.target.closest("video"));
      continue;
    }
    mutation.addedNodes.forEach(inspectAddedNode);
  }
  schedulePositionOverlays();
});

mutationObserver.observe(document.documentElement, {
  childList: true,
  subtree: true,
  attributes: true,
  attributeFilter: ["src", "type"],
});

if ("PerformanceObserver" in window) {
  try {
    const observer = new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) {
        const classified = classifyMediaUrl(entry.name);
        if (classified && !classified.downloadable) sendCandidate(entry.name);
      }
    });
    observer.observe({ type: "resource", buffered: true });
  } catch (error) {
    console.debug("FluxNet resource timing observer unavailable:", error);
  }
}

document.addEventListener("click", (event) => {
  if (!settings.automaticDownloadDetection || event.button !== 0 || event.defaultPrevented
    || event.ctrlKey || event.metaKey || event.shiftKey || event.altKey || !(event.target instanceof Element)) return;
  const anchor = event.target.closest("a[href]");
  if (!anchor) return;
  let url;
  try {
    url = new URL(anchor.href);
  } catch {
    return;
  }
  if (!["http:", "https:"].includes(url.protocol) || url.username || url.password) return;
  void chrome.runtime.sendMessage({
    type: "fluxnet:user-download-click",
    url: url.href,
    filename: anchor.download || null,
    allowNewTab: Boolean(anchor.target && !["_self", "_top"].includes(anchor.target.toLowerCase())),
    pageTitle: document.title.slice(0, 512),
    pageDomain: location.hostname.slice(0, 512),
  }).catch((error) => console.debug("FluxNet could not record the user-clicked download link:", error));
}, true);

chrome.runtime.onMessage.addListener((message) => {
  if (message?.type === "fluxnet:collect") scheduleScan();
  if (message?.type === "fluxnet:resource-confirmed") {
    const confirmed = classifyMediaUrl(message.url, message.mimeType || "");
    if (confirmed?.downloadable) {
      sentCandidateUrls.add(confirmed.url);
      registrationFailures.delete(confirmed.url);
      updateOverlaySourceRegistration(confirmed.url, true);
    }
    let matchedPlayer = false;
    trackedVideos.forEach((video) => {
      if (video.currentSrc === message.url || video.src === message.url
        || [...video.querySelectorAll("source")].some((source) => source.src === message.url)) {
        matchedPlayer = true;
        const existing = overlays.get(video);
        if (existing?.availableSources.length) return;
        addPlayerOverlay(video, message.url, message.mimeType || "", message);
      }
    });
    if (!matchedPlayer && (message.mediaType === "hls" || message.mediaType === "dash")) {
      const visiblePlayers = [...trackedVideos].filter((video) => {
        const rect = video.getBoundingClientRect();
        return rect.width >= 80 && rect.height >= 50 && rect.bottom > 0 && rect.top < innerHeight;
      });
      if (visiblePlayers.length === 1) {
        addPlayerOverlay(visiblePlayers[0], message.url, message.mimeType || "", message);
      }
    }
  }
  if (message?.type === "fluxnet:settings") {
    settings = normalizeSettings(message.settings);
    if (!settings.videoOverlay) schedulePositionOverlays();
    else scheduleScan();
  }
});

window.addEventListener("pageshow", scheduleScan);
window.addEventListener("popstate", scheduleScan);
window.addEventListener("hashchange", scheduleScan);
window.addEventListener("resize", schedulePositionOverlays, { passive: true });
window.addEventListener("scroll", schedulePositionOverlays, { passive: true, capture: true });

void chrome.storage.local.get("settings").then(({ settings: saved }) => {
  settings = normalizeSettings(saved);
  scanResources();
}).catch((error) => console.error("Could not load FluxNet detection settings:", error));
