const DIRECT_VIDEO_TYPES = new Set(["mp4", "webm", "mov", "m4v", "mkv", "avi", "ogv", "mpeg", "mpg", "3gp"]);
const STREAM_TYPES = new Map([["m3u8", "hls"], ["mpd", "dash"]]);
let settings = { automaticVideoDetection: true, videoOverlay: true, excludedDomains: [] };
const playerOverlays = new WeakMap();
const observedPerformanceEntries = new Set();
const sentCandidateUrls = new Set();
const watchedVideos = new WeakSet();
let refreshQueued = false;

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
  if (/^video\//i.test(mimeType)) return { url: url.href, mediaType: "video", downloadable: true };
  return null;
}

function normalizeSettings(value = {}) {
  return {
    automaticVideoDetection: value.automaticVideoDetection !== false,
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
  if (!settings.automaticVideoDetection || isExcludedPage()) return null;
  const classified = classifyMediaUrl(url, metadata.mimeType || "");
  if (!classified) return null;
  if (sentCandidateUrls.has(classified.url)) return classified;
  sentCandidateUrls.add(classified.url);
  if (sentCandidateUrls.size > 500) sentCandidateUrls.delete(sentCandidateUrls.values().next().value);
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
  void chrome.runtime.sendMessage({ type: "fluxnet:media", resource }).catch(() => {});
  return classified;
}

function positionOverlay(video, host) {
  const rect = video.getBoundingClientRect();
  if (!rect.width || !rect.height) {
    host.hidden = true;
    return;
  }
  host.hidden = false;
  host.style.left = `${Math.max(8, rect.left + rect.width - 178)}px`;
  host.style.top = `${Math.max(8, rect.top + 10)}px`;
}

function addPlayerOverlay(video, url, mimeType) {
  if (!settings.videoOverlay || !video.isConnected) return;
  const classified = classifyMediaUrl(url, mimeType);
  if (!classified?.downloadable) return;
  let overlay = playerOverlays.get(video);
  if (!overlay) {
    const host = document.createElement("div");
    host.dataset.fluxnetVideoOverlay = "true";
    host.style.cssText = "position:fixed;z-index:2147483646;pointer-events:auto";
    const shadow = host.attachShadow({ mode: "closed" });
    const button = document.createElement("button");
    button.type = "button";
    button.textContent = "Download with FluxNet";
    button.style.cssText = "border:1px solid #68dce5;border-radius:6px;padding:7px 10px;background:#0b111c;color:#e3fbfc;font:600 12px 'Segoe UI',sans-serif;box-shadow:0 2px 12px #0008;cursor:pointer";
    button.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      const current = playerOverlays.get(video);
      if (current) void chrome.runtime.sendMessage({
        type: "fluxnet:open-media-prompt",
        url: current.url,
      }).catch(() => {});
    });
    shadow.append(button);
    document.documentElement.append(host);
    overlay = { host, url: classified.url };
    playerOverlays.set(video, overlay);
    const reposition = () => positionOverlay(video, host);
    window.addEventListener("scroll", reposition, { passive: true });
    window.addEventListener("resize", reposition, { passive: true });
    video.addEventListener("loadedmetadata", reposition);
    positionOverlay(video, host);
  } else {
    overlay.url = classified.url;
    positionOverlay(video, overlay.host);
  }
}

function inspectVideo(video) {
  if (!(video instanceof HTMLVideoElement)) return;
  if (!watchedVideos.has(video)) {
    watchedVideos.add(video);
    video.addEventListener("loadedmetadata", () => inspectVideo(video));
  }
  const sources = [...video.querySelectorAll("source")];
  const candidates = [video.currentSrc, video.src, ...sources.map((source) => source.src)];
  for (const url of candidates) {
    if (!url) continue;
    const source = sources.find((element) => element.src === url);
    const mimeType = source?.type || video.getAttribute("type") || "";
    const classified = sendCandidate(url, {
      mimeType,
      width: video.videoWidth,
      height: video.videoHeight,
      fromPlayer: true,
    });
    if (classified?.downloadable) addPlayerOverlay(video, url, mimeType);
  }
}

function scanResources() {
  for (const entry of performance.getEntriesByType("resource")) {
    if (observedPerformanceEntries.has(entry.name)) continue;
    observedPerformanceEntries.add(entry.name);
    const classified = classifyMediaUrl(entry.name);
    if (classified) sendCandidate(entry.name);
  }
  document.querySelectorAll("video").forEach(inspectVideo);
  if (!settings.videoOverlay) {
    document.querySelectorAll("[data-fluxnet-video-overlay]").forEach((element) => element.remove());
  }
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
  let relevant = false;
  for (const mutation of mutations) {
    if (mutation.type === "attributes") {
      if (mutation.target instanceof HTMLVideoElement) inspectVideo(mutation.target);
      else if (mutation.target instanceof HTMLSourceElement) inspectVideo(mutation.target.closest("video"));
      relevant = true;
      continue;
    }
    for (const node of mutation.addedNodes) {
      if (!(node instanceof Element)) continue;
      if (node.matches("video")) inspectVideo(node);
      node.querySelectorAll("video").forEach(inspectVideo);
      if (node.matches("source")) {
        const video = node.closest("video");
        if (video) inspectVideo(video);
      }
      if (node.querySelector("video, source")) relevant = true;
    }
  }
  if (relevant) scheduleScan();
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
        if (classified) sendCandidate(entry.name);
      }
    });
    observer.observe({ type: "resource", buffered: true });
  } catch (error) {
    console.debug("FluxNet resource timing observer unavailable:", error);
  }
}

chrome.runtime.onMessage.addListener((message) => {
  if (message?.type === "fluxnet:collect") scheduleScan();
  if (message?.type === "fluxnet:resource-confirmed") {
    document.querySelectorAll("video").forEach((video) => {
      if (video.currentSrc === message.url || video.src === message.url
        || [...video.querySelectorAll("source")].some((source) => source.src === message.url)) {
        addPlayerOverlay(video, message.url, message.mimeType || "");
      }
    });
  }
  if (message?.type === "fluxnet:settings") {
    settings = normalizeSettings(message.settings);
    scheduleScan();
  }
});

void chrome.storage.local.get("settings").then(({ settings: saved }) => {
  settings = normalizeSettings(saved);
  scanResources();
}).catch((error) => console.error("Could not load FluxNet detection settings:", error));

window.addEventListener("pageshow", scheduleScan);
window.addEventListener("popstate", scheduleScan);
window.addEventListener("hashchange", scheduleScan);
