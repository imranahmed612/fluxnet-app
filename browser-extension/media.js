const DIRECT_VIDEO_TYPES = new Set(["mp4", "webm", "mov", "m4v", "mkv", "avi", "ogv", "mpeg", "mpg", "3gp"]);
const STREAM_TYPES = new Map([["m3u8", "hls"], ["mpd", "dash"]]);
export const DEFAULT_FILE_TYPES = [
  "mp4", "webm", "mov", "m4v", "mkv", "avi", "ogv", "mpeg", "mpg", "3gp",
  "mp3", "m4a", "aac", "wav", "flac", "ogg", "pdf", "zip", "rar", "7z",
  "exe", "msi", "apk", "jpg", "jpeg", "png", "gif", "webp", "txt", "doc",
  "docx", "xls", "xlsx", "ppt", "pptx", "csv", "iso", "dmg", "tar", "gz",
];
export const DEFAULT_SETTINGS = {
  automaticVideoDetection: true,
  automaticDownloadDetection: true,
  videoOverlay: true,
  fileTypes: DEFAULT_FILE_TYPES,
  excludedDomains: [],
  notificationBehavior: "badgeOnly",
};

export function classifyMediaUrl(rawUrl, mimeType = "") {
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

export function deduplicateResources(resources) {
  const unique = new Map();
  for (const resource of resources) {
    const classified = classifyMediaUrl(resource.url, resource.mimeType);
    if (!classified || unique.has(classified.url)) continue;
    unique.set(classified.url, { ...resource, ...classified });
  }
  return [...unique.values()];
}

export function normalizeSettings(value = {}) {
  const fileTypes = Array.isArray(value.fileTypes)
    ? [...new Set(value.fileTypes.map((type) => String(type).trim().toLowerCase().replace(/^\./, "")).filter((type) => /^[a-z0-9]{1,10}$/.test(type)))]
    : DEFAULT_FILE_TYPES;
  const excludedDomains = Array.isArray(value.excludedDomains)
    ? [...new Set(value.excludedDomains.map((domain) => String(domain).trim().toLowerCase().replace(/^\.+|\.+$/g, "")).filter((domain) => /^[a-z0-9.-]{1,253}$/.test(domain)))]
    : [];
  return {
    automaticVideoDetection: value.automaticVideoDetection !== false,
    automaticDownloadDetection: value.automaticDownloadDetection !== false,
    videoOverlay: value.videoOverlay !== false,
    fileTypes,
    excludedDomains,
    notificationBehavior: value.notificationBehavior === "desktopPrompt" ? "desktopPrompt" : "badgeOnly",
  };
}

export function isExcludedDomain(hostname, excludedDomains) {
  const host = String(hostname || "").toLowerCase().replace(/\.$/, "");
  return excludedDomains.some((domain) => host === domain || host.endsWith(`.${domain}`));
}

export function isEligibleDownload(item, settings = DEFAULT_SETTINGS) {
  if (!item || typeof item.url !== "string" || !/^https?:\/\//i.test(item.url)) return false;
  let url;
  try {
    url = new URL(item.finalUrl || item.url);
  } catch {
    return false;
  }
  if (!["http:", "https:"].includes(url.protocol) || url.username || url.password) return false;
  if (isExcludedDomain(url.hostname, settings.excludedDomains ?? [])) return false;
  if (item.referrer) {
    try {
      if (isExcludedDomain(new URL(item.referrer).hostname, settings.excludedDomains ?? [])) return false;
    } catch {
      return false;
    }
  }
  const fileTypes = settings.fileTypes ?? DEFAULT_FILE_TYPES;
  const filename = String(item.filename || url.pathname).toLowerCase();
  const extension = filename.split(/[\\/]/).pop()?.split(".").pop();
  return Boolean(extension && fileTypes.includes(extension));
}

export function createDownloadResource(item) {
  if (!item || typeof item.url !== "string") return null;
  let url;
  try {
    url = new URL(item.finalUrl || item.url);
  } catch {
    return null;
  }
  if (!["http:", "https:"].includes(url.protocol) || url.username || url.password) return null;
  const filename = String(item.filename || url.pathname.split("/").pop() || "download").split(/[\\/]/).pop().slice(0, 512);
  let pageDomain = "";
  try {
    const source = item.referrer ? new URL(item.referrer) : url;
    pageDomain = ["http:", "https:"].includes(source.protocol) ? source.hostname.slice(0, 512) : url.hostname.slice(0, 512);
  } catch {
    pageDomain = url.hostname.slice(0, 512);
  }
  return {
    url: url.href,
    mediaType: filename.toLowerCase().split(".").pop() || "download",
    pageTitle: filename,
    pageDomain,
    tabId: Number.isInteger(item.tabId) && item.tabId >= 0 ? item.tabId : null,
    filename,
    mimeType: typeof item.mime === "string" ? item.mime.slice(0, 128) : null,
    totalBytes: Number.isSafeInteger(item.totalBytes) && item.totalBytes >= 0 ? item.totalBytes : null,
    observedBytes: Number.isSafeInteger(item.bytesReceived) && item.bytesReceived > 0 ? item.bytesReceived : null,
    width: null,
    height: null,
  };
}

export function downloadResourceIdentity(item) {
  try {
    const url = new URL(item.finalUrl || item.url);
    url.search = "";
    url.hash = "";
    const filename = String(item.filename || "").split(/[\\/]/).pop() || url.pathname.split("/").pop() || "";
    return `${url.href}\n${filename}`;
  } catch {
    return null;
  }
}

export function downloadPromptDecision(decision, fileType, remember, settings) {
  if (decision !== "fluxnet" && decision !== "browser") throw new Error("Unknown download prompt decision.");
  if (!remember || !/^[a-z0-9]{1,10}$/i.test(fileType)) return null;
  const next = normalizeSettings(settings);
  if (decision === "browser") next.fileTypes = next.fileTypes.filter((type) => type !== fileType.toLowerCase());
  else if (!next.fileTypes.includes(fileType.toLowerCase())) next.fileTypes.push(fileType.toLowerCase());
  return next;
}

export function validateMediaMessage(message) {
  if (!message || typeof message !== "object" || Array.isArray(message)) return false;
  if (typeof message.url !== "string" || message.url.length > 8192) return false;
  const classified = classifyMediaUrl(message.url, message.mimeType);
  return Boolean(
    classified?.downloadable
    && classified.mediaType === message.mediaType
    && typeof message.pageTitle === "string"
    && message.pageTitle.length <= 512
    && typeof message.pageDomain === "string"
    && message.pageDomain.length <= 512
  );
}

export function validateDownloadMessage(message) {
  if (!message || typeof message !== "object" || Array.isArray(message)) return false;
  if (typeof message.url !== "string" || message.url.length > 8192) return false;
  let url;
  try {
    url = new URL(message.url);
  } catch {
    return false;
  }
  return ["http:", "https:"].includes(url.protocol)
    && !url.username
    && !url.password
    && typeof message.pageTitle === "string"
    && message.pageTitle.length <= 512
    && typeof message.pageDomain === "string"
    && message.pageDomain.length <= 512
    && typeof message.mediaType === "string"
    && /^[a-z0-9.-]{1,32}$/i.test(message.mediaType)
    && (message.mimeType === null || typeof message.mimeType === "string")
    && (message.totalBytes === null || (Number.isSafeInteger(message.totalBytes) && message.totalBytes >= 0));
}

export function createNativeMessage(action, pairingCode, resource) {
  if (!/^[0-9a-f]{32}$/i.test(pairingCode)) throw new Error("Invalid FluxNet pairing code.");
  if (!["sendMedia", "sendDownload", "reportUnsupported"].includes(action)) throw new Error("Unsupported Native Messaging action.");
  return { action, pairingCode, resource };
}

export function validateUnsupportedMessage(message) {
  if (!message || typeof message !== "object" || Array.isArray(message)) return false;
  if (typeof message.url !== "string" || message.url.length > 8192) return false;
  const classified = classifyMediaUrl(message.url);
  return Boolean(
    classified
    && !classified.downloadable
    && classified.mediaType === message.mediaType
    && typeof message.pageTitle === "string"
    && message.pageTitle.length <= 512
    && typeof message.pageDomain === "string"
    && message.pageDomain.length <= 512
  );
}
