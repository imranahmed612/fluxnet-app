const DIRECT_VIDEO_TYPES = new Set(["mp4", "webm"]);
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

export function reconnectDelayMinutes(attempt) {
  if (!Number.isInteger(attempt) || attempt < 1 || attempt > 6) return null;
  return Math.min(2 ** (attempt - 1), 5);
}

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
  const mimeVideoType = /^video\/(mp4|webm)(?:;|$)/i.exec(mimeType)?.[1]?.toLowerCase();
  if (mimeVideoType) return { url: url.href, mediaType: mimeVideoType, downloadable: true };
  return null;
}

function parseAttributes(value) {
  const attributes = {};
  for (const match of value.matchAll(/([A-Z0-9-]+)=("[^"]*"|[^,]*)/gi)) {
    attributes[match[1].toUpperCase()] = match[2].replace(/^"|"$/g, "");
  }
  return attributes;
}

function resolveManifestUrl(rawUrl, baseUrl) {
  try {
    const url = new URL(rawUrl, baseUrl);
    return ["http:", "https:"].includes(url.protocol) && !url.username && !url.password ? url.href : null;
  } catch {
    return null;
  }
}

function decodeXmlText(value) {
  return value.replace(/&(?:amp|quot|apos|lt|gt);/g, (entity) => ({
    "&amp;": "&",
    "&quot;": "\"",
    "&apos;": "'",
    "&lt;": "<",
    "&gt;": ">",
  })[entity]);
}

export function parseHlsManifest(source, manifestUrl) {
  const baseUrl = new URL(manifestUrl);
  if (!/^#EXTM3U(?:\s|$)/m.test(source)) throw new Error("The HLS manifest header is invalid.");
  const lines = source.split(/\r?\n/);
  const variants = [];
  const audioTracks = [];
  let pendingVariant = null;
  for (const rawLine of lines) {
    if (variants.length >= 100 && audioTracks.length >= 50) break;
    const line = rawLine.trim();
    if (pendingVariant && line && !line.startsWith("#")) {
      const variantUrl = resolveManifestUrl(line, baseUrl);
      if (variantUrl && variants.length < 100) {
        const [width, height] = (pendingVariant.RESOLUTION || "").split("x").map(Number);
        const videoCodec = /(?:avc1|avc3|hev1|hvc1|vp0[89]|av01|mp4v|theora|dvh1|dvhe)/i.test(pendingVariant.CODECS || "");
        const audioOnlyCodec = /(?:mp4a|ac-3|ec-3|opus|vorbis|flac)/i.test(pendingVariant.CODECS || "")
          && !videoCodec;
        if (!(audioOnlyCodec && !pendingVariant.RESOLUTION)) variants.push({
          url: variantUrl,
          width: Number.isSafeInteger(width) && width > 0 ? width : null,
          height: Number.isSafeInteger(height) && height > 0 ? height : null,
          bandwidth: Number.isSafeInteger(Number(pendingVariant["AVERAGE-BANDWIDTH"] || pendingVariant.BANDWIDTH))
            ? Number(pendingVariant["AVERAGE-BANDWIDTH"] || pendingVariant.BANDWIDTH)
            : null,
          video: width > 0 && height > 0 || videoCodec ? true : null,
        });
      }
      pendingVariant = null;
      continue;
    }
    if (line.startsWith("#EXT-X-STREAM-INF:")) {
      pendingVariant = parseAttributes(line.slice(line.indexOf(":") + 1));
    } else if (line.startsWith("#EXT-X-MEDIA:")) {
      const attributes = parseAttributes(line.slice(line.indexOf(":") + 1));
      if (attributes.TYPE !== "AUDIO") continue;
      if (audioTracks.length < 50) audioTracks.push({
        name: attributes.NAME || "Audio",
        url: attributes.URI ? resolveManifestUrl(attributes.URI, baseUrl) : null,
        groupId: attributes["GROUP-ID"] || null,
        default: attributes.DEFAULT === "YES",
      });
    }
  }
  return { variants, audioTracks };
}

export function parseDashManifest(source, manifestUrl) {
  const baseUrl = new URL(manifestUrl);
  if (!/<MPD(?:\s|>)/i.test(source)) throw new Error("The DASH manifest root is invalid.");
  const variants = [];
  const audioTracks = [];
  for (const adaptation of source.matchAll(/<AdaptationSet\b([^>]*?)(?:\/>|>([\s\S]*?)<\/AdaptationSet\s*>)/gi)) {
    if (variants.length >= 100 && audioTracks.length >= 50) break;
    const adaptationAttributes = parseAttributes(adaptation[1]);
    const contentType = (adaptationAttributes.CONTENTTYPE || adaptationAttributes.MIMETYPE || "").toLowerCase();
    if (contentType.startsWith("audio")) {
      if (audioTracks.length < 50) audioTracks.push({
        name: adaptationAttributes.LANG || "Audio",
        url: null,
        groupId: null,
        default: false,
      });
      continue;
    }
    if (contentType && !contentType.startsWith("video")) continue;
    const parentBase = (adaptation[2] || "").split(/<Representation\b/i, 1)[0];
    const inheritedBase = /<BaseURL(?:\s[^>]*)?>([^<]+)<\/BaseURL\s*>/i.exec(parentBase)?.[1]?.trim();
    for (const representation of (adaptation[2] || "").matchAll(/<Representation\b([^>]*?)(?:\/>|>([\s\S]*?)<\/Representation\s*>)/gi)) {
      if (variants.length >= 100) break;
      const attributes = { ...adaptationAttributes, ...parseAttributes(representation[1]) };
      const mimeType = (attributes.MIMETYPE || "").toLowerCase();
      if (attributes.CONTENTTYPE && attributes.CONTENTTYPE.toLowerCase() !== "video") continue;
      if (mimeType && !mimeType.startsWith("video/")) continue;
      const width = Number(attributes.WIDTH);
      const height = Number(attributes.HEIGHT);
      const representationBody = representation[2] || "";
      const rawBase = /<BaseURL(?:\s[^>]*)?>([^<]+)<\/BaseURL\s*>/i.exec(representationBody)?.[1]?.trim()
        || inheritedBase;
      const base = rawBase ? resolveManifestUrl(decodeXmlText(rawBase), baseUrl) : null;
      if (variants.length < 100) variants.push({
        id: attributes.ID || null,
        url: base,
        width: Number.isSafeInteger(width) && width > 0 ? width : null,
        height: Number.isSafeInteger(height) && height > 0 ? height : null,
        bandwidth: Number.isSafeInteger(Number(attributes.BANDWIDTH)) ? Number(attributes.BANDWIDTH) : null,
      });
    }
  }
  return { variants, audioTracks };
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

export function hasPendingDownloadPrompt(prompts, item) {
  const identity = downloadResourceIdentity(item);
  if (!identity || !prompts || typeof prompts !== "object") return false;
  return Object.values(prompts).some((prompt) => prompt?.kind === "download"
    && downloadResourceIdentity(prompt.resource) === identity);
}

export function takeUserDownloadIntent(item, intents, now = Date.now()) {
  const urls = new Set([item.url, item.finalUrl].filter((value) => typeof value === "string"));
  const recent = intents.filter((entry) => now - entry.at < 30_000);
  const index = recent.findIndex((entry) => urls.has(entry.url)
    && (entry.allowDifferentTab || !Number.isInteger(item.tabId) || item.tabId < 0 || entry.tabId === item.tabId));
  return {
    intent: index >= 0 ? recent.splice(index, 1)[0] : null,
    remaining: recent,
  };
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

export function createNativeMessage(action, pairingCodeOrResource, legacyResource) {
  if (!["sendMedia", "sendDownload", "reportUnsupported"].includes(action)) throw new Error("Unsupported Native Messaging action.");
  if (legacyResource !== undefined) {
    if (!/^[0-9a-f]{32}$/i.test(pairingCodeOrResource)) throw new Error("Invalid FluxNet pairing code.");
    return { action, pairingCode: pairingCodeOrResource, resource: legacyResource };
  }
  return { action, resource: pairingCodeOrResource };
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
