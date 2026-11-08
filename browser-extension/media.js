const DIRECT_VIDEO_TYPES = new Set(["mp4", "webm"]);
const STREAM_TYPES = new Map([["m3u8", "hls"], ["mpd", "dash"]]);
const MAX_HLS_MANIFEST_BYTES = 1_048_576;
const MAX_HLS_SEGMENTS = 64;
const MAX_HLS_EXPECTED_BYTES = 2 * 1024 * 1024 * 1024;
const MAX_HLS_HANDOFF_BYTES = 48 * 1024;
const NON_VIDEO_EXTENSIONS = new Set([
  "aac", "aiff", "amr", "apk", "ac3", "csv", "dmg", "doc", "docx", "exe",
  "flac", "gif", "gz", "iso", "jpeg", "jpg", "m4a", "m4b", "mid", "midi", "mp2",
  "mp3", "msi", "opus", "pdf", "png", "ppt", "pptx", "rar", "7z", "tar",
  "txt", "wav", "webp", "wma", "xls", "xlsx", "zip",
]);
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

export function classifyMediaUrl(rawUrl, mimeType = "", playableVideo = false) {
  let url;
  try {
    url = new URL(rawUrl);
  } catch {
    return null;
  }
  if (!["http:", "https:"].includes(url.protocol) || url.username || url.password) return null;
  const extension = url.pathname.toLowerCase().split(".").pop();
  if (STREAM_TYPES.has(extension)) return { url: url.href, mediaType: STREAM_TYPES.get(extension), downloadable: false };
  if (NON_VIDEO_EXTENSIONS.has(extension)) return null;
  const normalizedMime = String(mimeType).split(";")[0].trim().toLowerCase();
  if (normalizedMime.startsWith("image/") || normalizedMime.startsWith("audio/")
    || ["application/pdf", "application/zip", "application/x-zip-compressed", "application/x-msdownload",
      "application/x-executable", "text/html", "text/plain"].includes(normalizedMime)) return null;
  if (DIRECT_VIDEO_TYPES.has(extension)) return { url: url.href, mediaType: extension, downloadable: true };
  const mimeVideoType = /^(?:video\/)(mp4|webm)$/.exec(normalizedMime)?.[1];
  if (mimeVideoType) return { url: url.href, mediaType: mimeVideoType, downloadable: true };
  if (playableVideo && !NON_VIDEO_EXTENSIONS.has(extension)
    && (!normalizedMime || normalizedMime === "application/octet-stream" || normalizedMime === "binary/octet-stream"
      || normalizedMime.startsWith("video/"))) {
    return { url: url.href, mediaType: "unsupported-video", downloadable: false };
  }
  return null;
}

function parseAttributes(value) {
  const attributes = {};
  for (const match of value.matchAll(/([A-Z0-9-]+)=("[^"]*"|[^,]*)/gi)) {
    attributes[match[1].toUpperCase()] = match[2].replace(/^"|"$/g, "");
  }
  return attributes;
}

function parseHlsAttributes(value) {
  const attributes = {};
  const parts = [];
  let part = "";
  let quoted = false;
  for (const character of value) {
    if (character === "\"") quoted = !quoted;
    if (character === "," && !quoted) {
      parts.push(part);
      part = "";
    } else {
      part += character;
    }
  }
  if (quoted) throw new Error("The HLS attribute list contains an unterminated quoted value.");
  if (part || value.length) parts.push(part);
  for (const part of parts) {
    const separator = part.indexOf("=");
    if (separator <= 0) throw new Error("The HLS attribute list is malformed.");
    const name = part.slice(0, separator).trim();
    const rawValue = part.slice(separator + 1).trim();
    if (!/^[A-Z0-9-]+$/.test(name) || !rawValue || Object.hasOwn(attributes, name)) {
      throw new Error("The HLS attribute list is malformed.");
    }
    if (rawValue.startsWith("\"") !== rawValue.endsWith("\"")) {
      throw new Error("The HLS attribute list contains an unterminated quoted value.");
    }
    if (!rawValue.startsWith("\"") && rawValue.includes("\"")) {
      throw new Error("The HLS attribute list contains an invalid quote.");
    }
    if (rawValue.startsWith("\"") && rawValue.slice(1, -1).includes("\"")) {
      throw new Error("The HLS attribute list contains an invalid quoted value.");
    }
    attributes[name] = rawValue.startsWith("\"") ? rawValue.slice(1, -1) : rawValue;
  }
  return attributes;
}

function parseHlsByteRange(value, url, previousRangeEnds, allowDefaultOffset = false) {
  const match = /^([1-9]\d*)(?:@(\d+))?$/.exec(value);
  if (!match) throw new Error("The HLS byte range is invalid.");
  const length = Number(match[1]);
  const explicitOffset = match[2] === undefined ? null : Number(match[2]);
  const offset = explicitOffset ?? (allowDefaultOffset ? 0 : previousRangeEnds.get(url));
  if (!Number.isSafeInteger(length) || !Number.isSafeInteger(offset)
    || !Number.isSafeInteger(offset + length)) {
    throw new Error("The HLS byte range is outside the supported numeric range.");
  }
  previousRangeEnds.set(url, offset + length);
  return { length, offset };
}

function parseHlsMediaPlaylist(source, manifestUrl, manifestSizeBytes) {
  const lines = source.split(/\r?\n/);
  const segments = [];
  const previousRangeEnds = new Map();
  let init = null;
  let pendingDuration = null;
  let pendingRange = null;
  let ended = false;
  let hasMediaPlaylistTag = false;
  let hasTargetDuration = false;

  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) continue;
    if (!line.startsWith("#")) {
      if (ended) throw new Error("The HLS playlist contains media after EXT-X-ENDLIST.");
      if (pendingDuration === null) {
        if (hasMediaPlaylistTag) throw new Error("The HLS playlist contains a segment without EXTINF.");
        continue;
      }
      if (!init) throw new Error("The HLS media playlist is missing its EXT-X-MAP initialization segment.");
      const url = resolveManifestUrl(line, manifestUrl);
      if (!url) throw new Error("An HLS media segment URL is invalid.");
      const byteRange = pendingRange
        ? parseHlsByteRange(pendingRange, url, previousRangeEnds)
        : null;
      segments.push({ url, ...(byteRange ? { byteRange } : {}) });
      if (segments.length > MAX_HLS_SEGMENTS) {
        throw new Error(`The HLS playlist exceeds the ${MAX_HLS_SEGMENTS}-segment limit.`);
      }
      pendingDuration = null;
      pendingRange = null;
      continue;
    }
    if (line.startsWith("#EXTINF:")) {
      hasMediaPlaylistTag = true;
      if (pendingDuration !== null || ended) throw new Error("The HLS playlist has an unexpected EXTINF tag.");
      const duration = Number(line.slice("#EXTINF:".length).split(",", 1)[0]);
      if (!Number.isFinite(duration) || duration <= 0) throw new Error("The HLS segment duration is invalid.");
      pendingDuration = duration;
    } else if (line.startsWith("#EXT-X-MAP:")) {
      hasMediaPlaylistTag = true;
      if (ended) throw new Error("The HLS playlist contains an initialization segment after EXT-X-ENDLIST.");
      const attributes = parseHlsAttributes(line.slice("#EXT-X-MAP:".length));
      if (Object.keys(attributes).some((name) => !["URI", "BYTERANGE"].includes(name))) {
        throw new Error("The HLS initialization segment contains unsupported attributes.");
      }
      const url = resolveManifestUrl(attributes.URI, manifestUrl);
      if (!url) throw new Error("The HLS EXT-X-MAP initialization URL is missing or invalid.");
      if (new URL(url).pathname.toLowerCase().endsWith(".m3u8")) {
        throw new Error("An HLS playlist cannot be used as its own initialization segment.");
      }
      const byteRange = attributes.BYTERANGE
        ? parseHlsByteRange(attributes.BYTERANGE, url, previousRangeEnds, true)
        : null;
      const nextInit = { url, ...(byteRange ? { byteRange } : {}) };
      if (init && (init.url !== nextInit.url
        || JSON.stringify(init.byteRange ?? null) !== JSON.stringify(nextInit.byteRange ?? null))) {
        throw new Error("Changing HLS initialization segments are not supported in Phase 1.");
      }
      init = nextInit;
    } else if (line.startsWith("#EXT-X-BYTERANGE:")) {
      if (pendingRange !== null || pendingDuration === null) {
        throw new Error("The HLS byte range does not apply to a pending media segment.");
      }
      pendingRange = line.slice("#EXT-X-BYTERANGE:".length).trim();
    } else if (line.startsWith("#EXT-X-KEY:")) {
      const attributes = parseHlsAttributes(line.slice("#EXT-X-KEY:".length));
      if (attributes.METHOD !== "NONE" || Object.keys(attributes).some((name) => name !== "METHOD")) {
        throw new Error("Encrypted or key-signaled HLS playlists are not supported.");
      }
    } else if (line.startsWith("#EXT-X-SESSION-KEY:")) {
      throw new Error("HLS session keys and DRM-protected playlists are not supported.");
    } else if (line.startsWith("#EXT-X-TARGETDURATION:")) {
      const duration = Number(line.slice("#EXT-X-TARGETDURATION:".length));
      if (hasTargetDuration || !Number.isSafeInteger(duration) || duration <= 0) {
        throw new Error("The HLS target duration is invalid.");
      }
      hasTargetDuration = true;
    } else if (/^#EXT-X-(?:DISCONTINUITY|DISCONTINUITY-SEQUENCE|GAP|PART|PRELOAD-HINT|SKIP|RENDITION-REPORT|SERVER-CONTROL|I-FRAMES-ONLY|DEFINE)(?::|$)/.test(line)) {
      throw new Error("Discontinuous, partial, delta, or live HLS playlists are not supported.");
    } else if (line === "#EXT-X-ENDLIST") {
      hasMediaPlaylistTag = true;
      if (pendingDuration !== null || pendingRange !== null) {
        throw new Error("The HLS playlist ends before its final media segment.");
      }
      ended = true;
    } else if (line.startsWith("#EXT-X-STREAM-INF:") || line.startsWith("#EXT-X-I-FRAME-STREAM-INF:")) {
      return null;
    }
  }

  if (!hasMediaPlaylistTag) return null;
  if (!hasTargetDuration) throw new Error("The HLS media playlist is missing EXT-X-TARGETDURATION.");
  if (!ended) throw new Error("The HLS media playlist is not complete (EXT-X-ENDLIST is missing).");
  if (!init) throw new Error("The HLS media playlist is missing its EXT-X-MAP initialization segment.");
  if (pendingDuration !== null || pendingRange !== null || segments.length === 0) {
    throw new Error("The HLS media playlist is incomplete or contains no media segments.");
  }
  if (segments.some(({ url }) => url.endsWith(".m3u8"))) {
    throw new Error("Nested HLS playlists cannot be used as media segments.");
  }
  const allRangesKnown = Boolean(init.byteRange)
    && segments.every((segment) => Boolean(segment.byteRange));
  let expectedBytes = null;
  if (allRangesKnown) {
    expectedBytes = segments.reduce((total, segment) => total + segment.byteRange.length, init.byteRange.length);
    if (!Number.isSafeInteger(expectedBytes) || expectedBytes > MAX_HLS_EXPECTED_BYTES) {
      throw new Error("The HLS playlist exceeds the 2 GiB download limit.");
    }
  }
  const playlist = {
    version: 1,
    container: "fmp4",
    manifestSizeBytes,
    init,
    segments,
    expectedBytes,
  };
  if (new TextEncoder().encode(JSON.stringify(playlist)).byteLength > MAX_HLS_HANDOFF_BYTES) {
    throw new Error("The HLS playlist exceeds the safe Native Messaging handoff limit.");
  }
  return playlist;
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
  const manifestSizeBytes = new TextEncoder().encode(source).byteLength;
  if (manifestSizeBytes > MAX_HLS_MANIFEST_BYTES) {
    throw new Error("The HLS manifest exceeds the 1 MiB inspection limit.");
  }
  const playlist = parseHlsMediaPlaylist(source, baseUrl.href, manifestSizeBytes);
  if (playlist) return { variants: [], audioTracks: [], playlist };
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
  return { variants, audioTracks, playlist: null };
}

export function validateHlsPlaylist(playlist, manifestUrl) {
  if (!playlist || typeof playlist !== "object" || Array.isArray(playlist)
    || playlist.version !== 1 || playlist.container !== "fmp4"
    || !Number.isSafeInteger(playlist.manifestSizeBytes)
    || playlist.manifestSizeBytes < 1 || playlist.manifestSizeBytes > MAX_HLS_MANIFEST_BYTES
    || !Array.isArray(playlist.segments) || playlist.segments.length < 1
    || playlist.segments.length > MAX_HLS_SEGMENTS) return false;
  let manifest;
  try {
    manifest = new URL(manifestUrl);
  } catch {
    return false;
  }
  if (!["http:", "https:"].includes(manifest.protocol) || !manifest.pathname.toLowerCase().endsWith(".m3u8")) {
    return false;
  }
  const validResource = (resource) => {
    if (!resource || typeof resource !== "object" || typeof resource.url !== "string"
      || resource.url.length > 8192) return false;
    let url;
    try {
      url = new URL(resource.url);
    } catch {
      return false;
    }
    if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || url.hash) return false;
    if (resource.byteRange === undefined) return true;
    const range = resource.byteRange;
    return range && Number.isSafeInteger(range.length) && range.length > 0
      && Number.isSafeInteger(range.offset) && range.offset >= 0
      && Number.isSafeInteger(range.offset + range.length);
  };
  if (!validResource(playlist.init)
    || new URL(playlist.init.url).pathname.toLowerCase().endsWith(".m3u8")
    || !playlist.segments.every(validResource)
    || playlist.segments.some((segment) => new URL(segment.url).pathname.toLowerCase().endsWith(".m3u8"))) {
    return false;
  }
  const allRangesKnown = Boolean(playlist.init.byteRange)
    && playlist.segments.every((segment) => Boolean(segment.byteRange));
  let expectedBytes = null;
  if (allRangesKnown) {
    expectedBytes = playlist.segments.reduce(
      (total, segment) => total + segment.byteRange.length,
      playlist.init.byteRange.length,
    );
    if (!Number.isSafeInteger(expectedBytes) || expectedBytes > MAX_HLS_EXPECTED_BYTES) return false;
  }
  if (playlist.expectedBytes !== expectedBytes) return false;
  return new TextEncoder().encode(JSON.stringify(playlist)).byteLength <= MAX_HLS_HANDOFF_BYTES;
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

export function browserMediaIdentity(resource) {
  try {
    const url = new URL(resource.url);
    url.hash = "";
    const hostname = url.hostname.toLowerCase();
    const isInstagramCdn = hostname === "cdninstagram.com" || hostname.endsWith(".cdninstagram.com")
      || hostname === "fbcdn.net" || hostname.endsWith(".fbcdn.net");
    if (isInstagramCdn) url.search = "";
    return `${resource.mediaType}\n${url.href}`;
  } catch {
    return null;
  }
}

export function deduplicateResources(resources) {
  const unique = new Map();
  for (const resource of resources) {
    const classified = classifyMediaUrl(
      resource.url,
      resource.mimeType,
      resource.mediaType === "unsupported-video"
        && Number.isInteger(resource.width) && resource.width > 0
        && Number.isInteger(resource.height) && resource.height > 0,
    );
    const identity = classified && browserMediaIdentity({ ...resource, ...classified });
    if (!classified || !identity) continue;
    const previous = unique.get(identity);
    if (previous) {
      const hlsPlaylist = previous.hlsPlaylist || resource.hlsPlaylist || null;
      const hlsDownloadable = classified.mediaType === "hls"
        && validateHlsPlaylist(hlsPlaylist, previous.url);
      unique.set(identity, {
        ...resource,
        ...previous,
        hlsPlaylist,
        downloadable: previous.downloadable === true
          || classified.downloadable
          || hlsDownloadable,
      });
      continue;
    }
    const hlsPlaylist = resource.hlsPlaylist || null;
    const hlsDownloadable = classified.mediaType === "hls"
      && validateHlsPlaylist(hlsPlaylist, resource.url);
    unique.set(identity, {
      ...previous,
      ...resource,
      ...classified,
      hlsPlaylist,
      downloadable: classified.downloadable || hlsDownloadable,
    });
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

export function createVideoDownloadFilename(resource) {
  const sourceType = String(resource?.mediaType || "").toLowerCase();
  if (!["mp4", "webm", "hls"].includes(sourceType)) return "";
  const mediaType = sourceType === "hls" ? "mp4" : sourceType;
  const pageTitle = String(resource?.pageTitle || "").trim();
  const looksLikeUrl = /^(?:https?:\/\/|www\.)/i.test(pageTitle)
    || (!/\s/.test(pageTitle) && /\.(?:mp4|webm)(?:[?#].*)?$/i.test(pageTitle));
  const genericTitle = ["", "instagram", "instagram video", "facebook", "facebook video"]
    .includes(pageTitle.toLowerCase());
  const pageDomain = String(resource?.pageDomain || "").toLowerCase().replace(/^www\./, "");
  const fallback = pageDomain === "instagram.com" || pageDomain.endsWith(".instagram.com")
    ? "Instagram_Video"
    : pageDomain === "facebook.com" || pageDomain.endsWith(".facebook.com")
      || pageDomain === "fb.watch" || pageDomain.endsWith(".fb.watch")
      ? "Facebook_Video"
      : "Browser_Video";
  const title = !genericTitle && !looksLikeUrl ? pageTitle : fallback;
  const stem = [...title]
    .map((character) => /[<>:"/\\|?*\u0000-\u001f]/.test(character) ? "_" : character)
    .join("")
    .trim()
    .replace(/[ .]+$/g, "")
    .slice(0, 80)
    .replace(/[ .]+$/g, "") || fallback;
  return `${stem}.${mediaType}`;
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

export function appendProcessedDownloadId(processedIds, downloadId) {
  if (!Number.isInteger(downloadId) || downloadId < 0) return null;
  const ids = Array.isArray(processedIds) ? processedIds : [];
  if (ids.includes(downloadId)) return null;
  return [...ids, downloadId];
}

export function takeUserDownloadIntent(item, intents, now = Date.now()) {
  const urls = new Set([item.url, item.finalUrl].filter((value) => typeof value === "string"));
  const recent = intents.filter((entry) => now - entry.at < 30_000
    && (entry.gestureKind === "trusted" || entry.gestureKind === "correlated"));
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
  if (message.mediaType === "hls") {
    return classified?.mediaType === "hls"
      && message.downloadable === true
      && validateHlsPlaylist(message.hlsPlaylist, message.url)
      && typeof message.pageTitle === "string"
      && message.pageTitle.length <= 512
      && typeof message.pageDomain === "string"
      && message.pageDomain.length <= 512;
  }
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
  const validHls = message.mediaType === "hls"
    && message.downloadable === true
    && validateHlsPlaylist(message.hlsPlaylist, message.url);
  return ["http:", "https:"].includes(url.protocol)
    && !url.username
    && !url.password
    && typeof message.pageTitle === "string"
    && message.pageTitle.length <= 512
    && typeof message.pageDomain === "string"
    && message.pageDomain.length <= 512
    && typeof message.mediaType === "string"
    && /^[a-z0-9.-]{1,32}$/i.test(message.mediaType)
    && (message.mediaType !== "hls" || validHls)
    && (message.hlsPlaylist == null || validHls)
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
  const playableVideo = message.mediaType === "unsupported-video"
    && Number.isInteger(message.width) && message.width > 0
    && Number.isInteger(message.height) && message.height > 0;
  const classified = classifyMediaUrl(message.url, message.mimeType, playableVideo);
  return Boolean(
    classified
    && !classified.downloadable
    && classified.mediaType === message.mediaType
    && (message.mediaType !== "unsupported-video" || playableVideo)
    && typeof message.pageTitle === "string"
    && message.pageTitle.length <= 512
    && typeof message.pageDomain === "string"
    && message.pageDomain.length <= 512
  );
}
