const MAX_WEBSITE_URL_LENGTH = 8192;

export function validateWebsitePageUrl(rawUrl) {
  const trimmed = typeof rawUrl === "string" ? rawUrl.trim() : "";
  if (!trimmed) {
    return { ok: false, error: "Enter a public HTTP or HTTPS video page URL." };
  }
  if (trimmed.length > MAX_WEBSITE_URL_LENGTH) {
    return { ok: false, error: "Download URL is too long." };
  }
  let parsed;
  try {
    parsed = new URL(trimmed);
  } catch {
    return { ok: false, error: "Enter a public HTTP or HTTPS video page URL." };
  }
  if ((parsed.protocol !== "http:" && parsed.protocol !== "https:") || !parsed.hostname) {
    return { ok: false, error: "Enter a public HTTP or HTTPS video page URL." };
  }
  if (parsed.username || parsed.password) {
    return {
      ok: false,
      error: "Embedded URL credentials are not allowed. FluxNet only probes public video pages.",
    };
  }
  const host = parsed.hostname.replace(/\.$/, "").toLowerCase();
  if (
    host === "localhost"
    || host.endsWith(".localhost")
    || host.endsWith(".local")
    || host.endsWith(".internal")
    || host === "127.0.0.1"
    || host === "0.0.0.0"
    || host === "::1"
    || host === "[::1]"
  ) {
    return { ok: false, error: "Enter a public HTTP or HTTPS video page URL." };
  }
  return { ok: true, url: parsed.toString() };
}

export function isMuxedWebsiteFormat(format) {
  if (!format || typeof format !== "object") return false;
  const container = typeof format.container === "string" ? format.container.toLowerCase() : "";
  return (
    (container === "mp4" || container === "webm")
    && format.videoCodec !== "none"
    && format.audioCodec !== "none"
    && !format.requiresSeparateAudio
  );
}

export function buildWebsiteFormatSelection(selectionKey) {
  if (!selectionKey || selectionKey === "bestAvailable") {
    return "bestAvailable";
  }
  return { exact: String(selectionKey) };
}

export function resolveWebsiteContainerExtension(catalog, selectionKey) {
  if (!catalog || !Array.isArray(catalog.formats)) return null;
  if (!selectionKey || selectionKey === "bestAvailable") {
    if (!catalog.bestAvailable) return null;
    for (const ext of ["mp4", "webm"]) {
      const hasMatch = catalog.formats.some(
        (format) =>
          isMuxedWebsiteFormat(format)
          && typeof format.container === "string"
          && format.container.toLowerCase() === ext,
      );
      if (hasMatch) return ext;
    }
    return null;
  }
  const exact = catalog.formats.find((format) => format.formatId === selectionKey);
  if (!exact || !isMuxedWebsiteFormat(exact) || typeof exact.container !== "string") {
    return null;
  }
  const ext = exact.container.toLowerCase();
  return ext === "mp4" || ext === "webm" ? ext : null;
}

export function validateWebsiteDestinationPath(destination, expectedExtension) {
  const raw = typeof destination === "string" ? destination : "";
  const trimmed = raw.trim();
  const ext = typeof expectedExtension === "string" ? expectedExtension.toLowerCase() : "";
  if (!trimmed || (ext !== "mp4" && ext !== "webm")) {
    return {
      ok: false,
      error: "Choose a valid .mp4 or .webm destination file.",
    };
  }
  if (/[. ]$/.test(raw)) {
    return {
      ok: false,
      error: `The destination filename must end in .${ext} to match the selected ${ext.toUpperCase()} format.`,
    };
  }
  const normalized = raw.toLowerCase();
  if (!normalized.endsWith(`.${ext}`)) {
    return {
      ok: false,
      error: `The destination file extension must be .${ext} to match the selected ${ext.toUpperCase()} format. FluxNet never renames WebM to MP4 or vice versa.`,
    };
  }
  return { ok: true, destination: raw };
}

export function formatDiscoveredFormatDetails(format, formatBytesFn) {
  const container = typeof format?.container === "string" && format.container
    ? format.container.toUpperCase()
    : "MP4";
  const resolution = format?.height && format?.resolution
    ? `${format.height}p (${format.resolution})`
    : format?.height
      ? `${format.height}p`
      : format?.resolution || "Resolution unknown";
  const fps = typeof format?.fps === "number" && Number.isFinite(format.fps) && format.fps > 0
    ? `${Number.isInteger(format.fps) ? format.fps : format.fps.toFixed(1)} fps`
    : "FPS unavailable";
  const size = typeof format?.fileSize === "number" && Number.isFinite(format.fileSize)
    ? formatBytesFn(format.fileSize)
    : "Size unknown";
  return {
    container,
    resolution,
    fps,
    size,
    label: `${resolution} · ${container} · ${fps} · ${size} (ID: ${format?.formatId ?? "unknown"})`,
  };
}

export function formatWebsiteMediaBadge(websiteMedia) {
  if (!websiteMedia || typeof websiteMedia !== "object") return null;
  const container = typeof websiteMedia.container === "string"
    ? websiteMedia.container.toUpperCase()
    : "VIDEO";
  const height = typeof websiteMedia.height === "number" && websiteMedia.height > 0
    ? ` · ${websiteMedia.height}p`
    : "";
  const rawFormatId = typeof websiteMedia.formatId === "string" ? websiteMedia.formatId : "";
  const formatLabel = rawFormatId.startsWith("best[")
    ? `Best Available (${rawFormatId})`
    : rawFormatId
      ? `Format ${rawFormatId}`
      : "Muxed format";
  return `Website video · ${container}${height} · ${formatLabel}`;
}

export function canRetryDownloadDirectly(entry) {
  if (!entry || typeof entry !== "object") return false;
  if (entry.status !== "failed" && entry.status !== "cancelled") return false;
  return !entry.websiteMedia;
}
