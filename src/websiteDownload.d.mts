export type DiscoveredFormat = {
  formatId: string;
  resolution: string | null;
  height: number | null;
  fps: number | null;
  videoCodec: string | null;
  audioCodec: string | null;
  container: string | null;
  fileSize: number | null;
  hasVideo: boolean;
  hasAudio: boolean;
  requiresSeparateAudio: boolean;
};

export type VideoDiscovery = {
  title: string;
  durationSeconds: number | null;
  extractor: string;
  formats: DiscoveredFormat[];
  bestAvailable: boolean;
};

export type FormatSelection = "bestAvailable" | { exact: string };

export type WebsiteMediaInfo = {
  formatId: string;
  container: string;
  height?: number | null;
};

export function validateWebsitePageUrl(
  rawUrl: string,
): { ok: true; url: string } | { ok: false; error: string };

export function isMuxedWebsiteFormat(format: DiscoveredFormat): boolean;

export function buildWebsiteFormatSelection(selectionKey: string): FormatSelection;

export function resolveWebsiteContainerExtension(
  catalog: VideoDiscovery | null,
  selectionKey: string,
): "mp4" | "webm" | null;

export function validateWebsiteDestinationPath(
  destination: string,
  expectedExtension: "mp4" | "webm" | null,
): { ok: true; destination: string } | { ok: false; error: string };

export function formatDiscoveredFormatDetails(
  format: DiscoveredFormat,
  formatBytesFn: (bytes: number | null) => string,
): {
  container: string;
  resolution: string;
  fps: string;
  size: string;
  label: string;
};

export function formatWebsiteMediaBadge(
  websiteMedia: WebsiteMediaInfo | null | undefined,
): string | null;

export function canRetryDownloadDirectly(entry: {
  status: string;
  websiteMedia?: WebsiteMediaInfo | null;
}): boolean;
