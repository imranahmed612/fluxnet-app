import test from "node:test";
import assert from "node:assert/strict";
import {
  buildWebsiteFormatSelection,
  canRetryDownloadDirectly,
  formatDiscoveredFormatDetails,
  formatWebsiteMediaBadge,
  isMuxedWebsiteFormat,
  resolveWebsiteContainerExtension,
  validateWebsiteDestinationPath,
  validateWebsitePageUrl,
} from "../src/websiteDownload.mjs";

const sampleCatalog = {
  title: "Sample Public Video",
  durationSeconds: 42.5,
  extractor: "ExampleSite",
  bestAvailable: true,
  formats: [
    {
      formatId: "18",
      resolution: "640x360",
      height: 360,
      fps: 30,
      videoCodec: "avc1",
      audioCodec: "mp4a",
      container: "mp4",
      fileSize: 2_500_000,
      hasVideo: true,
      hasAudio: true,
      requiresSeparateAudio: false,
    },
    {
      formatId: "247-muxed",
      resolution: "1280x720",
      height: 720,
      fps: 60,
      videoCodec: "vp9",
      audioCodec: "opus",
      container: "webm",
      fileSize: null,
      hasVideo: true,
      hasAudio: true,
      requiresSeparateAudio: false,
    },
  ],
};

test("validates public HTTP and HTTPS website URLs and rejects credentials or local hosts", () => {
  assert.deepEqual(validateWebsitePageUrl(" https://example.com/watch?v=123 "), {
    ok: true,
    url: "https://example.com/watch?v=123",
  });
  assert.equal(validateWebsitePageUrl("").ok, false);
  assert.equal(validateWebsitePageUrl("file:///C:/video.mp4").ok, false);
  assert.equal(validateWebsitePageUrl("https://user:pass@example.com/watch").ok, false);
  assert.equal(validateWebsitePageUrl("http://localhost:8080/video").ok, false);
  assert.equal(validateWebsitePageUrl("http://127.0.0.1/video").ok, false);
  assert.equal(validateWebsitePageUrl("https://host.internal/video").ok, false);
});

test("builds Tauri IPC FormatSelection payloads for Best Available and exact format IDs", () => {
  assert.equal(buildWebsiteFormatSelection("bestAvailable"), "bestAvailable");
  assert.equal(buildWebsiteFormatSelection(""), "bestAvailable");
  assert.deepEqual(buildWebsiteFormatSelection("18"), { exact: "18" });
  assert.deepEqual(buildWebsiteFormatSelection("247-muxed"), { exact: "247-muxed" });
});

test("resolves matching .mp4 and .webm container extensions for Best Available and exact formats", () => {
  assert.equal(resolveWebsiteContainerExtension(sampleCatalog, "bestAvailable"), "mp4");
  assert.equal(resolveWebsiteContainerExtension(sampleCatalog, "18"), "mp4");
  assert.equal(resolveWebsiteContainerExtension(sampleCatalog, "247-muxed"), "webm");
  assert.equal(resolveWebsiteContainerExtension(sampleCatalog, "missing"), null);

  const webmOnlyCatalog = {
    ...sampleCatalog,
    formats: [sampleCatalog.formats[1]],
  };
  assert.equal(resolveWebsiteContainerExtension(webmOnlyCatalog, "bestAvailable"), "webm");
  assert.equal(isMuxedWebsiteFormat(sampleCatalog.formats[0]), true);
});

test("enforces matching .mp4 and .webm save destination extensions", () => {
  assert.deepEqual(
    validateWebsiteDestinationPath("C:\\Downloads\\Sample Public Video.mp4", "mp4"),
    { ok: true, destination: "C:\\Downloads\\Sample Public Video.mp4" },
  );
  assert.deepEqual(
    validateWebsiteDestinationPath("C:\\Downloads\\Sample Public Video.WEBM", "webm"),
    { ok: true, destination: "C:\\Downloads\\Sample Public Video.WEBM" },
  );
  const mismatch = validateWebsiteDestinationPath("C:\\Downloads\\Sample Public Video.webm", "mp4");
  assert.equal(mismatch.ok, false);
  assert.match(mismatch.error, /must be \.mp4/);

  const trailingSpace = validateWebsiteDestinationPath("C:\\Downloads\\Video.mp4 ", "mp4");
  assert.equal(trailingSpace.ok, false);
});

test("formats discovered format metadata and website download entry badges", () => {
  const details = formatDiscoveredFormatDetails(
    sampleCatalog.formats[0],
    (bytes) => `${(bytes / 1_000_000).toFixed(2)} MB`,
  );
  assert.equal(details.container, "MP4");
  assert.equal(details.resolution, "360p (640x360)");
  assert.equal(details.fps, "30 fps");
  assert.equal(details.size, "2.50 MB");
  assert.match(details.label, /360p \(640x360\) · MP4 · 30 fps · 2\.50 MB \(ID: 18\)/);

  const unknownDetails = formatDiscoveredFormatDetails(
    sampleCatalog.formats[1],
    () => "Unknown size",
  );
  assert.equal(unknownDetails.container, "WEBM");
  assert.equal(unknownDetails.fps, "60 fps");
  assert.equal(unknownDetails.size, "Size unknown");

  assert.equal(
    formatWebsiteMediaBadge({ formatId: "18", container: "mp4", height: 360 }),
    "Website video · MP4 · 360p · Format 18",
  );
  assert.equal(
    formatWebsiteMediaBadge({ formatId: "best[ext=webm]", container: "webm", height: 720 }),
    "Website video · WEBM · 720p · Best Available (best[ext=webm])",
  );
});

test("disables direct retry_download for website entries while preserving it for HTTP/HLS entries", () => {
  assert.equal(
    canRetryDownloadDirectly({ status: "failed", websiteMedia: null }),
    true,
  );
  assert.equal(
    canRetryDownloadDirectly({
      status: "failed",
      websiteMedia: { formatId: "18", container: "mp4", height: 360 },
    }),
    false,
  );
  assert.equal(
    canRetryDownloadDirectly({
      status: "cancelled",
      websiteMedia: { formatId: "best[ext=mp4]", container: "mp4", height: 720 },
    }),
    false,
  );
  assert.equal(
    canRetryDownloadDirectly({ status: "downloading", websiteMedia: null }),
    false,
  );
});
