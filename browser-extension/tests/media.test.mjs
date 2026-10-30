import test from "node:test";
import assert from "node:assert/strict";
import {
  createDownloadResource,
  createNativeMessage,
  classifyMediaUrl,
  deduplicateResources,
  DEFAULT_SETTINGS,
  downloadPromptDecision,
  downloadResourceIdentity,
  isEligibleDownload,
  isExcludedDomain,
  normalizeSettings,
  validateDownloadMessage,
  validateMediaMessage,
  validateUnsupportedMessage,
} from "../media.js";

test("classifies supported direct media and unsupported manifests", () => {
  assert.equal(classifyMediaUrl("https://cdn.example/a.mp4?token=1").mediaType, "mp4");
  assert.equal(classifyMediaUrl("https://cdn.example/a.webm").downloadable, true);
  assert.equal(classifyMediaUrl("https://cdn.example/a.m3u8").downloadable, false);
  assert.equal(classifyMediaUrl("https://cdn.example/a.mpd").mediaType, "dash");
  assert.equal(classifyMediaUrl("https://cdn.example/a.mkv").mediaType, "mkv");
  assert.equal(classifyMediaUrl("https://cdn.example/video?id=1", "video/mp4").mediaType, "video");
  assert.equal(classifyMediaUrl("https://cdn.example/a.jpg"), null);
});

test("filters non-HTTP and credential-bearing URLs", () => {
  assert.equal(classifyMediaUrl("file:///C:/video.mp4"), null);
  assert.equal(classifyMediaUrl("https://user:pass@cdn.example/video.mp4"), null);
});

test("deduplicates resources by exact URL", () => {
  const items = [
    { url: "https://cdn.example/a.mp4", pageTitle: "first" },
    { url: "https://cdn.example/a.mp4", pageTitle: "duplicate" },
  ];
  assert.equal(deduplicateResources(items).length, 1);
});

test("normalizes file filters and excludes matching subdomains", () => {
  const settings = normalizeSettings({
    fileTypes: [".PDF", "exe", "bad/type", "pdf"],
    excludedDomains: ["example.com", ".internal.test."],
  });
  assert.deepEqual(settings.fileTypes, ["pdf", "exe"]);
  assert.equal(isExcludedDomain("cdn.example.com", settings.excludedDomains), true);
  assert.equal(isExcludedDomain("notexample.com", settings.excludedDomains), false);
  assert.equal(isEligibleDownload({
    url: "https://assets.allowed.test/file.pdf",
    filename: "file.pdf",
  }, settings), true);
  assert.equal(isEligibleDownload({
    url: "https://assets.allowed.test/file.zip",
    filename: "file.zip",
  }, settings), false);
  assert.equal(isEligibleDownload({
    url: "https://internal.test/file.pdf",
    filename: "file.pdf",
  }, settings), false);
  assert.equal(isEligibleDownload({
    url: "https://assets.allowed.test/file.pdf",
    referrer: "https://internal.test/article",
    filename: "file.pdf",
  }, settings), false);
  assert.equal(isEligibleDownload({
    url: "https://user:pass@example.com/file.pdf",
    filename: "file.pdf",
  }, settings), false);
});

test("builds sanitized browser download metadata without referrer or headers", () => {
  const resource = createDownloadResource({
    url: "https://files.example/document.pdf?token=signed",
    filename: "C:\\Downloads\\document.pdf",
    mime: "application/pdf",
    totalBytes: 12345,
    bytesReceived: 45,
    tabId: 7,
    referrer: "https://private.example/path",
    requestHeaders: [{ name: "Authorization", value: "secret" }],
  });
  assert.deepEqual(resource, {
    url: "https://files.example/document.pdf?token=signed",
    mediaType: "pdf",
    pageTitle: "document.pdf",
    pageDomain: "private.example",
    tabId: 7,
    filename: "document.pdf",
    mimeType: "application/pdf",
    totalBytes: 12345,
    observedBytes: 45,
    width: null,
    height: null,
  });
});

test("deduplicates browser download events without storing signed query strings", () => {
  const first = downloadResourceIdentity({
    url: "https://files.example/archive.zip?token=first",
    filename: "archive.zip",
  });
  const repeated = downloadResourceIdentity({
    url: "https://files.example/archive.zip?token=second",
    filename: "archive.zip",
  });
  assert.equal(first, repeated);
  assert.equal(first.includes("token="), false);
  assert.notEqual(first, downloadResourceIdentity({
    url: "https://files.example/other.zip",
    filename: "other.zip",
  }));
});

test("applies remembered browser prompt choices by file type", () => {
  const declined = downloadPromptDecision("browser", "pdf", true, DEFAULT_SETTINGS);
  assert.equal(declined.fileTypes.includes("pdf"), false);
  const accepted = downloadPromptDecision("fluxnet", "bin", true, DEFAULT_SETTINGS);
  assert.equal(accepted.fileTypes.includes("bin"), true);
  assert.equal(downloadPromptDecision("browser", "pdf", false, DEFAULT_SETTINGS), null);
  assert.throws(() => downloadPromptDecision("ignore", "pdf", true, DEFAULT_SETTINGS));
});

test("validates only bounded direct media handoff messages", () => {
  const base = {
    url: "https://cdn.example/a.mp4?key=x",
    mediaType: "mp4",
    pageTitle: "Page",
    pageDomain: "example.com",
  };
  assert.equal(validateMediaMessage(base), true);
  assert.equal(validateMediaMessage({ ...base, mediaType: "hls" }), false);
  assert.equal(validateMediaMessage({ ...base, pageTitle: "x".repeat(513) }), false);
  assert.equal(validateMediaMessage({ ...base, url: `https://cdn.example/${"x".repeat(8200)}.mp4` }), false);
  assert.equal(validateMediaMessage({
    ...base,
    url: "https://cdn.example/video.mkv",
    mediaType: "mkv",
  }), true);
});

test("validates browser download handoffs and native messaging envelopes", () => {
  const resource = {
    url: "https://files.example/kit.zip?sig=temporary",
    mediaType: "zip",
    pageTitle: "kit.zip",
    pageDomain: "files.example",
    filename: "kit.zip",
    mimeType: "application/zip",
    totalBytes: 4096,
  };
  assert.equal(validateDownloadMessage(resource), true);
  assert.equal(validateDownloadMessage({ ...resource, url: "https://user:pass@files.example/kit.zip" }), false);
  assert.deepEqual(createNativeMessage("sendDownload", "a".repeat(32), resource), {
    action: "sendDownload",
    pairingCode: "a".repeat(32),
    resource,
  });
  assert.throws(() => createNativeMessage("sendDownload", "short", resource));
  assert.throws(() => createNativeMessage("unknown", "a".repeat(32), resource));
});

test("validates manifests only as explicitly unsupported reports", () => {
  const message = {
    url: "https://cdn.example/playlist.m3u8",
    mediaType: "hls",
    pageTitle: "Page",
    pageDomain: "example.com",
  };
  assert.equal(validateUnsupportedMessage(message), true);
  assert.equal(validateUnsupportedMessage({ ...message, mediaType: "mp4" }), false);
  assert.equal(validateUnsupportedMessage({ ...message, url: "https://cdn.example/video.mp4" }), false);
});
