import test from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import {
  createDownloadResource,
  createNativeMessage,
  classifyMediaUrl,
  deduplicateResources,
  DEFAULT_SETTINGS,
  downloadPromptDecision,
  downloadResourceIdentity,
  hasPendingDownloadPrompt,
  isEligibleDownload,
  isExcludedDomain,
  normalizeSettings,
  parseDashManifest,
  parseHlsManifest,
  reconnectDelayMinutes,
  takeUserDownloadIntent,
  validateDownloadMessage,
  validateMediaMessage,
  validateUnsupportedMessage,
} from "../media.js";
import "../video-sources.js";

const { videoSourceLabel, canDownloadVideoSource } = globalThis.FluxNetVideoSources;

test("manifest key produces the allowlisted stable extension ID", async () => {
  const manifest = JSON.parse(await readFile(new URL("../manifest.json", import.meta.url), "utf8"));
  const publicKey = Buffer.from(manifest.key, "base64");
  const hash = createHash("sha256").update(publicKey).digest();
  const id = [...hash.subarray(0, 16)]
    .map((byte) => String.fromCharCode(97 + (byte >> 4), 97 + (byte & 15)))
    .join("");
  assert.equal(id, "clgiddogjgpjllmjgpffopnmighkmhae");
  assert.equal(manifest.permissions.includes("alarms"), true);
});

test("automatic Native Messaging handoffs do not require a pairing code", () => {
  const resource = { url: "https://cdn.example/video.mp4" };
  assert.deepEqual(createNativeMessage("sendMedia", resource), {
    action: "sendMedia",
    resource,
  });
  assert.throws(() => createNativeMessage("unknown", resource));
});

test("Native Messaging reconnection retries are capped with increasing delays", () => {
  assert.deepEqual(
    [1, 2, 3, 4, 5, 6].map(reconnectDelayMinutes),
    [1, 2, 4, 5, 5, 5],
  );
  assert.equal(reconnectDelayMinutes(7), null);
});

test("classifies supported direct media and unsupported manifests", () => {
  assert.equal(classifyMediaUrl("https://cdn.example/a.mp4?token=1").mediaType, "mp4");
  assert.equal(classifyMediaUrl("https://cdn.example/a.webm").downloadable, true);
  assert.equal(classifyMediaUrl("https://cdn.example/a.m3u8").downloadable, false);
  assert.equal(classifyMediaUrl("https://cdn.example/a.mpd").mediaType, "dash");
  assert.equal(classifyMediaUrl("https://cdn.example/a.mkv"), null);
  assert.equal(classifyMediaUrl("https://cdn.example/video?id=1", "video/mp4").mediaType, "mp4");
  assert.equal(classifyMediaUrl("https://cdn.example/audio.mp3", "audio/mpeg"), null);
  assert.equal(classifyMediaUrl("https://cdn.example/unknown", "video/x-msvideo"), null);
  assert.equal(classifyMediaUrl("https://cdn.example/a.jpg"), null);
});

test("unknown source resolution is never presented as a quality variant", () => {
  const source = { url: "https://cdn.example/source.mp4", mediaType: "mp4", width: null, height: null };
  assert.equal(videoSourceLabel(source, 0, 1), "Original source · MP4 · resolution unavailable");
  assert.equal(videoSourceLabel({ ...source, width: 1280 }, 0, 1), "Original source · MP4 · resolution unavailable");
  assert.equal(videoSourceLabel({ ...source, width: 1920, height: 1080 }, 0, 1), "1080p (1920 × 1080)");
});

test("multiple URLs with unknown dimensions are separate sources, not quality variants", () => {
  const first = { url: "https://cdn.example/a.mp4", mediaType: "mp4", width: null, height: null };
  const second = { url: "https://cdn.example/b.mp4", mediaType: "mp4", width: null, height: null };
  const labels = [
    videoSourceLabel(first, 0, 2),
    videoSourceLabel(second, 1, 2),
  ];
  assert.deepEqual(labels, [
    "Source 1 · MP4 · resolution unavailable",
    "Source 2 · MP4 · resolution unavailable",
  ]);
  assert.equal(labels.some((label) => /\b(?:1080|720|480)p\b/.test(label)), false);
});

test("video download stays disabled until resource registration finishes", () => {
  const pending = { registered: false, pending: true };
  assert.equal(canDownloadVideoSource(pending), false);
  assert.equal(canDownloadVideoSource({ registered: false, pending: false }), false);
  assert.equal(canDownloadVideoSource({ registered: true, pending: false }), true);
  assert.equal(canDownloadVideoSource({ registered: true }, true), false);
});

test("parses accessible HLS variant playlists and audio tracks", () => {
  const parsed = parseHlsManifest(`#EXTM3U
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="English",DEFAULT=YES,URI="audio/en.m3u8"
#EXT-X-STREAM-INF:BANDWIDTH=2800000,AVERAGE-BANDWIDTH=2500000,RESOLUTION=1920x1080,AUDIO="audio"
video/1080/index.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=1200000,RESOLUTION=1280x720
video/720/index.m3u8
`, "https://cdn.example/master.m3u8");
  assert.deepEqual(parsed.variants.map(({ url, width, height }) => ({ url, width, height })), [
    { url: "https://cdn.example/video/1080/index.m3u8", width: 1920, height: 1080 },
    { url: "https://cdn.example/video/720/index.m3u8", width: 1280, height: 720 },
  ]);
  assert.deepEqual(parsed.audioTracks, [{
    name: "English",
    url: "https://cdn.example/audio/en.m3u8",
    groupId: "audio",
    default: true,
  }]);
  const audioOnly = parseHlsManifest(`#EXTM3U
#EXT-X-STREAM-INF:BANDWIDTH=128000,CODECS="mp4a.40.2"
audio.m3u8
`, "https://cdn.example/audio-master.m3u8");
  assert.equal(audioOnly.variants.length, 0);
  assert.throws(() => parseHlsManifest("#EXT-X-TARGETDURATION:6", "https://cdn.example/a.m3u8"));
});

test("parses DASH video representations and associated audio adaptations", () => {
  const parsed = parseDashManifest(`<MPD>
  <Period>
    <AdaptationSet mimeType="video/mp4" contentType="video">
      <Representation id="v1080" bandwidth="4000000" width="1920" height="1080"><BaseURL>video/1080.mp4</BaseURL></Representation>
      <Representation id="v720" bandwidth="1800000" width="1280" height="720"/>
    </AdaptationSet>
    <AdaptationSet mimeType="audio/mp4" contentType="audio" lang="en"/>
  </Period>
</MPD>`, "https://cdn.example/manifest.mpd");
  assert.deepEqual(parsed.variants, [
    { id: "v1080", url: "https://cdn.example/video/1080.mp4", width: 1920, height: 1080, bandwidth: 4000000 },
    { id: "v720", url: null, width: 1280, height: 720, bandwidth: 1800000 },
  ]);
  assert.deepEqual(parsed.audioTracks, [{
    name: "en",
    url: null,
    groupId: null,
    default: false,
  }]);
  assert.throws(() => parseDashManifest("<NotMPD/>", "https://cdn.example/a.mpd"));
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

test("only suppresses ordinary download prompts while an identical prompt is pending", () => {
  const resource = {
    url: "https://files.example/report.pdf?token=first",
    filename: "report.pdf",
  };
  const pendingPrompts = {
    "download-1": {
      kind: "download",
      resource,
    },
  };

  assert.equal(hasPendingDownloadPrompt(pendingPrompts, {
    url: "https://files.example/report.pdf?token=retry",
    filename: "report.pdf",
  }), true);
  assert.equal(hasPendingDownloadPrompt({}, resource), false);
});

test("matches ordinary download prompts only to recent same-tab user link clicks", () => {
  const intent = { url: "https://files.example/report.pdf?token=one", tabId: 7, at: 90_000 };
  const stale = { url: "https://files.example/old.zip", tabId: 7, at: 1 };
  const match = takeUserDownloadIntent({
    url: intent.url,
    finalUrl: "https://files.example/report.pdf?token=one",
    tabId: 7,
  }, [stale, intent], 100_000);
  assert.deepEqual(match, { intent, remaining: [] });
  assert.deepEqual(takeUserDownloadIntent({
    url: intent.url,
    tabId: 8,
  }, [intent], 100_000), { intent: null, remaining: [intent] });
  assert.equal(takeUserDownloadIntent({
    url: intent.url,
    tabId: 8,
  }, [{ ...intent, allowDifferentTab: true }], 100_000).intent.allowDifferentTab, true);
  assert.deepEqual(takeUserDownloadIntent({
    url: intent.url,
    tabId: 7,
  }, [intent], 120_001), { intent: null, remaining: [] });
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
    url: "https://cdn.example/video.webm",
    mediaType: "webm",
  }), true);
  assert.equal(validateMediaMessage({
    ...base,
    url: "https://cdn.example/video.mkv",
    mediaType: "mkv",
  }), false);
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
