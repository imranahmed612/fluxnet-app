import test from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createContext, Script } from "node:vm";
import {
  createDownloadResource,
  createVideoDownloadFilename,
  createNativeMessage,
  classifyMediaUrl,
  deduplicateResources,
  DEFAULT_SETTINGS,
  appendProcessedDownloadId,
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
import "../download-gesture.js";

const { videoSourceLabel, canDownloadVideoSource } = globalThis.FluxNetVideoSources;
const { createDownloadGestureTracker } = globalThis.FluxNetDownloadGesture;
const { selectVideoOverlayPlayer } = globalThis.FluxNetVideoMedia;

function createContentScriptHarness(initialVideos = []) {
  const animationFrames = [];
  const mutationObservers = [];
  const hosts = [];
  const sentMessages = [];
  const videos = [...initialVideos];
  const runtimeListeners = [];

  class MockElement {
    constructor() {
      this.dataset = {};
      this.style = {};
      this.listeners = new Map();
      this.children = [];
      this.value = "";
      this.hidden = false;
      this.disabled = false;
    }

    addEventListener(type, listener) {
      const listeners = this.listeners.get(type) || [];
      listeners.push(listener);
      this.listeners.set(type, listeners);
    }

    append(child) {
      this.children.push(child);
      if (!this.value) this.value = child.value;
    }

    replaceChildren() {
      this.children = [];
      this.value = "";
    }

    querySelector() {
      return null;
    }

    querySelectorAll() {
      return [];
    }

    attachShadow() {
      const elements = new Map();
      return {
        set innerHTML(_html) {},
        querySelector(selector) {
          if (!elements.has(selector)) elements.set(selector, new MockElement());
          return elements.get(selector);
        },
        querySelectorAll() {
          return [];
        },
      };
    }

    closest(selector) {
      return selector === "video" && this instanceof MockVideo ? this : null;
    }

    matches(selector) {
      return selector === "video" && this instanceof MockVideo;
    }

    getBoundingClientRect() {
      return { width: 0, height: 0, top: 0, right: 0, bottom: 0, left: 0 };
    }
  }

  class MockVideo extends MockElement {
    constructor({ paused = false } = {}) {
      super();
      this.currentSrc = "blob:https://www.instagram.com/reel-id";
      this.src = this.currentSrc;
      this.videoWidth = 720;
      this.videoHeight = 900;
      this.paused = paused;
      this.ended = false;
      this.isConnected = true;
    }

    getAttribute() {
      return null;
    }

    getClientRects() {
      return [this.getBoundingClientRect()];
    }

    getBoundingClientRect() {
      return { width: 360, height: 450, top: 100, right: 360, bottom: 550, left: 0 };
    }
  }

  class MockMutationObserver {
    constructor(callback) {
      this.callback = callback;
      mutationObservers.push(this);
    }

    observe() {}
  }

  const clickListeners = [];
  const document = {
    title: "Instagram video",
    documentElement: { append: (host) => hosts.push(host) },
    addEventListener(type, listener) {
      if (type === "click") clickListeners.push(listener);
    },
    createElement: () => new MockElement(),
    querySelectorAll: (selector) => selector === "video" ? videos : [],
  };
  const context = {
    Element: MockElement,
    HTMLVideoElement: MockVideo,
    MutationObserver: MockMutationObserver,
    ResizeObserver: class {
      observe() {}
      unobserve() {}
    },
    PerformanceObserver: undefined,
    document,
    window: { addEventListener() {} },
    location: { hostname: "www.instagram.com" },
    URL,
    innerHeight: 1_000,
    innerWidth: 1_200,
    performance: {
      getEntriesByName: () => [],
      getEntriesByType: () => [],
      now: () => 0,
    },
    getComputedStyle: () => ({ display: "block", visibility: "visible", opacity: "1" }),
    requestAnimationFrame: (callback) => animationFrames.push(callback),
    chrome: {
      runtime: {
        onMessage: { addListener: (listener) => runtimeListeners.push(listener) },
        sendMessage: (message) => {
          sentMessages.push(message);
          return Promise.resolve({ ok: true });
        },
      },
      storage: { local: { get: () => Promise.resolve({ settings: {} }) } },
    },
  };
  const vmContext = createContext(context);
  const contentScripts = [
    "../video-sources.js",
    "../download-gesture.js",
    "../content.js",
  ];

  return readFile(new URL(contentScripts[0], import.meta.url), "utf8")
    .then(async (videoSources) => {
      const scripts = await Promise.all(contentScripts.slice(1)
        .map((file) => readFile(new URL(file, import.meta.url), "utf8")));
      new Script([videoSources, ...scripts].join("\n"), { filename: "content-scripts.js" })
        .runInContext(vmContext);
      return Promise.resolve().then(() => ({
        videos,
        hosts,
        sentMessages,
        clickListeners,
        mutationObservers,
        runtimeListeners,
        animationFrames,
        MockVideo,
      }));
    });
}

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

test("content scripts have no duplicate declarations in their shared scope", async () => {
  const manifest = JSON.parse(await readFile(new URL("../manifest.json", import.meta.url), "utf8"));
  const sources = await Promise.all(manifest.content_scripts.flatMap(({ js }) => js)
    .map((file) => readFile(new URL(`../${file}`, import.meta.url), "utf8")));
  assert.doesNotThrow(() => new Script(sources.join("\n"), { filename: "content-scripts.js" }));
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
  assert.equal(classifyMediaUrl("https://cdn.example/a.mp4", "image/jpeg"), null);
  assert.equal(classifyMediaUrl("https://cdn.example/a.mp4", "application/pdf"), null);
  assert.equal(classifyMediaUrl("https://cdn.example/a.mp4", "audio/mpeg"), null);
});

test("classifies extensionless and generic-MIME video only with decoded player evidence", () => {
  const playable = (url, mimeType = "") => classifyMediaUrl(url, mimeType, true);

  assert.deepEqual(classifyMediaUrl("https://cdn.example/media?id=1", "video/mp4; codecs=avc1"), {
    url: "https://cdn.example/media?id=1",
    mediaType: "mp4",
    downloadable: true,
  });
  assert.deepEqual(playable("https://cdn.example/media?id=1", "application/octet-stream"), {
    url: "https://cdn.example/media?id=1",
    mediaType: "unsupported-video",
    downloadable: false,
  });
  assert.equal(classifyMediaUrl("https://cdn.example/media?id=1", "application/octet-stream"), null);
  assert.equal(playable("https://cdn.example/media.mkv", "video/x-matroska").mediaType, "unsupported-video");
  assert.equal(playable("https://cdn.example/media.avi", "application/octet-stream").mediaType, "unsupported-video");
  assert.equal(playable("https://cdn.example/media.ogg", "video/ogg").mediaType, "unsupported-video");
  assert.equal(playable("https://cdn.example/audio.mp3", "video/mp4"), null);
  assert.equal(playable("https://cdn.example/image.jpg", "video/mp4"), null);
  assert.equal(playable("https://cdn.example/image.jpg", "application/octet-stream"), null);
  assert.equal(playable("https://cdn.example/document.pdf"), null);
  assert.equal(playable("https://cdn.example/archive.zip"), null);
  assert.equal(playable("https://cdn.example/installer.exe"), null);
  assert.equal(playable("https://cdn.example/audio.mp3", "audio/mpeg"), null);
  assert.equal(playable("blob:https://cdn.example/stream-id", "application/octet-stream"), null);
});

test("validates unsupported video reports only with playable dimensions", () => {
  const resource = {
    url: "https://cdn.example/media?id=1",
    mediaType: "unsupported-video",
    pageTitle: "Video",
    pageDomain: "example.com",
    mimeType: "application/octet-stream",
    width: 1280,
    height: 720,
  };
  assert.equal(validateUnsupportedMessage(resource), true);
  assert.equal(validateUnsupportedMessage({ ...resource, width: null }), false);
  assert.equal(validateUnsupportedMessage({ ...resource, url: "https://cdn.example/document.pdf" }), false);
  assert.equal(validateUnsupportedMessage({ ...resource, url: "blob:https://cdn.example/stream-id" }), false);
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

test("parses finite HLS fMP4 media playlists with declared initialization data", () => {
  const parsed = parseHlsManifest(`#EXTM3U
#EXT-X-VERSION:7
#EXT-X-TARGETDURATION:6
#EXT-X-MAP:URI="init.mp4"
#EXTINF:6,
segment-1.m4s
#EXTINF:4,
segment-2.m4s
#EXT-X-ENDLIST
`, "https://cdn.example/media/index.m3u8");
  assert.deepEqual(parsed.playlist, {
    version: 1,
    container: "fmp4",
    manifestSizeBytes: new TextEncoder().encode(`#EXTM3U
#EXT-X-VERSION:7
#EXT-X-TARGETDURATION:6
#EXT-X-MAP:URI="init.mp4"
#EXTINF:6,
segment-1.m4s
#EXTINF:4,
segment-2.m4s
#EXT-X-ENDLIST
`).byteLength,
    init: { url: "https://cdn.example/media/init.mp4" },
    segments: [
      { url: "https://cdn.example/media/segment-1.m4s" },
      { url: "https://cdn.example/media/segment-2.m4s" },
    ],
    expectedBytes: null,
  });
  const resource = {
    url: "https://cdn.example/media/index.m3u8",
    mediaType: "hls",
    downloadable: true,
    hlsPlaylist: parsed.playlist,
    pageTitle: "Example video",
    pageDomain: "example.com",
    mimeType: null,
    totalBytes: null,
  };
  assert.equal(validateMediaMessage(resource), true);
  assert.equal(validateDownloadMessage(resource), true);
  assert.equal(createVideoDownloadFilename({
    mediaType: "hls",
    pageTitle: "Instagram",
    pageDomain: "www.instagram.com",
  }), "Instagram_Video.mp4");
});

test("resolves explicit and implicit HLS byte ranges in declared order", () => {
  const parsed = parseHlsManifest(`#EXTM3U
#EXT-X-TARGETDURATION:1
#EXT-X-MAP:URI="media.mp4",BYTERANGE="4@0"
#EXTINF:1,
#EXT-X-BYTERANGE:4@4
media.mp4
#EXTINF:1,
#EXT-X-BYTERANGE:4
media.mp4
#EXT-X-ENDLIST
`, "https://cdn.example/playlist.m3u8");
  assert.deepEqual(parsed.playlist.init.byteRange, { length: 4, offset: 0 });
  assert.deepEqual(parsed.playlist.segments.map((segment) => segment.byteRange), [
    { length: 4, offset: 4 },
    { length: 4, offset: 8 },
  ]);
  assert.equal(parsed.playlist.expectedBytes, 12);
});

test("rejects incomplete, encrypted, malformed, and uninitialized HLS media playlists", () => {
  const base = `#EXTM3U
#EXT-X-TARGETDURATION:4
#EXT-X-MAP:URI="init.mp4"
#EXTINF:4,
segment.m4s`;
  assert.throws(() => parseHlsManifest(`#EXTM3U
#EXTINF:4,
segment.m4s
#EXT-X-ENDLIST
`, "https://cdn.example/playlist.m3u8"), /initialization/i);
  assert.throws(() => parseHlsManifest(`${base}
`, "https://cdn.example/playlist.m3u8"), /ENDLIST/i);
  assert.throws(() => parseHlsManifest(`#EXTM3U
#EXT-X-TARGETDURATION:4
#EXT-X-MAP:URI="init.mp4"
#EXT-X-KEY:METHOD=AES-128,URI="key"
#EXTINF:4,
segment.m4s
#EXT-X-ENDLIST
`, "https://cdn.example/playlist.m3u8"), /encrypted|key-signaled/i);
  assert.throws(() => parseHlsManifest(`#EXTM3U
#EXT-X-TARGETDURATION:4
#EXT-X-MAP:URI="init.mp4,BYTERANGE="4"
#EXTINF:4,
segment.m4s
#EXT-X-ENDLIST
`, "https://cdn.example/playlist.m3u8"), /attribute|quoted/i);
  assert.throws(() => parseHlsManifest(`#EXTM3U
#EXT-X-TARGETDURATION:4
#EXT-X-MAP:URI="init.mp4"
#EXTINF:4,
segment.m4s
`, "https://cdn.example/playlist.m3u8"), /ENDLIST/i);
  assert.throws(() => parseHlsManifest(`#EXTM3U
#EXT-X-TARGETDURATION:4
#EXT-X-MAP:URI="init.mp4"
#EXT-X-ENDLIST
`, "https://cdn.example/playlist.m3u8"), /no media segments/i);
});

test("rejects HLS playlists with excessive segment counts and malformed byte ranges", () => {
  const segments = Array.from({ length: 65 }, (_, index) =>
    `#EXTINF:1,\nsegment-${index}.m4s`).join("\n");
  assert.throws(() => parseHlsManifest(`#EXTM3U
#EXT-X-TARGETDURATION:1
#EXT-X-MAP:URI="init.mp4"
${segments}
#EXT-X-ENDLIST
`, "https://cdn.example/playlist.m3u8"), /64-segment limit/i);
  assert.throws(() => parseHlsManifest(`#EXTM3U
#EXT-X-TARGETDURATION:1
#EXT-X-MAP:URI="init.mp4",BYTERANGE="0@0"
#EXTINF:1,
segment.mp4
#EXT-X-ENDLIST
`, "https://cdn.example/playlist.m3u8"), /byte range/i);
});

test("rejects oversized manifests and unsafe HLS resource URLs", () => {
assert.throws(() => parseHlsManifest(
  `#EXTM3U\n${"#".repeat(1_048_576)}`,
  "https://cdn.example/playlist.m3u8",
), /1 MiB/i);
assert.throws(() => parseHlsManifest(`#EXTM3U
#EXT-X-TARGETDURATION:1
#EXT-X-MAP:URI="init.mp4"
#EXTINF:1,
javascript:alert(1)
#EXT-X-ENDLIST
`, "https://cdn.example/playlist.m3u8"), /segment URL is invalid/i);
assert.throws(() => parseHlsManifest(`#EXTM3U
#EXT-X-TARGETDURATION:1
#EXT-X-MAP:URI="https://user:password@cdn.example/init.mp4"
#EXTINF:1,
segment.m4s
#EXT-X-ENDLIST
`, "https://cdn.example/playlist.m3u8"), /initialization URL is missing or invalid/i);
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

test("deduplicates Instagram video renditions with rotating CDN signatures", () => {
  const resources = [
    { url: "https://scontent.cdninstagram.com/v/t51/clip.mp4?_nc_sid=new", mediaType: "mp4", pageTitle: "Latest" },
    { url: "https://scontent.cdninstagram.com/v/t51/clip.mp4?_nc_sid=old", mediaType: "mp4", pageTitle: "Old" },
    { url: "https://scontent.cdninstagram.com/v/t51/other.mp4?_nc_sid=new", mediaType: "mp4", pageTitle: "Other clip" },
    { url: "https://cdn.example/clip.mp4?token=old", mediaType: "mp4" },
    { url: "https://cdn.example/clip.mp4?token=new", mediaType: "mp4" },
  ];
  const deduplicated = deduplicateResources(resources);
  assert.equal(deduplicated.length, 4);
  assert.equal(deduplicated[0].pageTitle, "Latest");
  assert.equal(deduplicated[1].pageTitle, "Other clip");
});

test("associates Instagram blob playback with its confirmed MP4 without ambiguous overlays", () => {
  const instagramVideo = {};
  const otherVideo = {};
  const players = [
    {
      video: instagramVideo,
      urls: ["blob:https://www.instagram.com/4fd0"],
      visible: true,
      width: 720,
      height: 900,
      playing: true,
    },
    {
      video: otherVideo,
      urls: ["blob:https://www.instagram.com/9a12"],
      visible: true,
      width: 720,
      height: 900,
      playing: false,
    },
  ];
  const mp4Url = "https://scontent.cdninstagram.com/v/t51/clip.mp4?signature=exact";
  assert.equal(selectVideoOverlayPlayer(players, mp4Url), instagramVideo);
  assert.equal(selectVideoOverlayPlayer([
    ...players,
    { ...players[1], video: {}, playing: true },
  ], mp4Url), null);
  assert.equal(selectVideoOverlayPlayer([
    { ...players[0], visible: false },
  ], mp4Url), null);

  const exactSourceVideo = {};
  assert.equal(selectVideoOverlayPlayer([
    { ...players[1], video: exactSourceVideo, urls: [mp4Url] },
    players[0],
  ], mp4Url), exactSourceVideo);
});

test("the first video click retries a confirmed Instagram blob-to-MP4 association", async () => {
  const harness = await createContentScriptHarness();
  const video = new harness.MockVideo({ paused: true });
  harness.videos.push(video);
  const mp4Url = "https://scontent.cdninstagram.com/reel.mp4?signature=first-click";
  harness.runtimeListeners[0]({
    type: "fluxnet:resource-confirmed",
    url: mp4Url,
    mediaType: "mp4",
    mimeType: "video/mp4",
  });
  assert.equal(harness.hosts.length, 0);

  harness.clickListeners[0]({
    button: 0,
    target: video,
    isTrusted: true,
    defaultPrevented: false,
    ctrlKey: false,
    metaKey: false,
    shiftKey: false,
    altKey: false,
    composedPath: () => [video],
  });
  video.paused = false;
  while (harness.animationFrames.length) harness.animationFrames.shift()();

  assert.equal(harness.hosts.length, 1);
  assert.equal(harness.sentMessages.some((message) => message.type === "fluxnet:video-download"), false);
});

test("a confirmed MP4 is associated when an Instagram blob video is discovered later", async () => {
  const harness = await createContentScriptHarness();
  const mp4Url = "https://scontent.cdninstagram.com/reel.mp4?signature=delayed";
  harness.runtimeListeners[0]({
    type: "fluxnet:resource-confirmed",
    url: mp4Url,
    mediaType: "mp4",
    mimeType: "video/mp4",
  });

  const video = new harness.MockVideo();
  harness.videos.push(video);
  harness.mutationObservers[0].callback([{ type: "childList", addedNodes: [video] }]);
  video.listeners.get("playing")[0]();

  assert.equal(harness.hosts.length, 1);
  assert.equal(harness.sentMessages.some((message) => message.type === "fluxnet:video-download"), false);
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

test("video handoff filenames prefer titles and never derive names from CDN URLs", () => {
  const cdnFilename = "a".repeat(180) + ".mp4";
  assert.equal(createVideoDownloadFilename({
    mediaType: "mp4",
    pageTitle: "Instagram",
    pageDomain: "www.instagram.com",
    url: `https://scontent.cdninstagram.com/${cdnFilename}?token=private#clip`,
  }), "Instagram_Video.mp4");
  assert.equal(createVideoDownloadFilename({
    mediaType: "webm",
    pageTitle: "夏の旅: Final/Clip?",
    pageDomain: "www.instagram.com",
    url: `https://scontent.cdninstagram.com/${cdnFilename}?token=private#clip`,
  }), "夏の旅_ Final_Clip_.webm");
  const longTitle = createVideoDownloadFilename({
    mediaType: "mp4",
    pageTitle: "é".repeat(100),
    pageDomain: "www.facebook.com",
    url: `https://video.xx.fbcdn.net/${cdnFilename}?tracking=1`,
  });
  assert.equal([...longTitle.slice(0, -4)].length, 80);
  assert.equal(longTitle.endsWith(".mp4"), true);
  assert.equal(createVideoDownloadFilename({
    mediaType: "mp4",
    pageTitle: "https://cdn.example/hash.mp4?token=private",
    pageDomain: "facebook.com",
  }), "Facebook_Video.mp4");
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

test("requires a trusted click and permits one nearby scripted anchor click", () => {
  const tracker = createDownloadGestureTracker(1_000);
  assert.equal(tracker.authorizeDownloadClick(false, 10), null);
  assert.equal(tracker.authorizeDownloadClick(true, 20), "trusted");

  tracker.noteTrustedClick(100);
  assert.equal(tracker.authorizeDownloadClick(false, 1_099), "correlated");
  assert.equal(tracker.authorizeDownloadClick(false, 1_100), null);

  tracker.noteTrustedClick(2_000);
  assert.equal(tracker.authorizeDownloadClick(false, 3_001), null);
  assert.equal(tracker.authorizeDownloadClick(true, 4_000), "trusted");
});

test("claims Chrome download IDs once while allowing distinct and intentional repeat downloads", () => {
  const processedIds = [];
  const first = appendProcessedDownloadId(processedIds, 12);
  assert.deepEqual(first, [12]);
  assert.equal(appendProcessedDownloadId(first, 12), null);

  const separateDownload = appendProcessedDownloadId(first, 13);
  assert.deepEqual(separateDownload, [12, 13]);

  const repeatedResource = {
    url: "https://files.example/report.pdf?token=first",
    filename: "report.pdf",
  };
  assert.equal(hasPendingDownloadPrompt({
    "download-12": { kind: "download", resource: repeatedResource },
  }, repeatedResource), true);
  assert.equal(hasPendingDownloadPrompt({}, repeatedResource), false);

  const repeatedUrlAfterAResolvedPrompt = appendProcessedDownloadId(separateDownload, 14);
  assert.deepEqual(repeatedUrlAfterAResolvedPrompt, [12, 13, 14]);
  assert.equal(appendProcessedDownloadId(repeatedUrlAfterAResolvedPrompt, -1), null);
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
  const intent = {
    url: "https://files.example/report.pdf?token=one",
    tabId: 7,
    at: 90_000,
    gestureKind: "trusted",
  };
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
  assert.deepEqual(takeUserDownloadIntent({
    url: intent.url,
    tabId: 7,
  }, [{ url: intent.url, tabId: 7, at: 99_000 }], 100_000), { intent: null, remaining: [] });
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
