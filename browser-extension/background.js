import {
  DEFAULT_SETTINGS,
  appendProcessedDownloadId,
  browserMediaIdentity,
  classifyMediaUrl,
  createDownloadResource,
  createNativeMessage,
  createVideoDownloadFilename,
  deduplicateResources,
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
} from "./media.js";

const RESOURCE_LIMIT = 100;
const MAX_PENDING_PROMPTS = 5;
const MAX_RECONNECT_ATTEMPTS = 6;
const RECONNECT_ALARM = "fluxnet-native-reconnect";
const HEALTH_ALARM = "fluxnet-native-health-check";
const processingMedia = new Map();
const processingDownloadKeys = new Set();
let promptCreationQueue = Promise.resolve();
let userDownloadIntentQueue = Promise.resolve();
let downloadIdClaimQueue = Promise.resolve();

function enqueueDownloadIntent(operation) {
  const task = userDownloadIntentQueue.then(operation);
  userDownloadIntentQueue = task.catch((error) => console.error("FluxNet user-download intent processing failed:", error));
  return task;
}

async function getSettings() {
  const { settings } = await chrome.storage.local.get("settings");
  return normalizeSettings(settings ?? DEFAULT_SETTINGS);
}

function sendNativeMessage(message) {
  return new Promise((resolve, reject) => {
    chrome.runtime.sendNativeMessage("com.fluxnet.browser", message, (response) => {
      if (chrome.runtime.lastError) {
        void chrome.storage.local.set({ fluxnetConnected: false });
        void scheduleReconnect();
        reject(new Error(chrome.runtime.lastError.message));
      } else if (!response?.ok) {
        void chrome.storage.local.set({ fluxnetConnected: false });
        reject(new Error(response?.message || "FluxNet rejected the message."));
      } else {
        void chrome.storage.local.set({ fluxnetConnected: true, reconnectAttempt: 0 });
        void chrome.alarms.clear(RECONNECT_ALARM);
        void chrome.alarms.create(HEALTH_ALARM, { delayInMinutes: 2 });
        resolve(response);
      }
    });
  });
}

async function scheduleReconnect() {
  const { reconnectAttempt = 0 } = await chrome.storage.local.get("reconnectAttempt");
  if (reconnectAttempt >= MAX_RECONNECT_ATTEMPTS) return;
  const nextAttempt = reconnectAttempt + 1;
  await chrome.storage.local.set({ reconnectAttempt: nextAttempt });
  await chrome.alarms.create(RECONNECT_ALARM, {
    delayInMinutes: reconnectDelayMinutes(nextAttempt),
  });
}

async function connectToFluxNet() {
  try {
    await sendNativeMessage({ action: "ping" });
  } catch (error) {
    console.debug("FluxNet Native Messaging is not available yet:", error);
  } finally {
    await chrome.alarms.create(HEALTH_ALARM, { delayInMinutes: 2 });
  }
}

chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === RECONNECT_ALARM) void connectToFluxNet();
  if (alarm.name === HEALTH_ALARM) void connectToFluxNet();
});
chrome.runtime.onStartup.addListener(() => {
  void chrome.storage.local.set({ reconnectAttempt: 0 });
  void connectToFluxNet();
});
chrome.runtime.onInstalled.addListener(() => {
  void chrome.storage.local.set({ reconnectAttempt: 0 });
  void connectToFluxNet();
});
void connectToFluxNet();

async function updateBadge(count) {
  await chrome.action.setBadgeBackgroundColor({ color: "#19a5b2" });
  await chrome.action.setBadgeText({ text: count > 0 ? String(Math.min(count, 99)) : "" });
}

async function rememberResource(resource) {
  const { detectedResources = [] } = await chrome.storage.local.get("detectedResources");
  const next = deduplicateResources([resource, ...detectedResources]).slice(0, RESOURCE_LIMIT);
  await chrome.storage.local.set({ detectedResources: next });
  await updateBadge(next.length);
  const identity = browserMediaIdentity(resource);
  const previous = detectedResources.find((item) => browserMediaIdentity(item) === identity);
  return !previous
    || previous.tabId !== resource.tabId
    || (resource.downloadable === true && previous.downloadable !== true)
    || (resource.hlsPlaylist && !previous.hlsPlaylist);
}

async function fetchManifestDetails(resource) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 8_000);
  try {
    const response = await fetch(resource.url, {
      credentials: "omit",
      redirect: "follow",
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`The manifest server returned HTTP ${response.status}.`);
    const finalUrl = new URL(response.url);
    if (!["http:", "https:"].includes(finalUrl.protocol)) throw new Error("The manifest redirected to an unsupported URL.");
    const declaredLength = Number(response.headers.get("content-length"));
    if (Number.isFinite(declaredLength) && declaredLength > 1_048_576) {
      throw new Error("The manifest is larger than the 1 MiB inspection limit.");
    }
    if (!response.body) throw new Error("The manifest response did not contain a readable body.");
    const reader = response.body.getReader();
    const chunks = [];
    let totalBytes = 0;
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        totalBytes += value.byteLength;
        if (totalBytes > 1_048_576) {
          await reader.cancel();
          throw new Error("The manifest is larger than the 1 MiB inspection limit.");
        }
        chunks.push(value);
      }
    } finally {
      reader.releaseLock();
    }
    const bytes = new Uint8Array(totalBytes);
    let offset = 0;
    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.byteLength;
    }
    const text = new TextDecoder().decode(bytes);
    return resource.mediaType === "hls"
      ? parseHlsManifest(text, response.url)
      : parseDashManifest(text, response.url);
  } finally {
    clearTimeout(timeout);
  }
}

async function createPrompt(prompt) {
  const task = promptCreationQueue.then(() => createPromptOnce(prompt));
  promptCreationQueue = task.catch((error) => console.error("FluxNet prompt queue failed:", error));
  return task;
}

async function createPromptOnce(prompt) {
  const current = await chrome.storage.session.get("pendingPrompts");
  const prompts = current.pendingPrompts ?? {};
  if (prompts[prompt.id]
    || (prompt.kind === "download" && hasPendingDownloadPrompt(prompts, prompt.resource))) return false;
  if (Object.keys(prompts).length >= MAX_PENDING_PROMPTS) return false;
  prompts[prompt.id] = prompt;
  await chrome.storage.session.set({ pendingPrompts: prompts });
  try {
    const window = await chrome.windows.create({
      url: chrome.runtime.getURL(`prompt.html?id=${encodeURIComponent(prompt.id)}`),
      type: "popup",
      width: 390,
      height: 330,
      focused: true,
    });
    prompts[prompt.id].windowId = window.id;
    await chrome.storage.session.set({ pendingPrompts: prompts });
    return true;
  } catch (error) {
    delete prompts[prompt.id];
    await chrome.storage.session.set({ pendingPrompts: prompts });
    throw error;
  }
}

async function detectMedia(candidate, sender) {
  if (!Number.isInteger(sender.tab?.id) || sender.tab.id < 0) return false;
  const settings = await getSettings();
  if (!settings.automaticVideoDetection) return false;
  const classified = classifyMediaUrl(candidate.url, candidate.mimeType, candidate.playableVideo === true);
  if (!classified) return false;
  const mediaKey = `${sender.tab.id}:${classified.url}`;
  const activeProcessing = processingMedia.get(mediaKey);
  if (activeProcessing) {
    if (candidate.fromPlayer === true) activeProcessing.playerCandidate = candidate;
    return await activeProcessing.promise;
  }
  const processing = {
    playerCandidate: candidate.fromPlayer === true ? candidate : null,
    promise: Promise.resolve(),
  };
  const task = processMediaCandidate(candidate, sender, classified, processing, settings);
  processing.promise = task;
  processingMedia.set(mediaKey, processing);
  try {
    return await task;
  } finally {
    if (processingMedia.get(mediaKey) === processing) processingMedia.delete(mediaKey);
  }
}

async function processMediaCandidate(candidate, sender, classified, processing, settings) {
  const sourceUrl = sender.tab.url || "";
  let pageDomain = "";
  try {
    pageDomain = new URL(sourceUrl).hostname.slice(0, 512);
  } catch {
    return false;
  }
  if (isExcludedDomain(pageDomain, settings.excludedDomains)) return false;
  const resource = {
    ...classified,
    pageTitle: String(candidate.pageTitle || sender.tab.title || "").slice(0, 512),
    pageDomain,
    tabId: sender.tab.id,
    observedBytes: Number.isSafeInteger(candidate.observedBytes) && candidate.observedBytes > 0 ? candidate.observedBytes : null,
    width: Number.isInteger(candidate.width) && candidate.width > 0 ? candidate.width : null,
    height: Number.isInteger(candidate.height) && candidate.height > 0 ? candidate.height : null,
    mimeType: typeof candidate.mimeType === "string" ? candidate.mimeType.slice(0, 128) : null,
    detectedAt: Date.now(),
    fromPlayer: candidate.fromPlayer === true,
  };
  if (classified.mediaType === "hls" || classified.mediaType === "dash") {
    try {
      const manifest = await fetchManifestDetails(resource);
      resource.manifestVariants = manifest.variants;
      resource.manifestAudioTracks = manifest.audioTracks;
      resource.hlsPlaylist = manifest.playlist || null;
      resource.downloadable = classified.mediaType === "hls" && Boolean(manifest.playlist);
      const audioSummary = manifest.audioTracks.slice(0, 5)
        .map((track) => String(track.name).slice(0, 80))
        .join(", ");
      resource.manifestStatus = manifest.playlist
        ? "Finite, unencrypted HLS fMP4 playlist confirmed."
        : manifest.variants.length
        ? `Manifest parsed: ${manifest.variants.length} variant(s). ${manifest.audioTracks.length ? `Audio: ${audioSummary}${manifest.audioTracks.length > 5 ? ", and more" : ""}. ` : ""}Segment merging is unavailable.`
        : "Manifest parsed, but no video variants were reported.";
    } catch (error) {
      resource.manifestVariants = [];
      resource.manifestAudioTracks = [];
      resource.hlsPlaylist = null;
      resource.downloadable = false;
      resource.manifestStatus = `Manifest unavailable: ${error.message}`;
      console.debug("FluxNet could not inspect the video manifest:", error);
    }
  }
  if (processing.playerCandidate) {
    resource.fromPlayer = true;
    resource.width = Number.isInteger(processing.playerCandidate.width) ? processing.playerCandidate.width : resource.width;
    resource.height = Number.isInteger(processing.playerCandidate.height) ? processing.playerCandidate.height : resource.height;
  }
  if (classified.mediaType === "hls" && !resource.fromPlayer) {
    resource.downloadable = false;
    resource.hlsPlaylist = null;
    resource.manifestStatus = "The HLS playlist is not associated with a detected video player.";
  }
  if (!classified.downloadable && !resource.downloadable && !resource.fromPlayer
    && !resource.manifestVariants.some((variant) => variant.video === true)) return false;
  if (resource.downloadable && !validateMediaMessage(resource)) {
    throw new Error("The detected video source did not pass validation.");
  }
  if (classified.mediaType === "unsupported-video" && !validateUnsupportedMessage(resource)) {
    throw new Error("The detected video source did not pass validation.");
  }
  const isNew = await rememberResource(resource);
  if (isNew) {
    void chrome.runtime.sendMessage({ type: "fluxnet:resource-updated" }).catch((error) => {
      console.debug("FluxNet popup is not open to receive the new resource:", error);
    });
  }
  if (settings.videoOverlay) {
    void chrome.tabs.sendMessage(sender.tab.id, {
      type: "fluxnet:resource-confirmed",
      url: resource.url,
      mediaType: resource.mediaType,
      mimeType: resource.mimeType,
      manifestVariants: resource.manifestVariants || [],
      manifestAudioTracks: resource.manifestAudioTracks || [],
      downloadable: resource.downloadable === true,
      hlsPlaylist: resource.hlsPlaylist || null,
      manifestStatus: resource.manifestStatus || "",
    }).catch((error) => console.debug("FluxNet could not attach the video overlay:", error));
  }
  return true;
}

async function digestResourceKey(item) {
  const key = downloadResourceIdentity(item);
  if (!key) throw new Error("The browser download URL is invalid.");
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(key));
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function recordUserDownloadClick(message, sender) {
  if (message.gestureKind !== "trusted" && message.gestureKind !== "correlated") return;
  if (!Number.isInteger(sender.tab?.id) || sender.tab.id < 0) return;
  const settings = await getSettings();
  if (!settings.automaticDownloadDetection) return;
  const url = typeof message.url === "string" ? message.url : "";
  const filename = typeof message.filename === "string" ? message.filename.slice(0, 512) : "";
  const referrer = sender.tab.url || "";
  const candidate = { url, filename, referrer };
  if (!isEligibleDownload(candidate, settings)) return;
  const resource = createDownloadResource({
    ...candidate,
    tabId: sender.tab.id,
    pageTitle: sender.tab.title,
  });
  if (!resource) return;
  const { userDownloadClicks = [] } = await chrome.storage.session.get("userDownloadClicks");
  const now = Date.now();
  const recent = userDownloadClicks.filter((entry) => now - entry.at < 30_000);
  recent.push({
    url: resource.url,
    filename: resource.filename,
    gestureKind: message.gestureKind,
    pageTitle: String(sender.tab.title || resource.filename).slice(0, 512),
    pageDomain: resource.pageDomain,
    tabId: sender.tab.id,
    allowDifferentTab: message.allowNewTab === true,
    referrer,
    at: now,
  });
  await chrome.storage.session.set({ userDownloadClicks: recent.slice(-RESOURCE_LIMIT) });
}

async function consumeUserDownloadClick(item) {
  return enqueueDownloadIntent(async () => {
    const { userDownloadClicks = [] } = await chrome.storage.session.get("userDownloadClicks");
    const { intent, remaining } = takeUserDownloadIntent(item, userDownloadClicks);
    await chrome.storage.session.set({ userDownloadClicks: remaining });
    return intent;
  });
}

function claimDownloadId(downloadId) {
  const task = downloadIdClaimQueue.then(async () => {
    const { processedDownloadIds = [] } = await chrome.storage.session.get("processedDownloadIds");
    const nextIds = appendProcessedDownloadId(processedDownloadIds, downloadId);
    if (!nextIds) return false;
    await chrome.storage.session.set({ processedDownloadIds: nextIds });
    return true;
  });
  downloadIdClaimQueue = task.catch((error) => {
    console.error("Could not claim Chrome download event ID:", error);
  });
  return task;
}

async function detectBrowserDownload(item) {
  if (!await claimDownloadId(item.id)) return;
  const settings = await getSettings();
  if (!settings.automaticDownloadDetection) return;
  const intent = await consumeUserDownloadClick(item);
  if (!intent) return;
  const initiatedItem = {
    ...item,
    filename: intent.filename || item.filename,
    referrer: intent.referrer || item.referrer,
  };
  if (!isEligibleDownload(initiatedItem, settings)) return;
  const key = await digestResourceKey(initiatedItem);
  if (processingDownloadKeys.has(key)) return;
  processingDownloadKeys.add(key);
  try {
    const { pendingPrompts = {} } = await chrome.storage.session.get("pendingPrompts");
    if (hasPendingDownloadPrompt(pendingPrompts, initiatedItem)) return;

    const resource = {
      ...createDownloadResource(initiatedItem),
      pageTitle: intent.pageTitle,
      pageDomain: intent.pageDomain,
    };
    if (!resource) return;
    const promptId = `download-${item.id}`;
    const prompt = {
      id: promptId,
      kind: "download",
      fileType: resource.mediaType,
      downloadId: item.id,
      resource,
    };
    try {
      await createPrompt(prompt);
    } catch (error) {
      console.error("Could not display FluxNet download prompt:", error);
    }
  } finally {
    processingDownloadKeys.delete(key);
  }
}

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message?.type === "fluxnet:media") {
    void detectMedia(message.resource || {}, sender)
      .then((registered) => sendResponse({ ok: registered === true }))
      .catch((error) => {
        console.error("Could not record browser media:", error);
        sendResponse({ ok: false });
      });
    return true;
  }
  if (message?.type === "fluxnet:open-media-prompt") {
    void (async () => {
      const { detectedResources = [] } = await chrome.storage.local.get("detectedResources");
      const resource = detectedResources.find((item) => item.url === message.url);
      if (!resource || !resource.downloadable) throw new Error("Media resource is no longer available.");
      await createPrompt({ id: `media-${crypto.randomUUID()}`, kind: "media", resource });
    })().then(() => sendResponse({ ok: true })).catch((error) => {
      console.error("Could not open FluxNet media prompt:", error);
      sendResponse({ ok: false });
    });
    return true;
  }
  if (message?.type === "fluxnet:video-download") {
    void (async () => {
      if (!Number.isInteger(sender.tab?.id) || sender.tab.id < 0) {
        throw new Error("The source browser tab is no longer available.");
      }
      const { detectedResources = [] } = await chrome.storage.local.get("detectedResources");
      const resource = detectedResources.find((item) => item.url === message.url);
      if (!resource || resource.tabId !== sender.tab.id || !resource.downloadable || resource.mediaType !== message.format
        || !validateMediaMessage(resource)) {
        throw new Error("The selected video format is no longer available.");
      }
      const download = {
        url: resource.url,
        mediaType: resource.mediaType,
        downloadable: resource.downloadable === true,
        pageTitle: resource.pageTitle,
        pageDomain: resource.pageDomain,
        tabId: sender.tab.id,
        filename: createVideoDownloadFilename(resource),
        mimeType: resource.mimeType || null,
        totalBytes: null,
        observedBytes: resource.observedBytes ?? null,
        width: resource.width ?? null,
        height: resource.height ?? null,
        hlsPlaylist: resource.hlsPlaylist ?? null,
      };
      if (!validateDownloadMessage(download)) throw new Error("The selected video did not pass download validation.");
      await sendNativeMessage(createNativeMessage("sendDownload", download));
      return { ok: true };
    })().then(sendResponse).catch((error) => {
      console.error("Could not start the selected FluxNet video download:", error);
      sendResponse({ ok: false, error: error.message });
    });
    return true;
  }
  if (message?.type === "fluxnet:user-download-click") {
    if (message.gestureKind !== "trusted" && message.gestureKind !== "correlated") {
      sendResponse({ ok: false, error: "A recent trusted browser click is required." });
      return false;
    }
    void enqueueDownloadIntent(() => recordUserDownloadClick(message, sender))
      .then(() => sendResponse({ ok: true }))
      .catch((error) => {
        console.error("Could not record a user-clicked download link:", error);
        sendResponse({ ok: false, error: error.message });
      });
    return true;
  }
  if (message?.type === "fluxnet:prompt-decision") {
    void handlePromptDecision(message)
      .then((result) => sendResponse(result))
      .catch((error) => {
        console.error("Could not process FluxNet prompt decision:", error);
        sendResponse({ ok: false, error: error.message });
      });
    return true;
  }
  if (message?.type === "fluxnet:get-prompt") {
    void chrome.storage.session.get("pendingPrompts")
      .then(({ pendingPrompts = {} }) => sendResponse(pendingPrompts[message.id] ?? null))
      .catch((error) => {
        console.error("Could not load FluxNet prompt:", error);
        sendResponse(null);
      });
    return true;
  }
  if (message?.type === "fluxnet:scan-results") {
    void chrome.storage.local.get("detectedResources")
      .then(({ detectedResources = [] }) => sendResponse(detectedResources));
    return true;
  }
  return false;
});

async function handlePromptDecision({ id, decision, remember }) {
  const { pendingPrompts = {} } = await chrome.storage.session.get("pendingPrompts");
  const prompt = pendingPrompts[id];
  if (!prompt) return { ok: false, error: "This prompt has expired." };
  if (decision !== "fluxnet" && decision !== "browser") throw new Error("Invalid prompt decision.");

  if (remember) {
    const settings = await getSettings();
    const updated = downloadPromptDecision(decision, prompt.fileType, true, settings);
    if (updated) await chrome.storage.local.set({ settings: updated });
  }

  let message = "Your browser download will continue normally.";
  if (decision === "fluxnet") {
    const resource = prompt.resource;
    const nativeAction = prompt.kind === "media" ? "sendMedia" : "sendDownload";
    const valid = nativeAction === "sendMedia" ? validateMediaMessage(resource) : validateDownloadMessage(resource);
    if (!valid) throw new Error("Verify the download details before sending.");
    await sendNativeMessage(createNativeMessage(nativeAction, resource));
    message = prompt.kind === "media"
      ? "Sent to FluxNet. Choose a destination in the app to queue it."
      : "FluxNet received the item. Choose a destination in its save dialog; the browser download remains active as a fallback.";
  }
  delete pendingPrompts[id];
  await chrome.storage.session.set({ pendingPrompts });
  return { ok: true, message };
}

chrome.webRequest.onBeforeRequest.addListener(
  (details) => {
    if (details.tabId < 0) return;
    const classified = classifyMediaUrl(details.url);
    if (!classified || classified.downloadable) return;
    void chrome.tabs.get(details.tabId)
      .then((tab) => detectMedia({ url: details.url, pageTitle: tab.title }, { tab }))
      .catch((error) => console.debug("FluxNet could not inspect the requesting tab:", error));
  },
  { urls: ["http://*/*", "https://*/*"], types: ["main_frame", "sub_frame", "xmlhttprequest", "media", "other"] },
);

chrome.webRequest.onHeadersReceived.addListener(
  (details) => {
    if (details.tabId < 0) return;
    const header = details.responseHeaders?.find((item) => item.name.toLowerCase() === "content-type");
    const mimeType = header?.value?.split(";")[0]?.trim() || "";
    if (!/^video\//i.test(mimeType)) return;
    void chrome.tabs.get(details.tabId).then((tab) => (
      detectMedia({ url: details.url, mimeType, pageTitle: tab.title }, { tab })
    )).catch((error) => console.debug("FluxNet could not inspect the requesting tab:", error));
  },
  { urls: ["http://*/*", "https://*/*"], types: ["main_frame", "sub_frame", "xmlhttprequest", "media", "other"] },
  ["responseHeaders"],
);

chrome.downloads.onCreated.addListener((item) => {
  void detectBrowserDownload(item).catch((error) => console.error("Could not process browser download event:", error));
});

chrome.tabs.onActivated.addListener(({ tabId }) => {
  void chrome.tabs.sendMessage(tabId, { type: "fluxnet:collect" }).catch((error) => {
    console.debug("FluxNet content script is not available in the active tab:", error);
  });
});

chrome.tabs.onUpdated.addListener((tabId, changeInfo) => {
  if (changeInfo.status === "complete" || changeInfo.url) {
    void chrome.tabs.sendMessage(tabId, { type: "fluxnet:collect" }).catch((error) => {
      console.debug("FluxNet content script is not available after navigation:", error);
    });
  }
});

chrome.windows.onRemoved.addListener((windowId) => {
  void chrome.storage.session.get("pendingPrompts").then(async ({ pendingPrompts = {} }) => {
    for (const [id, prompt] of Object.entries(pendingPrompts)) {
      if (prompt.windowId === windowId) delete pendingPrompts[id];
    }
    await chrome.storage.session.set({ pendingPrompts });
  }).catch((error) => console.error("Could not clear closed prompt:", error));
});

chrome.storage.onChanged.addListener((changes, area) => {
  if (area !== "local" || !changes.settings) return;
  const settings = normalizeSettings(changes.settings.newValue);
  void chrome.tabs.query({}).then((tabs) => {
    tabs.forEach((tab) => {
      if (!tab.id) return;
      void chrome.tabs.sendMessage(tab.id, { type: "fluxnet:settings", settings }).catch((error) => {
        console.debug("FluxNet settings could not reach a tab content script:", error);
      });
    });
  }).catch((error) => console.error("Could not update settings in open tabs:", error));
});

void chrome.storage.local.get("detectedResources")
  .then(({ detectedResources = [] }) => updateBadge(detectedResources.length))
  .catch((error) => console.error("Could not initialize FluxNet badge:", error));
