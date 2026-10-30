import {
  DEFAULT_SETTINGS,
  classifyMediaUrl,
  createDownloadResource,
  createNativeMessage,
  deduplicateResources,
  downloadPromptDecision,
  downloadResourceIdentity,
  isEligibleDownload,
  isExcludedDomain,
  normalizeSettings,
  validateDownloadMessage,
  validateMediaMessage,
} from "./media.js";

const RESOURCE_LIMIT = 100;
const MAX_PENDING_PROMPTS = 5;
const processingMedia = new Set();
const processingDownloadKeys = new Set();
let promptCreationQueue = Promise.resolve();

async function getSettings() {
  const { settings } = await chrome.storage.local.get("settings");
  return normalizeSettings(settings ?? DEFAULT_SETTINGS);
}

function sendNativeMessage(message) {
  return new Promise((resolve, reject) => {
    chrome.runtime.sendNativeMessage("com.fluxnet.browser", message, (response) => {
      if (chrome.runtime.lastError) {
        reject(new Error(chrome.runtime.lastError.message));
      } else if (!response?.ok) {
        reject(new Error(response?.message || "FluxNet rejected the message."));
      } else {
        resolve(response);
      }
    });
  });
}

async function updateBadge(count) {
  await chrome.action.setBadgeBackgroundColor({ color: "#19a5b2" });
  await chrome.action.setBadgeText({ text: count > 0 ? String(Math.min(count, 99)) : "" });
}

async function activeTabMatches(tabId) {
  if (!Number.isInteger(tabId) || tabId < 0) return false;
  const tabs = await chrome.tabs.query({ active: true, lastFocusedWindow: true });
  return tabs.some((tab) => tab.id === tabId);
}

async function rememberResource(resource) {
  const { detectedResources = [] } = await chrome.storage.local.get("detectedResources");
  const next = deduplicateResources([resource, ...detectedResources]).slice(0, RESOURCE_LIMIT);
  await chrome.storage.local.set({ detectedResources: next });
  await updateBadge(next.length);
  return !detectedResources.some((item) => item.url === resource.url);
}

async function createPrompt(prompt) {
  const task = promptCreationQueue.then(() => createPromptOnce(prompt));
  promptCreationQueue = task.catch((error) => console.error("FluxNet prompt queue failed:", error));
  return task;
}

async function createPromptOnce(prompt) {
  const current = await chrome.storage.session.get("pendingPrompts");
  const prompts = current.pendingPrompts ?? {};
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
  if (!sender.tab?.id || !await activeTabMatches(sender.tab.id)) return;
  const settings = await getSettings();
  if (!settings.automaticVideoDetection) return;
  const classified = classifyMediaUrl(candidate.url, candidate.mimeType);
  if (!classified) return;
  const mediaKey = `${sender.tab.id}:${classified.url}`;
  if (processingMedia.has(mediaKey)) return;
  processingMedia.add(mediaKey);
  try {
    const sourceUrl = sender.tab.url || "";
    let pageDomain = "";
    try {
      pageDomain = new URL(sourceUrl).hostname.slice(0, 512);
    } catch {
      return;
    }
    if (isExcludedDomain(pageDomain, settings.excludedDomains)) return;
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
    const isNew = await rememberResource(resource);
    if (isNew) {
      void chrome.runtime.sendMessage({ type: "fluxnet:resource-updated" }).catch((error) => {
        console.debug("FluxNet popup is not open to receive the new resource:", error);
      });
      if (classified.downloadable && settings.videoOverlay && !resource.fromPlayer) {
        void chrome.tabs.sendMessage(sender.tab.id, {
          type: "fluxnet:resource-confirmed",
          url: resource.url,
          mimeType: resource.mimeType,
        }).catch((error) => console.debug("FluxNet could not attach the video overlay:", error));
      }
    }
    if (isNew && settings.notificationBehavior === "desktopPrompt" && classified.downloadable) {
      await createPrompt({ id: `media-${crypto.randomUUID()}`, kind: "media", resource });
    }
  } finally {
    processingMedia.delete(mediaKey);
  }
}

async function digestResourceKey(item) {
  const key = downloadResourceIdentity(item);
  if (!key) throw new Error("The browser download URL is invalid.");
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(key));
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function detectBrowserDownload(item) {
  const settings = await getSettings();
  if (!settings.automaticDownloadDetection || !isEligibleDownload(item, settings)) return;
  const key = await digestResourceKey(item);
  if (processingDownloadKeys.has(key)) return;
  processingDownloadKeys.add(key);
  try {
    const now = Date.now();
    const { promptedDownloadKeys = [] } = await chrome.storage.local.get("promptedDownloadKeys");
    const recent = promptedDownloadKeys.filter((entry) => now - entry.at < 7 * 24 * 60 * 60 * 1000);
    if (recent.some((entry) => entry.key === key)) return;
    if (recent.length >= RESOURCE_LIMIT) recent.splice(0, recent.length - RESOURCE_LIMIT + 1);
    recent.push({ key, at: now });
    await chrome.storage.local.set({ promptedDownloadKeys: recent });

    const resource = createDownloadResource(item);
    if (!resource) return;
    const prompt = {
      id: `download-${item.id}`,
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
      .then(() => sendResponse({ ok: true }))
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
    const { pairingCode } = await chrome.storage.local.get("pairingCode");
    const resource = prompt.resource;
    const nativeAction = prompt.kind === "media" ? "sendMedia" : "sendDownload";
    const valid = nativeAction === "sendMedia" ? validateMediaMessage(resource) : validateDownloadMessage(resource);
    if (!pairingCode || !valid) throw new Error("Pair FluxNet and verify the download details before sending.");
    await sendNativeMessage(createNativeMessage(nativeAction, pairingCode, resource));
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
    if (!classified) return;
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
