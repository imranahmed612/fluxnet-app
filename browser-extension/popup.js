import {
  DEFAULT_SETTINGS,
  classifyMediaUrl,
  deduplicateResources,
  normalizeSettings,
  validateMediaMessage,
  validateUnsupportedMessage,
} from "./media.js";

const byId = (id) => document.getElementById(id);
const statusNode = byId("connection");
const feedback = byId("feedback");
const resourceContainer = byId("resources");

function setStatus(status) {
  statusNode.textContent = status === "connected" ? "Connected" : status === "disconnected" ? "Disconnected" : "Needs setup";
  statusNode.className = `connection ${status}`;
}

function sendNativeMessage(message) {
  return new Promise((resolve, reject) => {
    chrome.runtime.sendNativeMessage("com.fluxnet.browser", message, (response) => {
      if (chrome.runtime.lastError) {
        reject(new Error(`${chrome.runtime.lastError.message}. Open FluxNet and confirm Native Messaging is installed.`));
      } else if (!response?.ok) {
        reject(new Error(response?.message || "FluxNet rejected the message."));
      } else {
        resolve(response);
      }
    });
  });
}

async function connectToFluxNet() {
  try {
    await sendNativeMessage({ action: "ping" });
    setStatus("connected");
    feedback.textContent = "Connected to FluxNet. Automatic detection is active; approve each download handoff.";
  } catch (error) {
    setStatus("disconnected");
    feedback.textContent = `${error.message}. FluxNet will reconnect automatically when available.`;
  }
}

function populateSettings(settings) {
  byId("automatic-video").checked = settings.automaticVideoDetection;
  byId("automatic-download").checked = settings.automaticDownloadDetection;
  byId("video-overlay").checked = settings.videoOverlay;
  byId("file-types").value = settings.fileTypes.join(", ");
  byId("excluded-domains").value = settings.excludedDomains.join(", ");
}

async function loadSettings() {
  const { settings } = await chrome.storage.local.get("settings");
  populateSettings(normalizeSettings(settings ?? DEFAULT_SETTINGS));
}

function renderResources(resources) {
  resourceContainer.replaceChildren();
  if (!resources.length) {
    const empty = document.createElement("p");
    empty.className = "empty";
    empty.textContent = "No video candidates detected yet.";
    resourceContainer.append(empty);
    return;
  }
  for (const resource of resources) {
    const card = document.createElement("article");
    card.className = `resource ${resource.downloadable ? "" : "unsupported"}`;
    const heading = document.createElement("div");
    heading.className = "resource-head";
    const title = document.createElement("span");
    title.className = "resource-title";
    title.textContent = resource.pageTitle || resource.pageDomain || "Media resource";
    const type = document.createElement("span");
    type.className = "type";
    type.textContent = String(resource.mediaType).toUpperCase();
    heading.append(title, type);
    const url = document.createElement("code");
    url.className = "resource-url";
    url.textContent = resource.url;
    url.title = resource.url;
    const meta = document.createElement("div");
    meta.className = "resource-meta";
    const details = [];
    if (resource.mimeType) details.push(resource.mimeType);
    if (resource.observedBytes !== null && resource.observedBytes !== undefined) details.push(`Observed ${resource.observedBytes} B`);
    if (resource.width && resource.height) details.push(`${resource.width} × ${resource.height}`);
    details.push(resource.downloadable ? "Direct file candidate" : "Manifest only; not downloadable");
    meta.textContent = details.join(" · ");
    card.append(heading, url, meta);

    const send = document.createElement("button");
    send.className = "send";
    send.textContent = resource.downloadable ? "Send to FluxNet" : "Report unsupported manifest";
    send.addEventListener("click", async () => {
      const message = {
        url: resource.url,
        mediaType: resource.mediaType,
        pageTitle: resource.pageTitle || "",
        pageDomain: resource.pageDomain || "",
        tabId: resource.tabId ?? null,
        observedBytes: resource.observedBytes ?? null,
        width: resource.width ?? null,
        height: resource.height ?? null,
        mimeType: resource.mimeType ?? null,
        filename: resource.filename ?? null,
        totalBytes: resource.totalBytes ?? null,
      };
      const valid = resource.downloadable
        ? validateMediaMessage(message)
        : validateUnsupportedMessage(message);
      if (!valid) {
        feedback.textContent = "This resource failed local URL validation.";
        return;
      }
      send.disabled = true;
      try {
        await sendNativeMessage({
          action: resource.downloadable ? "sendMedia" : "reportUnsupported",
          resource: message,
        });
        setStatus("connected");
        feedback.textContent = resource.downloadable
          ? "Selected link sent to FluxNet. Choose a destination there to queue the download."
          : "Unsupported manifest reported to FluxNet; no download was started.";
        send.textContent = "Sent to FluxNet";
      } catch (error) {
        setStatus("disconnected");
        feedback.textContent = error.message;
        send.disabled = false;
      }
    });
    card.append(send);
    resourceContainer.append(card);
  }
}

async function loadResources() {
  const { detectedResources = [] } = await chrome.storage.local.get("detectedResources");
  renderResources(deduplicateResources(detectedResources));
}

byId("scan").addEventListener("click", async () => {
  feedback.textContent = "Refreshing the active tab…";
  try {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    if (!tab?.id || !tab.url || !/^https?:\/\//i.test(tab.url)) {
      throw new Error("Open an HTTP or HTTPS page before scanning.");
    }
    const [result] = await chrome.scripting.executeScript({
      target: { tabId: tab.id },
      func: () => {
        const resources = [];
        const mediaElements = [...document.querySelectorAll("video")];
        for (const element of mediaElements) {
          for (const url of [element.currentSrc, element.src, ...[...element.querySelectorAll("source")].map((source) => source.src)]) {
            if (url) resources.push({
              url,
              observedBytes: null,
              width: element.videoWidth || null,
              height: element.videoHeight || null,
              mimeType: [...element.querySelectorAll("source")].find((source) => source.src === url)?.type
                || element.getAttribute("type") || "",
            });
          }
        }
        return {
          pageTitle: document.title.slice(0, 512),
          pageDomain: location.hostname.slice(0, 512),
          resources: resources.map((resource) => ({ ...resource, pageTitle: document.title.slice(0, 512), pageDomain: location.hostname.slice(0, 512) })),
        };
      },
    });
    const { detectedResources = [] } = await chrome.storage.local.get("detectedResources");
    const scanned = (result?.result?.resources ?? [])
      .filter((resource) => classifyMediaUrl(resource.url, resource.mimeType))
      .map((resource) => {
        const classified = classifyMediaUrl(resource.url, resource.mimeType);
        return { ...resource, ...classified };
      });
    const merged = deduplicateResources([...scanned, ...detectedResources]);
    await chrome.storage.local.set({ detectedResources: merged.slice(0, 100) });
    await loadResources();
    feedback.textContent = `${scanned.length} candidate(s) found. Nothing was sent or downloaded.`;
  } catch (error) {
    feedback.textContent = error.message || "Could not scan this tab.";
  }
});

byId("save-settings").addEventListener("click", async () => {
  const settings = normalizeSettings({
    automaticVideoDetection: byId("automatic-video").checked,
    automaticDownloadDetection: byId("automatic-download").checked,
    videoOverlay: byId("video-overlay").checked,
    fileTypes: byId("file-types").value.split(/[,\s]+/),
    excludedDomains: byId("excluded-domains").value.split(/[,\s]+/),
  });
  try {
    await chrome.storage.local.set({ settings });
    populateSettings(settings);
    feedback.textContent = "Detection settings saved.";
  } catch (error) {
    feedback.textContent = `Could not save detection settings: ${error.message}`;
  }
});

chrome.runtime.onMessage.addListener((message) => {
  if (message?.type === "fluxnet:resource-updated") void loadResources();
});

void Promise.all([connectToFluxNet(), loadSettings(), loadResources()]).catch((error) => {
  feedback.textContent = `Could not load extension settings: ${error.message}`;
});
