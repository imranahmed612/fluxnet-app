const byId = (id) => document.getElementById(id);
const promptId = new URLSearchParams(location.search).get("id");
let prompt;

function sendNativeMessage(message) {
  return new Promise((resolve, reject) => {
    chrome.runtime.sendNativeMessage("com.fluxnet.browser", message, (response) => {
      if (chrome.runtime.lastError) reject(new Error(chrome.runtime.lastError.message));
      else if (!response?.ok) reject(new Error(response?.message || "FluxNet rejected the message."));
      else resolve(response);
    });
  });
}

function formatBytes(bytes) {
  if (!Number.isFinite(bytes) || bytes < 0) return "Size not reported";
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let value = bytes;
  let unit = -1;
  do {
    value /= 1024;
    unit += 1;
  } while (value >= 1024 && unit < units.length - 1);
  return `${value.toFixed(value >= 10 ? 0 : 1)} ${units[unit]}`;
}

function render() {
  if (!prompt) {
    byId("prompt-feedback").textContent = "This prompt expired. The browser download was not changed.";
    byId("accept").disabled = true;
    return;
  }
  const resource = prompt.resource;
  const browserDownload = prompt.kind === "download";
  byId("prompt-kind").textContent = browserDownload ? "Browser download detected" : "Video resource detected";
  byId("filename").textContent = resource.filename || resource.pageTitle || "Video resource";
  byId("source").textContent = resource.pageDomain || "Unknown source";
  byId("type").textContent = resource.mimeType
    ? `${resource.mimeType} · ${String(resource.mediaType).toUpperCase()}`
    : `${String(resource.mediaType).toUpperCase()} · MIME type not reported`;
  byId("size").textContent = browserDownload
    ? formatBytes(resource.totalBytes)
    : (resource.width && resource.height ? `${resource.width} × ${resource.height}` : "Resolution not reported");
  byId("limitation").textContent = browserDownload
    ? "Chrome reports downloads after transfer begins. FluxNet leaves the browser copy running while you choose a destination and confirms the FluxNet queue; only then should you cancel the browser copy if you want to avoid downloading twice."
    : "FluxNet will receive this link for review. Choose a destination in the FluxNet app before it enters the download queue.";
  byId("remember").disabled = !prompt.fileType;
  byId("prompt-feedback").textContent = "";
}

async function decide(decision) {
  const accept = byId("accept");
  const continueButton = byId("continue");
  accept.disabled = true;
  continueButton.disabled = true;
  try {
    const result = await chrome.runtime.sendMessage({
      type: "fluxnet:prompt-decision",
      id: promptId,
      decision,
      remember: byId("remember").checked,
    });
    if (!result?.ok) throw new Error(result?.error || "Could not apply this choice.");
    byId("prompt-feedback").textContent = result.message;
    window.setTimeout(() => window.close(), 700);
  } catch (error) {
    byId("prompt-feedback").textContent = `${error.message}. Your browser download continues normally.`;
    accept.disabled = false;
    continueButton.disabled = false;
  }
}

byId("accept").addEventListener("click", () => void decide("fluxnet"));
byId("continue").addEventListener("click", () => void decide("browser"));

void chrome.runtime.sendMessage({ type: "fluxnet:get-prompt", id: promptId }).then((result) => {
  prompt = result;
  render();
}).catch((error) => {
  byId("prompt-feedback").textContent = `Could not load prompt: ${error.message}`;
});
