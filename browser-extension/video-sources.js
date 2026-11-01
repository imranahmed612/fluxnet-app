function videoSourceLabel(source, index, sourceCount) {
  if (Number.isInteger(source.height) && source.height > 0) {
    const dimensions = Number.isInteger(source.width) && source.width > 0
      ? ` (${source.width} × ${source.height})`
      : "";
    const label = `${source.height}p${dimensions}`;
    return sourceCount > 1 ? `${label} · source ${index + 1}` : label;
  }
  const name = sourceCount > 1 ? `Source ${index + 1}` : "Original source";
  return `${name} · ${String(source.mediaType || "video").toUpperCase()} · resolution unavailable`;
}

function canDownloadVideoSource(source, downloadStarted = false) {
  return !downloadStarted && source?.registered === true;
}

globalThis.FluxNetVideoSources = Object.freeze({
  videoSourceLabel,
  canDownloadVideoSource,
});
