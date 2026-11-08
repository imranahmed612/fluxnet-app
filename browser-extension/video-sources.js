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

function selectVideoOverlayPlayer(players, resourceUrl) {
  const usable = players.filter((player) => player.visible
    && Number.isFinite(player.width) && player.width > 0
    && Number.isFinite(player.height) && player.height > 0);
  const exact = usable.filter((player) => player.urls.includes(resourceUrl));
  if (exact.length === 1) return exact[0].video;
  if (exact.length > 1) {
    const playingExact = exact.filter((player) => player.playing);
    return playingExact.length === 1 ? playingExact[0].video : null;
  }
  const playing = usable.filter((player) => player.playing);
  return playing.length === 1 ? playing[0].video : null;
}

globalThis.FluxNetVideoSources = Object.freeze({
  videoSourceLabel,
  canDownloadVideoSource,
});
globalThis.FluxNetVideoMedia = Object.freeze({ selectVideoOverlayPlayer });
