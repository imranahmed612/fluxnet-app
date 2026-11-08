function createDownloadGestureTracker(windowMs = 1_000) {
  let trustedAt = null;
  let correlatedGestureAvailable = false;

  return Object.freeze({
    noteTrustedClick(now = performance.now()) {
      trustedAt = now;
      correlatedGestureAvailable = true;
    },
    authorizeDownloadClick(isTrusted, now = performance.now()) {
      if (isTrusted) {
        trustedAt = now;
        correlatedGestureAvailable = false;
        return "trusted";
      }
      if (!correlatedGestureAvailable || trustedAt === null
        || now < trustedAt || now - trustedAt > windowMs) return null;
      correlatedGestureAvailable = false;
      return "correlated";
    },
  });
}

globalThis.FluxNetDownloadGesture = Object.freeze({ createDownloadGestureTracker });
