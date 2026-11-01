export function createBrowserHandoffRunner() {
  const inFlightUrls = new Set();
  const handledUrls = new Set();

  return async function run(url, handoff) {
    if (inFlightUrls.has(url) || handledUrls.has(url)) return undefined;
    inFlightUrls.add(url);
    try {
      const result = await handoff();
      if (result === true) handledUrls.add(url);
      return result;
    } finally {
      inFlightUrls.delete(url);
    }
  };
}
