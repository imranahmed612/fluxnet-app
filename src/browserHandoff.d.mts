export function createBrowserHandoffRunner(): <T>(
  url: string,
  handoff: () => Promise<T>,
) => Promise<T | undefined>;

export function openUserInitiatedBrowserDownloadDialog<T>(
  activateWindow: () => Promise<unknown>,
  openDialog: () => Promise<T>,
): Promise<T>;
