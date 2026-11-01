export function createBrowserHandoffRunner(): <T>(
  url: string,
  handoff: () => Promise<T>,
) => Promise<T | undefined>;
