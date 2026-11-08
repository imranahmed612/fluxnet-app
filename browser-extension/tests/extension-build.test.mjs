import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { buildExtension } from "../build.mjs";

test("reproducible extension package includes every manifest resource and gesture script", async () => {
  const tempDirectory = await mkdtemp(path.join(os.tmpdir(), "fluxnet-extension-build-"));
  const outputDirectory = path.join(tempDirectory, "unpacked");
  try {
    await buildExtension(outputDirectory);
    const manifest = JSON.parse(await readFile(path.join(outputDirectory, "manifest.json"), "utf8"));
    const manifestFiles = [
      ...Object.values(manifest.icons || {}),
      ...(manifest.content_scripts || []).flatMap(({ js = [], css = [] }) => [...js, ...css]),
      manifest.background?.service_worker,
      manifest.action?.default_popup,
      ...Object.values(manifest.action?.default_icon || {}),
      manifest.options_page,
      manifest.options_ui?.page,
    ].filter(Boolean);

    assert.ok(manifest.content_scripts.some(({ js }) => js.includes("download-gesture.js")));
    for (const file of manifestFiles) {
      await readFile(path.join(outputDirectory, file));
    }
    assert.equal(
      await readFile(path.join(outputDirectory, "download-gesture.js"), "utf8"),
      await readFile(new URL("../download-gesture.js", import.meta.url), "utf8"),
    );
    await assert.rejects(readFile(path.join(outputDirectory, "tests", "media.test.mjs")));
  } finally {
    await rm(tempDirectory, { recursive: true, force: true });
  }
});
