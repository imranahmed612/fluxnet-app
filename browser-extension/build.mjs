import { cp, mkdir, readFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const sourceDirectory = path.dirname(fileURLToPath(import.meta.url));
const repositoryRoot = path.resolve(sourceDirectory, "..");
const defaultOutputDirectory = path.join(repositoryRoot, "artifacts", "chrome-extension");

function isWithin(parent, candidate) {
  const relative = path.relative(parent, candidate);
  return relative !== "" && relative !== ".." && !relative.startsWith(`..${path.sep}`)
    && !path.isAbsolute(relative);
}

function manifestFiles(manifest) {
  return [
    ...Object.values(manifest.icons || {}),
    ...(manifest.content_scripts || []).flatMap(({ js = [], css = [] }) => [...js, ...css]),
    manifest.background?.service_worker,
    manifest.action?.default_popup,
    ...Object.values(manifest.action?.default_icon || {}),
    manifest.options_page,
    manifest.options_ui?.page,
  ].filter(Boolean);
}

export async function buildExtension(outputDirectory = defaultOutputDirectory) {
  const output = path.resolve(outputDirectory);
  const isDefaultOutput = output === defaultOutputDirectory;
  const isTemporaryOutput = isWithin(path.resolve(os.tmpdir()), output);
  if ((!isDefaultOutput && !isTemporaryOutput)
    || output === repositoryRoot
    || isWithin(output, sourceDirectory)
    || output === sourceDirectory) {
    throw new Error("The extension build output must be the configured artifact folder or a temporary test folder.");
  }

  await rm(output, { recursive: true, force: true });
  await mkdir(path.dirname(output), { recursive: true });
  await cp(sourceDirectory, output, {
    recursive: true,
    filter(source) {
      const relative = path.relative(sourceDirectory, source);
      if (!relative) return true;
      const [first, ...rest] = relative.split(path.sep);
      return first !== "tests" && !(first === "build.mjs" && rest.length === 0);
    },
  });

  const manifest = JSON.parse(await readFile(path.join(output, "manifest.json"), "utf8"));
  for (const file of manifestFiles(manifest)) {
    await readFile(path.join(output, file));
  }
  return output;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const output = process.argv[2] ? path.resolve(process.argv[2]) : defaultOutputDirectory;
  await buildExtension(output);
  console.log(`Built unpacked extension at ${output}`);
}
