import test from "node:test";
import assert from "node:assert/strict";
import {
  createBrowserHandoffRunner,
  openUserInitiatedBrowserDownloadDialog,
} from "../src/browserHandoff.mjs";

test("activates FluxNet before opening a user-initiated browser download dialog", async () => {
  const steps = [];
  const destination = await openUserInitiatedBrowserDownloadDialog(
    async () => { steps.push("activate"); },
    async () => {
      steps.push("dialog");
      return null;
    },
  );

  assert.deepEqual(steps, ["activate", "dialog"]);
  assert.equal(destination, null);
});

test("does not open a browser download dialog if window activation fails", async () => {
  let dialogOpened = false;
  await assert.rejects(openUserInitiatedBrowserDownloadDialog(
    async () => { throw new Error("focus failed"); },
    async () => {
      dialogOpened = true;
      return "C:\\Downloads\\video.mp4";
    },
  ), /focus failed/);
  assert.equal(dialogOpened, false);
});

test("a cancelled save dialog releases the URL for retry", async () => {
  const runHandoff = createBrowserHandoffRunner();
  let attempts = 0;

  const cancelled = await runHandoff("https://files.example/video.mp4", async () => {
    attempts += 1;
    return false;
  });
  const queued = await runHandoff("https://files.example/video.mp4", async () => {
    attempts += 1;
    return true;
  });

  assert.equal(cancelled, false);
  assert.equal(queued, true);
  assert.equal(attempts, 2);
});

test("a failed queue releases the URL for retry", async () => {
  const runHandoff = createBrowserHandoffRunner();
  let attempts = 0;

  const failedQueue = await runHandoff("https://files.example/video.mp4", async () => {
    attempts += 1;
    return false;
  });
  const queued = await runHandoff("https://files.example/video.mp4", async () => {
    attempts += 1;
    return true;
  });

  assert.equal(failedQueue, false);
  assert.equal(queued, true);
  assert.equal(attempts, 2);
});

test("an unexpected handoff error releases the URL for retry", async () => {
  const runHandoff = createBrowserHandoffRunner();
  let attempts = 0;

  await assert.rejects(runHandoff("https://files.example/video.mp4", async () => {
    attempts += 1;
    throw new Error("handoff failed");
  }), /handoff failed/);
  const queued = await runHandoff("https://files.example/video.mp4", async () => {
    attempts += 1;
    return true;
  });

  assert.equal(queued, true);
  assert.equal(attempts, 2);
});

test("a successful handoff is marked handled and does not start twice", async () => {
  const runHandoff = createBrowserHandoffRunner();
  let attempts = 0;

  const queued = await runHandoff("https://files.example/video.mp4", async () => {
    attempts += 1;
    return true;
  });
  const duplicate = await runHandoff("https://files.example/video.mp4", async () => {
    attempts += 1;
    return true;
  });

  assert.equal(queued, true);
  assert.equal(duplicate, undefined);
  assert.equal(attempts, 1);
});

test("duplicate simultaneous handoffs for one URL run only once", async () => {
  const runHandoff = createBrowserHandoffRunner();
  let finishHandoff;
  let attempts = 0;
  const first = runHandoff("https://files.example/video.mp4", () => {
    attempts += 1;
    return new Promise((resolve) => { finishHandoff = resolve; });
  });
  const duplicate = await runHandoff("https://files.example/video.mp4", async () => {
    attempts += 1;
    return "duplicate";
  });

  assert.equal(duplicate, undefined);
  assert.equal(attempts, 1);
  finishHandoff(true);
  assert.equal(await first, true);
});
