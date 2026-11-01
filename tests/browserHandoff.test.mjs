import test from "node:test";
import assert from "node:assert/strict";
import { createBrowserHandoffRunner } from "../src/browserHandoff.mjs";

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
