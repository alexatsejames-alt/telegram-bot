/**
 * Tests for event deduplication.
 *
 * Covered:
 *   - dedupKey: uses eventId, falls back to txHash:ledger
 *   - poller: first cycle sends all events, sets lastSeenEventId
 *   - poller: second cycle with same events skips them (all deduped)
 *   - poller: second cycle with partial overlap sends only new events
 *   - poller: lastSeenEventId missing in old cursor file → no skip
 *   - poller: lastSeenEventId not found on page → all events skipped (safe)
 */

import assert from "node:assert/strict";
import { writeFile } from "node:fs/promises";
import test from "node:test";

import { nativeToScVal } from "@stellar/stellar-sdk";
import { dedupKey } from "../dist/stellar/decode.js";
import { createPoller } from "../dist/poller.js";
import { createTempDataDir } from "./helpers/temp-data.mjs";

const dataDir = await createTempDataDir("mimir-dedup-");
test.after(() => dataDir.cleanup());

const MARKET_ID = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF";
const SQUAD_ID  = "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBF4";

function baseConfig(cursorFile) {
  return {
    botToken: "fake-token",
    chatId: "-1001234567890",
    marketContractId: MARKET_ID,
    squadContractId: SQUAD_ID,
    rpcUrl: "https://example.invalid/rpc",
    horizonUrl: "https://example.invalid/horizon",
    networkPassphrase: "Test SDF Network ; September 2015",
    explorerBaseUrl: "https://example.invalid/explorer",
    pollIntervalMs: 9_999_999,
    startLookbackLedgers: 60,
    cursorFile,
    maxNotificationsPerCycle: 20,
    healthHost: "127.0.0.1",
    healthPort: 0,
    healthStaleMs: 0,
  };
}

function makeCursor(ledger, tx = 1) {
  return `${(BigInt(ledger) << 32n) | BigInt(tx)}-0`;
}

/**
 * Build a raw RPC event that decodes to claim_cancelled (simplest — only needs
 * one topic after the name, no value fields).
 */
function makeRawEvent(id, claimId = 1, ledger = 100) {
  return {
    id,
    contractId: MARKET_ID,
    ledger,
    txHash: `tx-${id}`,
    ledgerClosedAt: "2026-01-01T00:00:00Z",
    topic: [
      nativeToScVal("claim_cancelled", { type: "string" }),
      nativeToScVal(BigInt(claimId), { type: "u64" }),
    ],
    value: nativeToScVal({}),
  };
}

const health = { status: "healthy", oldestLedger: 1, latestLedger: 200 };
const tipCursor = makeCursor(200);

function makeServer(rawEvents) {
  return {
    async getHealth() { return health; },
    async getEvents(req) {
      const id = req.filters?.[0]?.contractIds?.[0];
      if (id === MARKET_ID) {
        return { events: rawEvents, cursor: tipCursor, latestLedger: 200 };
      }
      return { events: [], cursor: tipCursor, latestLedger: 200 };
    },
  };
}

const fastOpts = { sendSpacingMs: 0, maxSendRetries: 1, initialBackoffMs: 0, maxBackoffMs: 0 };

async function runOneCycle(poller) {
  await poller.start();
  await new Promise((r) => setTimeout(r, 80));
  poller.stop();
}

// ── dedupKey unit tests ───────────────────────────────────────────────────────

test("dedupKey: uses eventId when present", () => {
  assert.equal(dedupKey({ eventId: "4900-0", txHash: "abc", ledger: 100 }), "4900-0");
});

test("dedupKey: falls back to txHash:ledger when eventId is empty", () => {
  assert.equal(dedupKey({ eventId: "", txHash: "cafebabe", ledger: 42 }), "cafebabe:42");
});

test("dedupKey: falls back when eventId is missing", () => {
  assert.equal(dedupKey({ txHash: "deadbeef", ledger: 7 }), "deadbeef:7");
});

test("dedupKey: uses ? for missing txHash in fallback", () => {
  assert.equal(dedupKey({ eventId: "", txHash: "", ledger: 1 }), "?:1");
});

// ── Poller dedup integration tests ───────────────────────────────────────────

test("lastSeenEventId saved after successful send", async () => {
  const cursorFile = dataDir.file("dedup-basic.json");
  const events = [makeRawEvent("5000-0", 1), makeRawEvent("5000-1", 2)];
  const sent = [];

  const poller = createPoller({
    config: baseConfig(cursorFile),
    server: makeServer(events),
    send: async (text) => { sent.push(text); },
    sendOptions: fastOpts,
  });

  await runOneCycle(poller);

  const t = poller.status().targets.find((t) => t.source === "market");
  assert.equal(t.lastSeenEventId, "5000-1", "lastSeenEventId set to last sent event");
  assert.equal(sent.length, 2, "both events sent on first cycle");
});

test("second cycle with identical events sends nothing", async () => {
  const cursorFile = dataDir.file("dedup-second.json");
  const events = [makeRawEvent("6000-0", 1), makeRawEvent("6000-1", 2)];
  const sent = [];

  await writeFile(cursorFile, JSON.stringify({
    version: 1, updatedAt: new Date().toISOString(),
    targets: {
      market: { cursor: makeCursor(109), lastEventLedger: 110, lastSeenEventId: "6000-1" },
      squad:  { cursor: makeCursor(109), lastEventLedger: null, lastSeenEventId: null },
    },
  }), "utf8");

  const poller = createPoller({
    config: baseConfig(cursorFile),
    server: makeServer(events),
    send: async (text) => { sent.push(text); },
    sendOptions: fastOpts,
  });

  await runOneCycle(poller);

  assert.equal(sent.length, 0, "no sends when all events already seen");
});

test("partial overlap: only events after lastSeenEventId are sent", async () => {
  const cursorFile = dataDir.file("dedup-partial.json");
  const events = [makeRawEvent("7000-0", 1), makeRawEvent("7000-1", 2), makeRawEvent("7000-2", 3)];
  const sent = [];

  await writeFile(cursorFile, JSON.stringify({
    version: 1, updatedAt: new Date().toISOString(),
    targets: {
      market: { cursor: makeCursor(119), lastEventLedger: 120, lastSeenEventId: "7000-0" },
      squad:  { cursor: makeCursor(119), lastEventLedger: null, lastSeenEventId: null },
    },
  }), "utf8");

  const poller = createPoller({
    config: baseConfig(cursorFile),
    server: makeServer(events),
    send: async (text) => { sent.push(text); },
    sendOptions: fastOpts,
  });

  await runOneCycle(poller);

  assert.equal(sent.length, 2, "only events after lastSeenEventId sent");
  assert.equal(
    poller.status().targets.find((t) => t.source === "market").lastSeenEventId,
    "7000-2",
  );
});

test("old cursor file without lastSeenEventId: all events sent", async () => {
  const cursorFile = dataDir.file("dedup-old.json");
  await writeFile(cursorFile, JSON.stringify({
    version: 1, updatedAt: new Date().toISOString(),
    targets: {
      market: { cursor: makeCursor(130), lastEventLedger: 130 },
      squad:  { cursor: makeCursor(130), lastEventLedger: null },
    },
  }), "utf8");

  const events = [makeRawEvent("8000-0", 1), makeRawEvent("8000-1", 2)];
  const sent = [];

  const poller = createPoller({
    config: baseConfig(cursorFile),
    server: makeServer(events),
    send: async (text) => { sent.push(text); },
    sendOptions: fastOpts,
  });

  await runOneCycle(poller);

  assert.equal(sent.length, 2, "all events sent when lastSeenEventId absent in old cursor");
});

test("lastSeenEventId not found on page: all events skipped (safe fallback)", async () => {
  const cursorFile = dataDir.file("dedup-notfound.json");
  await writeFile(cursorFile, JSON.stringify({
    version: 1, updatedAt: new Date().toISOString(),
    targets: {
      market: { cursor: makeCursor(140), lastEventLedger: 140, lastSeenEventId: "9999-0" },
      squad:  { cursor: makeCursor(140), lastEventLedger: null, lastSeenEventId: null },
    },
  }), "utf8");

  const events = [makeRawEvent("1000-0", 1), makeRawEvent("1000-1", 2)];
  const sent = [];

  const poller = createPoller({
    config: baseConfig(cursorFile),
    server: makeServer(events),
    send: async (text) => { sent.push(text); },
    sendOptions: fastOpts,
  });

  await runOneCycle(poller);

  // Anchor not found → conservative: all skipped rather than risk double-send
  assert.equal(sent.length, 0, "events skipped when anchor not found on page");
  assert.ok(poller.status().eventsSkipped >= 2);
});
