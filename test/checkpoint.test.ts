import { SqliteClient } from "@effect/sql-sqlite-node";
import { Effect, Layer } from "effect";
import { describe, expect, it } from "vitest";
import {
	CheckpointManager,
	CheckpointManagerLive,
} from "../src/services/Checkpoint.js";
import { Storage, StorageLive } from "../src/services/Storage.js";

const TestSqliteLayer = SqliteClient.layer({ filename: ":memory:" });
const TestStorageLayer = StorageLive.pipe(Layer.provide(TestSqliteLayer));
const TestCheckpointLayer = CheckpointManagerLive.pipe(
	Layer.provide(TestStorageLayer),
);
const TestLayer = Layer.mergeAll(TestStorageLayer, TestCheckpointLayer);

const runTest = <A, E>(
	effect: Effect.Effect<A, E, Storage | CheckpointManager>,
) => Effect.runPromise(effect.pipe(Effect.provide(TestLayer)));

describe("CheckpointManager", () => {
	it("returns configStartBlock when no checkpoint exists", () =>
		runTest(
			Effect.gen(function* () {
				const storage = yield* Storage;
				yield* storage.initialize;

				const cp = yield* CheckpointManager;
				const startBlock = yield* cp.getStartBlock("MyContract", 1000n);
				expect(startBlock).toBe(1000n);
			}),
		));

	it("returns lastBlock + 1 when checkpoint exists", () =>
		runTest(
			Effect.gen(function* () {
				const storage = yield* Storage;
				yield* storage.initialize;

				const cp = yield* CheckpointManager;
				yield* cp.save("MyContract", 500n, "0xhash500");

				const startBlock = yield* cp.getStartBlock("MyContract", 1000n);
				expect(startBlock).toBe(501n);
			}),
		));

	it("loads saved checkpoint", () =>
		runTest(
			Effect.gen(function* () {
				const storage = yield* Storage;
				yield* storage.initialize;

				const cp = yield* CheckpointManager;
				yield* cp.save("MyContract", 250n, "0xhash250");

				const loaded = yield* cp.load("MyContract");
				expect(loaded).not.toBeNull();
				expect(loaded!.lastBlock).toBe(250n);
				expect(loaded!.lastBlockHash).toBe("0xhash250");
			}),
		));

	it("returns null for non-existent contract", () =>
		runTest(
			Effect.gen(function* () {
				const storage = yield* Storage;
				yield* storage.initialize;

				const cp = yield* CheckpointManager;
				const loaded = yield* cp.load("NonExistent");
				expect(loaded).toBeNull();
			}),
		));
});
