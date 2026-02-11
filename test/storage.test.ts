import { SqliteClient } from "@effect/sql-sqlite-node"
import { Effect, Layer } from "effect"
import { describe, expect, it } from "vitest"
import {
	type InsertEvent,
	Storage,
	StorageLive,
} from "../src/services/Storage.js"

const TestSqliteLayer = SqliteClient.layer({ filename: ":memory:" })
const TestStorageLayer = StorageLive.pipe(Layer.provide(TestSqliteLayer))

const runTest = <A, E>(effect: Effect.Effect<A, E, Storage>) =>
	Effect.runPromise(effect.pipe(Effect.provide(TestStorageLayer)))

const sampleEvent = (overrides?: Partial<InsertEvent>): InsertEvent => ({
	contractName: "TestToken",
	eventName: "Transfer",
	blockNumber: 100n,
	txHash: "0xabc123",
	logIndex: 0,
	timestamp: 1700000000n,
	args: { from: "0xAAA", to: "0xBBB", value: "1000" },
	...overrides,
})

describe("Storage", () => {
	it("initializes schema without error", () =>
		runTest(
			Effect.gen(function* () {
				const storage = yield* Storage
				yield* storage.initialize
			}),
		))

	it("inserts and queries events", () =>
		runTest(
			Effect.gen(function* () {
				const storage = yield* Storage
				yield* storage.initialize

				yield* storage.insertEvents([
					sampleEvent(),
					sampleEvent({
						logIndex: 1,
						txHash: "0xabc124",
						eventName: "Approval",
					}),
				])

				const all = yield* storage.queryEvents({})
				expect(all).toHaveLength(2)

				const transfers = yield* storage.queryEvents({ eventName: "Transfer" })
				expect(transfers).toHaveLength(1)
				expect(transfers[0]!.event_name).toBe("Transfer")
			}),
		))

	it("handles idempotent inserts (INSERT OR IGNORE)", () =>
		runTest(
			Effect.gen(function* () {
				const storage = yield* Storage
				yield* storage.initialize

				const event = sampleEvent()
				yield* storage.insertEvents([event])
				yield* storage.insertEvents([event]) // duplicate

				const all = yield* storage.queryEvents({})
				expect(all).toHaveLength(1)
			}),
		))

	it("deletes events from a block number", () =>
		runTest(
			Effect.gen(function* () {
				const storage = yield* Storage
				yield* storage.initialize

				yield* storage.insertEvents([
					sampleEvent({ blockNumber: 10n, txHash: "0x1", logIndex: 0 }),
					sampleEvent({ blockNumber: 11n, txHash: "0x2", logIndex: 0 }),
					sampleEvent({ blockNumber: 12n, txHash: "0x3", logIndex: 0 }),
				])

				yield* storage.deleteEventsFrom(11n)
				const remaining = yield* storage.queryEvents({})
				expect(remaining).toHaveLength(1)
				expect(remaining[0]!.block_number).toBe(10)
			}),
		))

	it("counts events", () =>
		runTest(
			Effect.gen(function* () {
				const storage = yield* Storage
				yield* storage.initialize

				yield* storage.insertEvents([
					sampleEvent({ txHash: "0x1", logIndex: 0 }),
					sampleEvent({ txHash: "0x2", logIndex: 0 }),
				])

				const total = yield* storage.countEvents()
				expect(total).toBe(2)

				const filtered = yield* storage.countEvents({ eventName: "Transfer" })
				expect(filtered).toBe(2)
			}),
		))

	it("manages checkpoints", () =>
		runTest(
			Effect.gen(function* () {
				const storage = yield* Storage
				yield* storage.initialize

				const before = yield* storage.getCheckpoint("TestToken")
				expect(before).toBeNull()

				yield* storage.saveCheckpoint("TestToken", 100n, "0xhash100")

				const after = yield* storage.getCheckpoint("TestToken")
				expect(after).not.toBeNull()
				expect(after!.lastBlock).toBe(100n)
				expect(after!.lastBlockHash).toBe("0xhash100")

				// Update checkpoint
				yield* storage.saveCheckpoint("TestToken", 200n, "0xhash200")
				const updated = yield* storage.getCheckpoint("TestToken")
				expect(updated!.lastBlock).toBe(200n)
			}),
		))

	it("manages block hashes", () =>
		runTest(
			Effect.gen(function* () {
				const storage = yield* Storage
				yield* storage.initialize

				yield* storage.insertBlockHash(10n, "0xhash10")
				yield* storage.insertBlockHash(11n, "0xhash11")
				yield* storage.insertBlockHash(12n, "0xhash12")

				const hash = yield* storage.getBlockHash(11n)
				expect(hash).toBe("0xhash11")

				const recent = yield* storage.getRecentBlockHashes(2)
				expect(recent).toHaveLength(2)
				expect(recent[0]!.blockNumber).toBe(12n)

				yield* storage.deleteBlockHashesFrom(11n)
				const afterDelete = yield* storage.getRecentBlockHashes(10)
				expect(afterDelete).toHaveLength(1)
				expect(afterDelete[0]!.blockNumber).toBe(10n)
			}),
		))

	it("queries events with fromBlock/toBlock filters", () =>
		runTest(
			Effect.gen(function* () {
				const storage = yield* Storage
				yield* storage.initialize

				yield* storage.insertEvents([
					sampleEvent({ blockNumber: 10n, txHash: "0x1", logIndex: 0 }),
					sampleEvent({ blockNumber: 20n, txHash: "0x2", logIndex: 0 }),
					sampleEvent({ blockNumber: 30n, txHash: "0x3", logIndex: 0 }),
				])

				const range = yield* storage.queryEvents({
					fromBlock: 15n,
					toBlock: 25n,
				})
				expect(range).toHaveLength(1)
				expect(range[0]!.block_number).toBe(20)
			}),
		))
})
