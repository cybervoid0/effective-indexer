import { SqliteClient } from "@effect/sql-sqlite-node"
import { Effect, Layer } from "effect"
import { describe, expect, it } from "vitest"
import type { StorageError } from "../src/errors.js"
import { QueryApi, QueryApiLive } from "../src/query.js"
import {
	type InsertEvent,
	Storage,
	StorageLive,
	type StoredEvent,
} from "../src/services/Storage.js"

const TestSqliteLayer = SqliteClient.layer({ filename: ":memory:" })
const TestStorageLayer = StorageLive.pipe(Layer.provide(TestSqliteLayer))
const TestQueryLayer = QueryApiLive.pipe(Layer.provide(TestStorageLayer))
const TestLayer = Layer.mergeAll(TestStorageLayer, TestQueryLayer)

const runTest = <A, E>(effect: Effect.Effect<A, E, Storage | QueryApi>) =>
	Effect.runPromise(effect.pipe(Effect.provide(TestLayer)))

const sampleEvent = (overrides?: Partial<InsertEvent>): InsertEvent => ({
	contractName: "TestToken",
	eventName: "Transfer",
	blockNumber: 100n,
	txHash: "0xabc",
	logIndex: 0,
	timestamp: 1700000000n,
	args: { from: "0xAAA", to: "0xBBB", value: "1000" },
	...overrides,
})

describe("QueryApi", () => {
	it("returns parsed events with camelCase fields", () =>
		runTest(
			Effect.gen(function* () {
				const storage = yield* Storage
				yield* storage.initialize

				yield* storage.insertEvents([
					sampleEvent(),
					sampleEvent({ txHash: "0xdef", logIndex: 1 }),
				])

				const query = yield* QueryApi
				const events = yield* query.getEvents()
				expect(events).toHaveLength(2)
				expect(events[0]!.contractName).toBe("TestToken")
				expect(events[0]!.eventName).toBe("Transfer")
				expect(events[0]!.blockNumber).toBe(100n)
				expect(events[0]!.args).toEqual({
					from: "0xAAA",
					to: "0xBBB",
					value: "1000",
				})
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
					sampleEvent({ txHash: "0x3", logIndex: 0 }),
				])

				const query = yield* QueryApi
				const count = yield* query.getEventCount()
				expect(count).toBe(3)
			}),
		))

	it("gets latest block for a contract", () =>
		runTest(
			Effect.gen(function* () {
				const storage = yield* Storage
				yield* storage.initialize

				const query = yield* QueryApi

				const before = yield* query.getLatestBlock("TestToken")
				expect(before).toBeNull()

				yield* storage.saveCheckpoint("TestToken", 500n, "0xhash")
				const after = yield* query.getLatestBlock("TestToken")
				expect(after).toBe(500n)
			}),
		))

	it("filters by contract and event name", () =>
		runTest(
			Effect.gen(function* () {
				const storage = yield* Storage
				yield* storage.initialize

				yield* storage.insertEvents([
					sampleEvent({
						contractName: "TokenA",
						eventName: "Transfer",
						txHash: "0x1",
					}),
					sampleEvent({
						contractName: "TokenA",
						eventName: "Approval",
						txHash: "0x2",
					}),
					sampleEvent({
						contractName: "TokenB",
						eventName: "Transfer",
						txHash: "0x3",
					}),
				])

				const query = yield* QueryApi
				const tokenATransfers = yield* query.getEvents({
					contractName: "TokenA",
					eventName: "Transfer",
				})
				expect(tokenATransfers).toHaveLength(1)
				expect(tokenATransfers[0]!.contractName).toBe("TokenA")
			}),
		))

	it("fails with StorageError for invalid JSON payload", () =>
		runTest(
			Effect.gen(function* () {
				const badRow: StoredEvent = {
					id: 1,
					contract_name: "TestToken",
					event_name: "Transfer",
					block_number: 100,
					tx_hash: "0xbad",
					log_index: 0,
					timestamp: null,
					args: "{broken-json",
				}

				const FakeStorage = Layer.succeed(Storage, {
					initialize: Effect.void,
					insertEvents: () => Effect.void,
					deleteEventsFrom: () => Effect.void,
					queryEvents: () => Effect.succeed([badRow] as const),
					countEvents: () => Effect.succeed(1),
					insertBlockHash: () => Effect.void,
					getBlockHash: () => Effect.succeed(null),
					getRecentBlockHashes: () => Effect.succeed([]),
					deleteBlockHashesFrom: () => Effect.void,
					getCheckpoint: () => Effect.succeed(null),
					saveCheckpoint: () => Effect.void,
				})

				const program = Effect.gen(function* () {
					const query = yield* QueryApi
					return yield* query.getEvents()
				}).pipe(
					Effect.provide(QueryApiLive.pipe(Layer.provide(FakeStorage))),
					Effect.either,
				)

				const exit = yield* program
				expect(exit._tag).toBe("Left")
				if (exit._tag === "Left") {
					expect(exit.left._tag).toBe("StorageError")
					const error = exit.left as StorageError
					expect(error.operation).toBe("parseStoredEvent")
				}
			}),
		))
})
