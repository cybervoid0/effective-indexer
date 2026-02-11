import { SqliteClient } from "@effect/sql-sqlite-node"
import { Effect, Layer } from "effect"
import { describe, expect, it } from "vitest"
import { QueryApi, QueryApiLive } from "../src/query.js"
import {
	type InsertEvent,
	Storage,
	StorageLive,
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
})
