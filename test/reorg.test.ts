import { SqliteClient } from "@effect/sql-sqlite-node"
import { Effect, Layer } from "effect"
import { describe, expect, it } from "vitest"
import { ConfigLive } from "../src/config.js"
import {
	ReorgDetector,
	ReorgDetectorLive,
} from "../src/pipeline/ReorgDetector.js"
import {
	type InsertEvent,
	Storage,
	StorageLive,
} from "../src/services/Storage.js"
import { ERC20_ABI } from "./fixtures/abi.js"
import { makeBlock, makeBlockHash } from "./fixtures/blocks.js"

const TestConfig = ConfigLive({
	rpcUrl: "http://localhost",
	contracts: [
		{ name: "Test", address: "0x1", abi: ERC20_ABI, events: ["Transfer"] },
	],
	reorgDepth: 10,
})

const TestSqliteLayer = SqliteClient.layer({ filename: ":memory:" })
const TestStorageLayer = StorageLive.pipe(Layer.provide(TestSqliteLayer))
const TestReorgLayer = ReorgDetectorLive.pipe(
	Layer.provide(Layer.merge(TestStorageLayer, TestConfig)),
)

const TestLayer = Layer.mergeAll(TestStorageLayer, TestReorgLayer, TestConfig)

const runTest = <A, E>(effect: Effect.Effect<A, E, Storage | ReorgDetector>) =>
	Effect.runPromise(effect.pipe(Effect.provide(TestLayer)))

const sampleEvent = (blockNumber: bigint, txHash: string): InsertEvent => ({
	contractName: "Test",
	eventName: "Transfer",
	blockNumber,
	txHash,
	logIndex: 0,
	timestamp: null,
	args: {},
})

describe("ReorgDetector", () => {
	it("accepts blocks with correct parentHash chain", () =>
		runTest(
			Effect.gen(function* () {
				const storage = yield* Storage
				const detector = yield* ReorgDetector
				yield* storage.initialize

				const block1 = makeBlock(1n, 0n)
				const block2 = makeBlock(2n)
				const block3 = makeBlock(3n)

				yield* detector.verifyBlock(block1)
				yield* detector.verifyBlock(block2)
				yield* detector.verifyBlock(block3)

				// No error thrown — chain is valid
				const hash = yield* storage.getBlockHash(3n)
				expect(hash).toBe(makeBlockHash(3))
			}),
		))

	it("detects reorg when parentHash doesn't match", () =>
		runTest(
			Effect.gen(function* () {
				const storage = yield* Storage
				const detector = yield* ReorgDetector
				yield* storage.initialize

				// Build a normal chain: blocks 1-5
				for (let i = 1; i <= 5; i++) {
					yield* detector.verifyBlock(makeBlock(BigInt(i), BigInt(i - 1)))
				}

				// Insert events for blocks 3-5
				yield* storage.insertEvents([
					sampleEvent(3n, "0xtx3"),
					sampleEvent(4n, "0xtx4"),
					sampleEvent(5n, "0xtx5"),
				])

				// Now a forked block 4 arrives with wrong parentHash
				const forkedBlock4 = {
					number: 4n,
					hash: "0xforkedhash4",
					parentHash: "0xwrongparent",
					timestamp: 1700000120n,
				}

				const result = yield* Effect.either(detector.verifyBlock(forkedBlock4))
				expect(result._tag).toBe("Left")
				if (result._tag === "Left") {
					expect(result.left._tag).toBe("ReorgDetected")
					if (result.left._tag === "ReorgDetected") {
						expect(result.left.forkBlock).toBe(3n)
					}
				}

				// Handle the reorg
				yield* detector.handleReorg(3n)

				// Events from block 3 onward should be deleted
				const remaining = yield* storage.queryEvents({})
				expect(remaining).toHaveLength(0) // all events were at block 3+
			}),
		))

	it("handleReorg deletes block hashes from fork point", () =>
		runTest(
			Effect.gen(function* () {
				const storage = yield* Storage
				const detector = yield* ReorgDetector
				yield* storage.initialize

				// Build chain 1-5
				for (let i = 1; i <= 5; i++) {
					yield* detector.verifyBlock(makeBlock(BigInt(i), BigInt(i - 1)))
				}

				yield* detector.handleReorg(3n)

				// Only hashes for blocks 1-2 should remain
				const hashes = yield* storage.getRecentBlockHashes(10)
				expect(hashes).toHaveLength(2)
				expect(hashes.map(h => h.blockNumber).sort()).toEqual([1n, 2n])
			}),
		))
})
