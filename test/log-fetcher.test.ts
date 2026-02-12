import { Chunk, Effect, Layer, Stream } from "effect"
import { describe, expect, it } from "vitest"
import { ConfigLive } from "../src/config.js"
import { RpcError } from "../src/errors.js"
import {
	buildTopicFilter,
	buildTopicFilterEffect,
	fetchLogs,
} from "../src/pipeline/LogFetcher.js"
import type { RawLog } from "../src/services/RpcProvider.js"
import { RpcProvider } from "../src/services/RpcProvider.js"
import { ERC20_ABI } from "./fixtures/abi.js"

describe("buildTopicFilter", () => {
	it("builds topic filter for Transfer event", () => {
		const topics = buildTopicFilter(ERC20_ABI, ["Transfer"])
		expect(topics).toHaveLength(1)
		expect(topics[0]).toMatch(/^0x[0-9a-f]{64}$/)
	})

	it("builds topic filter for multiple events", () => {
		const topics = buildTopicFilter(ERC20_ABI, ["Transfer", "Approval"])
		expect(topics).toHaveLength(2)
	})

	it("throws for unknown event", () => {
		expect(() => buildTopicFilter(ERC20_ABI, ["NonExistent"])).toThrow()
	})
})

describe("buildTopicFilterEffect", () => {
	it("returns topics for valid event names", async () => {
		const topics = await Effect.runPromise(
			buildTopicFilterEffect(ERC20_ABI, ["Transfer"]),
		)
		expect(topics).toHaveLength(1)
		expect(topics[0]).toMatch(/^0x[0-9a-f]{64}$/)
	})

	it("fails with ConfigError for unknown event", async () => {
		const exit = await Effect.runPromise(
			buildTopicFilterEffect(ERC20_ABI, ["NonExistent"]).pipe(Effect.either),
		)
		expect(exit._tag).toBe("Left")
		if (exit._tag === "Left") {
			expect(exit.left._tag).toBe("ConfigError")
			expect(exit.left.field).toBe("contracts.events")
		}
	})
})

describe("fetchLogs", () => {
	it("chunks block ranges correctly", async () => {
		const fetchedRanges: Array<{ from: bigint; to: bigint }> = []

		const MockRpcProvider = Layer.succeed(RpcProvider, {
			getBlockNumber: Effect.succeed(100n),
			getLogs: params => {
				fetchedRanges.push({ from: params.fromBlock, to: params.toBlock })
				return Effect.succeed([] as RawLog[])
			},
			getBlock: () =>
				Effect.fail(
					new RpcError({ reason: "not implemented", method: "getBlock" }),
				),
		})

		const TestConfig = ConfigLive({
			rpcUrl: "http://localhost",
			contracts: [
				{ name: "Test", address: "0x1", abi: ERC20_ABI, events: ["Transfer"] },
			],
			network: {
				logs: {
					chunkSize: 100,
				},
			},
		})

		const TestLayer = Layer.merge(MockRpcProvider, TestConfig)

		const result = await Effect.runPromise(
			fetchLogs({
				address: "0x1",
				topics: ["0xabc"],
				fromBlock: 0n,
				toBlock: 250n,
			}).pipe(
				Stream.runCollect,
				Effect.map(Chunk.toReadonlyArray),
				Effect.provide(TestLayer),
			),
		)

		expect(result).toHaveLength(3) // 0-99, 100-199, 200-250
		expect(fetchedRanges[0]).toEqual({ from: 0n, to: 99n })
		expect(fetchedRanges[1]).toEqual({ from: 100n, to: 199n })
		expect(fetchedRanges[2]).toEqual({ from: 200n, to: 250n })
	})

	it("respects parallelRequests concurrency", async () => {
		const maxConcurrent = { value: 0 }
		const currentConcurrent = { value: 0 }

		const MockRpcProvider = Layer.succeed(RpcProvider, {
			getBlockNumber: Effect.succeed(100n),
			getLogs: () =>
				Effect.gen(function* () {
					currentConcurrent.value++
					if (currentConcurrent.value > maxConcurrent.value) {
						maxConcurrent.value = currentConcurrent.value
					}
					yield* Effect.sleep(50)
					currentConcurrent.value--
					return [] as RawLog[]
				}),
			getBlock: () =>
				Effect.fail(
					new RpcError({ reason: "not implemented", method: "getBlock" }),
				),
		})

		const TestConfig = ConfigLive({
			rpcUrl: "http://localhost",
			contracts: [
				{ name: "Test", address: "0x1", abi: ERC20_ABI, events: ["Transfer"] },
			],
			network: {
				logs: {
					chunkSize: 100,
					parallelRequests: 3,
				},
			},
		})

		const TestLayer = Layer.merge(MockRpcProvider, TestConfig)

		await Effect.runPromise(
			fetchLogs({
				address: "0x1",
				topics: ["0xabc"],
				fromBlock: 0n,
				toBlock: 999n, // 10 chunks of 100
			}).pipe(
				Stream.runCollect,
				Effect.map(Chunk.toReadonlyArray),
				Effect.provide(TestLayer),
			),
		)

		expect(maxConcurrent.value).toBeGreaterThan(1)
		expect(maxConcurrent.value).toBeLessThanOrEqual(3)
	})

	it("preserves chunk order with parallel requests", async () => {
		const MockRpcProvider = Layer.succeed(RpcProvider, {
			getBlockNumber: Effect.succeed(100n),
			getLogs: params =>
				Effect.gen(function* () {
					// Variable delay to test ordering
					const delay = Number(params.fromBlock % 3n) * 20 + 10
					yield* Effect.sleep(delay)
					return [
						{
							address: "0x1",
							topics: ["0xabc"],
							data: "0x",
							blockNumber: params.fromBlock,
							transactionHash: `0x${params.fromBlock.toString(16)}`,
							logIndex: 0,
							blockHash: `0xhash${params.fromBlock.toString(16)}`,
						},
					] as RawLog[]
				}),
			getBlock: () =>
				Effect.fail(
					new RpcError({ reason: "not implemented", method: "getBlock" }),
				),
		})

		const TestConfig = ConfigLive({
			rpcUrl: "http://localhost",
			contracts: [
				{ name: "Test", address: "0x1", abi: ERC20_ABI, events: ["Transfer"] },
			],
			network: {
				logs: {
					chunkSize: 100,
					parallelRequests: 3,
				},
			},
		})

		const TestLayer = Layer.merge(MockRpcProvider, TestConfig)

		const result = await Effect.runPromise(
			fetchLogs({
				address: "0x1",
				topics: ["0xabc"],
				fromBlock: 0n,
				toBlock: 499n, // 5 chunks
			}).pipe(
				Stream.runCollect,
				Effect.map(Chunk.toReadonlyArray),
				Effect.provide(TestLayer),
			),
		)

		expect(result).toHaveLength(5)
		const outputOrder = result.map(chunk => chunk[0]?.blockNumber ?? -1n)
		expect(outputOrder).toEqual([0n, 100n, 200n, 300n, 400n])
	})

	it("works sequentially with default parallelRequests=1", async () => {
		const maxConcurrent = { value: 0 }
		const currentConcurrent = { value: 0 }

		const MockRpcProvider = Layer.succeed(RpcProvider, {
			getBlockNumber: Effect.succeed(100n),
			getLogs: () =>
				Effect.gen(function* () {
					currentConcurrent.value++
					if (currentConcurrent.value > maxConcurrent.value) {
						maxConcurrent.value = currentConcurrent.value
					}
					yield* Effect.sleep(10)
					currentConcurrent.value--
					return [] as RawLog[]
				}),
			getBlock: () =>
				Effect.fail(
					new RpcError({ reason: "not implemented", method: "getBlock" }),
				),
		})

		const TestConfig = ConfigLive({
			rpcUrl: "http://localhost",
			contracts: [
				{ name: "Test", address: "0x1", abi: ERC20_ABI, events: ["Transfer"] },
			],
			network: {
				logs: {
					chunkSize: 100,
				},
			},
		})

		const TestLayer = Layer.merge(MockRpcProvider, TestConfig)

		await Effect.runPromise(
			fetchLogs({
				address: "0x1",
				topics: ["0xabc"],
				fromBlock: 0n,
				toBlock: 499n, // 5 chunks
			}).pipe(
				Stream.runCollect,
				Effect.map(Chunk.toReadonlyArray),
				Effect.provide(TestLayer),
			),
		)

		expect(maxConcurrent.value).toBe(1)
	})

	it("returns empty stream when fromBlock > toBlock", async () => {
		const MockRpcProvider = Layer.succeed(RpcProvider, {
			getBlockNumber: Effect.succeed(100n),
			getLogs: () => Effect.succeed([] as RawLog[]),
			getBlock: () =>
				Effect.fail(
					new RpcError({ reason: "not implemented", method: "getBlock" }),
				),
		})

		const TestConfig = ConfigLive({
			rpcUrl: "http://localhost",
			contracts: [
				{ name: "Test", address: "0x1", abi: ERC20_ABI, events: ["Transfer"] },
			],
		})

		const TestLayer = Layer.merge(MockRpcProvider, TestConfig)

		const result = await Effect.runPromise(
			fetchLogs({
				address: "0x1",
				topics: ["0xabc"],
				fromBlock: 200n,
				toBlock: 100n,
			}).pipe(
				Stream.runCollect,
				Effect.map(Chunk.toReadonlyArray),
				Effect.provide(TestLayer),
			),
		)

		expect(result).toHaveLength(0)
	})
})
