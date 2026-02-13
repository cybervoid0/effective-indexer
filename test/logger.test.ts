import { SqliteClient } from "@effect/sql-sqlite-node"
import { Chunk, Effect, Layer, Logger, LogLevel, Stream } from "effect"
import { describe, expect, it } from "vitest"
import { ConfigLive, type IndexerConfig, resolveConfig } from "../src/config.js"
import { RpcError } from "../src/errors.js"
import { LoggerLive } from "../src/logger.js"
import { fetchLogs } from "../src/pipeline/LogFetcher.js"
import {
	ReorgDetector,
	ReorgDetectorLive,
} from "../src/pipeline/ReorgDetector.js"
import { QueryApi, QueryApiLive } from "../src/query.js"
import type { RawLog } from "../src/services/RpcProvider.js"
import { RpcProvider } from "../src/services/RpcProvider.js"
import { Storage, StorageLive } from "../src/services/Storage.js"
import { ERC20_ABI } from "./fixtures/abi.js"
import { makeBlock, makeForkedBlock } from "./fixtures/blocks.js"

const minimalConfig: IndexerConfig = {
	rpcUrl: "http://localhost",
	contracts: [
		{ name: "Test", address: "0x1", abi: ERC20_ABI, events: ["Transfer"] },
	],
}

interface CapturedLog {
	readonly message: string
	readonly level: string
	readonly annotations: Record<string, unknown>
}

const makeCaptureLogger = () => {
	const logs: CapturedLog[] = []

	const logger = Logger.make(options => {
		const annotations: Record<string, unknown> = {}
		for (const [key, value] of options.annotations) {
			annotations[key] = value
		}
		logs.push({
			message: String(options.message),
			level: options.logLevel._tag,
			annotations,
		})
	})

	return { logs, logger }
}

describe("Logger", () => {
	describe("config defaults", () => {
		it("resolves defaults for logging fields", () => {
			const resolved = resolveConfig(minimalConfig)
			expect(resolved.logLevel).toBe("info")
			expect(resolved.logFormat).toBe("pretty")
			expect(resolved.enableTelemetry).toBe(true)
		})

		it("resolves custom logging config", () => {
			const resolved = resolveConfig({
				...minimalConfig,
				logLevel: "debug",
				logFormat: "json",
				enableTelemetry: false,
			})
			expect(resolved.logLevel).toBe("debug")
			expect(resolved.logFormat).toBe("json")
			expect(resolved.enableTelemetry).toBe(false)
		})

		it("resolves network config defaults", () => {
			const resolved = resolveConfig(minimalConfig)
			expect(resolved.network.polling.intervalMs).toBe(12000)
			expect(resolved.network.polling.confirmations).toBe(1)
			expect(resolved.network.logs.chunkSize).toBe(5000)
			expect(resolved.network.logs.maxRetries).toBe(5)
			expect(resolved.network.logs.retry.baseDelayMs).toBe(1000)
			expect(resolved.network.logs.retry.maxDelayMs).toBe(30000)
			expect(resolved.network.reorg.depth).toBe(20)
		})

		it("resolves custom network config", () => {
			const resolved = resolveConfig({
				...minimalConfig,
				network: {
					polling: { intervalMs: 2000, confirmations: 32 },
					logs: {
						chunkSize: 2000,
						maxRetries: 10,
						retry: { baseDelayMs: 500, maxDelayMs: 60000 },
					},
					reorg: { depth: 128 },
				},
			})
			expect(resolved.network.polling.intervalMs).toBe(2000)
			expect(resolved.network.polling.confirmations).toBe(32)
			expect(resolved.network.logs.chunkSize).toBe(2000)
			expect(resolved.network.logs.maxRetries).toBe(10)
			expect(resolved.network.logs.retry.baseDelayMs).toBe(500)
			expect(resolved.network.logs.retry.maxDelayMs).toBe(60000)
			expect(resolved.network.reorg.depth).toBe(128)
		})
	})

	describe("LoggerLive level filtering", () => {
		it("filters logs below configured level", async () => {
			const { logs, logger } = makeCaptureLogger()
			const ConfLayer = ConfigLive({
				...minimalConfig,
				logLevel: "warning",
			})

			const TestLayer = Layer.mergeAll(
				LoggerLive.pipe(Layer.provide(ConfLayer)),
				Logger.replace(Logger.defaultLogger, logger),
			)

			const program = Effect.gen(function* () {
				yield* Effect.logDebug("debug msg")
				yield* Effect.log("info msg")
				yield* Effect.logWarning("warning msg")
				yield* Effect.logError("error msg")
			})

			await Effect.runPromise(program.pipe(Effect.provide(TestLayer)))

			expect(logs).toHaveLength(2)
			expect(logs[0]!.message).toBe("warning msg")
			expect(logs[1]!.message).toBe("error msg")
		})
	})

	describe("enableTelemetry=false suppression", () => {
		it("only allows error-level logs when telemetry is disabled", async () => {
			const { logs, logger } = makeCaptureLogger()
			const ConfLayer = ConfigLive({
				...minimalConfig,
				enableTelemetry: false,
			})

			const TestLayer = Layer.mergeAll(
				LoggerLive.pipe(Layer.provide(ConfLayer)),
				Logger.replace(Logger.defaultLogger, logger),
			)

			const program = Effect.gen(function* () {
				yield* Effect.logDebug("debug msg")
				yield* Effect.log("info msg")
				yield* Effect.logWarning("warning msg")
				yield* Effect.logError("error msg")
			})

			await Effect.runPromise(program.pipe(Effect.provide(TestLayer)))

			expect(logs).toHaveLength(1)
			expect(logs[0]!.message).toBe("error msg")
		})
	})

	describe("annotations", () => {
		it("captures annotations from annotateLogs", async () => {
			const { logs, logger } = makeCaptureLogger()
			const CaptureLayer = Layer.merge(
				Logger.replace(Logger.defaultLogger, logger),
				Logger.minimumLogLevel(LogLevel.All),
			)

			const program = Effect.log("test msg").pipe(
				Effect.annotateLogs({ contract: "Token", block: "123" }),
			)

			await Effect.runPromise(program.pipe(Effect.provide(CaptureLayer)))

			expect(logs).toHaveLength(1)
			expect(logs[0]!.annotations).toMatchObject({
				contract: "Token",
				block: "123",
			})
		})
	})

	describe("reorg logging", () => {
		it("emits warning on reorg detection", async () => {
			const { logs, logger } = makeCaptureLogger()
			const CaptureLayer = Logger.replace(Logger.defaultLogger, logger)

			const TestConfig = ConfigLive({
				...minimalConfig,
				network: { reorg: { depth: 10 } },
			})
			const TestSqliteLayer = SqliteClient.layer({ filename: ":memory:" })
			const TestStorageLayer = StorageLive.pipe(Layer.provide(TestSqliteLayer))
			const TestReorgLayer = ReorgDetectorLive.pipe(
				Layer.provide(Layer.merge(TestStorageLayer, TestConfig)),
			)
			const TestLayer = Layer.mergeAll(
				TestStorageLayer,
				TestReorgLayer,
				TestConfig,
				CaptureLayer,
				Logger.minimumLogLevel(LogLevel.All),
			)

			await Effect.runPromise(
				Effect.gen(function* () {
					const storage = yield* Storage
					const detector = yield* ReorgDetector
					yield* storage.initialize

					// Build a normal chain: blocks 1-3
					for (let i = 1; i <= 3; i++) {
						yield* detector.verifyBlock(makeBlock(BigInt(i), BigInt(i - 1)))
					}

					// A forked block 3 arrives with wrong parentHash
					const forkedBlock = makeForkedBlock(3n)
					yield* Effect.either(detector.verifyBlock(forkedBlock))
				}).pipe(Effect.provide(TestLayer)),
			)

			const reorgWarnings = logs.filter(
				l => l.message === "Parent hash mismatch" && l.level === "Warning",
			)
			expect(reorgWarnings).toHaveLength(1)
			expect(reorgWarnings[0]!.annotations).toHaveProperty("block")
			expect(reorgWarnings[0]!.annotations).toHaveProperty("expected")
			expect(reorgWarnings[0]!.annotations).toHaveProperty("actual")
		})
	})

	describe("Storage init logging", () => {
		it("emits debug log on schema initialization", async () => {
			const { logs, logger } = makeCaptureLogger()
			const CaptureLayer = Logger.replace(Logger.defaultLogger, logger)
			const TestSqliteLayer = SqliteClient.layer({ filename: ":memory:" })
			const TestStorageLayer = StorageLive.pipe(Layer.provide(TestSqliteLayer))
			const TestLayer = Layer.mergeAll(
				TestStorageLayer,
				CaptureLayer,
				Logger.minimumLogLevel(LogLevel.All),
			)

			await Effect.runPromise(
				Effect.gen(function* () {
					const storage = yield* Storage
					yield* storage.initialize
				}).pipe(Effect.provide(TestLayer)),
			)

			const initLogs = logs.filter(
				l => l.message === "Storage schema initialized",
			)
			expect(initLogs).toHaveLength(1)
			expect(initLogs[0]!.level).toBe("Debug")
		})
	})

	describe("JSON format", () => {
		it("does not crash with json logFormat", async () => {
			const ConfLayer = ConfigLive({
				...minimalConfig,
				logFormat: "json",
			})
			const LogLayer = LoggerLive.pipe(Layer.provide(ConfLayer))

			const program = Effect.log("hello json")

			// This should not throw
			await Effect.runPromise(program.pipe(Effect.provide(LogLayer)))
		})

		it("does not crash with structured logFormat", async () => {
			const ConfLayer = ConfigLive({
				...minimalConfig,
				logFormat: "structured",
			})
			const LogLayer = LoggerLive.pipe(Layer.provide(ConfLayer))

			const program = Effect.log("hello structured")

			await Effect.runPromise(program.pipe(Effect.provide(LogLayer)))
		})
	})

	describe("query telemetry fields", () => {
		it("getEvents logs durationMs and filters", async () => {
			const { logs, logger } = makeCaptureLogger()
			const TestSqliteLayer = SqliteClient.layer({ filename: ":memory:" })
			const TestStorageLayer = StorageLive.pipe(Layer.provide(TestSqliteLayer))
			const TestQueryLayer = QueryApiLive.pipe(Layer.provide(TestStorageLayer))
			const TestLayer = Layer.mergeAll(
				TestStorageLayer,
				TestQueryLayer,
				Logger.replace(Logger.defaultLogger, logger),
				Logger.minimumLogLevel(LogLevel.Debug),
			)

			await Effect.runPromise(
				Effect.gen(function* () {
					const storage = yield* Storage
					yield* storage.initialize
					yield* storage.insertEvents([
						{
							contractName: "Token",
							eventName: "Transfer",
							blockNumber: 100n,
							txHash: "0xabc",
							logIndex: 0,
							timestamp: 1700000000n,
							args: { from: "0xA", to: "0xB" },
						},
					])

					const query = yield* QueryApi
					const events = yield* query.getEvents({
						contractName: "Token",
						limit: 10,
					})
					expect(events).toHaveLength(1)
				}).pipe(Effect.provide(TestLayer)),
			)

			const queryLogs = logs.filter(l => l.message === "Query executed")
			expect(queryLogs).toHaveLength(1)
			const ann = queryLogs[0]!.annotations
			expect(ann).toHaveProperty("resultCount", "1")
			expect(ann).toHaveProperty("durationMs")
			expect(typeof ann.durationMs).toBe("string")
			expect(Number(ann.durationMs)).toBeGreaterThanOrEqual(0)
			expect(ann).toHaveProperty("filters")
			expect(typeof ann.filters).toBe("string")
			const parsed = JSON.parse(ann.filters as string) as Record<
				string,
				unknown
			>
			expect(parsed).toMatchObject({ contractName: "Token", limit: 10 })
		})

		it("getEventCount logs durationMs and filters", async () => {
			const { logs, logger } = makeCaptureLogger()
			const TestSqliteLayer = SqliteClient.layer({ filename: ":memory:" })
			const TestStorageLayer = StorageLive.pipe(Layer.provide(TestSqliteLayer))
			const TestQueryLayer = QueryApiLive.pipe(Layer.provide(TestStorageLayer))
			const TestLayer = Layer.mergeAll(
				TestStorageLayer,
				TestQueryLayer,
				Logger.replace(Logger.defaultLogger, logger),
				Logger.minimumLogLevel(LogLevel.Debug),
			)

			await Effect.runPromise(
				Effect.gen(function* () {
					const storage = yield* Storage
					yield* storage.initialize

					const query = yield* QueryApi
					const count = yield* query.getEventCount({
						eventName: "Transfer",
					})
					expect(count).toBe(0)
				}).pipe(Effect.provide(TestLayer)),
			)

			const countLogs = logs.filter(l => l.message === "Count executed")
			expect(countLogs).toHaveLength(1)
			const ann = countLogs[0]!.annotations
			expect(ann).toHaveProperty("count", "0")
			expect(ann).toHaveProperty("durationMs")
			expect(typeof ann.durationMs).toBe("string")
			expect(Number(ann.durationMs)).toBeGreaterThanOrEqual(0)
			expect(ann).toHaveProperty("filters")
			const parsed = JSON.parse(ann.filters as string) as Record<
				string,
				unknown
			>
			expect(parsed).toMatchObject({ eventName: "Transfer" })
		})

		it("getEvents logs empty filters for undefined query", async () => {
			const { logs, logger } = makeCaptureLogger()
			const TestSqliteLayer = SqliteClient.layer({ filename: ":memory:" })
			const TestStorageLayer = StorageLive.pipe(Layer.provide(TestSqliteLayer))
			const TestQueryLayer = QueryApiLive.pipe(Layer.provide(TestStorageLayer))
			const TestLayer = Layer.mergeAll(
				TestStorageLayer,
				TestQueryLayer,
				Logger.replace(Logger.defaultLogger, logger),
				Logger.minimumLogLevel(LogLevel.Debug),
			)

			await Effect.runPromise(
				Effect.gen(function* () {
					const storage = yield* Storage
					yield* storage.initialize

					const query = yield* QueryApi
					yield* query.getEvents()
				}).pipe(Effect.provide(TestLayer)),
			)

			const queryLogs = logs.filter(l => l.message === "Query executed")
			expect(queryLogs).toHaveLength(1)
			expect(queryLogs[0]!.annotations.filters).toBe("{}")
		})
	})

	describe("retry telemetry fields", () => {
		it(
			"logs method, attempt, maxRetries, delayMs on retry",
			{ timeout: 10000 },
			async () => {
				const { logs, logger } = makeCaptureLogger()
				let callCount = 0

				const MockRpcProvider = Layer.succeed(RpcProvider, {
					getBlockNumber: Effect.succeed(100n),
					getLogs: () =>
						Effect.suspend(() => {
							callCount++
							if (callCount <= 1) {
								return Effect.fail(
									new RpcError({
										reason: "timeout",
										method: "eth_getLogs",
									}),
								)
							}
							return Effect.succeed([] as ReadonlyArray<RawLog>)
						}),
					getBlock: () =>
						Effect.fail(
							new RpcError({
								reason: "not implemented",
								method: "getBlock",
							}),
						),
				})

				const TestConfig = ConfigLive({
					...minimalConfig,
					network: {
						logs: { chunkSize: 10000, maxRetries: 3 },
					},
				})

				const TestLayer = Layer.mergeAll(
					MockRpcProvider,
					TestConfig,
					Logger.replace(Logger.defaultLogger, logger),
					Logger.minimumLogLevel(LogLevel.Debug),
				)

				await Effect.runPromise(
					fetchLogs({
						address: "0x1",
						topics: ["0xabc"],
						fromBlock: 0n,
						toBlock: 100n,
					}).pipe(
						Stream.runCollect,
						Effect.map(Chunk.toReadonlyArray),
						Effect.provide(TestLayer),
					),
				)

				const retryLogs = logs.filter(
					l => l.message === "RPC getLogs failed, retrying",
				)
				expect(retryLogs).toHaveLength(1)
				const ann = retryLogs[0]!.annotations
				expect(ann).toHaveProperty("method", "eth_getLogs")
				expect(ann).toHaveProperty("attempt", "1")
				expect(ann).toHaveProperty("maxRetries", "3")
				expect(ann).toHaveProperty("delayMs", "1000")
				expect(ann).toHaveProperty("from", "0")
				expect(ann).toHaveProperty("to", "100")
				expect(ann).toHaveProperty("reason", "timeout")
			},
		)
	})
})
