import { describe, expect, it, vi } from "vitest"
import {
	defineIndexerConfig,
	type IndexerConfig,
	type IndexerHandle,
	resolveIndexerConfigFromEnv,
	runIndexerWorker,
} from "../src/index.js"
import { ERC20_ABI } from "./fixtures/abi.js"

const makeConfig = (): IndexerConfig => ({
	rpcUrl: "https://rpc.example/{{API_KEY}}",
	dbPath: ":memory:",
	contracts: [
		{
			name: "Test",
			address: "0x1111111111111111111111111111111111111111",
			abi: ERC20_ABI,
			events: ["Transfer"],
		},
	],
})

const makeRuntimeMocks = () => {
	type SignalHandler = () => void
	const signalHandlers = new Map<string, SignalHandler>()
	const processMock = {
		on: vi.fn((signal: string, handler: SignalHandler) => {
			signalHandlers.set(signal, handler)
			return processMock
		}),
		off: vi.fn((signal: string, _handler: SignalHandler) => {
			signalHandlers.delete(signal)
			return processMock
		}),
	}

	return {
		signalHandlers,
		processMock,
	}
}

describe("DX API", () => {
	it("defineIndexerConfig returns the same object", () => {
		const config = makeConfig()
		const defined = defineIndexerConfig(config)
		expect(defined).toBe(config)
	})

	it("resolveIndexerConfigFromEnv resolves placeholders", () => {
		const resolved = resolveIndexerConfigFromEnv(makeConfig(), {
			env: {
				API_KEY: "secret-key",
			},
		})

		expect(resolved.rpcUrl).toBe("https://rpc.example/secret-key")
	})

	it("resolveIndexerConfigFromEnv uses rpc override env", () => {
		const resolved = resolveIndexerConfigFromEnv(makeConfig(), {
			env: {
				EVM_RPC_URL: "https://override.example",
				API_KEY: "ignored",
			},
		})

		expect(resolved.rpcUrl).toBe("https://override.example")
	})

	it("resolveIndexerConfigFromEnv throws for missing placeholders", () => {
		expect(() =>
			resolveIndexerConfigFromEnv(makeConfig(), {
				env: {},
			}),
		).toThrow("Missing required env var: API_KEY")
	})

	it("runIndexerWorker starts and handles graceful shutdown", async () => {
		const { signalHandlers, processMock } = makeRuntimeMocks()

		const start = vi.fn(async () => undefined)
		const stop = vi.fn(async () => undefined)
		const createIndexer = vi.fn(
			(): IndexerHandle => ({
				start,
				stop,
				waitForExit: async () => new Promise<void>(() => undefined),
				query: async () => [],
				count: async () => 0,
			}),
		)

		const running = runIndexerWorker(makeConfig(), {
			createIndexer,
			runtime: {
				process: processMock,
			},
		})

		await new Promise<void>(resolve => setTimeout(resolve, 0))
		expect(start).toHaveBeenCalledTimes(1)
		expect(processMock.on).toHaveBeenCalled()

		const sigint = signalHandlers.get("SIGINT" as NodeJS.Signals)
		expect(sigint).toBeTypeOf("function")
		sigint?.()

		await running

		expect(stop).toHaveBeenCalledTimes(1)
		expect(processMock.off).toHaveBeenCalled()
	})

	it("runIndexerWorker retries after crash and recovers", async () => {
		const { signalHandlers, processMock } = makeRuntimeMocks()
		const consoleErrorSpy = vi
			.spyOn(console, "error")
			.mockImplementation(() => undefined)

		const firstStart = vi.fn(async () => undefined)
		const firstStop = vi.fn(async () => undefined)
		const secondStart = vi.fn(async () => undefined)
		const secondStop = vi.fn(async () => undefined)

		const handles: IndexerHandle[] = [
			{
				start: firstStart,
				stop: firstStop,
				waitForExit: async () => {
					throw new Error("boom")
				},
				query: async () => [],
				count: async () => 0,
			},
			{
				start: secondStart,
				stop: secondStop,
				waitForExit: async () => new Promise<void>(() => undefined),
				query: async () => [],
				count: async () => 0,
			},
		]

		const createIndexer = vi.fn(() => {
			const next = handles.shift()
			if (!next) {
				throw new Error("No more handles configured")
			}
			return next
		})

		const running = runIndexerWorker(makeConfig(), {
			createIndexer,
			recovery: {
				initialRetryDelayMs: 1,
				maxRetryDelayMs: 1,
				maxRecoveryDurationMs: 10_000,
			},
			runtime: {
				process: processMock,
			},
		})

		await new Promise<void>(resolve => setTimeout(resolve, 20))
		const sigterm = signalHandlers.get("SIGTERM" as NodeJS.Signals)
		sigterm?.()
		await running

		expect(createIndexer).toHaveBeenCalledTimes(2)
		expect(firstStart).toHaveBeenCalledTimes(1)
		expect(firstStop).toHaveBeenCalledTimes(1)
		expect(secondStart).toHaveBeenCalledTimes(1)
		expect(secondStop).toHaveBeenCalledTimes(1)
		consoleErrorSpy.mockRestore()
	})

	it("runIndexerWorker notifies when recovery window is exhausted", async () => {
		const { processMock } = makeRuntimeMocks()
		const notify = vi.fn(async () => undefined)
		const consoleErrorSpy = vi
			.spyOn(console, "error")
			.mockImplementation(() => undefined)

		const createIndexer = vi.fn(
			(): IndexerHandle => ({
				start: async () => undefined,
				stop: async () => undefined,
				waitForExit: async () => {
					throw new Error("fatal")
				},
				query: async () => [],
				count: async () => 0,
			}),
		)

		await expect(
			runIndexerWorker(makeConfig(), {
				createIndexer,
				recovery: {
					maxRecoveryDurationMs: 0,
					initialRetryDelayMs: 1,
					maxRetryDelayMs: 1,
				},
				onRecoveryFailure: notify,
				runtime: {
					process: processMock,
				},
			}),
		).rejects.toThrow("fatal")

		expect(notify).toHaveBeenCalledTimes(1)
		consoleErrorSpy.mockRestore()
	})

	it("preserves original error when onRecoveryFailure throws", async () => {
		const { processMock } = makeRuntimeMocks()
		const notify = vi.fn(async () => {
			throw new Error("notify failed")
		})
		const consoleErrorSpy = vi
			.spyOn(console, "error")
			.mockImplementation(() => undefined)

		const createIndexer = vi.fn(
			(): IndexerHandle => ({
				start: async () => undefined,
				stop: async () => undefined,
				waitForExit: async () => {
					throw new Error("fatal")
				},
				query: async () => [],
				count: async () => 0,
			}),
		)

		await expect(
			runIndexerWorker(makeConfig(), {
				createIndexer,
				recovery: {
					maxRecoveryDurationMs: 0,
					initialRetryDelayMs: 1,
					maxRetryDelayMs: 1,
				},
				onRecoveryFailure: notify,
				runtime: {
					process: processMock,
				},
			}),
		).rejects.toThrow("fatal")

		expect(notify).toHaveBeenCalledTimes(1)
		consoleErrorSpy.mockRestore()
	})

	it("runIndexerWorker uses alert webhook from config", async () => {
		const { processMock } = makeRuntimeMocks()
		const fetchMock = vi.fn(async () => ({ ok: true, status: 200 }))
		const originalFetch = globalThis.fetch
		globalThis.fetch = fetchMock as unknown as typeof fetch

		const createIndexer = vi.fn(
			(): IndexerHandle => ({
				start: async () => undefined,
				stop: async () => undefined,
				waitForExit: async () => {
					throw new Error("fatal")
				},
				query: async () => [],
				count: async () => 0,
			}),
		)

		await expect(
			runIndexerWorker(
				{
					...makeConfig(),
					worker: {
						alert: {
							webhookUrl: "https://hooks.example.local/alert",
						},
					},
				},
				{
					createIndexer,
					recovery: {
						maxRecoveryDurationMs: 0,
					},
					runtime: {
						process: processMock,
					},
				},
			),
		).rejects.toThrow("fatal")

		expect(fetchMock).toHaveBeenCalledTimes(1)
		globalThis.fetch = originalFetch
	})
})
