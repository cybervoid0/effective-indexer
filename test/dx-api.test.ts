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

		const timer = { id: "keepalive" }
		const setIntervalMock = vi.fn(() => timer as unknown as NodeJS.Timeout)
		const clearIntervalMock = vi.fn()

		const start = vi.fn(async () => undefined)
		const stop = vi.fn(async () => undefined)
		const createIndexer = vi.fn(
			(): IndexerHandle => ({
				start,
				stop,
				query: async () => [],
				count: async () => 0,
			}),
		)

		const running = runIndexerWorker(makeConfig(), {
			createIndexer,
			runtime: {
				process: processMock,
				setInterval: setIntervalMock,
				clearInterval: clearIntervalMock,
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
		expect(clearIntervalMock).toHaveBeenCalledTimes(1)
		expect(processMock.off).toHaveBeenCalled()
	})
})
