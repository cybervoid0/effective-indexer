import { Effect, Layer } from "effect"
import { describe, it } from "vitest"
import { ConfigLive, type IndexerConfig } from "../src/config.js"
import {
	ProgressRenderer,
	ProgressRendererLive,
} from "../src/services/ProgressRenderer.js"
import { ProgressReporterLive } from "../src/services/ProgressReporter.js"

const minimalConfig: IndexerConfig = {
	rpcUrl: "http://localhost",
	contracts: [
		{
			name: "Test",
			address: "0x1",
			abi: [
				{
					type: "event",
					name: "Transfer",
					inputs: [
						{ indexed: true, name: "from", type: "address" },
						{ indexed: true, name: "to", type: "address" },
						{ indexed: false, name: "value", type: "uint256" },
					],
				},
			],
			events: ["Transfer"],
		},
	],
}

const makeTestLayer = (configOverrides: Partial<IndexerConfig> = {}) => {
	const ConfigLayer = ConfigLive({ ...minimalConfig, ...configOverrides })
	const ReporterLayer = ProgressReporterLive
	const RendererLayer = ProgressRendererLive.pipe(
		Layer.provide(Layer.merge(ConfigLayer, ReporterLayer)),
	)
	return Layer.merge(RendererLayer, ConfigLayer)
}

describe("ProgressRenderer", () => {
	it("disabled mode: startRendering/stopRendering are no-ops", async () => {
		const TestLayer = makeTestLayer({
			telemetry: { progress: { enabled: false } },
		})

		await Effect.runPromise(
			Effect.gen(function* () {
				const renderer = yield* ProgressRenderer
				// These should not throw or start any fiber
				yield* renderer.startRendering()
				yield* renderer.stopRendering()
			}).pipe(Effect.provide(TestLayer)),
		)
	})

	it("renderFinalSummary is a no-op when disabled", async () => {
		const TestLayer = makeTestLayer({
			telemetry: { progress: { enabled: false } },
		})

		await Effect.runPromise(
			Effect.gen(function* () {
				const renderer = yield* ProgressRenderer
				yield* renderer.renderFinalSummary(
					{
						contractName: "Token",
						totalBlocks: 1000n,
						processedBlocks: 1000n,
						totalEvents: 50,
						chunkCount: 10,
						elapsedMs: 5000,
						blocksPerSecond: 200,
						eventsPerSecond: 10,
						percentage: 100,
						etaMs: null,
					},
					{
						rpcUrl: "http://localhost",
						dbPath: "./indexer.db",
						contracts: minimalConfig.contracts,
						network: {
							polling: { intervalMs: 12000, confirmations: 1 },
							logs: {
								chunkSize: 5000,
								maxRetries: 5,
								retry: { baseDelayMs: 1000, maxDelayMs: 30000 },
								parallelRequests: 1,
							},
							reorg: { depth: 20 },
						},
						telemetry: { progress: { enabled: false, intervalMs: 3000 } },
						logLevel: "info",
						logFormat: "pretty",
						enableTelemetry: true,
					},
				)
			}).pipe(Effect.provide(TestLayer)),
		)
	})

	it("enabled mode: start/stop rendering lifecycle works", async () => {
		const TestLayer = makeTestLayer({
			telemetry: { progress: { enabled: true, intervalMs: 500 } },
		})

		await Effect.runPromise(
			Effect.gen(function* () {
				const renderer = yield* ProgressRenderer
				yield* renderer.startRendering()
				// Give a tiny window for the fiber to start
				yield* Effect.sleep(50)
				yield* renderer.stopRendering()
			}).pipe(Effect.provide(TestLayer)),
		)
	})

	it("is disabled when enableTelemetry=false", async () => {
		const TestLayer = makeTestLayer({
			enableTelemetry: false,
			telemetry: { progress: { enabled: true, intervalMs: 500 } },
		})

		await Effect.runPromise(
			Effect.gen(function* () {
				const renderer = yield* ProgressRenderer
				yield* renderer.startRendering()
				yield* renderer.stopRendering()
			}).pipe(Effect.provide(TestLayer)),
		)
	})
})
