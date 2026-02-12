import { Effect } from "effect"
import { describe, expect, it } from "vitest"
import {
	type IndexerConfig,
	resolveConfig,
	resolveConfigEffect,
} from "../src/config.js"

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

describe("resolveConfig", () => {
	describe("parallelRequests", () => {
		it("defaults to 1", () => {
			const resolved = resolveConfig(minimalConfig)
			expect(resolved.network.logs.parallelRequests).toBe(1)
		})

		it("resolves custom value", () => {
			const resolved = resolveConfig({
				...minimalConfig,
				network: { logs: { parallelRequests: 4 } },
			})
			expect(resolved.network.logs.parallelRequests).toBe(4)
		})

		it("throws for parallelRequests = 0", () => {
			expect(() =>
				resolveConfig({
					...minimalConfig,
					network: { logs: { parallelRequests: 0 } },
				}),
			).toThrow()
		})

		it("throws for parallelRequests = 1.5", () => {
			expect(() =>
				resolveConfig({
					...minimalConfig,
					network: { logs: { parallelRequests: 1.5 } },
				}),
			).toThrow()
		})

		it("throws for negative parallelRequests", () => {
			expect(() =>
				resolveConfig({
					...minimalConfig,
					network: { logs: { parallelRequests: -1 } },
				}),
			).toThrow()
		})
	})

	describe("telemetry", () => {
		it("defaults to enabled with 3000ms interval", () => {
			const resolved = resolveConfig(minimalConfig)
			expect(resolved.telemetry.progress.enabled).toBe(true)
			expect(resolved.telemetry.progress.intervalMs).toBe(3000)
		})

		it("resolves custom telemetry values", () => {
			const resolved = resolveConfig({
				...minimalConfig,
				telemetry: {
					progress: { enabled: false, intervalMs: 5000 },
				},
			})
			expect(resolved.telemetry.progress.enabled).toBe(false)
			expect(resolved.telemetry.progress.intervalMs).toBe(5000)
		})

		it("resolves enabled=false correctly", () => {
			const resolved = resolveConfig({
				...minimalConfig,
				telemetry: { progress: { enabled: false } },
			})
			expect(resolved.telemetry.progress.enabled).toBe(false)
			expect(resolved.telemetry.progress.intervalMs).toBe(3000)
		})

		it("throws for intervalMs < 500", () => {
			expect(() =>
				resolveConfig({
					...minimalConfig,
					telemetry: { progress: { intervalMs: 100 } },
				}),
			).toThrow()
		})

		it("allows intervalMs = 500", () => {
			const resolved = resolveConfig({
				...minimalConfig,
				telemetry: { progress: { intervalMs: 500 } },
			})
			expect(resolved.telemetry.progress.intervalMs).toBe(500)
		})

		it("throws for intervalMs = NaN", () => {
			expect(() =>
				resolveConfig({
					...minimalConfig,
					telemetry: { progress: { intervalMs: Number.NaN } },
				}),
			).toThrow()
		})

		it("throws for intervalMs = Infinity", () => {
			expect(() =>
				resolveConfig({
					...minimalConfig,
					telemetry: { progress: { intervalMs: Number.POSITIVE_INFINITY } },
				}),
			).toThrow()
		})

		it("throws for non-integer intervalMs", () => {
			expect(() =>
				resolveConfig({
					...minimalConfig,
					telemetry: { progress: { intervalMs: 750.5 } },
				}),
			).toThrow()
		})
	})
})

describe("resolveConfigEffect", () => {
	it("fails through Effect error channel for invalid parallelRequests", async () => {
		const exit = await Effect.runPromise(
			resolveConfigEffect({
				...minimalConfig,
				network: { logs: { parallelRequests: 0 } },
			}).pipe(Effect.either),
		)

		expect(exit._tag).toBe("Left")
		if (exit._tag === "Left") {
			expect(exit.left._tag).toBe("ConfigError")
			expect(exit.left.field).toBe("network.logs.parallelRequests")
		}
	})

	it("succeeds and returns resolved config", async () => {
		const resolved = await Effect.runPromise(resolveConfigEffect(minimalConfig))
		expect(resolved.network.logs.parallelRequests).toBe(1)
		expect(resolved.telemetry.progress.intervalMs).toBe(3000)
	})
})
