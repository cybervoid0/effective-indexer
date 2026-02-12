import { describe, expect, it } from "vitest"
import { type IndexerConfig, resolveConfig } from "../src/config.js"
import { ConfigError } from "../src/errors.js"

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
			).toThrow(ConfigError)
		})

		it("throws for parallelRequests = 1.5", () => {
			expect(() =>
				resolveConfig({
					...minimalConfig,
					network: { logs: { parallelRequests: 1.5 } },
				}),
			).toThrow(ConfigError)
		})

		it("throws for negative parallelRequests", () => {
			expect(() =>
				resolveConfig({
					...minimalConfig,
					network: { logs: { parallelRequests: -1 } },
				}),
			).toThrow(ConfigError)
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
			).toThrow(ConfigError)
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
			).toThrow(ConfigError)
		})

		it("throws for intervalMs = Infinity", () => {
			expect(() =>
				resolveConfig({
					...minimalConfig,
					telemetry: { progress: { intervalMs: Number.POSITIVE_INFINITY } },
				}),
			).toThrow(ConfigError)
		})

		it("throws for non-integer intervalMs", () => {
			expect(() =>
				resolveConfig({
					...minimalConfig,
					telemetry: { progress: { intervalMs: 750.5 } },
				}),
			).toThrow(ConfigError)
		})
	})
})
