import { Effect } from "effect"
import { describe, expect, it, vi } from "vitest"
import { ERC20_ABI } from "./fixtures/abi.js"

const loadIndexModule = async (
	runIndexer: Effect.Effect<void, unknown, never>,
) => {
	vi.resetModules()
	vi.doMock("../src/pipeline/Indexer.js", () => ({
		runIndexer,
	}))
	return import("../src/index.js")
}

describe("Public API", () => {
	it("exports Indexer.create factory", async () => {
		const mod = await loadIndexModule(Effect.never)
		expect(mod).toHaveProperty("Indexer")
		expect(mod.Indexer).toHaveProperty("create")
		expect(typeof mod.Indexer.create).toBe("function")
	})

	it("start resolves immediately and runs indexer in background", async () => {
		const { createIndexer } = await loadIndexModule(Effect.never)
		const indexer = createIndexer({
			rpcUrl: "http://localhost",
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

		const startResult = await Promise.race([
			indexer.start().then(() => "resolved"),
			new Promise<"timeout">(resolve => {
				setTimeout(() => resolve("timeout"), 50)
			}),
		])

		expect(startResult).toBe("resolved")
		await indexer.stop()
	})

	it("logs background runner failure", async () => {
		const consoleErrorSpy = vi
			.spyOn(console, "error")
			.mockImplementation(() => undefined)
		const failingRunIndexer = Effect.fail(new Error("boom in runIndexer"))
		const { createIndexer } = await loadIndexModule(failingRunIndexer)
		const indexer = createIndexer({
			rpcUrl: "http://localhost",
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

		await indexer.start()
		await new Promise<void>(resolve => setTimeout(resolve, 20))

		expect(consoleErrorSpy).toHaveBeenCalled()
		expect(consoleErrorSpy.mock.calls[0]?.[0]).toBe(
			"Indexer background worker failed:",
		)
		await indexer.stop()
		consoleErrorSpy.mockRestore()
	})

	it("stop is idempotent", async () => {
		const { createIndexer } = await loadIndexModule(Effect.never)
		const indexer = createIndexer({
			rpcUrl: "http://localhost",
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

		await indexer.start()
		await Promise.all([indexer.stop(), indexer.stop(), indexer.stop()])
	})
})
