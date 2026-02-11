import { Effect } from "effect";
import { describe, expect, it, vi } from "vitest";
import { ERC20_ABI } from "./fixtures/abi.js";

vi.mock("../src/pipeline/Indexer.js", () => ({
	runIndexer: Effect.never,
}));

describe("Public API", () => {
	it("exports Indexer.create factory", async () => {
		const mod = await import("../src/index.js");
		expect(mod).toHaveProperty("Indexer");
		expect(mod.Indexer).toHaveProperty("create");
		expect(typeof mod.Indexer.create).toBe("function");
	});

	it("start resolves immediately and runs indexer in background", async () => {
		const { createIndexer } = await import("../src/index.js");
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
		});

		const startResult = await Promise.race([
			indexer.start().then(() => "resolved"),
			new Promise<"timeout">((resolve) => {
				setTimeout(() => resolve("timeout"), 50);
			}),
		]);

		expect(startResult).toBe("resolved");
		await indexer.stop();
	});
});
