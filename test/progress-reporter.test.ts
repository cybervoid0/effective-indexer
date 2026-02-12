import { Effect } from "effect"
import { describe, expect, it } from "vitest"
import {
	type BackfillProgress,
	computeSnapshot,
	ProgressReporter,
	ProgressReporterLive,
} from "../src/services/ProgressReporter.js"

describe("computeSnapshot", () => {
	const makeProgress = (
		overrides: Partial<BackfillProgress> = {},
	): BackfillProgress => ({
		contractName: "Token",
		totalBlocks: 1000n,
		processedBlocks: 500n,
		totalEvents: 100,
		chunkCount: 5,
		startedAt: Date.now() - 10000, // 10 seconds ago
		...overrides,
	})

	it("computes percentage correctly", () => {
		const snap = computeSnapshot(makeProgress())
		expect(snap.percentage).toBeCloseTo(50, 0)
	})

	it("computes 0% for no processed blocks", () => {
		const snap = computeSnapshot(makeProgress({ processedBlocks: 0n }))
		expect(snap.percentage).toBe(0)
	})

	it("computes 100% when all blocks processed", () => {
		const snap = computeSnapshot(
			makeProgress({ processedBlocks: 1000n, totalBlocks: 1000n }),
		)
		expect(snap.percentage).toBeCloseTo(100, 0)
	})

	it("computes blocksPerSecond", () => {
		const snap = computeSnapshot(makeProgress())
		// 500 blocks in ~10 seconds → ~50 blk/s
		expect(snap.blocksPerSecond).toBeGreaterThan(40)
		expect(snap.blocksPerSecond).toBeLessThan(60)
	})

	it("computes eventsPerSecond", () => {
		const snap = computeSnapshot(makeProgress())
		// 100 events in ~10 seconds → ~10 ev/s
		expect(snap.eventsPerSecond).toBeGreaterThan(8)
		expect(snap.eventsPerSecond).toBeLessThan(12)
	})

	it("computes ETA", () => {
		const snap = computeSnapshot(makeProgress())
		// 500 remaining at ~50 blk/s → ~10s ETA
		expect(snap.etaMs).not.toBeNull()
		expect(snap.etaMs!).toBeGreaterThan(5000)
		expect(snap.etaMs!).toBeLessThan(15000)
	})

	it("returns null ETA when no blocks processed yet", () => {
		const snap = computeSnapshot(
			makeProgress({ processedBlocks: 0n, startedAt: Date.now() }),
		)
		expect(snap.etaMs).toBeNull()
	})

	it("handles zero totalBlocks", () => {
		const snap = computeSnapshot(
			makeProgress({ totalBlocks: 0n, processedBlocks: 0n }),
		)
		expect(snap.percentage).toBe(0)
	})

	it("preserves contract name and counts", () => {
		const snap = computeSnapshot(makeProgress())
		expect(snap.contractName).toBe("Token")
		expect(snap.totalBlocks).toBe(1000n)
		expect(snap.processedBlocks).toBe(500n)
		expect(snap.totalEvents).toBe(100)
		expect(snap.chunkCount).toBe(5)
	})
})

describe("ProgressReporter service", () => {
	const runWithReporter = <A>(
		f: (reporter: ProgressReporter["Type"]) => Effect.Effect<A>,
	) =>
		Effect.runPromise(
			Effect.gen(function* () {
				const reporter = yield* ProgressReporter
				return yield* f(reporter)
			}).pipe(Effect.provide(ProgressReporterLive)),
		)

	it("start → update → getSnapshot lifecycle", async () => {
		const snap = await runWithReporter(reporter =>
			Effect.gen(function* () {
				yield* reporter.start("Token", 1000n)
				yield* reporter.update("Token", 250n, 50)
				yield* reporter.incrementChunks("Token")
				return yield* reporter.getSnapshot("Token")
			}),
		)
		expect(snap).not.toBeNull()
		expect(snap!.processedBlocks).toBe(250n)
		expect(snap!.totalEvents).toBe(50)
		expect(snap!.chunkCount).toBe(1)
	})

	it("finish removes the entry", async () => {
		const snap = await runWithReporter(reporter =>
			Effect.gen(function* () {
				yield* reporter.start("Token", 1000n)
				yield* reporter.update("Token", 500n, 100)
				yield* reporter.finish("Token")
				return yield* reporter.getSnapshot("Token")
			}),
		)
		expect(snap).toBeNull()
	})

	it("tracks multiple contracts independently", async () => {
		const snapshots = await runWithReporter(reporter =>
			Effect.gen(function* () {
				yield* reporter.start("TokenA", 1000n)
				yield* reporter.start("TokenB", 2000n)
				yield* reporter.update("TokenA", 500n, 10)
				yield* reporter.update("TokenB", 100n, 5)
				return yield* reporter.getAllSnapshots()
			}),
		)
		expect(snapshots).toHaveLength(2)
		const a = snapshots.find(s => s.contractName === "TokenA")
		const b = snapshots.find(s => s.contractName === "TokenB")
		expect(a!.processedBlocks).toBe(500n)
		expect(b!.processedBlocks).toBe(100n)
	})

	it("accumulates events across updates", async () => {
		const snap = await runWithReporter(reporter =>
			Effect.gen(function* () {
				yield* reporter.start("Token", 1000n)
				yield* reporter.update("Token", 200n, 10)
				yield* reporter.update("Token", 400n, 20)
				yield* reporter.update("Token", 600n, 30)
				return yield* reporter.getSnapshot("Token")
			}),
		)
		expect(snap!.totalEvents).toBe(60)
		expect(snap!.processedBlocks).toBe(600n)
	})

	it("getSnapshot returns null for unknown contract", async () => {
		const snap = await runWithReporter(reporter =>
			reporter.getSnapshot("NonExistent"),
		)
		expect(snap).toBeNull()
	})
})
