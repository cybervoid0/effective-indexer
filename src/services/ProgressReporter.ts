import { Context, Effect, Layer, Ref } from "effect"

export interface BackfillProgress {
	readonly contractName: string
	readonly totalBlocks: bigint
	readonly processedBlocks: bigint
	readonly totalEvents: number
	readonly chunkCount: number
	readonly startedAt: number
}

export interface ProgressSnapshot {
	readonly contractName: string
	readonly totalBlocks: bigint
	readonly processedBlocks: bigint
	readonly totalEvents: number
	readonly chunkCount: number
	readonly elapsedMs: number
	readonly blocksPerSecond: number
	readonly eventsPerSecond: number
	readonly percentage: number
	readonly etaMs: number | null
}

export const computeSnapshot = (p: BackfillProgress): ProgressSnapshot => {
	const now = Date.now()
	const elapsedMs = Math.max(now - p.startedAt, 1)
	const elapsedSec = elapsedMs / 1000
	const processed = Number(p.processedBlocks)
	const total = Number(p.totalBlocks)
	const percentage = total > 0 ? (processed / total) * 100 : 0
	const blocksPerSecond = elapsedSec > 0 ? processed / elapsedSec : 0
	const eventsPerSecond = elapsedSec > 0 ? p.totalEvents / elapsedSec : 0
	const remaining = total - processed
	const etaMs =
		blocksPerSecond > 0 ? (remaining / blocksPerSecond) * 1000 : null

	return {
		contractName: p.contractName,
		totalBlocks: p.totalBlocks,
		processedBlocks: p.processedBlocks,
		totalEvents: p.totalEvents,
		chunkCount: p.chunkCount,
		elapsedMs,
		blocksPerSecond,
		eventsPerSecond,
		percentage,
		etaMs,
	}
}

export interface ProgressReporterService {
	readonly start: (
		contractName: string,
		totalBlocks: bigint,
	) => Effect.Effect<void>
	readonly update: (
		contractName: string,
		processedBlocks: bigint,
		eventsInChunk: number,
	) => Effect.Effect<void>
	readonly incrementChunks: (contractName: string) => Effect.Effect<void>
	readonly finish: (contractName: string) => Effect.Effect<void>
	readonly getSnapshot: (
		contractName: string,
	) => Effect.Effect<ProgressSnapshot | null>
	readonly getAllSnapshots: () => Effect.Effect<ReadonlyArray<ProgressSnapshot>>
}

export class ProgressReporter extends Context.Tag(
	"effective-indexer/ProgressReporter",
)<ProgressReporter, ProgressReporterService>() {}

export const ProgressReporterLive: Layer.Layer<ProgressReporter> = Layer.effect(
	ProgressReporter,
	Effect.gen(function* () {
		const state = yield* Ref.make(new Map<string, BackfillProgress>())

		return {
			start: (contractName, totalBlocks) =>
				Ref.update(state, map => {
					const next = new Map(map)
					next.set(contractName, {
						contractName,
						totalBlocks,
						processedBlocks: 0n,
						totalEvents: 0,
						chunkCount: 0,
						startedAt: Date.now(),
					})
					return next
				}),

			update: (contractName, processedBlocks, eventsInChunk) =>
				Ref.update(state, map => {
					const entry = map.get(contractName)
					if (!entry) return map
					const boundedProcessed =
						processedBlocks > entry.totalBlocks
							? entry.totalBlocks
							: processedBlocks
					const nextProcessed =
						boundedProcessed > entry.processedBlocks
							? boundedProcessed
							: entry.processedBlocks
					const next = new Map(map)
					next.set(contractName, {
						...entry,
						processedBlocks: nextProcessed,
						totalEvents: entry.totalEvents + eventsInChunk,
					})
					return next
				}),

			incrementChunks: contractName =>
				Ref.update(state, map => {
					const entry = map.get(contractName)
					if (!entry) return map
					const next = new Map(map)
					next.set(contractName, {
						...entry,
						chunkCount: entry.chunkCount + 1,
					})
					return next
				}),

			finish: contractName =>
				Ref.update(state, map => {
					const next = new Map(map)
					next.delete(contractName)
					return next
				}),

			getSnapshot: contractName =>
				Ref.get(state).pipe(
					Effect.map(map => {
						const entry = map.get(contractName)
						return entry ? computeSnapshot(entry) : null
					}),
				),

			getAllSnapshots: () =>
				Ref.get(state).pipe(
					Effect.map(map => Array.from(map.values()).map(computeSnapshot)),
				),
		}
	}),
)
