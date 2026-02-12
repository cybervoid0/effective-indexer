import { Context, Effect, Fiber, Layer, Option, Ref, Schedule } from "effect"
import type { ResolvedConfig } from "../config.js"
import { Config } from "../config.js"
import { ProgressReporter, type ProgressSnapshot } from "./ProgressReporter.js"

const formatDuration = (ms: number): string => {
	const totalSec = Math.floor(ms / 1000)
	const h = Math.floor(totalSec / 3600)
	const m = Math.floor((totalSec % 3600) / 60)
	const s = totalSec % 60
	const pad = (n: number) => String(n).padStart(2, "0")
	return h > 0 ? `${pad(h)}:${pad(m)}:${pad(s)}` : `${pad(m)}:${pad(s)}`
}

const formatNumber = (n: number): string =>
	n.toLocaleString("en-US", { maximumFractionDigits: 0 })

const formatRate = (n: number): string =>
	n >= 100
		? formatNumber(Math.round(n))
		: n.toLocaleString("en-US", { maximumFractionDigits: 1 })

const buildLine = (snap: ProgressSnapshot, config: ResolvedConfig): string => {
	const pct = snap.percentage.toFixed(1)
	const processed = formatNumber(Number(snap.processedBlocks))
	const total = formatNumber(Number(snap.totalBlocks))
	const bps = formatRate(snap.blocksPerSecond)
	const eps = formatRate(snap.eventsPerSecond)
	const eta = snap.etaMs !== null ? formatDuration(snap.etaMs) : "--:--"
	const p = config.network.logs.parallelRequests
	const chunk = config.network.logs.chunkSize
	return `[Backfill] ${snap.contractName} ${pct}% | ${processed}/${total} blocks | ${bps} blk/s | ${eps} ev/s | ETA ${eta} | p=${p} | chunk=${chunk}`
}

const buildSummaryLine = (
	snap: ProgressSnapshot,
	config: ResolvedConfig,
): string => {
	const total = formatNumber(Number(snap.totalBlocks))
	const events = formatNumber(snap.totalEvents)
	const chunks = snap.chunkCount
	const dur = formatDuration(snap.elapsedMs)
	const bps = formatRate(snap.blocksPerSecond)
	const eps = formatRate(snap.eventsPerSecond)
	const p = config.network.logs.parallelRequests
	const chunk = config.network.logs.chunkSize
	return `[Backfill complete] ${snap.contractName}: ${total} blocks | ${events} events | ${chunks} chunks | ${dur} (${bps} blk/s, ${eps} ev/s) | p=${p} | chunkSize=${chunk}`
}

/**
 * Service contract for rendering CLI backfill progress.
 */
export interface ProgressRendererService {
	readonly startRendering: () => Effect.Effect<void>
	readonly stopRendering: () => Effect.Effect<void>
	readonly renderFinalSummary: (
		snapshot: ProgressSnapshot,
		config: ResolvedConfig,
	) => Effect.Effect<void>
}

/**
 * Progress renderer service tag.
 */
export class ProgressRenderer extends Context.Tag(
	"effective-indexer/ProgressRenderer",
)<ProgressRenderer, ProgressRendererService>() {}

/**
 * Terminal/console renderer implementation for progress snapshots.
 */
export const ProgressRendererLive: Layer.Layer<
	ProgressRenderer,
	never,
	Config | ProgressReporter
> = Layer.effect(
	ProgressRenderer,
	Effect.gen(function* () {
		const config = yield* Config
		const reporter = yield* ProgressReporter
		const fiberRef = yield* Ref.make<Option.Option<Fiber.RuntimeFiber<void>>>(
			Option.none(),
		)

		const enabled = config.enableTelemetry && config.telemetry.progress.enabled
		const intervalMs = config.telemetry.progress.intervalMs
		const isTTY = typeof process !== "undefined" && !!process.stdout?.isTTY

		const renderOnce: Effect.Effect<void> = Effect.gen(function* () {
			const snapshots = yield* reporter.getAllSnapshots()
			if (snapshots.length === 0) return

			if (isTTY) {
				const { default: logUpdate } = yield* Effect.promise(
					() => import("log-update"),
				)
				const pc = yield* Effect.promise(() => import("picocolors"))
				const lines = snapshots.map(s => {
					const raw = buildLine(s, config)
					return pc.default.cyan(raw)
				})
				logUpdate(lines.join("\n"))
			} else {
				for (const s of snapshots) {
					yield* Effect.log(buildLine(s, config))
				}
			}
		})

		return {
			startRendering: () => {
				if (!enabled) return Effect.void
				return Effect.gen(function* () {
					const fiber = yield* renderOnce.pipe(
						Effect.repeat(Schedule.spaced(intervalMs)),
						Effect.asVoid,
						Effect.catchAll(() => Effect.void),
						Effect.fork,
					)
					yield* Ref.set(fiberRef, Option.some(fiber))
				})
			},

			stopRendering: () => {
				if (!enabled) return Effect.void
				return Effect.gen(function* () {
					const maybeFiber = yield* Ref.get(fiberRef)
					if (Option.isSome(maybeFiber)) {
						yield* Fiber.interrupt(maybeFiber.value)
						yield* Ref.set(fiberRef, Option.none())
						if (isTTY) {
							const { default: logUpdate } = yield* Effect.promise(
								() => import("log-update"),
							)
							logUpdate.clear()
						}
					}
				})
			},

			renderFinalSummary: (snapshot, cfg) => {
				if (!enabled) return Effect.void
				return Effect.log(buildSummaryLine(snapshot, cfg))
			},
		}
	}),
)
