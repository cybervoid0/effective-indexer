import { Context, Effect, Layer, Ref, Schedule, Stream } from "effect"
import { Config } from "../config.js"
import type { RpcError } from "../errors.js"
import { RpcProvider } from "../services/RpcProvider.js"

/**
 * Emits newly confirmed block numbers for live indexing.
 */
export class BlockCursor extends Context.Tag("effective-indexer/BlockCursor")<
	BlockCursor,
	{
		readonly liveBlocks: Stream.Stream<bigint, RpcError>
	}
>() {}

/**
 * Polling-based block cursor implementation.
 */
export const BlockCursorLive = Layer.effect(
	BlockCursor,
	Effect.gen(function* () {
		const rpc = yield* RpcProvider
		const config = yield* Config

		const lastSeen = yield* Ref.make<bigint>(-1n)
		const initialized = yield* Ref.make(false)

		const pollOnce = Effect.gen(function* () {
			const current = yield* rpc.getBlockNumber
			const confirmations = BigInt(config.network.polling.confirmations)
			const confirmed = current - confirmations
			const prev = yield* Ref.get(lastSeen)
			const isInitialized = yield* Ref.get(initialized)

			// Do not emit current head on first poll to avoid duplicates after backfill.
			if (!isInitialized) {
				yield* Ref.set(initialized, true)
				yield* Ref.set(lastSeen, confirmed)
				yield* Effect.logDebug("BlockCursor initialized").pipe(
					Effect.annotateLogs({
						confirmedHead: confirmed.toString(),
					}),
				)
				return [] as ReadonlyArray<bigint>
			}

			if (confirmed <= prev) {
				yield* Effect.logTrace("No new confirmed blocks")
				return [] as ReadonlyArray<bigint>
			}

			const blocks: bigint[] = []
			for (let block = prev + 1n; block <= confirmed; block += 1n) {
				blocks.push(block)
			}

			yield* Ref.set(lastSeen, confirmed)
			yield* Effect.logTrace("Blocks emitted").pipe(
				Effect.annotateLogs({
					from: (prev + 1n).toString(),
					to: confirmed.toString(),
					count: blocks.length.toString(),
				}),
			)
			return blocks as ReadonlyArray<bigint>
		})

		const liveBlocks: Stream.Stream<bigint, RpcError> =
			Stream.repeatEffectWithSchedule(
				pollOnce,
				Schedule.spaced(config.network.polling.intervalMs),
			).pipe(Stream.flatMap(Stream.fromIterable))

		return { liveBlocks }
	}),
)
