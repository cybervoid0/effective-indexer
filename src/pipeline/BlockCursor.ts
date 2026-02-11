import { Context, Effect, Layer, Ref, Schedule, Stream } from "effect"
import { Config } from "../config.js"
import type { RpcError } from "../errors.js"
import { RpcProvider } from "../services/RpcProvider.js"

export class BlockCursor extends Context.Tag("@rootstock/indexer/BlockCursor")<
	BlockCursor,
	{
		readonly liveBlocks: Stream.Stream<bigint, RpcError>
	}
>() {}

export const BlockCursorLive = Layer.effect(
	BlockCursor,
	Effect.gen(function* () {
		const rpc = yield* RpcProvider
		const config = yield* Config

		const lastSeen = yield* Ref.make<bigint>(-1n)
		const initialized = yield* Ref.make(false)

		const pollOnce = Effect.gen(function* () {
			const current = yield* rpc.getBlockNumber
			const confirmations = BigInt(config.confirmations)
			const confirmed = current - confirmations
			const prev = yield* Ref.get(lastSeen)
			const isInitialized = yield* Ref.get(initialized)

			// Do not emit current head on first poll to avoid duplicates after backfill.
			if (!isInitialized) {
				yield* Ref.set(initialized, true)
				yield* Ref.set(lastSeen, confirmed)
				return [] as ReadonlyArray<bigint>
			}

			if (confirmed <= prev) {
				return [] as ReadonlyArray<bigint>
			}

			const blocks: bigint[] = []
			for (let block = prev + 1n; block <= confirmed; block += 1n) {
				blocks.push(block)
			}

			yield* Ref.set(lastSeen, confirmed)
			return blocks as ReadonlyArray<bigint>
		})

		const liveBlocks: Stream.Stream<bigint, RpcError> =
			Stream.repeatEffectWithSchedule(
				pollOnce,
				Schedule.spaced(config.pollInterval),
			).pipe(Stream.flatMap(Stream.fromIterable))

		return { liveBlocks }
	}),
)
