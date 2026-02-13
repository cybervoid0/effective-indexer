import { Context, Effect, Layer, Ref } from "effect"
import { Config } from "../config.js"
import { ReorgDetected, type StorageError } from "../errors.js"
import type { BlockInfo } from "../services/RpcProvider.js"
import { Storage } from "../services/Storage.js"

/**
 * Detects chain reorganizations and coordinates rollback handling.
 */
export class ReorgDetector extends Context.Tag(
	"effective-indexer/ReorgDetector",
)<
	ReorgDetector,
	{
		readonly verifyBlock: (
			block: BlockInfo,
		) => Effect.Effect<void, ReorgDetected | StorageError>
		readonly handleReorg: (
			forkBlock: bigint,
		) => Effect.Effect<void, StorageError>
	}
>() {}

/**
 * Live implementation backed by persisted block hash history.
 */
export const ReorgDetectorLive = Layer.effect(
	ReorgDetector,
	Effect.gen(function* () {
		const storage = yield* Storage
		const config = yield* Config
		const reorgDepth = config.network.reorg.depth

		const blockHashBuffer = yield* Ref.make<Map<bigint, string>>(new Map())
		const initialized = yield* Ref.make(false)

		// Lazy bootstrap from persisted block hashes keeps startup non-blocking.
		const ensureInitialized = Effect.gen(function* () {
			const isInit = yield* Ref.get(initialized)
			if (!isInit) {
				const stored = yield* storage.getRecentBlockHashes(reorgDepth)
				yield* Ref.set(
					blockHashBuffer,
					new Map(stored.map(h => [h.blockNumber, h.blockHash])),
				)
				yield* Ref.set(initialized, true)
			}
		})

		// Validates parent linkage and advances the reorg buffer.
		const verifyBlock = (block: BlockInfo) =>
			Effect.gen(function* () {
				yield* ensureInitialized

				const buffer = yield* Ref.get(blockHashBuffer)

				if (block.number > 0n) {
					const prevHash = buffer.get(block.number - 1n)
					if (prevHash !== undefined && prevHash !== block.parentHash) {
						yield* Effect.logWarning("Parent hash mismatch").pipe(
							Effect.annotateLogs({
								block: block.number.toString(),
								expected: prevHash,
								actual: block.parentHash,
							}),
						)
						return yield* new ReorgDetected({
							forkBlock: block.number - 1n,
							expectedHash: prevHash,
							actualParentHash: block.parentHash,
						})
					}
				}

			yield* Ref.update(blockHashBuffer, buf => {
				const minBlock = block.number - BigInt(reorgDepth)
				const entries = Array.from(buf.entries()).filter(
					([k]) => k >= minBlock,
				)
				return new Map([...entries, [block.number, block.hash]])
			})

				yield* storage.insertBlockHash(block.number, block.hash)
			})

		// Drops forked history and rebuilds in-memory state from storage.
		const handleReorg = (forkBlock: bigint) =>
			Effect.gen(function* () {
				yield* storage.deleteEventsFrom(forkBlock)
				yield* storage.deleteBlockHashesFrom(forkBlock)
				const remaining = yield* storage.getRecentBlockHashes(reorgDepth)
				yield* Ref.set(
					blockHashBuffer,
					new Map(remaining.map(h => [h.blockNumber, h.blockHash])),
				)
				yield* Effect.logDebug("Reorg rollback complete").pipe(
					Effect.annotateLogs({
						fromBlock: forkBlock.toString(),
					}),
				)
			})

		return { verifyBlock, handleReorg }
	}),
)
