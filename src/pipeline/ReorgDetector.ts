import { Context, Effect, Layer, Ref } from "effect"
import { Config } from "../config.js"
import { ReorgDetected, type StorageError } from "../errors.js"
import type { BlockInfo } from "../services/RpcProvider.js"
import { Storage } from "../services/Storage.js"

export class ReorgDetector extends Context.Tag(
	"@rootstock/indexer/ReorgDetector",
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

export const ReorgDetectorLive = Layer.effect(
	ReorgDetector,
	Effect.gen(function* () {
		const storage = yield* Storage
		const config = yield* Config
		const reorgDepth = config.reorgDepth

		const blockHashBuffer = yield* Ref.make<Map<bigint, string>>(new Map())
		const initialized = yield* Ref.make(false)

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

		const verifyBlock = (block: BlockInfo) =>
			Effect.gen(function* () {
				yield* ensureInitialized

				const buffer = yield* Ref.get(blockHashBuffer)

				if (block.number > 0n) {
					const prevHash = buffer.get(block.number - 1n)
					if (prevHash !== undefined && prevHash !== block.parentHash) {
						return yield* new ReorgDetected({
							forkBlock: block.number - 1n,
							expectedHash: prevHash,
							actualParentHash: block.parentHash,
						})
					}
				}

				yield* Ref.update(blockHashBuffer, buf => {
					const newBuf = new Map(buf)
					newBuf.set(block.number, block.hash)
					const minBlock = block.number - BigInt(reorgDepth)
					for (const key of newBuf.keys()) {
						if (key < minBlock) newBuf.delete(key)
					}
					return newBuf
				})

				yield* storage.insertBlockHash(block.number, block.hash)
			})

		const handleReorg = (forkBlock: bigint) =>
			Effect.gen(function* () {
				yield* storage.deleteEventsFrom(forkBlock)
				yield* storage.deleteBlockHashesFrom(forkBlock)
				const remaining = yield* storage.getRecentBlockHashes(reorgDepth)
				yield* Ref.set(
					blockHashBuffer,
					new Map(remaining.map(h => [h.blockNumber, h.blockHash])),
				)
			})

		return { verifyBlock, handleReorg }
	}),
)
