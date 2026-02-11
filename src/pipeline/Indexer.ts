import { Effect, Stream } from "effect"
import { Config, type ContractConfig } from "../config.js"
import type { IndexerError } from "../errors.js"
import { CheckpointManager } from "../services/Checkpoint.js"
import { type DecodedEvent, EventDecoder } from "../services/EventDecoder.js"
import { RpcProvider } from "../services/RpcProvider.js"
import { Storage } from "../services/Storage.js"
import { BlockCursor } from "./BlockCursor.js"
import { buildTopicFilter, fetchLogs } from "./LogFetcher.js"
import { ReorgDetector } from "./ReorgDetector.js"

type IndexerDeps =
	| Config
	| Storage
	| RpcProvider
	| EventDecoder
	| CheckpointManager
	| BlockCursor
	| ReorgDetector

const indexContract = (
	contract: ContractConfig,
): Stream.Stream<DecodedEvent, IndexerError, IndexerDeps> =>
	Stream.unwrap(
		Effect.gen(function* () {
			const rpc = yield* RpcProvider
			const decoder = yield* EventDecoder
			const checkpoint = yield* CheckpointManager
			const reorgDetector = yield* ReorgDetector
			const storage = yield* Storage
			const blockCursor = yield* BlockCursor

			const startBlock = yield* checkpoint.getStartBlock(
				contract.name,
				contract.startBlock ?? 0n,
			)
			const currentHead = yield* rpc.getBlockNumber

			yield* Effect.log("Backfill starting").pipe(
				Effect.annotateLogs({
					fromBlock: startBlock.toString(),
					toBlock: currentHead.toString(),
				}),
			)

			const topics = buildTopicFilter(contract.abi, contract.events)

			// --- Phase 1: Historical Backfill ---
			const backfillStream: Stream.Stream<
				DecodedEvent,
				IndexerError,
				IndexerDeps
			> =
				startBlock <= currentHead
					? fetchLogs({
							address: contract.address,
							topics,
							fromBlock: startBlock,
							toBlock: currentHead,
						}).pipe(
							Stream.mapEffect(rawLogs =>
								Effect.gen(function* () {
									if (rawLogs.length === 0) return [] as DecodedEvent[]

									const decoded = yield* decoder.decodeBatch(
										contract.name,
										contract.abi,
										rawLogs,
									)

									// Get block info for timestamps and reorg verification
									const blockNumbers = [
										...new Set(decoded.map(d => d.blockNumber)),
									]
									const withTimestamp: DecodedEvent[] = []

									for (const bn of blockNumbers) {
										const blockInfo = yield* rpc.getBlock(bn)
										yield* reorgDetector.verifyBlock(blockInfo)

										for (const event of decoded) {
											if (event.blockNumber === bn) {
												withTimestamp.push({
													...event,
													timestamp: blockInfo.timestamp,
												})
											}
										}
									}

									yield* storage.insertEvents(
										withTimestamp.map(e => ({
											contractName: e.contractName,
											eventName: e.eventName,
											blockNumber: e.blockNumber,
											txHash: e.txHash,
											logIndex: e.logIndex,
											timestamp: e.timestamp,
											args: e.args,
										})),
									)

									const lastBlock = withTimestamp[withTimestamp.length - 1]
									if (lastBlock) {
										yield* checkpoint.save(
											contract.name,
											lastBlock.blockNumber,
											lastBlock.blockHash,
										)
									}

									yield* Effect.logDebug("Chunk indexed").pipe(
										Effect.annotateLogs({
											events: withTimestamp.length.toString(),
											checkpoint: lastBlock
												? lastBlock.blockNumber.toString()
												: "none",
										}),
									)

									return withTimestamp
								}).pipe(Effect.withLogSpan("backfill_chunk")),
							),
							Stream.flatMap(Stream.fromIterable),
						)
					: Stream.empty

			// --- Phase 2: Live Polling ---
			const liveStream: Stream.Stream<DecodedEvent, IndexerError, never> =
				blockCursor.liveBlocks.pipe(
					Stream.mapEffect(blockNumber =>
						Effect.gen(function* () {
							const blockInfo = yield* rpc.getBlock(blockNumber)

							const reorgResult = yield* Effect.either(
								reorgDetector.verifyBlock(blockInfo),
							)

							if (reorgResult._tag === "Left") {
								const err = reorgResult.left
								if (err._tag === "ReorgDetected") {
									yield* Effect.logWarning("Reorg detected").pipe(
										Effect.annotateLogs({
											forkBlock: err.forkBlock.toString(),
											expectedHash: err.expectedHash,
											actualParentHash: err.actualParentHash,
										}),
									)

									yield* reorgDetector.handleReorg(err.forkBlock)
									const rewindBlock =
										err.forkBlock > 0n ? err.forkBlock - 1n : 0n
									const rewindHash =
										(yield* storage.getBlockHash(rewindBlock)) ??
										blockInfo.parentHash
									yield* checkpoint.save(contract.name, rewindBlock, rewindHash)

									yield* Effect.log("Reorg handled").pipe(
										Effect.annotateLogs({
											rewindTo: rewindBlock.toString(),
										}),
									)
									// Skip this block. Next polls re-index from rewritten checkpoint.
									return [] as DecodedEvent[]
								}
								return yield* Effect.fail(err)
							}

							const rawLogs = yield* rpc.getLogs({
								address: contract.address,
								topics: [topics],
								fromBlock: blockNumber,
								toBlock: blockNumber,
							})

							const decoded = yield* decoder.decodeBatch(
								contract.name,
								contract.abi,
								rawLogs,
							)

							const withTimestamp = decoded.map(d => ({
								...d,
								timestamp: blockInfo.timestamp,
							}))

							yield* storage.insertEvents(
								withTimestamp.map(e => ({
									contractName: e.contractName,
									eventName: e.eventName,
									blockNumber: e.blockNumber,
									txHash: e.txHash,
									logIndex: e.logIndex,
									timestamp: e.timestamp,
									args: e.args,
								})),
							)

							yield* checkpoint.save(contract.name, blockNumber, blockInfo.hash)

							yield* Effect.logDebug("Block indexed").pipe(
								Effect.annotateLogs({
									block: blockNumber.toString(),
									events: withTimestamp.length.toString(),
								}),
							)

							return withTimestamp
						}).pipe(Effect.withLogSpan("live_block")),
					),
					Stream.flatMap(Stream.fromIterable),
				)

			return Stream.concat(
				backfillStream,
				Stream.concat(
					Stream.execute(Effect.log("Backfill complete, switching to live")),
					liveStream,
				),
			)
		}).pipe(Effect.annotateLogs("contract", contract.name)),
	)

export const runIndexer: Effect.Effect<void, IndexerError, IndexerDeps> =
	Effect.gen(function* () {
		const config = yield* Config
		const storage = yield* Storage

		yield* storage.initialize.pipe(Effect.withLogSpan("storage_init"))

		yield* Effect.log("Indexer starting").pipe(
			Effect.annotateLogs({
				contracts: config.contracts.length.toString(),
			}),
		)

		const streams = config.contracts.map(c => indexContract(c))

		yield* Stream.mergeAll(streams, { concurrency: streams.length }).pipe(
			Stream.runDrain,
			Effect.tapError(err =>
				Effect.logError("Indexer error").pipe(
					Effect.annotateLogs({
						errorTag: err._tag,
						reason: "reason" in err ? err.reason : String(err),
						...("method" in err ? { method: err.method } : {}),
						...("operation" in err ? { operation: err.operation } : {}),
					}),
				),
			),
		)

		yield* Effect.log("Indexer stopped")
	})
