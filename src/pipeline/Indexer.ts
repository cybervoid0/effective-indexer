import { Duration, Effect, Ref, Schedule, Stream } from "effect"
import { Config, type ContractConfig } from "../config.js"
import type { IndexerError } from "../errors.js"
import { CheckpointManager } from "../services/Checkpoint.js"
import { type DecodedEvent, EventDecoder } from "../services/EventDecoder.js"
import { ProgressRenderer } from "../services/ProgressRenderer.js"
import { ProgressReporter } from "../services/ProgressReporter.js"
import { RpcProvider } from "../services/RpcProvider.js"
import { Storage } from "../services/Storage.js"
import { BlockCursor } from "./BlockCursor.js"
import {
	buildTopicFilterEffect,
	fetchLogs,
	type LogChunk,
} from "./LogFetcher.js"
import { ReorgDetector } from "./ReorgDetector.js"

type IndexerDeps =
	| Config
	| Storage
	| RpcProvider
	| EventDecoder
	| CheckpointManager
	| BlockCursor
	| ReorgDetector
	| ProgressReporter
	| ProgressRenderer

// One contract = one stream: backfill first, then switch to live mode.
const indexContract = (
	contract: ContractConfig,
): Stream.Stream<DecodedEvent, IndexerError, IndexerDeps> =>
	Stream.unwrap(
		Effect.gen(function* () {
			const config = yield* Config
			const rpc = yield* RpcProvider
			const decoder = yield* EventDecoder
			const checkpoint = yield* CheckpointManager
			const reorgDetector = yield* ReorgDetector
			const storage = yield* Storage
			const blockCursor = yield* BlockCursor
			const progress = yield* ProgressReporter
			const renderer = yield* ProgressRenderer
			const { baseDelayMs, maxDelayMs } = config.network.logs.retry
			const maxRetries = config.network.logs.maxRetries
			const blockRetrySchedule = Schedule.exponential(
				Duration.millis(baseDelayMs),
			).pipe(
				Schedule.delayed(duration =>
					Duration.millis(Math.min(Duration.toMillis(duration), maxDelayMs)),
				),
				Schedule.compose(Schedule.recurs(maxRetries)),
			)
			const getBlockWithRetry = (blockNumber: bigint) =>
				rpc.getBlock(blockNumber).pipe(
					Effect.tapError(err =>
						Effect.logDebug("RPC getBlock failed, retrying").pipe(
							Effect.annotateLogs({
								block: blockNumber.toString(),
								reason: err.reason,
								method: "eth_getBlockByNumber",
							}),
						),
					),
					Effect.retry(blockRetrySchedule),
				)

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

			const topics = yield* buildTopicFilterEffect(
				contract.abi,
				contract.events,
			)

			// Phase 1: historical catch-up to current head.
			const needsBackfill = startBlock <= currentHead
			const totalBackfillBlocks = currentHead - startBlock + 1n
			if (needsBackfill) {
				yield* progress.start(contract.name, totalBackfillBlocks)
			}
			const processedBlocksRef = yield* Ref.make(0n)

			const backfillStream: Stream.Stream<
				DecodedEvent,
				IndexerError,
				IndexerDeps
			> = needsBackfill
				? fetchLogs({
						address: contract.address,
						topics,
						fromBlock: startBlock,
						toBlock: currentHead,
					}).pipe(
						Stream.mapEffect((chunk: LogChunk) =>
							Effect.gen(function* () {
								const { logs: rawLogs, chunkEnd } = chunk
								// Cache block info fetched during event enrichment
								// so we can reuse it for checkpoint without an extra RPC call.
								const blockCache = new Map<
									bigint,
									{ hash: string; timestamp: bigint }
								>()

								const withTimestamp =
									rawLogs.length > 0
										? yield* Effect.gen(function* () {
												const decoded = yield* decoder.decodeBatch(
													contract.name,
													contract.abi,
													rawLogs,
												)

												const blockNumbers = [
													...new Set(decoded.map(d => d.blockNumber)),
												]

												// Fetch block info sequentially — reorg verification
												// depends on parent hash chain order.
												const enriched = yield* Effect.forEach(
													blockNumbers,
													bn =>
														Effect.gen(function* () {
															const blockInfo =
																yield* getBlockWithRetry(bn)
															blockCache.set(bn, blockInfo)
															yield* reorgDetector.verifyBlock(blockInfo)
															return decoded
																.filter(e => e.blockNumber === bn)
																.map(e => ({
																	...e,
																	timestamp: blockInfo.timestamp,
																}))
														}),
													{ concurrency: 1 },
												)

												const events = enriched.flat()

												yield* storage.insertEvents(
													events.map(e => ({
														contractName: e.contractName,
														eventName: e.eventName,
														blockNumber: e.blockNumber,
														txHash: e.txHash,
														logIndex: e.logIndex,
														timestamp: e.timestamp,
														args: e.args,
													})),
												)

												return events
											})
										: ([] as DecodedEvent[])

								// Always save checkpoint at chunk boundary so empty chunks
								// are not re-fetched on restart. Reuse cached block info
								// when chunkEnd was already fetched during event enrichment.
								const cached = blockCache.get(chunkEnd)
								const chunkEndHash = cached
									? cached.hash
									: (yield* getBlockWithRetry(chunkEnd)).hash
								yield* checkpoint.save(
									contract.name,
									chunkEnd,
									chunkEndHash,
								)

								// Keep progress monotonic even when a chunk yields zero events.
								const chunkSize = BigInt(config.network.logs.chunkSize)
								const lastProcessed = yield* Ref.modify(
									processedBlocksRef,
									current => {
										const advanced = current + chunkSize
										const next =
											advanced > totalBackfillBlocks
												? totalBackfillBlocks
												: advanced
										return [next, next] as const
									},
								)
								yield* progress.update(
									contract.name,
									lastProcessed,
									withTimestamp.length,
								)
								yield* progress.incrementChunks(contract.name)

								yield* Effect.logDebug("Chunk indexed").pipe(
									Effect.annotateLogs({
										events: withTimestamp.length.toString(),
										checkpoint: chunkEnd.toString(),
									}),
								)

								return withTimestamp
							}).pipe(Effect.withLogSpan("backfill_chunk")),
						),
						Stream.flatMap(Stream.fromIterable),
					)
				: Stream.empty

			// Phase 2: steady-state indexing for newly confirmed blocks.
			const liveStream: Stream.Stream<DecodedEvent, IndexerError, never> =
				blockCursor.liveBlocks.pipe(
					Stream.mapEffect(blockNumber =>
						Effect.gen(function* () {
							const blockInfo = yield* getBlockWithRetry(blockNumber)

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

			const backfillTransition = needsBackfill
				? Stream.execute(
						Effect.gen(function* () {
							const snapshot = yield* progress.getSnapshot(contract.name)
							if (snapshot) {
								yield* renderer.renderFinalSummary(snapshot, config)
							}
							yield* progress.finish(contract.name)
							yield* Effect.log("Backfill complete, switching to live")
						}),
					)
				: Stream.execute(Effect.log("Backfill complete, switching to live"))

			return Stream.concat(
				backfillStream,
				Stream.concat(backfillTransition, liveStream),
			)
		}).pipe(Effect.annotateLogs("contract", contract.name)),
	)

export const runIndexer: Effect.Effect<void, IndexerError, IndexerDeps> =
	Effect.gen(function* () {
		const config = yield* Config
		const storage = yield* Storage
		const renderer = yield* ProgressRenderer

		yield* storage.initialize.pipe(Effect.withLogSpan("storage_init"))

		yield* Effect.log("Indexer starting").pipe(
			Effect.annotateLogs({
				contracts: config.contracts.length.toString(),
			}),
		)

		yield* renderer.startRendering()

		// Contracts are indexed independently and merged concurrently.
		const streams = config.contracts.map(c => indexContract(c))

		yield* Stream.mergeAll(streams, { concurrency: streams.length }).pipe(
			Stream.runDrain,
			Effect.ensuring(renderer.stopRendering()),
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
