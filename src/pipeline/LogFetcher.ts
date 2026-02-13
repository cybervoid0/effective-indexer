import { Duration, Effect, Ref, Schedule, Stream } from "effect"
import type { Abi } from "viem"
import { encodeEventTopics } from "viem"
import { Config } from "../config.js"
import { ConfigError, type RpcError } from "../errors.js"
import type { RawLog } from "../services/RpcProvider.js"
import { RpcProvider } from "../services/RpcProvider.js"

export const buildTopicFilterEffect = (
	abi: Abi,
	eventNames: readonly string[],
): Effect.Effect<readonly string[], ConfigError> =>
	Effect.forEach(eventNames, name =>
		Effect.gen(function* () {
			const abiEvent = abi.find(
				item => item.type === "event" && item.name === name,
			)
			if (!abiEvent || abiEvent.type !== "event") {
				return yield* Effect.fail(
					new ConfigError({
						reason: `Event "${name}" not found in ABI`,
						field: "contracts.events",
					}),
				)
			}
			const encoded = encodeEventTopics({ abi: [abiEvent], eventName: name })
			const topic = encoded[0]
			if (topic === undefined) {
				return yield* Effect.fail(
					new ConfigError({
						reason: `Failed to encode topic for event "${name}"`,
						field: "contracts.events",
					}),
				)
			}
			return topic
		}),
	)

export interface LogChunk {
	readonly logs: ReadonlyArray<RawLog>
	/** Upper block boundary of the chunk (inclusive). */
	readonly chunkEnd: bigint
}

/**
 * Fetches historical logs in deterministic chunk order with bounded concurrency.
 */
export const fetchLogs = (params: {
	readonly address: string
	readonly topics: readonly string[]
	readonly fromBlock: bigint
	readonly toBlock: bigint
}): Stream.Stream<LogChunk, RpcError, RpcProvider | Config> =>
	Stream.unwrap(
		Effect.gen(function* () {
			const config = yield* Config
			const rpc = yield* RpcProvider
			const chunkSize = BigInt(config.network.logs.chunkSize)
			const { baseDelayMs, maxDelayMs } = config.network.logs.retry
			const maxRetries = config.network.logs.maxRetries
			const concurrency = config.network.logs.parallelRequests

			const retrySchedule = Schedule.exponential(
				Duration.millis(baseDelayMs),
			).pipe(
				Schedule.delayed(d =>
					Duration.millis(Math.min(Duration.toMillis(d), maxDelayMs)),
				),
				Schedule.compose(Schedule.recurs(maxRetries)),
			)

			const span = params.toBlock - params.fromBlock + 1n
			const numChunks =
				span > 0n ? Number((span + chunkSize - 1n) / chunkSize) : 0
			const chunks = Array.from({ length: numChunks }, (_, i) => {
				const from = params.fromBlock + BigInt(i) * chunkSize
				const to =
					from + chunkSize - 1n > params.toBlock
						? params.toBlock
						: from + chunkSize - 1n
				return { from, to }
			})

			if (chunks.length === 0) {
				return Stream.empty
			}

			return Stream.fromIterable(chunks).pipe(
				Stream.mapEffect(
					chunk =>
						Effect.gen(function* () {
							const attempt = yield* Ref.make(0)
							const logs = yield* rpc
								.getLogs({
									address: params.address,
									topics: [params.topics],
									fromBlock: chunk.from,
									toBlock: chunk.to,
								})
								.pipe(
									// Retry policy is exponential and bounded by maxDelayMs.
									Effect.tapError(e =>
										Effect.gen(function* () {
											const n = yield* Ref.getAndUpdate(attempt, a => a + 1)
											const rawDelay = 2 ** n * baseDelayMs
											const delayMs = Math.min(rawDelay, maxDelayMs)
											yield* Effect.logDebug(
												"RPC getLogs failed, retrying",
											).pipe(
												Effect.annotateLogs({
													method: "eth_getLogs",
													from: chunk.from.toString(),
													to: chunk.to.toString(),
													reason: e.reason,
													attempt: (n + 1).toString(),
													maxRetries: maxRetries.toString(),
													delayMs: delayMs.toString(),
												}),
											)
										}),
									),
									Effect.retry(retrySchedule),
									Effect.tap(result =>
										Effect.logTrace("Logs fetched").pipe(
											Effect.annotateLogs({
												from: chunk.from.toString(),
												to: chunk.to.toString(),
												count: result.length.toString(),
											}),
										),
									),
								)
							return { logs, chunkEnd: chunk.to } satisfies LogChunk
						}),
					{ concurrency },
				),
			)
		}),
	)
