import { Effect, Ref, Schedule, Stream } from "effect"
import type { Abi } from "viem"
import { encodeEventTopics } from "viem"
import { Config } from "../config.js"
import type { RpcError } from "../errors.js"
import type { RawLog } from "../services/RpcProvider.js"
import { RpcProvider } from "../services/RpcProvider.js"

export const buildTopicFilter = (
	abi: Abi,
	eventNames: readonly string[],
): readonly string[] => {
	const topics: string[] = []
	for (const name of eventNames) {
		const abiEvent = abi.find(
			item => item.type === "event" && item.name === name,
		)
		if (!abiEvent || abiEvent.type !== "event") {
			throw new Error(`Event "${name}" not found in ABI`)
		}
		const encoded = encodeEventTopics({ abi: [abiEvent], eventName: name })
		if (encoded[0]) {
			topics.push(encoded[0])
		}
	}
	return topics
}

export const fetchLogs = (params: {
	readonly address: string
	readonly topics: readonly string[]
	readonly fromBlock: bigint
	readonly toBlock: bigint
}): Stream.Stream<ReadonlyArray<RawLog>, RpcError, RpcProvider | Config> =>
	Stream.unwrap(
		Effect.gen(function* () {
			const config = yield* Config
			const rpc = yield* RpcProvider
			const chunkSize = BigInt(config.chunkSize)

			const chunks: Array<{ from: bigint; to: bigint }> = []
			let cursor = params.fromBlock
			while (cursor <= params.toBlock) {
				const end =
					cursor + chunkSize - 1n > params.toBlock
						? params.toBlock
						: cursor + chunkSize - 1n
				chunks.push({ from: cursor, to: end })
				cursor = end + 1n
			}

			if (chunks.length === 0) {
				return Stream.empty
			}

			return Stream.fromIterable(chunks).pipe(
				Stream.mapEffect(chunk =>
					Effect.gen(function* () {
						const attempt = yield* Ref.make(0)
						return yield* rpc
							.getLogs({
								address: params.address,
								topics: [params.topics],
								fromBlock: chunk.from,
								toBlock: chunk.to,
							})
							.pipe(
								Effect.tapError(e =>
									Effect.gen(function* () {
										const n = yield* Ref.getAndUpdate(
											attempt,
											a => a + 1,
										)
										const delayMs = Math.pow(2, n) * 1000
										yield* Effect.logDebug(
											"RPC getLogs failed, retrying",
										).pipe(
											Effect.annotateLogs({
												method: "eth_getLogs",
												from: chunk.from.toString(),
												to: chunk.to.toString(),
												reason: e.reason,
												attempt: (n + 1).toString(),
												maxRetries: config.maxRetries.toString(),
												delayMs: delayMs.toString(),
											}),
										)
									}),
								),
								Effect.retry(
									Schedule.exponential("1 second").pipe(
										Schedule.compose(
											Schedule.recurs(config.maxRetries),
										),
									),
								),
								Effect.tap(logs =>
									Effect.logTrace("Logs fetched").pipe(
										Effect.annotateLogs({
											from: chunk.from.toString(),
											to: chunk.to.toString(),
											count: logs.length.toString(),
										}),
									),
								),
							)
					}),
				),
			)
		}),
	)
