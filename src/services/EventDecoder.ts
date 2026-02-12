import { Context, Effect, Layer } from "effect"
import type { Abi, Hex } from "viem"
import { decodeEventLog } from "viem"
import { DecodeError } from "../errors.js"
import type { RawLog } from "./RpcProvider.js"

/**
 * Decoded and normalized contract event.
 */
export interface DecodedEvent {
	readonly contractName: string
	readonly eventName: string
	readonly blockNumber: bigint
	readonly txHash: string
	readonly logIndex: number
	readonly blockHash: string
	readonly timestamp: bigint | null
	readonly args: Record<string, unknown>
}

/**
 * Decodes raw RPC logs into typed event payloads.
 */
export class EventDecoder extends Context.Tag("effective-indexer/EventDecoder")<
	EventDecoder,
	{
		readonly decode: (
			contractName: string,
			abi: Abi,
			log: RawLog,
		) => Effect.Effect<DecodedEvent | null, DecodeError>
		readonly decodeBatch: (
			contractName: string,
			abi: Abi,
			logs: ReadonlyArray<RawLog>,
		) => Effect.Effect<ReadonlyArray<DecodedEvent>, DecodeError>
	}
>() {}

const decodeLog = (
	contractName: string,
	abi: Abi,
	log: RawLog,
): DecodedEvent | null => {
	try {
		const decoded = decodeEventLog({
			abi,
			data: log.data as Hex,
			topics: log.topics as [Hex, ...Hex[]],
			strict: false,
		})
		const eventName = decoded.eventName as string | undefined
		if (!eventName) return null
		return {
			contractName,
			eventName,
			blockNumber: log.blockNumber,
			txHash: log.transactionHash,
			logIndex: log.logIndex,
			blockHash: log.blockHash,
			timestamp: null,
			args: (decoded.args ?? {}) as Record<string, unknown>,
		}
	} catch {
		return null
	}
}

/**
 * Stateless decoder implementation.
 */
export const EventDecoderLive = Layer.succeed(EventDecoder, {
	decode: (contractName: string, abi: Abi, log: RawLog) =>
		Effect.try({
			try: () => decodeLog(contractName, abi, log),
			catch: e => new DecodeError({ reason: String(e), log, cause: e }),
		}),

	decodeBatch: (contractName: string, abi: Abi, logs: ReadonlyArray<RawLog>) =>
		Effect.forEach(logs, log =>
			Effect.gen(function* () {
				const decoded = decodeLog(contractName, abi, log)
				if (decoded === null) {
					return yield* Effect.fail(
						new DecodeError({
							reason: "Failed to decode log with provided ABI",
							log,
						}),
					)
				}
				return decoded
			}),
		),
})
