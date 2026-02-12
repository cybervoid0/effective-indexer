import { Context, Effect, Layer } from "effect"
import { CheckpointError } from "../errors.js"
import type { Checkpoint } from "./Storage.js"
import { Storage } from "./Storage.js"

export { Checkpoint } from "./Storage.js"

/**
 * Manages per-contract indexing checkpoints.
 */
export class CheckpointManager extends Context.Tag(
	"effective-indexer/CheckpointManager",
)<
	CheckpointManager,
	{
		readonly load: (
			contractName: string,
		) => Effect.Effect<Checkpoint | null, CheckpointError>
		readonly save: (
			contractName: string,
			blockNumber: bigint,
			blockHash: string,
		) => Effect.Effect<void, CheckpointError>
		readonly getStartBlock: (
			contractName: string,
			configStartBlock: bigint,
		) => Effect.Effect<bigint, CheckpointError>
	}
>() {}

/**
 * Storage-backed checkpoint manager implementation.
 */
export const CheckpointManagerLive = Layer.effect(
	CheckpointManager,
	Effect.gen(function* () {
		const storage = yield* Storage

		const load = (contractName: string) =>
			storage
				.getCheckpoint(contractName)
				.pipe(
					Effect.mapError(
						e => new CheckpointError({ reason: e.reason, cause: e }),
					),
				)

		const save = (
			contractName: string,
			blockNumber: bigint,
			blockHash: string,
		) =>
			storage
				.saveCheckpoint(contractName, blockNumber, blockHash)
				.pipe(
					Effect.mapError(
						e => new CheckpointError({ reason: e.reason, cause: e }),
					),
				)

		const getStartBlock = (contractName: string, configStartBlock: bigint) =>
			Effect.gen(function* () {
				const cp = yield* load(contractName)
				return cp ? cp.lastBlock + 1n : configStartBlock
			})

		return { load, save, getStartBlock }
	}),
)
