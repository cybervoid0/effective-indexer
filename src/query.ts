import { Context, Effect, Layer } from "effect";
import type { StorageError } from "./errors.js";
import {
	type EventQuery,
	Storage,
	type StoredEvent,
} from "./services/Storage.js";

export interface ParsedEvent {
	readonly id: number;
	readonly contractName: string;
	readonly eventName: string;
	readonly blockNumber: bigint;
	readonly txHash: string;
	readonly logIndex: number;
	readonly timestamp: number | null;
	readonly args: Record<string, unknown>;
}

const parseStoredEvent = (row: StoredEvent): ParsedEvent => ({
	id: row.id,
	contractName: row.contract_name,
	eventName: row.event_name,
	blockNumber: BigInt(row.block_number),
	txHash: row.tx_hash,
	logIndex: row.log_index,
	timestamp: row.timestamp,
	args: JSON.parse(row.args) as Record<string, unknown>,
});

export class QueryApi extends Context.Tag("@rootstock/indexer/QueryApi")<
	QueryApi,
	{
		readonly getEvents: (
			query?: EventQuery,
		) => Effect.Effect<ReadonlyArray<ParsedEvent>, StorageError>;
		readonly getEventCount: (
			query?: EventQuery,
		) => Effect.Effect<number, StorageError>;
		readonly getLatestBlock: (
			contractName: string,
		) => Effect.Effect<bigint | null, StorageError>;
	}
>() {}

export const QueryApiLive = Layer.effect(
	QueryApi,
	Effect.gen(function* () {
		const storage = yield* Storage;

		const getEvents = (query?: EventQuery) =>
			storage
				.queryEvents(query ?? {})
				.pipe(Effect.map((rows) => rows.map(parseStoredEvent)));

		const getEventCount = (query?: EventQuery) => storage.countEvents(query);

		const getLatestBlock = (contractName: string) =>
			storage
				.getCheckpoint(contractName)
				.pipe(Effect.map((cp) => cp?.lastBlock ?? null));

		return { getEvents, getEventCount, getLatestBlock };
	}),
);
