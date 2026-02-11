import { Context, Duration, Effect, Layer } from "effect"
import type { StorageError } from "./errors.js"
import {
	type EventQuery,
	Storage,
	type StoredEvent,
} from "./services/Storage.js"

export interface ParsedEvent {
	readonly id: number
	readonly contractName: string
	readonly eventName: string
	readonly blockNumber: bigint
	readonly txHash: string
	readonly logIndex: number
	readonly timestamp: number | null
	readonly args: Record<string, unknown>
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
})

export class QueryApi extends Context.Tag("effective-indexer/QueryApi")<
	QueryApi,
	{
		readonly getEvents: (
			query?: EventQuery,
		) => Effect.Effect<ReadonlyArray<ParsedEvent>, StorageError>
		readonly getEventCount: (
			query?: EventQuery,
		) => Effect.Effect<number, StorageError>
		readonly getLatestBlock: (
			contractName: string,
		) => Effect.Effect<bigint | null, StorageError>
	}
>() {}

export const QueryApiLive = Layer.effect(
	QueryApi,
	Effect.gen(function* () {
		const storage = yield* Storage

		const serializeQuery = (q: EventQuery | undefined): string =>
			JSON.stringify(q ?? {}, (_, v) =>
				typeof v === "bigint" ? v.toString() : v,
			)

		const getEvents = (query?: EventQuery) =>
			storage.queryEvents(query ?? {}).pipe(
				Effect.map(rows => rows.map(parseStoredEvent)),
				Effect.timed,
				Effect.tap(([duration, results]) =>
					Effect.logDebug("Query executed").pipe(
						Effect.annotateLogs({
							resultCount: results.length.toString(),
							durationMs: Duration.toMillis(duration).toString(),
							filters: serializeQuery(query),
						}),
					),
				),
				Effect.map(([, results]) => results),
				Effect.withLogSpan("query"),
			)

		const getEventCount = (query?: EventQuery) =>
			storage.countEvents(query).pipe(
				Effect.timed,
				Effect.tap(([duration, count]) =>
					Effect.logDebug("Count executed").pipe(
						Effect.annotateLogs({
							count: count.toString(),
							durationMs: Duration.toMillis(duration).toString(),
							filters: serializeQuery(query),
						}),
					),
				),
				Effect.map(([, count]) => count),
				Effect.withLogSpan("count"),
			)

		const getLatestBlock = (contractName: string) =>
			storage
				.getCheckpoint(contractName)
				.pipe(Effect.map(cp => cp?.lastBlock ?? null))

		return { getEvents, getEventCount, getLatestBlock }
	}),
)
