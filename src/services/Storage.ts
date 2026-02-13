import { SqlClient } from "@effect/sql"
import type { SqlError } from "@effect/sql/SqlError"
import { Context, Effect, Layer } from "effect"
import { StorageError } from "../errors.js"

export interface StoredEvent {
	readonly id: number
	readonly contract_name: string
	readonly event_name: string
	readonly block_number: number
	readonly tx_hash: string
	readonly log_index: number
	readonly timestamp: number | null
	readonly args: string
}

export interface StoredCheckpoint {
	readonly contract_name: string
	readonly last_block: number
	readonly last_block_hash: string
	readonly updated_at: string
}

export interface StoredBlockHash {
	readonly block_number: number
	readonly block_hash: string
}

/**
 * Insert payload for a decoded blockchain event.
 */
export interface InsertEvent {
	readonly contractName: string
	readonly eventName: string
	readonly blockNumber: bigint
	readonly txHash: string
	readonly logIndex: number
	readonly timestamp: bigint | null
	readonly args: Record<string, unknown>
}

/**
 * Query filters and pagination for reading indexed events.
 */
export interface EventQuery {
	readonly contractName?: string | undefined
	readonly eventName?: string | undefined
	readonly fromBlock?: bigint | undefined
	readonly toBlock?: bigint | undefined
	readonly txHash?: string | undefined
	readonly limit?: number | undefined
	readonly offset?: number | undefined
	readonly order?: "asc" | "desc" | undefined
}

export interface Checkpoint {
	readonly contractName: string
	readonly lastBlock: bigint
	readonly lastBlockHash: string
	readonly updatedAt: string
}

export interface BlockHashEntry {
	readonly blockNumber: bigint
	readonly blockHash: string
}

const wrapSqlError = (operation: string) => (e: SqlError) =>
	new StorageError({
		reason: e.message ?? "Unknown SQL error",
		operation,
		cause: e,
	})

/** Build WHERE clause and params from an EventQuery. */
const buildWhereClause = (
	query: EventQuery,
): { readonly where: string; readonly params: readonly unknown[] } => {
	const pairs: ReadonlyArray<[unknown, string]> = [
		[query.contractName, "contract_name = ?"],
		[query.eventName, "event_name = ?"],
		[
			query.fromBlock !== undefined ? Number(query.fromBlock) : undefined,
			"block_number >= ?",
		],
		[
			query.toBlock !== undefined ? Number(query.toBlock) : undefined,
			"block_number <= ?",
		],
		[query.txHash, "tx_hash = ?"],
	]
	const active = pairs.filter(([v]) => v !== undefined)
	const where =
		active.length > 0 ? `WHERE ${active.map(([, c]) => c).join(" AND ")}` : ""
	const params = active.map(([v]) => v)
	return { where, params }
}

const toJsonValue = (_key: string, value: unknown): unknown =>
	typeof value === "bigint" ? value.toString() : value

const INSERT_BATCH_SIZE = 250

const chunkEvents = (
	events: ReadonlyArray<InsertEvent>,
	size: number,
): ReadonlyArray<ReadonlyArray<InsertEvent>> =>
	Array.from({ length: Math.ceil(events.length / size) }, (_, i) =>
		events.slice(i * size, (i + 1) * size),
	)

/**
 * Persistence boundary for events, checkpoints, and block hashes.
 */
export class Storage extends Context.Tag("effective-indexer/Storage")<
	Storage,
	{
		readonly initialize: Effect.Effect<void, StorageError>
		readonly insertEvents: (
			events: ReadonlyArray<InsertEvent>,
		) => Effect.Effect<void, StorageError>
		readonly deleteEventsFrom: (
			blockNumber: bigint,
		) => Effect.Effect<void, StorageError>
		readonly queryEvents: (
			query: EventQuery,
		) => Effect.Effect<ReadonlyArray<StoredEvent>, StorageError>
		readonly countEvents: (
			query?: EventQuery,
		) => Effect.Effect<number, StorageError>
		readonly insertBlockHash: (
			blockNumber: bigint,
			blockHash: string,
		) => Effect.Effect<void, StorageError>
		readonly getBlockHash: (
			blockNumber: bigint,
		) => Effect.Effect<string | null, StorageError>
		readonly getRecentBlockHashes: (
			count: number,
		) => Effect.Effect<ReadonlyArray<BlockHashEntry>, StorageError>
		readonly deleteBlockHashesFrom: (
			blockNumber: bigint,
		) => Effect.Effect<void, StorageError>
		readonly getCheckpoint: (
			contractName: string,
		) => Effect.Effect<Checkpoint | null, StorageError>
		readonly saveCheckpoint: (
			contractName: string,
			blockNumber: bigint,
			blockHash: string,
		) => Effect.Effect<void, StorageError>
	}
>() {}

/**
 * SQLite-backed storage implementation.
 */
export const StorageLive = Layer.effect(
	Storage,
	Effect.gen(function* () {
		const sql = yield* SqlClient.SqlClient

		const initialize = Effect.gen(function* () {
			// WAL mode allows concurrent reads while the worker writes,
			// and provides better durability on unexpected process kill.
			yield* sql`PRAGMA journal_mode=WAL`
			// Keep schema bootstrap idempotent for repeated worker restarts.
			yield* sql`
        CREATE TABLE IF NOT EXISTS events (
          id            INTEGER PRIMARY KEY AUTOINCREMENT,
          contract_name TEXT NOT NULL,
          event_name    TEXT NOT NULL,
          block_number  INTEGER NOT NULL,
          tx_hash       TEXT NOT NULL,
          log_index     INTEGER NOT NULL,
          timestamp     INTEGER,
          args          TEXT NOT NULL
        )
      `
			yield* sql`CREATE UNIQUE INDEX IF NOT EXISTS idx_events_unique ON events(tx_hash, log_index)`
			yield* sql`CREATE INDEX IF NOT EXISTS idx_events_contract ON events(contract_name, event_name)`
			yield* sql`CREATE INDEX IF NOT EXISTS idx_events_block ON events(block_number)`

			yield* sql`
        CREATE TABLE IF NOT EXISTS checkpoints (
          contract_name  TEXT PRIMARY KEY,
          last_block     INTEGER NOT NULL,
          last_block_hash TEXT NOT NULL,
          updated_at     TEXT DEFAULT (datetime('now'))
        )
      `

			yield* sql`
        CREATE TABLE IF NOT EXISTS block_hashes (
          block_number INTEGER PRIMARY KEY,
          block_hash   TEXT NOT NULL
        )
      `
			yield* Effect.logDebug("Storage schema initialized")
		}).pipe(Effect.mapError(wrapSqlError("initialize")))

		const insertEvents = (events: ReadonlyArray<InsertEvent>) =>
			Effect.gen(function* () {
				if (events.length === 0) {
					return
				}
				yield* Effect.forEach(chunkEvents(events, INSERT_BATCH_SIZE), batch => {
					const placeholders = batch
						.map(() => "(?, ?, ?, ?, ?, ?, ?)")
						.join(", ")
					const params = batch.flatMap(event => {
						// JSON payload must be bigint-safe for EVM numeric fields.
						const argsJson = JSON.stringify(event.args, toJsonValue)
						const blockNum = Number(event.blockNumber)
						const ts = event.timestamp !== null ? Number(event.timestamp) : null
						return [
							event.contractName,
							event.eventName,
							blockNum,
							event.txHash,
							event.logIndex,
							ts,
							argsJson,
						]
					})
					return sql.unsafe(
						`INSERT OR IGNORE INTO events (contract_name, event_name, block_number, tx_hash, log_index, timestamp, args)
            VALUES ${placeholders}`,
						params,
					)
				})
			}).pipe(Effect.mapError(wrapSqlError("insertEvents")))

		const deleteEventsFrom = (blockNumber: bigint) =>
			sql`DELETE FROM events WHERE block_number >= ${Number(blockNumber)}`.pipe(
				Effect.asVoid,
				Effect.mapError(wrapSqlError("deleteEventsFrom")),
			)

		const queryEvents = (query: EventQuery) =>
			Effect.gen(function* () {
				const { where, params } = buildWhereClause(query)
				const order = query.order === "desc" ? "DESC" : "ASC"
				const limit = Math.max(1, Math.min(query.limit ?? 1000, 10_000))
				const offset = Math.max(0, query.offset ?? 0)

				const rows = yield* sql.unsafe<StoredEvent>(
					`SELECT * FROM events ${where} ORDER BY block_number ${order}, log_index ASC LIMIT ? OFFSET ?`,
					[...params, limit, offset],
				)
				return rows
			}).pipe(Effect.mapError(wrapSqlError("queryEvents")))

		const countEvents = (query?: EventQuery) =>
			Effect.gen(function* () {
				const { where, params } = buildWhereClause(query ?? {})
				// Bare COUNT(*) without WHERE is faster in SQLite.
				const rows =
					where === ""
						? yield* sql<{
								count: number
							}>`SELECT COUNT(*) as count FROM events`
						: yield* sql.unsafe<{ count: number }>(
								`SELECT COUNT(*) as count FROM events ${where}`,
								[...params],
							)
				return rows[0]?.count ?? 0
			}).pipe(Effect.mapError(wrapSqlError("countEvents")))

		const insertBlockHash = (blockNumber: bigint, blockHash: string) =>
			sql`
        INSERT OR REPLACE INTO block_hashes (block_number, block_hash)
        VALUES (${Number(blockNumber)}, ${blockHash})
      `.pipe(Effect.asVoid, Effect.mapError(wrapSqlError("insertBlockHash")))

		const getBlockHash = (blockNumber: bigint) =>
			sql<StoredBlockHash>`
        SELECT * FROM block_hashes WHERE block_number = ${Number(blockNumber)}
      `.pipe(
				Effect.map(rows => rows[0]?.block_hash ?? null),
				Effect.mapError(wrapSqlError("getBlockHash")),
			)

		const getRecentBlockHashes = (count: number) =>
			sql<StoredBlockHash>`
        SELECT * FROM block_hashes ORDER BY block_number DESC LIMIT ${count}
      `.pipe(
				Effect.map(rows =>
					rows.map(r => ({
						blockNumber: BigInt(r.block_number),
						blockHash: r.block_hash,
					})),
				),
				Effect.mapError(wrapSqlError("getRecentBlockHashes")),
			)

		const deleteBlockHashesFrom = (blockNumber: bigint) =>
			sql`DELETE FROM block_hashes WHERE block_number >= ${Number(blockNumber)}`.pipe(
				Effect.asVoid,
				Effect.mapError(wrapSqlError("deleteBlockHashesFrom")),
			)

		const getCheckpoint = (contractName: string) =>
			sql<StoredCheckpoint>`
        SELECT * FROM checkpoints WHERE contract_name = ${contractName}
      `.pipe(
				Effect.map(rows => {
					const row = rows[0]
					if (!row) return null
					return {
						contractName: row.contract_name,
						lastBlock: BigInt(row.last_block),
						lastBlockHash: row.last_block_hash,
						updatedAt: row.updated_at,
					} satisfies Checkpoint
				}),
				Effect.mapError(wrapSqlError("getCheckpoint")),
			)

		const saveCheckpoint = (
			contractName: string,
			blockNumber: bigint,
			blockHash: string,
		) =>
			sql`
        INSERT OR REPLACE INTO checkpoints (contract_name, last_block, last_block_hash, updated_at)
        VALUES (${contractName}, ${Number(blockNumber)}, ${blockHash}, datetime('now'))
      `.pipe(Effect.asVoid, Effect.mapError(wrapSqlError("saveCheckpoint")))

		return {
			initialize,
			insertEvents,
			deleteEventsFrom,
			queryEvents,
			countEvents,
			insertBlockHash,
			getBlockHash,
			getRecentBlockHashes,
			deleteBlockHashesFrom,
			getCheckpoint,
			saveCheckpoint,
		}
	}),
)
