import { SqliteClient } from "@effect/sql-sqlite-node";
import { Effect, Layer, ManagedRuntime } from "effect";
import { ConfigLive, type IndexerConfig } from "./config.js";
import { BlockCursorLive } from "./pipeline/BlockCursor.js";
import { runIndexer } from "./pipeline/Indexer.js";
import { ReorgDetectorLive } from "./pipeline/ReorgDetector.js";
import { type ParsedEvent, QueryApi, QueryApiLive } from "./query.js";
import { CheckpointManagerLive } from "./services/Checkpoint.js";
import { EventDecoderLive } from "./services/EventDecoder.js";
import { RpcProviderLive } from "./services/RpcProvider.js";
import type { EventQuery } from "./services/Storage.js";
import { StorageLive } from "./services/Storage.js";

export type {
	ContractConfig,
	IndexerConfig,
	ResolvedConfig,
} from "./config.js";
export { Config, ConfigLive } from "./config.js";
export type {
	CheckpointError,
	ConfigError,
	DecodeError,
	IndexerError,
	ReorgDetected,
	RpcError,
	StorageError,
} from "./errors.js";
export { BlockCursor, BlockCursorLive } from "./pipeline/BlockCursor.js";
export { ReorgDetector, ReorgDetectorLive } from "./pipeline/ReorgDetector.js";
export type { ParsedEvent } from "./query.js";
export { QueryApi, QueryApiLive } from "./query.js";
export {
	CheckpointManager,
	CheckpointManagerLive,
} from "./services/Checkpoint.js";
export type { DecodedEvent } from "./services/EventDecoder.js";
export { EventDecoder, EventDecoderLive } from "./services/EventDecoder.js";
export { RpcProvider, RpcProviderLive } from "./services/RpcProvider.js";
export type { EventQuery } from "./services/Storage.js";
export { Storage, StorageLive } from "./services/Storage.js";

export interface IndexerHandle {
	readonly start: () => Promise<void>;
	readonly stop: () => Promise<void>;
	readonly query: (q?: EventQuery) => Promise<ReadonlyArray<ParsedEvent>>;
	readonly count: (q?: EventQuery) => Promise<number>;
}

const buildLayers = (config: IndexerConfig) => {
	const ConfigLayer = ConfigLive(config);
	const SqliteLayer = SqliteClient.layer({
		filename: config.dbPath ?? "./indexer.db",
	});

	// Foundation: Config + SQLite
	const FoundationLayer = Layer.merge(ConfigLayer, SqliteLayer);

	// Storage depends on SqlClient (from SqliteLayer)
	const StorageLayer = StorageLive.pipe(Layer.provide(FoundationLayer));

	// RpcProvider depends on Config
	const RpcLayer = RpcProviderLive.pipe(Layer.provide(ConfigLayer));

	// EventDecoder has no dependencies
	const DecoderLayer = EventDecoderLive;

	// Checkpoint depends on Storage
	const CheckpointLayer = CheckpointManagerLive.pipe(
		Layer.provide(StorageLayer),
	);

	// BlockCursor depends on RpcProvider + Config
	const CursorLayer = BlockCursorLive.pipe(
		Layer.provide(Layer.merge(RpcLayer, ConfigLayer)),
	);

	// ReorgDetector depends on Storage + Config
	const ReorgLayer = ReorgDetectorLive.pipe(
		Layer.provide(Layer.merge(StorageLayer, ConfigLayer)),
	);

	// QueryApi depends on Storage
	const QueryLayer = QueryApiLive.pipe(Layer.provide(StorageLayer));

	return Layer.mergeAll(
		ConfigLayer,
		StorageLayer,
		RpcLayer,
		DecoderLayer,
		CheckpointLayer,
		CursorLayer,
		ReorgLayer,
		QueryLayer,
	);
};

export const createIndexer = (config: IndexerConfig): IndexerHandle => {
	const ServicesLive = buildLayers(config);
	let abortController: AbortController | null = null;
	const runtime = ManagedRuntime.make(ServicesLive);
	let runningPromise: Promise<void> | null = null;

	return {
		start: async () => {
			if (runningPromise !== null) {
				return;
			}
			const controller = new AbortController();
			abortController = controller;
			runningPromise = runtime.runPromise(runIndexer, {
				signal: controller.signal,
			});
			// Avoid unhandled rejections while still surfacing errors on stop().
			runningPromise.catch(() => undefined);
		},

		stop: async () => {
			abortController?.abort();
			const wasAborted = abortController?.signal.aborted ?? false;
			if (runningPromise !== null) {
				try {
					await runningPromise;
				} catch (error) {
					if (!wasAborted) {
						throw error;
					}
				} finally {
					runningPromise = null;
				}
			}
			await runtime.dispose();
			abortController = null;
		},

		query: async (q) => {
			return runtime.runPromise(
				Effect.gen(function* () {
					const queryApi = yield* QueryApi;
					return yield* queryApi.getEvents(q);
				}),
			);
		},

		count: async (q) => {
			return runtime.runPromise(
				Effect.gen(function* () {
					const queryApi = yield* QueryApi;
					return yield* queryApi.getEventCount(q);
				}),
			);
		},
	};
};

export const Indexer = {
	create: createIndexer,
} as const;
