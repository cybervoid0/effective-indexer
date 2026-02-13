import { mkdir } from "node:fs/promises"
import { dirname } from "node:path"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { Duration, Effect, Layer, ManagedRuntime } from "effect"
import { ConfigLive, type IndexerConfig, resolveConfig } from "./config.js"
import { LoggerLive } from "./logger.js"
import { BlockCursorLive } from "./pipeline/BlockCursor.js"
import { runIndexer } from "./pipeline/Indexer.js"
import { ReorgDetectorLive } from "./pipeline/ReorgDetector.js"
import { type ParsedEvent, QueryApi, QueryApiLive } from "./query.js"
import { CheckpointManagerLive } from "./services/Checkpoint.js"
import { EventDecoderLive } from "./services/EventDecoder.js"
import { ProgressRendererLive } from "./services/ProgressRenderer.js"
import { ProgressReporterLive } from "./services/ProgressReporter.js"
import { RpcProviderLive } from "./services/RpcProvider.js"
import type { EventQuery } from "./services/Storage.js"
import { StorageLive } from "./services/Storage.js"

export type {
	ContractConfig,
	IndexerConfig,
	LogsConfig,
	NetworkConfig,
	PollingConfig,
	ProgressConfig,
	ReorgConfig,
	ResolvedConfig,
	ResolvedLogsConfig,
	ResolvedNetworkConfig,
	ResolvedPollingConfig,
	ResolvedProgressConfig,
	ResolvedReorgConfig,
	ResolvedRetryConfig,
	ResolvedTelemetryConfig,
	RetryConfig,
	TelemetryConfig,
	WorkerAlertConfig,
	WorkerConfig,
	WorkerRecoveryConfig,
} from "./config.js"
export {
	Config,
	ConfigLive,
	defineIndexerConfig,
	resolveConfig,
	resolveConfigEffect,
} from "./config.js"
export {
	type ResolveIndexerConfigFromEnvOptions,
	resolveIndexerConfigFromEnv,
} from "./env-config.js"
export type {
	CheckpointError,
	ConfigError,
	DecodeError,
	IndexerError,
	ReorgDetected,
	RpcError,
	StorageError,
} from "./errors.js"
export { LoggerLive } from "./logger.js"
export { BlockCursor, BlockCursorLive } from "./pipeline/BlockCursor.js"
export { ReorgDetector, ReorgDetectorLive } from "./pipeline/ReorgDetector.js"
export type { ParsedEvent } from "./query.js"
export { QueryApi, QueryApiLive } from "./query.js"
export {
	CheckpointManager,
	CheckpointManagerLive,
} from "./services/Checkpoint.js"
export type { DecodedEvent } from "./services/EventDecoder.js"
export { EventDecoder, EventDecoderLive } from "./services/EventDecoder.js"
export {
	ProgressRenderer,
	ProgressRendererLive,
} from "./services/ProgressRenderer.js"
export type {
	BackfillProgress,
	ProgressSnapshot,
} from "./services/ProgressReporter.js"
export {
	computeSnapshot,
	ProgressReporter,
	ProgressReporterLive,
} from "./services/ProgressReporter.js"
export { RpcProvider, RpcProviderLive } from "./services/RpcProvider.js"
export type { EventQuery } from "./services/Storage.js"
export { Storage, StorageLive } from "./services/Storage.js"

export interface IndexerHandle {
	readonly start: () => Promise<void>
	readonly stop: () => Promise<void>
	readonly waitForExit: () => Promise<void>
	readonly query: (q?: EventQuery) => Promise<ReadonlyArray<ParsedEvent>>
	readonly count: (q?: EventQuery) => Promise<number>
}

interface WorkerProcess {
	on(event: NodeJS.Signals, listener: () => void): unknown
	off(event: NodeJS.Signals, listener: () => void): unknown
}

export interface WorkerRuntime {
	readonly process: WorkerProcess
}

export interface RunIndexerWorkerOptions {
	readonly ensureDbDirectory?: boolean | undefined
	readonly shutdownSignals?: readonly NodeJS.Signals[] | undefined
	readonly recovery?:
		| {
				readonly enabled?: boolean | undefined
				readonly maxRecoveryDurationMs?: number | undefined
				readonly initialRetryDelayMs?: number | undefined
				readonly maxRetryDelayMs?: number | undefined
				readonly backoffFactor?: number | undefined
		  }
		| undefined
	readonly onRecoveryFailure?:
		| ((notification: WorkerFailureNotification) => Promise<void> | void)
		| undefined
	readonly createIndexer?:
		| ((config: IndexerConfig) => IndexerHandle)
		| undefined
	readonly runtime?: WorkerRuntime | undefined
}

export interface WorkerFailureNotification {
	readonly attempts: number
	readonly recoveryDurationMs: number
	readonly error: unknown
	readonly timestamp: string
}

export const createWebhookNotifier =
	(
		webhookUrl: string,
		init?: {
			readonly headers?: Readonly<Record<string, string>> | undefined
		},
	) =>
	async (notification: WorkerFailureNotification): Promise<void> => {
		const response = await fetch(webhookUrl, {
			method: "POST",
			headers: {
				"content-type": "application/json",
				...(init?.headers ?? {}),
			},
			body: JSON.stringify(notification),
		})
		if (!response.ok) {
			throw new Error(
				`Notification webhook failed with status ${response.status}`,
			)
		}
	}

const buildLayers = (config: IndexerConfig) => {
	const resolved = resolveConfig(config)
	const ConfigLayer = ConfigLive(config)
	const SqliteLayer = SqliteClient.layer({
		filename: config.dbPath ?? "./indexer.db",
	})
	const LoggerLayer = LoggerLive(resolved)

	// Foundation: resolved config + sqlite client.
	const FoundationLayer = Layer.merge(ConfigLayer, SqliteLayer)

	// Storage depends on SqlClient (from SqliteLayer)
	const StorageLayer = StorageLive.pipe(Layer.provide(FoundationLayer))

	// RpcProvider depends on Config
	const RpcLayer = RpcProviderLive.pipe(Layer.provide(ConfigLayer))

	// Pure decoder, no runtime dependencies.
	const DecoderLayer = EventDecoderLive

	// Checkpoint depends on Storage
	const CheckpointLayer = CheckpointManagerLive.pipe(
		Layer.provide(StorageLayer),
	)

	// BlockCursor depends on RpcProvider + Config
	const CursorLayer = BlockCursorLive.pipe(
		Layer.provide(Layer.merge(RpcLayer, ConfigLayer)),
	)

	// ReorgDetector depends on Storage + Config
	const ReorgLayer = ReorgDetectorLive.pipe(
		Layer.provide(Layer.merge(StorageLayer, ConfigLayer)),
	)

	// QueryApi depends on Storage
	const QueryLayer = QueryApiLive.pipe(Layer.provide(StorageLayer))

	// In-memory progress tracker.
	const ProgressReporterLayer = ProgressReporterLive

	// ProgressRenderer depends on Config + ProgressReporter
	const ProgressRendererLayer = ProgressRendererLive.pipe(
		Layer.provide(Layer.merge(ConfigLayer, ProgressReporterLayer)),
	)

	return Layer.mergeAll(
		ConfigLayer,
		StorageLayer,
		RpcLayer,
		DecoderLayer,
		CheckpointLayer,
		CursorLayer,
		ReorgLayer,
		QueryLayer,
		ProgressReporterLayer,
		ProgressRendererLayer,
		LoggerLayer,
	)
}

export const createIndexer = (config: IndexerConfig): IndexerHandle => {
	const ServicesLive = buildLayers(config)
	let abortController: AbortController | null = null
	const runtime = ManagedRuntime.make(ServicesLive)
	let runningPromise: Promise<void> | null = null
	let stopPromise: Promise<void> | null = null
	let disposed = false

	return {
		start: async () => {
			if (runningPromise !== null) {
				return
			}
			const controller = new AbortController()
			abortController = controller
			runningPromise = runtime.runPromise(runIndexer, {
				signal: controller.signal,
			})
			// Prevent unhandled rejections while keeping failures visible.
			runningPromise.catch(error => {
				if (!controller.signal.aborted) {
					console.error("Indexer background worker failed:", error)
				}
			})
		},

		stop: async () => {
			if (disposed) {
				return
			}
			if (stopPromise !== null) {
				return stopPromise
			}
			stopPromise = (async () => {
				abortController?.abort()
				const wasAborted = abortController?.signal.aborted ?? false
				if (runningPromise !== null) {
					try {
						await runningPromise
					} catch (error) {
						if (!wasAborted) {
							throw error
						}
					} finally {
						runningPromise = null
					}
				}
				await runtime.dispose()
				abortController = null
				disposed = true
			})()

			try {
				await stopPromise
			} finally {
				stopPromise = null
			}
		},

		waitForExit: async () => {
			if (runningPromise === null) {
				return
			}
			await runningPromise
		},

		query: async q => {
			return runtime.runPromise(
				Effect.gen(function* () {
					const queryApi = yield* QueryApi
					return yield* queryApi.getEvents(q)
				}),
			)
		},

		count: async q => {
			return runtime.runPromise(
				Effect.gen(function* () {
					const queryApi = yield* QueryApi
					return yield* queryApi.getEventCount(q)
				}),
			)
		},
	}
}

const defaultWorkerRuntime: WorkerRuntime = {
	process,
}

const resolveDbPath = (config: IndexerConfig): string =>
	config.dbPath ?? "./indexer.db"

const ensureDbDirectory = async (config: IndexerConfig): Promise<void> => {
	const dbPath = resolveDbPath(config)
	if (dbPath === ":memory:") {
		return
	}
	await mkdir(dirname(dbPath), { recursive: true })
}

/**
 * Runs a long-lived worker process with graceful shutdown and DB bootstrap.
 */
export const runIndexerWorker = async (
	config: IndexerConfig,
	options?: RunIndexerWorkerOptions,
): Promise<void> => {
	if (options?.ensureDbDirectory ?? true) {
		await ensureDbDirectory(config)
	}

	const runtime = options?.runtime ?? defaultWorkerRuntime
	const create = options?.createIndexer ?? createIndexer
	const signals = options?.shutdownSignals ?? ["SIGINT", "SIGTERM"]
	const webhookUrl = config.worker?.alert?.webhookUrl
	const configNotifier =
		webhookUrl && webhookUrl.length > 0
			? createWebhookNotifier(webhookUrl)
			: null
	const onRecoveryFailure =
		options?.onRecoveryFailure ?? configNotifier ?? undefined
	const recovery = {
		enabled:
			options?.recovery?.enabled ?? config.worker?.recovery?.enabled ?? true,
		maxRecoveryDurationMs:
			options?.recovery?.maxRecoveryDurationMs ??
			config.worker?.recovery?.maxRecoveryDurationMs ??
			15 * 60 * 1000,
		initialRetryDelayMs:
			options?.recovery?.initialRetryDelayMs ??
			config.worker?.recovery?.initialRetryDelayMs ??
			1000,
		maxRetryDelayMs:
			options?.recovery?.maxRetryDelayMs ??
			config.worker?.recovery?.maxRetryDelayMs ??
			30_000,
		backoffFactor:
			options?.recovery?.backoffFactor ??
			config.worker?.recovery?.backoffFactor ??
			2,
	}

	const signalHandlers = new Map<NodeJS.Signals, () => void>()
	let stopRequested = false
	let activeIndexer: IndexerHandle | null = null

	const stopSignalPromise = new Promise<void>(resolve => {
		for (const signal of signals) {
			const handler = () => {
				stopRequested = true
				resolve()
			}
			signalHandlers.set(signal, handler)
			runtime.process.on(signal, handler)
		}
	})

	const cleanup = () => {
		for (const signal of signals) {
			const handler = signalHandlers.get(signal)
			if (handler !== undefined) {
				runtime.process.off(signal, handler)
			}
		}
	}

	const stopActiveIndexer = async () => {
		if (activeIndexer !== null) {
			try {
				await activeIndexer.stop()
			} finally {
				activeIndexer = null
			}
		}
	}

	let firstFailureAt: number | null = null
	let attempts = 0

	try {
		while (!stopRequested) {
			activeIndexer = create(config)
			try {
				await activeIndexer.start()
				await Promise.race([stopSignalPromise, activeIndexer.waitForExit()])
				if (stopRequested) {
					break
				}
				throw new Error("Indexer worker exited unexpectedly")
			} catch (error) {
				if (stopRequested) {
					break
				}

				attempts += 1
				firstFailureAt = firstFailureAt ?? Date.now()
				const recoveryDurationMs = Date.now() - firstFailureAt
				await stopActiveIndexer()

				if (
					!recovery.enabled ||
					recoveryDurationMs >= recovery.maxRecoveryDurationMs
				) {
					if (onRecoveryFailure) {
						await onRecoveryFailure({
							attempts,
							recoveryDurationMs,
							error,
							timestamp: new Date().toISOString(),
						})
					}
					throw error
				}

				const delay = Math.min(
					recovery.initialRetryDelayMs *
						recovery.backoffFactor ** Math.max(attempts - 1, 0),
					recovery.maxRetryDelayMs,
				)
				console.error(
					`Indexer worker crashed, retrying in ${delay}ms (attempt ${attempts})`,
					error,
				)
				await Effect.runPromise(Effect.sleep(Duration.millis(delay)))
			}
		}
	} finally {
		await stopActiveIndexer()
		cleanup()
	}
}

export const Indexer = {
	create: createIndexer,
} as const
