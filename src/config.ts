import { Context, Layer } from "effect"
import type { Abi } from "viem"

export interface ContractConfig {
	readonly name: string
	readonly address: string
	readonly abi: Abi
	readonly events: readonly string[]
	readonly startBlock?: bigint | undefined
}

// --- Network config (user-facing, deeply optional) ---

export interface RetryConfig {
	readonly baseDelayMs?: number | undefined
	readonly maxDelayMs?: number | undefined
}

export interface PollingConfig {
	readonly intervalMs?: number | undefined
	readonly confirmations?: number | undefined
}

export interface LogsConfig {
	readonly chunkSize?: number | undefined
	readonly maxRetries?: number | undefined
	readonly retry?: RetryConfig | undefined
}

export interface ReorgConfig {
	readonly depth?: number | undefined
}

export interface NetworkConfig {
	readonly polling?: PollingConfig | undefined
	readonly logs?: LogsConfig | undefined
	readonly reorg?: ReorgConfig | undefined
}

// --- Resolved network config (all fields required) ---

export interface ResolvedRetryConfig {
	readonly baseDelayMs: number
	readonly maxDelayMs: number
}

export interface ResolvedPollingConfig {
	readonly intervalMs: number
	readonly confirmations: number
}

export interface ResolvedLogsConfig {
	readonly chunkSize: number
	readonly maxRetries: number
	readonly retry: ResolvedRetryConfig
}

export interface ResolvedReorgConfig {
	readonly depth: number
}

export interface ResolvedNetworkConfig {
	readonly polling: ResolvedPollingConfig
	readonly logs: ResolvedLogsConfig
	readonly reorg: ResolvedReorgConfig
}

// --- Top-level config ---

export interface IndexerConfig {
	readonly rpcUrl: string
	readonly dbPath?: string | undefined
	readonly contracts: readonly [ContractConfig, ...ContractConfig[]]
	readonly network?: NetworkConfig | undefined

	// Logging (not network-specific)
	readonly logLevel?:
		| "trace"
		| "debug"
		| "info"
		| "warning"
		| "error"
		| "none"
		| undefined
	readonly logFormat?: "pretty" | "json" | "structured" | undefined
	readonly enableTelemetry?: boolean | undefined
}

export interface ResolvedConfig {
	readonly rpcUrl: string
	readonly dbPath: string
	readonly contracts: readonly [ContractConfig, ...ContractConfig[]]
	readonly network: ResolvedNetworkConfig
	readonly logLevel: "trace" | "debug" | "info" | "warning" | "error" | "none"
	readonly logFormat: "pretty" | "json" | "structured"
	readonly enableTelemetry: boolean
}

const resolveNetwork = (config: IndexerConfig): ResolvedNetworkConfig => {
	const n = config.network
	return {
		polling: {
			intervalMs: n?.polling?.intervalMs ?? 12000,
			confirmations: n?.polling?.confirmations ?? 1,
		},
		logs: {
			chunkSize: n?.logs?.chunkSize ?? 5000,
			maxRetries: n?.logs?.maxRetries ?? 5,
			retry: {
				baseDelayMs: n?.logs?.retry?.baseDelayMs ?? 1000,
				maxDelayMs: n?.logs?.retry?.maxDelayMs ?? 30000,
			},
		},
		reorg: {
			depth: n?.reorg?.depth ?? 20,
		},
	}
}

export const resolveConfig = (config: IndexerConfig): ResolvedConfig => {
	const network = resolveNetwork(config)
	return {
		rpcUrl: config.rpcUrl,
		dbPath: config.dbPath ?? "./indexer.db",
		contracts: config.contracts,
		network,
		logLevel: config.logLevel ?? "info",
		logFormat: config.logFormat ?? "pretty",
		enableTelemetry: config.enableTelemetry ?? true,
	}
}

export class Config extends Context.Tag("effective-indexer/Config")<
	Config,
	ResolvedConfig
>() {}

export const ConfigLive = (raw: IndexerConfig): Layer.Layer<Config> =>
	Layer.succeed(Config, resolveConfig(raw))
