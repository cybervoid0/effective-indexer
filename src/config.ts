import { Context, Layer } from "effect"
import type { Abi } from "viem"

export interface ContractConfig {
	readonly name: string
	readonly address: string
	readonly abi: Abi
	readonly events: readonly string[]
	readonly startBlock?: bigint | undefined
}

export interface IndexerConfig {
	readonly rpcUrl: string
	readonly dbPath?: string | undefined
	readonly contracts: readonly [ContractConfig, ...ContractConfig[]]
	readonly chunkSize?: number | undefined
	readonly pollInterval?: number | undefined
	readonly confirmations?: number | undefined
	readonly maxRetries?: number | undefined
	readonly reorgDepth?: number | undefined
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
	readonly chunkSize: number
	readonly pollInterval: number
	readonly confirmations: number
	readonly maxRetries: number
	readonly reorgDepth: number
	readonly logLevel: "trace" | "debug" | "info" | "warning" | "error" | "none"
	readonly logFormat: "pretty" | "json" | "structured"
	readonly enableTelemetry: boolean
}

export const resolveConfig = (config: IndexerConfig): ResolvedConfig => ({
	rpcUrl: config.rpcUrl,
	dbPath: config.dbPath ?? "./indexer.db",
	contracts: config.contracts,
	chunkSize: config.chunkSize ?? 5000,
	pollInterval: config.pollInterval ?? 15000,
	confirmations: config.confirmations ?? 0,
	maxRetries: config.maxRetries ?? 5,
	reorgDepth: config.reorgDepth ?? 10,
	logLevel: config.logLevel ?? "info",
	logFormat: config.logFormat ?? "pretty",
	enableTelemetry: config.enableTelemetry ?? true,
})

export class Config extends Context.Tag("@rootstock/indexer/Config")<
	Config,
	ResolvedConfig
>() {}

export const ConfigLive = (raw: IndexerConfig): Layer.Layer<Config> =>
	Layer.succeed(Config, resolveConfig(raw))
