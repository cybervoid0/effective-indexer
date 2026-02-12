import { Config, ConfigProvider, Effect, Option, Redacted } from "effect"
import type { IndexerConfig } from "./config.js"

export interface ResolveIndexerConfigFromEnvOptions {
	readonly env?: Readonly<Record<string, string | undefined>> | undefined
	readonly rpcUrlOverrideEnv?: string | undefined
	readonly sensitiveEnvNames?: readonly string[] | undefined
}

// Effect Config provider expects a flat map of string values.
const toConfigMap = (
	env: Readonly<Record<string, string | undefined>>,
): Map<string, string> => {
	const map = new Map<string, string>()
	for (const [key, value] of Object.entries(env)) {
		if (typeof value === "string") {
			map.set(key, value)
		}
	}
	return map
}

const getProvider = (
	env?: Readonly<Record<string, string | undefined>>,
): ConfigProvider.ConfigProvider =>
	ConfigProvider.fromMap(toConfigMap(env ?? process.env))

const readOptionalString = (
	name: string,
	provider: ConfigProvider.ConfigProvider,
): Option.Option<string> =>
	Effect.runSync(
		Effect.withConfigProvider(provider)(
			Config.option(Config.nonEmptyString(name)),
		),
	)

const readRequiredString = (
	name: string,
	provider: ConfigProvider.ConfigProvider,
): string => {
	try {
		return Effect.runSync(
			Effect.withConfigProvider(provider)(Config.nonEmptyString(name)),
		)
	} catch {
		throw new Error(`Missing required env var: ${name}`)
	}
}

const readRequiredRedactedString = (
	name: string,
	provider: ConfigProvider.ConfigProvider,
): string => {
	try {
		const redacted = Effect.runSync(
			Effect.withConfigProvider(provider)(
				Config.redacted(Config.nonEmptyString(name)),
			),
		)
		return Redacted.value(redacted)
	} catch {
		throw new Error(`Missing required env var: ${name}`)
	}
}

// Supports Hardhat-like placeholders, e.g. "{{EVM_RPC_API_KEY}}".
const resolveRpcUrl = (
	template: string,
	provider: ConfigProvider.ConfigProvider,
	sensitiveEnvNames: ReadonlySet<string>,
): string =>
	template.replace(/\{\{([A-Z0-9_]+)\}\}/g, (_input, envName: string) =>
		sensitiveEnvNames.has(envName)
			? readRequiredRedactedString(envName, provider)
			: readRequiredString(envName, provider),
	)

/**
 * Resolves runtime env placeholders while keeping the public API plain TypeScript.
 */
export const resolveIndexerConfigFromEnv = <T extends IndexerConfig>(
	config: T,
	options?: ResolveIndexerConfigFromEnvOptions,
): T => {
	const provider = getProvider(options?.env)
	const rpcUrlOverrideEnv = options?.rpcUrlOverrideEnv ?? "EVM_RPC_URL"
	const rpcUrlOverride = readOptionalString(rpcUrlOverrideEnv, provider)
	const sensitiveEnvNames = new Set(
		options?.sensitiveEnvNames ?? ["EVM_RPC_API_KEY"],
	)

	if (Option.isSome(rpcUrlOverride)) {
		return {
			...config,
			rpcUrl: rpcUrlOverride.value,
		}
	}

	return {
		...config,
		rpcUrl: resolveRpcUrl(config.rpcUrl, provider, sensitiveEnvNames),
	}
}
