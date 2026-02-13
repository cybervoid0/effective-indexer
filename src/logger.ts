import { Effect, Layer, Logger, LogLevel } from "effect"
import { Config } from "./config.js"

const parseLogLevel = (level: string): LogLevel.LogLevel => {
	switch (level) {
		case "trace":
			return LogLevel.Trace
		case "debug":
			return LogLevel.Debug
		case "info":
			return LogLevel.Info
		case "warning":
			return LogLevel.Warning
		case "error":
			return LogLevel.Error
		case "none":
			return LogLevel.None
		default:
			return LogLevel.Info
	}
}

const pickLoggerLayer = (format: string): Layer.Layer<never> => {
	switch (format) {
		case "json":
			return Logger.json
		case "structured":
			return Logger.structured
		default:
			return Logger.pretty
	}
}

/**
 * Logger layer derived from resolved Config — no sync escape hatches.
 */
export const LoggerLive: Layer.Layer<never, never, Config> = Layer.unwrapEffect(
	Effect.gen(function* () {
		const config = yield* Config
		const level = config.enableTelemetry
			? parseLogLevel(config.logLevel)
			: LogLevel.Error
		return Layer.merge(
			pickLoggerLayer(config.logFormat),
			Logger.minimumLogLevel(level),
		)
	}),
)
