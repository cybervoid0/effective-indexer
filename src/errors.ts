import { Data } from "effect";

export class RpcError extends Data.TaggedError("RpcError")<{
	readonly reason: string;
	readonly method: string;
	readonly cause?: unknown;
}> {}

export class DecodeError extends Data.TaggedError("DecodeError")<{
	readonly reason: string;
	readonly log?: unknown;
	readonly cause?: unknown;
}> {}

export class StorageError extends Data.TaggedError("StorageError")<{
	readonly reason: string;
	readonly operation: string;
	readonly cause?: unknown;
}> {}

export class ReorgDetected extends Data.TaggedError("ReorgDetected")<{
	readonly forkBlock: bigint;
	readonly expectedHash: string;
	readonly actualParentHash: string;
}> {}

export class CheckpointError extends Data.TaggedError("CheckpointError")<{
	readonly reason: string;
	readonly cause?: unknown;
}> {}

export class ConfigError extends Data.TaggedError("ConfigError")<{
	readonly reason: string;
	readonly field?: string;
}> {}

export type IndexerError =
	| RpcError
	| DecodeError
	| StorageError
	| ReorgDetected
	| CheckpointError
	| ConfigError;
