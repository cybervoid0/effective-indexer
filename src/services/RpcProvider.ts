import { Context, Effect, Layer } from "effect";
import type { Hex } from "viem";
import { createPublicClient, http } from "viem";
import { Config } from "../config.js";
import { RpcError } from "../errors.js";

export interface RawLog {
	readonly address: string;
	readonly topics: readonly string[];
	readonly data: string;
	readonly blockNumber: bigint;
	readonly transactionHash: string;
	readonly logIndex: number;
	readonly blockHash: string;
}

export interface BlockInfo {
	readonly number: bigint;
	readonly hash: string;
	readonly parentHash: string;
	readonly timestamp: bigint;
}

export interface GetLogsParams {
	readonly address: string | readonly string[];
	readonly topics: readonly (readonly string[])[];
	readonly fromBlock: bigint;
	readonly toBlock: bigint;
}

interface EthGetLogsResult {
	readonly address: string;
	readonly topics: readonly string[];
	readonly data: string;
	readonly blockNumber: Hex;
	readonly transactionHash: string;
	readonly logIndex: Hex;
	readonly blockHash: string;
}

const toHexQuantity = (value: bigint): Hex => `0x${value.toString(16)}` as Hex;

export class RpcProvider extends Context.Tag("@rootstock/indexer/RpcProvider")<
	RpcProvider,
	{
		readonly getBlockNumber: Effect.Effect<bigint, RpcError>;
		readonly getLogs: (
			params: GetLogsParams,
		) => Effect.Effect<ReadonlyArray<RawLog>, RpcError>;
		readonly getBlock: (
			blockNumber: bigint,
		) => Effect.Effect<BlockInfo, RpcError>;
	}
>() {}

export const RpcProviderLive = Layer.effect(
	RpcProvider,
	Effect.gen(function* () {
		const config = yield* Config;
		const client = createPublicClient({
			transport: http(config.rpcUrl),
		});

		const getBlockNumber = Effect.tryPromise({
			try: () => client.getBlockNumber(),
			catch: (e) =>
				new RpcError({
					reason: String(e),
					method: "eth_blockNumber",
					cause: e,
				}),
		});

		const getLogs = (params: GetLogsParams) =>
			Effect.tryPromise({
				try: () =>
					client.request({
						method: "eth_getLogs",
						params: [
							{
								address: params.address as Hex | Hex[],
								topics: params.topics.map(
									(topicGroup) => [...topicGroup] as Hex[],
								),
								fromBlock: toHexQuantity(params.fromBlock),
								toBlock: toHexQuantity(params.toBlock),
							},
						],
					}),
				catch: (e) =>
					new RpcError({ reason: String(e), method: "eth_getLogs", cause: e }),
			}).pipe(
				Effect.map((logs) =>
					(logs as ReadonlyArray<EthGetLogsResult>).map((log) => ({
						address: log.address,
						topics: log.topics,
						data: log.data,
						blockNumber: BigInt(log.blockNumber),
						transactionHash: log.transactionHash,
						logIndex: Number(BigInt(log.logIndex)),
						blockHash: log.blockHash,
					})),
				),
			);

		const getBlock = (blockNumber: bigint) =>
			Effect.tryPromise({
				try: () => client.getBlock({ blockNumber }),
				catch: (e) =>
					new RpcError({
						reason: String(e),
						method: "eth_getBlockByNumber",
						cause: e,
					}),
			}).pipe(
				Effect.map((block) => ({
					number: block.number,
					hash: block.hash,
					parentHash: block.parentHash,
					timestamp: block.timestamp,
				})),
			);

		return { getBlockNumber, getLogs, getBlock };
	}),
);
