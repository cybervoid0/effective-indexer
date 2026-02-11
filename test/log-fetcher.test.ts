import { Chunk, Effect, Layer, Stream } from "effect";
import { describe, expect, it } from "vitest";
import { ConfigLive } from "../src/config.js";
import { RpcError } from "../src/errors.js";
import { buildTopicFilter, fetchLogs } from "../src/pipeline/LogFetcher.js";
import type { RawLog } from "../src/services/RpcProvider.js";
import { RpcProvider } from "../src/services/RpcProvider.js";
import { ERC20_ABI } from "./fixtures/abi.js";

describe("buildTopicFilter", () => {
	it("builds topic filter for Transfer event", () => {
		const topics = buildTopicFilter(ERC20_ABI, ["Transfer"]);
		expect(topics).toHaveLength(1);
		expect(topics[0]).toMatch(/^0x[0-9a-f]{64}$/);
	});

	it("builds topic filter for multiple events", () => {
		const topics = buildTopicFilter(ERC20_ABI, ["Transfer", "Approval"]);
		expect(topics).toHaveLength(2);
	});

	it("throws for unknown event", () => {
		expect(() => buildTopicFilter(ERC20_ABI, ["NonExistent"])).toThrow();
	});
});

describe("fetchLogs", () => {
	it("chunks block ranges correctly", async () => {
		const fetchedRanges: Array<{ from: bigint; to: bigint }> = [];

		const MockRpcProvider = Layer.succeed(RpcProvider, {
			getBlockNumber: Effect.succeed(100n),
			getLogs: (params) => {
				fetchedRanges.push({ from: params.fromBlock, to: params.toBlock });
				return Effect.succeed([] as RawLog[]);
			},
			getBlock: () =>
				Effect.fail(
					new RpcError({ reason: "not implemented", method: "getBlock" }),
				),
		});

		const TestConfig = ConfigLive({
			rpcUrl: "http://localhost",
			contracts: [
				{ name: "Test", address: "0x1", abi: ERC20_ABI, events: ["Transfer"] },
			],
			chunkSize: 100,
		});

		const TestLayer = Layer.merge(MockRpcProvider, TestConfig);

		const result = await Effect.runPromise(
			fetchLogs({
				address: "0x1",
				topics: ["0xabc"],
				fromBlock: 0n,
				toBlock: 250n,
			}).pipe(
				Stream.runCollect,
				Effect.map(Chunk.toReadonlyArray),
				Effect.provide(TestLayer),
			),
		);

		expect(result).toHaveLength(3); // 0-99, 100-199, 200-250
		expect(fetchedRanges[0]).toEqual({ from: 0n, to: 99n });
		expect(fetchedRanges[1]).toEqual({ from: 100n, to: 199n });
		expect(fetchedRanges[2]).toEqual({ from: 200n, to: 250n });
	});

	it("returns empty stream when fromBlock > toBlock", async () => {
		const MockRpcProvider = Layer.succeed(RpcProvider, {
			getBlockNumber: Effect.succeed(100n),
			getLogs: () => Effect.succeed([] as RawLog[]),
			getBlock: () =>
				Effect.fail(
					new RpcError({ reason: "not implemented", method: "getBlock" }),
				),
		});

		const TestConfig = ConfigLive({
			rpcUrl: "http://localhost",
			contracts: [
				{ name: "Test", address: "0x1", abi: ERC20_ABI, events: ["Transfer"] },
			],
		});

		const TestLayer = Layer.merge(MockRpcProvider, TestConfig);

		const result = await Effect.runPromise(
			fetchLogs({
				address: "0x1",
				topics: ["0xabc"],
				fromBlock: 200n,
				toBlock: 100n,
			}).pipe(
				Stream.runCollect,
				Effect.map(Chunk.toReadonlyArray),
				Effect.provide(TestLayer),
			),
		);

		expect(result).toHaveLength(0);
	});
});
