import { Effect, Layer } from "effect";
import { describe, expect, it, vi } from "vitest";
import { ConfigLive } from "../src/config.js";
import { RpcProvider, RpcProviderLive } from "../src/services/RpcProvider.js";
import { ERC20_ABI } from "./fixtures/abi.js";

const viemMocks = vi.hoisted(() => {
	return {
		getBlockNumber: vi.fn(async () => 123n),
		request: vi.fn(async () => []),
		getBlock: vi.fn(async () => ({
			number: 123n,
			hash: "0xhash",
			parentHash: "0xparent",
			timestamp: 1700000000n,
		})),
		createPublicClient: vi.fn(),
		http: vi.fn((url: string) => ({ url })),
	};
});

vi.mock("viem", () => {
	viemMocks.createPublicClient.mockImplementation(() => ({
		getBlockNumber: viemMocks.getBlockNumber,
		request: viemMocks.request,
		getBlock: viemMocks.getBlock,
	}));

	return {
		createPublicClient: viemMocks.createPublicClient,
		http: viemMocks.http,
	};
});

describe("RpcProvider", () => {
	it("passes topics to eth_getLogs request", async () => {
		const TestConfig = ConfigLive({
			rpcUrl: "http://localhost",
			contracts: [
				{ name: "Test", address: "0x1", abi: ERC20_ABI, events: ["Transfer"] },
			],
		});
		const TestLayer = RpcProviderLive.pipe(Layer.provide(TestConfig));

		await Effect.runPromise(
			Effect.gen(function* () {
				const rpc = yield* RpcProvider;
				yield* rpc.getLogs({
					address: "0x1111111111111111111111111111111111111111",
					topics: [
						[
							"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
						],
					],
					fromBlock: 1n,
					toBlock: 2n,
				});
			}).pipe(Effect.provide(TestLayer)),
		);

		expect(viemMocks.request).toHaveBeenCalledTimes(1);
		expect(viemMocks.request).toHaveBeenCalledWith(
			expect.objectContaining({
				method: "eth_getLogs",
				params: [
					expect.objectContaining({
						topics: [
							[
								"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
							],
						],
					}),
				],
			}),
		);
	});
});
