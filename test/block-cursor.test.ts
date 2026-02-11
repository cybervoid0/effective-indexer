import { Chunk, Effect, Layer, Stream } from "effect";
import { describe, expect, it } from "vitest";
import { ConfigLive } from "../src/config.js";
import { BlockCursor, BlockCursorLive } from "../src/pipeline/BlockCursor.js";
import { RpcError } from "../src/errors.js";
import { RpcProvider } from "../src/services/RpcProvider.js";
import { ERC20_ABI } from "./fixtures/abi.js";

describe("BlockCursor", () => {
	it("emits all intermediate confirmed blocks after head jump", async () => {
		let calls = 0;
		const MockRpcProvider = Layer.succeed(RpcProvider, {
			getBlockNumber: Effect.sync(() => {
				const value = calls === 0 ? 100n : 102n;
				calls += 1;
				return value;
			}),
			getLogs: () => Effect.succeed([]),
			getBlock: () =>
				Effect.fail(
					new RpcError({
						reason: "not implemented",
						method: "eth_getBlockByNumber",
					}),
				),
		});

		const TestConfig = ConfigLive({
			rpcUrl: "http://localhost",
			contracts: [
				{ name: "Test", address: "0x1", abi: ERC20_ABI, events: ["Transfer"] },
			],
			pollInterval: 1,
			confirmations: 0,
		});

		const TestLayer = BlockCursorLive.pipe(
			Layer.provide(Layer.merge(MockRpcProvider, TestConfig)),
		);

		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const blockCursor = yield* BlockCursor;
				const values = yield* blockCursor.liveBlocks.pipe(
					Stream.take(2),
					Stream.runCollect,
					Effect.map(Chunk.toReadonlyArray),
				);
				return values;
			}).pipe(Effect.provide(TestLayer)),
		);

		expect(result).toEqual([101n, 102n]);
	});
});
