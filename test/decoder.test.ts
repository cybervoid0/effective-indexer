import { Effect } from "effect"
import { describe, expect, it } from "vitest"
import { EventDecoder, EventDecoderLive } from "../src/services/EventDecoder.js"
import { ERC20_ABI } from "./fixtures/abi.js"
import { SAMPLE_APPROVAL_LOGS, SAMPLE_TRANSFER_LOGS } from "./fixtures/logs.js"

const runTest = <A, E>(effect: Effect.Effect<A, E, EventDecoder>) =>
	Effect.runPromise(effect.pipe(Effect.provide(EventDecoderLive)))

describe("EventDecoder", () => {
	it("decodes a Transfer event", () =>
		runTest(
			Effect.gen(function* () {
				const decoder = yield* EventDecoder
				const result = yield* decoder.decode(
					"TestToken",
					ERC20_ABI,
					SAMPLE_TRANSFER_LOGS[0]!,
				)
				expect(result).not.toBeNull()
				expect(result!.eventName).toBe("Transfer")
				expect(result!.contractName).toBe("TestToken")
				expect(result!.args).toHaveProperty("value")
			}),
		))

	it("decodes a batch of logs", () =>
		runTest(
			Effect.gen(function* () {
				const decoder = yield* EventDecoder
				const results = yield* decoder.decodeBatch("TestToken", ERC20_ABI, [
					...SAMPLE_TRANSFER_LOGS,
					...SAMPLE_APPROVAL_LOGS,
				])
				expect(results).toHaveLength(4)
				const names = results.map(r => r.eventName)
				expect(names.filter(n => n === "Transfer")).toHaveLength(3)
				expect(names.filter(n => n === "Approval")).toHaveLength(1)
			}),
		))

	it("fails batch when a log cannot be decoded", () =>
		runTest(
			Effect.gen(function* () {
				const decoder = yield* EventDecoder
				const badLog = {
					address: "0x1234567890abcdef1234567890abcdef12345678",
					topics: ["0xdeadbeef"],
					data: "0x",
					blockNumber: 100n,
					transactionHash: "0xbad",
					logIndex: 0,
					blockHash: "0xblock",
				}
				const exit = yield* Effect.either(
					decoder.decodeBatch("TestToken", ERC20_ABI, [
						SAMPLE_TRANSFER_LOGS[0]!,
						badLog,
					]),
				)
				expect(exit._tag).toBe("Left")
				if (exit._tag === "Left") {
					expect(exit.left._tag).toBe("DecodeError")
				}
			}),
		))
})
