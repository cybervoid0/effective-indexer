import { readFileSync } from "node:fs"
import { join } from "node:path"
import { GOVERNOR_ABI, STRIF_TOKEN_ABI } from "@test/fixtures/abi"
import { Chunk, Effect, Layer, Stream } from "effect"
import { describe, expect, it } from "vitest"
import { type Config, ConfigLive } from "@/config"
import { buildTopicFilter, fetchLogs } from "@/pipeline/LogFetcher"
import { EventDecoder, EventDecoderLive } from "@/services/EventDecoder"
import { RpcProvider, RpcProviderLive } from "@/services/RpcProvider"

interface IntegrationScenario {
	readonly name: string
	readonly rpcUrl: string
	readonly contractAddress: string
	readonly startBlock: bigint
	readonly windowSize: bigint
	readonly maxWindows: number
	readonly events: readonly [string, ...string[]]
	readonly abi: typeof STRIF_TOKEN_ABI | typeof GOVERNOR_ABI
}

const readEnvVar = (
	fileName: ".env" | ".env.test",
	key: string,
): string | null => {
	try {
		const filePath = join(process.cwd(), fileName)
		const content = readFileSync(filePath, "utf8")
		const line = content
			.split("\n")
			.map(value => value.trim())
			.find(value => value.startsWith(`${key}=`))
		if (!line) return null
		const rawValue = line.replace(`${key}=`, "").trim()
		return rawValue.length > 0 ? rawValue : null
	} catch {
		return null
	}
}

const readRpcUrl = (fileName: ".env" | ".env.test"): string | null =>
	readEnvVar(fileName, "EVM_RPC_URL")

const mainnetRpcUrl = readRpcUrl(".env")
const testnetRpcUrl = readRpcUrl(".env.test")

const scenarios: readonly IntegrationScenario[] = [
	{
		name: "StRIF testnet token",
		rpcUrl: testnetRpcUrl ?? "",
		contractAddress: "0x4861198e9A6814EBfb152552D1b1a37426C54D23",
		startBlock: 5582973n,
		windowSize: 50_000n,
		maxWindows: 10,
		events: ["DelegateChanged", "Transfer", "Approval"],
		abi: STRIF_TOKEN_ABI,
	},
	{
		name: "StRIF mainnet token",
		rpcUrl: mainnetRpcUrl ?? "",
		contractAddress: "0x5Db91E24BD32059584bbdB831a901F1199f3D459",
		startBlock: 6704080n,
		windowSize: 50_000n,
		maxWindows: 10,
		events: ["DelegateChanged", "Transfer", "Approval"],
		abi: STRIF_TOKEN_ABI,
	},
	{
		name: "Governor mainnet",
		rpcUrl: mainnetRpcUrl ?? "",
		contractAddress: "0x71ac6fF904A17F50f2C07B693376cCc1c92627F0",
		startBlock: 6704091n,
		windowSize: 100_000n,
		maxWindows: 10,
		events: [
			"ProposalCreated",
			"ProposalQueued",
			"ProposalExecuted",
			"ProposalCanceled",
			"VoteCast",
			"VoteCastWithParams",
		],
		abi: GOVERNOR_ABI,
	},
]

const runIntegration =
	process.env.RUN_EVM_INTEGRATION === "true" &&
	mainnetRpcUrl !== null &&
	testnetRpcUrl !== null
const describeIntegration = runIntegration ? describe : describe.skip

const createIntegrationLayer = (scenario: IntegrationScenario) => {
	const integrationConfig = ConfigLive({
		rpcUrl: scenario.rpcUrl,
		contracts: [
			{
				name: scenario.name,
				address: scenario.contractAddress,
				abi: scenario.abi,
				events: scenario.events,
				startBlock: scenario.startBlock,
			},
		],
		network: {
			logs: {
				chunkSize: 2_000,
				maxRetries: 3,
			},
		},
	})

	return Layer.mergeAll(
		integrationConfig,
		RpcProviderLive.pipe(Layer.provide(integrationConfig)),
		EventDecoderLive,
	)
}

const findWindowWithLogs = (
	scenario: IntegrationScenario,
): Effect.Effect<
	{
		readonly fromBlock: bigint
		readonly toBlock: bigint
		readonly logsCount: number
	},
	Error,
	RpcProvider | Config
> =>
	Effect.gen(function* () {
		const rpc = yield* RpcProvider
		const head = yield* rpc.getBlockNumber.pipe(
			Effect.mapError(
				error =>
					new Error(
						`Could not get head block for ${scenario.name}: ${error.reason}`,
					),
			),
		)
		const topics = buildTopicFilter(scenario.abi, scenario.events)

		for (let index = 0; index < scenario.maxWindows; index += 1) {
			const fromBlock =
				scenario.startBlock + BigInt(index) * scenario.windowSize
			if (fromBlock > head) {
				break
			}
			const rawToBlock = fromBlock + scenario.windowSize - 1n
			const toBlock = rawToBlock > head ? head : rawToBlock

			const chunks = yield* fetchLogs({
				address: scenario.contractAddress,
				topics,
				fromBlock,
				toBlock,
			}).pipe(
				Stream.runCollect,
				Effect.map(Chunk.toReadonlyArray),
				Effect.mapError(
					error =>
						new Error(
							`Failed fetching logs for ${scenario.name} at range ${fromBlock}-${toBlock}: ${error.reason}`,
						),
				),
			)

			const logsCount = chunks.flat().length
			if (logsCount > 0) {
				return { fromBlock, toBlock, logsCount }
			}
		}

		return yield* Effect.fail(
			new Error(
				`No logs found for ${scenario.name} after scanning ${scenario.maxWindows} windows from block ${scenario.startBlock}.`,
			),
		)
	})

describeIntegration("EVM integration (real contracts)", () => {
	for (const scenario of scenarios) {
		it(`fetches and decodes logs for ${scenario.name}`, async () => {
			const integrationLayer = createIntegrationLayer(scenario)

			const result = await Effect.runPromise(
				Effect.gen(function* () {
					const decoder = yield* EventDecoder
					const foundWindow = yield* findWindowWithLogs(scenario)
					const chunks = yield* fetchLogs({
						address: scenario.contractAddress,
						topics: buildTopicFilter(scenario.abi, scenario.events),
						fromBlock: foundWindow.fromBlock,
						toBlock: foundWindow.toBlock,
					}).pipe(Stream.runCollect, Effect.map(Chunk.toReadonlyArray))

					const logs = chunks.flat()
					const decoded = yield* decoder.decodeBatch(
						scenario.name,
						scenario.abi,
						logs,
					)

					return {
						window: foundWindow,
						logsCount: logs.length,
						decoded,
					}
				}).pipe(Effect.provide(integrationLayer)),
			)

			expect(result.logsCount).toBeGreaterThan(0)
			expect(result.decoded.length).toBeGreaterThan(0)
			expect(
				result.decoded.every(event =>
					scenario.events.includes(event.eventName),
				),
			).toBe(true)
		}, 90_000)
	}
})
