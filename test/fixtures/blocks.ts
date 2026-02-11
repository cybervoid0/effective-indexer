import type { BlockInfo } from "../../src/services/RpcProvider.js"

export const makeBlockHash = (n: number): string =>
	`0x${n.toString(16).padStart(64, "0")}`

export const makeBlock = (
	number: bigint,
	parentNumber?: bigint,
): BlockInfo => ({
	number,
	hash: makeBlockHash(Number(number)),
	parentHash: makeBlockHash(Number(parentNumber ?? number - 1n)),
	timestamp: 1700000000n + number * 30n,
})

export const makeChain = (from: number, to: number): BlockInfo[] => {
	const blocks: BlockInfo[] = []
	for (let i = from; i <= to; i++) {
		blocks.push(makeBlock(BigInt(i)))
	}
	return blocks
}

// A forked block that has a different hash but claims same parent
export const makeForkedBlock = (number: bigint): BlockInfo => ({
	number,
	hash: `0xfork${Number(number).toString(16).padStart(60, "0")}`,
	parentHash: `0xbad${Number(number - 1n)
		.toString(16)
		.padStart(61, "0")}`,
	timestamp: 1700000000n + number * 30n,
})
