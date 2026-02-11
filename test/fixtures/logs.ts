import { encodeAbiParameters, encodeEventTopics } from "viem";
import type { RawLog } from "../../src/services/RpcProvider.js";
import { ERC20_ABI } from "./abi.js";

const ADDR_A = "0x000000000000000000000000aaaaaaaaaaaaaaaa";
const ADDR_B = "0x000000000000000000000000bbbbbbbbbbbbbbbb";

const transferTopic = encodeEventTopics({
	abi: ERC20_ABI,
	eventName: "Transfer",
})[0]!;

const approvalTopic = encodeEventTopics({
	abi: ERC20_ABI,
	eventName: "Approval",
})[0]!;

export const makeTransferLog = (
	blockNumber: bigint,
	logIndex: number,
	txHash: string,
	blockHash: string,
): RawLog => ({
	address: "0x1234567890abcdef1234567890abcdef12345678",
	topics: [
		transferTopic,
		`0x${ADDR_A.slice(2).padStart(64, "0")}`,
		`0x${ADDR_B.slice(2).padStart(64, "0")}`,
	],
	data: encodeAbiParameters([{ type: "uint256" }], [1000n]),
	blockNumber,
	transactionHash: txHash,
	logIndex,
	blockHash,
});

export const makeApprovalLog = (
	blockNumber: bigint,
	logIndex: number,
	txHash: string,
	blockHash: string,
): RawLog => ({
	address: "0x1234567890abcdef1234567890abcdef12345678",
	topics: [
		approvalTopic,
		`0x${ADDR_A.slice(2).padStart(64, "0")}`,
		`0x${ADDR_B.slice(2).padStart(64, "0")}`,
	],
	data: encodeAbiParameters([{ type: "uint256" }], [5000n]),
	blockNumber,
	transactionHash: txHash,
	logIndex,
	blockHash,
});

export const SAMPLE_TRANSFER_LOGS: RawLog[] = [
	makeTransferLog(100n, 0, "0xaaa1", "0xbbb1"),
	makeTransferLog(100n, 1, "0xaaa2", "0xbbb1"),
	makeTransferLog(101n, 0, "0xaaa3", "0xbbb2"),
];

export const SAMPLE_APPROVAL_LOGS: RawLog[] = [
	makeApprovalLog(100n, 2, "0xaaa4", "0xbbb1"),
];
