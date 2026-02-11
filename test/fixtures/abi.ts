import type { Abi } from "viem";

export const ERC20_ABI: Abi = [
	{
		type: "event",
		name: "Transfer",
		inputs: [
			{ indexed: true, name: "from", type: "address" },
			{ indexed: true, name: "to", type: "address" },
			{ indexed: false, name: "value", type: "uint256" },
		],
	},
	{
		type: "event",
		name: "Approval",
		inputs: [
			{ indexed: true, name: "owner", type: "address" },
			{ indexed: true, name: "spender", type: "address" },
			{ indexed: false, name: "value", type: "uint256" },
		],
	},
];

export const STRIF_TOKEN_ABI: Abi = [
	...ERC20_ABI,
	{
		type: "event",
		name: "DelegateChanged",
		inputs: [
			{ indexed: true, name: "delegator", type: "address" },
			{ indexed: true, name: "fromDelegate", type: "address" },
			{ indexed: true, name: "toDelegate", type: "address" },
		],
	},
];

export const GOVERNOR_ABI: Abi = [
	{
		type: "event",
		name: "ProposalCreated",
		inputs: [
			{ indexed: false, name: "proposalId", type: "uint256" },
			{ indexed: true, name: "proposer", type: "address" },
			{ indexed: false, name: "targets", type: "address[]" },
			{ indexed: false, name: "values", type: "uint256[]" },
			{ indexed: false, name: "signatures", type: "string[]" },
			{ indexed: false, name: "calldatas", type: "bytes[]" },
			{ indexed: false, name: "voteStart", type: "uint256" },
			{ indexed: false, name: "voteEnd", type: "uint256" },
			{ indexed: false, name: "description", type: "string" },
		],
	},
	{
		type: "event",
		name: "ProposalQueued",
		inputs: [
			{ indexed: false, name: "proposalId", type: "uint256" },
			{ indexed: false, name: "etaSeconds", type: "uint256" },
		],
	},
	{
		type: "event",
		name: "ProposalExecuted",
		inputs: [{ indexed: false, name: "proposalId", type: "uint256" }],
	},
	{
		type: "event",
		name: "ProposalCanceled",
		inputs: [{ indexed: false, name: "proposalId", type: "uint256" }],
	},
	{
		type: "event",
		name: "VoteCast",
		inputs: [
			{ indexed: true, name: "voter", type: "address" },
			{ indexed: false, name: "proposalId", type: "uint256" },
			{ indexed: false, name: "support", type: "uint8" },
			{ indexed: false, name: "weight", type: "uint256" },
			{ indexed: false, name: "reason", type: "string" },
		],
	},
	{
		type: "event",
		name: "VoteCastWithParams",
		inputs: [
			{ indexed: true, name: "voter", type: "address" },
			{ indexed: false, name: "proposalId", type: "uint256" },
			{ indexed: false, name: "support", type: "uint8" },
			{ indexed: false, name: "weight", type: "uint256" },
			{ indexed: false, name: "reason", type: "string" },
			{ indexed: false, name: "params", type: "bytes" },
		],
	},
];
