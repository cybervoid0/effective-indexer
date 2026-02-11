# @rootstock/indexer

Lightweight event indexer for Rootstock (RSK).

It indexes smart contract events into SQLite with:
- historical backfill (`eth_getLogs` in chunks)
- live polling for new blocks
- checkpoint resume after restart
- basic reorg handling

## Requirements

- Node.js `>=20`
- RPC endpoint with `eth_getLogs` enabled

## Install

```bash
npm install @rootstock/indexer effect
```

`effect` is a peer dependency.

## Quick Start

```ts
import { Indexer } from "@rootstock/indexer"
import type { Abi } from "viem"

const abi: Abi = [
	{
		type: "event",
		name: "Transfer",
		inputs: [
			{ indexed: true, name: "from", type: "address" },
			{ indexed: true, name: "to", type: "address" },
			{ indexed: false, name: "value", type: "uint256" },
		],
	},
]

const indexer = Indexer.create({
	rpcUrl: "https://rpc.mainnet.rootstock.io/<API_KEY>",
	dbPath: "./data/events.db",
	contracts: [
		{
			name: "Token",
			address: "0x5Db91E24BD32059584bbdB831a901F1199f3D459",
			abi,
			events: ["Transfer"],
			startBlock: 6704080n,
		},
	],
})

await indexer.start() // non-blocking, runs in background

const events = await indexer.query({
	contractName: "Token",
	eventName: "Transfer",
	fromBlock: 6704080n,
	limit: 50,
	order: "desc",
})

console.log(events.length)

// later
await indexer.stop()
```

## API

### `Indexer.create(config)`

Returns `IndexerHandle`:
- `start(): Promise<void>` start indexing loop (non-blocking)
- `stop(): Promise<void>` stop and dispose runtime
- `query(q?: EventQuery): Promise<ParsedEvent[]>`
- `count(q?: EventQuery): Promise<number>`

### `IndexerConfig` (important fields)

- `rpcUrl: string`
- `dbPath?: string` (default `./indexer.db`)
- `contracts: [{ name, address, abi, events, startBlock? }]`
- `chunkSize?: number` (default `5000`)
- `pollInterval?: number` (default `15000`)
- `confirmations?: number` (default `0`)
- `maxRetries?: number` (default `5`)
- `reorgDepth?: number` (default `10`)

### `EventQuery`

- `contractName?: string`
- `eventName?: string`
- `fromBlock?: bigint`
- `toBlock?: bigint`
- `txHash?: string`
- `limit?: number`
- `offset?: number`
- `order?: "asc" | "desc"`

## Operational Notes

- Use one writer process per SQLite database file.
- Keep database file on persistent storage.
- On restart, the indexer resumes from checkpoint and backfills missed blocks.
- If RPC does not support `eth_getLogs`, indexing cannot work.

## Development

```bash
npm run build
npm run typecheck
npm run test
npm run check
```

### Live Integration Tests (real RSK contracts)

Integration tests read RPC URLs from:
- `.env` (mainnet)
- `.env.test` (testnet)

Run:

```bash
npm run test:integration
```

This runs real-chain fetch + decode checks against known contracts.
