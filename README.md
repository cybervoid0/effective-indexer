# Effective Indexer

Lightweight EVM smart contract event indexer built with [Effect](https://effect.website).

Indexes smart contract events into SQLite with:
- Historical backfill (`eth_getLogs` in chunks)
- Live polling for new blocks
- Checkpoint resume after restart
- Reorg detection and rollback

Works with any EVM-compatible chain (Ethereum, Rootstock, Polygon, Arbitrum, etc.).

## Requirements

- Node.js `>=20`
- RPC endpoint with `eth_getLogs` support

## Install

```bash
npm install effective-indexer effect
```

`effect` is a peer dependency.

## Quick Start

```ts
import { Indexer } from "effective-indexer"
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
	rpcUrl: "https://eth.llamarpc.com",
	dbPath: "./data/events.db",
	contracts: [
		{
			name: "USDT",
			address: "0xdAC17F958D2ee523a2206206994597C13D831ec7",
			abi,
			events: ["Transfer"],
			startBlock: 19000000n,
		},
	],
	network: {
		polling: { intervalMs: 12000, confirmations: 2 },
		logs: { chunkSize: 2000 },
		reorg: { depth: 64 },
	},
})

await indexer.start() // non-blocking, runs in background

const events = await indexer.query({
	contractName: "USDT",
	eventName: "Transfer",
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

### `IndexerConfig`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `rpcUrl` | `string` | — | RPC endpoint URL |
| `dbPath` | `string` | `"./indexer.db"` | SQLite database path |
| `contracts` | `ContractConfig[]` | — | Contracts to index |
| `network` | `NetworkConfig` | see below | Network tuning |
| `logLevel` | `string` | `"info"` | Minimum log level |
| `logFormat` | `string` | `"pretty"` | Log output format |
| `enableTelemetry` | `boolean` | `true` | Set `false` for errors-only |

### `NetworkConfig`

```ts
{
  polling: {
    intervalMs: 12000,     // block polling interval
    confirmations: 1,       // blocks behind head to consider confirmed
  },
  logs: {
    chunkSize: 5000,        // blocks per eth_getLogs request
    maxRetries: 5,          // retry count on RPC failure
    retry: {
      baseDelayMs: 1000,    // initial retry delay
      maxDelayMs: 30000,    // cap for exponential backoff
    },
  },
  reorg: {
    depth: 20,              // block hash buffer depth for reorg detection
  },
}
```

All fields are optional — defaults are shown above.

### Network Tuning Profiles

| Chain | `polling.intervalMs` | `polling.confirmations` | `logs.chunkSize` | `reorg.depth` |
|-------|---------------------|------------------------|------------------|---------------|
| Ethereum | 12000 | 2 | 2000 | 64 |
| Rootstock | 30000 | 1 | 5000 | 20 |
| Polygon | 2000 | 32 | 2000 | 128 |
| Arbitrum | 1000 | 0 | 5000 | 1 |

### `EventQuery`

- `contractName?: string`
- `eventName?: string`
- `fromBlock?: bigint`
- `toBlock?: bigint`
- `txHash?: string`
- `limit?: number`
- `offset?: number`
- `order?: "asc" | "desc"`

## Telemetry & Logging

The indexer uses Effect's native logging system. All log output is controlled via config — no `console.log` calls in source.

| Level | What's emitted |
|-------|---------------|
| `error` | Indexer errors (RPC failures, storage errors) |
| `warning` | Reorg detection, parent hash mismatches |
| `info` | Indexer start/stop, backfill start/complete, reorg handled |
| `debug` | Chunk indexed, block indexed, storage init, query/count execution, reorg rollback, BlockCursor init |
| `trace` | Individual log fetches, block emissions, no-new-blocks polls |

### Recommendations

- **Production**: `logLevel: "info"` — lifecycle events and warnings
- **Troubleshooting**: `logLevel: "debug"` — per-chunk/block detail
- **Deep inspection**: `logLevel: "trace"` — every RPC call and poll
- **Silent**: `enableTelemetry: false` — only errors

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

### Live Integration Tests

Integration tests read RPC URLs from `.env` (mainnet) and `.env.test` (testnet) using `EVM_RPC_URL`.

```bash
npm run test:integration
```
