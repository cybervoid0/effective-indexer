# Effective Indexer

EVM event indexing without hosted lock-in.

`effective-indexer` runs as your own worker, writes directly to your SQLite database, and gives you a typed query API.

## Why this approach

- **Own your data**: events are stored in your DB, not in a third-party service.
- **Simple operations**: one worker process, one config file, no subgraph deployment pipeline.
- **Production-safe behavior**: checkpoint resume, reorg detection, retry/backoff, live polling.
- **Fast backfill**: parallel `eth_getLogs` with deterministic chunk ordering.
- **Typed DX**: TypeScript-first config and query surface.

## Install

```bash
npm install effective-indexer effect
```

`effect` is a peer dependency.

## License

Free for noncommercial use under PolyForm Noncommercial 1.0.0.
Commercial use requires a paid commercial license (see `LICENSE`).
Contact: Aleksandr Shenshin <shenshin@me.com>.

## 5-minute setup

### 1) Create `indexer.config.ts`

```ts
import { defineIndexerConfig } from "effective-indexer"
import type { Abi } from "viem"

const transferAbi: Abi = [
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

export default defineIndexerConfig({
	rpcUrl: "https://rpc.mainnet.rootstock.io/{{EVM_RPC_API_KEY}}",
	dbPath: "./data/events.db",
	contracts: [
		{
			name: "Token",
			address: "0xYourContractAddress",
			abi: transferAbi,
			events: ["Transfer"],
			startBlock: 0n,
		},
	],
	network: {
		logs: {
			chunkSize: 2000,
			parallelRequests: 3,
		},
	},
})
```

### 2) Create `scripts/indexer.ts`

```ts
import config from "../indexer.config"
import { resolveIndexerConfigFromEnv, runIndexerWorker } from "effective-indexer"

const resolvedConfig = resolveIndexerConfigFromEnv(config)

runIndexerWorker(resolvedConfig).catch(error => {
	console.error("Indexer worker failed:", error)
	process.exit(1)
})
```

### 3) Add env and run

`.env`:

```bash
EVM_RPC_API_KEY=your-rpc-api-key
# Optional full URL override:
# EVM_RPC_URL=https://rpc.mainnet.rootstock.io/<API_KEY>
```

Run:

```bash
node --import tsx ./scripts/indexer.ts
```

## Query data

```ts
import config from "../indexer.config"
import { Indexer, resolveIndexerConfigFromEnv } from "effective-indexer"

const indexer = Indexer.create(resolveIndexerConfigFromEnv(config))

const events = await indexer.query({
	contractName: "Token",
	eventName: "Transfer",
	order: "desc",
	limit: 50,
})

console.log(events.length)
await indexer.stop()
```

## Public API

- `defineIndexerConfig(config)`
  Identity helper for typed config files (Hardhat-style).
- `resolveIndexerConfigFromEnv(config, options?)`
  Resolves `{{ENV_VAR}}` placeholders and optional RPC URL override.
- `runIndexerWorker(config, options?)`
  Runs long-lived worker with built-in DB directory creation and graceful shutdown.
- `Indexer.create(config)`
  Returns handle: `start()`, `stop()`, `query()`, `count()`.

## Config essentials

- `rpcUrl`: RPC endpoint URL (supports placeholders like `{{EVM_RPC_API_KEY}}`)
- `dbPath`: SQLite path (default `./indexer.db`)
- `contracts`: non-empty list of contracts and events to index
- `network.polling`: block polling interval and confirmations
- `network.logs`: chunk size, retries, parallel requests
- `network.reorg.depth`: reorg buffer depth
- `telemetry.progress`: CLI progress rendering
- `logLevel`, `logFormat`, `enableTelemetry`

## Operational notes

- Run a single writer process per SQLite file.
- Keep DB on persistent storage.
- Worker resumes from checkpoint after restart.
- RPC must support `eth_getLogs`.

## Development

```bash
npm run build
npm run typecheck
npm run test
npm run check
```

Repository: [github.com/cybervoid0/effective-indexer](https://github.com/cybervoid0/effective-indexer)
# Effective Indexer

Lightweight EVM smart contract event indexer built with [Effect](https://effect.website).

Index EVM events to your own database in minutes — no hosted lock-in, no PhD required.

Repository: [github.com/cybervoid0/effective-indexer](https://github.com/cybervoid0/effective-indexer)

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

### `defineIndexerConfig(config)`

Identity helper for a typed config file (Hardhat-style DX).

### `resolveIndexerConfigFromEnv(config, options?)`

Resolves `{{ENV_VAR}}` placeholders in `rpcUrl` and supports optional RPC override from env (`EVM_RPC_URL` by default).

### `runIndexerWorker(config, options?)`

Runs a long-lived worker with built-in:
- SQLite directory creation
- graceful shutdown on `SIGINT` / `SIGTERM`
- keep-alive process loop

### `IndexerConfig`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `rpcUrl` | `string` | — | RPC endpoint URL |
| `dbPath` | `string` | `"./indexer.db"` | SQLite database path |
| `contracts` | `ContractConfig[]` | — | Contracts to index |
| `network` | `NetworkConfig` | see below | Network tuning |
| `telemetry` | `TelemetryConfig` | see below | Backfill progress settings |
| `logLevel` | `string` | `"info"` | Minimum log level |
| `logFormat` | `string` | `"pretty"` | Log output format |
| `enableTelemetry` | `boolean` | `true` | Set `false` for errors-only |

### `NetworkConfig`

```ts
{
  polling: {
    intervalMs: 12000,       // block polling interval
    confirmations: 1,        // blocks behind head to consider confirmed
  },
  logs: {
    chunkSize: 5000,         // blocks per eth_getLogs request
    maxRetries: 5,           // retry count on RPC failure
    parallelRequests: 1,     // concurrent eth_getLogs requests during backfill
    retry: {
      baseDelayMs: 1000,     // initial retry delay
      maxDelayMs: 30000,     // cap for exponential backoff
    },
  },
  reorg: {
    depth: 20,               // block hash buffer depth for reorg detection
  },
}
```

All fields are optional — defaults are shown above.

### `TelemetryConfig`

```ts
{
  telemetry: {
    progress: {
      enabled: true,         // show backfill progress in terminal
      intervalMs: 3000,      // progress update frequency (ms, minimum 500)
    },
  },
}
```

`enableTelemetry: false` disables progress rendering and keeps error-level logs only.

When enabled, the indexer displays a live progress line during backfill:

```
[Backfill] Token 42.8% | 1,234,000/2,880,000 blocks | 3,450 blk/s | 12.4 ev/s | ETA 00:07:43 | p=3 | chunk=5000
```

On non-TTY environments, periodic info logs are emitted instead. A final summary is logged when backfill completes:

```
[Backfill complete] Token: 2,880,000 blocks | 45,230 events | 312 chunks | 00:13:54 (3,453 blk/s, 54.2 ev/s) | p=3 | chunkSize=5000
```

## Worker Setup (Recommended)

Run the indexer as a dedicated long-lived worker process (not in request handlers).

Create `scripts/indexer.ts`:

```ts
import config from "../indexer.config"
import { resolveIndexerConfigFromEnv, runIndexerWorker } from "effective-indexer"

const resolvedConfig = resolveIndexerConfigFromEnv(config)

runIndexerWorker(resolvedConfig).catch(error => {
	console.error("Indexer worker failed:", error)
	process.exit(1)
})
```

Create `indexer.config.ts`:

```ts
import { defineIndexerConfig } from "effective-indexer"
import type { Abi } from "viem"

const transferAbi: Abi = [
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

export default defineIndexerConfig({
	rpcUrl: "https://rpc.mainnet.rootstock.io/{{EVM_RPC_API_KEY}}",
	dbPath: "./data/events.db",
	contracts: [
		{
			name: "Token",
			address: "0xYourContractAddress",
			abi: transferAbi,
			events: ["Transfer"],
			startBlock: 0n,
		},
	],
})
```

Create `.env`:

```bash
EVM_RPC_API_KEY=your-rpc-api-key
# Optional full RPC URL override:
# EVM_RPC_URL=https://rpc.mainnet.rootstock.io/<API_KEY>
```

Add scripts (with `tsx` installed):

```bash
npm install -D tsx
```

```json
{
  "scripts": {
    "indexer:start": "node --import tsx ./scripts/indexer.ts",
    "indexer:debug": "INDEXER_LOG_LEVEL=debug node --import tsx ./scripts/indexer.ts"
  }
}
```

Run:

```bash
npm run indexer:start
```

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

## Parallel Backfill

Set `network.logs.parallelRequests` to speed up historical backfill by issuing multiple `eth_getLogs` requests concurrently. Chunk ordering is preserved regardless of concurrency.

```ts
const indexer = Indexer.create({
  rpcUrl: "https://eth.llamarpc.com",
  contracts: [/* ... */],
  network: {
    logs: {
      chunkSize: 2000,
      parallelRequests: 4,
    },
  },
})
```

**Recommended values**: Start with `parallelRequests: 3` and increase if the RPC allows. Public endpoints may rate-limit above 5-10 concurrent requests.

### Benchmarking

To measure the effect of parallelism:

1. Use a fixed RPC endpoint and contract/block range
2. Start with an empty database each run
3. Compare `parallelRequests` values 1, 2, 3, 4
4. Run 3 times each and take the median
5. Use the progress summary line for timing: `[Backfill complete] ... blk/s`

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
