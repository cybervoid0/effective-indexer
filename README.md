# Effective Indexer

EVM event indexing without hosted lock-in.

One worker process, one config file, your own SQLite database. No subgraph deployment pipeline, no token staking, no PhD required.

Works with any EVM chain: Ethereum, Rootstock, Polygon, Arbitrum, Base, you name it.

## Why

- **Own your data** — events land in your DB, not in someone else's cloud.
- **Fast backfill** — parallel `eth_getLogs` with deterministic chunk ordering.
- **Production-safe** — checkpoint resume, reorg detection, retry with backoff, live polling.
- **Self-healing** — automatic crash recovery with configurable alerting.
- **Typed DX** — TypeScript-first config and query API, Hardhat-style config files.

## Install

```bash
npm install effective-indexer effect
```

`effect` is a peer dependency — the only runtime dependency besides `viem`.

## Quick start

### 1. Config file

Create `indexer.config.ts`:

```ts
import { defineIndexerConfig } from "effective-indexer"
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

export default defineIndexerConfig({
  rpcUrl: "https://rpc.mainnet.rootstock.io/{{EVM_RPC_API_KEY}}",
  dbPath: "./data/events.db",
  contracts: [
    {
      name: "Token",
      address: "0xYourContractAddress",
      abi,
      events: ["Transfer"],
      startBlock: 0n,
    },
  ],
})
```

`{{EVM_RPC_API_KEY}}` is resolved from env at runtime. Secrets stay in `.env`, config stays typed.

### 2. Worker script

Create `scripts/indexer.ts`:

```ts
import config from "../indexer.config"
import { resolveIndexerConfigFromEnv, runIndexerWorker } from "effective-indexer"

const resolved = resolveIndexerConfigFromEnv(config)

runIndexerWorker(resolved).catch(error => {
  console.error("Indexer worker failed:", error)
  process.exit(1)
})
```

### 3. Environment and run

`.env`:

```bash
EVM_RPC_API_KEY=your-api-key
# Full URL override (takes priority over template):
# EVM_RPC_URL=https://eth.llamarpc.com
```

```bash
npm install -D tsx
node --import tsx ./scripts/indexer.ts
```

That's it. The worker creates the DB directory, connects, backfills, and switches to live polling.

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

console.log(events)
await indexer.stop()
```

## API

### `Indexer.create(config): IndexerHandle`

Creates an indexer instance. Returns:

| Method | Description |
|--------|-------------|
| `start()` | Start indexing (non-blocking, runs in background) |
| `stop()` | Gracefully stop and dispose runtime (idempotent) |
| `waitForExit()` | Await the indexing loop (rejects on crash) |
| `query(q?)` | Query stored events → `Promise<ParsedEvent[]>` |
| `count(q?)` | Count stored events → `Promise<number>` |

### `defineIndexerConfig(config)`

Identity function for typed config files. Zero runtime cost, pure DX.

### `resolveIndexerConfigFromEnv(config, options?)`

Resolves `{{ENV_VAR}}` placeholders in `rpcUrl` from `process.env`. Supports:

- **Sensitive placeholders** — read via `Config.redacted` (default: `EVM_RPC_API_KEY`)
- **Full URL override** — `EVM_RPC_URL` env var takes priority over the template
- **Custom env source** — pass `{ env: myEnvMap }` for testing

### `runIndexerWorker(config, options?)`

Long-lived worker with batteries included:

- Creates DB directory if missing
- Registers `SIGINT`/`SIGTERM` handlers for graceful shutdown
- Keeps the process alive during live polling
- Auto-restarts on crash with exponential backoff
- Calls `onRecoveryFailure` webhook when recovery window is exhausted
- Always re-throws the original error (notification failures are logged, never mask the cause)

### `createWebhookNotifier(url, init?)`

Helper that returns an `onRecoveryFailure` callback — POSTs a JSON payload to the given URL.

```ts
import { createWebhookNotifier } from "effective-indexer"

const notify = createWebhookNotifier("https://hooks.slack.com/...")
```

Or configure it in the config file directly (see `worker.alert.webhookUrl` below).

### `EventQuery`

| Field | Type | Description |
|-------|------|-------------|
| `contractName` | `string?` | Filter by contract name |
| `eventName` | `string?` | Filter by event name |
| `fromBlock` | `bigint?` | Min block number |
| `toBlock` | `bigint?` | Max block number |
| `txHash` | `string?` | Filter by transaction hash |
| `limit` | `number?` | Max results |
| `offset` | `number?` | Skip first N results |
| `order` | `"asc" \| "desc"?` | Sort by block number |

### `ParsedEvent`

| Field | Type |
|-------|------|
| `id` | `number` |
| `contractName` | `string` |
| `eventName` | `string` |
| `blockNumber` | `bigint` |
| `txHash` | `string` |
| `logIndex` | `number` |
| `timestamp` | `number \| null` |
| `args` | `Record<string, unknown>` |

## Full config reference

All fields except `rpcUrl` and `contracts` are optional — sensible defaults are applied.

### Top-level

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `rpcUrl` | `string` | — | RPC endpoint (supports `{{ENV}}` placeholders) |
| `dbPath` | `string` | `"./indexer.db"` | SQLite database path |
| `contracts` | `ContractConfig[]` | — | At least one contract required |
| `network` | `NetworkConfig` | see below | RPC and chain tuning |
| `telemetry` | `TelemetryConfig` | see below | Progress rendering |
| `worker` | `WorkerConfig` | see below | Recovery and alerting |
| `logLevel` | `string` | `"info"` | `trace \| debug \| info \| warning \| error \| none` |
| `logFormat` | `string` | `"pretty"` | `pretty \| json \| structured` |
| `enableTelemetry` | `boolean` | `true` | `false` = errors only, no progress bar |

### `contracts[]`

| Field | Type | Description |
|-------|------|-------------|
| `name` | `string` | Unique contract name (used in queries) |
| `address` | `string` | Contract address (hex) |
| `abi` | `Abi` | Viem-compatible ABI (only event entries needed) |
| `events` | `[string, ...string[]]` | Event names to index (non-empty) |
| `startBlock` | `bigint?` | Block to start indexing from (default: `0n`) |

### `network`

```ts
network: {
  polling: {
    intervalMs: 12000,        // block poll interval (ms)
    confirmations: 1,         // blocks behind tip = "confirmed"
  },
  logs: {
    chunkSize: 5000,          // blocks per eth_getLogs request
    maxRetries: 5,            // retries per failed RPC call
    parallelRequests: 1,      // concurrent eth_getLogs during backfill
    retry: {
      baseDelayMs: 1000,      // initial retry delay
      maxDelayMs: 30000,      // backoff cap
    },
  },
  reorg: {
    depth: 20,                // block hash buffer for reorg detection
  },
}
```

### `telemetry`

```ts
telemetry: {
  progress: {
    enabled: true,            // show live progress bar during backfill
    intervalMs: 3000,         // render frequency (min 500ms)
  },
}
```

When enabled, the terminal shows a live progress line:

```
[Backfill] Token 42.8% | 1,234,000/2,880,000 blocks | 3,450 blk/s | 12.4 ev/s | ETA 00:07:43 | p=3 | chunk=5000
```

On non-TTY (CI, logs), periodic info messages are emitted instead.

### `worker`

```ts
worker: {
  recovery: {
    enabled: true,               // auto-restart on crash
    maxRecoveryDurationMs: 900000, // give up after 15 min of failures
    initialRetryDelayMs: 1000,   // first retry delay
    maxRetryDelayMs: 30000,      // backoff cap
    backoffFactor: 2,            // exponential multiplier
  },
  alert: {
    webhookUrl: "",              // POST failure notification here
  },
}
```

When the worker crashes, it automatically restarts with exponential backoff. If it keeps failing beyond `maxRecoveryDurationMs`, it sends a JSON notification to `alert.webhookUrl` (if set) and exits with the original error.

The notification payload (`WorkerFailureNotification`):

```ts
{
  attempts: number          // total restart attempts
  recoveryDurationMs: number // time since first failure
  error: unknown            // the error that killed it
  timestamp: string         // ISO timestamp
}
```

## Chain tuning profiles

| Chain | `polling.intervalMs` | `polling.confirmations` | `logs.chunkSize` | `reorg.depth` |
|-------|---------------------|------------------------|------------------|---------------|
| Ethereum | 12000 | 2 | 2000 | 64 |
| Rootstock | 30000 | 1 | 5000 | 20 |
| Polygon | 2000 | 32 | 2000 | 128 |
| Arbitrum | 1000 | 0 | 5000 | 1 |

## Parallel backfill

Set `network.logs.parallelRequests` to speed up historical indexing. Chunk ordering is preserved regardless of concurrency.

```ts
network: {
  logs: {
    chunkSize: 2000,
    parallelRequests: 4,
  },
}
```

Start with `3` and increase if the RPC allows. Public endpoints may rate-limit above 5–10.

## Logging

Uses Effect's native logging — no `console.log` in source code.

| Level | What you see |
|-------|-------------|
| `error` | RPC failures, storage errors |
| `warning` | Reorg detection, parent hash mismatches |
| `info` | Start/stop, backfill progress, reorg handled |
| `debug` | Per-chunk detail, queries, storage init |
| `trace` | Every RPC call and poll tick |

**Production**: `logLevel: "info"`. **Debugging**: `"debug"`. **Silent**: `enableTelemetry: false`.

## Operational notes

- One writer process per SQLite file. This is not a suggestion.
- Keep the DB on persistent storage.
- On restart, the indexer resumes from the last checkpoint.
- RPC must support `eth_getLogs` — if it doesn't, nothing will work.
- Graceful shutdown: `Ctrl+C` or `kill <pid>` — the worker finishes the current operation and writes the checkpoint.

## Development

```bash
npm run build
npm run typecheck
npm run test
npm run check
```

## License

Free for noncommercial use under PolyForm Noncommercial 1.0.0.
Commercial use requires a paid license — see `LICENSE`.
Contact: Aleksandr Shenshin <shenshin@me.com>.

Repository: [github.com/cybervoid0/effective-indexer](https://github.com/cybervoid0/effective-indexer)
