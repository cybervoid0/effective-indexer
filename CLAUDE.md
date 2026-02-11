# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`effective-indexer` — a lightweight EVM smart contract event indexer built with Effect and viem. Works with any EVM-compatible chain. Published as a dual ESM/CJS package.

## Commands

```bash
npm run build          # Build with tsup (dual ESM/CJS + types)
npm run dev            # Build in watch mode
npm run typecheck      # tsc --noEmit
npm run check          # Biome format + lint (auto-fix)
npm test               # Run all unit tests (vitest)
npx vitest run test/storage.test.ts   # Run a single test file
npm run test:watch     # Vitest in watch mode
RUN_EVM_INTEGRATION=true npm run test:integration  # Live EVM integration test
```

## TypeScript Configuration

Strict mode is enabled with `exactOptionalPropertyTypes` and `noUncheckedIndexedAccess`. Path aliases: `@/*` → `src/*`, `@test/*` → `test/*`.

## Code Style

Biome handles formatting and linting. Key settings:
- Tabs for indentation, 80-char line width
- Semicolons: `asNeeded`, trailing commas: `all`, arrow parens: `asNeeded`
- Non-null assertions (`!.`) allowed in `test/**` only

## Architecture

### Effect-based dependency injection

Every service is a `Context.Tag` (namespaced `effective-indexer/*`) with a `Layer` implementation. Services are composed into an explicit dependency tree — not merged flat. The layer graph:

```
Config + SqliteClient (foundation)
├→ Storage (depends on SqliteClient)
├→ RpcProvider (depends on Config)
├→ CheckpointManager (depends on Storage)
├→ BlockCursor (depends on RpcProvider, Config)
├→ ReorgDetector (depends on Storage, Config)
├→ QueryApi (depends on Storage)
└→ EventDecoder (no deps — Layer.succeed)
```

### Network config structure

`IndexerConfig.network` is the primary config for chain-specific tuning:
- `network.polling` — `intervalMs`, `confirmations`
- `network.logs` — `chunkSize`, `maxRetries`, `retry.baseDelayMs`, `retry.maxDelayMs`
- `network.reorg` — `depth`

Old flat fields (`chunkSize`, `pollInterval`, `confirmations`, `maxRetries`, `reorgDepth`) are deprecated but still supported via backward-compatible mapping. `ResolvedConfig` keeps flat aliases computed from `network`.

### Pipeline flow (src/pipeline/)

Two-phase indexing per contract:
1. **Backfill** — fetch historical logs from startBlock to chain head, decode, verify blocks, store events, update checkpoint
2. **Live** — poll for new confirmed blocks via BlockCursor, process incrementally, handle reorgs by rewinding checkpoint and deleting affected events

Multiple contracts are indexed concurrently via `Stream.mergeAll`.

### Key source files

- `src/index.ts` — public API: `createIndexer(config)` returns an `IndexerHandle` with `start()`, `stop()`, `query()`, `count()`
- `src/config.ts` — `IndexerConfig` (user-facing) → `ResolvedConfig` (with defaults applied), `NetworkConfig` → `ResolvedNetworkConfig`
- `src/errors.ts` — tagged error types: `RpcError`, `DecodeError`, `StorageError`, `ReorgDetected`, `CheckpointError`, `ConfigError`
- `src/services/Storage.ts` — SQLite schema (events, checkpoints, block_hashes tables), batch insert with `INSERT OR IGNORE`
- `src/services/RpcProvider.ts` — viem `PublicClient` wrapper
- `src/services/EventDecoder.ts` — viem `decodeEventLog` with `strict: false`
- `src/pipeline/Indexer.ts` — orchestrates backfill + live phases
- `src/pipeline/ReorgDetector.ts` — parentHash chain verification, rollback on fork
- `src/pipeline/LogFetcher.ts` — chunked `eth_getLogs` with exponential backoff (capped by `maxDelayMs`)
- `src/query.ts` — query API for stored events (filter by contract, event, block range, tx hash)

### Testing patterns

- Unit tests use in-memory SQLite: `SqliteClient.layer({ filename: ":memory:" })`
- Test fixtures in `test/fixtures/` provide `makeBlock()`, `makeTransferLog()`, etc.
- Effect pipelines tested via `Effect.runPromise(effect.pipe(Effect.provide(TestLayer)))`
- RPC calls mocked with `vi.fn()` / `vi.mock()`

### EVM defaults

Default `intervalMs: 12000` (Ethereum ~12s blocks). Override for other chains via `network.polling.intervalMs`. `eth_getLogs` chunked to 5000 blocks per request. Retry uses exponential backoff from 1s base, capped at 30s.
