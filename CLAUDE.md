# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`@rootstock/indexer` — a lightweight smart contract event indexer for Rootstock (RSK) blockchain, built with Effect and viem. Published as a dual ESM/CJS package.

## Commands

```bash
npm run build          # Build with tsup (dual ESM/CJS + types)
npm run dev            # Build in watch mode
npm run typecheck      # tsc --noEmit
npm run check          # Biome format + lint (auto-fix)
npm test               # Run all unit tests (vitest)
npx vitest run test/storage.test.ts   # Run a single test file
npm run test:watch     # Vitest in watch mode
RUN_RSK_INTEGRATION=true npm run test:integration  # Live RSK testnet test
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

Every service is a `Context.Tag` with a `Layer` implementation. Services are composed into an explicit dependency tree — not merged flat. The layer graph:

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

### Pipeline flow (src/pipeline/)

Two-phase indexing per contract:
1. **Backfill** — fetch historical logs from startBlock to chain head, decode, verify blocks, store events, update checkpoint
2. **Live** — poll for new confirmed blocks via BlockCursor, process incrementally, handle reorgs by rewinding checkpoint and deleting affected events

Multiple contracts are indexed concurrently via `Stream.mergeAll`.

### Key source files

- `src/index.ts` — public API: `createIndexer(config)` returns an `IndexerHandle` with `start()`, `stop()`, `query()`, `count()`
- `src/config.ts` — `IndexerConfig` (user-facing) → `ResolvedConfig` (with defaults applied)
- `src/errors.ts` — tagged error types: `RpcError`, `DecodeError`, `StorageError`, `ReorgDetected`, `CheckpointError`, `ConfigError`
- `src/services/Storage.ts` — SQLite schema (events, checkpoints, block_hashes tables), batch insert with `INSERT OR IGNORE`
- `src/services/RpcProvider.ts` — viem `PublicClient` wrapper
- `src/services/EventDecoder.ts` — viem `decodeEventLog` with `strict: false`
- `src/pipeline/Indexer.ts` — orchestrates backfill + live phases
- `src/pipeline/ReorgDetector.ts` — parentHash chain verification, rollback on fork
- `src/query.ts` — query API for stored events (filter by contract, event, block range, tx hash)

### Testing patterns

- Unit tests use in-memory SQLite: `SqliteClient.layer({ filename: ":memory:" })`
- Test fixtures in `test/fixtures/` provide `makeBlock()`, `makeTransferLog()`, etc.
- Effect pipelines tested via `Effect.runPromise(effect.pipe(Effect.provide(TestLayer)))`
- RPC calls mocked with `vi.fn()` / `vi.mock()`

### RSK-specific defaults

~30s block time → 15s poll interval. `eth_getLogs` chunked to 5000 blocks per request.
