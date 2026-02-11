import { defineConfig } from "tsup";

export default defineConfig({
	entry: ["src/index.ts"],
	format: ["cjs", "esm"],
	dts: true,
	clean: true,
	sourcemap: true,
	splitting: false,
	treeshake: true,
	external: [
		"effect",
		"@effect/sql",
		"@effect/sql-sqlite-node",
		"@effect/platform",
		"@effect/platform-node",
		"@effect/schema",
		"better-sqlite3",
	],
});
