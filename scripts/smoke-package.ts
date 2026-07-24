/**
 * Fresh-consumer smoke test for every declared package export.
 * Packs the current checkout, installs only the tarball in a temporary package,
 * and imports the public surface through Node's TypeScript stripping runtime.
 */

import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

interface PackageManifest {
  name: string;
  exports: Record<string, string>;
}

interface PackResult {
  filename: string;
}

const root = process.cwd();
const npmCliPath = process.env.npm_execpath;
if (!npmCliPath) {
  throw new Error("npm_execpath is required for cross-platform package smoke");
}
const runNpm = (
  args: string[],
  options: Parameters<typeof execFileSync>[2],
): ReturnType<typeof execFileSync> =>
  execFileSync(process.execPath, [npmCliPath, ...args], options);
const manifest = JSON.parse(
  readFileSync(path.join(root, "package.json"), "utf8"),
) as PackageManifest;
const consumer = mkdtempSync(path.join(tmpdir(), "pi-telegram-package-smoke-"));
let tarballPath: string | undefined;

try {
  const packed = JSON.parse(
    String(
      runNpm(["pack", "--json"], {
        cwd: root,
        encoding: "utf8",
        stdio: ["ignore", "pipe", "inherit"],
      }),
    ),
  ) as PackResult[];
  if (packed.length !== 1 || typeof packed[0]?.filename !== "string") {
    throw new Error("npm pack did not produce exactly one tarball");
  }
  tarballPath = path.join(root, packed[0].filename);
  writeFileSync(
    path.join(consumer, "package.json"),
    JSON.stringify({ name: "pi-telegram-package-smoke", private: true, type: "module" }),
  );
  runNpm(
    ["install", "--ignore-scripts", "tsx", tarballPath],
    { cwd: consumer, stdio: "inherit" },
  );

  const specifiers = Object.keys(manifest.exports).map((exportPath) =>
    exportPath === "." ? manifest.name : `${manifest.name}/${exportPath.slice(2)}`
  );
  const smokePath = path.join(consumer, "smoke.ts");
  writeFileSync(
    smokePath,
    `const specifiers = ${JSON.stringify(specifiers)};\n` +
      `for (const specifier of specifiers) await import(specifier);\n` +
      `console.log(\`Imported \${specifiers.length} public package exports from tarball.\`);\n`,
  );
  runNpm(["exec", "--offline", "--", "tsx", smokePath], {
    cwd: consumer,
    stdio: "inherit",
  });
} finally {
  if (tarballPath) rmSync(tarballPath, { force: true });
  rmSync(consumer, { recursive: true, force: true });
}
