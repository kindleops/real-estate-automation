import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";

const projectRoot = process.cwd();
const targets = ["src", "tests", "scripts"];
const files = [];

function walk(currentPath) {
  if (!fs.existsSync(currentPath)) return;

  const stat = fs.statSync(currentPath);
  if (stat.isFile()) {
    if (currentPath.endsWith(".js") || currentPath.endsWith(".mjs")) {
      files.push(currentPath);
    }
    return;
  }

  for (const entry of fs.readdirSync(currentPath)) {
    walk(path.join(currentPath, entry));
  }
}

for (const target of targets) {
  walk(path.join(projectRoot, target));
}

const failures = [];

for (const file of files) {
  const result = spawnSync(
    process.execPath,
    ["--check", file],
    {
      cwd: projectRoot,
      encoding: "utf8",
    }
  );

  if (result.status !== 0) {
    failures.push({
      file: path.relative(projectRoot, file),
      output: result.stderr || result.stdout || "Unknown syntax check failure",
    });
  }
}

if (failures.length) {
  for (const failure of failures) {
    console.error(`syntax check failed: ${failure.file}`);
    console.error(failure.output.trim());
  }
  process.exit(1);
}

console.log(`syntax check passed for ${files.length} files`);
