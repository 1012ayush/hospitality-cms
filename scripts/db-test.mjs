import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const projectName = `hcm-pgtap-${process.pid}`;
const databaseUrl = "postgres://hcm_test:hcm_test_local_only@localhost:55432/hcm_test?sslmode=disable";
const compose = (...args) => ["compose", "--project-name", projectName, "--profile", "pgtap", ...args];

function run(command, args, env = process.env) {
  const rendered = [command, ...args].join(" ");
  console.log(`> ${rendered}`);
  const result = spawnSync(command, args, {
    cwd: root,
    env,
    encoding: "utf8",
    maxBuffer: 8 * 1024 * 1024,
  });
  if (result.stdout) process.stdout.write(result.stdout);
  if (result.stderr) process.stderr.write(result.stderr);
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`Command failed (${result.status}): ${rendered}`);
  return result.stdout ?? "";
}

function atlasCommand() {
  const bundled = path.join(root, process.platform === "win32" ? "atlas.exe" : "atlas");
  if (existsSync(bundled)) return bundled;
  return process.platform === "win32" ? "atlas.exe" : "atlas";
}

let stackStarted = false;
try {
  stackStarted = true;
  run("docker", compose("up", "-d", "--build", "--wait", "--wait-timeout", "120", "postgres-pgtap"));

  const env = { ...process.env, DATABASE_URL: databaseUrl };
  run(atlasCommand(), ["migrate", "apply", "--env", "local"], env);
  run("docker", compose("exec", "-T", "postgres-pgtap", "psql", "-X", "-U", "hcm_test", "-d", "hcm_test", "-v", "ON_ERROR_STOP=1", "-f", "/workspace/roles-and-grants.sql"));

  const tap = run("docker", compose("exec", "-T", "postgres-pgtap", "psql", "-X", "-U", "hcm_test", "-d", "hcm_test", "-v", "ON_ERROR_STOP=1", "-f", "/workspace/tests/001_schema.pgtap.sql"));
  const passed = tap.match(/^ok\s+\d+\s+-/gm) ?? [];
  if (/^not ok\s+\d+/m.test(tap) || !tap.includes("1..10") || passed.length !== 10) {
    throw new Error(`pgTAP did not report ten passing assertions (found ${passed.length}).`);
  }
  console.log(`pgTAP passed: ${passed.length}/10 assertions.`);
} catch (error) {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
} finally {
  if (stackStarted) {
    try {
      run("docker", compose("down", "--volumes", "--remove-orphans"));
    } catch (error) {
      console.error(`Test stack cleanup failed: ${error instanceof Error ? error.message : error}`);
      process.exitCode = 1;
    }
  }
}
