// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { existsSync, mkdirSync } from "node:fs";
import { homedir } from "node:os";
import { resolve, dirname } from "node:path";
import { pathToFileURL } from "node:url";
import { ProfileStore } from "./profile-store.mjs";

const HELP = `Kitesurf 0.5.0 — a local browser with persistent profiles

kitesurf serve [--profile FILE] [--create-profile] [--port 8787]
               [--allow-private-network]
kitesurf profile create FILE
kitesurf status|flush|stop [--port 8787]
kitesurf backup --output FILE [--port 8787]
kitesurf control --owner NAME [--takeover] [--port 8787]

The default profile is created once under your user data directory.
An explicit --profile must already exist unless --create-profile is used.
Disconnecting CDP keeps tabs alive. Browser.close saves and exits.
`;

/** @returns {Promise<void>} */
export async function main(
  argv = process.argv.slice(2),
  root = resolve(import.meta.dirname, ".."),
) {
  if (argv.includes("--version")) {
    console.log("kitesurf 0.5.0");
    return;
  }
  if (argv.includes("--help") || argv.includes("-h")) {
    console.log(HELP);
    return;
  }
  const command = argv[0] && !argv[0].startsWith("-") ? argv.shift() : "serve";
  if (command === "profile") {
    if (argv.length !== 2 || argv[0] !== "create")
      throw new Error("Usage: kitesurf profile create FILE");
    const profile = new ProfileStore(argv[1], { create: true });
    console.log(JSON.stringify(profile.status()));
    profile.close();
    return;
  }
  const options = { port: 8787 };
  const names = {
    "--port": "port",
    "--profile": "profile",
    "--dist": "dist",
    "--assets": "assets",
    "--output": "output",
    "--owner": "owner",
  };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (["--create-profile", "--allow-private-network", "--takeover"].includes(arg))
      options[arg.slice(2)] = true;
    else if (names[arg] && argv[i + 1]) options[names[arg]] = argv[++i];
    else throw new Error(`Unknown or incomplete argument: ${arg}`);
  }
  options.port = Number(options.port);
  if (!Number.isInteger(options.port) || options.port < 0 || options.port > 65535)
    throw new Error("Invalid port");
  if (["status", "flush", "stop", "backup", "control"].includes(command)) {
    const route = {
      status: "session",
      stop: "close",
      flush: "flush",
      backup: "backup",
      control: "control",
    }[command];
    if (command === "backup" && !options.output) throw new Error("backup requires --output FILE");
    if (command === "control" && !options.owner) throw new Error("control requires --owner NAME");
    const response = await fetch(`http://127.0.0.1:${options.port}/${route}`, {
      method: command === "status" ? "GET" : "POST",
      ...(command === "backup" ? { body: JSON.stringify({ path: resolve(options.output) }) } : {}),
      ...(command === "control"
        ? { body: JSON.stringify({ owner: options.owner, takeover: !!options.takeover }) }
        : {}),
    });
    const text = await response.text();
    if (!response.ok) throw new Error(text);
    console.log(text);
    return;
  }
  if (command !== "serve") throw new Error(`Unknown command: ${command}`);
  const dataRoot =
    process.env.XDG_DATA_HOME ||
    (process.platform === "darwin"
      ? resolve(homedir(), "Library/Application Support")
      : process.platform === "win32"
        ? process.env.LOCALAPPDATA || resolve(homedir(), "AppData/Local")
        : resolve(homedir(), ".local/share"));
  const profilePath = resolve(options.profile || resolve(dataRoot, "kitesurf/default.sqlite"));
  if (!options.profile) mkdirSync(dirname(profilePath), { recursive: true, mode: 0o700 });
  const profile = new ProfileStore(profilePath, {
    create: options["create-profile"] || (!options.profile && !existsSync(profilePath)),
  });
  const { startBrowser } = await import("./server.mjs");
  let browser;
  try {
    browser = await startBrowser({
      profile,
      port: options.port,
      dist: resolve(options.dist || resolve(root, "engine")),
      assets: resolve(options.assets || resolve(root, "public")),
      allowPrivateNetwork: !!options["allow-private-network"],
    });
  } catch (error) {
    profile.close();
    throw error;
  }
  const shutdown = () => {
    void browser.close().catch((error) => console.error(`Close failed: ${error.message}`));
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
  console.log(
    JSON.stringify({
      ready: true,
      port: browser.port,
      profile: profile.path,
      generation: browser.generation,
    }),
  );
  const reason = await browser.done;
  process.off("SIGINT", shutdown);
  process.off("SIGTERM", shutdown);
  if (reason === "engine-lost")
    throw new Error("Browser engine was lost; restart explicitly to load the saved profile");
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  main().catch((error) => {
    console.error(`kitesurf: ${error.message}`);
    process.exitCode = 1;
  });
}
