# Kitesurf local browser 0.5.0

A local Kitesurf browser for agents, with one SQLite profile and one live browser session.
The release executable includes Node 24, workerd, the browser engine and its fonts. It needs
no Cloudflare account, cloud deployment, Node installation or separate workerd installation.
On startup it expands its bundled runtime into a private temporary directory and starts workerd
as a child process. This is a single-file distribution, not a single-process browser.
Linux requires glibc 2.35 or later. Native macOS arm64 builds are provided by the release workflow.

## Start and connect

```sh
./kitesurf serve
./kitesurf status
```

The default endpoint is `ws://127.0.0.1:8787/devtools/browser`. Discovery is available at
`http://127.0.0.1:8787/json/version` and `/json/list`; `/session` returns the profile, live session
ID, process generation, controller and tabs. An agent should discover and observe the existing
session before deciding what to do after a disconnect. Do not replay a timed-out write blindly.

Connect with Puppeteer, Playwright or a CDP client. Use `browser.disconnect()` to leave the
browser running; `browser.close()` explicitly saves and exits. CDP clients are independent of
the browser lifetime. The local host removes the upstream ten-minute idle and one-hour session
deadlines; individual operation and network deadlines remain.

For a graphical login, open `chrome://inspect/#devices` in Chrome, enable network target
discovery, add `127.0.0.1:8787`, and inspect the desired tab. Use the DevTools screencast to enter
the website and log in. Disconnect the agent controller before connecting DevTools. Close the
DevTools window when finished; the browser keeps the same tabs and profile for the agent.

## Profiles and durability

```sh
./kitesurf profile create ./work.sqlite
./kitesurf serve --profile ./work.sqlite --port 8788
./kitesurf flush --port 8788
./kitesurf backup --port 8788 --output ./work-backup.sqlite
./kitesurf stop --port 8788
```

An explicit profile must already exist, unless `--create-profile` is supplied. Invalid,
unsupported or occupied files fail without silently opening a fresh account. SQLite holds an
exclusive lock for the browser lifetime; symlink aliases cannot open another owner, and hard-link
profiles are rejected. Backups use SQLite's consistent snapshot mechanism. Copy or replace a
profile only after stopping its browser; a journal may exist while it is open.

| State                                              | CDP disconnect/reconnect                     | Browser process restart      |
| -------------------------------------------------- | -------------------------------------------- | ---------------------------- |
| Cookies, including HttpOnly and their scope/expiry | Retained                                     | Restored from SQLite         |
| localStorage, separated by origin                  | Retained and shared between same-origin tabs | Restored before page scripts |
| sessionStorage, separated by tab and origin        | Retained, including navigation/reload        | Fresh                        |
| Tabs, live DOM, forms and JavaScript heap          | Same live pages                              | Fresh browser session        |
| Incognito browser contexts                         | Retained while live                          | Not persisted                |

Changes autosave after a short debounce. `flush`, `backup`, `stop`, and `Browser.close` wait for
pending storage and cookie writes. A failed save prevents a clean close; fix the storage failure
and retry. A force kill restores only committed state. A restart changes the session generation
and never pretends the former pages are still running. Website expiry and server-side revocation
still apply; storing a profile does not guarantee an account remains logged in forever.

The default file lives under `$XDG_DATA_HOME/kitesurf`, macOS
`~/Library/Application Support/kitesurf`, or Linux `~/.local/share/kitesurf`. Profiles contain
credentials in plaintext and are created with owner-only file permissions.

## Control and network

Only one controlling CDP connection is active at a time. The default owner is `default`.
An agent can connect with `?owner=agent-name&session=SESSION_ID`. A viewer connection uses
`?role=viewer` and can read the page and screenshots, but cannot evaluate arbitrary JavaScript,
type, navigate, or close the browser. Explicit takeover fences the old controller immediately:

```sh
./kitesurf control --owner human --takeover
```

The new controller connects with `?owner=human`. Takeover does not cancel already dispatched
commands; re-observe their outcome before acting. This is local session ownership, not a
multi-tenant authorization service. The control endpoint binds to loopback and checks Host/Origin.

Internet access uses the existing Kitesurf network stack. Page access to loopback/private
networks is blocked by default; enable it intentionally with `--allow-private-network` for local
sites. Existing cookie and origin policies remain in effect.

## Build and release

Install pnpm 9.15.4 and Bazelisk, then run from the repository root:

```sh
pnpm install --frozen-lockfile
bazel run //packages/local-browser:package
bazel test //packages/local-browser:profile_test //packages/local-browser:browser_test
pnpm run check
```

The executable appears at `dist/local-browser/kitesurf`, alongside licenses, a dependency
inventory and SHA-256 checksum. The GitHub Actions browser release workflow builds from source
for Linux x64/arm64 and macOS arm64. A `v0.5.*` tag prepares a draft GitHub release. Manual runs
produce downloadable artifacts. No Cloudflare secrets or infrastructure are required. The
upstream terminal client has a separate `cli-v*` release trigger.

## Boundaries

This remains an experimental Kitesurf browser. IndexedDB, Cache Storage and service-worker
state are not added to profile persistence. A disk profile is not a snapshot of a running
computer. WebGL, video and other rendering improvements follow upstream. Individual sites,
SSO providers and anti-bot flows may require browser features Kitesurf does not yet implement.
Linux and macOS builds are native to each runner; this release does not cross-compile workerd.
