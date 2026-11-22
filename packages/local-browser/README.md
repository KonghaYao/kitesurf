# Kitesurf local browser 0.5.3

A local Kitesurf browser for agents, with one SQLite profile and one live browser session.
The native Rust executable embeds workerd, engine modules and browser assets. It extracts the
runtime into a private temporary directory and starts workerd as a child process. The Rust host
owns the SQLite profile, the CDP connection and the system font service. A standalone copy of
`kitesurf` is enough to start the browser on a supported machine with installed fonts.

The release targets Linux x64, Linux arm64, and macOS arm64. On Linux the release builds target
glibc 2.35 or later and use Fontconfig. On macOS the browser uses Core Text.

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

Several page sessions can be controlled at the same time on their own
`ws://127.0.0.1:8787/devtools/page/{targetId}` connections. A page session's ID is its primary
target ID; one target belongs to one page session, and a second controller for the same page is
rejected. A page session's primary target is bound as soon as its controller connects, so another
session cannot attach it while the controller's first attach is still in flight. The
`/devtools/browser` endpoint keeps the single-controller rule above and still sees every target and
its discovery events. Provision pages through that endpoint first (`Target.createTarget`), then
connect one page session per target. A frame or popup belongs to the page session whose target the
engine reports as its `openerFrameId` or `openerId` parent. A page session can attach only targets
it already owns and their reported children; any target the engine reports as another session's
child, and any target with no reported owner, returns an explicit error instead of being attached.
Target exclusivity therefore covers frames and popups, not just top-level pages.

Page connections operate only their own target and CDP sessions. Instance-level methods such as
`Browser.close`, `Target.createTarget`, `Target.createBrowserContext`,
`Target.disposeBrowserContext`, `Storage.clearDataForOrigin`, `Storage.clearCookies`,
`Network.clearBrowserCookies` and `Network.clearBrowserCache` return an explicit CDP error instead
of being forwarded. `Kitesurf.*` is instance-level except for the per-target state, scene,
screencast and diagnostic methods that accept a `targetId`; a page connection must pass the
`targetId` of a target it owns, and target-less or otherwise instance-wide calls (including
`Kitesurf.setOutboundFilters`, `Kitesurf.disposeRenderer`, `Kitesurf.setPartialRaster`,
`Kitesurf.getMemoryStats` and `Kitesurf.getPageState` without a target) are rejected.
`Fetch.enable` and the rest of `Fetch.*` are rejected until Fetch interception can be isolated per
page. `Target.getTargets` and `/json/list?page={targetId}` list only the session's targets,
`Target.getTargetInfo` without a target defaults to the session's primary page, and other
sessions' target and attach events are not delivered. `Target.setAutoAttach` on a page connection
is rejected rather than silently doing nothing; use the `/devtools/browser` endpoint when
instance-wide auto-attach is required. `/json/list?page={targetId}` trusts its query parameter
under the loopback single-user trust model: it is a convenience filter, not an authorization check.
All page sessions share the instance's single SQLite profile, so they see the same cookies and
same-origin localStorage; use separate profiles for independent accounts. Viewer connections to
`/devtools/page/{targetId}` keep their existing attach and frame behavior.

Internet access uses the existing Kitesurf network stack. Page access to loopback/private
networks is blocked by default; enable it intentionally with `--allow-private-network` for local
sites. Existing cookie and origin policies remain in effect. Outbound requests go directly: the
bundled workerd does not read the system proxy configuration.

## Build and release

Install pnpm 9.15.4 and Bazelisk, then run from the repository root:

```sh
pnpm install --frozen-lockfile
bazel run -c opt //packages/local-browser:package
bazel test -c opt //packages/local-browser:binary_test \
  --test_env=KITESURF_TEST_BINARY="$PWD/dist/local-browser/kitesurf"
pnpm run check
```

The directory `dist/local-browser` contains `kitesurf`, the licenses, a component inventory
and `SHA256SUMS`. Pass `-- --archive "$PWD/dist"` to also create a versioned `.tar.gz` and its
archive checksum. The browser release workflow builds from source on each platform and tests
the executable extracted from the archive with an empty `PATH`. A matching `v0.5.*` tag
prepares a draft GitHub release. Manual runs upload the archives as workflow artifacts.
The upstream terminal client has a separate `cli-v*` release trigger.

The repository keeps the Node host as a compatibility test reference. The v0.5.0 release
contains the historical Node SEA executable.

Run the 61-minute idle test against the native release binary with an extended Bazel timeout.
The test disconnects CDP and checks the live page after reconnecting, without a heartbeat.

```sh
bazel test -c opt //packages/local-browser:native_soak_test --test_timeout=4200 --nocache_test_results
```

Use `//packages/local-browser:soak_test` with the same timeout to run the Node host baseline.

The release workflow also runs the memory acceptance on linux-x64 against the executable
extracted from the archive; see [Memory](#memory).

## Memory

The generated workerd config passes `--expose-gc` and `--no-flush-liftoff-code` to the
embedded workerd. workerd signals memory pressure for every new page isolate, so without the
second flag V8 drops the process-wide Liftoff code and recompiles Kitesurf's wasm on each
navigation. The v0.5.3 benchmark measured workerd on Linux x64: 4 live pages after 96
navigations took 1,107 MiB by default and 545 MiB with the flag. Eight live pages measured
about 457 MiB either way; eight pages typed in and screenshotted measured 568 and 585 MiB.
After closing every page and waiting 30 seconds the process held 1,082 and 510 MiB, and
reopening eight pages reached 1,234 MiB and 637 MiB (default first). The flag is part of the
release, not a benchmark-only setting.

`//packages/local-browser:workerd_memory_test` repeats the 4-page, 96-navigation case against
the archived release executable on the linux-x64 release runner. It sums the PSS of the native
host and its workerd child from `/proc/<pid>/smaps_rollup`, asserts the resolved workerd config
contains both flags, and fails above 800 MiB. The budget sits above the 545 MiB flagged run and
well below the 1,107 MiB default run. The test prints the startup, post-navigation and
after-close PSS; the last one is taken 30 seconds after closing every page, like the benchmark.
It skips on macOS and on non-x64 hosts, because that accounting is Linux-only and the budget is
calibrated for the linux-x64 runner.

Run it against a package built in this tree:

```sh
bazel run -c opt //packages/local-browser:package
bazel test -c opt //packages/local-browser:workerd_memory_test \
  --test_env=KITESURF_TEST_BINARY="$PWD/dist/local-browser/kitesurf"
```

This acceptance covers the Linux release binary with one host and one agent. It does not
measure the macOS host or concurrent agents, and it reports but does not attribute the memory
still held after every page closes.

## System fonts

The browser needs a usable sans-serif font to start. On Debian and Ubuntu,
`sudo apt install fontconfig fonts-dejavu-core` supplies the baseline. Install `fonts-noto-cjk`
for CJK coverage and `fonts-noto-color-emoji` for color emoji. A page can still load its own
fonts through CSS `@font-face`.

Run the native rendering test with CJK coverage required after installing a CJK font.

```sh
bazel test //packages/local-browser:native_system_fonts_test --test_env=KITESURF_REQUIRE_CJK=1
```

The release workflow requires CJK coverage on both Linux architectures.

## Boundaries

This remains an experimental Kitesurf browser. IndexedDB, Cache Storage and service-worker
state are not added to profile persistence. A disk profile is not a snapshot of a running
computer. WebGL, video and other rendering improvements follow upstream. Individual sites,
SSO providers and anti-bot flows may require browser features Kitesurf does not yet implement.
Linux and macOS builds are native to each runner; this release does not cross-compile workerd.
