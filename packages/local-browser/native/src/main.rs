// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// The local host starts workerd and opens one upstream CDP connection before publishing its port.

#[cfg(any(test, feature = "embedded-runtime"))]
mod bundle;

use std::ffi::{OsStr, OsString};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::timeout;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Body;
use hyper::header::{CONTENT_TYPE, HOST};
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;

use kitesurf_local_host::broker::{BrokerHandle, CloseKind};
use kitesurf_local_host::fonts::FontService;
use kitesurf_local_host::observe::Observer;
use kitesurf_local_host::profile::ProfileStore;
use kitesurf_local_host::relay::{HostServer, SessionInfo};
use kitesurf_local_host::services::{
    MemoryProfile, PrivateServices, ProfileBackend, ProfileService, SqliteProfile,
};
#[cfg(feature = "test-faults")]
use kitesurf_local_host::services::FaultProfile;
use kitesurf_local_host::supervisor::{self, EngineSocket, EngineSpec};
use kitesurf_local_host::transport::{Upstream, MAX_FRAME_BYTES};
use kitesurf_local_host::{describe_error, HostResult};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const DEFAULT_PORT: u16 = 8787;

const CONTROL_TIMEOUT: Duration = Duration::from_secs(15);
const MANAGEMENT_TIMEOUT: Duration = Duration::from_secs(125);
const SMALL_RESPONSE_BYTES: usize = 64 * 1024;

#[cfg(feature = "embedded-runtime")]
const HELP: &str = concat!(
    "Kitesurf ",
    env!("CARGO_PKG_VERSION"),
    " — a local browser with persistent profiles

kitesurf serve [--profile FILE] [--create-profile] [--port 8787]
               [--allow-private-network]
kitesurf profile create|status FILE
kitesurf profile backup FILE --output FILE
kitesurf status|flush|stop [--port 8787]
kitesurf backup --output FILE [--port 8787]
kitesurf control --owner NAME [--takeover] [--port 8787]

The default profile is created once under your user data directory.
An explicit --profile must already exist unless --create-profile is used.
Disconnecting CDP keeps tabs alive. Browser.close saves and exits.
"
);

#[cfg(not(feature = "embedded-runtime"))]
const HELP: &str = concat!(
    "Kitesurf native local host ",
    env!("CARGO_PKG_VERSION"),
    "

kitesurf-native serve --workerd FILE --config FILE --assets-root DIR
                     --assets-manifest FILE --profile FILE [--create-profile]
                     [--port 8787] [--startup-timeout-ms N] [--test-observe FILE]
kitesurf-native serve --test-memory-profile [--profile-origin URL] ...
kitesurf-native profile create FILE
kitesurf-native profile status FILE
kitesurf-native profile backup FILE --output FILE
kitesurf-native status|flush|stop [--port 8787]
kitesurf-native backup --output FILE [--port 8787]
kitesurf-native control --owner NAME [--takeover] [--port 8787]
kitesurf-native --help | --version

The generated config carries the public/private network choice. This process owns the
workerd child, its sockets and the single upstream CDP connection.
"
);

struct RuntimePaths {
    workerd: PathBuf,
    config: PathBuf,
    assets_root: PathBuf,
    assets_manifest: PathBuf,
}

struct Args {
    runtime: Option<RuntimePaths>,
    #[cfg_attr(not(feature = "embedded-runtime"), allow(dead_code))]
    allow_private_network: bool,
    port: u16,
    profile: Option<PathBuf>,
    create_profile: bool,
    memory_profile: bool,
    profile_origin: String,
    startup_timeout_ms: u64,
    observe: Option<PathBuf>,
}

impl Args {
    fn parse(argv: &[OsString]) -> HostResult<Self> {
        let mut args = Args {
            runtime: None,
            allow_private_network: false,
            port: DEFAULT_PORT,
            profile: None,
            create_profile: false,
            memory_profile: false,
            profile_origin: "http://profile.invalid".to_string(),
            startup_timeout_ms: 30_000,
            observe: None,
        };
        let mut workerd = None;
        let mut config = None;
        let mut assets_root = None;
        let mut assets_manifest = None;
        let mut rest = argv.iter();
        while let Some(argument) = rest.next() {
            let argument = argument.to_string_lossy();
            let mut take = || take_arg(&mut rest, &argument);
            match argument.as_ref() {
                "--workerd" => workerd = Some(PathBuf::from(take()?)),
                "--config" => config = Some(PathBuf::from(take()?)),
                "--assets-root" => assets_root = Some(PathBuf::from(take()?)),
                "--assets-manifest" => assets_manifest = Some(PathBuf::from(take()?)),
                "--profile-origin" => args.profile_origin = take()?.to_string_lossy().into_owned(),
                "--profile" => args.profile = Some(PathBuf::from(take()?)),
                "--create-profile" => args.create_profile = true,
                "--test-memory-profile" => args.memory_profile = true,
                "--port" => {
                    args.port = take()?
                        .to_string_lossy()
                        .parse()
                        .map_err(|_| "Invalid port")?;
                }
                "--startup-timeout-ms" => {
                    let millis = take()?.to_string_lossy().parse::<u64>()?;
                    if millis < 100 {
                        return Err("--startup-timeout-ms must be at least 100".into());
                    }
                    args.startup_timeout_ms = millis;
                }
                "--test-observe" => args.observe = Some(PathBuf::from(take()?)),
                "--allow-private-network" => args.allow_private_network = true,
                other => return Err(format!("Unknown or incomplete argument: {other}").into()),
            }
        }
        if !args.profile_origin.starts_with("http://")
            && !args.profile_origin.starts_with("https://")
        {
            return Err("--profile-origin must be an http(s) origin".into());
        }
        match (&args.profile, args.memory_profile) {
            (Some(_), true) => {
                return Err("--profile and --test-memory-profile are mutually exclusive".into())
            }
            (None, true) if args.create_profile => {
                return Err("--create-profile requires --profile".into())
            }
            _ => {}
        }
        if workerd.is_some()
            || config.is_some()
            || assets_root.is_some()
            || assets_manifest.is_some()
        {
            args.runtime = Some(RuntimePaths {
                workerd: workerd.ok_or("--workerd is required")?,
                config: config.ok_or("--config is required")?,
                assets_root: assets_root.ok_or("--assets-root is required")?,
                assets_manifest: assets_manifest.ok_or("--assets-manifest is required")?,
            });
        }
        if let Some(runtime) = &args.runtime {
            runtime.validate()?;
        }
        #[cfg(not(feature = "embedded-runtime"))]
        if args.runtime.is_none() {
            return Err("serve requires --workerd FILE --config FILE --assets-root DIR --assets-manifest FILE".into());
        }
        Ok(args)
    }
}

impl RuntimePaths {
    fn validate(&self) -> HostResult<()> {
        for (name, path) in [
            ("workerd", &self.workerd),
            ("config", &self.config),
            ("assets manifest", &self.assets_manifest),
        ] {
            if !path.is_file() {
                return Err(format!("{name} not found: {}", path.display()).into());
            }
        }
        if !self.assets_root.is_dir() {
            return Err(format!("assets root not found: {}", self.assets_root.display()).into());
        }
        Ok(())
    }
}

fn take_arg<'a>(
    args: &mut impl Iterator<Item = &'a OsString>,
    flag: &str,
) -> HostResult<&'a OsStr> {
    args.next()
        .map(OsString::as_os_str)
        .ok_or_else(|| format!("{flag} requires a value").into())
}

struct RunDir {
    dir: tempfile::TempDir,
}

impl RunDir {
    fn create() -> HostResult<Self> {
        let dir = tempfile::Builder::new()
            .prefix("kitesurf-host-")
            .tempdir_in(std::env::temp_dir())?;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))?;
        let longest = dir.path().join("profile.sock");
        if longest.as_os_str().len() > 100 {
            return Err(format!(
                "run directory is too long for a Unix socket: {}",
                longest.display()
            )
            .into());
        }
        Ok(Self { dir })
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }
}

fn generation() -> HostResult<String> {
    let mut bytes = [0u8; 16];
    getrandom::getrandom(&mut bytes).map_err(|error| std::io::Error::other(error.to_string()))?;
    let mut hex = String::with_capacity(32);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(hex, "{byte:02x}");
    }
    Ok(hex)
}

fn run_profile_command(args: &[OsString]) -> HostResult<()> {
    let command = args
        .first()
        .map(|argument| argument.to_string_lossy())
        .ok_or("usage: profile create|status|backup FILE")?;
    let path = args
        .get(1)
        .map(PathBuf::from)
        .ok_or("profile command requires a FILE")?;
    match command.as_ref() {
        "create" | "status" => {
            if args.len() != 2 {
                return Err(format!("Usage: kitesurf profile {command} FILE").into());
            }
            let store = ProfileStore::open(&path, command == "create")?;
            print_json(store.status()?.to_json())?;
        }
        "backup" => {
            let mut output = None;
            let mut rest = args[2..].iter();
            while let Some(argument) = rest.next() {
                if argument.to_string_lossy() == "--output" {
                    output = Some(PathBuf::from(take_arg(&mut rest, "--output")?));
                } else {
                    return Err("usage: profile backup FILE --output FILE".into());
                }
            }
            let output = output.ok_or("--output is required")?;
            let store = ProfileStore::open(&path, false)?;
            let destination = store.backup(&output)?;
            print_json(json!({ "path": destination.display().to_string() }))?;
        }
        other => return Err(format!("unknown profile command: {other}").into()),
    }
    Ok(())
}

fn print_json(value: serde_json::Value) -> HostResult<()> {
    println!("{value}");
    std::io::stdout().flush()?;
    Ok(())
}

// Management runs before any runtime extraction or profile I/O. Requests are never retried.
async fn run_management_command(command: &str, args: &[OsString]) -> HostResult<()> {
    let mut owner = None;
    let mut output = None;
    let mut takeover = false;
    let mut port = DEFAULT_PORT;
    let mut rest = args.iter();
    while let Some(argument) = rest.next() {
        let argument = argument.to_string_lossy();
        let mut take = || take_arg(&mut rest, &argument);
        match argument.as_ref() {
            "--owner" if command == "control" => owner = Some(take()?.to_string_lossy()),
            "--takeover" if command == "control" => takeover = true,
            "--output" if command == "backup" => output = Some(take()?),
            "--port" => {
                port = take()?
                    .to_string_lossy()
                    .parse::<u16>()
                    .map_err(|_| "--port must be an integer from 0 to 65535".to_string())?;
            }
            other => return Err(format!("unknown or incomplete argument: {other}").into()),
        }
    }
    let (method, route, body) = match command {
        "status" => (Method::GET, "/session", None),
        "flush" => (Method::POST, "/flush", None),
        "stop" => (Method::POST, "/close", None),
        "backup" => {
            let output = output
                .filter(|value| !value.is_empty())
                .ok_or("backup requires --output FILE")?;
            let path = std::path::absolute(output)?;
            let path = path
                .to_str()
                .ok_or("backup output path must be valid UTF-8")?;
            (Method::POST, "/backup", Some(json!({ "path": path })))
        }
        "control" => {
            let owner = owner
                .filter(|value| !value.is_empty())
                .ok_or("control requires --owner NAME")?;
            (
                Method::POST,
                "/control",
                Some(json!({ "owner": owner, "takeover": takeover })),
            )
        }
        _ => unreachable!("only management commands reach this function"),
    };
    // Flush and close may first drain admitted CDP work before saving the profile.
    let deadline = if command == "control" {
        CONTROL_TIMEOUT
    } else {
        MANAGEMENT_TIMEOUT
    };
    let (status, body) = timeout(
        deadline,
        management_exchange(command, port, method, route, body),
    )
    .await
    .map_err(|_| format!("{command} request to 127.0.0.1:{port} timed out"))??;
    let text = String::from_utf8_lossy(&body);
    if !status.is_success() {
        let message = text.trim_end_matches(['\r', '\n']);
        if message.is_empty() {
            return Err(format!("{command} request failed with status {}", status.as_u16()).into());
        }
        return Err(message.to_string().into());
    }
    println!("{text}");
    std::io::stdout().flush()?;
    Ok(())
}

async fn management_exchange(
    command: &str,
    port: u16,
    method: Method,
    route: &str,
    body: Option<serde_json::Value>,
) -> HostResult<(StatusCode, Vec<u8>)> {
    let stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .map_err(|error| format!("cannot connect to 127.0.0.1:{port}: {error}"))?;
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|error| format!("{command} HTTP handshake failed: {error}"))?;
    let mut request = Request::builder()
        .method(method)
        .uri(route)
        .header(HOST, format!("127.0.0.1:{port}"));
    let mut payload = Bytes::new();
    if let Some(body) = body {
        request = request.header(CONTENT_TYPE, "application/json");
        payload = Bytes::from(body.to_string());
    }
    let request = request
        .body(Full::new(payload))
        .map_err(|error| format!("{command} request is invalid: {error}"))?;
    let exchange = async move {
        let response = sender.send_request(request).await?;
        let status = response.status();
        let body = read_management_body(command, response.into_body()).await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((status, body))
    };
    let (result, _) = tokio::join!(exchange, connection);
    result
}

async fn read_management_body(
    command: &str,
    mut body: hyper::body::Incoming,
) -> HostResult<Vec<u8>> {
    let limit = if command == "status" {
        MAX_FRAME_BYTES + SMALL_RESPONSE_BYTES
    } else {
        SMALL_RESPONSE_BYTES
    };
    if body
        .size_hint()
        .upper()
        .is_some_and(|upper| upper > limit as u64)
    {
        return Err(format!("{command} response exceeds {limit} bytes").into());
    }
    let mut data = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame?;
        let Ok(chunk) = frame.into_data() else {
            continue;
        };
        if data.len().saturating_add(chunk.len()) > limit {
            return Err(format!("{command} response exceeds {limit} bytes").into());
        }
        data.extend_from_slice(&chunk);
    }
    Ok(data)
}

#[tokio::main]
async fn main() {
    let code = match run().await {
        Ok(true) => 0,
        Ok(false) => 1,
        Err(error) => {
            eprintln!("kitesurf: {}", describe_error(&*error));
            1
        }
    };
    std::process::exit(code);
}

async fn run() -> HostResult<bool> {
    let argv: Vec<OsString> = std::env::args_os().skip(1).collect();
    // The legacy CLI recognizes help and version even where an argument value is expected.
    if argv.iter().any(|argument| argument == "--version") {
        println!("kitesurf {VERSION}");
        return Ok(true);
    }
    if argv
        .iter()
        .any(|argument| argument == "--help" || argument == "-h")
    {
        print!("{HELP}");
        return Ok(true);
    }
    let (command, rest) = match argv.split_first() {
        Some((first, rest)) if !first.as_encoded_bytes().starts_with(b"-") => {
            (first.to_string_lossy(), rest)
        }
        _ => ("serve".into(), argv.as_slice()),
    };
    match command.as_ref() {
        "profile" => run_profile_command(rest)?,
        "status" | "flush" | "stop" | "backup" | "control" => {
            run_management_command(&command, rest).await?
        }
        "serve" => return run_serve(Args::parse(rest)?).await,
        other => return Err(format!("Unknown command: {other}").into()),
    }
    Ok(true)
}

fn open_profile(args: &Args) -> HostResult<Arc<dyn ProfileBackend>> {
    if let Some(path) = &args.profile {
        return Ok(Arc::new(SqliteProfile::open(path, args.create_profile)?));
    }
    if args.memory_profile {
        return Ok(Arc::new(MemoryProfile::new(&args.profile_origin)?));
    }
    let data_home = match std::env::var_os("XDG_DATA_HOME").filter(|value| !value.is_empty()) {
        Some(directory) => PathBuf::from(directory),
        None => {
            let home = std::env::home_dir().ok_or("cannot determine the user data directory")?;
            if cfg!(target_os = "macos") {
                home.join("Library/Application Support")
            } else {
                home.join(".local/share")
            }
        }
    };
    let folder = data_home.join("kitesurf");
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&folder)?;
    let path = folder.join("default.sqlite");
    let create = args.create_profile || !path.try_exists()?;
    Ok(Arc::new(SqliteProfile::open(&path, create)?))
}

async fn run_serve(mut args: Args) -> HostResult<bool> {
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;
    let observer = match &args.observe {
        Some(path) => Arc::new(Observer::create(path)?),
        None => Arc::new(Observer::disabled()),
    };
    let run_dir = RunDir::create()?;
    let mode = std::fs::metadata(run_dir.path())?.permissions().mode() & 0o777;
    observer.with(
        "run-dir",
        json!({
          "kind": "run-dir",
          "path": run_dir.path().display().to_string(),
          "mode": mode,
        }),
    );
    let backend = open_profile(&args)?;
    let runtime = match args.runtime.take() {
        Some(explicit) => explicit,
        #[cfg(feature = "embedded-runtime")]
        None => bundle::unpack(run_dir.path(), args.allow_private_network)?,
        #[cfg(not(feature = "embedded-runtime"))]
        None => unreachable!("the development host requires explicit runtime files"),
    };
    #[cfg(feature = "test-faults")]
    let backend: Arc<dyn ProfileBackend> = match std::env::var_os("KITESURF_TEST_FAULTS") {
        Some(path) if !path.is_empty() => Arc::new(FaultProfile::new(backend, PathBuf::from(path))),
        _ => backend,
    };
    let profile_service = Arc::new(ProfileService::new(backend));
    let mut fonts = match FontService::start(run_dir.path()).await {
        Ok(fonts) => fonts,
        Err(error) => {
            let dir_path = run_dir.path().to_path_buf();
            drop(run_dir);
            observer.with(
                "shutdown",
                json!({ "clean": false, "exit": "font-startup", "dirRemoved": !dir_path.exists() }),
            );
            return Err(error);
        }
    };
    let services = match PrivateServices::bind(
        run_dir.path(),
        &runtime.assets_root,
        &runtime.assets_manifest,
        profile_service.clone(),
        observer.clone(),
    ) {
        Ok(services) => services,
        Err(error) => {
            let fonts_clean = fonts.shutdown().await;
            let dir_path = run_dir.path().to_path_buf();
            drop(run_dir);
            observer.with(
                "shutdown",
                json!({ "clean": false, "exit": "service-startup", "fonts": fonts_clean, "dirRemoved": !dir_path.exists() }),
            );
            return Err(error);
        }
    };
    let socket = EngineSocket::Unix(run_dir.path().join("engine.sock"));
    let externals = vec![
        ("profile".to_string(), services.paths().profile.clone()),
        ("assets".to_string(), services.paths().assets.clone()),
        ("fonts".to_string(), fonts.path().to_path_buf()),
    ];
    let spec = EngineSpec::workerd(
        &runtime.workerd,
        &runtime.config,
        socket,
        &externals,
        Duration::from_millis(args.startup_timeout_ms),
        observer.clone(),
    );
    let mut engine = match supervisor::spawn(spec).await {
        Ok(engine) => engine,
        Err(error) => {
            services.shutdown().await;
            let fonts_clean = fonts.shutdown().await;
            drop(services);
            drop(profile_service);
            let dir_path = run_dir.path().to_path_buf();
            drop(run_dir);
            observer.with(
                "shutdown",
                json!({ "clean": false, "exit": "engine-spawn", "fonts": fonts_clean, "dirRemoved": !dir_path.exists() }),
            );
            return Err(error);
        }
    };
    let started = async {
        engine.wait_ready().await?;
        let mut upstream = Upstream::connect(&engine.endpoint(), observer.clone()).await?;
        let session = upstream
            .request("Kitesurf.getSessionInfo", None, Duration::from_secs(30))
            .await?;
        let session = SessionInfo::from_raw(&session, &upstream.handle.budget, VERSION)?;
        if session.session_id.is_empty() {
            return Err("engine returned no session id".into());
        }
        let generation = generation()?;
        let status = profile_service
            .status()
            .await
            .map_err(|error| error.to_string())?;
        let recovery = if status["revision"].as_i64().unwrap_or(0) > 0 {
            "profile-restored"
        } else {
            "new-profile"
        };
        engine.check_diagnostics()?;
        let broker = BrokerHandle::start(upstream, profile_service.clone(), observer.clone())
            .with_diagnostics(engine.fatal_watch());
        let host = HostServer::bind(
            args.port,
            broker.clone(),
            session,
            generation.clone(),
            recovery.to_string(),
            profile_service.clone(),
            observer.clone(),
        )?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((broker, host, generation))
    }
    .await;
    let (broker, host, generation) = match started {
        Ok(started) => started,
        Err(error) => {
            let exit = engine.shutdown().await;
            services.shutdown().await;
            let fonts_clean = fonts.shutdown().await;
            drop(services);
            drop(profile_service);
            let dir_path = run_dir.path().to_path_buf();
            drop(run_dir);
            observer.with("shutdown", json!({ "clean": false, "exit": exit.to_string(), "fonts": fonts_clean, "dirRemoved": !dir_path.exists() }));
            return Err(error);
        }
    };
    let port = host.port();
    let ready = json!({
      "ready": true,
      "port": port,
      "profile": profile_service.display(),
      "generation": generation,
    });
    println!("{ready}");
    std::io::stdout().flush()?;
    observer.with(
        "ready",
        json!({ "kind": "ready", "port": port, "generation": generation }),
    );

    let mut fatal = engine.fatal_watch();
    let clean = tokio::select! {
      _ = sigterm.recv() => {
        observer.kind("signal-sigterm");
        let mut fatal = engine.fatal_watch();
        tokio::select! {
            result = broker.close(CloseKind::Host, None, None, None) => result.is_ok(),
            _ = fatal.wait_for(|value| value.is_some()) => { broker.engine_lost(); false }
        }
      }
      _ = sigint.recv() => {
        observer.kind("signal-sigint");
        let mut fatal = engine.fatal_watch();
        tokio::select! {
            result = broker.close(CloseKind::Host, None, None, None) => result.is_ok(),
            _ = fatal.wait_for(|value| value.is_some()) => { broker.engine_lost(); false }
        }
      }
      clean = broker.wait_terminal() => clean,
      _ = fatal.wait_for(|value| value.is_some()) => {
        broker.engine_lost();
        false
      }
      exit = engine.wait_exit() => {
        // 正常 Browser.close 也可能先退出 workerd；等待 actor 消化已收到的关闭回复。
        if exit.code == Some(0) && engine.check_diagnostics().is_ok() {
            broker.wait_terminal().await
        } else {
            broker.engine_lost();
            false
        }
      }
    };

    host.stop().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let exit = engine.shutdown().await;
    let clean = clean
        && engine.check_diagnostics().is_ok()
        && broker.lost_watch().borrow().is_none()
        && (exit.code == Some(0) || exit.signal == Some(libc::SIGTERM));
    if engine.check_diagnostics().is_err() {
        broker.engine_lost();
    }
    services.shutdown().await;
    drop(services);
    let fonts_clean = fonts.shutdown().await;
    if !fonts_clean {
        eprintln!("kitesurf: system font worker did not shut down cleanly");
    }
    let clean = clean && fonts_clean;
    drop(profile_service);
    let dir_path = run_dir.path().to_path_buf();
    drop(run_dir);
    observer.with(
    "shutdown",
    json!({ "kind": "shutdown", "clean": clean, "fonts": fonts_clean, "exit": exit.to_string(), "dirRemoved": !dir_path.exists() }),
  );
    if !clean {
        eprintln!(
            "kitesurf: browser engine was lost; restart explicitly to load the saved profile"
        );
    }
    Ok(clean)
}
