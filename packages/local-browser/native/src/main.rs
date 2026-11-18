// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// T2 原生入口：解析显式启动参数，绑定私有服务，启动固定版本的 workerd，打开唯一的上游
// WebSocket，最后暴露 loopback 监听器。它绝不读取 engine-mf.json，也绝不使用 Node；Node
// 只存在于构建期配置生成器和外部测试驱动中。

use std::ffi::OsString;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::signal::unix::{signal, SignalKind};

use kitesurf_local_host::broker::{BrokerHandle, CloseKind};
use kitesurf_local_host::observe::Observer;
use kitesurf_local_host::profile::ProfileStore;
use kitesurf_local_host::relay::{HostServer, SessionInfo};
use kitesurf_local_host::services::{
    MemoryProfile, PrivateServices, ProfileBackend, ProfileService, SqliteProfile,
};
#[cfg(feature = "test-faults")]
use kitesurf_local_host::services::FaultProfile;
use kitesurf_local_host::supervisor::{self, EngineSocket, EngineSpec};
use kitesurf_local_host::transport::Upstream;
use kitesurf_local_host::{describe_error, HostResult};

const VERSION: &str = "0.5.0";

const HELP: &str = "\
Kitesurf native local host 0.5.0

kitesurf-native serve --workerd FILE --config FILE --assets-root DIR
                     --assets-manifest FILE --profile FILE [--create-profile]
                     [--port 8787] [--startup-timeout-ms N] [--test-observe FILE]
kitesurf-native serve --test-memory-profile [--profile-origin URL] ...
kitesurf-native profile create FILE
kitesurf-native profile status FILE
kitesurf-native profile backup FILE --output FILE
kitesurf-native --help | --version

The generated config carries the public/private network choice. This process owns the
workerd child, its sockets and the single upstream CDP connection.
";

struct Args {
    workerd: PathBuf,
    config: PathBuf,
    assets_root: PathBuf,
    assets_manifest: PathBuf,
    port: u16,
    profile: Option<PathBuf>,
    create_profile: bool,
    memory_profile: bool,
    profile_origin: String,
    startup_timeout_ms: u64,
    observe: Option<PathBuf>,
}

impl Args {
    fn parse(argv: Vec<OsString>) -> HostResult<Self> {
        let mut rest: Vec<OsString> = argv.into_iter().collect();
        if let Some(first) = rest.first() {
            let text = first.to_string_lossy().to_string();
            if text == "--help" || text == "-h" {
                print!("{HELP}");
                std::process::exit(0);
            }
            if text == "--version" {
                println!("kitesurf {VERSION}");
                std::process::exit(0);
            }
            if text == "serve" {
                rest.remove(0);
            } else if !text.starts_with('-') {
                return Err(format!("unknown command: {text}").into());
            }
        }
        let mut args = Args {
            workerd: PathBuf::new(),
            config: PathBuf::new(),
            assets_root: PathBuf::new(),
            assets_manifest: PathBuf::new(),
            port: 8787,
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
        let mut rest = rest.into_iter();
        while let Some(argument) = rest.next() {
            let argument = argument.to_string_lossy().to_string();
            if argument == "--help" || argument == "-h" {
                print!("{HELP}");
                std::process::exit(0);
            }
            if argument == "--version" {
                println!("kitesurf {VERSION}");
                std::process::exit(0);
            }
            let mut take = |flag: &str| -> HostResult<String> {
                rest.next()
                    .map(|value| value.to_string_lossy().to_string())
                    .ok_or_else(|| format!("{flag} requires a value").into())
            };
            match argument.as_str() {
                "--workerd" => workerd = Some(PathBuf::from(take("--workerd")?)),
                "--config" => config = Some(PathBuf::from(take("--config")?)),
                "--assets-root" => assets_root = Some(PathBuf::from(take("--assets-root")?)),
                "--assets-manifest" => {
                    assets_manifest = Some(PathBuf::from(take("--assets-manifest")?))
                }
                "--profile-origin" => args.profile_origin = take("--profile-origin")?,
                "--profile" => args.profile = Some(PathBuf::from(take("--profile")?)),
                "--create-profile" => args.create_profile = true,
                "--test-memory-profile" => args.memory_profile = true,
                "--port" => {
                    args.port = take("--port")?.parse::<u16>()?;
                }
                "--startup-timeout-ms" => {
                    let millis = take("--startup-timeout-ms")?.parse::<u64>()?;
                    if millis < 100 {
                        return Err("--startup-timeout-ms must be at least 100".into());
                    }
                    args.startup_timeout_ms = millis;
                }
                "--test-observe" => args.observe = Some(PathBuf::from(take("--test-observe")?)),
                "--allow-private-network" => {}
                other => return Err(format!("unknown or incomplete argument: {other}").into()),
            }
        }
        args.workerd = workerd.ok_or("--workerd is required")?;
        args.config = config.ok_or("--config is required")?;
        args.assets_root = assets_root.ok_or("--assets-root is required")?;
        args.assets_manifest = assets_manifest.ok_or("--assets-manifest is required")?;
        if !args.workerd.is_file() {
            return Err(format!("workerd not found: {}", args.workerd.display()).into());
        }
        if !args.config.is_file() {
            return Err(format!("config not found: {}", args.config.display()).into());
        }
        if !args.assets_root.is_dir() {
            return Err(format!("assets root not found: {}", args.assets_root.display()).into());
        }
        if !args.assets_manifest.is_file() {
            return Err(format!(
                "assets manifest not found: {}",
                args.assets_manifest.display()
            )
            .into());
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
            (None, false) => {
                return Err("serve requires --profile FILE or --test-memory-profile".into())
            }
            (None, true) if args.create_profile => {
                return Err("--create-profile requires --profile".into())
            }
            _ => {}
        }
        Ok(args)
    }
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

    fn path(&self) -> &std::path::Path {
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

/// 仅保留 T3 需要的 profile 入口；完整 CLI 迁移属于后续任务。
fn run_profile_command(args: &[OsString]) -> HostResult<()> {
    let command = args
        .first()
        .map(|argument| argument.to_string_lossy().to_string())
        .ok_or("usage: profile create|status|backup FILE")?;
    let path = args
        .get(1)
        .map(PathBuf::from)
        .ok_or("profile command requires a FILE")?;
    match command.as_str() {
        "create" => {
            let store = ProfileStore::open(&path, true)?;
            print_json(store.status()?.to_json())?;
        }
        "status" => {
            let store = ProfileStore::open(&path, false)?;
            print_json(store.status()?.to_json())?;
        }
        "backup" => {
            let mut output = None;
            let mut rest = args[2..].iter();
            while let Some(argument) = rest.next() {
                if argument.to_string_lossy() == "--output" {
                    output = Some(PathBuf::from(
                        rest.next()
                            .ok_or("--output requires a value")?
                            .to_string_lossy()
                            .to_string(),
                    ));
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
    if argv
        .first()
        .is_some_and(|argument| argument.to_string_lossy() == "profile")
    {
        run_profile_command(&argv[1..])?;
        return Ok(true);
    }
    let args = Args::parse(argv)?;
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
    let backend: Arc<dyn ProfileBackend> = match &args.profile {
        Some(path) => Arc::new(SqliteProfile::open(path, args.create_profile)?),
        None => Arc::new(MemoryProfile::new(&args.profile_origin)?),
    };
    #[cfg(feature = "test-faults")]
    let backend: Arc<dyn ProfileBackend> = match std::env::var_os("KITESURF_TEST_FAULTS") {
        Some(path) if !path.is_empty() => Arc::new(FaultProfile::new(backend, PathBuf::from(path))),
        _ => backend,
    };
    let profile_service = Arc::new(ProfileService::new(backend));
    let services = PrivateServices::bind(
        run_dir.path(),
        &args.assets_root,
        &args.assets_manifest,
        profile_service.clone(),
        observer.clone(),
    )?;
    let socket = EngineSocket::Unix(run_dir.path().join("engine.sock"));
    let externals = vec![
        ("profile".to_string(), services.paths().profile.clone()),
        ("assets".to_string(), services.paths().assets.clone()),
    ];
    let spec = EngineSpec::workerd(
        &args.workerd,
        &args.config,
        socket,
        &externals,
        Duration::from_millis(args.startup_timeout_ms),
        observer.clone(),
    );
    let mut engine = supervisor::spawn(spec).await?;
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
            drop(services);
            drop(profile_service);
            let dir_path = run_dir.path().to_path_buf();
            drop(run_dir);
            observer.with("shutdown", json!({ "clean": false, "exit": exit.to_string(), "dirRemoved": !dir_path.exists() }));
            return Err(error);
        }
    };
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;
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
    drop(profile_service);
    let dir_path = run_dir.path().to_path_buf();
    drop(run_dir);
    observer.with(
    "shutdown",
    json!({ "kind": "shutdown", "clean": clean, "exit": exit.to_string(), "dirRemoved": !dir_path.exists() }),
  );
    if !clean {
        eprintln!(
            "kitesurf: browser engine was lost; restart explicitly to load the saved profile"
        );
    }
    Ok(clean)
}
