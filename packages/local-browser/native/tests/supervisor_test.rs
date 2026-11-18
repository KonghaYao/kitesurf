// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 真实 workerd 运行无法确定性覆盖的 supervisor 契约：控制管道故障、启动超时、提前退出、
// 继承监听器和 SIGTERM 回收。

use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use kitesurf_local_host::observe::Observer;
use kitesurf_local_host::supervisor::{self, EngineSocket, EngineSpec, RunningEngine};
use kitesurf_local_host::transport::UpstreamAddress;
use tokio::net::TcpListener;

fn example_at(directory: &std::path::Path) -> Option<PathBuf> {
    let classic = directory.join("examples/fake_engine");
    if classic.is_file() {
        return Some(classic);
    }
    for entry in std::fs::read_dir(directory.join(".out")).ok()? {
        let path = entry.ok()?.path();
        let name = path.file_name()?.to_string_lossy().to_string();
        if !name.starts_with("fake_engine-") || name.ends_with(".lock") {
            continue;
        }
        let binary = if path.is_dir() {
            path.join("fake_engine")
        } else {
            path
        };
        if binary.is_file() {
            return Some(binary);
        }
    }
    None
}

fn fake_engine() -> PathBuf {
    if let Ok(path) = std::env::var("FAKE_ENGINE") {
        return PathBuf::from(path);
    }
    let current = std::env::current_exe().expect("test binary path");
    let mut ancestor = current.parent();
    while let Some(directory) = ancestor {
        if let Some(candidate) = example_at(directory) {
            return candidate;
        }
        ancestor = directory.parent();
    }
    panic!(
        "build the fake engine with `cargo build --examples` or set FAKE_ENGINE (from {})",
        current.display()
    );
}

fn custom(mode: &str, socket: EngineSocket, timeout: Duration) -> EngineSpec {
    EngineSpec::custom(
        fake_engine(),
        vec![OsString::from(mode)],
        socket,
        timeout,
        Arc::new(Observer::disabled()),
    )
}

async fn listen_socket() -> (TcpListener, PathBuf) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    (listener, PathBuf::from("unused-engine.sock"))
}

#[tokio::test]
async fn invalid_control_message_fails_startup() {
    let (listener, _) = listen_socket().await;
    let mut engine = supervisor::spawn(custom(
        "invalid",
        EngineSocket::Inherited(listener),
        Duration::from_secs(5),
    ))
    .await
    .expect("spawn");
    let error = engine
        .wait_ready()
        .await
        .expect_err("invalid line fails")
        .to_string();
    assert!(
        error.contains("invalid engine control message: not-json"),
        "{error}"
    );
    engine.shutdown().await;
}

#[tokio::test]
async fn wrong_socket_name_fails_startup() {
    let (listener, _) = listen_socket().await;
    let mut engine = supervisor::spawn(custom(
        "wrong-socket",
        EngineSocket::Inherited(listener),
        Duration::from_secs(5),
    ))
    .await
    .expect("spawn");
    let error = engine
        .wait_ready()
        .await
        .expect_err("wrong socket fails")
        .to_string();
    assert!(
        error.contains("listened on other, expected engine"),
        "{error}"
    );
    engine.shutdown().await;
}

#[tokio::test]
async fn closing_the_control_pipe_fails_startup() {
    let (listener, _) = listen_socket().await;
    let mut engine = supervisor::spawn(custom(
        "close-control",
        EngineSocket::Inherited(listener),
        Duration::from_secs(5),
    ))
    .await
    .expect("spawn");
    let error = engine
        .wait_ready()
        .await
        .expect_err("EOF fails")
        .to_string();
    assert!(
        error.contains("control pipe closed during startup"),
        "{error}"
    );
    engine.shutdown().await;
}

#[tokio::test]
async fn early_exit_is_reported_with_its_code() {
    let (listener, _) = listen_socket().await;
    let mut engine = supervisor::spawn(custom(
        "exit",
        EngineSocket::Inherited(listener),
        Duration::from_secs(5),
    ))
    .await
    .expect("spawn");
    let error = engine
        .wait_ready()
        .await
        .expect_err("exit fails")
        .to_string();
    assert!(
        error.contains("engine exited during startup (code 7)"),
        "{error}"
    );
    assert!(engine.has_exited());
    let status = engine.wait_exit().await;
    assert_eq!(status.code, Some(7));
    assert_eq!(status.signal, None);
}

#[tokio::test]
async fn a_silent_engine_times_out() {
    let (listener, _) = listen_socket().await;
    let mut engine = supervisor::spawn(custom(
        "silent",
        EngineSocket::Inherited(listener),
        Duration::from_millis(250),
    ))
    .await
    .expect("spawn");
    let error = engine
        .wait_ready()
        .await
        .expect_err("timeout fails")
        .to_string();
    assert!(
        error.contains("did not listen on engine in time"),
        "{error}"
    );
    engine.shutdown().await;
}

#[tokio::test]
async fn the_inherited_listener_is_a_ready_socket_with_the_bound_port() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let expected = listener.local_addr().expect("listener address").port();
    let mut engine = supervisor::spawn(custom(
        "inherit",
        EngineSocket::Inherited(listener),
        Duration::from_secs(5),
    ))
    .await
    .expect("spawn");
    let listen = engine
        .wait_ready()
        .await
        .expect("the child sees FD 4 as a listening socket");
    assert_eq!(listen.port, expected);
    match engine.endpoint() {
        UpstreamAddress::Tcp(address) => assert_eq!(address.port(), expected),
        other => panic!("inherited engine endpoint is not TCP: {other}"),
    }
    engine.shutdown().await;
}

#[tokio::test]
async fn shutdown_reaps_the_child_with_sigterm() {
    let (_listener, path) = listen_socket().await;
    let mut engine: RunningEngine = supervisor::spawn(custom(
        "hold",
        EngineSocket::Unix(path),
        Duration::from_secs(5),
    ))
    .await
    .expect("spawn");
    let listen = engine.wait_ready().await.expect("hold reports listen");
    assert_eq!(listen.port, 1);
    let status = engine.shutdown().await;
    assert_eq!(status.code, None);
    assert_eq!(status.signal, Some(libc::SIGTERM));
}
