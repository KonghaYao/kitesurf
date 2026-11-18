// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 在测试 crate 内编译同一 transport 源码，预算诊断不必成为库的生产 API。
#[allow(dead_code)]
#[path = "../src/transport.rs"]
mod transport;
use kitesurf_local_host::{observe, HostResult};

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::{Sink, SinkExt, Stream, StreamExt};
use crate::transport::{
    websocket_config, BudgetedSocket, LeasedMessage, PayloadBudget, CODEC_BYTES, INTERNAL_BYTES,
    MAX_FRAME_BYTES, ORDINARY_BYTES,
};
use tokio::io::{AsyncWriteExt, DuplexStream};
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

thread_local! {
    static LARGEST: Cell<usize> = const { Cell::new(0) };
    static LIVE_LARGE: Cell<isize> = const { Cell::new(0) };
}

struct TrackingAllocator;

// 仅记录测试线程上的显式大 allocation，不把 allocator 观测等同于进程 RSS。
unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LARGEST.with(|size| size.set(size.get().max(layout.size())));
        if layout.size() > 128 * 1024 {
            LIVE_LARGE.with(|count| count.set(count.get() + 1));
        }
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if layout.size() > 128 * 1024 {
            LIVE_LARGE.with(|count| count.set(count.get() - 1));
        }
        System.dealloc(ptr, layout);
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        LARGEST.with(|largest| largest.set(largest.get().max(size)));
        LIVE_LARGE.with(|count| {
            count.set(
                count.get() + isize::from(size > 128 * 1024)
                    - isize::from(layout.size() > 128 * 1024),
            )
        });
        System.realloc(ptr, layout, size)
    }
}

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

async fn socket(budget: &PayloadBudget) -> (BudgetedSocket<DuplexStream>, DuplexStream) {
    let (io, peer) = tokio::io::duplex(1024 * 1024);
    let codec = budget.try_acquire(CODEC_BYTES, true).unwrap();
    let ws = WebSocketStream::from_raw_socket(io, Role::Client, Some(websocket_config())).await;
    (BudgetedSocket::new(ws, budget.clone(), codec, false), peer)
}

fn pending(socket: &mut BudgetedSocket<DuplexStream>) {
    let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
    assert!(Pin::new(socket).poll_next(&mut cx).is_pending());
}

#[test]
fn bounded_serializer_reserves_before_allocating_and_drops_permits() {
    let budget = PayloadBudget::default();
    let input = "x".repeat(1024 * 1024);
    let occupied = budget
        .try_acquire(ORDINARY_BYTES - MAX_FRAME_BYTES, false)
        .unwrap();
    LARGEST.with(|size| size.set(0));
    assert!(crate::transport::serialize_payload(&budget, &input, false).is_none());
    assert!(LARGEST.with(Cell::get) < 128 * 1024);
    drop(occupied);
    let (text, lease) = crate::transport::serialize_payload(&budget, &input, false).unwrap();
    assert_eq!(lease.bytes(), text.len());
    assert_eq!(budget.used().0, MAX_FRAME_BYTES + text.len());
    drop(text);
    drop(lease);
    assert_eq!(budget.used().0, MAX_FRAME_BYTES);
    let oversized = "x".repeat(MAX_FRAME_BYTES);
    LARGEST.with(|size| size.set(0));
    assert!(crate::transport::serialize_payload(&budget, &oversized, false).is_none());
    assert!(LARGEST.with(Cell::get) < 128 * 1024);
}

#[test]
fn production_budget_limits_and_failed_queue_transfers_conserve_leases() {
    assert_eq!(ORDINARY_BYTES, 128 * 1024 * 1024);
    assert_eq!(INTERNAL_BYTES, 20 * 1024 * 1024);
    assert_eq!(MAX_FRAME_BYTES, 16 * 1024 * 1024);
    let budget = PayloadBudget::default();
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let message = || LeasedMessage {
        message: Message::text("abc"),
        lease: budget.try_acquire(3, false).unwrap(),
    };
    sender.try_send(message()).unwrap();
    let rejected = sender.try_send(message()).unwrap_err();
    assert_eq!(budget.used().0, MAX_FRAME_BYTES + 6);
    drop(rejected);
    assert_eq!(budget.used().0, MAX_FRAME_BYTES + 3);
    drop(receiver);
    assert_eq!(budget.used().0, MAX_FRAME_BYTES);
    drop(sender.try_send(message()).unwrap_err());
    assert_eq!(budget.used().0, MAX_FRAME_BYTES);
}

#[tokio::test]
async fn oversized_continuation_and_control_rejected_before_allocation() {
    let (io, mut peer) = tokio::io::duplex(1024);
    let mut config = websocket_config();
    config.max_message_size = Some(3);
    let mut ws = WebSocketStream::from_raw_socket(io, Role::Client, Some(config)).await;
    peer.write_all(&[0x02, 3, 1, 2, 3, 0x89, 1, b'p'])
        .await
        .unwrap();
    assert!(ws.next().await.unwrap().unwrap().is_ping());
    peer.write_all(&[0x80, 127, 0, 0, 0, 0, 0, 0x10, 0, 0])
        .await
        .unwrap();
    LARGEST.with(|size| size.set(0));
    assert!(ws.next().await.unwrap().is_err());
    assert!(LARGEST.with(Cell::get) < 128 * 1024);
    let (io, mut peer) = tokio::io::duplex(1024);
    let mut ws = WebSocketStream::from_raw_socket(io, Role::Client, Some(websocket_config())).await;
    peer.write_all(&[0x89, 127, 0, 0, 0, 0, 0, 0x10, 0, 0])
        .await
        .unwrap();
    LARGEST.with(|size| size.set(0));
    assert!(ws.next().await.unwrap().is_err());
    assert!(LARGEST.with(Cell::get) < 128 * 1024);
}

#[tokio::test]
async fn historical_large_backing_released_with_small_read_ahead() {
    let (io, _peer) = tokio::io::duplex(1024);
    let size = 2 * 1024 * 1024;
    let mut raw = vec![0x82, 127];
    raw.extend_from_slice(&(size as u64).to_be_bytes());
    raw.resize(raw.len() + size, 0);
    raw.extend_from_slice(&[0x81, 1, b'x']);
    let before = LIVE_LARGE.with(Cell::get) - 1;
    let mut ws =
        WebSocketStream::from_partially_read(io, raw, Role::Client, Some(websocket_config())).await;
    let first = ws.next().await.unwrap().unwrap();
    assert_eq!(ws.read_buffer_state().buffered_bytes, 3);
    drop(first);
    assert_eq!(LIVE_LARGE.with(Cell::get), before);
    assert_eq!(ws.next().await.unwrap().unwrap(), Message::text("x"));
    ws.release_empty_buffers();
    assert_eq!(LIVE_LARGE.with(Cell::get), before);
}

#[tokio::test]
async fn upstream_handshake_tail_contains_two_frames() {
    use tokio::io::AsyncReadExt;
    let (io, mut peer) = tokio::io::duplex(4096);
    let server = tokio::spawn(async move {
        let mut request = Vec::new();
        loop {
            let byte = peer.read_u8().await.unwrap();
            request.push(byte);
            if request.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        let request = String::from_utf8(request).unwrap();
        let key = request
            .lines()
            .find_map(|line| {
                line.split_once(':')
                    .filter(|(name, _)| name.eq_ignore_ascii_case("sec-websocket-key"))
                    .map(|(_, value)| value.trim())
            })
            .unwrap();
        let accept = tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes());
        let mut response = format!("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").into_bytes();
        response.extend_from_slice(&[0x81, 1, b'a', 0x81, 1, b'b']);
        peer.write_all(&response).await.unwrap();
        peer
    });
    let budget = PayloadBudget::default();
    let handshake = budget.try_acquire(CODEC_BYTES, true).unwrap();
    let codec = budget.try_acquire(CODEC_BYTES, true).unwrap();
    let (ws, _) = tokio_tungstenite::client_async_with_config(
        "ws://localhost/devtools/browser",
        io,
        Some(websocket_config()),
    )
    .await
    .unwrap();
    let _peer = server.await.unwrap();
    assert_eq!(ws.read_buffer_state().buffered_bytes, 6);
    let mut ws = BudgetedSocket::new(ws, budget.clone(), codec, true).with_handshake(handshake);
    assert_eq!(
        ws.next().await.unwrap().unwrap().message,
        Message::text("a")
    );
    assert_eq!(
        ws.next().await.unwrap().unwrap().message,
        Message::text("b")
    );
}

#[tokio::test]
async fn hyper_upgrade_preserves_first_frame_in_same_write() {
    use std::convert::Infallible;
    use bytes::Bytes;
    use http_body_util::Full;
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    let budget = PayloadBudget::default();
    let http = std::sync::Arc::new(budget.try_acquire(32 * 1024, true).unwrap());
    let server_budget = budget.clone();
    let (io, mut peer) = tokio::io::duplex(4096);
    let (result, received) = tokio::sync::oneshot::channel();
    let result = std::sync::Arc::new(std::sync::Mutex::new(Some(result)));
    let server = tokio::spawn(async move {
        let service = service_fn(move |mut request| {
            let upgrade = hyper::upgrade::on(&mut request);
            let codec = server_budget.try_acquire(CODEC_BYTES, true).unwrap();
            let budget = server_budget.clone();
            let result = result.lock().unwrap().take().unwrap();
            let http = http.clone();
            tokio::spawn(async move {
                let upgraded = upgrade.await.unwrap();
                let ws = WebSocketStream::from_raw_socket(
                    TokioIo::new(upgraded),
                    Role::Server,
                    Some(websocket_config()),
                )
                .await;
                let mut ws = BudgetedSocket::new(ws, budget, codec, false);
                result
                    .send(ws.next().await.unwrap().unwrap().message)
                    .unwrap();
                drop(ws);
                drop(http);
            });
            async {
                Ok::<_, Infallible>(
                    hyper::Response::builder()
                        .status(101)
                        .header("connection", "upgrade")
                        .header("upgrade", "websocket")
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
            }
        });
        hyper::server::conn::http1::Builder::new()
            .max_buf_size(32 * 1024)
            .serve_connection(TokioIo::new(io), service)
            .with_upgrades()
            .await
            .unwrap();
    });
    let mut request =
        b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n"
            .to_vec();
    request.extend_from_slice(&[0x81, 0x81, 0, 0, 0, 0, b'a']);
    peer.write_all(&request).await.unwrap();
    assert_eq!(received.await.unwrap(), Message::text("a"));
    server.await.unwrap();
    assert_eq!(budget.used(), (MAX_FRAME_BYTES, 0));
}

#[tokio::test]
async fn cancelled_ingress_send_releases_reader_held_envelope() {
    let budget = PayloadBudget::default();
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let message = || LeasedMessage {
        message: Message::text("abc"),
        lease: budget.try_acquire(3, false).unwrap(),
    };
    sender.send(message()).await.unwrap();
    let mut send = Box::pin(sender.send(message()));
    let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
    assert!(std::future::Future::poll(send.as_mut(), &mut cx).is_pending());
    assert_eq!(budget.used().0, MAX_FRAME_BYTES + 6);
    drop(send);
    assert_eq!(budget.used().0, MAX_FRAME_BYTES + 3);
    drop(receiver);
    assert_eq!(budget.used().0, MAX_FRAME_BYTES);
}

#[tokio::test]
async fn codec_api_tracks_header_and_zero_fragment() {
    let (io, mut peer) = tokio::io::duplex(1024);
    let mut ws = WebSocketStream::from_raw_socket(io, Role::Client, Some(websocket_config())).await;
    peer.write_all(&[0x81]).await.unwrap();
    let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
    assert!(Pin::new(&mut ws).poll_next(&mut cx).is_pending());
    assert_eq!(ws.read_buffer_state().buffered_bytes, 1);
    peer.write_all(&[3]).await.unwrap();
    assert!(Pin::new(&mut ws).poll_next(&mut cx).is_pending());
    assert_eq!(ws.read_buffer_state().frame_payload_len, Some(3));
    assert_eq!(ws.read_buffer_state().buffered_bytes, 0);
    peer.write_all(b"abc\x01\x00").await.unwrap();
    assert_eq!(ws.next().await.unwrap().unwrap(), Message::text("abc"));
    assert!(Pin::new(&mut ws).poll_next(&mut cx).is_pending());
    assert_eq!(ws.read_buffer_state().fragmented_message_bytes, Some(0));
    ws.release_empty_buffers();
    assert_eq!(ws.read_buffer_state().fragmented_message_bytes, Some(0));
}

#[tokio::test]
async fn partial_pending_and_next_cancellation_keep_socket_lease() {
    let budget = PayloadBudget::default();
    let (mut ws, mut peer) = socket(&budget).await;
    pending(&mut ws);
    assert_eq!(budget.used(), (MAX_FRAME_BYTES, CODEC_BYTES));
    peer.write_all(&[0x01, 0]).await.unwrap();
    pending(&mut ws);
    assert_eq!(budget.used().0, 2 * MAX_FRAME_BYTES);
    let mut next = ws.next();
    let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
    assert!(std::future::Future::poll(Pin::new(&mut next), &mut cx).is_pending());
    drop(next);
    assert_eq!(budget.used().0, 2 * MAX_FRAME_BYTES);
    peer.write_all(&[0x80, 0]).await.unwrap();
    drop(ws.next().await.unwrap().unwrap());
    assert_eq!(budget.used().0, MAX_FRAME_BYTES);
    drop(ws);
    assert_eq!(budget.used(), (MAX_FRAME_BYTES, 0));
}

#[tokio::test]
async fn utf8_control_and_two_message_read_ahead() {
    let budget = PayloadBudget::default();
    let (mut ws, mut peer) = socket(&budget).await;
    peer.write_all(&[
        0x01, 1, 0xe2, 0x89, 1, b'p', 0x80, 2, 0x82, 0xac, 0x81, 1, b'x',
    ])
    .await
    .unwrap();
    let ping = ws.next().await.unwrap().unwrap();
    assert_eq!(ping.message, Message::Ping(vec![b'p'].into()));
    assert_eq!(budget.used().0, 2 * MAX_FRAME_BYTES + 1);
    drop(ping);
    let first = ws.next().await.unwrap().unwrap();
    assert_eq!(first.message, Message::text("€"));
    assert_eq!(first.lease.bytes(), 3);
    let second = ws.next().await.unwrap().unwrap();
    assert_eq!(second.message, Message::text("x"));
    drop((first, second));
    pending(&mut ws);
    assert_eq!(budget.used().0, MAX_FRAME_BYTES);
}

#[tokio::test]
async fn thirty_two_idle_sockets_only_hold_small_codec_leases() {
    let budget = PayloadBudget::default();
    let mut sockets = Vec::new();
    for _ in 0..32 {
        let (mut ws, peer) = socket(&budget).await;
        pending(&mut ws);
        sockets.push((ws, peer));
    }
    assert_eq!(budget.used(), (MAX_FRAME_BYTES, 32 * CODEC_BYTES));
    drop(sockets);
    assert_eq!(budget.used(), (MAX_FRAME_BYTES, 0));
}

#[tokio::test]
async fn thirty_two_idle_sockets_release_historical_large_backing() {
    let budget = PayloadBudget::default();
    let before = LIVE_LARGE.with(Cell::get);
    let mut sockets = Vec::new();
    for _ in 0..32 {
        let (io, peer) = tokio::io::duplex(1024);
        let mut raw = vec![0x82, 127];
        raw.extend_from_slice(&(256 * 1024_u64).to_be_bytes());
        raw.resize(raw.len() + 256 * 1024, 0);
        let ws =
            WebSocketStream::from_partially_read(io, raw, Role::Client, Some(websocket_config()))
                .await;
        let codec = budget.try_acquire(CODEC_BYTES, true).unwrap();
        let mut ws = BudgetedSocket::new(ws, budget.clone(), codec, false);
        drop(ws.next().await.unwrap().unwrap());
        let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
        assert!(Pin::new(&mut ws).poll_next(&mut cx).is_pending());
        sockets.push((ws, peer));
    }
    assert_eq!(LIVE_LARGE.with(Cell::get), before);
    assert_eq!(budget.used(), (MAX_FRAME_BYTES, 32 * CODEC_BYTES));
    drop(sockets);
    assert_eq!(budget.used(), (MAX_FRAME_BYTES, 0));
}

#[tokio::test]
async fn budget_wait_cancel_and_owned_lease_release() {
    let budget = PayloadBudget::default();
    let held = budget
        .try_acquire(ORDINARY_BYTES - MAX_FRAME_BYTES, false)
        .unwrap();
    let internal = budget
        .try_acquire(INTERNAL_BYTES - MAX_FRAME_BYTES - 2 * CODEC_BYTES, true)
        .unwrap();
    let (mut ws, mut peer) = socket(&budget).await;
    peer.write_all(&[0x81, 1, b'a']).await.unwrap();
    pending(&mut ws);
    let (io, mut upstream_peer) = tokio::io::duplex(1024);
    let codec = budget.try_acquire(CODEC_BYTES, true).unwrap();
    let upstream =
        WebSocketStream::from_raw_socket(io, Role::Client, Some(websocket_config())).await;
    let mut upstream = BudgetedSocket::new(upstream, budget.clone(), codec, true);
    upstream_peer.write_all(&[0x01, 0]).await.unwrap();
    let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
    assert!(Pin::new(&mut upstream).poll_next(&mut cx).is_pending());
    assert_eq!(budget.used(), (ORDINARY_BYTES, INTERNAL_BYTES));
    assert_eq!(budget.high_water(), (ORDINARY_BYTES, INTERNAL_BYTES));
    eprintln!(
        "payload high-water ordinary={} internal={} (not RSS)",
        ORDINARY_BYTES, INTERNAL_BYTES
    );
    drop(ws);
    drop(upstream);
    drop((held, internal));
    assert_eq!(budget.used(), (MAX_FRAME_BYTES, 0));
}

#[tokio::test]
async fn writer_lease_lives_until_flush_and_shared_socket_drop() {
    let budget = PayloadBudget::default();
    let (ws, peer) = socket(&budget).await;
    let (mut sink, source) = ws.split();
    let data = vec![0; 2 * 1024 * 1024];
    let lease = budget.try_acquire(data.len(), false).unwrap();
    let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
    assert!(matches!(
        Pin::new(&mut sink).poll_ready(&mut cx),
        Poll::Ready(Ok(()))
    ));
    Pin::new(&mut sink)
        .start_send(LeasedMessage {
            message: Message::Binary(data.into()),
            lease,
        })
        .unwrap();
    assert!(Pin::new(&mut sink).poll_flush(&mut cx).is_pending());
    assert_eq!(budget.used().0, MAX_FRAME_BYTES + 2 * 1024 * 1024);
    drop(peer);
    assert!(matches!(
        Pin::new(&mut sink).poll_flush(&mut cx),
        Poll::Ready(Err(_))
    ));
    drop(sink);
    assert_eq!(budget.used().0, MAX_FRAME_BYTES + 2 * 1024 * 1024);
    drop(source);
    assert_eq!(budget.used(), (MAX_FRAME_BYTES, 0));
}

#[tokio::test]
async fn cancelled_writer_send_keeps_codec_lease_until_both_halves_drop() {
    let budget = PayloadBudget::default();
    let (ws, _peer) = socket(&budget).await;
    let (mut sink, source) = ws.split();
    let lease = budget.try_acquire(2 * 1024 * 1024, false).unwrap();
    let mut sending = Box::pin(sink.send(LeasedMessage {
        message: Message::Binary(vec![0; 2 * 1024 * 1024].into()),
        lease,
    }));
    let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
    assert!(std::future::Future::poll(sending.as_mut(), &mut cx).is_pending());
    drop(sending);
    drop(sink);
    assert_eq!(budget.used().0, MAX_FRAME_BYTES + 2 * 1024 * 1024);
    drop(source);
    assert_eq!(budget.used(), (MAX_FRAME_BYTES, 0));
}

#[tokio::test]
async fn codec_poll_never_reaches_io_without_receive_lease() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    struct Probe {
        polls: Arc<AtomicUsize>,
        budget: PayloadBudget,
    }
    impl AsyncRead for Probe {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            assert!(self.budget.used().0 >= 2 * MAX_FRAME_BYTES);
            self.polls.fetch_add(1, Ordering::Relaxed);
            Poll::Pending
        }
    }
    impl AsyncWrite for Probe {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Pending
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
    let budget = PayloadBudget::default();
    let polls = Arc::new(AtomicUsize::new(0));
    let io = Probe {
        polls: polls.clone(),
        budget: budget.clone(),
    };
    let codec = budget.try_acquire(CODEC_BYTES, true).unwrap();
    let ws = WebSocketStream::from_raw_socket(io, Role::Client, Some(websocket_config())).await;
    let mut ws = BudgetedSocket::new(ws, budget.clone(), codec, false);
    let held = budget
        .try_acquire(ORDINARY_BYTES - MAX_FRAME_BYTES, false)
        .unwrap();
    let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
    assert!(Pin::new(&mut ws).poll_next(&mut cx).is_pending());
    assert_eq!(polls.load(Ordering::Relaxed), 0);
    drop(held);
    assert!(Pin::new(&mut ws).poll_next(&mut cx).is_pending());
    assert_eq!(polls.load(Ordering::Relaxed), 1);
    assert_eq!(budget.used().0, MAX_FRAME_BYTES);
}

#[tokio::test]
async fn successful_writer_flush_releases_payload() {
    let budget = PayloadBudget::default();
    let (mut ws, _peer) = socket(&budget).await;
    let lease = budget.try_acquire(3, false).unwrap();
    ws.send(LeasedMessage {
        message: Message::text("abc"),
        lease,
    })
    .await
    .unwrap();
    assert_eq!(budget.used().0, MAX_FRAME_BYTES);
}
