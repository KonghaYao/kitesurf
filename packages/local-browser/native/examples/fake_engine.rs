// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// supervisor 测试用的替身引擎。它在 FD 3 上使用 workerd 控制协议；在 "inherit" 模式下
// 先证明 FD 4 是监听 socket，再上报该 socket 的端口。

use std::os::fd::RawFd;
use std::time::Duration;

const CONTROL_FD: RawFd = 3;
const LISTENER_FD: RawFd = 4;

fn write_control(line: &str) {
    let payload = format!("{line}\n");
    let bytes = payload.as_bytes();
    unsafe {
        libc::write(CONTROL_FD, bytes.as_ptr().cast(), bytes.len());
    }
}

fn main() {
    let mode = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "hold".to_string());
    match mode.as_str() {
        "invalid" => write_control("not-json"),
        "wrong-socket" => write_control(r#"{"event":"listen","socket":"other","port":0}"#),
        "close-control" => unsafe {
            libc::close(CONTROL_FD);
        },
        "exit" => {
            eprintln!("fake engine failed");
            std::process::exit(7);
        }
        "diagnostic-exit" => {
            eprint!("[ks-gc] {{\"at\":1,\"done\":2,\"extra\":\"synthetic-output-sensitive\"}} UnCaUgHt Error");
            std::process::exit(0);
        }
        "delayed-exit" => {
            std::thread::sleep(Duration::from_millis(100));
            eprint!("workerd INTERNAL   ERROR synthetic-output-sensitive");
            std::process::exit(7);
        }
        "silent" => sleep(),
        "inherit" => inherit(),
        "hold" => {
            let port = std::env::var("KS_FAKE_PORT").unwrap_or_else(|_| "1".to_string());
            write_control(&format!(
                r#"{{"event":"listen","socket":"engine","port":{port}}}"#
            ));
        }
        other => {
            eprintln!("fake engine: unknown mode {other}");
            std::process::exit(2);
        }
    }
    sleep();
}

fn inherit() {
    if unsafe { libc::listen(LISTENER_FD, 128) } != 0 {
        write_control("fd4-is-not-a-listening-socket");
        return;
    }
    let mut address: libc::sockaddr_in = unsafe { std::mem::zeroed() };
    let mut length = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
    let result = unsafe {
        libc::getsockname(
            LISTENER_FD,
            (&mut address as *mut libc::sockaddr_in).cast(),
            &mut length,
        )
    };
    if result != 0 {
        write_control("fd4-has-no-name");
        return;
    }
    let port = u16::from_be(address.sin_port);
    write_control(&format!(
        r#"{{"event":"listen","socket":"engine","port":{port}}}"#
    ));
}

fn sleep() {
    std::thread::sleep(Duration::from_secs(120));
}
