// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 真实 SQLite 上的 profile 契约：创建不覆盖、格式与权限检查、独占锁、保存的 revision
// 与原始 TEXT 规则、备份的 0600/不覆盖/读回一致。测试不 mock 数据库。

use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use kitesurf_local_host::profile::{ProfileErrorKind, ProfileStore};
use kitesurf_local_host::profile_state::{self, Json};
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

const EMPTY_STATE: &str = r#"{"cookies":[],"localStorage":[]}"#;
const APPLICATION_ID: i64 = 0x4b53_5046;
const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;

fn workspace() -> TempDir {
    tempfile::Builder::new()
        .prefix("ks-profile-test-")
        .tempdir()
        .expect("temp dir")
}

fn owner(text: &str) -> Vec<u16> {
    text.encode_utf16().collect()
}

fn state(text: &str) -> Json {
    profile_state::parse(text).expect("valid fixture json")
}

fn revision(text: &str) -> Json {
    profile_state::parse(text).expect("valid fixture revision")
}

fn new_store(directory: &TempDir) -> (PathBuf, ProfileStore) {
    let path = directory.path().join("profile.db");
    let store = ProfileStore::open(&path, true).expect("create profile");
    (path, store)
}

fn seed(path: &Path, application_id: i64, user_version: i64, revision: i64, text: &str) {
    let connection = Connection::open(path).expect("seed sqlite");
    connection
        .pragma_update(None, "application_id", application_id)
        .expect("application id");
    connection
        .pragma_update(None, "user_version", user_version)
        .expect("user version");
    connection
        .execute_batch(
            "CREATE TABLE profile (id INTEGER PRIMARY KEY CHECK(id=1), \
             revision INTEGER NOT NULL, state TEXT NOT NULL)",
        )
        .expect("schema");
    connection
        .execute(
            "INSERT INTO profile VALUES (1, ?1, ?2)",
            params![revision, text],
        )
        .expect("row");
}

fn id_for(path: &Path) -> String {
    let digest = Sha256::digest(path.as_os_str().as_encoded_bytes());
    let mut id = String::new();
    for byte in digest.iter().take(8) {
        id.push_str(&format!("{byte:02x}"));
    }
    id
}

#[test]
fn create_writes_the_empty_profile_and_status_id() {
    let directory = workspace();
    let (path, store) = new_store(&directory);
    let canonical = fs::canonicalize(&path).expect("canonical profile");
    let alpha = owner("http://fixture.test:8080");
    let read = store.acquire(&alpha).expect("first acquire");
    assert_eq!(read.revision, 0);
    assert_eq!(read.state_text, EMPTY_STATE);
    let status = store.status().expect("status");
    assert_eq!(status.revision, 0);
    assert_eq!(status.format, 1);
    assert_eq!(status.path, canonical);
    assert_eq!(status.id, id_for(&canonical));
    assert_eq!(status.id.len(), 16);
    assert!(store.acquire(&owner("http://other.test")).is_err());
    assert!(store.acquire(&owner("")).is_err());
    let status_json = status.to_json();
    assert_eq!(status_json["revision"], 0);
    assert_eq!(status_json["format"], 1);
    assert_eq!(status_json["id"], id_for(&canonical));
}

#[test]
fn create_never_overwrites_and_missing_profiles_fail_closed() {
    let directory = workspace();
    let path = directory.path().join("profile.db");
    assert!(ProfileStore::open(&path, false).is_err());
    assert!(!path.exists());

    let store = ProfileStore::open(&path, true).expect("create");
    let alpha = owner("http://fixture.test");
    store.acquire(&alpha).expect("acquire");
    store
        .save(
            &alpha,
            &revision("1"),
            Some(&state(r#"{"cookies":[],"localStorage":[]}"#)),
        )
        .expect("save");
    drop(store);

    assert!(ProfileStore::open(&path, true).is_err());
    let reopened = ProfileStore::open(&path, false).expect("reopen");
    let read = reopened.acquire(&alpha).expect("read");
    assert_eq!(read.revision, 1);
    drop(reopened);

    fs::write(&path, b"not a database").expect("write junk");
    assert!(ProfileStore::open(&path, false).is_err());
    assert_eq!(fs::read(&path).expect("junk intact"), b"not a database");
}

#[test]
fn existing_profiles_must_match_the_format_and_contract() {
    let cases: Vec<(i64, i64, i64, &str)> = vec![
        (0x1234_5678, 1, 0, EMPTY_STATE),
        (APPLICATION_ID, 2, 0, EMPTY_STATE),
        (APPLICATION_ID, 1, -1, EMPTY_STATE),
        (APPLICATION_ID, 1, MAX_SAFE_INTEGER + 1, EMPTY_STATE),
        (APPLICATION_ID, 1, 0, "[]"),
        (APPLICATION_ID, 1, 0, "{"),
        (APPLICATION_ID, 1, 0, r#"{"cookies":1,"localStorage":[]}"#),
    ];
    for (index, (application_id, version, revision, text)) in cases.into_iter().enumerate() {
        let directory = workspace();
        let path = directory.path().join(format!("case-{index}.db"));
        seed(&path, application_id, version, revision, text);
        assert!(
            ProfileStore::open(&path, false).is_err(),
            "case {index} must be rejected"
        );
    }
}

#[test]
fn existing_profile_permissions_and_links_are_checked() {
    let directory = workspace();
    let (path, store) = new_store(&directory);
    drop(store);

    fs::set_permissions(&path, fs::Permissions::from_mode(0o666)).expect("chmod");
    assert!(ProfileStore::open(&path, false).is_err());
    fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).expect("chmod");
    let store = ProfileStore::open(&path, false).expect("0640 is accepted");
    drop(store);

    let linked = directory.path().join("linked.db");
    fs::hard_link(&path, &linked).expect("hard link");
    assert!(ProfileStore::open(&path, false).is_err());
    assert_eq!(fs::metadata(&path).expect("metadata").nlink(), 2);
    fs::remove_file(&linked).expect("unlink");
    ProfileStore::open(&path, false).expect("single link accepted");
}

#[test]
fn only_one_process_connection_holds_the_profile() {
    let directory = workspace();
    let (path, store) = new_store(&directory);
    let alpha = owner("http://fixture.test");
    store.acquire(&alpha).expect("acquire");
    assert!(ProfileStore::open(&path, false).is_err());
    assert!(ProfileStore::open(&path, true).is_err());
    drop(store);
    let reopened = ProfileStore::open(&path, false).expect("after close");
    assert_eq!(reopened.acquire(&alpha).expect("read").revision, 0);
}

#[test]
fn save_accepts_only_the_next_revision_or_an_exact_retry() {
    let directory = workspace();
    let (_path, store) = new_store(&directory);
    let alpha = owner("http://fixture.test");
    let beta = owner("http://other.test");
    let first = state(r#"{"cookies":[],"localStorage":[],"a":1}"#);

    assert!(matches!(
        store
            .save(&alpha, &revision("1"), Some(&first))
            .unwrap_err()
            .kind(),
        ProfileErrorKind::Conflict
    ));
    store.acquire(&alpha).expect("acquire");
    assert!(matches!(
        store.save(&alpha, &revision("1"), None).unwrap_err().kind(),
        ProfileErrorKind::InvalidState
    ));
    store
        .save(&alpha, &revision("1"), Some(&first))
        .expect("first save");
    assert_eq!(
        store.acquire(&alpha).expect("read").state_text,
        r#"{"cookies":[],"localStorage":[],"a":1}"#
    );
    store
        .save(&alpha, &revision("1"), Some(&first))
        .expect("lost-reply retry");

    let changed = state(r#"{"cookies":[],"localStorage":[],"a":2}"#);
    for rejected in ["0", "-1", "1.5", "9007199254740992"] {
        assert!(
            matches!(
                store
                    .save(&alpha, &revision(rejected), Some(&changed))
                    .unwrap_err()
                    .kind(),
                ProfileErrorKind::Conflict
            ),
            "revision {rejected} must be rejected"
        );
    }
    assert!(matches!(
        store
            .save(&alpha, &revision("1"), Some(&changed))
            .unwrap_err()
            .kind(),
        ProfileErrorKind::Conflict
    ));
    assert!(matches!(
        store
            .save(&beta, &revision("1"), Some(&first))
            .unwrap_err()
            .kind(),
        ProfileErrorKind::Conflict
    ));
    assert!(matches!(
        store
            .save(&alpha, &revision("2"), Some(&state("[]")))
            .unwrap_err()
            .kind(),
        ProfileErrorKind::InvalidState
    ));
    let read = store.acquire(&alpha).expect("read after rejection");
    assert_eq!(read.revision, 1);
    assert_eq!(read.state_text, r#"{"cookies":[],"localStorage":[],"a":1}"#);

    let second = state(r#"{"cookies":[],"localStorage":[],"b":2}"#);
    store
        .save(&alpha, &revision("2"), Some(&second))
        .expect("next save");
    assert_eq!(store.status().expect("status").revision, 2);
}

#[test]
fn load_returns_the_stored_text_byte_for_byte() {
    let directory = workspace();
    let path = directory.path().join("seeded.db");
    let text = "{\"cookies\": [], \"localStorage\":[], \"__proto__\": {\"a\": 1}}";
    seed(&path, APPLICATION_ID, 1, 5, text);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).expect("chmod");
    let store = ProfileStore::open(&path, false).expect("open seeded");
    let alpha = owner("http://fixture.test");
    let read = store.acquire(&alpha).expect("read");
    assert_eq!(read.revision, 5);
    assert_eq!(read.state_text, text);
    assert!(matches!(
        store
            .save(&alpha, &revision("5"), Some(&state(text)))
            .unwrap_err()
            .kind(),
        ProfileErrorKind::Conflict
    ));
    assert_eq!(store.acquire(&alpha).expect("read again").state_text, text);
}

#[test]
fn backup_is_private_readable_and_never_overwrites() {
    let directory = workspace();
    let (_path, store) = new_store(&directory);
    let alpha = owner("http://fixture.test");
    store.acquire(&alpha).expect("acquire");
    let saved = state(r#"{"cookies":[],"localStorage":[],"n":1}"#);
    store
        .save(&alpha, &revision("1"), Some(&saved))
        .expect("save");

    let destination = directory.path().join("backup.db");
    let produced = store.backup(&destination).expect("backup");
    assert_eq!(
        produced,
        fs::canonicalize(&destination).expect("canonical backup")
    );
    let metadata = fs::metadata(&destination).expect("backup metadata");
    assert_eq!(metadata.mode() & 0o777, 0o600);
    assert_eq!(metadata.nlink(), 1);

    let backup =
        Connection::open_with_flags(&destination, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("open backup");
    let application_id: i64 = backup
        .query_row("PRAGMA application_id", [], |row| row.get(0))
        .expect("backup application id");
    let version: i64 = backup
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("backup version");
    let row: (i64, String) = backup
        .query_row(
            "SELECT revision, state FROM profile WHERE id=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("backup row");
    assert_eq!(application_id, APPLICATION_ID);
    assert_eq!(version, 1);
    let backup_text = r#"{"cookies":[],"localStorage":[],"n":1}"#;
    assert_eq!(row, (1, backup_text.to_string()));
    drop(backup);

    let before = fs::read(&destination).expect("backup bytes");
    assert!(store.backup(&destination).is_err());
    assert_eq!(fs::read(&destination).expect("backup intact"), before);
    assert_eq!(
        store.acquire(&alpha).expect("profile still open").revision,
        1
    );

    let target = directory.path().join("target.db");
    fs::write(&target, b"keep").expect("target");
    let link = directory.path().join("link.db");
    std::os::unix::fs::symlink(&target, &link).expect("symlink");
    assert!(store.backup(&link).is_err());
    assert_eq!(fs::read(&target).expect("target intact"), b"keep");

    assert!(store
        .backup(&directory.path().join("missing/out.db"))
        .is_err());

    let open_dir = directory.path().join("open");
    fs::create_dir(&open_dir).expect("open dir");
    fs::set_permissions(&open_dir, fs::Permissions::from_mode(0o777)).expect("chmod");
    assert!(store.backup(&open_dir.join("out.db")).is_err());
    assert_eq!(fs::read_dir(&open_dir).expect("dir").count(), 0);
    fs::set_permissions(&open_dir, fs::Permissions::from_mode(0o700)).expect("restore");
}

#[test]
fn insecure_parents_are_rejected_without_leaving_files() {
    let directory = workspace();
    let open_dir = directory.path().join("open");
    fs::create_dir(&open_dir).expect("open dir");
    fs::set_permissions(&open_dir, fs::Permissions::from_mode(0o777)).expect("chmod");
    let path = open_dir.join("profile.db");
    assert!(ProfileStore::open(&path, true).is_err());
    assert!(!path.exists());
    assert_eq!(fs::read_dir(&open_dir).expect("dir").count(), 0);
    fs::set_permissions(&open_dir, fs::Permissions::from_mode(0o700)).expect("restore");

    let nested = directory.path().join("nested");
    fs::create_dir(&nested).expect("nested");
    fs::set_permissions(&nested, fs::Permissions::from_mode(0o0600)).expect("chmod");
    assert!(ProfileStore::open(&nested.join("profile.db"), true).is_err());
    fs::set_permissions(&nested, fs::Permissions::from_mode(0o700)).expect("restore");
}

mod private_services {
    use std::fs;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use bytes::Bytes;
    use http_body_util::{BodyExt, Full};
    use hyper::header::CONTENT_TYPE;
    use hyper::{Method, Request};
    use hyper_util::rt::TokioIo;
    use kitesurf_local_host::observe::Observer;
    use kitesurf_local_host::services::{PrivateServices, ProfileService, SqliteProfile};
    use serde_json::Value;
    use tempfile::TempDir;
    use tokio::net::UnixStream;

    use super::workspace;

    struct Fixture {
        directory: TempDir,
        services: PrivateServices,
        observer: PathBuf,
    }

    fn start_services() -> Fixture {
        let directory = workspace();
        let run = directory.path().join("run");
        fs::create_dir(&run).expect("run dir");
        fs::set_permissions(&run, fs::Permissions::from_mode(0o700)).expect("run mode");
        let assets = directory.path().join("assets");
        fs::create_dir(&assets).expect("assets dir");
        let manifest = directory.path().join("assets.json");
        fs::write(&manifest, br#"{"files":[]}"#).expect("manifest");
        let profile = directory.path().join("profile.db");
        let backend = SqliteProfile::open(&profile, true).expect("profile backend");
        let profile = Arc::new(ProfileService::new(Arc::new(backend)));
        let observer = directory.path().join("observe.jsonl");
        let observer_sink = Arc::new(Observer::create(&observer).expect("observer"));
        let services = PrivateServices::bind(&run, &assets, &manifest, profile, observer_sink)
            .expect("bind private services");
        Fixture {
            directory,
            services,
            observer,
        }
    }

    async fn request(socket: &Path, method: Method, path: &str, body: &str) -> (u16, String) {
        let stream = UnixStream::connect(socket).await.expect("connect socket");
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .expect("http handshake");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from(body.to_owned())))
            .expect("request");
        let response = sender.send_request(request).await.expect("response");
        let status = response.status().as_u16();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    fn json(text: &str) -> Value {
        serde_json::from_str(text).expect("json response")
    }

    fn assert_no_private_content(body: &str) {
        for forbidden in [
            "owner-a",
            "owner-b",
            "secret-state-value",
            "other-state-value",
            "SELECT",
            "UPDATE",
            "INSERT",
            "DELETE",
        ] {
            assert!(
                !body.contains(forbidden),
                "response leaked {forbidden}: {body}"
            );
        }
    }

    #[tokio::test]
    async fn private_sockets_are_owner_only() {
        let fixture = start_services();
        for socket in [
            &fixture.services.paths().profile,
            &fixture.services.paths().assets,
        ] {
            let metadata = fs::symlink_metadata(socket).expect("socket metadata");
            assert_eq!(metadata.mode() & 0o777, 0o600, "{}", socket.display());
        }
        let run = fixture
            .services
            .paths()
            .profile
            .parent()
            .expect("run directory");
        assert_eq!(
            fs::metadata(run).expect("run metadata").mode() & 0o777,
            0o700
        );
        let profile = fixture.directory.path().join("profile.db");
        assert_eq!(
            fs::metadata(profile).expect("profile").mode() & 0o777,
            0o600
        );
        fixture.services.shutdown().await;
    }

    #[tokio::test]
    async fn profile_http_classifies_errors_and_keeps_state_private() {
        let fixture = start_services();
        let socket = fixture.services.paths().profile.clone();
        let secret = "secret-state-value";
        let state = format!(r#"{{"cookies":[],"localStorage":[],"marker":"{secret}"}}"#);
        let save = |owner: &str, revision: &str, state: &str| {
            format!(r#"{{"owner":"{owner}","revision":{revision},"state":{state}}}"#)
        };

        let (status, body) =
            request(&socket, Method::POST, "/load", r#"{"owner":"owner-a"}"#).await;
        assert_eq!(status, 200);
        assert_eq!(json(&body)["revision"], 0);
        assert!(!body.contains("owner-a"));

        let (status, body) = request(
            &socket,
            Method::POST,
            "/save",
            &save("owner-a", "1", &state),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body, r#"{"saved":true}"#);

        let changed = r#"{"cookies":[],"localStorage":[],"marker":"other-state-value"}"#;
        for (owner, revision, state_text) in [
            ("owner-a", "1", changed),
            ("owner-a", "3", &state),
            ("owner-a", "1.5", &state),
            ("owner-b", "2", &state),
        ] {
            let (status, body) = request(
                &socket,
                Method::POST,
                "/save",
                &save(owner, revision, state_text),
            )
            .await;
            assert_eq!(status, 409, "{owner} {revision}");
            let error = json(&body);
            assert_eq!(error["code"], "COMMIT_UNKNOWN");
            assert_eq!(error["message"], "Profile operation rejected");
            assert_no_private_content(&body);
        }

        let (status, body) = request(&socket, Method::POST, "/save", "{").await;
        assert_eq!(status, 409);
        assert_eq!(json(&body)["code"], "COMMIT_UNKNOWN");
        assert_no_private_content(&body);

        let missing_owner = format!(r#"{{"revision":2,"state":{state}}}"#);
        let (status, body) = request(&socket, Method::POST, "/save", &missing_owner).await;
        assert_eq!(status, 409);
        assert_no_private_content(&body);

        let missing_state = r#"{"owner":"owner-a","revision":2}"#;
        let (status, body) = request(&socket, Method::POST, "/save", missing_state).await;
        assert_eq!(status, 422);
        assert_eq!(json(&body)["code"], "INVALID_STATE");
        assert_no_private_content(&body);

        let invalid = r#"{"cookies":{},"localStorage":[]}"#;
        let (status, body) = request(
            &socket,
            Method::POST,
            "/save",
            &save("owner-a", "2", invalid),
        )
        .await;
        assert_eq!(status, 422);
        assert_eq!(json(&body)["code"], "INVALID_STATE");
        assert_no_private_content(&body);

        let (status, body) = request(&socket, Method::POST, "/unknown", "{}").await;
        assert_eq!(status, 404);
        assert!(body.is_empty());
        let (status, body) = request(&socket, Method::GET, "/load", "").await;
        assert_eq!(status, 404);
        assert!(body.is_empty());

        let (status, body) = request(&socket, Method::POST, "/../../etc/passwd", "{}").await;
        assert_eq!(status, 404);
        assert!(!body.contains("root:"));
        let (status, body) = request(
            &socket,
            Method::POST,
            "/load?path=../../etc/passwd",
            r#"{"owner":"owner-a"}"#,
        )
        .await;
        assert_eq!(status, 200);
        assert!(!body.contains("root:"));

        let (status, body) =
            request(&socket, Method::POST, "/load", r#"{"owner":"owner-a"}"#).await;
        assert_eq!(status, 200);
        let loaded = json(&body);
        assert_eq!(loaded["revision"], 1, "failed revisions must not be stored");
        assert_eq!(loaded["state"]["marker"], secret);

        let events = fs::read_to_string(&fixture.observer).expect("observer file");
        let mut observed = 0;
        for line in events.lines().filter(|line| !line.trim().is_empty()) {
            let event = json(line);
            let object = event.as_object().expect("observer object");
            assert!(object
                .keys()
                .all(|key| ["kind", "operation", "revision", "status"].contains(&key.as_str())));
            assert_no_private_content(line);
            observed += 1;
        }
        assert!(observed > 0);
        fixture.services.shutdown().await;
    }
}
