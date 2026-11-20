// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 正式 SQLite profile：单连接以 EXCLUSIVE locking mode 持锁到关闭，state 保存原始紧凑
// TEXT，保存只接受下一 revision 或完全相同的 lost-reply 重试。路径、目录和备份目标按
// fail-closed 检查；错误信息固定，不包含 state、cookie、owner 或 SQL 参数。

use std::fs::File;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::profile_state::{self, Json};
use crate::HostResult;

const APPLICATION_ID: i64 = 0x4b53_5046;
const FORMAT: i64 = 1;
const EMPTY_STATE: &str = r#"{"cookies":[],"localStorage":[]}"#;
const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProfileErrorKind {
    InvalidState,
    Conflict,
}

#[derive(Clone, Copy, Debug)]
pub struct ProfileError {
    kind: ProfileErrorKind,
}

impl ProfileError {
    pub fn invalid_state() -> Self {
        Self {
            kind: ProfileErrorKind::InvalidState,
        }
    }

    pub fn conflict() -> Self {
        Self {
            kind: ProfileErrorKind::Conflict,
        }
    }

    pub fn kind(&self) -> ProfileErrorKind {
        self.kind
    }
}

impl std::fmt::Display for ProfileError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self.kind {
            ProfileErrorKind::InvalidState => "Invalid profile state",
            ProfileErrorKind::Conflict => "Profile operation rejected",
        })
    }
}

impl std::error::Error for ProfileError {}

pub struct ProfileRead {
    pub revision: i64,
    pub state_text: String,
}

pub struct ProfileStatus {
    pub path: PathBuf,
    pub revision: i64,
    pub format: i64,
    pub id: String,
}

impl ProfileStatus {
    pub fn to_json(&self) -> Value {
        json!({
          "path": self.path.display().to_string(),
          "revision": self.revision,
          "format": self.format,
          "id": self.id,
        })
    }
}

pub struct ProfileStore {
    path: PathBuf,
    inner: Mutex<Inner>,
}

struct Inner {
    connection: Connection,
    owner: Option<Vec<u16>>,
}

impl ProfileStore {
    /// 打开既有 profile；create 只允许创建新文件，绝不覆盖已有路径。
    pub fn open(path: &Path, create: bool) -> HostResult<Self> {
        let requested = absolute(path)?;
        let created = if create {
            let file = create_exclusive(&requested)?;
            let metadata = file.metadata()?;
            let identity = FileIdentity {
                dev: metadata.dev(),
                ino: metadata.ino(),
            };
            // 必须在 SQLite 持锁前关闭：关闭同文件的任意 fd 会释放该进程的全部 fcntl 锁。
            drop(file);
            Some(identity)
        } else {
            None
        };
        let result = open_existing(&requested, created);
        if result.is_err() {
            if let Some(identity) = created {
                remove_created(&requested, identity);
            }
        }
        result
    }

    pub fn acquire(&self, owner: &[u16]) -> Result<ProfileRead, ProfileError> {
        if owner.is_empty() {
            return Err(ProfileError::conflict());
        }
        let mut inner = self.lock()?;
        match &inner.owner {
            Some(current) if current.as_slice() != owner => return Err(ProfileError::conflict()),
            Some(_) => {}
            None => inner.owner = Some(owner.to_vec()),
        }
        read_locked(&inner.connection)
    }

    /// 先按 Node 顺序检查 owner、revision、state，再在事务内比较并提交。
    pub fn save(
        &self,
        owner: &[u16],
        revision: &Json,
        state: Option<&Json>,
    ) -> Result<(), ProfileError> {
        let inner = self.lock()?;
        if inner.owner.as_deref() != Some(owner) {
            return Err(ProfileError::conflict());
        }
        let revision = profile_state::safe_integer(revision).ok_or_else(ProfileError::conflict)?;
        let state = state.ok_or_else(ProfileError::invalid_state)?;
        let text =
            profile_state::canonicalize_state(state).map_err(|_| ProfileError::invalid_state())?;
        inner
            .connection
            .execute_batch("BEGIN EXCLUSIVE")
            .map_err(db_error)?;
        let outcome = commit_save(&inner.connection, revision, &text);
        match outcome {
            Ok(()) => match inner.connection.execute_batch("COMMIT") {
                Ok(()) => Ok(()),
                Err(_) => {
                    let _ = inner.connection.execute_batch("ROLLBACK");
                    Err(ProfileError::conflict())
                }
            },
            Err(error) => {
                let _ = inner.connection.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    pub fn status(&self) -> Result<ProfileStatus, ProfileError> {
        self.with_status(|path, revision, format, id| {
            Ok(ProfileStatus {
                path: path.to_path_buf(),
                revision,
                format,
                id: id.to_owned(),
            })
        })
    }

    /// 借用字段仅在同步回调内有效；调用方在复制前取得自身的容量预算。
    pub(crate) fn with_status<T>(
        &self,
        view: impl FnOnce(&Path, i64, i64, &str) -> Result<T, ProfileError>,
    ) -> Result<T, ProfileError> {
        let inner = self.lock()?;
        let revision: i64 = inner
            .connection
            .query_row("SELECT revision FROM profile WHERE id=1", [], |row| {
                row.get(0)
            })
            .map_err(|_| ProfileError::conflict())?;
        (0..=MAX_SAFE_INTEGER)
            .contains(&revision)
            .then_some(())
            .ok_or_else(ProfileError::conflict)?;
        let digest = Sha256::digest(self.path.as_os_str().as_encoded_bytes());
        let mut id = [0u8; 16];
        for (index, byte) in digest.iter().take(8).enumerate() {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            id[index * 2] = HEX[(byte >> 4) as usize];
            id[index * 2 + 1] = HEX[(byte & 15) as usize];
        }
        let result = view(
            &self.path,
            revision,
            FORMAT,
            std::str::from_utf8(&id).map_err(|_| ProfileError::conflict())?,
        );
        drop(inner);
        result
    }

    /// Verify and sync the SQLite snapshot before publishing it without replacing a file.
    pub fn backup(&self, destination: &Path) -> HostResult<PathBuf> {
        let destination = resolve_destination(destination)?;
        ensure_secure_parent(&destination)?;
        match std::fs::symlink_metadata(&destination) {
            Ok(_) => return Err("backup destination already exists".into()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let parent = destination.parent().ok_or("backup path has no parent")?;
        let draft = tempfile::Builder::new()
            .prefix(".kitesurf-backup-")
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir_in(parent)?;
        let file = tempfile::NamedTempFile::new_in(draft.path())?;
        let handle = file.as_file().metadata()?;
        let inner = self
            .inner
            .lock()
            .map_err(|_| "profile lock is poisoned".to_string())?;
        let result = write_backup(&inner.connection, file.path(), &handle, &self.path);
        drop(inner);
        result.map_err(|error| format!("backup failed: {error}"))?;
        file.as_file().sync_all()?;
        file.persist_noclobber(&destination)
            .map_err(|error| error.error)?;
        Ok(destination)
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Inner>, ProfileError> {
        self.inner.lock().map_err(|_| ProfileError::conflict())
    }
}

#[derive(Clone, Copy)]
struct FileIdentity {
    dev: u64,
    ino: u64,
}

fn open_existing(requested: &Path, created: Option<FileIdentity>) -> HostResult<ProfileStore> {
    let path = std::fs::canonicalize(requested)?;
    ensure_secure_parent(&path)?;
    let metadata = std::fs::metadata(&path)?;
    ensure_profile_file(&metadata)?;
    if let Some(created) = created {
        if created.dev != metadata.dev() || created.ino != metadata.ino() {
            return Err("profile path was replaced during creation".into());
        }
    }
    let connection = Connection::open_with_flags(
        &path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|_| "Cannot open profile: sqlite open failed".to_string())?;
    let after = std::fs::metadata(&path)?;
    ensure_profile_file(&after)?;
    if after.dev() != metadata.dev() || after.ino() != metadata.ino() {
        return Err("profile path changed while opening".into());
    }
    connection
        .busy_timeout(Duration::ZERO)
        .map_err(|_| "Cannot open profile: sqlite timeout failed".to_string())?;
    connection
        .execute_batch("PRAGMA locking_mode=EXCLUSIVE; PRAGMA synchronous=FULL; BEGIN EXCLUSIVE")
        .map_err(|_| "Cannot open profile: sqlite lock failed".to_string())?;
    if created.is_some() {
        create_schema(&connection)?;
    } else {
        check_format(&connection)?;
    }
    connection
        .execute_batch("COMMIT")
        .map_err(|_| "Cannot open profile: commit failed".to_string())?;
    read_locked(&connection).map_err(|_| "Cannot open profile: invalid state".to_string())?;
    Ok(ProfileStore {
        path,
        inner: Mutex::new(Inner {
            connection,
            owner: None,
        }),
    })
}

fn create_schema(connection: &Connection) -> HostResult<()> {
    connection
        .pragma_update(None, "application_id", APPLICATION_ID)
        .map_err(|_| "Cannot open profile: application id failed".to_string())?;
    connection
        .pragma_update(None, "user_version", FORMAT)
        .map_err(|_| "Cannot open profile: format failed".to_string())?;
    connection
        .execute_batch(
            "CREATE TABLE profile (id INTEGER PRIMARY KEY CHECK(id=1), \
             revision INTEGER NOT NULL, state TEXT NOT NULL)",
        )
        .map_err(|_| "Cannot open profile: schema failed".to_string())?;
    connection
        .execute(
            "INSERT INTO profile VALUES (1, 0, ?1)",
            params![EMPTY_STATE],
        )
        .map_err(|_| "Cannot open profile: initial state failed".to_string())?;
    Ok(())
}

fn check_format(connection: &Connection) -> HostResult<()> {
    let app: i64 = connection
        .query_row("PRAGMA application_id", [], |row| row.get(0))
        .map_err(|_| "Cannot open profile: application id unreadable".to_string())?;
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|_| "Cannot open profile: format unreadable".to_string())?;
    if app != APPLICATION_ID || version != FORMAT {
        return Err("Unsupported profile format".into());
    }
    let check: String = connection
        .query_row("PRAGMA quick_check", [], |row| row.get(0))
        .map_err(|_| "Cannot open profile: integrity check failed".to_string())?;
    if check != "ok" {
        return Err("Cannot open profile: integrity check failed".into());
    }
    Ok(())
}

fn commit_save(connection: &Connection, revision: i64, text: &str) -> Result<(), ProfileError> {
    let previous = connection
        .query_row(
            "SELECT revision, state FROM profile WHERE id=1",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(|_| ProfileError::conflict())?
        .ok_or_else(ProfileError::conflict)?;
    if previous.0 == revision {
        return (previous.1 == text)
            .then_some(())
            .ok_or_else(ProfileError::conflict);
    }
    if revision != previous.0.saturating_add(1) {
        return Err(ProfileError::conflict());
    }
    connection
        .execute(
            "UPDATE profile SET revision=?1, state=?2 WHERE id=1",
            params![revision, text],
        )
        .map_err(|_| ProfileError::conflict())?;
    Ok(())
}

fn read_locked(connection: &Connection) -> Result<ProfileRead, ProfileError> {
    let row = connection
        .query_row(
            "SELECT revision, state FROM profile WHERE id=1",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(|_| ProfileError::conflict())?
        .ok_or_else(ProfileError::conflict)?;
    if !(0..=MAX_SAFE_INTEGER).contains(&row.0) {
        return Err(ProfileError::conflict());
    }
    let parsed = profile_state::parse(&row.1).map_err(|_| ProfileError::conflict())?;
    profile_state::validate_state(&parsed).map_err(|_| ProfileError::conflict())?;
    Ok(ProfileRead {
        revision: row.0,
        state_text: row.1,
    })
}

fn write_backup(
    connection: &Connection,
    destination: &Path,
    handle: &std::fs::Metadata,
    source: &Path,
) -> HostResult<()> {
    let live: (i64, String) = connection.query_row(
        "SELECT revision, state FROM profile WHERE id=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let Some(destination_text) = destination.to_str() else {
        return Err("backup path is not UTF-8".into());
    };
    connection
        .execute("VACUUM INTO ?1", params![destination_text])
        .map_err(|_| "vacuum into failed".to_string())?;
    verify_backup(destination, handle, source, &live)
}

fn verify_backup(
    destination: &Path,
    handle: &std::fs::Metadata,
    source: &Path,
    live: &(i64, String),
) -> HostResult<()> {
    let metadata = std::fs::metadata(destination)?;
    if metadata.dev() != handle.dev() || metadata.ino() != handle.ino() {
        return Err("backup path was replaced".into());
    }
    if !metadata.is_file() || metadata.nlink() != 1 || metadata.mode() & 0o777 != 0o600 {
        return Err("backup permissions are not private".into());
    }
    let source_metadata = std::fs::metadata(source)?;
    if metadata.dev() == source_metadata.dev() && metadata.ino() == source_metadata.ino() {
        return Err("backup aliases the live profile".into());
    }
    let connection = Connection::open_with_flags(
        destination,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|_| "backup is not a readable database".to_string())?;
    check_format(&connection)?;
    let row: (i64, String) = connection.query_row(
        "SELECT revision, state FROM profile WHERE id=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if &row != live {
        return Err("backup content differs from the profile".into());
    }
    Ok(())
}

fn create_exclusive(path: &Path) -> HostResult<File> {
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|error| format!("cannot create {}: {error}", path.display()).into())
}

fn resolve_destination(path: &Path) -> HostResult<PathBuf> {
    let absolute = absolute(path)?;
    let parent = absolute.parent().ok_or("backup path has no parent")?;
    let name = absolute.file_name().ok_or("backup path has no file name")?;
    Ok(std::fs::canonicalize(parent)?.join(name))
}

fn ensure_profile_file(metadata: &std::fs::Metadata) -> HostResult<()> {
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err("profile must be a regular file without hard links".into());
    }
    if metadata.mode() & 0o022 != 0 {
        return Err("profile must not be writable by other users".into());
    }
    let euid = unsafe { libc::geteuid() };
    if metadata.uid() != euid {
        return Err("profile must be owned by the current user".into());
    }
    Ok(())
}

/// 目录若可被其他用户替换文件则拒绝；sticky 且属于当前用户或 root 的目录才算安全。
fn ensure_secure_parent(path: &Path) -> HostResult<()> {
    let euid = unsafe { libc::geteuid() };
    let mut current = path.parent();
    while let Some(directory) = current {
        let metadata = std::fs::metadata(directory)?;
        if !metadata.is_dir() {
            return Err("profile parent is not a directory".into());
        }
        let writable = metadata.mode() & 0o022 != 0;
        let trusted = metadata.uid() == euid || metadata.uid() == 0;
        let sticky = metadata.mode() & 0o1000 != 0;
        if writable && (!trusted || !sticky) {
            return Err("profile directory is writable by other users".into());
        }
        current = directory.parent();
    }
    Ok(())
}

fn remove_created(path: &Path, created: FileIdentity) {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return;
    };
    if metadata.dev() == created.dev && metadata.ino() == created.ino && metadata.nlink() == 1 {
        let _ = std::fs::remove_file(path);
    }
}

fn absolute(path: &Path) -> HostResult<PathBuf> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    Ok(path)
}

fn db_error(_: rusqlite::Error) -> ProfileError {
    ProfileError::conflict()
}
