// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

use std::fs::{self, DirBuilder, OpenOptions, Permissions};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;

use kitesurf_local_host::HostResult;
use sha2::{Digest, Sha256};

use super::RuntimePaths;

// A file count follows the magic. Each record contains a path length, file length, SHA-256,
// path and file contents. The numeric fields are big endian; the builder uses the same limits.
const MAGIC: &[u8] = b"KSRTv001";
const MAX_BYTES: usize = 1024 * 1024 * 1024;
const MAX_FILE_BYTES: usize = 512 * 1024 * 1024;
const MAX_FILES: u32 = 32768;
const MAX_PATH_BYTES: usize = 1024;

#[cfg(feature = "embedded-runtime")]
pub(super) fn unpack(run_dir: &Path, allow_private_network: bool) -> HostResult<RuntimePaths> {
    let bundle = include_bytes!(env!("KITESURF_RUNTIME_BUNDLE"));
    unpack_payload(run_dir, bundle, allow_private_network)
}

fn unpack_payload(
    run_dir: &Path,
    payload: &[u8],
    allow_private_network: bool,
) -> HostResult<RuntimePaths> {
    let root = run_dir.join("runtime");
    DirBuilder::new().mode(0o700).create(&root)?;
    unpack_files(&root, payload)
        .and_then(|()| runtime_paths(&root, allow_private_network))
        .inspect_err(|_| {
            let _ = fs::remove_dir_all(&root);
        })
}

fn unpack_files(root: &Path, payload: &[u8]) -> HostResult<()> {
    if payload.len() > MAX_BYTES {
        return Err("embedded runtime exceeds its size limit".into());
    }
    let mut input = payload
        .strip_prefix(MAGIC)
        .ok_or("invalid embedded runtime header")?;
    let count = u32::from_be_bytes(*take_array(&mut input)?);
    if count > MAX_FILES {
        return Err("embedded runtime has too many files".into());
    }
    for _ in 0..count {
        unpack_file(root, &mut input)?;
    }
    if !input.is_empty() {
        return Err("embedded runtime has trailing data".into());
    }
    Ok(())
}

fn unpack_file(root: &Path, input: &mut &[u8]) -> HostResult<()> {
    let name_len = u16::from_be_bytes(*take_array(input)?) as usize;
    let file_len = u32::from_be_bytes(*take_array(input)?) as usize;
    if name_len > MAX_PATH_BYTES || file_len > MAX_FILE_BYTES {
        return Err("embedded runtime entry exceeds its size limit".into());
    }
    let expected = take_array::<32>(input)?;
    let name = std::str::from_utf8(take(input, name_len)?)?;
    if !valid_path(name) {
        return Err("invalid path in embedded runtime".into());
    }
    let contents = take(input, file_len)?;
    let actual: [u8; 32] = Sha256::digest(contents).into();
    if actual != *expected {
        return Err(format!("embedded runtime checksum mismatch: {name}").into());
    }
    let destination = root.join(name);
    let parent = destination
        .parent()
        .ok_or("embedded runtime path has no parent")?;
    DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(parent)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(destination)?;
    file.write_all(contents)?;
    if name == "workerd" {
        file.set_permissions(Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn valid_path(name: &str) -> bool {
    (name == "workerd" || name.starts_with("config/") || name.starts_with("assets/"))
        && name.split('/').all(|component| {
            !component.is_empty()
                && component.len() <= 255
                && component != "."
                && component != ".."
                && component
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        })
}

fn take<'a>(input: &mut &'a [u8], count: usize) -> HostResult<&'a [u8]> {
    let (bytes, rest) = input
        .split_at_checked(count)
        .ok_or("truncated embedded runtime")?;
    *input = rest;
    Ok(bytes)
}

fn take_array<'a, const N: usize>(input: &mut &'a [u8]) -> HostResult<&'a [u8; N]> {
    Ok(take(input, N)?.try_into()?)
}

fn runtime_paths(root: &Path, allow_private_network: bool) -> HostResult<RuntimePaths> {
    let config_root = root.join("config");
    let public = config_root.join("local-public.capnp");
    let private = config_root.join("local-private.capnp");
    let workerd = root.join("workerd");
    let assets_root = root.join("assets");
    let assets_manifest = config_root.join("assets-manifest.json");
    let required = [
        &workerd,
        &public,
        &private,
        &assets_manifest,
        &config_root.join("modules.capnp"),
        &config_root.join("modules/index.js"),
    ];
    if !assets_root.is_dir() || required.iter().any(|path| !path.is_file()) {
        return Err("embedded runtime is missing required components".into());
    }
    Ok(RuntimePaths {
        workerd,
        config: if allow_private_network {
            private
        } else {
            public
        },
        assets_root,
        assets_manifest,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const FILES: &[(&str, &[u8])] = &[
        ("workerd", b"workerd"),
        ("config/local-public.capnp", b"public"),
        ("config/local-private.capnp", b"private"),
        ("config/assets-manifest.json", b"{}"),
        ("config/modules.capnp", b"modules"),
        ("config/modules/index.js", b"js"),
        ("assets/rbi/scene_host.json", b"RBI"),
    ];

    fn payload(files: &[(&str, &[u8])]) -> Vec<u8> {
        let mut bytes = MAGIC.to_vec();
        bytes.extend_from_slice(&(files.len() as u32).to_be_bytes());
        for (name, contents) in files {
            bytes.extend_from_slice(&(name.len() as u16).to_be_bytes());
            bytes.extend_from_slice(&(contents.len() as u32).to_be_bytes());
            bytes.extend_from_slice(&Sha256::digest(contents));
            bytes.extend_from_slice(name.as_bytes());
            bytes.extend_from_slice(contents);
        }
        bytes
    }

    #[test]
    fn unpacks_private_runtime_and_both_network_configs() {
        let bundle = payload(FILES);
        for private in [false, true] {
            let run_dir = tempfile::tempdir().unwrap();
            let paths = unpack_payload(run_dir.path(), &bundle, private).unwrap();
            let expected_config = if private { "private" } else { "public" };
            assert_eq!(fs::read_to_string(paths.config).unwrap(), expected_config);
            assert_eq!(fs::read_to_string(paths.assets_manifest).unwrap(), "{}");
            let rbi = paths.assets_root.join("rbi/scene_host.json");
            assert_eq!(fs::read_to_string(&rbi).unwrap(), "RBI");
            assert_eq!(
                fs::metadata(&rbi).unwrap().permissions().mode() & 0o777,
                0o600
            );
            assert_eq!(fs::read_to_string(&paths.workerd).unwrap(), "workerd");
            assert_eq!(
                fs::metadata(&paths.workerd).unwrap().permissions().mode() & 0o777,
                0o700
            );
            let root = run_dir.path().join("runtime");
            assert_eq!(
                fs::metadata(&root).unwrap().permissions().mode() & 0o777,
                0o700
            );
            drop(run_dir);
            assert!(!root.exists());
        }
    }

    #[test]
    fn rejects_malformed_bundles_and_rolls_back_files() {
        let bundle = payload(FILES);
        let mut corrupted = bundle.clone();
        *corrupted.last_mut().unwrap() ^= 1;
        let mut trailing = bundle.clone();
        trailing.push(0);
        let mut large = payload(&[("workerd", b"workerd")]);
        let file_len = MAGIC.len() + 4 + 2;
        large[file_len..file_len + 4].copy_from_slice(&u32::MAX.to_be_bytes());
        let mut duplicate = FILES.to_vec();
        duplicate.push(FILES[0]);
        let malformed = [
            bundle[..bundle.len() - 1].to_vec(),
            corrupted,
            trailing,
            large,
            payload(&duplicate),
            payload(&[("assets/../../escaped", b"x")]),
            payload(&[("/escaped", b"x")]),
            payload(&[("assets/bad\\name", b"x")]),
            payload(&[("workerd", b"workerd")]),
        ];
        let run_dir = tempfile::tempdir().unwrap();
        for invalid in malformed {
            assert!(unpack_payload(run_dir.path(), &invalid, false).is_err());
            assert_eq!(fs::read_dir(run_dir.path()).unwrap().count(), 0);
        }
    }

    #[test]
    fn preserves_preexisting_runtime_path() {
        let run_dir = tempfile::tempdir().unwrap();
        let existing = run_dir.path().join("runtime");
        fs::write(&existing, "preserve").unwrap();
        assert!(unpack_payload(run_dir.path(), &payload(FILES), false).is_err());
        assert_eq!(fs::read_to_string(existing).unwrap(), "preserve");
    }
}
