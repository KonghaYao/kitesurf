// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

//! Discovers system fonts, matches CSS-like requests, and returns bounded single-face SFNTs.
//! The process speaks a length-framed protocol on stdin/stdout. It never accepts paths, directory
//! names, process arguments, or Fontconfig configuration from its caller.

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use fontique::{
    Collection, CollectionOptions, FallbackKey, FamilyInfo, FontInfo, FontStyle, FontWeight,
    FontWidth, GenericFamily, Language, Script, SourceKind,
};
use read_fonts::{FontRef, TableProvider};
use serde::{Deserialize, Serialize};

pub const MAX_FRAME_HEADER_BYTES: u32 = 64 * 1024;
pub const MAX_FACE_BYTES: usize = 24 * 1024 * 1024;
pub const MAX_SOURCE_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_TOTAL_FACE_BYTES: usize = 96 * 1024 * 1024;
const MAX_EMOJI_SBIX_BYTES: usize = 12 * 1024 * 1024;
const MAX_TEXT_CODEPOINTS: usize = 256;
const MAX_TEXT_BYTES: usize = 4096;
const MAX_FAMILY_NAME_BYTES: usize = 256;
const MAX_MATCH_FACES: usize = 4;
const EMOJI_PROBE_SAMPLE: char = '\u{1F600}';
const MAX_ISSUED_IDS: usize = 256;
const ID_TTL: Duration = Duration::from_secs(10 * 60);
const SFNT_HEADER_BYTES: usize = 12;
const SFNT_TABLE_RECORD_BYTES: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    BadRequest,
    UnknownId,
    ExpiredId,
    FaceTooLarge,
    FileChanged,
    UnsafeSource,
    InvalidFont,
    Io,
    EntropyUnavailable,
    NoUsableFace,
}

impl ErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BadRequest => "bad_request",
            Self::UnknownId => "unknown_id",
            Self::ExpiredId => "expired_id",
            Self::FaceTooLarge => "face_too_large",
            Self::FileChanged => "file_changed",
            Self::UnsafeSource => "unsafe_source",
            Self::InvalidFont => "invalid_font",
            Self::Io => "io",
            Self::EntropyUnavailable => "entropy_unavailable",
            Self::NoUsableFace => "no_usable_face",
        }
    }
}

#[derive(Debug, Clone)]
pub struct HostError {
    pub code: ErrorCode,
    pub message: &'static str,
}

impl HostError {
    pub fn new(code: ErrorCode, message: &'static str) -> Self {
        Self { code, message }
    }
}

pub type HostResult<T> = Result<T, HostError>;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatchRequest {
    #[serde(default)]
    pub family: Option<String>,
    #[serde(default)]
    pub script: Option<String>,
    #[serde(default)]
    pub locale: Option<String>,
    #[serde(default)]
    pub weight: Option<f32>,
    #[serde(default)]
    pub style: Option<String>,
    #[serde(default)]
    pub text: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FaceSummary {
    pub id: String,
    pub family: String,
    pub weight: f32,
    pub style: &'static str,
    pub width: f32,
    pub covered: bool,
    pub missing: u32,
    pub bytes: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct MatchResponse {
    pub faces: Vec<FaceSummary>,
    pub covered: bool,
    pub missing: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<&'static str>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProbeResponse {
    pub sans: bool,
    pub serif: bool,
    pub mono: bool,
    pub system_ui: bool,
    pub emoji: bool,
    pub families: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatsResponse {
    pub faces: usize,
    pub cached_faces: usize,
    pub cached_bytes: usize,
    pub matches: u64,
    pub served: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub limited_emoji: u64,
    pub rejected: u64,
}

#[derive(Debug, Clone)]
struct LoadedFace {
    bytes: Vec<u8>,
    source_len: u64,
    source_hash: u64,
    path: PathBuf,
}

struct MatchCandidate {
    family: FamilyInfo,
    index: usize,
    loaded: LoadedFace,
    missing: u32,
}

struct FaceEntry {
    key: (PathBuf, u32),
    family: String,
    weight: f32,
    style: FontStyle,
    width: f32,
    source_len: u64,
    source_hash: u64,
    face_index: u32,
    is_emoji: bool,
    extracted: Option<Arc<Vec<u8>>>,
    output_len: usize,
    created: Instant,
    last_used: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FamilyKind {
    Generic(GenericFamily),
    Script(Script),
    Emoji,
}

pub struct FontHost {
    collection: Collection,
    faces: HashMap<String, FaceEntry>,
    by_key: HashMap<(PathBuf, u32), String>,
    use_counter: u64,
    cache_bytes: usize,
    matches: u64,
    served: u64,
    cache_hits: u64,
    cache_misses: u64,
    limited_emoji: u64,
    rejected: u64,
}

impl Default for FontHost {
    fn default() -> Self {
        Self::new()
    }
}

impl FontHost {
    pub fn new() -> Self {
        let collection = Collection::new(CollectionOptions {
            shared: false,
            system_fonts: true,
        });
        Self {
            collection,
            faces: HashMap::new(),
            by_key: HashMap::new(),
            use_counter: 0,
            cache_bytes: 0,
            matches: 0,
            served: 0,
            cache_hits: 0,
            cache_misses: 0,
            limited_emoji: 0,
            rejected: 0,
        }
    }

    pub fn probe(&mut self) -> ProbeResponse {
        let families = self.collection.family_names().count();
        ProbeResponse {
            sans: self.has_generic(GenericFamily::SansSerif),
            serif: self.has_generic(GenericFamily::Serif),
            mono: self.has_generic(GenericFamily::Monospace),
            system_ui: self.has_generic(GenericFamily::SystemUi),
            emoji: self.has_emoji_sample(),
            families,
        }
    }

    fn has_generic(&mut self, generic: GenericFamily) -> bool {
        let ids: Vec<_> = self.collection.generic_families(generic).collect();
        ids.into_iter()
            .any(|id| self.collection.family(id).is_some())
    }

    fn has_emoji_sample(&mut self) -> bool {
        // Fontconfig can substitute a text family for the emoji generic.
        let request = MatchRequest {
            family: Some("emoji".to_string()),
            ..Default::default()
        };
        self.select_face(&request, &[EMOJI_PROBE_SAMPLE])
            .is_ok_and(|(candidate, _, _)| candidate.is_some_and(|face| face.missing == 0))
    }

    pub fn stats(&mut self) -> StatsResponse {
        self.purge_expired();
        StatsResponse {
            faces: self.faces.len(),
            cached_faces: self
                .faces
                .values()
                .filter(|entry| entry.extracted.is_some())
                .count(),
            cached_bytes: self.cache_bytes,
            matches: self.matches,
            served: self.served,
            cache_hits: self.cache_hits,
            cache_misses: self.cache_misses,
            limited_emoji: self.limited_emoji,
            rejected: self.rejected,
        }
    }

    pub fn match_faces(&mut self, request: &MatchRequest) -> HostResult<MatchResponse> {
        validate_match_request(request)?;
        self.purge_expired();
        self.matches += 1;
        let text = request.text.as_deref().unwrap_or("");
        let text_chars: Vec<char> = text.chars().collect();
        let (best, last_reason, is_emoji) = self.select_face(request, &text_chars)?;
        if let Some(candidate) = best {
            let summary = self.issue_face(
                &candidate.family.fonts()[candidate.index],
                candidate.family.name(),
                candidate.loaded,
                is_emoji,
                candidate.missing,
            )?;
            return Ok(MatchResponse {
                covered: summary.covered,
                missing: summary.missing,
                reason: (!summary.covered).then_some("missing_coverage"),
                faces: vec![summary],
            });
        }
        Ok(MatchResponse {
            faces: Vec::new(),
            covered: false,
            missing: text_chars.len() as u32,
            reason: Some(last_reason.as_str()),
        })
    }

    fn select_face(
        &mut self,
        request: &MatchRequest,
        text_chars: &[char],
    ) -> HostResult<(Option<MatchCandidate>, ErrorCode, bool)> {
        let (families, is_emoji) = self.candidate_families(request)?;
        let style = parse_style(request.style.as_deref())?;
        let weight = FontWeight::new(request.weight.unwrap_or(400.0).clamp(1.0, 1000.0));
        let width = FontWidth::from_ratio(1.0);
        let mut best: Option<MatchCandidate> = None;
        let mut last_reason = ErrorCode::NoUsableFace;

        'families: for family_id in families {
            let Some(family) = self.collection.family(family_id) else {
                continue;
            };
            if is_last_resort(family.name()) || family.name().len() > MAX_FAMILY_NAME_BYTES {
                continue;
            }
            let default_index = family.match_index(width, style, weight, true);
            let mut indices: Vec<usize> = default_index.into_iter().collect();
            if !text_chars.is_empty() {
                for index in 0..family.fonts().len() {
                    if !indices.contains(&index) {
                        indices.push(index);
                    }
                }
            }
            for index in indices {
                let Some(info) = family.fonts().get(index) else {
                    continue;
                };
                let loaded = match self.load_face(info, is_emoji) {
                    Ok(loaded) => loaded,
                    Err(error) => {
                        last_reason = error.code;
                        self.rejected += 1;
                        continue;
                    }
                };
                let missing = count_missing(&loaded.bytes, text_chars);
                if best
                    .as_ref()
                    .is_none_or(|candidate| missing < candidate.missing)
                {
                    best = Some(MatchCandidate {
                        family: family.clone(),
                        index,
                        loaded,
                        missing,
                    });
                }
                if missing == 0 {
                    break 'families;
                }
            }
        }

        Ok((best, last_reason, is_emoji))
    }

    pub fn face_bytes(&mut self, id: &str) -> HostResult<Arc<Vec<u8>>> {
        validate_id(id)?;
        self.purge_expired();
        let Some(entry) = self.faces.get_mut(id) else {
            return Err(HostError::new(ErrorCode::UnknownId, "unknown font id"));
        };
        self.use_counter += 1;
        entry.last_used = self.use_counter;
        if let Some(bytes) = &entry.extracted {
            self.cache_hits += 1;
            self.served += 1;
            return Ok(bytes.clone());
        }
        self.cache_misses += 1;
        let key = entry.key.clone();
        let source_len = entry.source_len;
        let source_hash = entry.source_hash;
        let face_index = entry.face_index;
        let is_emoji = entry.is_emoji;
        let (source, hash) = read_source(&key.0)?;
        if source.len() as u64 != source_len || hash != source_hash {
            self.faces.remove(id);
            self.by_key.remove(&key);
            return Err(HostError::new(
                ErrorCode::FileChanged,
                "font source changed after discovery",
            ));
        }
        let bytes = Arc::new(extract_face(
            &source,
            face_index,
            is_emoji,
            &mut self.limited_emoji,
        )?);
        self.cache_face(id, bytes.clone());
        self.served += 1;
        Ok(bytes)
    }

    fn candidate_families(
        &mut self,
        request: &MatchRequest,
    ) -> HostResult<(Vec<fontique::FamilyId>, bool)> {
        let kind = parse_family(request)?;
        let mut ids: Vec<fontique::FamilyId> = Vec::new();
        match kind {
            FamilyKind::Generic(generic) => {
                ids.extend(self.collection.generic_families(generic));
            }
            FamilyKind::Script(script) => {
                let lang = parse_locale(request.locale.as_deref());
                let key = FallbackKey::new(script, lang.as_ref());
                ids.extend(self.collection.fallback_families(key));
                if ids.is_empty()
                    && (script == Script::from_bytes(*b"Hira")
                        || script == Script::from_bytes(*b"Kana"))
                {
                    let hani = FallbackKey::new(Script::from_bytes(*b"Hani"), lang.as_ref());
                    ids.extend(self.collection.fallback_families(hani));
                }
            }
            FamilyKind::Emoji => {
                ids.extend(self.collection.generic_families(GenericFamily::Emoji));
            }
        }
        let mut seen = std::collections::HashSet::new();
        ids.retain(|id| seen.insert(*id));
        if ids.len() > MAX_MATCH_FACES {
            ids.truncate(MAX_MATCH_FACES);
        }
        Ok((ids, matches!(kind, FamilyKind::Emoji)))
    }

    fn load_face(&mut self, info: &FontInfo, is_emoji: bool) -> HostResult<LoadedFace> {
        let path = match info.source().kind() {
            SourceKind::Path(path) => PathBuf::from(path.as_ref()),
            SourceKind::Memory(_) => {
                return Err(HostError::new(
                    ErrorCode::UnsafeSource,
                    "in-memory font sources are not selectable",
                ))
            }
        };
        let (source, source_hash) = read_source(&path)?;
        let bytes = extract_face(&source, info.index(), is_emoji, &mut self.limited_emoji)?;
        Ok(LoadedFace {
            bytes,
            source_len: source.len() as u64,
            source_hash,
            path,
        })
    }

    fn issue_face(
        &mut self,
        info: &FontInfo,
        family_name: &str,
        loaded: LoadedFace,
        is_emoji: bool,
        missing: u32,
    ) -> HostResult<FaceSummary> {
        let key = (loaded.path, info.index());
        let bytes = Arc::new(loaded.bytes);
        self.use_counter += 1;
        let last_used = self.use_counter;
        if let Some(existing) = self.by_key.get(&key).cloned() {
            let unchanged = self.faces.get_mut(&existing).is_some_and(|entry| {
                if entry.source_len != loaded.source_len || entry.source_hash != loaded.source_hash
                {
                    return false;
                }
                entry.last_used = last_used;
                true
            });
            if unchanged {
                self.cache_face(&existing, bytes);
                let entry = self.faces.get(&existing).expect("issued font id");
                return Ok(summary_for(&existing, entry, missing));
            }
            self.remove_face(&existing);
        }
        let id = new_id(system_entropy)?;
        let entry = FaceEntry {
            key: key.clone(),
            family: family_name.to_string(),
            weight: info.weight().value(),
            style: info.style(),
            width: info.width().ratio(),
            source_len: loaded.source_len,
            source_hash: loaded.source_hash,
            face_index: info.index(),
            is_emoji,
            extracted: None,
            output_len: bytes.len(),
            created: Instant::now(),
            last_used,
        };
        self.faces.insert(id.clone(), entry);
        self.by_key.insert(key, id.clone());
        self.cache_face(&id, bytes);
        self.enforce_id_limit();
        let entry = self.faces.get(&id).expect("inserted font id");
        Ok(summary_for(&id, entry, missing))
    }

    fn cache_face(&mut self, id: &str, bytes: Arc<Vec<u8>>) {
        if bytes.len() > MAX_TOTAL_FACE_BYTES {
            return;
        }
        if self
            .faces
            .get(id)
            .and_then(|entry| entry.extracted.as_ref())
            .is_some()
        {
            return;
        }
        while self.cache_bytes + bytes.len() > MAX_TOTAL_FACE_BYTES {
            let victim = self
                .faces
                .iter()
                .filter(|(key, entry)| entry.extracted.is_some() && key.as_str() != id)
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone());
            let Some(victim) = victim else {
                break;
            };
            if let Some(entry) = self.faces.get_mut(&victim) {
                if let Some(old) = entry.extracted.take() {
                    self.cache_bytes = self.cache_bytes.saturating_sub(old.len());
                }
            }
        }
        if let Some(entry) = self.faces.get_mut(id) {
            self.cache_bytes += bytes.len();
            entry.extracted = Some(bytes);
        }
    }

    fn purge_expired(&mut self) {
        let now = Instant::now();
        let expired: Vec<String> = self
            .faces
            .iter()
            .filter(|(_, entry)| now.duration_since(entry.created) > ID_TTL)
            .map(|(id, _)| id.clone())
            .collect();
        for id in expired {
            self.remove_face(&id);
        }
    }

    fn enforce_id_limit(&mut self) {
        while self.faces.len() > MAX_ISSUED_IDS {
            let oldest = self
                .faces
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(id, _)| id.clone());
            let Some(oldest) = oldest else {
                break;
            };
            self.remove_face(&oldest);
        }
    }

    fn remove_face(&mut self, id: &str) {
        if let Some(entry) = self.faces.remove(id) {
            if let Some(bytes) = entry.extracted {
                self.cache_bytes = self.cache_bytes.saturating_sub(bytes.len());
            }
            self.by_key.remove(&entry.key);
        }
    }

    #[cfg(test)]
    fn insert_test_face(&mut self, id: &str, bytes: Vec<u8>) {
        let entry = FaceEntry {
            key: (PathBuf::from("/nonexistent"), 0),
            family: "Test".to_string(),
            weight: 400.0,
            style: FontStyle::Normal,
            width: 1.0,
            source_len: 0,
            source_hash: 0,
            face_index: 0,
            is_emoji: false,
            extracted: Some(Arc::new(bytes)),
            output_len: 1,
            created: Instant::now(),
            last_used: 0,
        };
        self.faces.insert(id.to_string(), entry);
    }
}

fn summary_for(id: &str, entry: &FaceEntry, missing: u32) -> FaceSummary {
    FaceSummary {
        id: id.to_string(),
        family: entry.family.clone(),
        weight: entry.weight,
        style: style_name(entry.style),
        width: entry.width,
        covered: missing == 0,
        missing,
        bytes: entry.output_len,
    }
}

fn style_name(style: FontStyle) -> &'static str {
    match style {
        FontStyle::Normal => "normal",
        FontStyle::Italic => "italic",
        FontStyle::Oblique(_) => "oblique",
    }
}

fn is_last_resort(name: &str) -> bool {
    let name = name.trim();
    name.eq_ignore_ascii_case("lastresort") || name.eq_ignore_ascii_case(".lastresort")
}

fn parse_family(request: &MatchRequest) -> HostResult<FamilyKind> {
    if let Some(script) = request.script.as_deref() {
        if request
            .family
            .as_deref()
            .is_none_or(|family| family == "script")
        {
            return Ok(FamilyKind::Script(parse_script(script)?));
        }
    }
    let Some(family) = request.family.as_deref() else {
        return Err(HostError::new(ErrorCode::BadRequest, "family is required"));
    };
    Ok(match family {
        "sans" | "sans-serif" | "ui-sans" => FamilyKind::Generic(GenericFamily::SansSerif),
        "serif" | "ui-serif" => FamilyKind::Generic(GenericFamily::Serif),
        "mono" | "monospace" | "ui-mono" => FamilyKind::Generic(GenericFamily::Monospace),
        "system-ui" => FamilyKind::Generic(GenericFamily::SystemUi),
        "emoji" => FamilyKind::Emoji,
        _ => return Err(HostError::new(ErrorCode::BadRequest, "unknown family")),
    })
}

fn parse_script(raw: &str) -> HostResult<Script> {
    let bytes = raw.as_bytes();
    if bytes.len() != 4 || !bytes.iter().all(u8::is_ascii_alphabetic) {
        return Err(HostError::new(ErrorCode::BadRequest, "invalid script tag"));
    }
    let mut tag = [0u8; 4];
    tag[0] = bytes[0].to_ascii_uppercase();
    for (target, source) in tag[1..].iter_mut().zip(&bytes[1..]) {
        *target = source.to_ascii_lowercase();
    }
    Ok(Script::from_bytes(tag))
}

fn parse_locale(raw: Option<&str>) -> Option<Language> {
    let raw = raw?.trim();
    if raw.is_empty() || raw.len() > 32 {
        return None;
    }
    if !raw
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return None;
    }
    raw.parse().ok()
}

fn parse_style(raw: Option<&str>) -> HostResult<FontStyle> {
    let Some(raw) = raw else {
        return Ok(FontStyle::Normal);
    };
    FontStyle::parse_css(raw).ok_or_else(|| HostError::new(ErrorCode::BadRequest, "invalid style"))
}

fn validate_match_request(request: &MatchRequest) -> HostResult<()> {
    if let Some(text) = &request.text {
        if text.len() > MAX_TEXT_BYTES || text.chars().count() > MAX_TEXT_CODEPOINTS {
            return Err(HostError::new(
                ErrorCode::BadRequest,
                "text sample too large",
            ));
        }
    }
    if let Some(family) = &request.family {
        if family.len() > 32 {
            return Err(HostError::new(ErrorCode::BadRequest, "family too long"));
        }
    }
    if let Some(locale) = &request.locale {
        if locale.len() > 32 {
            return Err(HostError::new(ErrorCode::BadRequest, "locale too long"));
        }
    }
    Ok(())
}

fn validate_id(id: &str) -> HostResult<()> {
    if id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(HostError::new(ErrorCode::BadRequest, "invalid font id"))
    }
}

fn count_missing(bytes: &[u8], chars: &[char]) -> u32 {
    if chars.is_empty() {
        return 0;
    }
    let Ok(font) = FontRef::from_index(bytes, 0) else {
        return chars.len() as u32;
    };
    let Ok(cmap) = font.cmap() else {
        return chars.len() as u32;
    };
    let Some((_, _, subtable)) = cmap.best_subtable() else {
        return chars.len() as u32;
    };
    chars
        .iter()
        .filter(|ch| subtable.map_codepoint(**ch as u32).is_none())
        .count() as u32
}

#[cfg(test)]
thread_local! {
    static EXTRA_FONT_ROOT: std::cell::RefCell<Option<PathBuf>> =
        const { std::cell::RefCell::new(None) };
}

fn allowed_font_root(path: &Path) -> bool {
    #[cfg(test)]
    if EXTRA_FONT_ROOT.with(|root| {
        root.borrow()
            .as_ref()
            .is_some_and(|root| path.starts_with(root))
    }) {
        return true;
    }
    let mut roots: Vec<PathBuf> = vec![
        PathBuf::from("/System/Library/Fonts"),
        PathBuf::from("/System/Library/AssetsV2"),
        PathBuf::from("/Library/Fonts"),
        PathBuf::from("/usr/share/fonts"),
        PathBuf::from("/usr/local/share/fonts"),
        PathBuf::from("/run/host/fonts"),
    ];
    if let Some(home) = std::env::var_os("HOME") {
        let home = PathBuf::from(home);
        roots.push(home.join("Library/Fonts"));
        roots.push(home.join(".fonts"));
        roots.push(home.join(".local/share/fonts"));
    }
    if let Some(data) = std::env::var_os("XDG_DATA_HOME") {
        roots.push(PathBuf::from(data).join("fonts"));
    }
    roots.iter().any(|root| path.starts_with(root))
}

fn read_source(path: &Path) -> HostResult<(Vec<u8>, u64)> {
    if !path.is_absolute() {
        return Err(HostError::new(
            ErrorCode::UnsafeSource,
            "font path is not absolute",
        ));
    }
    let canonical = std::fs::canonicalize(path)
        .map_err(|_| HostError::new(ErrorCode::Io, "font source is unavailable"))?;
    if !allowed_font_root(&canonical) {
        return Err(HostError::new(
            ErrorCode::UnsafeSource,
            "font source is outside the system font roots",
        ));
    }
    let metadata = std::fs::metadata(&canonical)
        .map_err(|_| HostError::new(ErrorCode::Io, "font source is unavailable"))?;
    if !metadata.is_file() {
        return Err(HostError::new(
            ErrorCode::UnsafeSource,
            "font source is not a regular file",
        ));
    }
    let len = metadata.len();
    if len == 0 || len > MAX_SOURCE_BYTES {
        return Err(HostError::new(
            ErrorCode::FaceTooLarge,
            "font source size is not allowed",
        ));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let file = options
        .open(&canonical)
        .map_err(|_| HostError::new(ErrorCode::Io, "font source could not be opened"))?;
    let opened = file
        .metadata()
        .map_err(|_| HostError::new(ErrorCode::Io, "font source metadata is unavailable"))?;
    if !opened.is_file() || opened.len() != len {
        return Err(HostError::new(
            ErrorCode::FileChanged,
            "font source changed while opening",
        ));
    }
    let mut bytes = Vec::with_capacity(len as usize);
    let mut limited = file.take(MAX_SOURCE_BYTES + 1);
    limited
        .read_to_end(&mut bytes)
        .map_err(|_| HostError::new(ErrorCode::Io, "font source could not be read"))?;
    if bytes.len() as u64 > MAX_SOURCE_BYTES {
        return Err(HostError::new(
            ErrorCode::FaceTooLarge,
            "font source is too large",
        ));
    }
    let hash = fnv1a64(&bytes);
    Ok((bytes, hash))
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(any(all(target_os = "linux", target_env = "gnu"), target_os = "macos"))]
fn system_entropy(bytes: &mut [u8; 16]) -> std::io::Result<()> {
    // SAFETY: getentropy writes at most bytes.len() bytes to this live buffer.
    let result = unsafe { libc::getentropy(bytes.as_mut_ptr().cast(), bytes.len()) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(any(all(target_os = "linux", target_env = "gnu"), target_os = "macos")))]
fn system_entropy(bytes: &mut [u8; 16]) -> std::io::Result<()> {
    std::fs::File::open("/dev/urandom")?.read_exact(bytes)
}

fn new_id(mut fill: impl FnMut(&mut [u8; 16]) -> std::io::Result<()>) -> HostResult<String> {
    let mut bytes = [0u8; 16];
    loop {
        match fill(&mut bytes) {
            Ok(()) => break,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => {
                return Err(HostError::new(
                    ErrorCode::EntropyUnavailable,
                    "system entropy is unavailable",
                ))
            }
        }
    }
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn read_u16(bytes: &[u8], offset: usize) -> Option<u16> {
    Some(u16::from_be_bytes(
        bytes.get(offset..offset + 2)?.try_into().ok()?,
    ))
}

fn read_u32(bytes: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_be_bytes(
        bytes.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

struct TableRecord {
    tag: [u8; 4],
    checksum: u32,
    offset: usize,
    len: usize,
}

fn parse_tables(bytes: &[u8], face_index: u32) -> HostResult<([u8; 4], Vec<TableRecord>)> {
    let base = if bytes.len() >= 4 && &bytes[0..4] == b"ttcf" {
        let count = read_u32(bytes, 8)
            .ok_or_else(|| HostError::new(ErrorCode::InvalidFont, "truncated font collection"))?;
        if face_index >= count {
            return Err(HostError::new(
                ErrorCode::BadRequest,
                "face index is outside the collection",
            ));
        }
        read_u32(bytes, 12 + face_index as usize * 4)
            .ok_or_else(|| HostError::new(ErrorCode::InvalidFont, "truncated font collection"))?
            as usize
    } else {
        if face_index != 0 {
            return Err(HostError::new(
                ErrorCode::BadRequest,
                "face index is outside the font",
            ));
        }
        0
    };
    let version: [u8; 4] = bytes
        .get(base..base + 4)
        .and_then(|slice| slice.try_into().ok())
        .ok_or_else(|| HostError::new(ErrorCode::InvalidFont, "truncated font header"))?;
    if version != [0x00, 0x01, 0x00, 0x00] && &version != b"OTTO" {
        return Err(HostError::new(
            ErrorCode::InvalidFont,
            "unsupported font version",
        ));
    }
    let count = read_u16(bytes, base + 4)
        .ok_or_else(|| HostError::new(ErrorCode::InvalidFont, "truncated font header"))?
        as usize;
    if count == 0 || count > 4096 {
        return Err(HostError::new(
            ErrorCode::InvalidFont,
            "unsupported table count",
        ));
    }
    let directory_end = base
        .checked_add(SFNT_HEADER_BYTES)
        .and_then(|value| value.checked_add(count * SFNT_TABLE_RECORD_BYTES))
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| HostError::new(ErrorCode::InvalidFont, "truncated table directory"))?;
    let _ = directory_end;
    let mut records = Vec::with_capacity(count);
    for index in 0..count {
        let record = base + SFNT_HEADER_BYTES + index * SFNT_TABLE_RECORD_BYTES;
        let tag: [u8; 4] = bytes
            .get(record..record + 4)
            .and_then(|slice| slice.try_into().ok())
            .ok_or_else(|| HostError::new(ErrorCode::InvalidFont, "truncated table record"))?;
        let checksum = read_u32(bytes, record + 4)
            .ok_or_else(|| HostError::new(ErrorCode::InvalidFont, "truncated table record"))?;
        let offset = read_u32(bytes, record + 8)
            .ok_or_else(|| HostError::new(ErrorCode::InvalidFont, "truncated table record"))?
            as usize;
        let len = read_u32(bytes, record + 12)
            .ok_or_else(|| HostError::new(ErrorCode::InvalidFont, "truncated table record"))?
            as usize;
        offset
            .checked_add(len)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| HostError::new(ErrorCode::InvalidFont, "table exceeds font data"))?;
        records.push(TableRecord {
            tag,
            checksum,
            offset,
            len,
        });
    }
    Ok((version, records))
}

fn table<'a>(bytes: &'a [u8], records: &[TableRecord], tag: &[u8; 4]) -> Option<&'a [u8]> {
    let record = records.iter().find(|record| &record.tag == tag)?;
    bytes.get(record.offset..record.offset + record.len)
}

fn checksum(data: &[u8]) -> u32 {
    let mut sum = 0u32;
    let mut offset = 0;
    while offset + 4 <= data.len() {
        sum = sum.wrapping_add(u32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]));
        offset += 4;
    }
    if offset < data.len() {
        let mut tail = [0u8; 4];
        tail[..data.len() - offset].copy_from_slice(&data[offset..]);
        sum = sum.wrapping_add(u32::from_be_bytes(tail));
    }
    sum
}

fn extract_face(
    bytes: &[u8],
    face_index: u32,
    is_emoji: bool,
    limited_emoji: &mut u64,
) -> HostResult<Vec<u8>> {
    let (version, records) = parse_tables(bytes, face_index)?;
    let has_sbix = records.iter().any(|record| &record.tag == b"sbix");
    let is_collection = bytes.len() >= 4 && &bytes[0..4] == b"ttcf";
    let total: usize = records.iter().map(|record| record.len).sum();
    let needs_rebuild = is_collection || (is_emoji && total > MAX_FACE_BYTES);
    if !needs_rebuild && !has_sbix {
        if bytes.len() > MAX_FACE_BYTES {
            return Err(HostError::new(
                ErrorCode::FaceTooLarge,
                "font face exceeds the size budget",
            ));
        }
        validate_face(bytes)?;
        return Ok(bytes.to_vec());
    }
    if total > MAX_FACE_BYTES && !(is_emoji && has_sbix) {
        return Err(HostError::new(
            ErrorCode::FaceTooLarge,
            "font face exceeds the size budget",
        ));
    }

    let sbix_limited = if is_emoji && has_sbix {
        let sbix = table(bytes, &records, b"sbix")
            .ok_or_else(|| HostError::new(ErrorCode::InvalidFont, "missing sbix table"))?;
        if sbix.len() > MAX_EMOJI_SBIX_BYTES {
            *limited_emoji += 1;
            Some(limit_sbix_strikes(sbix, MAX_EMOJI_SBIX_BYTES)?)
        } else {
            None
        }
    } else {
        None
    };

    let output_len = output_size(&records, sbix_limited.as_deref())?;
    if output_len > MAX_FACE_BYTES {
        return Err(HostError::new(
            ErrorCode::FaceTooLarge,
            "font face exceeds the size budget",
        ));
    }
    let out = build_sfnt(&version, bytes, &records, sbix_limited.as_deref())?;
    validate_face(&out)?;
    Ok(out)
}

fn output_size(records: &[TableRecord], sbix: Option<&[u8]>) -> HostResult<usize> {
    let count = records.len();
    let mut size = SFNT_HEADER_BYTES
        .checked_add(count * SFNT_TABLE_RECORD_BYTES)
        .ok_or_else(|| HostError::new(ErrorCode::FaceTooLarge, "font output overflows"))?;
    for record in records {
        let len = if &record.tag == b"sbix" {
            sbix.map_or(record.len, |bytes| bytes.len())
        } else {
            record.len
        };
        size = size
            .checked_add((len + 3) & !3)
            .ok_or_else(|| HostError::new(ErrorCode::FaceTooLarge, "font output overflows"))?;
    }
    Ok(size)
}

fn build_sfnt(
    version: &[u8; 4],
    source: &[u8],
    records: &[TableRecord],
    sbix: Option<&[u8]>,
) -> HostResult<Vec<u8>> {
    let count = records.len();
    let count_u16 = u16::try_from(count)
        .map_err(|_| HostError::new(ErrorCode::FaceTooLarge, "too many tables"))?;
    let record_bytes = count_u16
        .checked_mul(SFNT_TABLE_RECORD_BYTES as u16)
        .ok_or_else(|| HostError::new(ErrorCode::FaceTooLarge, "table directory overflows"))?;
    let entry_selector = (31 - (count as u32).leading_zeros()) as u16;
    let search_range = 16u32
        .checked_shl(u32::from(entry_selector))
        .unwrap_or(u32::from(record_bytes))
        .min(u32::from(u16::MAX)) as u16;
    let range_shift = record_bytes.saturating_sub(search_range);

    let mut out = Vec::with_capacity(output_size(records, sbix)?);
    out.extend_from_slice(version);
    out.extend_from_slice(&count_u16.to_be_bytes());
    out.extend_from_slice(&search_range.to_be_bytes());
    out.extend_from_slice(&entry_selector.to_be_bytes());
    out.extend_from_slice(&range_shift.to_be_bytes());
    let mut data_offset = SFNT_HEADER_BYTES + count * SFNT_TABLE_RECORD_BYTES;
    let mut head_record = None;
    let mut sbix_record = None;
    for record in records {
        let data = if &record.tag == b"sbix" {
            sbix.unwrap_or_else(|| {
                source
                    .get(record.offset..record.offset + record.len)
                    .unwrap_or(&[])
            })
        } else {
            source
                .get(record.offset..record.offset + record.len)
                .unwrap_or(&[])
        };
        let sum = if &record.tag == b"sbix" {
            checksum(data)
        } else {
            record.checksum
        };
        let record_start = out.len();
        out.extend_from_slice(&record.tag);
        out.extend_from_slice(&sum.to_be_bytes());
        out.extend_from_slice(&(data_offset as u32).to_be_bytes());
        out.extend_from_slice(&(data.len() as u32).to_be_bytes());
        if &record.tag == b"head" {
            head_record = Some(record_start);
        }
        if &record.tag == b"sbix" {
            sbix_record = Some(record_start);
        }
        data_offset += (data.len() + 3) & !3;
    }
    for record in records {
        let data = if &record.tag == b"sbix" {
            sbix.unwrap_or_else(|| {
                source
                    .get(record.offset..record.offset + record.len)
                    .unwrap_or(&[])
            })
        } else {
            source
                .get(record.offset..record.offset + record.len)
                .unwrap_or(&[])
        };
        out.extend_from_slice(data);
        while out.len() % 4 != 0 {
            out.push(0);
        }
    }
    if let Some(record_start) = head_record {
        let offset = read_u32(&out, record_start + 8)
            .ok_or_else(|| HostError::new(ErrorCode::InvalidFont, "missing head offset"))?
            as usize;
        let len = read_u32(&out, record_start + 12)
            .ok_or_else(|| HostError::new(ErrorCode::InvalidFont, "missing head length"))?
            as usize;
        if len < 12 || offset + len > out.len() {
            return Err(HostError::new(ErrorCode::InvalidFont, "invalid head table"));
        }
        out[offset + 8..offset + 12].fill(0);
        let table_sum = checksum(&out[offset..offset + len]);
        out[record_start + 4..record_start + 8].copy_from_slice(&table_sum.to_be_bytes());
        let adjustment = 0xB1B0_AFBAu32.wrapping_sub(checksum(&out));
        out[offset + 8..offset + 12].copy_from_slice(&adjustment.to_be_bytes());
    }
    if let Some(record_start) = sbix_record {
        let offset = read_u32(&out, record_start + 8)
            .ok_or_else(|| HostError::new(ErrorCode::InvalidFont, "missing sbix offset"))?
            as usize;
        let len = read_u32(&out, record_start + 12)
            .ok_or_else(|| HostError::new(ErrorCode::InvalidFont, "missing sbix length"))?
            as usize;
        let sum = checksum(&out[offset..offset + len]);
        out[record_start + 4..record_start + 8].copy_from_slice(&sum.to_be_bytes());
    }
    Ok(out)
}

fn limit_sbix_strikes(sbix: &[u8], budget: usize) -> HostResult<Vec<u8>> {
    let count = read_u32(sbix, 4)
        .ok_or_else(|| HostError::new(ErrorCode::InvalidFont, "truncated sbix header"))?
        as usize;
    if count == 0 || count > 64 {
        return Err(HostError::new(
            ErrorCode::InvalidFont,
            "invalid sbix strike count",
        ));
    }
    let table_len = sbix.len();
    let mut offsets = Vec::with_capacity(count + 1);
    for index in 0..count {
        let offset = read_u32(sbix, 8 + index * 4)
            .ok_or_else(|| HostError::new(ErrorCode::InvalidFont, "truncated sbix offsets"))?
            as usize;
        if offset < 8 + count * 4 || offset >= table_len {
            return Err(HostError::new(
                ErrorCode::InvalidFont,
                "invalid sbix strike offset",
            ));
        }
        offsets.push(offset);
    }
    offsets.push(table_len);
    for pair in offsets.windows(2) {
        if pair[0] >= pair[1] {
            return Err(HostError::new(
                ErrorCode::InvalidFont,
                "unsorted sbix strikes",
            ));
        }
    }
    let mut selected = 0usize;
    let mut total = 8 + 4 * count;
    for index in 0..count {
        let size = offsets[index + 1] - offsets[index];
        let next_total = total + size;
        if selected == 0 || next_total <= budget {
            if selected == 0 && next_total > budget {
                return Err(HostError::new(
                    ErrorCode::FaceTooLarge,
                    "smallest emoji strike exceeds the size budget",
                ));
            }
            selected += 1;
            total = next_total;
        } else {
            break;
        }
    }
    let header_len = 8 + 4 * selected;
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(&sbix[0..8]);
    out[4..8].copy_from_slice(&(selected as u32).to_be_bytes());
    let mut cursor = header_len;
    for index in 0..selected {
        out.extend_from_slice(&(cursor as u32).to_be_bytes());
        cursor += offsets[index + 1] - offsets[index];
    }
    for index in 0..selected {
        out.extend_from_slice(&sbix[offsets[index]..offsets[index + 1]]);
    }
    Ok(out)
}

fn validate_face(bytes: &[u8]) -> HostResult<()> {
    let font = FontRef::from_index(bytes, 0)
        .map_err(|_| HostError::new(ErrorCode::InvalidFont, "font face does not parse"))?;
    font.head()
        .map_err(|_| HostError::new(ErrorCode::InvalidFont, "font has no head table"))?;
    Ok(())
}

/// Parses a length-framed request header, rejecting oversized frames.
pub fn parse_frame_header(header: &[u8]) -> HostResult<serde_json::Value> {
    if header.len() > MAX_FRAME_HEADER_BYTES as usize {
        return Err(HostError::new(
            ErrorCode::BadRequest,
            "frame header is too large",
        ));
    }
    serde_json::from_slice(header)
        .map_err(|_| HostError::new(ErrorCode::BadRequest, "frame header is not JSON"))
}

/// Serializes a response header and optional body into the framed wire format.
pub fn encode_response(header: &impl Serialize, body: &[u8]) -> Vec<u8> {
    let header = serde_json::to_vec(header).unwrap_or_else(|_| b"{\"ok\":false}".to_vec());
    let mut out = Vec::with_capacity(8 + header.len() + body.len());
    out.extend_from_slice(&(header.len() as u32).to_be_bytes());
    out.extend_from_slice(&header);
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOTO_SANS: &[u8] = include_bytes!("../../../engine/public/fonts/NotoSans.ttf");

    fn wrap_ttc(faces: &[&[u8]]) -> Vec<u8> {
        let header_len = 12 + faces.len() * 4;
        let mut offsets = Vec::with_capacity(faces.len());
        let mut cursor = header_len;
        let mut out = Vec::new();
        out.extend_from_slice(b"ttcf");
        out.extend_from_slice(&[0, 1, 0, 0]);
        out.extend_from_slice(&(faces.len() as u32).to_be_bytes());
        for face in faces {
            offsets.push(cursor);
            out.extend_from_slice(&(cursor as u32).to_be_bytes());
            cursor += face.len();
        }
        for face in faces {
            out.extend_from_slice(face);
        }
        for offset in offsets {
            let count = u16::from_be_bytes([out[offset + 4], out[offset + 5]]) as usize;
            for index in 0..count {
                let record = offset + 12 + index * 16;
                let old = u32::from_be_bytes(out[record + 8..record + 12].try_into().unwrap());
                out[record + 8..record + 12].copy_from_slice(&(old + offset as u32).to_be_bytes());
            }
        }
        out
    }

    #[test]
    fn parses_and_rejects_collection_indices() {
        let ttc = wrap_ttc(&[NOTO_SANS, NOTO_SANS]);
        let extracted = extract_face(&ttc, 1, false, &mut 0).unwrap();
        assert_eq!(&extracted[0..4], &[0, 1, 0, 0]);
        assert!(validate_face(&extracted).is_ok());
        assert_eq!(
            extract_face(&ttc, 2, false, &mut 0).unwrap_err().code,
            ErrorCode::BadRequest
        );
    }

    #[test]
    fn rejects_bad_table_offsets() {
        let mut face = NOTO_SANS.to_vec();
        face[12 + 8..12 + 12].copy_from_slice(&0xFFFF_FF00u32.to_be_bytes());
        assert_eq!(
            extract_face(&face, 0, false, &mut 0).unwrap_err().code,
            ErrorCode::InvalidFont
        );
    }

    #[test]
    fn enforces_face_budget() {
        let records = vec![TableRecord {
            tag: *b"cmap",
            checksum: 0,
            offset: 0,
            len: MAX_FACE_BYTES,
        }];
        assert!(output_size(&records, None).unwrap() > MAX_FACE_BYTES);

        let mut face = NOTO_SANS.to_vec();
        face.resize(MAX_FACE_BYTES + 1, 0);
        assert_eq!(
            extract_face(&face, 0, false, &mut 0).unwrap_err().code,
            ErrorCode::FaceTooLarge
        );
    }

    #[test]
    fn unknown_and_expired_ids_are_rejected() {
        let mut host = FontHost::new();
        assert_eq!(
            host.face_bytes("00000000000000000000000000000000")
                .unwrap_err()
                .code,
            ErrorCode::UnknownId
        );
        host.insert_test_face("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", vec![1, 2, 3]);
        assert!(host.face_bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").is_ok());
        host.faces
            .get_mut("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .unwrap()
            .created = Instant::now() - ID_TTL - Duration::from_secs(1);
        assert_eq!(
            host.face_bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                .unwrap_err()
                .code,
            ErrorCode::UnknownId
        );
    }

    #[test]
    fn rejects_invalid_ids_and_oversized_text() {
        let mut host = FontHost::new();
        assert_eq!(
            host.face_bytes("../etc/passwd").unwrap_err().code,
            ErrorCode::BadRequest
        );
        let request = MatchRequest {
            family: Some("sans".to_string()),
            text: Some("x".repeat(MAX_TEXT_BYTES + 1)),
            ..Default::default()
        };
        assert_eq!(
            host.match_faces(&request).unwrap_err().code,
            ErrorCode::BadRequest
        );
    }

    #[test]
    fn rejects_symlinked_sources() {
        let dir = std::env::temp_dir().join(format!("ks-font-host-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let target = dir.join("target.ttf");
        let link = dir.join("link.ttf");
        std::fs::write(&target, NOTO_SANS).unwrap();
        let _ = std::fs::remove_file(&link);
        #[cfg(unix)]
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert_eq!(
            read_source(&link).unwrap_err().code,
            ErrorCode::UnsafeSource
        );
        let _ = std::fs::remove_file(&target);
        let _ = std::fs::remove_file(&link);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn detects_replaced_sources() {
        let dir = std::env::temp_dir().join(format!("ks-font-host-change-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        EXTRA_FONT_ROOT.with(|root| {
            *root.borrow_mut() = std::fs::canonicalize(&dir).ok();
        });
        let path = dir.join("face.ttf");
        std::fs::write(&path, NOTO_SANS).unwrap();
        let (_, hash) = read_source(&path).unwrap();
        let mut replaced_bytes = NOTO_SANS.to_vec();
        replaced_bytes[0] = 0x4f;
        std::fs::write(&path, replaced_bytes).unwrap();
        let (_, replaced) = read_source(&path).unwrap();
        assert_ne!(hash, replaced);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
        EXTRA_FONT_ROOT.with(|root| *root.borrow_mut() = None);
    }

    #[test]
    fn last_resort_is_not_a_success() {
        assert!(is_last_resort("LastResort"));
        assert!(is_last_resort(".LastResort"));
        assert!(!is_last_resort("Helvetica"));
    }

    #[test]
    fn font_ids_retry_interrupts_and_require_entropy() {
        use std::io::ErrorKind;

        let mut calls = 0;
        let id = new_id(|bytes| {
            calls += 1;
            if calls == 1 {
                return Err(ErrorKind::Interrupted.into());
            }
            system_entropy(bytes)
        })
        .unwrap();
        assert_eq!(calls, 2);
        assert!(validate_id(&id).is_ok());
        assert_ne!(id, new_id(system_entropy).unwrap());
        assert_eq!(
            new_id(|_| Err(ErrorKind::Unsupported.into()))
                .unwrap_err()
                .code,
            ErrorCode::EntropyUnavailable
        );
    }

    #[test]
    fn reports_request_coverage_for_only_the_best_face() {
        const NOTO_SANS_ARABIC: &[u8] =
            include_bytes!("../../../engine/public/fonts/NotoSansArabic.ttf");
        let dir = std::env::temp_dir().join(format!(
            "ks-font-host-match-{}",
            new_id(system_entropy).unwrap()
        ));
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(dir.join("sans.ttf"), NOTO_SANS).unwrap();
        std::fs::write(dir.join("arabic.ttf"), NOTO_SANS_ARABIC).unwrap();
        EXTRA_FONT_ROOT.with(|root| *root.borrow_mut() = std::fs::canonicalize(&dir).ok());

        let mut host = FontHost::new();
        host.collection = Collection::new(CollectionOptions {
            shared: false,
            system_fonts: false,
        });
        host.collection.load_fonts_from_paths([&dir]);
        let sans = host.collection.family_id("Noto Sans").unwrap();
        let arabic = host.collection.family_id("Noto Sans Arabic").unwrap();
        host.collection
            .set_generic_families(GenericFamily::SansSerif, [sans, arabic].into_iter());
        host.collection
            .set_generic_families(GenericFamily::Emoji, std::iter::once(sans));
        assert_eq!(count_missing(NOTO_SANS, &[EMOJI_PROBE_SAMPLE]), 1);
        assert!(
            !host.probe().emoji,
            "a generic fallback can miss the emoji sample"
        );
        assert_eq!(host.stats().faces, 0);

        let mut request = MatchRequest {
            family: Some("sans-serif".to_string()),
            text: Some("Aاب".to_string()),
            ..Default::default()
        };
        let partial = host.match_faces(&request).unwrap();
        assert_eq!(partial.faces.len(), 1);
        let face = &partial.faces[0];
        assert_eq!(
            (face.covered, face.missing),
            (partial.covered, partial.missing)
        );
        assert_eq!((partial.covered, partial.missing), (false, 1));
        assert_eq!(partial.reason, Some("missing_coverage"));
        assert_eq!(face.family, "Noto Sans Arabic");
        assert_eq!(host.stats().faces, 1);
        let bytes = host.face_bytes(&face.id).unwrap();
        assert_eq!(bytes.len(), face.bytes);
        assert_eq!(count_missing(&bytes, &['A', 'ا', 'ب']), 1);

        request.text = Some("اب".to_string());
        let complete = host.match_faces(&request).unwrap();
        assert_eq!((complete.covered, complete.missing), (true, 0));
        assert_eq!(complete.reason, None);
        assert_eq!(complete.faces[0].id, face.id);
        assert_eq!(
            (complete.faces[0].covered, complete.faces[0].missing),
            (true, 0)
        );
        assert_eq!(host.stats().faces, 1);

        request.text = Some("Aا".to_string());
        let tied = host.match_faces(&request).unwrap();
        assert_eq!((tied.covered, tied.missing), (false, 1));
        assert_eq!(tied.faces[0].family, "Noto Sans");

        std::fs::write(dir.join("arabic.ttf"), NOTO_SANS).unwrap();
        host.collection
            .set_generic_families(GenericFamily::SansSerif, std::iter::once(arabic));
        request.text = Some("اب".to_string());
        let replaced = host.match_faces(&request).unwrap();
        let replacement = &replaced.faces[0];
        assert_eq!((replacement.covered, replacement.missing), (false, 2));
        assert_ne!(replacement.id, face.id);
        assert_eq!(
            host.face_bytes(&replacement.id).unwrap().as_slice(),
            NOTO_SANS
        );

        EXTRA_FONT_ROOT.with(|root| *root.borrow_mut() = None);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn real_cjk_and_emoji_faces_are_bounded() {
        let mut host = FontHost::new();
        let probe = host.probe();
        let require_emoji = std::env::var("KITESURF_REQUIRE_EMOJI").as_deref() == Ok("1");
        assert!(
            probe.emoji || !require_emoji,
            "expected a system emoji face"
        );
        if probe.emoji {
            let request = MatchRequest {
                family: Some("emoji".to_string()),
                text: Some(EMOJI_PROBE_SAMPLE.to_string()),
                ..Default::default()
            };
            let response = host.match_faces(&request).unwrap();
            assert!(response.covered, "emoji face must cover the sample");
            let face = response.faces.first().expect("probed system emoji face");
            assert!(face.bytes <= MAX_FACE_BYTES);
            let bytes = host.face_bytes(&face.id).unwrap();
            assert_eq!(bytes.len(), face.bytes);
            assert!(matches!(&bytes[..4], [0, 1, 0, 0] | b"OTTO"));
            assert_eq!(count_missing(&bytes, &[EMOJI_PROBE_SAMPLE]), 0);
            let entry = &host.faces[&face.id];
            let (source, _) = read_source(&entry.key.0).unwrap();
            let (_, source_tables) = parse_tables(&source, entry.face_index).unwrap();
            let (_, face_tables) = parse_tables(&bytes, 0).unwrap();
            let color_tags = [b"sbix", b"CBDT", b"CBLC", b"COLR", b"CPAL", b"SVG "];
            let mut color_tables = source_tables
                .iter()
                .filter(|record| color_tags.contains(&&record.tag))
                .peekable();
            assert!(
                !require_emoji || color_tables.peek().is_some(),
                "expected a color system emoji face"
            );
            for expected in color_tables {
                assert!(face_tables.iter().any(|record| record.tag == expected.tag));
            }
            eprintln!(
                "emoji family={} bytes={} limited={}",
                face.family,
                bytes.len(),
                host.stats().limited_emoji
            );
        }
        let request = MatchRequest {
            family: Some("script".to_string()),
            script: Some("Hani".to_string()),
            locale: Some("zh-CN".to_string()),
            text: Some("\u{4E16}".to_string()),
            ..Default::default()
        };
        if let Ok(response) = host.match_faces(&request) {
            if let Some(face) = response.faces.first() {
                assert!(response.covered, "CJK face must cover the sample");
                let bytes = host.face_bytes(&face.id).unwrap();
                assert_eq!(count_missing(&bytes, &['\u{4E16}']), 0);
            }
        }
    }

    #[test]
    fn probe_reports_real_generic_coverage_when_present() {
        let mut host = FontHost::new();
        let probe = host.probe();
        if !probe.sans && !probe.serif && !probe.mono {
            eprintln!("no system base fonts installed; skipping real-family assertion");
            return;
        }
        let request = MatchRequest {
            family: Some("sans".to_string()),
            text: Some("Ag".to_string()),
            ..Default::default()
        };
        let response = host.match_faces(&request).unwrap();
        assert!(response.covered);
        let face = response.faces.first().expect("system sans face");
        assert!(!is_last_resort(&face.family));
        assert!(face.bytes > 0);
        let bytes = host.face_bytes(&face.id).unwrap();
        assert_eq!(count_missing(&bytes, &['A', 'g']), 0);
    }
}
