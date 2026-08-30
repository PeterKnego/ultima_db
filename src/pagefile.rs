// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
//! Append-only, physically preallocated page store (`pages.bin`). See spec §3.
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

use crate::child::PageId;
use crate::{Error, Result};

/// What a page's payload holds. Distinguishes the paged B-tree's four leaf/
/// inner shapes (data tree vs. an index's own tree) so `read` can hand back
/// the right decoder without a second lookup.
// No production caller yet — this is the first, tree-independent task of the
// disk layer (see the module doc); the paged B-tree's checkpoint writer and
// `NodeSource` reader are later tasks in this stage. Exercised today by this
// module's own unit tests.
#[allow(dead_code)]
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum PageKind {
    DataLeaf = 1,
    DataInner = 2,
    IndexLeaf = 3,
    IndexInner = 4,
}

impl TryFrom<u8> for PageKind {
    type Error = Error;
    fn try_from(b: u8) -> Result<Self> {
        Ok(match b {
            1 => Self::DataLeaf,
            2 => Self::DataInner,
            3 => Self::IndexLeaf,
            4 => Self::IndexInner,
            o => return Err(Error::CheckpointCorrupted(format!("unknown page kind {o}"))),
        })
    }
}

/// `kind u8 | fmt u8 | flags u8 | pad u8 | payload_len u32 LE | crc32 u32 LE`.
/// Fixed-width and CRC'd separately from the payload so a torn/garbage tail
/// (recovery re-appending over a crash-truncated `pages.bin`) is detected
/// without reading past the declared length.
// No production caller yet — see the `PageKind` note above.
#[allow(dead_code)]
pub(crate) const PAGE_HEADER_LEN: usize = 12;
/// The only page format this build writes or reads; a mismatch here means a
/// future on-disk layout this binary doesn't understand, not corruption.
// No production caller yet — see the `PageKind` note above.
#[allow(dead_code)]
pub(crate) const PAGE_FMT_V1: u8 = 1;

/// Upper bound on a page's payload, checked in `read` before trusting a
/// decoded `payload_len` enough to allocate for it. Keys are capped at 64
/// KiB by `check_encoded_key_len`; a node beyond 64 MiB is a corruption
/// signal, not a workload — one constant, one place to change.
// No production caller yet — see the `PageKind` note above.
#[allow(dead_code)]
pub(crate) const MAX_PAGE_BYTES: usize = 64 << 20;

/// The mutable append state, behind one `Mutex` so concurrent `append`s
/// serialize on the single write cursor (reads never take this lock).
// No production caller yet — see the `PageKind` note above.
#[allow(dead_code)]
struct WriteHead {
    /// Next byte offset to write at == this page file's logical end.
    cursor: u64,
    /// How far the file has been physically zero-filled (>= `cursor`); grown
    /// a `chunk` at a time so `append` amortizes the `preallocate_to` cost.
    capacity: u64,
}

/// Preallocated, append-only page store backing the paged B-tree's
/// checkpoints. A `PageId` is the byte offset of the page's header; pages
/// are never moved, so an id stays valid for the file's lifetime (until a
/// hole is punched under it, at which point re-reading it is a bug in the
/// caller, not in `PageFile`).
// No production caller yet — see the `PageKind` note above.
#[allow(dead_code)]
pub(crate) struct PageFile {
    file: File,
    w: Mutex<WriteHead>,
    /// Physical zero-fill granularity for grow-ahead (bigger than a single
    /// page so `preallocate_to` — one `sync_all` per call — is amortized
    /// across many appends).
    chunk: u64,
    /// First-read buffer size for `read`: big enough that most pages are
    /// satisfied by a single positioned read, with a second read only for
    /// payloads that overrun it.
    prefetch: usize,
}

/// The fixed on-disk filename inside a store's persistence directory,
/// matching `wal.bin`/`checkpoint-*.bin`'s sibling-file convention.
// No production caller yet — see the `PageKind` note above.
#[allow(dead_code)]
pub(crate) fn page_file_path(dir: &Path) -> PathBuf {
    dir.join("pages.bin")
}

// No production caller yet — see the `PageKind` note above; applies to
// every method below.
#[allow(dead_code)]
impl PageFile {
    /// Open (creating if absent) the page file at `path`. `cursor` is the
    /// logical write head to resume at — 0 for a fresh file, or the last
    /// known-durable `file_end()` when recovering (anything physically past
    /// it is garbage from a torn write and gets overwritten, never read).
    /// `chunk` sizes the grow-ahead preallocation; `prefetch` sizes the
    /// first-read buffer.
    pub(crate) fn open(path: &Path, cursor: u64, chunk: u64, prefetch: usize) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|e| Error::Persistence(format!("open page file {}: {e}", path.display())))?;
        let capacity = file
            .metadata()
            .map_err(|e| Error::Persistence(format!("stat page file {}: {e}", path.display())))?
            .len();
        fadvise_random(&file);
        Ok(Self { file, w: Mutex::new(WriteHead { cursor, capacity }), chunk, prefetch: prefetch.max(PAGE_HEADER_LEN) })
    }

    /// Bytes a page with this payload length occupies on disk, header included.
    pub(crate) fn page_len(payload_len: usize) -> u64 {
        (PAGE_HEADER_LEN + payload_len) as u64
    }

    /// Current logical write cursor == the file's logical end.
    pub(crate) fn file_end(&self) -> u64 {
        self.w.lock().cursor
    }

    /// Recovery hook: reposition the write cursor without touching the
    /// physical file (used when the root record names a durable `file_end`
    /// that precedes whatever garbage a torn write left past it).
    pub(crate) fn set_cursor(&self, at: u64) {
        self.w.lock().cursor = at;
    }

    /// Append one page, growing the physical file ahead of the cursor in
    /// `chunk`-sized zero-filled extensions when needed. Returns the new
    /// page's id (its header's byte offset).
    pub(crate) fn append(&self, kind: PageKind, payload: &[u8]) -> Result<PageId> {
        let len = Self::page_len(payload.len());
        let mut w = self.w.lock();
        let id = w.cursor;
        let need = id + len;
        if need > w.capacity {
            let to = need.div_ceil(self.chunk) * self.chunk;
            let mut f = self
                .file
                .try_clone()
                .map_err(|e| Error::Persistence(format!("clone page file handle for page {id}: {e}")))?;
            // Physical zero-fill + sync_all inside; durable before we write into the region.
            crate::wal::preallocate_to(&mut f, w.capacity, to)?;
            w.capacity = to;
        }
        let mut hdr = [0u8; PAGE_HEADER_LEN];
        hdr[0] = kind as u8;
        hdr[1] = PAGE_FMT_V1;
        hdr[2] = 0;
        hdr[3] = 0;
        hdr[4..8].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        let mut h = crc32fast::Hasher::new();
        h.update(&hdr[..8]);
        h.update(payload);
        hdr[8..12].copy_from_slice(&h.finalize().to_le_bytes());
        self.file
            .write_all_at(&hdr, id)
            .map_err(|e| Error::Persistence(format!("write page {id} header: {e}")))?;
        self.file
            .write_all_at(payload, id + PAGE_HEADER_LEN as u64)
            .map_err(|e| Error::Persistence(format!("write page {id} payload: {e}")))?;
        w.cursor = need;
        Ok(id)
    }

    /// Read the page at `id`. First read is a single positioned read of
    /// `prefetch` bytes (satisfies header + most payloads in one syscall); a
    /// second positioned read fires only if the declared payload length
    /// overruns that buffer. CRC-verified before returning.
    pub(crate) fn read(&self, id: PageId) -> Result<(PageKind, Vec<u8>)> {
        let mut buf = vec![0u8; self.prefetch];
        let got = read_fully_at(&self.file, &mut buf, id, id)?;
        if got < PAGE_HEADER_LEN {
            return Err(Error::CheckpointCorrupted(format!("page {id}: short header ({got} bytes)")));
        }
        let kind = PageKind::try_from(buf[0]).map_err(|_| {
            Error::CheckpointCorrupted(format!("page {id}: unknown page kind {}", buf[0]))
        })?;
        if buf[1] != PAGE_FMT_V1 {
            return Err(Error::CheckpointCorrupted(format!("page {id}: unsupported page format {}", buf[1])));
        }
        let plen = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        // Bound-check the decoded length against a hard ceiling and the
        // file's known physical extent *before* it drives an allocation —
        // a bit-flipped `payload_len` can decode to ~4 GiB, and an
        // unchecked `Vec::resize` to that size hits the global alloc-error
        // handler and aborts the process instead of returning an `Err`.
        if plen > MAX_PAGE_BYTES {
            return Err(Error::CheckpointCorrupted(format!("page {id}: payload_len {plen} exceeds limits")));
        }
        let total = PAGE_HEADER_LEN + plen;
        let capacity = self.w.lock().capacity;
        if id + total as u64 > capacity {
            return Err(Error::CheckpointCorrupted(format!("page {id}: payload_len {plen} exceeds limits")));
        }
        if total > buf.len() {
            buf.resize(total, 0);
            let more = read_fully_at(&self.file, &mut buf[got..total], id + got as u64, id)?;
            if got + more < total {
                return Err(Error::CheckpointCorrupted(format!(
                    "page {id}: short payload (wanted {plen}, got {})",
                    got + more - PAGE_HEADER_LEN
                )));
            }
        } else if got < total {
            // Symmetric with the second-read path above: a page torn right
            // after its header (declared payload never landed, but the
            // first read's buffer was already big enough to have held it)
            // must report "short payload", not fall through to a
            // misleading "crc mismatch" against zero-filled bytes.
            return Err(Error::CheckpointCorrupted(format!(
                "page {id}: short payload (wanted {plen}, got {})",
                got - PAGE_HEADER_LEN
            )));
        }
        let payload = buf[PAGE_HEADER_LEN..total].to_vec();
        let mut h = crc32fast::Hasher::new();
        h.update(&buf[..8]);
        h.update(&payload);
        if h.finalize() != crc {
            return Err(Error::CheckpointCorrupted(format!("page {id}: crc mismatch")));
        }
        Ok((kind, payload))
    }

    /// Flush written page bytes to durable storage (`sync_data`, not
    /// `sync_all` — the file's length/allocation metadata was already made
    /// durable by `preallocate_to`'s `sync_all` when the region was grown).
    pub(crate) fn sync(&self) -> Result<()> {
        self.file.sync_data().map_err(|e| Error::Persistence(format!("sync page file: {e}")))
    }

    /// Punch holes over `[off, off+len)` ranges (`fallocate` with
    /// `PUNCH_HOLE|KEEP_SIZE`): reclaims physical blocks for pages that are
    /// no longer live without moving any surviving page's offset, so ids
    /// elsewhere in the file stay valid.
    pub(crate) fn punch(&self, ranges: &[(u64, u64)]) -> Result<()> {
        use std::os::fd::AsRawFd;
        for &(off, len) in ranges {
            // SAFETY: plain syscall on our own open fd, no pointers involved.
            let r = unsafe {
                libc::fallocate(
                    self.file.as_raw_fd(),
                    libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
                    off as libc::off_t,
                    len as libc::off_t,
                )
            };
            if r != 0 {
                return Err(Error::Persistence(format!(
                    "punch hole [{off}, {}): {}",
                    off + len,
                    std::io::Error::last_os_error()
                )));
            }
        }
        Ok(())
    }
}

/// Read up to `buf.len()` bytes at `off`, retrying short reads (a positioned
/// read can return fewer bytes than requested even short of EOF) and
/// `Interrupted` errors. `page_id` is only for the error message. Returns
/// the number of bytes actually read (< `buf.len()` at EOF).
// No production caller yet — see the `PageKind` note above.
#[allow(dead_code)]
fn read_fully_at(f: &File, buf: &mut [u8], mut off: u64, page_id: PageId) -> Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match f.read_at(&mut buf[n..], off) {
            Ok(0) => break,
            Ok(k) => {
                n += k;
                off += k as u64;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(Error::Persistence(format!("read page {page_id}: {e}"))),
        }
    }
    Ok(n)
}

/// Advise the OS this file is read randomly (pages are fetched by id, not
/// scanned sequentially), disabling readahead that would otherwise waste
/// I/O on a workload with no locality.
// No production caller yet — see the `PageKind` note above.
#[allow(dead_code)]
fn fadvise_random(f: &File) {
    use std::os::fd::AsRawFd;
    // SAFETY: plain syscall on our own open fd; a failed hint is harmless.
    unsafe {
        libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_RANDOM);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> (tempfile::TempDir, PageFile) {
        let d = tempfile::tempdir().unwrap();
        let pf = PageFile::open(&page_file_path(d.path()), 0, 1 << 20, 4096).unwrap();
        (d, pf)
    }

    #[test]
    fn append_read_roundtrip_all_kinds() {
        let (_d, pf) = tmp();
        for (i, kind) in
            [PageKind::DataLeaf, PageKind::DataInner, PageKind::IndexLeaf, PageKind::IndexInner].into_iter().enumerate()
        {
            let payload = vec![i as u8; 100 + i];
            let id = pf.append(kind, &payload).unwrap();
            assert_eq!(pf.read(id).unwrap(), (kind, payload));
        }
    }

    #[test]
    fn ids_are_byte_offsets_and_file_end_advances() {
        let (_d, pf) = tmp();
        let a = pf.append(PageKind::DataLeaf, &[1; 10]).unwrap();
        let b = pf.append(PageKind::DataLeaf, &[2; 10]).unwrap();
        assert_eq!(a, 0);
        assert_eq!(b, PageFile::page_len(10));
        assert_eq!(pf.file_end(), 2 * PageFile::page_len(10));
    }

    #[test]
    fn grow_ahead_zero_fills_in_chunks() {
        let d = tempfile::tempdir().unwrap();
        let pf = PageFile::open(&page_file_path(d.path()), 0, 8192, 4096).unwrap();
        pf.append(PageKind::DataLeaf, &[0; 100]).unwrap();
        assert_eq!(
            std::fs::metadata(page_file_path(d.path())).unwrap().len(),
            8192,
            "physically zero-filled to one chunk"
        );
        // 8100, not the brief's 8000: remaining headroom after the first append
        // is exactly 8192 - 112 = 8080 bytes, so an 8000-byte payload (8012
        // bytes on disk with its header) still fits in the first chunk and
        // never exercises the second grow-ahead call this test checks for.
        // 8100 (8112 on disk) exceeds that headroom, forcing the second chunk.
        pf.append(PageKind::DataLeaf, &vec![0; 8100]).unwrap();
        assert_eq!(std::fs::metadata(page_file_path(d.path())).unwrap().len(), 16384);
    }

    #[test]
    fn prefetch_boundaries() {
        let (_d, pf) = tmp();
        for n in [4096 - PAGE_HEADER_LEN - 1, 4096 - PAGE_HEADER_LEN, 4096 - PAGE_HEADER_LEN + 1, 70_000] {
            let p: Vec<u8> = (0..n).map(|i| i as u8).collect();
            let id = pf.append(PageKind::DataLeaf, &p).unwrap();
            assert_eq!(pf.read(id).unwrap().1, p, "payload_len {n}");
        }
    }

    #[test]
    fn flipped_byte_fails_crc() {
        let (d, pf) = tmp();
        let id = pf.append(PageKind::DataLeaf, &[9; 50]).unwrap();
        pf.sync().unwrap();
        {
            use std::os::unix::fs::FileExt;
            let f = std::fs::OpenOptions::new().write(true).open(page_file_path(d.path())).unwrap();
            f.write_at(&[8], id + PAGE_HEADER_LEN as u64 + 3).unwrap();
        }
        let e = pf.read(id).unwrap_err();
        assert!(matches!(e, crate::Error::CheckpointCorrupted(_)), "{e:?}");
    }

    #[test]
    fn corrupted_payload_len_rejected_not_aborted() {
        // A bit-flipped payload_len (bytes 4-7 of the header) can decode to
        // ~4 GiB. Before the MAX_PAGE_BYTES guard, this drove an unchecked
        // `Vec::resize` straight into the global alloc-error handler, which
        // aborts the whole process — not a `Result` a caller can catch.
        let (d, pf) = tmp();
        let id = pf.append(PageKind::DataLeaf, &[1; 20]).unwrap();
        {
            use std::os::unix::fs::FileExt;
            let f = std::fs::OpenOptions::new().write(true).open(page_file_path(d.path())).unwrap();
            f.write_at(&0xFFFF_FFFFu32.to_le_bytes(), id + 4).unwrap();
        }
        let e = pf.read(id).unwrap_err();
        assert!(matches!(e, crate::Error::CheckpointCorrupted(_)), "{e:?}");
    }

    #[test]
    fn payload_len_past_file_extent_rejected() {
        // plen well under MAX_PAGE_BYTES but large enough that id + total
        // lands past the file's known physical extent (`capacity`) — a page
        // cannot legitimately end past what was ever zero-filled.
        let (d, pf) = tmp(); // chunk = 1 << 20, so capacity is exactly 1 MiB after this append
        let id = pf.append(PageKind::DataLeaf, &[1; 20]).unwrap();
        {
            use std::os::unix::fs::FileExt;
            let f = std::fs::OpenOptions::new().write(true).open(page_file_path(d.path())).unwrap();
            let bogus_len = 1u32 << 20; // == capacity; id + PAGE_HEADER_LEN + bogus_len overshoots it
            f.write_at(&bogus_len.to_le_bytes(), id + 4).unwrap();
        }
        let e = pf.read(id).unwrap_err();
        assert!(matches!(e, crate::Error::CheckpointCorrupted(_)), "{e:?}");
    }

    #[test]
    fn torn_page_right_after_header_reports_short_payload() {
        let (d, pf) = tmp();
        let id = pf.append(PageKind::DataLeaf, &[1; 50]).unwrap();
        pf.sync().unwrap();
        {
            let f = std::fs::OpenOptions::new().write(true).open(page_file_path(d.path())).unwrap();
            f.set_len(id + PAGE_HEADER_LEN as u64).unwrap(); // torn right after the header
        }
        let e = pf.read(id).unwrap_err();
        assert!(format!("{e}").contains("short payload"), "{e}");
    }

    #[test]
    fn unknown_kind_or_fmt_rejected() {
        let (d, pf) = tmp();
        let id = pf.append(PageKind::DataLeaf, &[1; 8]).unwrap();
        {
            use std::os::unix::fs::FileExt;
            let f = std::fs::OpenOptions::new().write(true).open(page_file_path(d.path())).unwrap();
            f.write_at(&[99], id).unwrap();
        }
        assert!(pf.read(id).is_err());
    }

    #[test]
    fn punch_frees_blocks_but_keeps_offsets() {
        let (d, pf) = tmp();
        let big = vec![7u8; 1 << 20];
        let a = pf.append(PageKind::DataLeaf, &big).unwrap();
        let b = pf.append(PageKind::DataLeaf, &big).unwrap();
        pf.sync().unwrap();
        let blocks_before = {
            use std::os::unix::fs::MetadataExt;
            std::fs::metadata(page_file_path(d.path())).unwrap().blocks()
        };
        pf.punch(&[(a, PageFile::page_len(big.len()))]).unwrap();
        let blocks_after = {
            use std::os::unix::fs::MetadataExt;
            std::fs::metadata(page_file_path(d.path())).unwrap().blocks()
        };
        assert!(blocks_after < blocks_before, "hole punched ({blocks_before} -> {blocks_after})");
        assert_eq!(pf.read(b).unwrap().1, big, "neighbour intact at the same offset");
    }

    #[test]
    fn reopen_at_cursor_ignores_garbage_past_it() {
        let d = tempfile::tempdir().unwrap();
        let path = page_file_path(d.path());
        let end = {
            let pf = PageFile::open(&path, 0, 8192, 4096).unwrap();
            pf.append(PageKind::DataLeaf, &[1; 10]).unwrap();
            pf.sync().unwrap();
            pf.file_end()
        };
        {
            use std::os::unix::fs::FileExt;
            let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            f.write_at(&[0xAB; 64], end).unwrap(); // torn page
        }
        let pf = PageFile::open(&path, end, 8192, 4096).unwrap();
        let id = pf.append(PageKind::DataLeaf, &[2; 10]).unwrap();
        assert_eq!(id, end, "next append overwrites the garbage");
        assert_eq!(pf.read(id).unwrap().1, vec![2; 10]);
    }
}
