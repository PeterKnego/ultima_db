// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use std::path::PathBuf;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use crate::notifier::Signal;
use crate::{Durability, JournalError};

use super::segment::SegmentFile;

pub(crate) struct AppendRequest {
    pub seq: u64,
    pub meta: u64,
    pub payload: Vec<u8>,
    pub signal: Signal,
}

pub(crate) struct WriterState {
    pub dir: PathBuf,
    pub segment_size: u64,
    pub durability: Durability,
    pub segments: Vec<SegmentFile>,
    pub last_seq: Option<u64>,
    pub first_seq: Option<u64>,
}

pub(crate) struct Writer {
    pub tx: Sender<AppendRequest>,
    pub handle: Option<JoinHandle<()>>,
}

impl Writer {
    pub fn spawn(state: Arc<Mutex<WriterState>>) -> Self {
        let (tx, rx) = channel::<AppendRequest>();
        let handle = thread::spawn(move || writer_loop(rx, state));
        Self { tx, handle: Some(handle) }
    }
}

fn writer_loop(rx: Receiver<AppendRequest>, state: Arc<Mutex<WriterState>>) {
    while let Ok(first) = rx.recv() {
        let mut batch = vec![first];
        // Drain anything already queued — this is the group-commit window.
        while let Ok(req) = rx.try_recv() {
            batch.push(req);
        }
        let result = write_batch(&state, &batch);
        let result_arc: Arc<Result<(), JournalError>> = Arc::new(
            result.as_ref().map(|_| ()).map_err(clone_err),
        );
        for req in batch {
            req.signal.complete_arc(Arc::clone(&result_arc));
        }
    }
}

fn write_batch(
    state: &Arc<Mutex<WriterState>>,
    batch: &[AppendRequest],
) -> Result<(), JournalError> {
    let mut st = state.lock().unwrap();
    for req in batch {
        if let Some(last) = st.last_seq
            && req.seq <= last
        {
            return Err(JournalError::NonMonotonicSeq {
                expected_gt: last,
                got: req.seq,
            });
        }
        let body_len = 16 + req.payload.len();
        let total = (4 + body_len + 4) as u64;
        if total > st.segment_size {
            return Err(JournalError::PayloadTooLargeForSegment {
                segment_size: st.segment_size,
                record_size: total,
            });
        }
        let need_new = match st.segments.last() {
            None => true,
            Some(seg) => seg.size()? >= st.segment_size,
        };
        if need_new {
            let path = st.dir.join(format!("seg-{:020}.log", req.seq));
            st.segments.push(SegmentFile::create(&path, req.seq)?);
        }
        let seg = st.segments.last_mut().unwrap();
        seg.append_record(req.seq, req.meta, &req.payload)?;
        if st.first_seq.is_none() {
            st.first_seq = Some(req.seq);
        }
        st.last_seq = Some(req.seq);
    }
    if matches!(st.durability, Durability::Consistent)
        && let Some(seg) = st.segments.last_mut()
    {
        seg.fsync()?;
    }
    Ok(())
}

pub(crate) fn clone_err(e: &JournalError) -> JournalError {
    use JournalError::*;
    match e {
        Io(io) => Io(std::io::Error::new(io.kind(), io.to_string())),
        Corrupted { segment, offset, reason } => Corrupted {
            segment: segment.clone(),
            offset: *offset,
            reason: reason.clone(),
        },
        NonMonotonicSeq { expected_gt, got } => {
            NonMonotonicSeq { expected_gt: *expected_gt, got: *got }
        }
        SeqOutOfRange => SeqOutOfRange,
        PayloadTooLargeForSegment { segment_size, record_size } => {
            PayloadTooLargeForSegment { segment_size: *segment_size, record_size: *record_size }
        }
        Closed => Closed,
    }
}
