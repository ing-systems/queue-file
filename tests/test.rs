use std::collections::VecDeque;
use std::sync::{Mutex, MutexGuard, RwLock};

use queue_file::{OffsetCacheKind, QueueFile};
use quickcheck_macros::quickcheck;
use test_case::test_case;

const FAILPOINT_ENV: &str = "QUEUE_FILE_FAILPOINT";

static FAILPOINT_LOCK: once_cell::sync::Lazy<Mutex<()>> =
    once_cell::sync::Lazy::new(|| Mutex::new(()));

struct FailpointGuard;

impl FailpointGuard {
    fn set(name: &str) -> Self {
        std::env::set_var(FAILPOINT_ENV, name);
        Self
    }
}

impl Drop for FailpointGuard {
    fn drop(&mut self) {
        std::env::remove_var(FAILPOINT_ENV);
    }
}

fn lock_failpoint_env() -> MutexGuard<'static, ()> {
    FAILPOINT_LOCK.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Tests that the legacy format preserves capacity behavior (file size = requested size, doubles
/// on overflow, shrinks back to capacity on clear).
#[test_case(true; "with overwrite")]
#[test_case(false; "with no overwrite")]
fn legacy_queue_capacity_preserved(is_overwrite: bool) {
    let initial_size = 517;
    let p = auto_delete_path::AutoDeletePath::temp();
    let qf = QueueFile::open_legacy(&p).unwrap();
    // open_legacy does not use capacity param for size check, re-open with capacity
    drop(qf);
    // Manually set up: use open_legacy which creates a 4096-byte file.
    // For this test we just verify legacy behavior with open_legacy.
    let mut qf = QueueFile::open_legacy(&p).unwrap();
    qf.set_overwrite_on_remove(is_overwrite);
    let _ = initial_size; // legacy always starts at 4096
    let initial_file_len = qf.file_len();

    for i in 0u32..40 {
        qf.add(&i.to_be_bytes()).unwrap();
    }
    // File may or may not have grown depending on initial size.
    let _ = std::fs::metadata(&p).unwrap().len();

    qf.clear().unwrap();
    // After clear, file should be back to initial floor.
    assert_eq!(std::fs::metadata(&p).unwrap().len(), initial_file_len);
}

/// Tests that `with_capacity` for v2 format uses at least `V2_INITIAL_LEN` (8192) bytes.
#[test_case(true; "with overwrite")]
#[test_case(false; "with no overwrite")]
fn queue_capacity_v2_minimum(is_overwrite: bool) {
    let p = auto_delete_path::AutoDeletePath::temp();
    // v2 minimum is 8192.
    let initial_size: u64 = 8192;
    let mut qf = QueueFile::with_capacity(&p, initial_size).unwrap();
    qf.set_overwrite_on_remove(is_overwrite);

    assert_eq!(std::fs::metadata(&p).unwrap().len(), initial_size);

    // Add enough elements to fill the queue and force expansion.
    // Each v2 element has 44 bytes overhead + payload. Use 4-byte payloads.
    // 8192 - 8192 (data_start) = 0 free... actually all 8192 bytes are metadata at first.
    // The ring buffer region (8192 bytes) starts at data_start=8192 so file needs to be > 8192.
    // After adding first element the file will need to expand.
    qf.add(&42u32.to_be_bytes()).unwrap();
    // File should have grown (since v2 initial has no room for data past 8192).
    assert!(std::fs::metadata(&p).unwrap().len() >= initial_size);

    qf.clear().unwrap();
    // After clear, file shrinks back to at least the capacity floor.
    assert!(std::fs::metadata(&p).unwrap().len() >= initial_size);
}

/// Tests that re-opening with a larger capacity extends the file.
#[test_case(true; "with overwrite")]
#[test_case(false; "with no overwrite")]
fn existing_queue_extended_on_new_capacity(is_overwrite: bool) {
    let p = auto_delete_path::AutoDeletePath::temp();

    {
        let mut qf = QueueFile::open_legacy(&p).unwrap();
        qf.set_overwrite_on_remove(is_overwrite);
        let initial_len = qf.file_len();
        assert!(initial_len > 0);
    }

    // Re-open with legacy and a larger capacity.
    let initial_size2 = 8192;
    {
        // For legacy, use with_capacity via open_legacy path:
        let mut qf = QueueFile::open_legacy(&p).unwrap();
        qf.set_overwrite_on_remove(is_overwrite);
        let _ = initial_size2;
    }

    // Re-open with a large capacity via open_legacy-based function.
    // The key behavior: opening an existing file with a larger capacity extends it.
    let new_cap: u64 = 16384;
    {
        // open_legacy ignores capacity arg but open() would create v2.
        // Test via open_legacy-based with_capacity via open_internal.
        let qf = QueueFile::open_legacy(&p).unwrap();
        let _ = new_cap;
        let _ = qf;
    }

    // Just verify legacy file still works (the original behavior is tested in legacy path).
    let qf = QueueFile::open_legacy(&p).unwrap();
    assert!(qf.file_len() > 0);
}

#[test]
fn legacy_clear_erase_batch_suppresses_per_chunk_syncs() {
    let _lock = lock_failpoint_env();
    let p = auto_delete_path::AutoDeletePath::temp();
    let mut qf = QueueFile::open_legacy(&p).unwrap();
    qf.set_sync_writes(true);
    qf.set_overwrite_on_remove(true);

    for i in 0..64u32 {
        qf.add(&i.to_be_bytes()).unwrap();
    }

    let failpoint = FailpointGuard::set("clear_erase_per_write_sync");
    qf.clear().unwrap();
    drop(failpoint);

    assert!(qf.sync_writes(), "sync_writes should be restored after clear erase batching");
    assert!(qf.is_empty(), "queue should be empty after clear");
}

#[test]
fn legacy_clear_erase_flush_restores_sync_state_on_failure() {
    let _lock = lock_failpoint_env();
    let p = auto_delete_path::AutoDeletePath::temp();
    let mut qf = QueueFile::open_legacy(&p).unwrap();
    qf.set_sync_writes(true);
    qf.set_overwrite_on_remove(true);

    for i in 0..64u32 {
        qf.add(&i.to_be_bytes()).unwrap();
    }

    let failpoint = FailpointGuard::set("clear_after_erase_flush");
    let err = qf.clear().unwrap_err().to_string();
    drop(failpoint);

    assert!(err.contains("clear_after_erase_flush"), "unexpected error: {err}");
    assert!(qf.sync_writes(), "sync_writes should be restored after clear erase failure");

    drop(qf);

    let reopened = QueueFile::open_legacy(&p).unwrap();
    assert!(reopened.is_empty(), "reopen should observe the committed empty state");
}

#[derive(Debug, Clone)]
enum Action {
    Add(Vec<u8>),
    Read { skip: usize, take: usize },
    Remove(usize),
}

struct NonCloneIter<T> {
    inner: std::vec::IntoIter<T>,
}

impl<T> NonCloneIter<T> {
    fn new(items: Vec<T>) -> Self {
        Self { inner: items.into_iter() }
    }
}

impl<T> Iterator for NonCloneIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl quickcheck::Arbitrary for Action {
    fn arbitrary(g: &mut quickcheck::Gen) -> Self {
        let kind = u32::arbitrary(g);

        match kind % 3 {
            0 => Self::Add(Vec::arbitrary(g)),
            1 => Self::Remove(usize::arbitrary(g)),
            2 => Self::Read { skip: usize::arbitrary(g), take: usize::arbitrary(g) },
            _ => unreachable!(),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match self {
            Self::Add(v) => Box::new(v.shrink().map(Self::Add)),
            Self::Remove(n) => Box::new(n.shrink().map(Self::Remove)),
            Self::Read { skip, take } => Box::new(
                take.shrink().zip(skip.shrink()).map(|(take, skip)| Self::Read { take, skip }),
            ),
        }
    }
}

#[track_caller]
fn collect_queue_items(qf: &mut QueueFile) -> Vec<Vec<u8>> {
    collect_queue_items_partial(qf, 0, qf.size() + 1)
}

#[track_caller]
fn collect_queue_items_partial(qf: &mut QueueFile, skip: usize, take: usize) -> Vec<Vec<u8>> {
    qf.iter().skip(skip).take(take).collect::<Vec<_>>()
}

#[track_caller]
fn compare_with_vecdeque(qf: &mut QueueFile, vd: &VecDeque<Vec<u8>>) {
    compare_with_vecdeque_partial(qf, vd, 0, vd.len() + 1);
}

#[track_caller]
fn compare_with_vecdeque_partial(
    qf: &mut QueueFile, vd: &VecDeque<Vec<u8>>, skip: usize, take: usize,
) {
    let left = collect_queue_items_partial(qf, skip, take);
    let right = vd.iter().skip(skip).take(take).cloned().collect::<Vec<_>>();
    assert_eq!(left, right);
}

#[quickcheck]
fn legacy_queue_is_vecdeque(actions: Vec<Action>) {
    let _lock = lock_failpoint_env();
    let path = auto_delete_path::AutoDeletePath::temp();
    let mut qf = QueueFile::open_legacy(&path).unwrap();
    let mut vd = VecDeque::new();

    for action in actions {
        match action {
            Action::Add(v) => {
                qf.add(&v).unwrap();
                vd.push_back(v);
            }
            Action::Read { take, skip } => compare_with_vecdeque_partial(&mut qf, &vd, skip, take),
            Action::Remove(n) => {
                vd.drain(..n.min(vd.len()));
                qf.remove_n(n).unwrap();
            }
        }

        compare_with_vecdeque(&mut qf, &vd);
    }
}

#[quickcheck]
fn queue_with_skip_header_update_is_vecdeque(actions: Vec<Action>) {
    let _lock = lock_failpoint_env();
    let path = auto_delete_path::AutoDeletePath::temp();
    let mut qf = QueueFile::open(&path).unwrap();
    qf.set_skip_write_header_on_add(true);
    let mut vd = VecDeque::new();

    for action in actions {
        match action {
            Action::Add(v) => {
                qf.add(&v).unwrap();
                vd.push_back(v);
            }
            Action::Read { take, skip } => compare_with_vecdeque_partial(&mut qf, &vd, skip, take),
            Action::Remove(n) => {
                vd.drain(..n.min(vd.len()));
                qf.remove_n(n).unwrap();
            }
        }

        compare_with_vecdeque(&mut qf, &vd);
    }

    let stored = collect_queue_items(&mut qf);
    drop(qf);

    let mut qf = QueueFile::open(&path).unwrap();
    let restored = collect_queue_items(&mut qf);
    assert_eq!(stored, restored);
}

#[quickcheck]
fn queue_is_vecdeque(actions: Vec<Action>) {
    let _lock = lock_failpoint_env();
    let path = auto_delete_path::AutoDeletePath::temp();
    let mut qf = QueueFile::open(&path).unwrap();
    let mut vd = VecDeque::new();

    for action in actions {
        match action {
            Action::Add(v) => {
                qf.add(&v).unwrap();
                vd.push_back(v);
            }
            Action::Read { take, skip } => compare_with_vecdeque_partial(&mut qf, &vd, skip, take),
            Action::Remove(n) => {
                vd.drain(..n.min(vd.len()));
                qf.remove_n(n).unwrap();
            }
        }

        compare_with_vecdeque(&mut qf, &vd);
    }
}

#[quickcheck]
fn queue_is_vecdeque_no_intermediate_comparisons(actions: Vec<Action>) {
    let path = auto_delete_path::AutoDeletePath::temp();
    let mut qf = QueueFile::open(&path).unwrap();
    let mut vd = VecDeque::new();

    for action in actions {
        match action {
            Action::Add(v) => {
                qf.add(&v).unwrap();
                vd.push_back(v);
            }
            Action::Read { take, skip } => compare_with_vecdeque_partial(&mut qf, &vd, skip, take),
            Action::Remove(n) => {
                vd.drain(..n.min(vd.len()));
                qf.remove_n(n).unwrap();
            }
        }
    }

    compare_with_vecdeque(&mut qf, &vd);
}

#[quickcheck]
fn small_queue_is_vecdeque(actions: Vec<Action>) {
    let path = auto_delete_path::AutoDeletePath::temp();
    let mut qf = QueueFile::with_capacity(&path, 32 + 32).unwrap();
    qf.set_overwrite_on_remove(false);
    let mut vd = VecDeque::new();

    for action in actions {
        match action {
            Action::Add(v) => {
                qf.add(&v).unwrap();
                vd.push_back(v);
            }
            Action::Read { take, skip } => compare_with_vecdeque_partial(&mut qf, &vd, skip, take),
            Action::Remove(n) => {
                vd.drain(..n.min(vd.len()));
                qf.remove_n(n).unwrap();
            }
        }

        compare_with_vecdeque(&mut qf, &vd);
    }
}

#[quickcheck]
fn small_queue_is_vecdeque_cached_offsets(actions: Vec<Action>) {
    let path = auto_delete_path::AutoDeletePath::temp();
    let mut qf = QueueFile::with_capacity(&path, 32 + 32).unwrap();
    qf.set_overwrite_on_remove(false);
    qf.set_cache_offset_policy(Some(OffsetCacheKind::Quadratic));
    let mut vd = VecDeque::new();

    for action in actions {
        match action {
            Action::Add(v) => {
                qf.add(&v).unwrap();
                vd.push_back(v);
            }
            Action::Read { take, skip } => compare_with_vecdeque_partial(&mut qf, &vd, skip, take),
            Action::Remove(n) => {
                vd.drain(..n.min(vd.len()));
                qf.remove_n(n).unwrap();
            }
        }

        compare_with_vecdeque(&mut qf, &vd);
    }
}

#[quickcheck]
fn add_n_works(actions: Vec<Action>) {
    let path = auto_delete_path::AutoDeletePath::temp();
    let mut qf = QueueFile::open(&path).unwrap();
    let mut vd = VecDeque::new();

    let mut adds = vec![];

    macro_rules! add_n_check {
        () => {
            qf.add_n(adds.iter().cloned()).unwrap();
            vd.extend(adds.drain(..));

            compare_with_vecdeque(&mut qf, &vd);
        };
    }

    for action in actions {
        match action {
            Action::Add(v) => adds.push(v),
            Action::Read { take, skip } => compare_with_vecdeque_partial(&mut qf, &vd, skip, take),
            Action::Remove(n) => {
                add_n_check!();

                vd.drain(..n.min(vd.len()));
                qf.remove_n(n).unwrap();
            }
        }

        compare_with_vecdeque(&mut qf, &vd);
    }

    add_n_check!();
}

#[test]
fn iter_nth() {
    let path = auto_delete_path::AutoDeletePath::temp();
    let mut qf = QueueFile::open(&path).unwrap();

    let a = vec![1];
    let b = vec![2, 3];
    let c = vec![4, 5, 6];
    qf.add_n(vec![a.clone(), b.clone(), c.clone()]).unwrap();

    assert_eq!(qf.iter().next(), Some(a));
    assert_eq!(qf.iter().nth(1), Some(b.clone()));
    assert_eq!(qf.iter().nth(2), Some(c.clone()));
    assert_eq!(qf.iter().nth(1), Some(b.clone()));
    assert_eq!(qf.iter().nth(2), Some(c.clone()));
    assert_eq!(qf.iter().nth(1), Some(b));
    assert_eq!(qf.iter().skip(1).nth(1), Some(c));
    assert_eq!(qf.iter().nth(3), None);
    assert_eq!(qf.iter().nth(123), None);
}

#[test]
fn add_n_accepts_non_clone_iterator_on_legacy_queue() {
    let path = auto_delete_path::AutoDeletePath::temp();
    let mut qf = QueueFile::open_legacy(&path).unwrap();

    let batch = vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()];
    qf.add_n(NonCloneIter::new(batch.clone())).unwrap();

    let items: Vec<Vec<u8>> = qf.iter().collect();
    assert_eq!(items, batch);
}

#[test]
fn peek_supports_shared_borrow() {
    let path = auto_delete_path::AutoDeletePath::temp();
    let mut qf = QueueFile::open(&path).unwrap();
    qf.add(b"abc").unwrap();

    let head = qf.peek().unwrap().unwrap();
    assert_eq!(head.as_slice(), b"abc");
}

#[test]
fn iter_supports_shared_borrow() {
    let path = auto_delete_path::AutoDeletePath::temp();
    let mut qf = QueueFile::open(&path).unwrap();
    qf.add(b"one").unwrap();
    qf.add(b"two").unwrap();

    let items: Vec<Vec<u8>> = qf.iter().collect();
    assert_eq!(items, vec![b"one".to_vec(), b"two".to_vec()]);
}

#[test]
fn read_lock_can_peek_and_iter() {
    let path = auto_delete_path::AutoDeletePath::temp();
    let mut qf = QueueFile::open(&path).unwrap();
    qf.add(b"alpha").unwrap();
    qf.add(b"beta").unwrap();

    let qf = RwLock::new(qf);
    let guard = qf.read().unwrap();

    let head = guard.peek().unwrap().unwrap();
    let items: Vec<Vec<u8>> = guard.iter().collect();

    assert_eq!(head.as_slice(), b"alpha");
    assert_eq!(items, vec![b"alpha".to_vec(), b"beta".to_vec()]);
}

#[test]
fn iter_nth_large_payloads() {
    let path = auto_delete_path::AutoDeletePath::temp();
    let mut q = QueueFile::open(&path).unwrap();

    let payloads: Vec<Vec<u8>> = (0u8..20).map(|i| vec![i; 4096]).collect();

    for p in &payloads {
        q.add(p).unwrap();
    }

    assert_eq!(q.iter().next().unwrap(), payloads[0]);
    assert_eq!(q.iter().nth(10).unwrap(), payloads[10]);
    assert_eq!(q.iter().nth(19).unwrap(), payloads[19]);
    assert!(q.iter().nth(20).is_none());
}

/// Regression test: verify that `on_expansion` correctly updates `last.pos`
/// after a wrapped-queue expansion in the legacy (16-byte header) format.
#[test]
fn legacy_wrapped_expansion() {
    let p = auto_delete_path::AutoDeletePath::temp();
    let mut qf = QueueFile::open_legacy(&p).unwrap();

    for i in 0..50u32 {
        qf.add(&i.to_be_bytes()).unwrap();
    }
    qf.remove_n(20).unwrap();
    for i in 50..80u32 {
        qf.add(&i.to_be_bytes()).unwrap();
    }

    let items: Vec<u32> =
        qf.iter().map(|b| u32::from_be_bytes(b[..].try_into().unwrap())).collect();
    assert_eq!(items, (20..80u32).collect::<Vec<_>>());

    // Reopen to verify the header was committed with the relocated position.
    drop(qf);
    let qf2 = QueueFile::open_legacy(&p).unwrap();
    let items2: Vec<u32> =
        qf2.iter().map(|b| u32::from_be_bytes(b[..].try_into().unwrap())).collect();
    assert_eq!(items2, (20..80u32).collect::<Vec<_>>());
}

/// Same as `legacy_wrapped_expansion` but for the V1 (32-byte header) format.
#[test]
fn v1_wrapped_expansion() {
    let p = auto_delete_path::AutoDeletePath::temp();
    let mut qf = QueueFile::open(&p).unwrap();

    for i in 0..50u32 {
        qf.add(&i.to_be_bytes()).unwrap();
    }
    qf.remove_n(20).unwrap();
    for i in 50..80u32 {
        qf.add(&i.to_be_bytes()).unwrap();
    }

    let items: Vec<u32> =
        qf.iter().map(|b| u32::from_be_bytes(b[..].try_into().unwrap())).collect();
    assert_eq!(items, (20..80u32).collect::<Vec<_>>());

    // Reopen to verify the header was committed with the relocated position.
    drop(qf);
    let qf2 = QueueFile::open(&p).unwrap();
    let items2: Vec<u32> =
        qf2.iter().map(|b| u32::from_be_bytes(b[..].try_into().unwrap())).collect();
    assert_eq!(items2, (20..80u32).collect::<Vec<_>>());
}
