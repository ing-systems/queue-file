use std::collections::VecDeque;

use quickcheck_macros::quickcheck;
use test_case::test_case;

use queue_file::QueueFile;

#[test_case(true; "with overwrite")]
#[test_case(false; "with no overwrite")]
fn queue_capacity_preserved(is_overwrite: bool) {
    let initial_size = 517;
    let p = auto_delete_path::AutoDeletePath::temp();
    let mut qf = QueueFile::with_capacity(&p, initial_size).unwrap();
    qf.set_overwrite_on_remove(is_overwrite);

    assert_eq!(std::fs::metadata(&p).unwrap().len(), initial_size);

    for i in 0u32..40 {
        qf.add(&i.to_be_bytes()).unwrap();
    }
    assert_eq!(std::fs::metadata(&p).unwrap().len(), initial_size);

    for i in 0u32..40 {
        qf.add(&i.to_be_bytes()).unwrap();
    }
    assert_eq!(std::fs::metadata(&p).unwrap().len(), initial_size * 2);

    qf.clear().unwrap();
    assert_eq!(std::fs::metadata(&p).unwrap().len(), initial_size);
}

#[test_case(true; "with overwrite")]
#[test_case(false; "with no overwrite")]
fn existing_queue_extended_on_new_capacity(is_overwrite: bool) {
    let initial_size = 200;
    let p = auto_delete_path::AutoDeletePath::temp();

    {
        let mut qf = QueueFile::with_capacity(&p, initial_size).unwrap();
        qf.set_overwrite_on_remove(is_overwrite);

        assert_eq!(std::fs::metadata(&p).unwrap().len(), initial_size);
    }

    let initial_size2 = 350;
    let _qf = QueueFile::with_capacity(&p, initial_size2).unwrap();
    assert_eq!(std::fs::metadata(&p).unwrap().len(), initial_size2);
}

#[derive(Debug, Clone)]
enum Action {
    Add(Vec<u8>),
    Read { skip: usize, take: usize },
    Remove(usize),
}

impl quickcheck::Arbitrary for Action {
    fn arbitrary(mut g: &mut quickcheck::Gen) -> Self {
        let kind = u32::arbitrary(&mut g);

        match kind % 3 {
            0 => Self::Add(Vec::arbitrary(g)),
            1 => Self::Remove(usize::arbitrary(g)),
            2 => Self::Read { skip: usize::arbitrary(&mut g), take: usize::arbitrary(&mut g) },
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
    qf.iter().skip(skip).take(take).map(Vec::from).collect::<Vec<_>>()
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
