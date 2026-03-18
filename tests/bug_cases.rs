use queue_file::QueueFile;

#[test]
fn reopen_bigger_capacity_wrong_file_len() {
    let path = auto_delete_path::AutoDeletePath::temp();

    {
        let qf = QueueFile::with_capacity(&path, 1024 * 5).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), qf.file_len());
    }

    let qf = QueueFile::with_capacity(&path, 1024 * 6).unwrap();
    assert_eq!(std::fs::metadata(&path).unwrap().len(), qf.file_len());
}

#[test]
fn transfer_expand_invalid_file_len() {
    let path = auto_delete_path::AutoDeletePath::temp();
    let mut qf = QueueFile::with_capacity(path, 32 + (4 + 1) * 3).unwrap();

    qf.add_n(&[&[1], &[2], &[3]]).unwrap();
    qf.remove_n(2).unwrap();
    qf.add_n(&[&[1]]).unwrap();

    qf.add_n(&[&[2], &[4]]).unwrap();

    assert_eq!(qf.iter().map(|v| v[0]).collect::<Vec<_>>(), vec![3, 1, 2, 4]);
}

#[test]
fn into_inner_file_flushes_deferred_header_safely() {
    let path = auto_delete_path::AutoDeletePath::temp();
    let mut qf = QueueFile::open(&path).unwrap();
    qf.set_skip_write_header_on_add(true);
    qf.add(b"abc").unwrap();

    let file = qf.into_inner_file().unwrap();
    drop(file);

    let reopened = QueueFile::open(&path).unwrap();
    assert_eq!(reopened.iter().map(Vec::from).collect::<Vec<_>>(), vec![b"abc".to_vec()]);
}
