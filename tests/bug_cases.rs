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
