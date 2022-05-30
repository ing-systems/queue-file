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
fn bigger_write_buffer_overwrites_read_buffer() {
    let path = auto_delete_path::AutoDeletePath::temp();
    let mut qf = QueueFile::with_capacity(path, 32 + 4 * 2 + 2 + 4).unwrap();
    qf.set_overwrite_on_remove(false);
    qf.set_read_buffer_size(7);

    qf.add_n(&[&[1, 2, 3], &[4, 5, 6]]).unwrap();
    qf.remove().unwrap();
    qf.remove().unwrap();

    qf.add_n(&[&[7, 8, 9, 10][..], &[0, 0]]).unwrap();
    qf.remove().unwrap();

    qf.add(&[99]).unwrap();
    qf.remove().unwrap();
}
