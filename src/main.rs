use queue_file::QueueFile;

fn main() {
    let mut qf = QueueFile::open("/home/khr/Downloads/usaf-qf/ACS-CENTRAL.pqf")
        .expect("cannot open queue file");
    let mut qff = QueueFile::open("ACS-CENTRAL-FILTERED.pqf")
        .expect("cannot open queue file");
    qff.set_sync_writes(false);

    for (index, elem) in qf.iter().enumerate() {
        if elem.len() > 0 {
            qff.add(&elem).expect("add failed");
        }
        // println!(
        //     "{}: {} bytes",
        //     index,
        //     elem.len()
        // );
    }

    qff.sync_all();
}
