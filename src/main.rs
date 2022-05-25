// use std::sync::mpsc::{channel, sync_channel};
// use std::thread;
use std::time::{Duration, Instant};

// use rand::prelude::*;

use queue_file::QueueFile as Qf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let p = "/tmp/.db";
    // let _ = std::fs::remove_file(p);

    let mut q = Qf::open(&p)?;
    // q.set_read_buffer_size(4096 * 4);
    q.set_overwrite_on_remove(false);
    q.set_skip_write_header_on_add(true);

    println!("file size = {}, left = {}", std::fs::metadata(p)?.len(), q.size());

    // for i in 0u32..10_000_000 {
    //     q.add(&i.to_be_bytes()).unwrap();
    // }

    // println!("~~ end adding ~~");

    // let start = Instant::now();

    // let val = q.iter().skip(5_000_000).next().unwrap();

    // println!("iter took {:?} (val: {:?})", start.elapsed(), val);

    // let start = Instant::now();

    // let val = q.iter().skip(500_000).next().unwrap();

    // println!("iter took {:?} (val: {:?})", start.elapsed(), val);

    // q.remove_n(50);

    // let start = Instant::now();

    // let val = q.iter().skip(500_000 - 50).next().unwrap();

    // println!("iter took {:?} (val: {:?})", start.elapsed(), val);

    // ~~~

    // {
    //     let start = Instant::now();

    //     let mut iter = q.borrowed_iter();
    //     let val = {
    //         for _ in 0..500_000 {
    //             iter.next();
    //         }
    //         iter.next().unwrap()
    //     };

    //     println!("borrowed_iter took {:?} (val: {:?})", start.elapsed(), val);
    // }

    // {
    //     let start = Instant::now();

    //     let mut iter = q.borrowed_iter();
    //     let val = {
    //         for _ in 0..500_000 {
    //             iter.next();
    //         }
    //         iter.next().unwrap()
    //     };

    //     println!("borrowed_iter took {:?} (val: {:?})", start.elapsed(), val);
    // }

    // {
    //     let start = Instant::now();

    //     let mut iter = q.borrowed_iter();
    //     let val = {
    //         for _ in 0..500_000 - 50 {
    //             iter.next();
    //         }
    //         iter.next().unwrap()
    //     };

    //     println!("borrowed_iter took {:?} (val: {:?})", start.elapsed(), val);
    // }

    // let start = Instant::now();

    // let val = q.iter().skip(5_000_000 - 50).take(100).count();

    // println!("iter took {:?} (val: {:?})", start.elapsed(), val);

    // println!("len after insert 1k: {}", std::fs::metadata(p)?.len());

    // for i in 0..30 {
    //     q.remove_n(1000).unwrap();

    //     println!("[{i}] file size = {}, left = {}", std::fs::metadata(p)?.len(), q.size());

    // for i in 1000u32..1500 {
    //     q.add(&i.to_be_bytes()).unwrap();
    // }
    // println!("len after adding 0.5k {}", std::fs::metadata(p)?.len());
    // }

    Ok(())

    // let buf = (0..512).map(|_| rand::random()).collect::<Vec<u8>>();

    // let (tx_a, rx_a) = sync_channel(4096 * 16);
    // let (tx_a_ack, rx_a_ack) = sync_channel(32);
    // let (tx_b, rx_b) = sync_channel(4096 * 16);

    // let _ = thread::spawn(move || loop {
    //     tx_b.send(()).unwrap();
    //     tx_a.send(()).unwrap();
    // });

    // let _ = thread::spawn(move || {
    //     let mut rng = rand::thread_rng();
    //     loop {
    //         thread::sleep(Duration::from_millis(rng.gen_range(0..15000)));

    //         rx_b.recv().unwrap();
    //         let count = 1 + rx_b.try_iter().take(7).count();
    //         tx_a_ack.send(count).unwrap();
    //     }
    // });

    // loop {
    //     for _ in rx_a.try_iter().take(8) {
    //         q.add(buf.as_slice())?;
    //     }

    //     if let Ok(count) = rx_a_ack.try_recv() {
    //         let count = count + rx_a_ack.try_iter().sum::<usize>();
    //         q.remove_n(count)?;
    //     }
    // }
}
