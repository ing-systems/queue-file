use auto_delete_path::AutoDeletePath;
use criterion::{Bencher, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use queue_file::QueueFile;

const BATCH_SIZE: usize = 1000;
const SIZES: &[usize] = &[64, 1024, 10240];

fn bench_write_macro(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_macro");
    for &size in SIZES {
        let data = vec![0u8; size];
        group.throughput(Throughput::Elements(BATCH_SIZE as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            &size,
            |b: &mut Bencher, &_size| {
                b.iter_batched(
                    || {
                        let path = AutoDeletePath::temp();
                        let qf = QueueFile::open(path.as_ref()).unwrap();
                        (path, qf)
                    },
                    |(_path, mut qf): (AutoDeletePath, QueueFile)| {
                        for _ in 0..BATCH_SIZE {
                            qf.add(&data).unwrap();
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_read_iter_macro(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_iter_macro");
    for &size in SIZES {
        let data = vec![0u8; size];
        group.throughput(Throughput::Elements(BATCH_SIZE as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            &size,
            |b: &mut Bencher, &_size| {
                b.iter_batched(
                    || {
                        let path = AutoDeletePath::temp();
                        let mut qf = QueueFile::open(path.as_ref()).unwrap();
                        for _ in 0..BATCH_SIZE {
                            qf.add(&data).unwrap();
                        }
                        (path, qf)
                    },
                    |(_path, qf): (AutoDeletePath, QueueFile)| {
                        for item in qf.iter() {
                            criterion::black_box(item);
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_read_remove_macro(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_remove_macro");
    for &size in SIZES {
        let data = vec![0u8; size];
        group.throughput(Throughput::Elements(BATCH_SIZE as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            &size,
            |b: &mut Bencher, &_size| {
                b.iter_batched(
                    || {
                        let path = AutoDeletePath::temp();
                        let mut qf = QueueFile::open(path.as_ref()).unwrap();
                        for _ in 0..BATCH_SIZE {
                            qf.add(&data).unwrap();
                        }
                        (path, qf)
                    },
                    |(_path, mut qf): (AutoDeletePath, QueueFile)| {
                        for _ in 0..BATCH_SIZE {
                            qf.remove().unwrap();
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_write_macro, bench_read_iter_macro, bench_read_remove_macro);
criterion_main!(benches);
