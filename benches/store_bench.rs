use criterion::{criterion_group, criterion_main, Criterion};
use pprof::criterion::{Output, PProfProfiler};
use ultima_db::Store;

fn bench_create(c: &mut Criterion) {
    c.bench_function("store_create", |b| b.iter(Store::default));
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_create
}
criterion_main!(benches);
