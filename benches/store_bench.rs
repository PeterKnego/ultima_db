use criterion::{criterion_group, criterion_main, Criterion};
use ultima_db::Store;

fn bench_create(c: &mut Criterion) {
    c.bench_function("store_create", |b| b.iter(Store::new));
}

criterion_group!(benches, bench_create);
criterion_main!(benches);
