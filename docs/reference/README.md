# Reference

Dry, factual description, for looking things up while you work.

The **API reference** — every type, method, trait, and their contracts — is
rustdoc, published at [docs.rs/ultima-db](https://docs.rs/ultima-db) and
[docs.rs/ultima-vector](https://docs.rs/ultima-vector), or locally via
`cargo doc --open`. The pages here describe the surfaces that cut across
individual API items:

- [Configuration reference](configuration.md) — every `StoreConfig` field,
  the writer/durability/WAL/isolation variants, defaults, and which
  combinations are legal, inert, or rejected.
- [Cargo features](cargo-features.md) — what each feature enables and what
  docs.rs builds with.
- [Isolation levels reference](isolation-levels.md) — which anomalies each
  level prevents, read-tracking granularity, validation rules, measured
  cost, and current limitations.
- [Key encoding and storage formats](key-encoding-and-formats.md) — the
  `PrimaryKey` encoding rules, type ids, the key-length cap, and the v2
  WAL / checkpoint / snapshot-stream formats.
- [Performance reference](performance.md) — the current provenance-pinned
  YCSB numbers against RocksDB, Fjall, and ReDB.
