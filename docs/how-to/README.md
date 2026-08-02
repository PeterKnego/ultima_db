# How-to guides

Each guide here solves one real problem, assuming you already know your way
around a `Store` and its transactions. (If you don't yet, take the
[tutorial](../tutorials/getting-started.md) first.) Guides link to the
[reference](../reference/README.md) for exhaustive options and to the
[explanations](../explanation/README.md) for the reasoning behind a design.

## Durability and data movement

Keeping data alive across restarts, crashes, versions, and machines.

- [How to set up durable persistence and recover after a crash](set-up-durable-persistence.md)
- [How to bulk-load, back up, and restore data](bulk-load-and-restore.md)
- [How to replicate a store with snapshot streams](replicate-with-snapshot-streams.md) — including running under a Raft/Paxos consensus log
- [How to migrate a persistent store from 0.2.x to 0.3.0](migrate-from-0-2-to-0-3.md)

## Concurrency and transactions

Writing from several threads — or an async runtime — without losing updates.

- [How to handle write conflicts between concurrent writers](handle-write-conflicts.md)
- [How to prevent write skew with Serializable isolation](prevent-write-skew.md)
- [How to use UltimaDB from async code](use-from-async-code.md)

## Modeling and querying data

Shaping tables around your data and finding records by more than their id.

- [How to use natural primary keys instead of auto-increment ids](use-natural-primary-keys.md)
- [How to find records by fields other than the primary key](query-with-indexes.md) — secondary, full-text, and custom indexes
- [How to add vector search to your data](add-vector-search.md)

## Configuration

- [How to choose a configuration for your workload](choose-a-configuration.md)
