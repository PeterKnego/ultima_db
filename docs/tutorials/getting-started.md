# Getting started: build a task tracker

In this tutorial we will build a small task-tracking program on UltimaDB.
When we are done, our program will store tasks in a typed table, update them
inside transactions, read an old version of the data back out of history, and
answer "what is still to do?" through a secondary index.

We need a working Rust toolchain (Rust 1.88 or newer) and about fifteen
minutes. Everything happens in one file; every step ends with something we
can run and see.

## 1. Create the project

In a terminal, in any directory:

```console
$ cargo new task-tracker
$ cd task-tracker
$ cargo add ultima-db
```

`cargo add` writes the dependency into `Cargo.toml`. That is all the setup
there is: UltimaDB is an in-process library, so there is no server to
install or start.

## 2. Store our first tasks

Open `src/main.rs` and replace its contents with:

```rust
use ultima_db::Store;

#[derive(Debug, Clone)]
struct Task {
    title: String,
    done: bool,
}

fn main() -> ultima_db::Result<()> {
    let store = Store::default();

    let mut wtx = store.begin_write(None)?;
    {
        let mut tasks = wtx.open_table::<Task>("tasks")?;
        tasks.insert(Task { title: "buy milk".into(), done: false })?;
        tasks.insert(Task { title: "write report".into(), done: false })?;
        tasks.insert(Task { title: "water plants".into(), done: false })?;
    }
    let version = wtx.commit()?;
    println!("committed version {version}");

    Ok(())
}
```

`Task` is a plain Rust struct — no derive macros beyond `Debug` and `Clone`,
no schema definition. `Store::default()` gives us an in-memory store, and
`begin_write` opens a write transaction; nothing we do inside it is visible
to anyone else until `commit`.

Run it:

```console
$ cargo run
committed version 1
```

Cargo also prints a `dead_code` warning saying the `title` and `done` fields
are never read. That is true so far — we only wrote them. The next step
reads them, and the warning disappears.

Notice the version number: every commit produces a new numbered version of
the whole store. We will use that number in step 4.

## 3. Read the tasks back

Add this to `main`, just above the final `Ok(())`:

```rust
    let rtx = store.begin_read(None)?;
    let tasks = rtx.open_table::<Task>("tasks")?;
    println!("{} tasks at version {}:", tasks.len(), rtx.version());
    for (id, task) in tasks.iter() {
        println!("  {id}: {} (done: {})", task.title, task.done);
    }
```

Run it again:

```console
$ cargo run
committed version 1
3 tasks at version 1:
  1: buy milk (done: false)
  2: write report (done: false)
  3: water plants (done: false)
```

Notice the ids 1, 2, 3: we never chose them. Tables keyed by the default
`u64` hand out auto-increment ids on `insert`. (Tables can also use keys we
choose — strings, tuples — see
[How to use natural primary keys](../how-to/use-natural-primary-keys.md)
after this tutorial.)

## 4. Change a task, then look back in time

We bought the milk. Add this above the final `Ok(())`:

```rust
    let mut wtx = store.begin_write(None)?;
    {
        let mut tasks = wtx.open_table::<Task>("tasks")?;
        tasks.update(1, Task { title: "buy milk".into(), done: true })?;
    }
    let v2 = wtx.commit()?;
    println!("milk bought at version {v2}");

    let now = store.begin_read(None)?;
    let then = store.begin_read(Some(version))?;
    let tasks_now = now.open_table::<Task>("tasks")?;
    let tasks_then = then.open_table::<Task>("tasks")?;
    println!("task 1 today: done = {}", tasks_now.get(1).unwrap().done);
    println!(
        "task 1 at version {}: done = {}",
        version,
        tasks_then.get(1).unwrap().done
    );
```

The second `begin_read` is the interesting one: we pass `Some(version)` —
the number step 2 printed — and get the store *as it was at that commit*.

Run it:

```console
$ cargo run
committed version 1
3 tasks at version 1:
  1: buy milk (done: false)
  2: write report (done: false)
  3: water plants (done: false)
milk bought at version 2
task 1 today: done = true
task 1 at version 1: done = false
```

Notice the last two lines. Both reads ran after our update committed, yet
the version-1 read still sees `done = false`. Old versions stay readable
because commits create new versions instead of overwriting — see
[the architecture explanation](../explanation/architecture.md) for why this
is cheap.

## 5. Ask what is still to do

Scanning every task to find the unfinished ones works, but a secondary index
answers directly. Add this above the final `Ok(())`:

```rust
    let mut wtx = store.begin_write(None)?;
    {
        let mut tasks = wtx.open_table::<Task>("tasks")?;
        tasks.define_index("by_done", IndexKind::NonUnique, |t: &Task| t.done)?;
    }
    wtx.commit()?;

    let rtx = store.begin_read(None)?;
    let tasks = rtx.open_table::<Task>("tasks")?;
    println!("still to do:");
    for (id, task) in tasks.get_by_index("by_done", &false)? {
        println!("  {id}: {}", task.title);
    }
```

And change the first line of the file to import `IndexKind`:

```rust
use ultima_db::{IndexKind, Store};
```

The closure `|t: &Task| t.done` tells the index which field to organize
tasks by; existing rows are indexed immediately.

Run it one last time:

```console
$ cargo run
committed version 1
3 tasks at version 1:
  1: buy milk (done: false)
  2: write report (done: false)
  3: water plants (done: false)
milk bought at version 2
task 1 today: done = true
task 1 at version 1: done = false
still to do:
  2: write report
  3: water plants
```

Task 1 is gone from the list — the index saw our earlier update.

If your output shows task 1 still to do, the `define_index` call probably
ran before the update committed in step 4; check that the step-4 code sits
above the step-5 code in `main`.

## Where we are

We built a program that writes and reads typed records transactionally,
reached back into the store's history, and queried by a field other than the
id — the heart of working with UltimaDB. Everything ran in memory and
vanished at exit; the natural next step is keeping data across runs with
[How to set up durable persistence](../how-to/set-up-durable-persistence.md),
and after that, whatever your project needs next in the
[how-to guides](../how-to/) and the
[configuration reference](../reference/configuration.md).
