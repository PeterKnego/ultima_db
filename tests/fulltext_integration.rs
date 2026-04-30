#![cfg(feature = "fulltext")]

use ultima_db::{FullTextIndex, Store, Table};

#[derive(Debug, Clone)]
#[cfg_attr(feature = "persistence", derive(serde::Serialize, serde::Deserialize))]
struct Article {
    title: String,
    body: String,
}

fn article(title: &str, body: &str) -> Article {
    Article { title: title.into(), body: body.into() }
}

fn make_index() -> FullTextIndex<Article> {
    FullTextIndex::new(|a: &Article| format!("{} {}", a.title, a.body))
}

#[test]
fn fulltext_on_table() {
    let mut table = Table::<Article>::new();
    table.define_custom_index("search", make_index()).unwrap();

    let id1 = table.insert(article("Rust Programming", "Systems language")).unwrap();
    let id2 = table.insert(article("Python Guide", "Scripting language")).unwrap();

    let idx = table.custom_index::<FullTextIndex<Article>>("search").unwrap();

    let results = idx.search("rust");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, id1);

    let results = idx.search("python");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, id2);

    let results = idx.search("language");
    assert_eq!(results.len(), 2);
}

#[test]
fn fulltext_through_store_transactions() {
    let store = Store::default();

    let mut wtx = store.begin_write(None).unwrap();
    {
        let mut table = wtx.open_table::<Article>("articles").unwrap();
        table.define_custom_index("search", make_index()).unwrap();
        table.insert(article("Rust MVCC", "Copy-on-write B-trees for versioning")).unwrap();
        table.insert(article("Go Channels", "Concurrency with goroutines")).unwrap();
    }
    wtx.commit().unwrap();

    let rtx = store.begin_read(None).unwrap();
    let table = rtx.open_table::<Article>("articles").unwrap();
    let idx = table.custom_index::<FullTextIndex<Article>>("search").unwrap();

    let results = idx.search("rust");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, 1);

    let results = idx.search("concurrency");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, 2);
}

#[test]
fn fulltext_snapshot_isolation() {
    let store = Store::default();

    let mut wtx = store.begin_write(None).unwrap();
    {
        let mut table = wtx.open_table::<Article>("articles").unwrap();
        table.define_custom_index("search", make_index()).unwrap();
        table.insert(article("Rust", "A systems language")).unwrap();
    }
    wtx.commit().unwrap();

    let rtx_v1 = store.begin_read(None).unwrap();

    let mut wtx2 = store.begin_write(None).unwrap();
    {
        let mut table = wtx2.open_table::<Article>("articles").unwrap();
        table.insert(article("More Rust", "Rust is great")).unwrap();
    }
    wtx2.commit().unwrap();

    // v1 snapshot sees only 1 match
    let table_v1 = rtx_v1.open_table::<Article>("articles").unwrap();
    let idx_v1 = table_v1.custom_index::<FullTextIndex<Article>>("search").unwrap();
    assert_eq!(idx_v1.search("rust").len(), 1);

    // Latest snapshot sees 2 matches
    let rtx_v2 = store.begin_read(None).unwrap();
    let table_v2 = rtx_v2.open_table::<Article>("articles").unwrap();
    let idx_v2 = table_v2.custom_index::<FullTextIndex<Article>>("search").unwrap();
    assert_eq!(idx_v2.search("rust").len(), 2);
}

#[test]
fn fulltext_update_through_store() {
    let store = Store::default();

    let mut wtx = store.begin_write(None).unwrap();
    {
        let mut table = wtx.open_table::<Article>("articles").unwrap();
        table.define_custom_index("search", make_index()).unwrap();
        table.insert(article("Rust", "Old content about rust")).unwrap();
    }
    wtx.commit().unwrap();

    // Update the article to be about Python instead
    let mut wtx2 = store.begin_write(None).unwrap();
    {
        let mut table = wtx2.open_table::<Article>("articles").unwrap();
        table.update(1, article("Python", "New content about python")).unwrap();
    }
    wtx2.commit().unwrap();

    let rtx = store.begin_read(None).unwrap();
    let table = rtx.open_table::<Article>("articles").unwrap();
    let idx = table.custom_index::<FullTextIndex<Article>>("search").unwrap();

    assert!(idx.search("rust").is_empty());
    assert_eq!(idx.search("python").len(), 1);
}

#[test]
fn fulltext_delete_through_store() {
    let store = Store::default();

    let mut wtx = store.begin_write(None).unwrap();
    {
        let mut table = wtx.open_table::<Article>("articles").unwrap();
        table.define_custom_index("search", make_index()).unwrap();
        table.insert(article("Rust", "Systems language")).unwrap();
        table.insert(article("Go", "Compiled language")).unwrap();
    }
    wtx.commit().unwrap();

    let mut wtx2 = store.begin_write(None).unwrap();
    {
        let mut table = wtx2.open_table::<Article>("articles").unwrap();
        table.delete(1).unwrap();
    }
    wtx2.commit().unwrap();

    let rtx = store.begin_read(None).unwrap();
    let table = rtx.open_table::<Article>("articles").unwrap();
    let idx = table.custom_index::<FullTextIndex<Article>>("search").unwrap();

    assert!(idx.search("rust").is_empty());
    assert_eq!(idx.search("language").len(), 1);
    assert_eq!(idx.search("language")[0].id, 2);
}

#[test]
fn fulltext_batch_insert() {
    let mut table = Table::<Article>::new();
    table.define_custom_index("search", make_index()).unwrap();

    let articles = vec![
        article("Alpha", "First article"),
        article("Beta", "Second article"),
        article("Gamma", "Third article"),
    ];
    let ids = table.insert_batch(articles).unwrap();
    assert_eq!(ids.len(), 3);

    let idx = table.custom_index::<FullTextIndex<Article>>("search").unwrap();
    assert_eq!(idx.search("article").len(), 3);
    assert_eq!(idx.search("alpha").len(), 1);
    assert_eq!(idx.search("beta").len(), 1);
}

#[test]
fn fulltext_backfill_existing_data() {
    let mut table = Table::<Article>::new();
    table.insert(article("Rust", "Systems programming")).unwrap();
    table.insert(article("Go", "Concurrent programming")).unwrap();

    // Define index after data already exists — should backfill
    table.define_custom_index("search", make_index()).unwrap();

    let idx = table.custom_index::<FullTextIndex<Article>>("search").unwrap();
    assert_eq!(idx.search("programming").len(), 2);
    assert_eq!(idx.search("rust").len(), 1);
}

#[test]
fn fulltext_ranking_across_transactions() {
    let store = Store::default();

    let mut wtx = store.begin_write(None).unwrap();
    {
        let mut table = wtx.open_table::<Article>("articles").unwrap();
        table.define_custom_index("search", make_index()).unwrap();
        table.insert(article("Databases", "A short intro")).unwrap();
        table.insert(article("Databases and Indexing", "Databases use indexes for fast lookup in databases")).unwrap();
    }
    wtx.commit().unwrap();

    let rtx = store.begin_read(None).unwrap();
    let table = rtx.open_table::<Article>("articles").unwrap();
    let idx = table.custom_index::<FullTextIndex<Article>>("search").unwrap();

    let results = idx.search("databases");
    assert_eq!(results.len(), 2);
    // Doc 2 has more occurrences of "databases" — should rank higher
    assert_eq!(results[0].id, 2);
    assert!(results[0].score > results[1].score);
}

#[test]
fn fulltext_skips_records_with_no_indexable_tokens() {
    // tokenize() filters punctuation and whitespace; an article whose
    // text contains only such characters has zero tokens. add_doc and
    // remove_doc must early-return without touching the postings/index
    // state, leaving the index identical before and after the operation.
    let mut table = Table::<Article>::new();
    table.define_custom_index("search", make_index()).unwrap();

    let id_real = table
        .insert(article("Rust", "Systems language"))
        .unwrap();
    let id_empty = table.insert(article("   ", "...!?")).unwrap();

    let idx = table.custom_index::<FullTextIndex<Article>>("search").unwrap();
    assert_eq!(idx.search("rust").len(), 1);
    assert_eq!(idx.search("rust")[0].id, id_real);

    // Deleting the all-punctuation row hits the empty-tokens early-return
    // in remove_doc and must leave search results stable.
    table.delete(id_empty).unwrap();
    let idx = table.custom_index::<FullTextIndex<Article>>("search").unwrap();
    assert_eq!(idx.search("rust").len(), 1);
}
