use std::collections::HashMap;
use std::sync::Arc;

use crate::btree::BTree;
use crate::index::CustomIndex;
use crate::persistence::Record;
use crate::Result;

#[derive(Debug, Clone)]
pub struct SearchResult {
    pub id: u64,
    pub score: f64,
}

pub struct FullTextIndex<R: Record> {
    postings: BTree<(String, u64), u32>,
    doc_freq: BTree<String, u32>,
    doc_lengths: BTree<u64, u32>,
    total_docs: u64,
    total_doc_length: u64,
    k1: f64,
    b: f64,
    extractor: Arc<dyn Fn(&R) -> String + Send + Sync>,
}

impl<R: Record> Clone for FullTextIndex<R> {
    fn clone(&self) -> Self {
        Self {
            postings: self.postings.clone(),
            doc_freq: self.doc_freq.clone(),
            doc_lengths: self.doc_lengths.clone(),
            total_docs: self.total_docs,
            total_doc_length: self.total_doc_length,
            k1: self.k1,
            b: self.b,
            extractor: Arc::clone(&self.extractor),
        }
    }
}

impl<R: Record> FullTextIndex<R> {
    pub fn new(extractor: impl Fn(&R) -> String + Send + Sync + 'static) -> Self {
        Self {
            postings: BTree::new(),
            doc_freq: BTree::new(),
            doc_lengths: BTree::new(),
            total_docs: 0,
            total_doc_length: 0,
            k1: 1.2,
            b: 0.75,
            extractor: Arc::new(extractor),
        }
    }

    pub fn with_bm25_params(mut self, k1: f64, b: f64) -> Self {
        self.k1 = k1;
        self.b = b;
        self
    }

    pub fn search(&self, query: &str) -> Vec<SearchResult> {
        self.search_with_limit(query, usize::MAX)
    }

    pub fn search_with_limit(&self, query: &str, limit: usize) -> Vec<SearchResult> {
        let tokens = tokenize(query);
        if tokens.is_empty() || self.total_docs == 0 {
            return Vec::new();
        }

        let avgdl = self.total_doc_length as f64 / self.total_docs as f64;
        let mut scores: HashMap<u64, f64> = HashMap::new();

        for token in &tokens {
            let df = self.doc_freq.get(token).copied().unwrap_or(0) as f64;
            let idf = ((self.total_docs as f64 - df + 0.5) / (df + 0.5) + 1.0).ln();
            let idf = idf.max(0.0);

            for ((_, doc_id), tf) in
                self.postings.range((token.clone(), 0u64)..=(token.clone(), u64::MAX))
            {
                let tf = *tf as f64;
                let dl = self.doc_lengths.get(doc_id).copied().unwrap_or(0) as f64;
                let score = idf * (tf * (self.k1 + 1.0))
                    / (tf + self.k1 * (1.0 - self.b + self.b * dl / avgdl));
                *scores.entry(*doc_id).or_insert(0.0) += score;
            }
        }

        let mut results: Vec<SearchResult> = scores
            .into_iter()
            .map(|(id, score)| SearchResult { id, score })
            .collect();

        results.sort_unstable_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));

        results.truncate(limit);
        results
    }

    fn add_doc(&mut self, id: u64, record: &R) {
        let text = (self.extractor)(record);
        let tokens = tokenize(&text);
        if tokens.is_empty() {
            return;
        }

        let mut term_freqs: HashMap<&str, u32> = HashMap::new();
        for token in &tokens {
            *term_freqs.entry(token).or_insert(0) += 1;
        }

        for (term, tf) in term_freqs {
            self.postings = self.postings.insert((term.to_string(), id), tf);
            let new_df = self.doc_freq.get(&term.to_string()).copied().unwrap_or(0) + 1;
            self.doc_freq = self.doc_freq.insert(term.to_string(), new_df);
        }

        self.doc_lengths = self.doc_lengths.insert(id, tokens.len() as u32);
        self.total_docs += 1;
        self.total_doc_length += tokens.len() as u64;
    }

    fn remove_doc(&mut self, id: u64, record: &R) {
        let text = (self.extractor)(record);
        let tokens = tokenize(&text);
        if tokens.is_empty() {
            return;
        }

        let mut seen: HashMap<&str, bool> = HashMap::new();
        for token in &tokens {
            if seen.insert(token, true).is_some() {
                continue;
            }
            if let Ok(new_postings) = self.postings.remove(&(token.to_string(), id)) {
                self.postings = new_postings;
            }
            let df = self.doc_freq.get(&token.to_string()).copied().unwrap_or(1);
            if df <= 1 {
                if let Ok(new_df) = self.doc_freq.remove(&token.to_string()) {
                    self.doc_freq = new_df;
                }
            } else {
                self.doc_freq = self.doc_freq.insert(token.to_string(), df - 1);
            }
        }

        let dl = self.doc_lengths.get(&id).copied().unwrap_or(0);
        if let Ok(new_dl) = self.doc_lengths.remove(&id) {
            self.doc_lengths = new_dl;
        }
        self.total_docs -= 1;
        self.total_doc_length -= dl as u64;
    }
}

impl<R: Record> CustomIndex<R> for FullTextIndex<R> {
    fn on_insert(&mut self, id: u64, record: &R) -> Result<()> {
        self.add_doc(id, record);
        Ok(())
    }

    fn on_update(&mut self, id: u64, old: &R, new: &R) -> Result<()> {
        self.remove_doc(id, old);
        self.add_doc(id, new);
        Ok(())
    }

    fn on_delete(&mut self, id: u64, record: &R) {
        self.remove_doc(id, record);
    }
}

fn tokenize(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| c.is_whitespace() || c.is_ascii_punctuation())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "persistence", derive(serde::Serialize, serde::Deserialize))]
    struct Article {
        title: String,
        body: String,
    }

    fn make_index() -> FullTextIndex<Article> {
        FullTextIndex::new(|a: &Article| format!("{} {}", a.title, a.body))
    }

    #[test]
    fn tokenize_basic() {
        assert_eq!(tokenize("Hello, World!"), vec!["hello", "world"]);
        assert_eq!(tokenize("  spaces   between  "), vec!["spaces", "between"]);
        assert_eq!(tokenize(""), Vec::<String>::new());
        assert_eq!(tokenize("one"), vec!["one"]);
    }

    #[test]
    fn tokenize_punctuation() {
        assert_eq!(tokenize("it's a test."), vec!["it", "s", "a", "test"]);
        assert_eq!(tokenize("key=value;other"), vec!["key", "value", "other"]);
    }

    #[test]
    fn insert_and_search() {
        let mut idx = make_index();
        idx.on_insert(
            1,
            &Article {
                title: "Rust Programming".into(),
                body: "Rust is a systems programming language.".into(),
            },
        )
        .unwrap();
        idx.on_insert(
            2,
            &Article {
                title: "Python Guide".into(),
                body: "Python is great for scripting.".into(),
            },
        )
        .unwrap();

        let results = idx.search("rust");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 1);

        let results = idx.search("programming");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 1);

        let results = idx.search("python");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 2);
    }

    #[test]
    fn search_no_match() {
        let mut idx = make_index();
        idx.on_insert(
            1,
            &Article {
                title: "Rust".into(),
                body: "Systems language.".into(),
            },
        )
        .unwrap();

        let results = idx.search("java");
        assert!(results.is_empty());
    }

    #[test]
    fn search_empty_query() {
        let mut idx = make_index();
        idx.on_insert(
            1,
            &Article {
                title: "Rust".into(),
                body: "Test".into(),
            },
        )
        .unwrap();

        assert!(idx.search("").is_empty());
        assert!(idx.search("   ").is_empty());
    }

    #[test]
    fn search_empty_corpus() {
        let idx = make_index();
        assert!(idx.search("anything").is_empty());
    }

    #[test]
    fn multi_word_query_ranking() {
        let mut idx = make_index();
        idx.on_insert(
            1,
            &Article {
                title: "Rust".into(),
                body: "A language.".into(),
            },
        )
        .unwrap();
        idx.on_insert(
            2,
            &Article {
                title: "Rust Concurrency".into(),
                body: "Rust has great concurrency support.".into(),
            },
        )
        .unwrap();

        let results = idx.search("rust concurrency");
        assert_eq!(results.len(), 2);
        // Doc 2 matches both "rust" and "concurrency"; doc 1 only matches "rust"
        assert_eq!(results[0].id, 2);
        assert!(results[0].score > results[1].score);
    }

    #[test]
    fn delete_removes_from_index() {
        let mut idx = make_index();
        let article = Article {
            title: "Rust".into(),
            body: "Test".into(),
        };
        idx.on_insert(1, &article).unwrap();
        assert_eq!(idx.search("rust").len(), 1);

        idx.on_delete(1, &article);
        assert!(idx.search("rust").is_empty());
        assert_eq!(idx.total_docs, 0);
        assert_eq!(idx.total_doc_length, 0);
    }

    #[test]
    fn update_reindexes() {
        let mut idx = make_index();
        let old = Article {
            title: "Rust".into(),
            body: "Old content.".into(),
        };
        idx.on_insert(1, &old).unwrap();
        assert_eq!(idx.search("rust").len(), 1);
        assert!(idx.search("python").is_empty());

        let new = Article {
            title: "Python".into(),
            body: "New content.".into(),
        };
        idx.on_update(1, &old, &new).unwrap();
        assert!(idx.search("rust").is_empty());
        assert_eq!(idx.search("python").len(), 1);
        assert_eq!(idx.total_docs, 1);
    }

    #[test]
    fn search_with_limit() {
        let mut idx = make_index();
        for i in 0..10 {
            idx.on_insert(
                i,
                &Article {
                    title: format!("Rust article {}", i),
                    body: "Common rust content.".into(),
                },
            )
            .unwrap();
        }

        let results = idx.search_with_limit("rust", 3);
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn clone_independence() {
        let mut idx = make_index();
        idx.on_insert(
            1,
            &Article {
                title: "Rust".into(),
                body: "Test".into(),
            },
        )
        .unwrap();

        let cloned = idx.clone();

        idx.on_insert(
            2,
            &Article {
                title: "Rust again".into(),
                body: "More rust.".into(),
            },
        )
        .unwrap();

        assert_eq!(idx.search("rust").len(), 2);
        assert_eq!(cloned.search("rust").len(), 1);
    }

    #[test]
    fn bm25_term_frequency_matters() {
        let mut idx = make_index();
        idx.on_insert(
            1,
            &Article {
                title: "Rust".into(),
                body: "A language.".into(),
            },
        )
        .unwrap();
        idx.on_insert(
            2,
            &Article {
                title: "Rust Rust Rust".into(),
                body: "Rust is rust and more rust.".into(),
            },
        )
        .unwrap();

        let results = idx.search("rust");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, 2);
        assert!(results[0].score > results[1].score);
    }

    #[test]
    fn rebuild_from_existing_data() {
        let mut idx = make_index();
        let articles = [
            (
                1u64,
                Article {
                    title: "Rust".into(),
                    body: "Systems.".into(),
                },
            ),
            (
                2,
                Article {
                    title: "Python".into(),
                    body: "Scripting.".into(),
                },
            ),
        ];

        idx.rebuild(articles.iter().map(|(id, r)| (*id, r))).unwrap();

        assert_eq!(idx.search("rust").len(), 1);
        assert_eq!(idx.search("python").len(), 1);
        assert_eq!(idx.total_docs, 2);
    }

    #[test]
    fn custom_bm25_params() {
        let idx = FullTextIndex::<Article>::new(|a| a.title.clone())
            .with_bm25_params(2.0, 0.5);
        assert_eq!(idx.k1, 2.0);
        assert_eq!(idx.b, 0.5);
    }
}
