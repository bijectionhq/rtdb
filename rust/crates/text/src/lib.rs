//! Text analysis shared by full-text search's index and its residual check, so that both see the same tokens.
//! Behavioural spec: dev/doc/fts.allium.

use std::collections::BTreeMap;

pub const SIMPLE_ANALYZER: &str = "simple";

/// Bumped whenever tokenisation changes, so that search files built by an earlier analyzer are never read by a later one.
pub const ANALYZER_VERSION: i32 = 1;

pub fn tokens(text: &str) -> Vec<String> {
    text.split(|character: char| !character.is_alphanumeric()).filter(|token| !token.is_empty()).map(str::to_lowercase).collect()
}

/// A query's distinct terms, in first-occurrence order, and its phrases: runs of terms written between double quotes.
/// A document matches when it holds every term, and every phrase as consecutive tokens; an unclosed quote runs to the end.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Query { terms: Vec<String>, phrases: Vec<Vec<String>> }

impl Query {
    pub fn parse(text: &str) -> Self {
        let mut terms = Vec::new();
        let mut phrases = Vec::new();
        for (index, part) in text.split('"').enumerate() {
            let words = tokens(part);
            for word in &words { if !terms.contains(word) { terms.push(word.clone()); } }
            if index % 2 == 1 && words.len() > 1 { phrases.push(words); }
        }
        Self { terms, phrases }
    }
    pub fn terms(&self) -> &[String] { &self.terms }
    pub fn phrases(&self) -> &[Vec<String>] { &self.phrases }
    /// A query with no terms matches nothing.
    pub fn matches(&self, document: &[String]) -> bool {
        !self.terms.is_empty() && self.terms.iter().all(|term| document.contains(term))
            && self.phrases.iter().all(|phrase| document.windows(phrase.len()).any(|window| window == phrase.as_slice()))
    }
}

/// Statistics of the documents a score is relative to.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Corpus { pub documents: u64, pub tokens: u64, pub frequencies: BTreeMap<String, u64> }

impl Corpus {
    pub fn add(&mut self, other: &Corpus) {
        self.documents += other.documents;
        self.tokens += other.tokens;
        for (term, frequency) in &other.frequencies { *self.frequencies.entry(term.clone()).or_default() += frequency; }
    }
}

const K1: f64 = 1.2;
const B: f64 = 0.75;

/// BM25 of a document's tokens for a query, relative to a corpus.
/// Without a corpus, or for a term the corpus does not hold, the term's inverse document frequency is 1 and the document is taken to be of average length.
pub fn bm25(query: &Query, document: &[String], corpus: Option<&Corpus>) -> f64 {
    let length = document.len() as f64;
    let average = corpus.filter(|corpus| corpus.documents > 0 && corpus.tokens > 0).map_or(length, |corpus| corpus.tokens as f64 / corpus.documents as f64);
    query.terms.iter().map(|term| {
        let frequency = document.iter().filter(|token| *token == term).count() as f64;
        if frequency == 0.0 { return 0.0; }
        let idf = corpus.and_then(|corpus| corpus.frequencies.get(term).map(|df| (corpus.documents as f64, *df as f64)))
            .map_or(1.0, |(documents, df)| (1.0 + (documents - df + 0.5) / (df + 0.5)).ln());
        let norm = if average > 0.0 { 1.0 - B + B * length / average } else { 1.0 };
        idf * frequency * (K1 + 1.0) / (frequency + K1 * norm)
    }).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_split_on_non_alphanumerics_and_lowercase() {
        assert_eq!(tokens("Invoice #42 is OVERDUE—since Ünter-Tag!"), ["invoice", "42", "is", "overdue", "since", "ünter", "tag"]);
        assert!(tokens(" ,;! ").is_empty());
    }

    #[test]
    fn a_query_matches_documents_holding_all_of_its_terms() {
        let query = Query::parse("Overdue invoice overdue");
        assert_eq!(query.terms(), ["overdue", "invoice"]);
        assert!(query.matches(&tokens("The invoice is overdue")));
        assert!(!query.matches(&tokens("The invoice is paid")));
        assert!(!Query::parse("!!").matches(&tokens("anything")));
    }

    #[test]
    fn a_phrase_matches_only_its_terms_in_order_and_side_by_side() {
        let query = Query::parse(r#"paid "overdue invoice""#);
        assert_eq!(query.terms(), ["paid", "overdue", "invoice"]);
        assert_eq!(query.phrases(), [vec!["overdue".to_string(), "invoice".to_string()]]);
        assert!(query.matches(&tokens("The overdue invoice was paid")));
        assert!(!query.matches(&tokens("The invoice was overdue and paid")));
        assert!(!query.matches(&tokens("An overdue, unpaid invoice")));
        assert!(Query::parse(r#""overdue""#).phrases().is_empty(), "a one-word phrase is a term");
        assert_eq!(Query::parse(r#"a "b c"#).phrases(), [vec!["b".to_string(), "c".to_string()]]);
    }

    #[test]
    fn bm25_favours_rarer_terms_and_shorter_documents() {
        let corpus = Corpus { documents: 100, tokens: 1000, frequencies: BTreeMap::from([("common".into(), 90), ("rare".into(), 2)]) };
        let query = Query::parse("common rare");
        let rare = bm25(&query, &tokens("rare words here"), Some(&corpus));
        let common = bm25(&query, &tokens("common words here"), Some(&corpus));
        assert!(rare > common, "{rare} > {common}");
        let short = bm25(&Query::parse("rare"), &tokens("rare"), Some(&corpus));
        let long = bm25(&Query::parse("rare"), &tokens("rare a b c d e f g h i j k l m n o p q r s t"), Some(&corpus));
        assert!(short > long);
        assert_eq!(bm25(&query, &tokens("nothing"), Some(&corpus)), 0.0);
        assert!(bm25(&query, &tokens("rare"), None) > 0.0);
    }
}
