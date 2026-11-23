//! Text analysis shared by full-text search's index and its residual check, so that both see the same tokens.
//! Behavioural spec: dev/doc/fts.allium.

use std::collections::BTreeMap;
use std::sync::Arc;

pub const SIMPLE_ANALYZER: &str = "simple";

/// Bumped whenever tokenisation changes, so that search files built by an earlier analyzer are never read by a later one.
pub const ANALYZER_VERSION: i32 = 2;

/// The longest token a search index holds, in bytes: tantivy drops longer ones, so a longer run of alphanumerics is split into tokens of at most this many.
pub const MAX_TOKEN_BYTES: usize = 65_530;

pub fn tokens(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    for word in text.split(|character: char| !character.is_alphanumeric()).filter(|token| !token.is_empty()) {
        let word = word.to_lowercase();
        if word.len() <= MAX_TOKEN_BYTES { tokens.push(word); continue; }
        let mut piece = String::new();
        for character in word.chars() {
            if piece.len() + character.len_utf8() > MAX_TOKEN_BYTES { tokens.push(std::mem::take(&mut piece)); }
            piece.push(character);
        }
        tokens.push(piece);
    }
    tokens
}

/// A query's distinct terms, in first-occurrence order, and its phrases: runs of terms written between double quotes.
/// A document matches when it holds every term, and every phrase as consecutive tokens; an unclosed quote runs to the end.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

/// A document length as the search index stores it: rounded down to one of 256 values, the same lossy encoding tantivy's field norms use.
pub fn stored_length(tokens: usize) -> u32 {
    let tokens = u32::try_from(tokens).unwrap_or(u32::MAX);
    LENGTHS[LENGTHS.binary_search(&tokens).unwrap_or_else(|index| index - 1)]
}

const LENGTHS: [u32; 256] = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
    16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
    32, 33, 34, 35, 36, 37, 38, 39, 40, 42, 44, 46, 48, 50, 52, 54,
    56, 60, 64, 68, 72, 76, 80, 84, 88, 96, 104, 112, 120, 128, 136, 144,
    152, 168, 184, 200, 216, 232, 248, 264, 280, 312, 344, 376, 408, 440, 472, 504,
    536, 600, 664, 728, 792, 856, 920, 984, 1_048, 1_176, 1_304, 1_432, 1_560, 1_688, 1_816, 1_944,
    2_072, 2_328, 2_584, 2_840, 3_096, 3_352, 3_608, 3_864, 4_120, 4_632, 5_144, 5_656, 6_168, 6_680, 7_192, 7_704,
    8_216, 9_240, 10_264, 11_288, 12_312, 13_336, 14_360, 15_384, 16_408, 18_456, 20_504, 22_552, 24_600, 26_648, 28_696, 30_744,
    32_792, 36_888, 40_984, 45_080, 49_176, 53_272, 57_368, 61_464, 65_560, 73_752, 81_944, 90_136, 98_328, 106_520, 114_712, 122_904,
    131_096, 147_480, 163_864, 180_248, 196_632, 213_016, 229_400, 245_784, 262_168, 294_936, 327_704, 360_472, 393_240, 426_008, 458_776, 491_544,
    524_312, 589_848, 655_384, 720_920, 786_456, 851_992, 917_528, 983_064, 1_048_600, 1_179_672, 1_310_744, 1_441_816, 1_572_888, 1_703_960, 1_835_032, 1_966_104,
    2_097_176, 2_359_320, 2_621_464, 2_883_608, 3_145_752, 3_407_896, 3_670_040, 3_932_184, 4_194_328, 4_718_616, 5_242_904, 5_767_192, 6_291_480, 6_815_768, 7_340_056, 7_864_344,
    8_388_632, 9_437_208, 10_485_784, 11_534_360, 12_582_936, 13_631_512, 14_680_088, 15_728_664, 16_777_240, 18_874_392, 20_971_544, 23_068_696, 25_165_848, 27_263_000, 29_360_152, 31_457_304,
    33_554_456, 37_748_760, 41_943_064, 46_137_368, 50_331_672, 54_525_976, 58_720_280, 62_914_584, 67_108_888, 75_497_496, 83_886_104, 92_274_712, 100_663_320, 109_051_928, 117_440_536, 125_829_144,
    134_217_752, 150_994_968, 167_772_184, 184_549_400, 201_326_616, 218_103_832, 234_881_048, 251_658_264, 268_435_480, 301_989_912, 335_544_344, 369_098_776, 402_653_208, 436_207_640, 469_762_072, 503_316_504,
    536_870_936, 603_979_800, 671_088_664, 738_197_528, 805_306_392, 872_415_256, 939_524_120, 1_006_632_984, 1_073_741_848, 1_207_959_576, 1_342_177_304, 1_476_395_032, 1_610_612_760, 1_744_830_488, 1_879_048_216, 2_013_265_944,
];

fn inverse_frequency(term: &str, corpus: Option<&Corpus>) -> f64 {
    corpus.and_then(|corpus| corpus.frequencies.get(term).map(|df| (corpus.documents as f64, *df as f64)))
        .map_or(1.0, |(documents, df)| (1.0 + (documents - df + 0.5) / (df + 0.5)).ln())
}

fn rounded_term_score(frequency: u32, length: u32, average: Option<f64>, idf: f64) -> f64 {
    let (frequency, length) = (f64::from(frequency), f64::from(length));
    let norm = match average {
        Some(average) if average > 0.0 => 1.0 - B + B * length / average,
        _ => 1.0,
    };
    idf * frequency * (K1 + 1.0) / (frequency + K1 * norm)
}

/// BM25 parameters fixed once for the scan and the SQL expressions that score its rows.
pub struct Scorer {
    query: Query,
    average: Option<f64>,
    idfs: Vec<f64>,
}

impl Scorer {
    pub fn new(query: Query, corpus: Option<&Corpus>) -> Self {
        let average = corpus.filter(|corpus| corpus.documents > 0 && corpus.tokens > 0)
            .map(|corpus| corpus.tokens as f64 / corpus.documents as f64);
        let idfs: Vec<f64> = query.terms.iter().map(|term| inverse_frequency(term, corpus)).collect();
        Self { query, average, idfs }
    }

    pub fn query(&self) -> &Query { &self.query }

    /// The document's score from its stored length and frequencies in query-term order.
    pub fn score(&self, frequencies: &[u32], length: u32) -> f64 {
        frequencies.iter().take(self.idfs.len()).enumerate().map(|(term, &frequency)| {
            self.term_score(term, frequency, length)
        }).fold(-0.0, |total, term| total + term)
    }

    /// The contribution of one term at its index in query-term order.
    pub fn term_score(&self, term: usize, frequency: u32, length: u32) -> f64 {
        if frequency == 0 { 0.0 } else { rounded_term_score(frequency, length, self.average, self.idfs[term]) }
    }

    pub fn bm25(&self, document: &[String]) -> f64 {
        let frequencies: Vec<u32> = self.query.terms.iter().map(|term| document.iter().filter(|token| *token == term).count() as u32).collect();
        self.score(&frequencies, stored_length(document.len()))
    }

    pub fn for_ranking(self: &Arc<Self>) -> Option<RankScorer> {
        if self.idfs.iter().any(|&idf| idf != 0.0 && !(f64::EPSILON / 4096.0..=64.0).contains(&idf)) {
            return None;
        }
        Some(RankScorer(Arc::clone(self)))
    }
}

/// A scorer whose weights are in the domain of the window bound.
pub struct RankScorer(Arc<Scorer>);

impl RankScorer {
    pub fn scorer(&self) -> &Arc<Scorer> { &self.0 }

    /// Prefix maxima of the exact document-local term scores for the common low-frequency case.
    /// The binary64 formula can round non-monotonically, so the maximum observed frequency alone is insufficient.
    pub fn exact_document_local_bounds(&self, term: usize, maximum_frequency: u32) -> Vec<f64> {
        const MAX_EXACT_FREQUENCY: u32 = 256;
        if self.0.average.is_some() { return Vec::new(); }
        let mut maximum = 0.0f64;
        (0..=maximum_frequency.min(MAX_EXACT_FREQUENCY)).map(|frequency| {
            maximum = maximum.max(self.0.term_score(term, frequency, 0));
            maximum
        }).collect()
    }

    /// Bounds a term independently so adding the bounds in score order preserves the bound at each rounding step.
    /// For positive weights in the admitted range, the four operations after normalization are normal binary64.
    /// The selected padding covers rounding of both the row's score and the window's score. See verification/smt/rank-bound.smt2.
    pub fn term_bound(&self, term: usize, maximum_frequency: u32, minimum_length: u32) -> f64 {
        let idf = self.0.idfs[term];
        if idf == 0.0 || maximum_frequency == 0 { return 0.0; }
        rounded_term_score(maximum_frequency, minimum_length, self.0.average, idf) * (1.0 + 16.0 * f64::EPSILON)
    }
}

/// BM25 of a document's tokens for a query, relative to a corpus.
pub fn bm25(query: &Query, document: &[String], corpus: Option<&Corpus>) -> f64 {
    Scorer::new(query.clone(), corpus).bm25(document)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_document_local_bounds_cover_every_admitted_frequency_and_length() {
        let scoring = std::sync::Arc::new(Scorer::new(Query::parse("invoice"), None));
        let rank = scoring.for_ranking().unwrap();
        let bound = rank.exact_document_local_bounds(0, 257);
        assert_eq!(bound.len(), 257);
        let lengths = [0, 1, 40, 2048, 1 << 31, u32::MAX];
        for frequency in 0..=256 {
            let score = scoring.term_score(0, frequency, 0);
            for length in lengths {
                assert_eq!(scoring.term_score(0, frequency, length), score);
            }
            assert!(score <= bound[frequency as usize]);
            assert_eq!(bound[frequency as usize], (0..=frequency)
                .map(|n| scoring.term_score(0, n, 0)).fold(0.0f64, f64::max));
        }
    }

    #[test]
    fn tokens_split_on_non_alphanumerics_and_lowercase() {
        assert_eq!(tokens("Invoice #42 is OVERDUE—since Ünter-Tag!"), ["invoice", "42", "is", "overdue", "since", "ünter", "tag"]);
        assert!(tokens(" ,;! ").is_empty());
    }

    #[test]
    fn a_word_longer_than_an_index_token_is_split_into_tokens_the_index_holds() {
        let word = "é".repeat(MAX_TOKEN_BYTES);
        let found = tokens(&format!("short {word} tail"));
        assert_eq!(found.len(), 4);
        assert!(found.iter().all(|token| token.len() <= MAX_TOKEN_BYTES && !token.is_empty()));
        assert_eq!(found[1..3].concat(), word);
        assert!(Query::parse(&word).matches(&found), "the word still matches itself");
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
    fn stored_lengths_are_exact_up_to_forty_and_round_down_beyond() {
        assert_eq!((stored_length(0), stored_length(1), stored_length(40)), (0, 1, 40));
        assert!(stored_length(1000) <= 1000 && stored_length(1000) > 900);
        assert_eq!(stored_length(stored_length(1000) as usize), stored_length(1000));
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

    #[test]
    fn a_rounded_frequency_reversal_still_has_a_conservative_window_bound() {
        let corpus = Corpus { documents: 1, tokens: 1_340_810_819, frequencies: BTreeMap::new() };
        let scoring = Arc::new(Scorer::new(Query::parse("term"), Some(&corpus)));
        let scoring_bound = scoring.for_ranking().unwrap();
        let (frequency, length) = (402_652_755, 402_653_208);
        let score = scoring.score(&[frequency], length);
        let unpadded = scoring.score(&[frequency + 1], length);
        assert_eq!(score.to_bits(), 0x4001_9999_992e_8a4d);
        assert_eq!(unpadded.to_bits(), 0x4001_9999_992e_8a4c);
        assert!(score <= scoring_bound.term_bound(0, frequency + 1, length), "rounded score exceeds its window bound");
    }

    #[test]
    fn a_weight_outside_the_proof_domain_cannot_admit_ranked_pruning() {
        let corpus = Corpus { documents: 0, tokens: 0, frequencies: BTreeMap::from([("term".into(), 1)]) };
        let scoring = Arc::new(Scorer::new(Query::parse("term"), Some(&corpus)));
        assert!(scoring.for_ranking().is_none());
        assert!(scoring.score(&[1], 1) < 0.0);
    }
}
