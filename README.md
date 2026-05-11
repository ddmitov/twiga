
<img align="center" width="100" height="100" src="assets/giraffe_svgrepo_com.png">

Twiga
--------------------------------------------------------------------------------

Twiga is a lexical search experiment using partitioned index of hashed words in SQL tables.  

## Design Objectives

- [x] Subsecond lexical search across a large number of texts

- [x] Index data stored entirely in standard DuckDB tables

- [x] Language-agnostic architecture with different languages coexisting in a single index

- [x] Independent locations of the index and text data

## Database Architecture

Twiga uses a **bin-sharding architecture** in DuckDB that distributes hash entries across multiple tables for scalability and performance. The system maintains data across two separate databases:

### Index Database (`twiga_index.duckdb`)

Located in the `data/` directory, this database contains the core index structure:

#### Word Counts Table (`word_counts`)

- Stores word counts for each indexed text
- Columns:
  - `text_id` (INTEGER PRIMARY KEY): Unique identifier for each text
  - `words_total` (INTEGER): Total number of indexed words (after stopword removal)
- Used for relevance scoring during search ranking

#### Hash Index Bin Tables (`bin_*`)

The bin-sharding structure distributes all word hashes across multiple tables for scalability:

- **Table Naming**: `bin_1`, `bin_2`, ... `bin_N`
- **Bin Assignment**: `bin_number = (int(hash_value, 16) % index_bins) + 1`
- **Purpose**: Store word hashes distributed by bin assignment with their document positions

Each `bin_*` table contains:
- `hash` (VARCHAR, dictionary-compressed): BLAKE2b-256 hash of the original word
- `text_id` (INTEGER): Reference to source document (foreign key to `word_counts.text_id`)
- `positions` (INTEGER[]): Array of zero-indexed word positions within the text

**Bin-Sharding Benefits:**
- **Scalability**: Distributes data across tables, reducing per-table size
- **Parallelism**: Multiple bins can be queried concurrently
- **Memory Efficiency**: Smaller working sets during indexing
- **Maintenance**: Specific bins can be rebuilt independently
- **Cache Locality**: Tables can be reordered by hash for improved performance

#### Hash Metadata Table (`hash_metadata`)

Metadata table created during index optimization tracking hash frequency:

- `hash` (VARCHAR PRIMARY KEY): The word hash value
- `document_count` (INTEGER): Number of documents containing this hash

### Text Database (`twiga_texts.duckdb`)

**`texts_bin_*` Tables (Optional)**
- Stores original text content if texts are indexed in the Twiga system
- Alternative to external text storage; texts may also be stored separately outside Twiga

### Data Flow

#### Indexing Process (`twiga_core_index.py`)

1. **Text Normalization**: NFD Unicode decomposition, accent stripping, lowercase conversion
2. **Tokenization**: Whitespace splitting, punctuation removal, digit handling
3. **Hashing** (multiprocessing): BLAKE2b with 16-byte digest, words hashed independently, position tracking maintained
4. **Bin Assignment**: Hash modulo determines bin distribution
5. **Batch Writing**: Insertion into respective `bin_*` tables

Example: 3 100 860 texts having a total of 1 521 483 534 non-unique words were indexed for 8 hours 13 minutes 36 seconds.

#### Search Process (`twiga_core_search.py`)

Implements lexical search across the bin-sharded index:

1. **Request Hashing**: Normalize, tokenize, and hash search terms (same as indexing)
2. **Hash Lookup**: Query relevant `bin_*` tables containing the search hashes
3. **Document Identification**: Identify documents containing ALL search hashes
4. **Result Assembly**: Return matching document IDs, optionally ordered by relevance

### Performance Characteristics

**Write Performance:**
- Multiprocessing across CPU cores distributes hashing work
- Batch insertion reduces transaction overhead
- Multiple threads write to different bins with no contention

**Query Performance:**
- Bin-sharding distributes data, reducing per-table size
- Hash lookup uses relevant `bin_*` tables to find documents
- Reordered tables maintain hash locality for better cache efficiency
- UNION queries combine results from multiple bins efficiently

**Optimization Process (`twiga_index_optimizer.py`):**
1. Analyzes hash frequency across all documents
2. Creates `hash_metadata` table recording document count for each hash
3. Reorders all `bin_*` tables by hash and text_id for improved cache locality

Example: Optimization of an index with 3,100,860 text items and a total of 1,521,483,534 non-unique words took 1 hour 48 minutes 25 seconds.

## Word Definition

A word is any sequence of Unicode lowercase alphanumeric characters between two whitespaces.

## Search Criteria

Twiga returns text IDs that match the following criteria:

* **1.** They have the full set of unique words presented in the search request.
* **2.** They have one or more sequences of words identical to the sequence of words in the search request.

## Ranking

Twiga uses **BM25 (Best Matching 25)** scoring for ranking search results. BM25 is a probabilistic ranking function that considers:

- **Term Frequency (TF)**: How many times a search term appears in a document
- **Inverse Document Frequency (IDF)**: How rare or common a term is across all documents
- **Document Length Normalization**: Adjusts scores to account for document length variations

BM25 formula: `IDF(term) × (TF × (k1 + 1)) / (TF + k1 × (1 - b + b × |D| / avgdl))`

Where:
- `IDF = LN((N - df + 0.5) / (df + 0.5))`
- `N` = total number of documents
- `df` = number of documents containing the term
- `|D|` = document length
- `avgdl` = average document length
- `k1` = 1.5 (term frequency saturation parameter, configurable)
- `b` = 0.75 (length normalization parameter, configurable)

For **single-word searches**, BM25 score is calculated for that term.

For **multi-word searches** (any position), BM25 scores for each matching term are summed.

For **phrase searches**, the minimum document frequency across phrase terms is used as the IDF component, with phrase match count as the term frequency.

## Limitations

Twiga is a lexical search experiment with a focused scope having the following limitations:

- **No Stopword Removal**: Common words are indexed equally with less common words.
- **AND Logic Only**: All search words must be present in the results, Boolean OR and NOT are not supported.
- **No Wildcard or Fuzzy Search**: Pattern matching and typo tolerance are not implemented.
- **No Proximity Search**: Cannot search for words within a specific distance of each other.

## Limiting Factors

These are the most important factors limiting the performance of the search architecture implemented in Twiga:

- **High-Frequency Words**: Stopwords and other high-frequency words in a search request require scanning larger tables and this is increasing the search latency. A search request composed entirely of high-frequency words takes longer to complete than one with mixed-frequency terms.
- **Number of Indexed Texts**: The largest index used by Twiga has 3,100,860 text entries with a sum of 1,521,483,534 non-unique words. Larger indexes increase the total data volume across all bin tables, which can impact query performance.

## Name

Twiga, meaning giraffe in Swahili, is a name inspired by the story of [the three giraffe calfs orphaned during a severe drought around 2018 in Northern Kenya and saved thanks to the kindness and efforts of a local community](https://science.sandiegozoo.org/science-blog/lekiji-fupi-and-twiga).  
  
Today we use complex data processing technologies thanks to the knowledge, persistence and efforts of many people of a large global community. Just like the small giraffe calfs in the story above, we owe much to our community and should always be thankful to its members for their goodwill and contributions!  

## [Thanks and Credits](./CREDITS.md)

## [License](./LICENSE)

This program is licensed under the terms of the Apache License 2.0.

## Author

[Dimitar D. Mitov](https://www.linkedin.com/in/dimitar-mitov-12388982/), 2025 - 2026
