
<img align="center" width="100" height="100" src="assets/giraffe_svgrepo_com.png">

Twiga
--------------------------------------------------------------------------------

Twiga is a lexical search experiment using partitioned index of hashed words in SQL tables.  

## Design Objectives

- [ ] Subsecond lexical search across a large number of texts

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

The bin-sharding structure stores **low-frequency hashes** distributed across multiple tables for scalability:

- **Table Naming**: `bin_1`, `bin_2`, ... `bin_N`
- **Bin Assignment**: `bin_number = (int(hash_value, 16) % index_bins) + 1`
- **Purpose**: Store low-frequency word hashes (those appearing in ≤10% of documents) with their document positions
- **Optimization**: High-frequency hashes are moved to separate `hash_*` tables (see below)

Each `bin_*` table contains:
- `hash` (VARCHAR, dictionary-compressed): BLAKE2b-256 hash of the original word
- `text_id` (INTEGER): Reference to source document (foreign key to `word_counts.text_id`)
- `positions` (INTEGER[]): Array of zero-indexed word positions within the text

**Bin-Sharding Benefits:**
- **Scalability**: Distributes data across tables, reducing per-table size
- **Parallelism**: Multiple bins can be queried concurrently
- **Memory Efficiency**: Smaller working sets during indexing
- **Maintenance**: Specific bins can be rebuilt independently
- **Query Optimization**: Separating high-frequency hashes reduces data volume in bins

#### High-Frequency Hashes Tables (`hash_*`)

This optimization identifies and separates **high-frequency hashes** into dedicated tables.  
A high-frequency word hash is a word hash appearing in >10% of all documents in the index.

- **Table Naming**: `hash_<BLAKE2b_hash>` (one table per high-frequency hash)
- **Purpose**: Store high-frequency hash occurrences separately for efficient filtering.
- **Optimization Strategy**: High-frequency hashes are only queried after confirming a document contains all required low-frequency hashes, if any.

Each `hash_*` table contains:
- `hash` (VARCHAR): The high-frequency hash value
- `text_id` (INTEGER): Reference to source document
- `positions` (INTEGER[]): Array of word positions within the text

#### High-Frequency Hashes Metadata Table (`high_frequency_hashes`)

Metadata table tracking all high-frequency hashes identified during optimization:

- `hash` (VARCHAR PRIMARY KEY): The high-frequency hash
- `document_count` (INTEGER): Number of documents containing this hash
- `document_percentage` (DOUBLE): Percentage of total documents containing this hash
- `table_name` (VARCHAR): Name of the corresponding `hash_*` table

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

Implements a two-tier search strategy optimized for mixed-frequency queries:

1. **Request Hashing**: Normalize, tokenize, and hash search terms (same as indexing)
2. **Hash Classification**: Separate search hashes into low-frequency (from bins) and high-frequency (from `hash_*` tables)
3. **Low-Frequency Query**: Query relevant `bin_*` tables for all low-frequency search hashes
4. **Document Filtering**: Identify documents containing ALL required low-frequency hashes
5. **High-Frequency Query**: Query `hash_*` tables only for high-frequency hashes in qualifying documents (if any)
6. **Result Assembly**: Combine low and high-frequency results, maintaining original request order

### Performance Characteristics

**Write Performance:**
- Multiprocessing across CPU cores distributes hashing work
- Batch insertion reduces transaction overhead
- Multiple threads write to different bins with no contention

**Query Performance:**
- Two-tier strategy minimizes data scanned for high-frequency terms
- Low-frequency hashes narrow document set before querying high-frequency tables
- High-frequency hash queries benefit from dedicated, smaller tables
- UNION queries combine results efficiently
- Hash mapping optimizes result ordering

**Optimization Process (`twiga_index_optimizer.py`):**
1. Analyzes hash distribution across all documents
2. Identifies high-frequency hashes (>10% document coverage)
3. Creates dedicated `hash_*` tables for each high-frequency hash
4. Generates `high_frequency_hashes` metadata table
5. Removes high-frequency entries from bin tables
6. Reorders remaining bin tables by hash for cache locality

Example: Optimization of an index with 3 100 860 text items and a total of 1 521 483 534 non-unique words took 1 hour 48 minutes 25 seconds.

## Word Definition

A word is any sequence of Unicode lowercase alphanumeric characters between two whitespaces.

## Search Criteria

Twiga returns text IDs that match the following criteria:

* **1.** They have the full set of unique words presented in the search request.
* **2.** They have one or more sequences of words identical to the sequence of words in the search request.

## Ranking Criterion

Matching words frequency is the ranking criterion. It is defined as the number of search request words found in a document divided by the number of all words in the document. Short documents having high number of matching words are at the top of the search results.

## Limitations

Twiga is a lexical search experiment with a focused scope having the following limitations:

- **No Stopword Removal**: Common words are indexed equally with less common words.
- **AND Logic Only**: All search words must be present in the results, Boolean OR and NOT are not supported.
- **No Wildcard or Fuzzy Search**: Pattern matching and typo tolerance are not implemented.
- **No Proximity Search**: Cannot search for words within a specific distance of each other.

## Limiting Factors

These are the most important factors limiting the performance of the search architecture implemented in Twiga:

- **High-Frequency Words**: Stopwords and all other high-frequency words in a search request increase significantly the search latency, especially when a search request is composed only of high-frequency words. While most search requests having 6 words or less are completed with a sub-second latency even in exact phrase mode, some search requests composed entirely of high-frequency words can take up to 4 seconds.
- **Number of Indexed Texts**: The largest index used by Twiga has 3 100 860 text entries with a sum of 1 521 483 534 non-unique words. Larger indexes are technically possible, but they will have larger tables of high-frequency words further increasing the search latency.

## Name

Twiga, meaning giraffe in Swahili, is a name inspired by the story of [the three giraffe calfs orphaned during a severe drought around 2018 in Northern Kenya and saved thanks to the kindness and efforts of a local community](https://science.sandiegozoo.org/science-blog/lekiji-fupi-and-twiga).  
  
Today we use complex data processing technologies thanks to the knowledge, persistence and efforts of many people of a large global community. Just like the small giraffe calfs in the story above, we owe much to our community and should always be thankful to its members for their goodwill and contributions!  

## [Thanks and Credits](./CREDITS.md)

## [License](./LICENSE)

This program is licensed under the terms of the Apache License 2.0.

## Author

[Dimitar D. Mitov](https://www.linkedin.com/in/dimitar-mitov-12388982/), 2025 - 2026
