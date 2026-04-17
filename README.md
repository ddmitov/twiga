
<img align="center" width="100" height="100" src="assets/giraffe_svgrepo_com.png">

Twiga
--------------------------------------------------------------------------------

Twiga is a lexical search experiment using partitioned index of hashed words in SQL tables.  

## Design Objectives

- [x] Fast subsecond lexical search across a large number of texts

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

The core indexing structure uses bin-sharding for distributed storage:

- **Table Naming**: `bin_1`, `bin_2`, ... `bin_N`
- **Bin Assignment**: `bin_number = (int(hash_value, 16) % index_bins) + 1`
- **Purpose**: Store all word hash occurrences with their document positions for quick lookup

Each `bin_*` table contains:
- `hash` (VARCHAR, dictionary-compressed): BLAKE2b-256 hash of the original word (32 hex characters, typically 64 characters total)
- `text_id` (INTEGER): Reference to source document (foreign key to `word_counts.text_id`)
- `positions` (INTEGER[]): Array of zero-indexed word positions within the text, used for phrase queries and proximity calculations

**Bin-Sharding Benefits:**
- **Scalability**: Distributes data across tables, reducing per-table size
- **Parallelism**: Multiple bins can be queried concurrently
- **Memory Efficiency**: Smaller working sets during indexing
- **Maintenance**: Specific bins can be rebuilt independently

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

#### Search Process (`twiga_core_search.py`)

1. **Request Hashing**: Same normalization/tokenization as indexing
2. **Bin Lookup**: Calculate which bin(s) contain each search term hash
3. **Parallel Queries**: Query relevant bins using UNION
4. **Result Assembly**: Combine results with original request order mapping

### Performance Characteristics

**Write Performance:**
- Multiprocessing across CPU cores distributes hashing work
- Batch insertion reduces transaction overhead
- Multiple threads write to different bins with no contention

**Query Performance:**
- Single search may touch multiple bins
- UNION queries combine results efficiently
- Hash mapping optimizes result ordering

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

## Name

Twiga, meaning giraffe in Swahili, is a name inspired by the story of [the three giraffe calfs orphaned during a severe drought around 2018 in Northern Kenya and saved thanks to the kindness and efforts of a local community](https://science.sandiegozoo.org/science-blog/lekiji-fupi-and-twiga).  
  
Today we use complex data processing technologies thanks to the knowledge, persistence and efforts of many people of a large global community. Just like the small giraffe calfs in the story above, we owe much to our community and should always be thankful to its members for their goodwill and contributions!  

## [Thanks and Credits](./CREDITS.md)

## [License](./LICENSE)

This program is licensed under the terms of the Apache License 2.0.

## Author

[Dimitar D. Mitov](https://www.linkedin.com/in/dimitar-mitov-12388982/), 2025 - 2026
