
<img align="center" width="100" height="100" src="assets/giraffe_svgrepo_com.png">

Twiga
--------------------------------------------------------------------------------

Twiga is a lexical search experiment using partitioned index of hashed words in SQL tables.  

## Design Objectives

- [x] Fast subsecond lexical search across a large number of texts

- [x] Index data stored entirely in standard SQL tables

- [ ] Usability in a variety of SQL-driven systems

## Features

- [x] Twiga is language-agnostic, different languages can coexist in a single index.

- [x] Index and text locations are independent from one another, texts may live in a different system.

## Indexing Workflow

- All texts are split into words using a normalizer and a pre-tokenizer from the Tokenizers Python module.

- Each word is hashed and its position is saved in a table, part of a partitioned index.

## Searching Workflow

- Every search requests is converted into hashes of words.

- Only small part of the index tables are contacted during search.

- Search and ranking are performed using vanilla DuckDB SQL with no use of DuckDB extensions.

## Word Definition

A word is any sequence of Unicode lowercase alphanumeric characters between two whitespaces.

## Search Criteria

Twiga returns text IDs that match the following criteria:

* **1.** They have the full set of unique words presented in the search request.
* **2.** They have one or more sequences of words identical to the sequence of words in the search request.

## Ranking Criterion

Term frequency is the ranking criterion. It is defined as the number of search request words found in a document divided by the number of all words in the document. Short documents having high number of matching words are at the top of the search results.

## Name

Twiga, meaning giraffe in Swahili, is a name inspired by the story of [the three giraffe calfs orphaned during a severe drought around 2018 in Northern Kenya and saved thanks to the kindness and efforts of a local community](https://science.sandiegozoo.org/science-blog/lekiji-fupi-and-twiga).  
  
Today we use complex data processing technologies thanks to the knowledge, persistence and efforts of many people of a large global community. Just like the small giraffe calfs in the story above, we owe much to our community and should always be thankful to its members for their goodwill and contributions!  

## [Thanks and Credits](./CREDITS.md)

## [License](./LICENSE)

This program is licensed under the terms of the Apache License 2.0.

## Author

[Dimitar D. Mitov](https://www.linkedin.com/in/dimitar-mitov-12388982/), 2025 - 2026
