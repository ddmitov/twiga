
<img align="center" width="100" height="100" src="assets/giraffe_svgrepo_com.png">

Twiga
--------------------------------------------------------------------------------

Twiga is a lexical search experiment using partitioned index of hashed words in SQL tables.  

## Design Objectives

* **1.** Fast subsecond lexical search
* **2.** Fast indexing of large number of texts
* **3.** Index data based entirely in standard SQL tables
* **4.** Usability in a variety of SQL-driven systems

## Features

- [x] Index data is stored only in ordinary SQL tables.

- [x] Twiga is language-agnostic, different languages can coexist in a single index.

- [x] The index and text locations are independent from one another, texts may live in a different system.

## Workflow

- [x] Texts are split to words using a normalizer and a pre-tokenizer from the Tokenizers Python module.

- [x] Words are hashed and their positions are saved in tables.

- [x] Only the tables of the hashed words in the search request are contacted during search.

- [x] Words are represented by hash alias integers during search.

- [x] Search is performed using DuckDB SQL.

## Word Definition

A word is any sequence of Unicode lowercase alphanumeric characters between two whitespaces.

## Search Criteria

Twiga selects the IDs of texts that match the following criteria:

* **1.** They have the full set of unique word hashes presented in the search request.
* **2.** They have one or more sequences of word hashes identical to the sequence of word hashes in the search request.

## Ranking Criterion

Matching words frequency is the ranking criterion. It is defined as the number of search request words found in a document divided by the number of all words in the document. Short documents having high number of matching words are at the top of the search results.

## Name

Twiga, meaning giraffe in Swahili, is a name inspired by the story of [the three giraffe calfs orphaned during a severe drought around 2018 in Northern Kenya and saved thanks to the kindness and efforts of a local community](https://science.sandiegozoo.org/science-blog/lekiji-fupi-and-twiga).  
  
Today we use complex data processing technologies thanks to the knowledge, persistence and efforts of many people of a large global community. Just like the small giraffe calfs in the story above, we owe much to our community and should always be thankful to its members for their goodwill and contributions!  

## [Thanks and Credits](./CREDITS.md)

## [License](./LICENSE)

This program is licensed under the terms of the Apache License 2.0.

## Author

[Dimitar D. Mitov](https://www.linkedin.com/in/dimitar-mitov-12388982/), 2025
