#!/usr/bin/env python3

# Core modules:
import hashlib

# PIP modules:
import pyarrow    as     pa
from   tokenizers import normalizers
from   tokenizers import pre_tokenizers


def twiga_request_hasher(
    search_request: str,
    stopword_set:   set
) -> list:
    """Normalize, tokenize, and hash a search request into word hashes."""

    normalizer = normalizers.Sequence(
        [
            normalizers.NFD(),          # Decompose Unicode characters
            normalizers.StripAccents(), # Remove accents after decomposition
            normalizers.Lowercase()     # Convert to lowercase
        ]
    )

    normalized_search_request = normalizer.normalize_str(search_request)

    pre_tokenizer = pre_tokenizers.Sequence(
        [
            pre_tokenizers.Whitespace(),
            pre_tokenizers.Punctuation(behavior='removed'),
            pre_tokenizers.Digits(individual_digits=False)
        ]
    )

    pre_tokenized_search_request = \
        pre_tokenizer.pre_tokenize_str(normalized_search_request)

    hash_list = [
        hashlib.blake2b(word_tuple[0].encode(), digest_size=32).hexdigest()
        for word_tuple in pre_tokenized_search_request
        if word_tuple[0] not in stopword_set
    ]

    return hash_list


def twiga_index_reader(
    duckdb_connection: object,
    index_bins:        int,
    request_hash_list: list
) -> tuple[None, None] | tuple[list, pa.Table]:
    """Look up hash entries from sharded bins and return matching index data."""

    if len(request_hash_list) == 0:
        return None, None

    hash_set = set(request_hash_list)
    bin_dict = {}

    for hash_item in hash_set:
        bin_number = (int(hash_item, 16) % index_bins) + 1

        if bin_number not in bin_dict:
            bin_dict[bin_number] = []

        bin_dict[bin_number].append(hash_item)

    mapping_query  = ''
    hash_query     = ''
    query_number   = 0

    for bin_number, hash_list in bin_dict.items():
        query_number += 1

        hash_list_string = "'" + "', '".join(map(str, set(hash_list))) + "'"

        mapping_query += f"""
            SELECT
                hash,
                hash_id,
            FROM bin_{str(bin_number)}_hash_dict
            WHERE hash IN ({hash_list_string})
        """

        hash_query += f"""
            SELECT
                hi.hash_id,
                hi.text_id AS text_id,
                hi.positions AS positions
            FROM
                bin_{str(bin_number)}_hash_index AS hi
                INNER JOIN mapping_table AS mt
                    ON mt.hash_id = hi.hash_id
            WHERE mt.hash IN ({hash_list_string})
        """

        if query_number < len(bin_dict):
            mapping_query += 'UNION'
            hash_query    += 'UNION'


    # The order of execution of the SQL queries is important here:
    mapping_table = duckdb_connection.sql(mapping_query).fetch_arrow_table()
    hash_table    = duckdb_connection.sql(hash_query).fetch_arrow_table()

    mapping_dict = dict(
        zip(
            mapping_table['hash'].to_pylist(),
            mapping_table['hash_id'].to_pylist()
        )
    )

    try:
        hash_id_list = [mapping_dict[item] for item in request_hash_list]
    except Exception:
        return None, None

    return hash_id_list, hash_table


def twiga_searcher(
    duckdb_connection:   object,
    hash_table:          pa.Table,
    hash_id_list:        list,
    results_number:      int
) -> None | pa.Table:
    """
    Find texts containing consecutive word sequences (phrase matching).

    Uses an offset-based islands-and-gaps algorithm to detect phrases:
    - For a phrase like "quick brown fox", words must appear consecutively
    - Key insight: if "quick" is at position P, "brown" must be at P+1, "fox" at P+2
    - Therefore: position - expected_offset = P (constant for valid phrase match)

    Args:
        results_number: Maximum results to return. Use 0 for unlimited results.
    """

    # Build LIMIT clause only if results_number > 0:
    limit_clause = f'LIMIT {results_number}' if results_number > 0 else ''

    # Single word - use simpler lookup:
    if len(hash_id_list) == 1:
        search_query = f"""
            SELECT
                ht.text_id,
                LEN(FIRST(ht.positions)) AS matching_words,
                FIRST(wc.words_total) AS words_total,
                ROUND(
                    (matching_words / FIRST(wc.words_total)), 5
                ) AS term_frequency
            FROM
                hash_table AS ht
                LEFT JOIN word_counts AS wc
                    ON wc.text_id = ht.text_id
            GROUP BY ht.text_id
            ORDER BY term_frequency DESC
            {limit_clause}
        """

    # Multiple words - use offset-based islands-and-gaps for phrase matching:
    if len(hash_id_list) > 1:
        # Build search_offsets values: (hash_id, expected_offset) pairs
        # Expected offset is the word's position in the search phrase (0-indexed)
        offsets_values = ', '.join(
            [
                f'({hash_id}, {offset})'
                for offset, hash_id in enumerate(hash_id_list)
            ]
        )

        search_words_count = len(hash_id_list)
        unique_hash_count  = len(set(hash_id_list))

        search_query = f"""
            WITH
                -- Pre-filter: only texts containing all unique hashes
                texts_with_all_hashes AS (
                    SELECT text_id
                    FROM hash_table
                    GROUP BY text_id
                    HAVING COUNT(DISTINCT hash_id) = {unique_hash_count}
                ),

                -- Map each hash_id to its expected offset in the search phrase
                search_offsets (hash_id, expected_offset) AS (
                    VALUES {offsets_values}
                ),

                -- Flatten position arrays into individual rows
                positions_flat AS (
                    SELECT
                        ht.text_id,
                        ht.hash_id,
                        UNNEST(ht.positions) AS position
                    FROM hash_table AS ht
                    WHERE ht.text_id IN (
                        SELECT text_id
                        FROM texts_with_all_hashes
                    )
                ),

                -- Calculate phrase_start: position - expected_offset
                -- For a valid phrase, all words have the same phrase_start
                positions_with_offsets AS (
                    SELECT
                        pf.text_id,
                        pf.position - so.expected_offset AS phrase_start,
                        so.expected_offset
                    FROM positions_flat AS pf
                    INNER JOIN search_offsets AS so
                        ON so.hash_id = pf.hash_id
                ),

                -- Find valid phrases: all expected offsets present at same phrase_start
                valid_phrases AS (
                    SELECT
                        text_id,
                        phrase_start
                    FROM positions_with_offsets
                    GROUP BY
                        text_id,
                        phrase_start
                    HAVING COUNT(DISTINCT expected_offset) = {search_words_count}
                ),

                -- Count phrases per text, calculate matching words
                phrase_counts AS (
                    SELECT
                        text_id,
                        COUNT(*) * {search_words_count} AS matching_words
                    FROM valid_phrases
                    GROUP BY text_id
                )

            SELECT
                pc.text_id,
                pc.matching_words,
                wc.words_total,
                ROUND(pc.matching_words / wc.words_total, 5) AS term_frequency
            FROM phrase_counts AS pc
            LEFT JOIN word_counts AS wc
                ON wc.text_id = pc.text_id
            ORDER BY term_frequency DESC
            {limit_clause}
        """

    result_table = duckdb_connection.sql(search_query).fetch_arrow_table()

    if result_table.num_rows == 0:
        result_table = None

    return result_table
