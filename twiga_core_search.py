#!/usr/bin/env python3

# Core modules:
import hashlib

# PIP modules:
import pyarrow    as     pa
from   tokenizers import normalizers
from   tokenizers import pre_tokenizers


def twiga_request_hasher(search_request: str) -> list:
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
        hashlib.blake2b(word_tuple[0].encode(), digest_size=16).hexdigest()
        for word_tuple in pre_tokenized_search_request
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

    # Get unique hashes only:
    hash_set = set(request_hash_list)

    # Create a mapping of each hash to its index in the original list of request hashes.
    # This is needed to minimize the memory overhead of passing the hashes to the searcher function.
    mapping_dict = {hash_item: index for index, hash_item in enumerate(hash_set)}

    # Map each hash to its corresponding bin:
    bin_dict = {}

    for hash_item in hash_set:
        bin_number = (int(hash_item, 16) % index_bins) + 1

        if bin_number not in bin_dict:
            bin_dict[bin_number] = []

        bin_dict[bin_number].append(hash_item)

    # Compose and execute the query to retrieve hash entries from all bins:
    hash_query   = ''
    query_number = 0

    for bin_number, hash_list in bin_dict.items():
        for hash_item in hash_list:
            query_number += 1

            hash_query += f"""
                SELECT
                    {mapping_dict[hash_item]} AS hash_id,
                    text_id,
                    positions
                FROM bin_{bin_number}
                WHERE hash = '{hash_item}'
            """

            if query_number < len(hash_set):
                hash_query += 'UNION'

    hash_table = duckdb_connection.sql(hash_query).fetch_arrow_table()

    # Reconstruct the list of hash IDs in the order of the original request:
    hash_id_list = [mapping_dict[hash_item] for hash_item in request_hash_list]

    return hash_id_list, hash_table


def twiga_single_word_searcher(
    duckdb_connection: object,
    hash_table:        pa.Table,
    results_number:    int
) -> None | pa.Table:
    """
    Find texts containing consecutive word sequences (phrase matching).

    Args:
        results_number: Maximum results to return. Use 0 for unlimited results.
    """

    # Build LIMIT clause only if results_number > 0:
    limit_clause = f'LIMIT {str(results_number)}' if results_number > 0 else ''

    search_query = f"""
        SELECT
            ht.text_id,
            LEN(FIRST(ht.positions)) AS matching_words,
            FIRST(wc.words_total) AS words_total,
            ROUND(
                (matching_words / FIRST(wc.words_total)), 5
            ) AS matching_words_frequency
        FROM
            hash_table AS ht
            LEFT JOIN word_counts AS wc
                ON wc.text_id = ht.text_id
        GROUP BY ht.text_id
        ORDER BY matching_words_frequency DESC
        {limit_clause}
    """

    result_table = duckdb_connection.sql(search_query).fetch_arrow_table()

    if result_table.num_rows == 0:
        result_table = None

    return result_table


def twiga_any_position_searcher(
    duckdb_connection: object,
    hash_table:        pa.Table,
    hash_id_list:      list,
    results_number:    int
) -> None | pa.Table:
    """
    Find texts containing words in any order.

    Args:
        results_number: Maximum results to return. Use 0 for unlimited results.
    """

    # Build LIMIT clause only if results_number > 0:
    limit_clause = f'LIMIT {str(results_number)}' if results_number > 0 else ''

    request_sequence_string = ''.join(map(str, hash_id_list))

    search_query = f"""
        WITH
            -- Step 1. Keep only texts that contain all required hashes:
            texts_with_all_hashes AS (
                SELECT text_id
                FROM hash_table
                GROUP BY text_id
                HAVING COUNT(DISTINCT(hash_id)) = {str(len(set(hash_id_list)))}
            ),

            -- Step 2. Count the number of positions:
            positions AS (
                SELECT
                    ht.hash_id,
                    ht.text_id,
                    LENGTH(ht.positions) AS positions_number
                FROM hash_table AS ht  
                    INNER JOIN texts_with_all_hashes AS twa
                        ON twa.text_id = ht.text_id
            )

        -- Step 3. Score texts by total number of matching words:
        SELECT
            p.text_id,
            SUM(p.positions_number) AS matching_words,
            FIRST(wc.words_total) AS words_total,
            ROUND(
                (matching_words / FIRST(wc.words_total)), 5
            ) AS matching_words_frequency
        FROM
            positions AS p
            LEFT JOIN word_counts AS wc
                ON wc.text_id = p.text_id
        GROUP BY p.text_id
        ORDER BY matching_words_frequency DESC
        {limit_clause}
    """

    result_table = duckdb_connection.sql(search_query).fetch_arrow_table()

    if result_table.num_rows == 0:
        result_table = None

    return result_table


def twiga_exact_phrase_searcher(
    duckdb_connection: object,
    hash_table:        pa.Table,
    hash_id_list:      list,
    results_number:    int
) -> None | pa.Table:
    """
    Find texts containing consecutive word sequences (phrase matching).

    Args:
        results_number: Maximum results to return. Use 0 for unlimited results.
    """

    # Build LIMIT clause only if results_number > 0:
    limit_clause = f'LIMIT {str(results_number)}' if results_number > 0 else ''

    request_sequence_string = ''.join(map(str, hash_id_list))

    search_query = f"""
        WITH
            -- Step 1. Keep only texts that contain all required hashes:
            texts_with_all_hashes AS (
                SELECT text_id
                FROM hash_table
                GROUP BY text_id
                HAVING COUNT(DISTINCT(hash_id)) = {str(len(set(hash_id_list)))}
            ),

            -- Step 2. Flatten position arrays into individual rows:
            positions AS (
                SELECT
                    ht.hash_id,
                    ht.text_id,
                    UNNEST(ht.positions) AS position
                FROM hash_table AS ht  
                    INNER JOIN texts_with_all_hashes AS twa
                        ON twa.text_id = ht.text_id
            ),

            -- Step 3. Define sequence groups using ROW_NUMBER().
            -- ROW_NUMBER() - position is constant for consecutive positions:
            sequences AS (
                SELECT
                    text_id,
                    hash_id,
                    position,
                    ROW_NUMBER() OVER (
                        PARTITION BY text_id
                        ORDER BY position ASC
                    ) - position AS sequence_id
                FROM positions
            ),

            -- Step 4. Build sequence strings for every sequence:
            sequences_by_text AS (
                SELECT
                    text_id,
                    STRING_AGG(hash_id, '' ORDER BY position) AS sequence
                FROM sequences
                GROUP BY
                    text_id,
                    sequence_id
                HAVING COUNT(hash_id) = {str(len(hash_id_list))}
            )

        -- Step 5. Match sequences containing the search pattern:
        SELECT
            sbt.text_id,
            COUNT(sbt.sequence) * {str(len(hash_id_list))} AS matching_words,
            FIRST(wc.words_total) AS words_total,
            ROUND(
                (matching_words / FIRST(wc.words_total)), 5
            ) AS matching_words_frequency
        FROM
            sequences_by_text AS sbt
            LEFT JOIN word_counts AS wc
                ON wc.text_id = sbt.text_id
        WHERE sbt.sequence = '{request_sequence_string}'
        GROUP BY sbt.text_id
        ORDER BY matching_words_frequency DESC
        {limit_clause}
    """

    result_table = duckdb_connection.sql(search_query).fetch_arrow_table()

    if result_table.num_rows == 0:
        result_table = None

    return result_table
