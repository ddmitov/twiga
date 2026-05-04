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
    """
    Look up hash entries:
    low-frequency bins first, then high-frequency tables.

    Priority: Read low-frequency hashes from bins, find texts with all of them,
    then extract high-frequency hashes only for those texts.
    """

    if len(request_hash_list) == 0:
        return None, None

    # Get unique hashes only:
    hash_set = set(request_hash_list)

    # Create a mapping of
    # each hash to its index in the original list of request hashes:
    mapping_dict = {
        hash_item: index for index, hash_item in enumerate(hash_set)
    }

    # Determine which hashes are high-frequency vs low-frequency:
    try:
        high_frequency_result = duckdb_connection.execute(
            f"""
                SELECT DISTINCT hash
                FROM high_frequency_hashes
                WHERE hash IN (
                    {','.join(repr(hash_item) for hash_item in hash_set)}
                )
            """
        ).fetchall()

        high_frequency_hashes = set(
            row[0] for row in high_frequency_result
        )
    except:
        high_frequency_hashes = set()

    low_frequency_hashes = hash_set - high_frequency_hashes

    # Query low-frequency hashes from bin tables:
    low_freq_query = ''
    query_number   = 0

    for hash_item in low_frequency_hashes:
        bin_number = (int(hash_item, 16) % index_bins) + 1
        query_number += 1

        low_freq_query += f"""
            SELECT
                {mapping_dict[hash_item]} AS hash_id,
                text_id,
                positions
            FROM bin_{bin_number}
            WHERE hash = '{hash_item}'
        """

        if query_number < len(low_frequency_hashes):
            low_freq_query += 'UNION'

    # Get low-frequency results:
    if low_freq_query:
        low_freq_table = duckdb_connection.sql(
            low_freq_query
        ).fetch_arrow_table()

        # Identify texts that contain ALL required low-frequency hashes:
        texts_with_all_low_query = f"""
            SELECT DISTINCT text_id
            FROM low_freq_table
            GROUP BY text_id
            HAVING COUNT(DISTINCT hash_id) = {len(low_frequency_hashes)}
        """

        texts_with_all_low = duckdb_connection.sql(
            texts_with_all_low_query
        ).fetch_arrow_table()

        if texts_with_all_low.num_rows == 0:
            return None, None

        text_ids_list = texts_with_all_low['text_id'].to_pylist()
        text_ids_str  = ','.join(str(text_id) for text_id in text_ids_list)
        result_tables = [low_freq_table]

    elif len(low_frequency_hashes) > 0:
        # Low-frequency hashes were required but none found:
        return None, None
    else:
        # No low-frequency hashes - all are high-frequency:
        text_ids_list = None
        text_ids_str  = None
        result_tables = []

    # Query high-frequency hashes only for qualifying texts:
    if high_frequency_hashes:
        high_frequency_query = ''
        query_number         = 0

        for hash_item in high_frequency_hashes:
            query_number += 1

            if text_ids_list is not None:
                # Get only high-frequency hashes
                # for texts that have all low-frequency hashes:
                high_frequency_query += f"""
                    SELECT
                        {mapping_dict[hash_item]} AS hash_id,
                        text_id,
                        positions
                    FROM hash_{hash_item}
                    WHERE text_id IN ({text_ids_str})
                """
            else:
                # No low-frequency hashes, get all high-frequency hash data:
                high_frequency_query += f"""
                    SELECT
                        {mapping_dict[hash_item]} AS hash_id,
                        text_id,
                        positions
                    FROM hash_{hash_item}
                """

            if query_number < len(high_frequency_hashes):
                high_frequency_query += 'UNION'

        high_frequency_table = \
            duckdb_connection.sql(high_frequency_query).fetch_arrow_table()

        result_tables.append(high_frequency_table)

    # Combine low and high-frequency results:
    if len(result_tables) > 0:
        if len(result_tables) == 1:
            hash_table = result_tables[0]
        else:
            hash_table = pa.concat_tables(result_tables)

    else:
        return None, None

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
            -- Keep only texts that contain all required hashes:
            texts_with_all_hashes AS (
                SELECT text_id
                FROM hash_table
                GROUP BY text_id
                HAVING COUNT(DISTINCT(hash_id)) = {str(len(set(hash_id_list)))}
            ),

            -- Count the number of positions:
            positions AS (
                SELECT
                    ht.hash_id,
                    ht.text_id,
                    LENGTH(ht.positions) AS positions_number
                FROM hash_table AS ht
                    INNER JOIN texts_with_all_hashes AS twa
                        ON twa.text_id = ht.text_id
            )

        -- Score texts by total number of matching words:
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

    # Build request sequence with delimiter to avoid ambiguity
    # (e.g., "01" vs "0,1"):
    request_sequence_string = ','.join(map(str, hash_id_list))

    search_query = f"""
        WITH
            -- Keep only texts that contain all required hashes:
            texts_with_all_hashes AS (
                SELECT text_id
                FROM hash_table
                GROUP BY text_id
                HAVING COUNT(DISTINCT(hash_id)) = {str(len(set(hash_id_list)))}
            ),

            -- Flatten position arrays into individual rows:
            positions AS (
                SELECT
                    ht.hash_id,
                    ht.text_id,
                    UNNEST(ht.positions) AS position
                FROM hash_table AS ht
                    INNER JOIN texts_with_all_hashes AS twa
                        ON twa.text_id = ht.text_id
            ),

            -- Define sequence groups using ROW_NUMBER().
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

            -- Build sequence strings for every sequence using delimiter:
            sequences_by_text AS (
                SELECT
                    text_id,
                    STRING_AGG(
                        CAST(hash_id AS VARCHAR), ',' ORDER BY position
                    ) AS sequence
                FROM sequences
                GROUP BY
                    text_id,
                    sequence_id
                HAVING COUNT(hash_id) = {str(len(hash_id_list))}
            )

        -- Match sequences containing the search pattern:
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
