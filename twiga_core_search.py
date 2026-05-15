#!/usr/bin/env python3

# Core modules:
import hashlib

# PIP modules:
import pyarrow    as     pa
from   tokenizers import normalizers
from   tokenizers import pre_tokenizers


def twiga_request_hasher(search_request: str) -> list:
    """Normalizes, tokenizes, and hashes a search request into word hashes."""

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
    Looks up hash entries using INTERSECT statement.

    Creates INTERSECT statement of hashes
    ordered from lowest to highest document count
    in order to extract only text_ids where all hashes match
    in the minimal amount of time.
    """

    if len(request_hash_list) == 0:
        return None, None

    # Get unique hashes only:
    hash_set = set(request_hash_list)

    # Get all unique hashes in the search request, ordered by document count:
    hashes_query = f"""
        SELECT hash
        FROM hash_metadata
        WHERE hash IN (
            {','.join(repr(hash_item) for hash_item in hash_set)}
        )
        ORDER BY document_count ASC
    """

    hashes_result = duckdb_connection.execute(hashes_query).fetchall()

    # Create a hash list - alredy ordered by document count from the query:
    hashes_list = [row[0] for row in hashes_result]

    # Build INTERSECT statement:
    intersect_sql = ''
    hash_index    = 0

    for hash_item in hashes_list:
        hash_index += 1
        bin_number = (int(hash_item, 16) % index_bins) + 1

        # Formatting of the SQL queries is adapted for readability if printed.
        if hash_index < len(hashes_list):
            select_statement = f"""
            SELECT text_id
            FROM bin_{bin_number}
            WHERE hash = '{hash_item}'
            INTERSECT"""

            intersect_sql += select_statement
        else:
            # Final hash:
            select_statement = f"""
            SELECT text_id
            FROM bin_{bin_number}
            WHERE hash = '{hash_item}'
            """

            intersect_sql += select_statement

    # print(intersect_sql, flush=True)

    try:
        text_ids_table = duckdb_connection.execute(
            intersect_sql
        ).fetch_arrow_table()

        if text_ids_table.num_rows == 0:
            return None, None

    except Exception as e:
        return None, None

    # Now extract hash positions for all hashes in the filtered text_ids:
    hash_positions_query_parts = []

    for hash_item in hashes_list:
        bin_number = (int(hash_item, 16) % index_bins) + 1

        hash_positions_query_parts.append(f"""
            SELECT
                b.hash,
                b.text_id,
                b.positions
            FROM
                bin_{bin_number} AS b
                INNER JOIN text_ids_table AS tit
                    ON tit.text_id = b.text_id
            WHERE b.hash = '{hash_item}'
        """)

    # Combine all SELECT statements with UNION:
    # Formatting of the SQL query is adapted for readability if printed.
    hash_positions_query = '    UNION '.join(hash_positions_query_parts)

    # print(hash_positions_query, flush=True)

    hash_table = None

    try:
        hash_table = duckdb_connection.sql(
            hash_positions_query
        ).fetch_arrow_table()

        if hash_table.num_rows == 0:
            return None, None

    except Exception as e:
        return None, None

    return request_hash_list, hash_table


def twiga_single_word_searcher(
    duckdb_connection: object,
    index_bins:        int,
    request_hash:      str,
    results_number:    int,
    bm25_k1:           float = 1.5,
    bm25_b:            float = 0.75
) -> None | pa.Table:
    """
    Finds texts containing a single word with BM25 scoring.

    Args:
        results_number: Maximum results to return.
        bm25_k1: BM25 tuning parameter (default 1.5).
        bm25_b: BM25 tuning parameter (default 0.75).
    """

    bin_number = (int(request_hash, 16) % index_bins) + 1

    search_query = f"""
        WITH
            -- Get the average document length and the total document count:
            stats AS (
                SELECT
                    AVG(words_total) AS average_document_length,
                    COUNT(*) AS total_documents
                FROM word_counts
            )

        SELECT
            hash_index_table.text_id,
            ROUND(
                LN(
                    (
                        FIRST(stats.total_documents)
                        -
                        FIRST(hash_metadata_table.document_count)
                        +
                        0.5
                    )
                    /
                    (
                        FIRST(hash_metadata_table.document_count)
                        +
                        0.5
                    )
                )
                *
                (
                    LEN(FIRST(hash_index_table.positions))
                    *
                    (
                        {bm25_k1}
                        +
                        1
                    )
                )
                /
                (
                    LEN(FIRST(hash_index_table.positions))
                    +
                    {bm25_k1}
                    *
                    (
                        1
                        -
                        {bm25_b}
                        +
                        {bm25_b}
                        *
                        FIRST(word_counts_table.words_total)
                        /
                        FIRST(stats.average_document_length)
                    )
                ),
                3
            ) AS bm25_score,
            LEN(FIRST(hash_index_table.positions)) AS matching_words,
        FROM
            bin_{bin_number} AS hash_index_table
            LEFT JOIN word_counts AS word_counts_table
                ON word_counts_table.text_id = hash_index_table.text_id
            LEFT JOIN hash_metadata AS hash_metadata_table
                ON hash_metadata_table.hash = '{request_hash}'
            CROSS JOIN stats
        WHERE hash_index_table.hash = '{request_hash}'
        GROUP BY hash_index_table.text_id
        ORDER BY bm25_score DESC
        LIMIT {str(results_number)}
    """

    result_table = duckdb_connection.sql(search_query).fetch_arrow_table()

    if result_table.num_rows == 0:
        result_table = None

    return result_table


def twiga_any_position_searcher(
    duckdb_connection: object,
    hash_table:        pa.Table,
    request_hash_list: list,
    results_number:    int,
    bm25_k1:           float = 1.5,
    bm25_b:            float = 0.75
) -> None | pa.Table:
    """
    Finds texts containing words in any order with BM25 scoring.

    Args:
        results_number: Maximum results to return.
        bm25_k1: BM25 tuning parameter (default 1.5).
        bm25_b:  BM25 tuning parameter (default 0.75).
    """

    search_query = f"""
        WITH
            -- Get the average document length and the total document count:
            stats AS (
                SELECT
                    AVG(words_total) AS average_document_length,
                    COUNT(*) AS total_documents
                FROM word_counts
            ),

            -- Get the term frequency for each hash in each text:
            positions AS (
                SELECT
                    hash,
                    text_id,
                    LENGTH(positions) AS term_frequency
                FROM hash_table
            ),

            -- Calculate the BM25 scores for each term in each text:
            bm25_scores AS (
                SELECT
                    positions.text_id,
                    positions.hash,
                    ROUND(
                        LN(
                            (
                                stats.total_documents
                                -
                                hash_metadata_table.document_count
                                +
                                0.5
                            )
                            /
                            (
                                hash_metadata_table.document_count
                                +
                                0.5
                            )
                        )
                        *
                        (
                            positions.term_frequency
                            *
                            (
                                {bm25_k1}
                                +
                                1
                            )
                        )
                        /
                        (
                            positions.term_frequency + {bm25_k1}
                            *
                            (
                                1
                                -
                                {bm25_b}
                                +
                                {bm25_b}
                                *
                                word_counts_table.words_total
                                /
                                stats.average_document_length
                            )
                        ),
                        3
                    ) AS bm25_term_score
                FROM
                    positions
                    LEFT JOIN hash_metadata AS hash_metadata_table
                        ON hash_metadata_table.hash = positions.hash
                    LEFT JOIN word_counts AS word_counts_table
                        ON word_counts_table.text_id = positions.text_id
                    CROSS JOIN stats
            )
        SELECT
            bm25_scores.text_id,
            SUM(bm25_term_score) AS bm25_score,
            COUNT(DISTINCT hash) AS matching_words
        FROM
            bm25_scores
            LEFT JOIN word_counts AS word_counts_table
                ON word_counts_table.text_id = bm25_scores.text_id
        GROUP BY bm25_scores.text_id
        ORDER BY bm25_score DESC
        LIMIT {str(results_number)}
    """

    result_table = duckdb_connection.sql(search_query).fetch_arrow_table()

    if result_table.num_rows == 0:
        result_table = None

    return result_table


def twiga_exact_phrase_searcher(
    duckdb_connection: object,
    hash_table:        pa.Table,
    request_hash_list: list,
    results_number:    int,
    bm25_k1:           float = 1.5,
    bm25_b:            float = 0.75
) -> None | pa.Table:
    """
    Finds texts containing consecutive word sequences with BM25 scoring.

    Args:
        results_number: Maximum results to return.
        bm25_k1: BM25 tuning parameter (default 1.5).
        bm25_b:  BM25 tuning parameter (default 0.75).
    """

    # Build the VALUES rows for phrase_pattern from request_hash_list.
    # Each row is of phrase_offset and expected_hash.
    phrase_pattern_values = ', '.join(
        f"({offset}, '{hash_item}')"
        for offset, hash_item in enumerate(request_hash_list)
    )

    search_query = f"""
        WITH
            -- Get the average document length and the total document count:
            stats AS (
                SELECT
                    AVG(words_total) AS average_document_length,
                    COUNT(*) AS total_documents
                FROM word_counts
            ),

            -- Flatten all position arrays into individual rows:
            positions AS (
                SELECT
                    hash,
                    text_id,
                    UNNEST(positions) AS position
                FROM hash_table
            ),

            -- Compose the phrase pattern as a table,
            -- one row per (phrase_offset, expected_hash):
            phrase_pattern (phrase_offset, expected_hash) AS (
                VALUES {phrase_pattern_values}
            ),
 
            -- Self-join each indexed (text_id, hash, position)
            -- against every phrase slot it could occupy,
            -- normalising position into sequence_id:
            sequences AS (
                SELECT
                    pos.text_id,
                    pos.position - pp.phrase_offset AS sequence_id
                FROM positions AS pos
                JOIN phrase_pattern AS pp
                    ON pp.expected_hash = pos.hash
            ),
 
            -- A real phrase match contributes one row per phrase slot,
            -- all sharing the same sequence_id.
            sequences_by_text AS (
                SELECT
                    text_id,
                    sequence_id
                FROM sequences
                GROUP BY
                    text_id,
                    sequence_id
                HAVING COUNT(*) = {len(request_hash_list)}
            ),

            -- Get the minimum document frequency for the terms in the phrase:
            phrase_document_frequency AS (
                SELECT MIN(document_count) AS value
                FROM hash_metadata
                WHERE hash IN (SELECT DISTINCT hash FROM hash_table)
            )

        -- Match all sequences containing the search pattern:
        SELECT
            sequences_by_text.text_id,
            ROUND(
                LN(
                    (
                        FIRST(stats.total_documents)
                        -
                        FIRST(phrase_document_frequency.value)
                        +
                        0.5
                    )
                    /
                    (
                        FIRST(phrase_document_frequency.value)
                        +
                        0.5
                    )
                )
                *
                (
                    COUNT(sequences_by_text.sequence_id)
                    *
                    (
                        {bm25_k1}
                        +
                        1
                    )
                )
                /
                (
                    COUNT(sequences_by_text.sequence_id)
                    +
                    {bm25_k1}
                    *
                    (
                        1
                        -
                        {bm25_b}
                        +
                        {bm25_b}
                        *
                        FIRST(word_counts_table.words_total)
                        /
                        FIRST(stats.average_document_length)
                    )
                ),
                3
            ) AS bm25_score,
            COUNT(
                sequences_by_text.sequence_id)
                *
                {str(len(request_hash_list))}
            AS matching_words
        FROM
            sequences_by_text
            LEFT JOIN word_counts AS word_counts_table
                ON word_counts_table.text_id = sequences_by_text.text_id
            CROSS JOIN stats
            CROSS JOIN phrase_document_frequency
        GROUP BY sequences_by_text.text_id
        ORDER BY bm25_score DESC
        LIMIT {str(results_number)}
    """

    result_table = duckdb_connection.sql(search_query).fetch_arrow_table()

    if result_table.num_rows == 0:
        result_table = None

    return result_table
