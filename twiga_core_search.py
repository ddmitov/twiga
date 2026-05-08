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
    Look up hash entries using pairwise INTERSECT CTE statements.

    Creates INTERSECT CTEs for pairs of hashes ordered from highest to lowest
    document count, then intersects all pair results together with any odd hash.
    """

    if len(request_hash_list) == 0:
        return None, None

    # Get unique hashes only:
    hash_set = set(request_hash_list)

    # Create a mapping of each hash to its index in the original request:
    mapping_dict = {
        hash_item: index for index, hash_item in enumerate(hash_set)
    }

    # Get document counts for all hashes from the hash_metadata table:
    hash_doc_count_query = f"""
        SELECT
            hash,
            document_count
        FROM hash_metadata
        WHERE hash IN (
            {','.join(repr(hash_item) for hash_item in hash_set)}
        )
        ORDER BY document_count ASC
    """

    hash_doc_counts = duckdb_connection.execute(
        hash_doc_count_query
    ).fetchall()

    # Create a dict of hash -> document_count:
    hash_counts_dict = {row[0]: row[1] for row in hash_doc_counts}

    # Sort hashes by document count (lowest first):
    sorted_hashes = sorted(
        hash_set,
        key=lambda hash_item: hash_counts_dict.get(hash_item, float('inf'))
    )

    # Reverse to process pairs from highest to lowest document count:
    sorted_hashes_reversed = sorted_hashes[::-1]

    # Build pairwise INTERSECT CTEs:
    cte_clauses = []
    pair_index = 0
    odd_hash_cte = None

    # Process hashes in pairs from highest to lowest:
    for hash_index in range(0, len(sorted_hashes_reversed), 2):
        hash_item_1 = sorted_hashes_reversed[hash_index]
        bin_number_1 = (int(hash_item_1, 16) % index_bins) + 1

        if hash_index + 1 < len(sorted_hashes_reversed):
            # We have a pair:
            hash_item_2 = sorted_hashes_reversed[hash_index + 1]
            bin_number_2 = (int(hash_item_2, 16) % index_bins) + 1

            pair_cte = f"""pair_{pair_index} AS (
                SELECT text_id
                FROM bin_{bin_number_1}
                WHERE hash = '{hash_item_1}'
                INTERSECT
                SELECT text_id
                FROM bin_{bin_number_2}
                WHERE hash = '{hash_item_2}'
            )"""
            cte_clauses.append(pair_cte)
            pair_index += 1
        else:
            # Odd hash - no pair:
            odd_hash_cte = f"""odd_hash AS (
                SELECT text_id
                FROM bin_{bin_number_1}
                WHERE hash = '{hash_item_1}'
            )"""
            cte_clauses.append(odd_hash_cte)

    # Build the final INTERSECT combining all pairs and odd hash:
    if pair_index > 0:
        # We have at least one pair
        intersect_parts = [
            f"SELECT text_id FROM pair_{hash_index}"
            for hash_index in range(pair_index)
        ]

        if odd_hash_cte is not None:
            intersect_parts.append("SELECT text_id FROM odd_hash")

        final_intersect = " INTERSECT ".join(intersect_parts)
        cte_clauses.append(f"final_result AS ({final_intersect})")
    else:
        # Only odd hash (single hash case):
        cte_clauses.append("final_result AS (SELECT text_id FROM odd_hash)")

    # Build the full query with CTEs:
    cte_string = ", ".join(cte_clauses)

    # Get the final list of text_ids containing all hashes:
    text_ids_query = f"""
        WITH {cte_string}
            SELECT DISTINCT text_id
            FROM final_result
    """

    try:
        text_ids_result = duckdb_connection.execute(text_ids_query).fetchall()
        text_ids_list = [row[0] for row in text_ids_result]

        if len(text_ids_list) == 0:
            return None, None

        text_ids_str = ','.join(str(text_id) for text_id in text_ids_list)

    except Exception as e:
        return None, None

    # Now extract hash positions for all hashes in the filtered text_ids:
    hash_positions_query_parts = []

    for hash_item in sorted_hashes:
        bin_number = (int(hash_item, 16) % index_bins) + 1
        hash_positions_query_parts.append(f"""
            SELECT
                {mapping_dict[hash_item]} AS hash_id,
                text_id,
                positions
            FROM bin_{bin_number}
            WHERE
                hash = '{hash_item}'
                AND text_id IN ({text_ids_str})
        """)

    # Combine all queries with UNION:
    hash_positions_query = ' UNION '.join(hash_positions_query_parts)

    try:
        hash_table = duckdb_connection.sql(
            hash_positions_query
        ).fetch_arrow_table()

        if hash_table.num_rows == 0:
            return None, None

    except Exception as e:
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
            -- Count the number of positions:
            positions AS (
                SELECT
                    hash_id,
                    text_id,
                    LENGTH(positions) AS positions_number
                FROM hash_table
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
            -- Flatten position arrays into individual rows:
            positions AS (
                SELECT
                    hash_id,
                    text_id,
                    UNNEST(positions) AS position
                FROM hash_table
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
