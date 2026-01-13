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

    hash_query   = ''
    query_number = 0

    for bin_number, hash_list in bin_dict.items():
        query_number += 1

        hash_list_string = "'" + "', '".join(map(str, set(hash_list))) + "'"

        hash_query += f"""
            SELECT
                hash_id,
                text_id,
                positions
            FROM bin_{str(bin_number)}_hash_index
            WHERE hash_id IN (
                SELECT hash_id
                FROM bin_{str(bin_number)}_hash_dict
                WHERE hash IN ({hash_list_string})
            )
        """

        if query_number < len(bin_dict):
            hash_query += 'UNION'

    hash_table = duckdb_connection.sql(hash_query).fetch_arrow_table()

    hash_id_query = """
        SELECT hash_id
        FROM hash_table
        GROUP BY hash_id
    """

    hash_id_table = duckdb_connection.sql(hash_id_query).fetch_arrow_table()

    try:
        hash_id_list = hash_id_table['hash_id'].to_pylist()
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
        request_sequence_string = '#'.join(map(str, hash_id_list))

        search_query = f"""
            WITH
                full_hash_set AS (
                    SELECT text_id
                    FROM hash_table
                    GROUP BY text_id
                    HAVING COUNT(DISTINCT(hash_id)) = {str(len(set(hash_id_list)))}
                ),

                positions_by_hash AS (
                    SELECT
                        ht.hash_id,
                        ht.text_id,
                        UNNEST(ht.positions) AS position
                    FROM
                        hash_table AS ht
                        INNER JOIN full_hash_set AS fhs
                            ON fhs.text_id = ht.text_id
                ),

                positions_by_text AS (
                    SELECT
                        text_id,
                        hash_id,
                        position
                    FROM positions_by_hash
                    GROUP BY
                        text_id,
                        hash_id,
                        position
                ),

                distances AS (
                    SELECT
                        text_id,
                        hash_id,
                        position,
                        LEAD(position) OVER (
                            PARTITION BY text_id
                            ORDER BY position ASC
                            ROWS BETWEEN CURRENT ROW and 1 FOLLOWING
                        ) - position AS lead,
                    FROM positions_by_text
                ),

                borders AS (
                    SELECT
                        text_id,
                        position,
                        CASE
                            WHEN lead > 1
                            THEN CONCAT(CAST(hash_id AS VARCHAR), '##')
                            ELSE CAST(hash_id AS VARCHAR)
                        END AS hash_id_string
                    FROM distances
                ),

                texts AS (
                    SELECT
                        text_id,
                        STRING_AGG(hash_id_string, '#' ORDER BY position) AS text
                    FROM borders
                    GROUP BY text_id
                ),

                sequences AS (
                    SELECT
                        text_id,
                        UNNEST(STRING_SPLIT(text, '###')) AS sequence
                    FROM texts
                )

            SELECT
                s.text_id,
                COUNT(s.sequence) * {str(len(hash_id_list))} AS matching_words,
                FIRST(wc.words_total) AS words_total,
                ROUND(
                    (matching_words / FIRST(wc.words_total)), 5
                ) AS term_frequency
            FROM
                sequences AS s
                LEFT JOIN word_counts AS wc
                    ON wc.text_id = s.text_id
            WHERE
                s.sequence = '{request_sequence_string}'
                OR s.sequence LIKE '%{request_sequence_string}'
                OR s.sequence LIKE '{request_sequence_string}%'
            GROUP BY s.text_id
            ORDER BY term_frequency DESC
            {limit_clause}
        """

    result_table = duckdb_connection.sql(search_query).fetch_arrow_table()

    if result_table.num_rows == 0:
        result_table = None

    return result_table
