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
                hi.text_id,
                hi.positions
            FROM
                bin_{str(bin_number)}_hash_index AS hi
                INNER JOIN mapping_table AS mt
                    ON mt.hash_id = hi.hash_id
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
        request_sequence_string = ''.join(map(str, hash_id_list))

        search_query = f"""
            WITH
                full_hash_set_texts AS (
                    SELECT text_id
                    FROM hash_table
                    GROUP BY text_id
                    HAVING COUNT(DISTINCT(hash_id)) = {str(len(set(hash_id_list)))}
                ),

                positions AS (
                    SELECT
                        ht.hash_id,
                        ht.text_id,
                        UNNEST(ht.positions) AS position
                    FROM
                        hash_table AS ht
                        INNER JOIN full_hash_set_texts AS fhst
                            ON fhst.text_id = ht.text_id
                ),

                sequences AS (
                    SELECT
                        text_id,
                        hash_id,
                        position,
                        DENSE_RANK() OVER (
                            PARTITION BY text_id
                            ORDER BY position ASC
                        ) - position AS sequence_id
                    FROM positions
                ),

                sequences_by_text AS (
                    SELECT
                        text_id,
                        STRING_AGG(hash_id, '' ORDER BY position) AS sequence
                    FROM sequences
                    GROUP BY
                        text_id,
                        sequence_id
                )

            SELECT
                sbt.text_id,
                COUNT(sbt.sequence) * {str(len(hash_id_list))} AS matching_words,
                FIRST(wc.words_total) AS words_total,
                ROUND(
                    (matching_words / FIRST(wc.words_total)), 5
                ) AS term_frequency
            FROM
                sequences_by_text AS sbt
                LEFT JOIN word_counts AS wc
                    ON wc.text_id = sbt.text_id
            WHERE
                sbt.sequence = '{request_sequence_string}'
                OR sbt.sequence LIKE '%{request_sequence_string}'
                OR sbt.sequence LIKE '{request_sequence_string}%'
            GROUP BY sbt.text_id
            ORDER BY term_frequency DESC
            {limit_clause}
        """

    result_table = duckdb_connection.sql(search_query).fetch_arrow_table()

    if result_table.num_rows == 0:
        result_table = None

    return result_table
