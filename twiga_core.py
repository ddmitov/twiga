#!/usr/bin/env python3

# Core module:
import hashlib

# PIP modules:
import duckdb
import pyarrow    as     pa
from   tokenizers import normalizers
from   tokenizers import pre_tokenizers


def twiga_index_database_creator(
    index_pathname: str,
    bins_total:     int
) -> object:
    duckdb_index_connection = duckdb.connect(index_pathname)

    # Create a single sequence for all hash dictionary tables:
    duckdb_index_connection.execute('CREATE SEQUENCE hash_id_sequence START 1')

    duckdb_index_connection.execute(
        f'''
            CREATE TABLE word_counts (
                text_id     INTEGER,
                words_total INTEGER
            )
        '''
    )

    for bin_number in range(1, bins_total + 1):
        duckdb_index_connection.execute(
            f'''
                CREATE TABLE bin_{str(bin_number)}_hash_dict (
                    hash_id INTEGER,
                    hash    VARCHAR
                )
            '''
        )

        duckdb_index_connection.execute(
            f'''
                CREATE TABLE bin_{str(bin_number)}_hash_index (
                    hash_id   INTEGER,
                    text_id   INTEGER,
                    positions INTEGER[]
                )
            '''
        )

    return duckdb_index_connection


def twiga_index_writer(
    duckdb_index_connection: object,
    bins_total:              int,
    text_table:              pa.Table,
    stopword_set:            set
) -> True:
    # Extract the 'text_id' and 'text' Arrow table columns as lists:
    text_id_list = text_table.column('text_id').to_pylist()
    text_list    = text_table.column('text').to_pylist()

    normalizer = normalizers.Sequence(
        [
            normalizers.NFD(),          # Decompose Unicode characters
            normalizers.StripAccents(), # Remove accents after decomposition
            normalizers.Lowercase()     # Convert to lowercase
        ]
    )

    normalized_texts = [
        normalizer.normalize_str(text)
        for text in text_list
    ]

    pre_tokenizer = pre_tokenizers.Sequence(
        [
            pre_tokenizers.Whitespace(),
            pre_tokenizers.Punctuation(behavior='removed'),
            pre_tokenizers.Digits(individual_digits=False)
        ]
    )

    pre_tokenized_texts = [
        pre_tokenizer.pre_tokenize_str(text)
        for text in normalized_texts
    ]

    words_batch = [
        [
            word_tuple[0]
            for word_tuple in pre_tokenized_text
        ]
        for pre_tokenized_text in pre_tokenized_texts
    ]

    # List of dictionaries:
    hashes      = []
    word_counts = []

    # Iterate all texts in a batch:
    for text_id, word_list in zip(text_id_list, words_batch):
        text_hash_list = [
            hashlib.blake2b(word.encode(), digest_size=32).hexdigest()
            for word in word_list
            if word not in stopword_set
        ]

        words_count_record = {}

        words_count_record['text_id']     = int(text_id)
        words_count_record['words_total'] = len(text_hash_list)

        word_counts.append(words_count_record)

        # Dictionary of lists:
        positions = {}

        for position, hashed_word in enumerate(text_hash_list):
            if hashed_word not in positions:
                positions[hashed_word] = []

            positions[hashed_word].append(position)

        for hashed_word in text_hash_list:
            hashed_word_item = {}

            bin_number = (int(hashed_word, 16) % bins_total) + 1

            hashed_word_item['bin']         = int(bin_number)
            hashed_word_item['hash']        = str(hashed_word)
            hashed_word_item['text_id']     = int(text_id)
            hashed_word_item['positions']   = positions[hashed_word]

            hashes.append(hashed_word_item)

    batch_hash_table = pa.Table.from_pylist(hashes)

    batch_hash_table = duckdb_index_connection.sql(
        '''
            SELECT *
            FROM batch_hash_table
            ORDER BY
                bin,
                hash
        '''
    ).arrow()

    word_counts_table = pa.Table.from_pylist(word_counts)

    duckdb_index_connection.execute(
        f'''
            INSERT INTO word_counts
            SELECT *
            FROM word_counts_table
        '''
    )

    for bin_number in range(1, bins_total + 1):
        batch_hashes_table = duckdb_index_connection.query(
            f'''
                SELECT DISTINCT(hash) AS hash
                FROM batch_hash_table
                WHERE bin = {str(bin_number)}
            '''
        ).arrow()

        known_hashes_table = duckdb_index_connection.sql(
            f"""
                SELECT
                    hd.hash_id AS hash_id,
                    hd.hash AS hash
                FROM
                    bin_{str(bin_number)}_hash_dict AS hd
                    INNER JOIN batch_hashes_table AS uhs
                        ON uhs.hash = hd.hash
            """
        ).arrow()

        unknown_hashes_table = duckdb_index_connection.sql(
            f"""
                SELECT hash
                FROM batch_hashes_table
                EXCEPT
                SELECT hash
                FROM known_hashes_table
            """
        ).arrow()

        duckdb_index_connection.execute(
            f'''
                INSERT INTO bin_{str(bin_number)}_hash_dict
                SELECT
                    NEXTVAL('hash_id_sequence') AS hash_id,
                    hash
                FROM unknown_hashes_table
            '''
        )

        hash_mapping_table = duckdb_index_connection.sql(
            f"""
                SELECT
                    hash_id,
                    hash
                FROM bin_{str(bin_number)}_hash_dict
            """
        ).arrow()

        duckdb_index_connection.execute(
            f"""
                INSERT INTO bin_{str(bin_number)}_hash_index
                SELECT
                    hmt.hash_id AS hash_id,
                    bht.text_id AS text_id,
                    bht.positions AS positions
                FROM
                    batch_hash_table AS bht
                    LEFT JOIN hash_mapping_table AS hmt
                        ON hmt.hash = bht.hash
                WHERE bin = {str(bin_number)}
            """
        )

    return True


def twiga_request_hasher(
    stopword_set:   set,
    search_request: str
) -> list:
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
    duckdb_index_connection: object,
    bins_total:              int,
    request_hash_list:       list
) -> tuple[None, None] | tuple[list, pa.Table]:
    if len(request_hash_list) == 0:
        return None, None

    hash_set = set(request_hash_list)
    bin_dict = {}

    for hash_item in hash_set:
        bin_number = (int(hash_item, 16) % bins_total) + 1

        if bin_number not in bin_dict:
            bin_dict[bin_number] = []

        bin_dict[bin_number].append(hash_item)

    mapping_query  = ''
    hash_query     = ''
    query_number   = 0

    for bin_number, hash_list in bin_dict.items():
        query_number += 1

        hash_list_string = "'" + "', '".join(map(str, set(hash_list))) + "'"

        mapping_query += f'''
            SELECT
                hash,
                hash_id,
            FROM bin_{str(bin_number)}_hash_dict
            WHERE hash IN ({hash_list_string})
        '''

        hash_query += f'''
            SELECT
                hi.hash_id,
                hi.text_id AS text_id,
                hi.positions AS positions
            FROM
                bin_{str(bin_number)}_hash_index AS hi
                INNER JOIN mapping_table AS mt
                    ON mt.hash_id = hi.hash_id
            WHERE mt.hash IN ({hash_list_string})
        '''

        if query_number < len(bin_dict):
            mapping_query += 'UNION'
            hash_query    += 'UNION'


    # The order of execution of the SQL queries is important here:
    mapping_table = duckdb_index_connection.sql(mapping_query).arrow()
    hash_table = duckdb_index_connection.sql(hash_query).arrow()

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


def twiga_single_word_searcher(
    duckdb_index_connection: object,
    hash_table:              pa.Table,
    results_number:          int
) -> None | pa.Table:
    results_number_string = str(results_number)

    search_query = f'''
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
        LIMIT {results_number_string}
    '''

    result_table = duckdb_index_connection.sql(search_query).arrow()

    if result_table.num_rows == 0:
        result_table = None

    return result_table


def twiga_multiple_words_searcher(
    duckdb_index_connection: object,
    hash_table:              pa.Table,
    hash_id_list:            list,
    results_number:          int
) -> None | pa.Table:
    request_sequence_string = '#'.join(map(str, hash_id_list))

    search_query = f'''
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
            ) AS matching_words_frequency
        FROM
            sequences AS s
            LEFT JOIN word_counts AS wc
                ON wc.text_id = s.text_id
        WHERE
            s.sequence = '{request_sequence_string}'
            OR s.sequence LIKE '%{request_sequence_string}'
            OR s.sequence LIKE '{request_sequence_string}%'
        GROUP BY s.text_id
        ORDER BY matching_words_frequency DESC
        LIMIT {str(results_number)}
    '''

    result_table = duckdb_index_connection.sql(search_query).arrow()

    if result_table.num_rows == 0:
        result_table = None

    return result_table
