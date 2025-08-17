#!/usr/bin/env python3

# PIP module:
import pyarrow as pa


def twiga_text_writer(
    duckdb_text_connection: object,
    bins_total:             int,
    batch_table:            pa.Table
) -> True:
    batch_table = duckdb_text_connection.sql(
        f'''
            SELECT
                *,
                ((text_id % {str(bins_total)}) + 1) AS bin
            FROM batch_table
        '''
    ).arrow()

    for bin_number in range(1, bins_total + 1):
        duckdb_text_connection.execute(
            f'''
                INSERT INTO texts_bin_{str(bin_number)}
                SELECT * EXCLUDE (bin),
                FROM batch_table
                WHERE bin = {str(bin_number)}
            '''
        )

    return True


def twiga_text_reader(
    duckdb_text_connection: object,
    bins_total:             int,
    text_id_table:          pa.Table
) -> None | pa.Table:
    text_id_list = text_id_table.column('text_id').to_pylist()

    bin_dict = {}

    for text_id in text_id_list:
        bin_number = (text_id % bins_total) + 1

        if bin_number not in bin_dict:
            bin_dict[bin_number] = []

        bin_dict[bin_number].append(text_id)

    text_query   = ''
    query_number = 0

    for bin_number, bin_text_id_list in bin_dict.items():
        query_number += 1

        text_id_string = ', '.join(map(str, set(bin_text_id_list)))

        text_query += f'''
            SELECT *
            FROM texts_bin_{str(bin_number)}
            WHERE text_id IN ({text_id_string})
        '''

        if query_number < len(bin_dict):
            text_query += 'UNION'

    text_table = duckdb_text_connection.sql(text_query).arrow()

    search_result_table = duckdb_text_connection.query(
        '''
            SELECT
                tit.matching_words,
                tit.words_total AS total_words,
                tit.matching_words_frequency,
                tt.* EXCLUDE (text),
                tt.text
            FROM
                text_id_table AS tit
                LEFT JOIN text_table AS tt
                    ON tt.text_id = tit.text_id
            ORDER BY tit.matching_words_frequency DESC
        '''
    ).arrow()

    if search_result_table.num_rows == 0:
        search_result_table = None

    return search_result_table
