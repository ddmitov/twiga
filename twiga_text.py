#!/usr/bin/env python3

# PIP module:
import pyarrow as pa


def twiga_text_writer(
    duckdb_connection: object,
    text_bins:         int,
    batch_table:       pa.Table
) -> bool:
    """Writes texts to sharded bins based on text_id modulo distribution."""

    # Partition rows by bin:
    text_ids = batch_table.column('text_id').to_pylist()

    bin_indices = {}

    for index, text_id in enumerate(text_ids):
        bin_number = (text_id % text_bins) + 1

        if bin_number not in bin_indices:
            bin_indices[bin_number] = []

        bin_indices[bin_number].append(index)

    duckdb_connection.execute("BEGIN TRANSACTION")

    # Insert each pre-partitioned subset directly
    for bin_number, indices in bin_indices.items():
        partition = batch_table.take(indices)

        duckdb_connection.execute(
            f"INSERT INTO text.texts_bin_{bin_number} SELECT * FROM partition"
        )

    duckdb_connection.execute("COMMIT")

    return True


def twiga_text_reader(
    duckdb_connection: object,
    text_bins:         int,
    text_id_table:     pa.Table
) -> None | pa.Table:
    """Retrieves texts from sharded bins and joins them with search results."""

    text_id_list = text_id_table.column('text_id').to_pylist()

    bin_dict = {}

    for text_id in text_id_list:
        bin_number = (text_id % text_bins) + 1

        if bin_number not in bin_dict:
            bin_dict[bin_number] = []

        bin_dict[bin_number].append(text_id)

    text_tables = []

    for bin_number, bin_text_id_list in bin_dict.items():
        for text_id in bin_text_id_list:

            text_query = f"""
                SELECT *
                FROM texts_bin_{str(bin_number)}
                WHERE text_id = {text_id}
            """

            text_table = duckdb_connection.sql(text_query).fetch_arrow_table()

            text_tables.append(text_table)

    final_text_table = pa.concat_tables(text_tables)

    search_result_table = duckdb_connection.query(
        """
            SELECT
                tit.bm25_score,
                tit.matching_words,
                tt.* EXCLUDE (text),
                tt.text
            FROM
                text_id_table AS tit
                LEFT JOIN final_text_table AS tt
                    ON tt.text_id = tit.text_id
            ORDER BY tit.bm25_score DESC
        """
    ).fetch_arrow_table()

    if search_result_table.num_rows == 0:
        search_result_table = None

    return search_result_table
