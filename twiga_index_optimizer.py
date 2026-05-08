#!/usr/bin/env python3

"""
Optimize the hash index by reordering bin tables for better cache locality.

This script:
1. Calculates hash frequency for every hash in the index
2. Records hash frequency in a hash_metadata table
3. Reorders bin tables by hash and text_id columns for cache locality
"""

# Core modules:
import argparse
from   datetime import datetime
from   datetime import timedelta
import logging
import os
from   pathlib  import Path
from   time     import time

# PIP modules:
from   dotenv import find_dotenv
from   dotenv import load_dotenv
import duckdb
import pyarrow as    pa

# Start the optimization process:
# docker run --rm -it --user $(id -u):$(id -g) -v $PWD:/app \
# twiga-demo python /app/twiga_index_optimizer.py /app/data/twiga_index.duckdb

load_dotenv(find_dotenv())


def logger_starter() -> logging.Logger:
    """Initialize and return a logger for the index optimization process."""

    start_datetime_string = (datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))

    os.makedirs('/app/data/logs', exist_ok=True)

    logging.basicConfig(
        level    = logging.INFO,
        datefmt  = '%Y-%m-%d %H:%M:%S',
        format   = '%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
        filename = f'/app/data/logs/twiga_{start_datetime_string}.log',
        filemode = 'a'
    )

    logger = logging.getLogger()

    return logger


def documents_counter(duckdb_connection: object) -> int:
    """
    Counts the total number of documents in the index.

    Args:
        duckdb_connection: DuckDB connection object

    Returns:
        Total number of documents (rows in word_counts table)
    """

    cursor = duckdb_connection.cursor()

    result = cursor.execute(
        "SELECT COUNT(*) FROM index.word_counts"
    ).fetchone()

    cursor.close()

    return result[0]


def hash_frequency_calculator(
    duckdb_connection: object,
    bin_tables:        int
) -> dict:
    """
    Calculates the frequency of each hash (how many documents it appears in).

    Args:
        duckdb_connection: DuckDB connection object
        bin_tables: Number of bin tables

    Returns:
        PyArrow table with columns: hash (string), document_count (integer)
    """

    # Process each bin table separately to avoid expression depth limits.
    # Accumulate hash frequencies across all bin tables:
    frequency_table_list = []

    for bin_index in range(1, bin_tables + 1):
        bin_frequency_table = duckdb_connection.sql(f"""
                SELECT
                    hash,
                    COUNT(text_id) as document_count
                FROM index.bin_{bin_index}
                GROUP BY hash
            """
        ).fetch_arrow_table()

        # Accumulate frequencies from this bin table:
        frequency_table_list.append(bin_frequency_table)

    hash_frequency_table_raw = pa.concat_tables(frequency_table_list)

    hash_frequency_table = duckdb_connection.sql(f"""
            SELECT
                hash,
                SUM(document_count) as document_count
            FROM hash_frequency_table_raw
            GROUP BY hash
            ORDER BY hash ASC
        """
    ).fetch_arrow_table()

    return hash_frequency_table


def hash_metadata_table_creator(
    duckdb_connection:    object,
    hash_frequency_table: pa.Table
) -> int:
    """
    Creates a metadata table recording frequency for every hash.

    Args:
        duckdb_connection: DuckDB connection object
        hash_frequency_table: PyArrow table with hash frequencies

    Returns:
        Number of hashes recorded in metadata table
    """

    cursor = duckdb_connection.cursor()

    # Drop table if it exists:
    cursor.execute("DROP TABLE IF EXISTS index.hash_metadata")

    # Create the hash metadata table:
    cursor.execute(
        """
            CREATE OR REPLACE TABLE index.hash_metadata (
                hash           VARCHAR PRIMARY KEY,
                document_count INTEGER
            )
        """
    )

    cursor.execute("BEGIN TRANSACTION")

    # Insert metadata:
    cursor.execute(
        """
            INSERT INTO index.hash_metadata
            SELECT * FROM hash_frequency_table
        """

    )

    cursor.execute("COMMIT")
    cursor.close()

    return hash_frequency_table.num_rows


def bin_table_ordinator(
    duckdb_connection: object,
    table_name:        str
) -> int:
    """
    Reorders a single bin table by hash column (ascending).

    Args:
        duckdb_connection: DuckDB connection object
        table_name: Name of the table to reorder

    Returns:
        Number of rows in the reordered table
    """

    cursor = duckdb_connection.cursor()

    # Create a temporary table with reordered data:
    cursor.execute(
        f"""
            CREATE TEMPORARY TABLE temp_reorder AS
            SELECT
                hash,
                text_id,
                positions
            FROM index.{table_name}
            ORDER BY
                hash    ASC,
                text_id ASC
        """
    )

    # Get row count:
    row_count_result = cursor.execute(
        "SELECT COUNT(*) FROM temp_reorder"
    ).fetchone()

    row_count = row_count_result[0]

    # Delete original table data and re-insert in sorted order:
    cursor.execute("BEGIN TRANSACTION")

    cursor.execute(f"DELETE FROM index.{table_name}")

    cursor.execute(
        f"""
            INSERT INTO index.{table_name}
            SELECT * FROM temp_reorder
        """
    )

    cursor.execute("COMMIT")

    # Drop temporary table:
    cursor.execute("DROP TABLE temp_reorder")
    cursor.close()

    return row_count


def main():
    """Main entry point for the script."""

    script_start = time()
    logger = logger_starter()

    print("\nStarting index optimization process ...", flush=True)

    parser = argparse.ArgumentParser(
        description="Optimize hash index."
    )

    parser.add_argument(
        "database_file",
        help="Path to the DuckDB index database file"
    )

    args = parser.parse_args()

    # Validate database file exists:
    db_path = Path(args.database_file)

    if not db_path.exists():
        raise FileNotFoundError(
            f"Database file not found: {args.database_file}"
        )

    # Connect to database:
    duckdb_connection = duckdb.connect()
    duckdb_connection.execute(f"ATTACH '{args.database_file}' AS index")

    # Get the number of bin tables:
    bin_tables = int(os.environ['INDEX_BINS'])

    # Calculate the total number of documents:
    total_documents = documents_counter(duckdb_connection)

    message = f"Total documents in index: {total_documents}"

    print(message, flush=True)
    logger.info(message)

    # Calculate the hash frequencies:
    print("\nCalculating hash frequencies ...", flush=True)

    hash_frequencies = hash_frequency_calculator(
        duckdb_connection,
        bin_tables
    )

    message = f"Unique hashes in index: {hash_frequencies.num_rows}"

    print(message, flush=True)
    logger.info(message)

    # Create metadata table for all hashes:
    print("\nCreating hash metadata table ...", flush=True)

    try:
        metadata_count = hash_metadata_table_creator(
            duckdb_connection,
            hash_frequencies
        )

        message = f"Hash metadata table rows: {metadata_count}"

        print(message, flush=True)
        logger.info(message)

    except Exception as error:
        message = f"ERROR creating hash metadata table: {str(error)}"

        print(message, flush=True)
        logger.error(message)

        raise

    # Reorder bin tables by hash and text_id:
    print("\nReordering bin tables by hash and text_id ...", flush=True)

    stats = {
        "total_tables":     bin_tables,
        "total_rows":       0,
        "tables_processed": []
    }

    for index in range(1, bin_tables + 1):
        table_name = f"bin_{index}"

        try:
            row_count = bin_table_ordinator(duckdb_connection, table_name)

            stats["total_rows"] += row_count
            stats["tables_processed"].append({
                "name":   table_name,
                "rows":   row_count,
                "status": "success"
            })

            message = \
                f"[{index}/{bin_tables}] {table_name}: Rows: {row_count}"

            print(message, flush=True)
            logger.info(message)

        except Exception as error:
            stats["tables_processed"].append({
                "name":   table_name,
                "error":  str(error),
                "status": "failed"
            })

            message = \
                f"[{index}/{bin_tables}] {table_name}: ERROR: {str(error)}"

            print(message, flush=True)
            logger.error(message)

    # Checkpoint to ensure all changes are written to disk:
    duckdb_connection.execute("CHECKPOINT index")
    duckdb_connection.close()

    # Print final summary:
    print("\nSummary:", flush=True)
    logger.info("Summary:")

    message = f"Total documents: {total_documents}"
    print(message, flush=True)
    logger.info(message)

    message = f"Total unique hashes: {hash_frequencies.num_rows}"
    print(message, flush=True)
    logger.info(message)

    message = f"Total bin tables: {stats['total_tables']}"
    print(message, flush=True)
    logger.info(message)

    successful = sum(
        1 for item in stats["tables_processed"]
        if item["status"] == "success"
    )

    message = f"Successfully processed bin tables: {successful}"
    print(message, flush=True)
    logger.info(message)

    message = f"Total rows in bin tables: {stats['total_rows']}"
    print(message, flush=True)
    logger.info(message)

    script_time = round((time() - script_start))
    script_time_string = str(timedelta(seconds=script_time))

    message = (f'Index optimization took {script_time_string}.')
    print(message, flush=True)
    logger.info(message)

    return True


if __name__ == "__main__":
    main()
