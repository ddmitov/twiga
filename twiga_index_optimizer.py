#!/usr/bin/env python3

"""
Optimize the hash index by identifying and separating high-frequency hashes.

This script:
1. Calculates hash frequency for every hash in the index
2. Creates a high_frequency_hashes table for hashes found in >10% of documents
3. Moves high-frequency hashes to separate tables for better query performance
4. Reorders remaining bin tables by hash column for cache locality
"""

# Core modules:
import argparse
from   datetime import datetime
from   datetime import timedelta
import logging
import os
from   pathlib import Path
from   time     import time

# PIP modules:
from   dotenv import find_dotenv
from   dotenv import load_dotenv
import duckdb

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


def get_total_documents(duckdb_connection: object) -> int:
    """
    Get the total number of documents in the index.

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


def calculate_hash_frequencies(
    duckdb_connection: object,
    bin_tables:        int
) -> dict:
    """
    Calculate the frequency of each hash (how many documents it appears in).

    Args:
        duckdb_connection: DuckDB connection object
        bin_tables: Number of bin tables

    Returns:
        Dictionary mapping hash to document count
    """

    cursor = duckdb_connection.cursor()
    cursor.execute("SET threads TO 1")

    # Process each bin table separately to avoid expression depth limits.
    # Accumulate hash frequencies across all bin tables:
    hash_frequencies = {}

    for index in range(1, bin_tables + 1):
        cursor.execute(
            f"""
                SELECT
                    hash,
                    COUNT(DISTINCT text_id) as document_count
                FROM index.bin_{index}
                GROUP BY hash
            """
        )

        results = cursor.fetchall()

        # Accumulate frequencies from this bin table:
        for hash_item, count in results:
            if hash_item in hash_frequencies:
                hash_frequencies[hash_item] += count
            else:
                hash_frequencies[hash_item] = count

    cursor.close()

    return hash_frequencies


def identify_high_frequency_hashes(
    hash_frequencies:            dict,
    total_documents:             int,
    frequency_threshold_percent: float = 10.0
) -> set:
    """
    Identify hashes that appear in
    more than the threshold percentage of documents.

    Args:
        hash_frequencies: Dictionary mapping hash to document count
        total_documents: Total number of documents in index
        frequency_threshold_percent: Threshold percentage (default 10%)

    Returns:
        Set of high-frequency hashes
    """

    threshold_count = int(
        total_documents * (frequency_threshold_percent / 100)
    )

    high_frequency_hashes = {
        hash_item
        for hash_item, count in hash_frequencies.items()
        if count > threshold_count
    }

    return high_frequency_hashes


def create_high_frequency_hashes_table(
    duckdb_connection:           object,
    hash_frequencies:            dict,
    high_frequency_hashes:       set,
    total_documents:             int,
    frequency_threshold_percent: float = 10.0
) -> int:
    """
    Create the high_frequency_hashes metadata table.

    Args:
        duckdb_connection: DuckDB connection object
        hash_frequencies: Dictionary mapping hash to document count
        high_frequency_hashes: Set of high-frequency hashes
        total_documents: Total number of documents
        frequency_threshold_percent: Threshold percentage used

    Returns:
        Number of high-frequency hashes
    """

    cursor = duckdb_connection.cursor()

    # Drop table if it exists:
    cursor.execute("DROP TABLE IF EXISTS index.high_frequency_hashes")

    # Create the high_frequency_hashes table:
    cursor.execute(
        """
            CREATE TABLE index.high_frequency_hashes (
                hash                VARCHAR PRIMARY KEY,
                document_count      INTEGER,
                document_percentage DOUBLE,
                table_name          VARCHAR
            )
        """
    )

    cursor.execute("BEGIN TRANSACTION")

    # Insert high-frequency hash metadata:
    for hash_item in sorted(high_frequency_hashes):
        doc_count      = hash_frequencies[hash_item]
        doc_percentage = (doc_count / total_documents) * 100
        table_name     = f"hash_{hash_item}"

        cursor.execute(
        """
            INSERT INTO index.high_frequency_hashes
            VALUES (?, ?, ?, ?)
        """, [hash_item, doc_count, doc_percentage, table_name])

    cursor.execute("COMMIT")
    cursor.close()

    return len(high_frequency_hashes)


def create_high_frequency_hash_tables(
    duckdb_connection:     object,
    high_frequency_hashes: set,
    bin_tables:            int
) -> dict:
    """
    Create a separate table for each high-frequency hash.

    Args:
        duckdb_connection: DuckDB connection object
        high_frequency_hashes: Set of high-frequency hashes
        bin_tables: Number of bin tables

    Returns:
        Dictionary mapping high-frequency hash to row count
    """

    cursor = duckdb_connection.cursor()
    cursor.execute("SET threads TO 1")

    stats = {}

    for hash_item in high_frequency_hashes:
        table_name = f"hash_{hash_item}"

        # Create table for this high-frequency hash:
        cursor.execute(
            f"""
                CREATE TABLE index.{table_name} (
                    hash      VARCHAR,
                    text_id   INTEGER,
                    positions INTEGER[]
                )
            """
        )

        cursor.execute("BEGIN TRANSACTION")

        # Insert data from each bin table iteratively
        # to avoid expression depth limits:
        for index in range(1, bin_tables + 1):
            insert_query = f"""
                INSERT INTO index.{table_name}
                SELECT
                    hash,
                    text_id,
                    positions
                FROM index.bin_{index}
                WHERE hash = '{hash_item}'
                ORDER BY
                    hash ASC,
                    text_id ASC
            """

            cursor.execute(insert_query)

        # Get the row count:
        row_count_result = cursor.execute(
            f"SELECT COUNT(*) FROM index.{table_name}"
        ).fetchone()

        row_count = row_count_result[0] if row_count_result else 0
        stats[hash_item] = row_count

        cursor.execute("COMMIT")

    cursor.close()

    return stats


def remove_high_frequency_hashes_from_bins(
    duckdb_connection:     object,
    high_frequency_hashes: set,
    bin_tables:            int
) -> None:
    """
    Remove high-frequency hash entries from bin tables.

    Args:
        duckdb_connection: DuckDB connection object
        high_frequency_hashes: Set of high-frequency hashes
        bin_tables: Number of bin tables
    """

    cursor = duckdb_connection.cursor()
    cursor.execute("SET threads TO 1")
    
    for index in range(1, bin_tables + 1):
        table_name = f"bin_{index}"

        cursor.execute("BEGIN TRANSACTION")

        # Delete high-frequency hashes from bin table iteratively:
        for hash_item in high_frequency_hashes:
            cursor.execute(
                f"""
                    DELETE FROM index.{table_name}
                    WHERE hash = '{hash_item}'
                """
            )

        cursor.execute("COMMIT")

    cursor.close()


def reorder_bin_table(
    duckdb_connection: object,
    table_name:        str
) -> int:
    """
    Reorder a single bin table by hash column (ascending).

    Args:
        duckdb_connection: DuckDB connection object
        table_name: Name of the table to reorder

    Returns:
        Number of rows in the reordered table
    """

    cursor = duckdb_connection.cursor()
    cursor.execute("SET threads TO 1")
    
    # Create a temporary table with reordered data:
    cursor.execute(
        f"""
            CREATE TEMPORARY TABLE temp_reorder AS
            SELECT
                hash,
                text_id,
                positions
            FROM index.{table_name}
            ORDER BY hash ASC
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

    # STEP 1: Calculate the total number of documents:
    message = "Starting index optimization process ..."

    print(message, flush=True)
    logger.info(message)

    total_documents = get_total_documents(duckdb_connection)

    message = f"Total documents in index: {total_documents}"

    print(message, flush=True)
    logger.info(message)

    # STEP 2: Calculate the hash frequencies:
    message = "Step 2: Calculating hash frequencies ..."

    print(message, flush=True)
    logger.info(message)

    hash_frequencies = calculate_hash_frequencies(
        duckdb_connection,
        bin_tables
    )

    message = f"Unique hashes in index: {len(hash_frequencies)}"

    print(message, flush=True)
    logger.info(message)

    # STEP 3: Identify high-frequency hashes:
    message = "Step 3: Identifying high-frequency hashes ..."

    print(message, flush=True)
    logger.info(message)

    high_frequency_hashes = identify_high_frequency_hashes(
        hash_frequencies,
        total_documents,
        frequency_threshold_percent=10.0
    )

    message = f"High-frequency hashes found: {len(high_frequency_hashes)}"

    print(message, flush=True)
    logger.info(message)

    if high_frequency_hashes:
        # STEP 4: Create high_frequency_hashes metadata table:
        message = "Step 4: Creating high_frequency_hashes metadata table..."

        print(message, flush=True)
        logger.info(message)

        try:
            hf_count = create_high_frequency_hashes_table(
                duckdb_connection,
                hash_frequencies,
                high_frequency_hashes,
                total_documents,
                frequency_threshold_percent=10.0
            )

            message = \
                f"High-frequency hash table created with {hf_count} entries"

            print(message, flush=True)
            logger.info(message)

        except Exception as error:
            message = f"ERROR creating high-frequency hash table: {str(error)}"

            print(message, flush=True)
            logger.error(message)

            raise

        # STEP 5: Create separate tables for high-frequency hashes:
        message = "Step 5: Creating tables for high-frequency hashes ..."

        print(message, flush=True)
        logger.info(message)

        try:
            hf_table_stats = create_high_frequency_hash_tables(
                duckdb_connection,
                high_frequency_hashes,
                bin_tables
            )

            total_hf_rows = sum(hf_table_stats.values())

            message = \
                f"Created {len(hf_table_stats)} high-frequency hash tables " +
                "with {total_hf_rows} total rows"

            print(message, flush=True)
            logger.info(message)

            for hash_item, row_count in sorted(hf_table_stats.items()):
                message = \
                    f"  hash_{hash_item}: {row_count} rows " +
                    f"({hash_frequencies[hash_item]} documents)"

                print(message, flush=True)
                logger.info(message)

        except Exception as error:
            message = f"ERROR creating high-frequency hash table: {str(error)}"

            print(message, flush=True)
            logger.error(message)

            raise

        # STEP 6: Remove high-frequency hashes from bin tables:
        message = "Step 6: Removing high-frequency hashes from bin tables..."
        print(message, flush=True)
        logger.info(message)

        try:
            remove_high_frequency_hashes_from_bins(
                duckdb_connection,
                high_frequency_hashes,
                bin_tables
            )

            message = "High-frequency hashes are removed from bin tables."

            print(message, flush=True)
            logger.info(message)

        except Exception as error:
            message = \
                f"ERROR removing high-frequency hashes from bins: {str(error)}"

            print(message, flush=True)
            logger.error(message)

            raise

    # STEP 7: Reorder remaining bin tables:
    message = "Step 7: Reordering remaining bin tables..."
    print(message, flush=True)
    logger.info(message)

    stats = {
        "total_tables":     bin_tables,
        "total_rows":       0,
        "tables_processed": [],
        "high_frequency_hashes": len(high_frequency_hashes)
    }

    for index in range(1, bin_tables + 1):
        table_name = f"bin_{index}"

        try:
            row_count = reorder_bin_table(duckdb_connection, table_name)

            stats["total_rows"] += row_count
            stats["tables_processed"].append({
                "name":   table_name,
                "rows":   row_count,
                "status": "success"
            })

            message = \
                f"[{idx}/{bin_tables}] {table_name}: " +
                f"Reordered rows: {row_count}"

            print(message, flush=True)
            logger.info(message)

        except Exception as error:
            stats["tables_processed"].append({
                "name":   table_name,
                "error":  str(error),
                "status": "failed"
            })

            message = \
                f"[{idx}/{bin_tables}] {table_name}: " +
                f"ERROR: {str(error)}"

            print(message, flush=True)
            logger.error(message)

    # Checkpoint to ensure all changes are written to disk:
    duckdb_connection.execute("CHECKPOINT index")
    duckdb_connection.close()

    # Print final summary:
    print("\n=== OPTIMIZATION SUMMARY ===", flush=True)
    logger.info("=== OPTIMIZATION SUMMARY ===")

    message = f"Total documents: {total_documents}"
    print(message, flush=True)
    logger.info(message)

    message = f"Total unique hashes: {len(hash_frequencies)}"
    print(message, flush=True)
    logger.info(message)

    message = f"High-frequency hashes: {stats['high_frequency_hashes']}"
    print(message, flush=True)
    logger.info(message)

    message = f"Bin tables processed: {stats['total_tables']}"
    print(message, flush=True)
    logger.info(message)

    successful = sum(
        1 for item in stats["tables_processed"]
        if item["status"] == "success"
    )

    message = f"Successfully processed tables: {successful}"
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
