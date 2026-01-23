#!/usr/bin/env python3

"""
Reorder all hash index tables by hash column.

This script reorganizes all bin_* tables in the DuckDB index database
by sorting them alphabetically by the 'hash' and 'text_id' columns.
This improves:
- Query performance through better cache locality
- Compression efficiency (similar hashes are adjacent)
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

# Start the reordering process:
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
    cursor.execute(f"""
        CREATE TEMPORARY TABLE temp_reorder AS
        SELECT
            hash,
            text_id,
            positions
        FROM index.{table_name}
        ORDER BY hash ASC
    """)
    
    # Get row count:
    row_count_result = cursor.execute(
        "SELECT COUNT(*) FROM temp_reorder"
    ).fetchone()

    row_count = row_count_result[0]
    
    # Delete original table data and re-insert in sorted order:
    cursor.execute("BEGIN TRANSACTION")
    
    cursor.execute(f"DELETE FROM index.{table_name}")
    
    cursor.execute(f"""
        INSERT INTO index.{table_name}
        SELECT * FROM temp_reorder
    """)
    
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
        description="Reorder hash index tables by hash column for better performance."
    )

    parser.add_argument(
        "database_file",
        help="Path to the DuckDB index database file (e.g., /app/data/twiga_index.duckdb)"
    )

    args = parser.parse_args()

    # Validate database file exists:
    db_path = Path(args.database_file)

    if not db_path.exists():
        raise FileNotFoundError(f"Database file not found: {args.database_file}")

    # Connect to database:
    duckdb_connection = duckdb.connect()
    duckdb_connection.execute(f"ATTACH '{args.database_file}' AS index")

    # Get the number of bin tables:
    bin_tables = int(os.environ['INDEX_BINS'])

    stats = {
        "total_tables":     bin_tables,
        "total_rows":       0,
        "tables_processed": []
    }
    
    # Reorder each bin table:
    for idx in range(1, bin_tables + 1):
        table_name = f"bin_{idx}"

        try:
            row_count = reorder_bin_table(duckdb_connection, table_name)

            stats["total_rows"] += row_count
            stats["tables_processed"].append({
                "name":   table_name,
                "rows":   row_count,
                "status": "success"
            })

            message = f"[{idx}/{bin_tables}] {table_name}: Reordered rows: {row_count}"
            print(message, flush=True)
            logger.info(message)

        except Exception as error:
            stats["tables_processed"].append({
                "name":   table_name,
                "error":  str(error),
                "status": "failed"
            })

            message = f"[{idx}/{bin_tables}] {table_name}: ERROR - {str(error)}"
            print(message, flush=True)
            logger.error(message)

    # Checkpoint to ensure all changes are written to disk:
    duckdb_connection.execute("CHECKPOINT index")
    duckdb_connection.close()

    message = f"Total processed tables: {stats['total_tables']}"
    print(message, flush=True)
    logger.info(message)

    successful = sum(
        1 for item in stats["tables_processed"]
        if item["status"] == "success"
    )

    message = f"Successfully processed tables: {successful}"
    print(message, flush=True)
    logger.info(message)

    message = f"Total rows reordered: {stats['total_rows']}"
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
