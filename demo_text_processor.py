#!/usr/bin/env python3

# Core modules:
from   datetime import datetime
from   datetime import timedelta
import gc
import logging
import os
import shutil
from   time     import time

# PIP modules:
from   datasets import load_dataset
from   dotenv   import find_dotenv
from   dotenv   import load_dotenv
import duckdb
import psutil

# Twiga module:
from twiga_text import twiga_text_writer

# Start the indexing process:
# docker run --rm -it --user $(id -u):$(id -g) \
# -v $PWD:/app \
# -v $PWD/data:/.cache \
# twiga-demo python /app/demo_text_processor.py

load_dotenv(find_dotenv())


def logger_starter() -> logging.Logger:
    """Initialize and return a timestamped file logger."""

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

def main():
    """Main function to start the text processing."""

    dataset_year  = os.environ['DEMO_DATASET_YEAR']

    valid_years = {
        '2016',
        '2017',
        '2018',
        '2019',
        '2020',
        '2021',
        '2022',
        '2023',
        '2024',
        'default'
    }

    if dataset_year not in valid_years:
        raise ValueError(
            f"Invalid DEMO_DATASET_YEAR '{dataset_year}'. " +
            f"Must be one of: {valid_years}"
        )

    first_table_number = int(os.environ['DEMO_DATASET_FIRST_TABLE_NUMBER'])
    last_table_number  = int(os.environ['DEMO_DATASET_LAST_TABLE_NUMBER'])
    texts_per_table    = int(os.environ['DEMO_DATASET_TEXTS_PER_TABLE'])
    text_bins          = int(os.environ['TEXT_BINS'])

    # Start measuring runtime and set logging:
    script_start = time()
    logger = logger_starter()

    # Initialize DuckDB connection and text database:
    print('Preparing the text database ...', flush=True)

    duckdb_connection = duckdb.connect()

    # Limit DuckDB to 50% of system RAM (converted from bytes to GB):
    duckdb_memory_limit = int(psutil.virtual_memory().total * 0.5 // (1024**3))

    duckdb_connection.execute(f"SET memory_limit = '{duckdb_memory_limit}GB'")

    duckdb_connection.execute("ATTACH '/app/data/twiga_texts.duckdb' AS text")

    duckdb_connection.execute(
        "CREATE SEQUENCE IF NOT EXISTS text.text_id_sequence START 1"
    )

    # Create partitioned tables (bins) to distribute texts for parallel processing:
    for bin_number in range(1, text_bins + 1):
        duckdb_connection.execute(
            f"""
                CREATE TABLE IF NOT EXISTS text.texts_bin_{str(bin_number)} (
                    text_id INTEGER PRIMARY KEY,
                    title   VARCHAR,
                    date    DATE,
                    text    VARCHAR
                )
            """
        )

    message = f'Texts per batch: {texts_per_table}'
    print(message, flush=True)
    logger.info(message)

    # Initialize a source dataset for the demo:
    print(
        '\n' +
        'Preparing the dataset for the demo. ' +
        'This may take a minute or two.' +
        '\n',
        flush=True
    )

    dataset = load_dataset(
        path      = 'stanford-oval/ccnews',
        split     = 'train',
        name      = dataset_year,
        streaming = True
    ).select_columns(
        [
            'title',
            'published_date',
            'plain_text',
            'language',
            'language_score'
        ]
    ).with_format('arrow')

    # Use skip() for efficient seeking to the start point:
    if first_table_number > 0:
        skip_records = first_table_number * texts_per_table

        message = f'Skipping {skip_records} records to start point ...'
        print(message, flush=True)
        logger.info(message)

        dataset = dataset.skip(skip_records)

    # Iterate over the source dataset and
    # get texts as a sequence of Arrow record batches:
    print('\nProcessing the dataset ...\n', flush=True)

    table_number = first_table_number
    texts_total  = 0
    batches_remaining = last_table_number - first_table_number

    for record_batch in dataset.iter(batch_size=texts_per_table):
        table_number += 1

        if table_number > last_table_number:
            break

        # Assign auto-incrementing IDs via NEXTVAL and
        # filter for Bulgarian and English texts with high confidence:
        batch_table = duckdb_connection.sql(
            """
                SELECT
                    NEXTVAL('text.text_id_sequence') AS text_id,
                    title,
                    published_date AS date,
                    plain_text AS text
                FROM record_batch
                WHERE
                    language IN ('bg', 'en')
                    AND language_score >= 0.85
            """
        ).fetch_arrow_table()

        batch_texts = batch_table.num_rows
        texts_total += batch_texts

        if batch_texts == 0:
            message = 'No more data available in the dataset.'

            print(message, flush=True)
            logger.info(message)

            break

        # Write the texts in the batch:
        writing_start = time()

        twiga_text_writer(duckdb_connection, text_bins, batch_table)

        writing_time = round((time() - writing_start))
        writing_time_string = str(timedelta(seconds=writing_time))

        # Log batch processing data:
        message = (
            'text batch ' +
            f'{str(table_number)}/{str(last_table_number)} - ' +
            f'{str(batch_texts)} texts written for ' +
            f'{writing_time_string}'
        )

        print(message, flush=True)
        logger.info(message)

        # Perform garbage collection to prevent memory leaks:
        del batch_table
        gc.collect()

    # Explicitly close the DuckDB connection to
    # flush any pending WAL file data:
    duckdb_connection.close()

    # Remove all dataset files:
    shutil.rmtree('/app/data/huggingface', ignore_errors=True)

    # Get the script runtime:
    script_time = round((time() - script_start))
    script_time_string = str(timedelta(seconds=script_time))

    # Log final script statistics:
    message = (f'\n{texts_total} texts were written for {script_time_string}')

    print(message, flush=True)
    logger.info(message)

    return True


if __name__ == '__main__':
    main()
