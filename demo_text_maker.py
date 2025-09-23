#!/usr/bin/env python3

# Core modules:
from   datetime import datetime
from   datetime import timedelta
import gc
import logging
import math
import os
import psutil
import shutil
from   time     import sleep
from   time     import time

# PIP modules:
from   datasets import load_dataset
from   dotenv   import find_dotenv
from   dotenv   import load_dotenv
import duckdb

# Twiga module:
from twiga_text import twiga_text_writer

# Start the indexing process:
# docker run --rm -it --user $(id -u):$(id -g) \
# -v $PWD:/app \
# -v $PWD/data:/.cache \
# twiga-demo python /app/demo_text_maker.py

load_dotenv(find_dotenv())


def logger_starter() -> logging.Logger:

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

    YEAR = '2024'

    FIRST_DATASET_TABLE_NUMBER = 1
    LAST_DATASET_TABLE_NUMBER  = 750
    TEXTS_PER_DATASET_TABLE    = 25000

    BINS_TOTAL = 500

    # Start measuring runtime and set logging:
    script_start = time()
    logger = logger_starter()

    message = (f'First dataset table: {FIRST_DATASET_TABLE_NUMBER}')
    print(message, flush=True)
    logger.info(message)

    message = (f'Last dataset table: {LAST_DATASET_TABLE_NUMBER}')
    print(message, flush=True)
    logger.info(message)

    message = (f'Texts per dataset table: {TEXTS_PER_DATASET_TABLE}')
    print(message, flush=True)
    logger.info(message)

    # Initialize DuckDB connection and text database:
    print('\nPreparing the text database ...\n', flush=True)

    duckdb_connection = duckdb.connect()

    duckdb_memory_limit = \
        int(math.floor(psutil.virtual_memory().total * 0.5) / (1024**3))

    duckdb_connection.execute(f"SET memory_limit = '{duckdb_memory_limit}GB'")

    duckdb_connection.execute(
        "ATTACH '/app/data/twiga_texts.duckdb' AS text"
    )

    duckdb_connection.execute(
        "CREATE SEQUENCE IF NOT EXISTS text.text_id_sequence START 1"
    )

    for bin_number in range(1, BINS_TOTAL + 1):
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

    # Initialize a source dataset for the demo:
    print(
        'Preparing the dataset for the demo. ' +
        'This may take a minute or two.\n',
        flush=True
    )

    dataset = load_dataset(
        path      = 'stanford-oval/ccnews',
        split     = 'train',
        name      = YEAR,
        streaming = True
    ).select_columns(
        ['title', 'published_date', 'plain_text', 'language']
    ).with_format('arrow')

    # Iterate over the source dataset and
    # get texts as a sequence of Arrow tables:
    print('\nProcessing the dataset ...\n', flush=True)

    table_number = 0
    texts_total  = 0

    for batch_table in dataset.iter(batch_size=TEXTS_PER_DATASET_TABLE):
        table_number += 1

        if table_number < FIRST_DATASET_TABLE_NUMBER:
            sleep(1)

        if (
            table_number >= FIRST_DATASET_TABLE_NUMBER and
            table_number <= LAST_DATASET_TABLE_NUMBER
        ):
            writing_time_string  = ''

            # Prepare the texts in the batch:
            batch_table = duckdb_connection.sql(
                f"""
                    SELECT
                        NEXTVAL('text.text_id_sequence') AS text_id,
                        title,
                        published_date AS date,
                        plain_text AS text
                    FROM batch_table
                    WHERE language IN ('bg', 'en')
                """
            ).arrow()

            batch_texts = batch_table.num_rows
            texts_total += batch_texts

            if batch_texts == 0:
                message = 'No more data available in the dataset.'

                print(message, flush=True)
                logger.info(message)

                break

            # Write the texts in the batch:
            writing_start = time()

            twiga_text_writer(duckdb_connection, BINS_TOTAL, batch_table)

            writing_time = round((time() - writing_start))
            writing_time_string = str(timedelta(seconds=writing_time))

            # Log batch processing data:
            message = (
                f'batch ' +
                f'{str(table_number)}/{str(LAST_DATASET_TABLE_NUMBER)} - ' +
                f'{str(batch_texts)} texts written for ' +
                f'{writing_time_string}'
            )

            print(message, flush=True)
            logger.info(message)

        # Perform garbage collection to prevent memory leaks:
        del batch_table
        gc.collect()

        if table_number == LAST_DATASET_TABLE_NUMBER:
            break

    # Explicitly close the DuckDB connection to
    # flush any pending WAL file data:
    duckdb_connection.close()

    # Remove all dataset files:
    shutil.rmtree('/app/data/huggingface', ignore_errors=True)

    # Get the script runtime:
    script_time = round((time() - script_start))
    script_time_string = str(timedelta(seconds=script_time))

    # Log final script statistics:
    message = (f'{texts_total} texts were written for {script_time_string}')

    print(message, flush=True)
    logger.info(message)

    return True


if __name__ == '__main__':
    main()
