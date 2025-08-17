#!/usr/bin/env python3

# Core modules:
from   datetime import datetime
from   datetime import timedelta
import json
import logging
import os
import shutil
from   time     import time

# PIP modules:
from   datasets import load_dataset
from   dotenv   import find_dotenv
from   dotenv   import load_dotenv
import duckdb

# Twiga modules:
from twiga_core import twiga_index_database_creator
from twiga_core import twiga_index_writer
from twiga_text import twiga_text_writer

# Start the indexing process:
# docker run --rm -it --user $(id -u):$(id -g) \
# -v $PWD:/app \
# -v $PWD/data:/.cache \
# twiga-demo python /app/demo_indexer.py

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
    LAST_DATASET_TABLE_NUMBER  = 400
    TEXTS_PER_DATASET_TABLE    = 25000

    BINS_TOTAL = 500

    # Start measuring runtime and set logging:
    script_start = time()
    logger = logger_starter()

    # Initialize stopwords list:
    stopword_set = None

    with open('/home/twiga/stopwords-iso.json', 'r') as stopwords_json_file:
        stopword_json_data = json.load(stopwords_json_file)

        stopwords_bg_set = set(stopword_json_data['bg'])
        stopwords_en_set = set(stopword_json_data['en'])

        stopword_set = stopwords_bg_set | stopwords_en_set

    # Initialize the text DuckDB database and connection:
    duckdb_text_connection = duckdb.connect('/app/data/twiga_texts.db')

    duckdb_text_connection.execute(
        'CREATE SEQUENCE IF NOT EXISTS text_id_sequence START 1'
    )

    for bin_number in range(1, BINS_TOTAL + 1):
        duckdb_text_connection.execute(
            f'''
                CREATE TABLE IF NOT EXISTS texts_bin_{str(bin_number)} (
                    text_id INTEGER,
                    title   VARCHAR,
                    date    DATE,
                    text    VARCHAR
                )
            '''
        )

    # Initialize the index DuckDB database and connection:
    duckdb_index_connection = None

    if os.path.isfile('/app/data/twiga_index.db'):
        duckdb_index_connection = duckdb.connect('/app/data/twiga_index.db')
    else:
        duckdb_index_connection = twiga_index_database_creator(
            '/app/data/twiga_index.db',
            BINS_TOTAL
        )

    # Initialize the dataset for the demo:
    print('Reading the dataset for the demo ...', flush=True)

    dataset = load_dataset(
        path      = 'stanford-oval/ccnews',
        split     = 'train',
        name      = YEAR,
        streaming = True
    ).with_format('arrow')

    # Iterate over the dataset for the demo and get the texts in batches:
    table_number = 0

    for dataset_table in dataset.iter(batch_size=TEXTS_PER_DATASET_TABLE):
        table_number += 1

        if (
            table_number >= FIRST_DATASET_TABLE_NUMBER and
            table_number <= LAST_DATASET_TABLE_NUMBER
        ):
            # Prepare the texts in the batch:
            batch_table = duckdb_text_connection.sql(
                f'''
                    SELECT
                        NEXTVAL('text_id_sequence') AS text_id,
                        title,
                        published_date AS date,
                        plain_text AS text
                    FROM dataset_table
                    WHERE language IN ('bg', 'en')
                '''
            ).arrow()

            # Get the number of texts in the batch:
            batch_texts_total = duckdb_text_connection.query(
                '''
                    SELECT COUNT(text_id) AS texts_total
                    FROM batch_table
                '''
            ).arrow().column('texts_total')[0].as_py()

            if batch_texts_total == 0:
                break

            # Write the texts in the batch:
            processing_start = time()

            twiga_text_writer(duckdb_text_connection, BINS_TOTAL, batch_table)

            processing_time = round((time() - processing_start))
            processing_time_string = str(timedelta(seconds=processing_time))

            message = (
                f'Batch {str(table_number)}/{str(LAST_DATASET_TABLE_NUMBER)}' +
                f' - {str(batch_texts_total)} texts written for ' +
                f'{processing_time_string}'
            )

            print(message, flush=True)
            logger.info(message)

            # Index the texts in the batch:
            processing_start = time()

            batch_table.drop_columns(['title', 'date'])

            twiga_index_writer(
                duckdb_index_connection,
                BINS_TOTAL,
                batch_table,
                stopword_set
            )

            processing_time = round((time() - processing_start))
            processing_time_string = str(timedelta(seconds=processing_time))

            message = (
                f'Batch {str(table_number)}/{str(LAST_DATASET_TABLE_NUMBER)}' +
                f' - {str(batch_texts_total)} texts indexed for ' +
                f'{processing_time_string}'
            )

            print(message, flush=True)
            logger.info(message)

        if table_number == LAST_DATASET_TABLE_NUMBER:
            break

    # Explicitly close all DuckDB connections:
    duckdb_text_connection.close()
    duckdb_index_connection.close()

    # Remove all dataset files:
    shutil.rmtree('/app/data/huggingface', ignore_errors=True)

    # Get the script runtime:
    script_time = round((time() - script_start))
    script_time_string = str(timedelta(seconds=script_time))

    message = f'Total script runtime: {script_time_string}.'
    print(message, flush=True)
    logger.info(message)

    return True


if __name__ == '__main__':
    main()
