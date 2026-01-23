#!/usr/bin/env python3

# Core modules:
from   datetime import datetime
from   datetime import timedelta
import gc
import logging
import os
from   time     import time

# PIP modules:
from   dotenv import find_dotenv
from   dotenv import load_dotenv
import duckdb
import numpy  as     np

# Twiga modules:
from twiga_core_index import twiga_index_creator
from twiga_core_index import twiga_index_writer

# Start the indexing process:
# docker run --rm -it --user $(id -u):$(id -g) -v $PWD:/app \
# twiga-demo python /app/demo_indexer.py

load_dotenv(find_dotenv())


def logger_starter() -> logging.Logger:
    """Initialize and return a logger for the indexing process."""

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
    """Process texts from bins and build the search index."""

    text_bins     = int(os.environ['TEXT_BINS'])
    index_bins    = int(os.environ['INDEX_BINS'])
    batch_maximum = 500000

    script_start = time()
    logger = logger_starter()

    index_database_file_path = '/app/data/twiga_index.duckdb'
    text_database_file_path  = '/app/data/twiga_texts.duckdb'

    # Check if text database exists:
    if not os.path.isfile(text_database_file_path):
        message = (
            f'Text database not found at {text_database_file_path}. ' +
            'Please run demo_text_processor.py first.'
        )
        print(message, flush=True)
        logger.error(message)
        return False

    # Create index database if it doesn't exist:
    if not os.path.isfile(index_database_file_path):
        twiga_index_creator(
            index_database_file_path,
            index_bins
        )
        message = 'Index database is created.'
        print(message, flush=True)
        logger.info(message)

    # Connect to text database (read-only):
    duckdb_text_connection = duckdb.connect()

    duckdb_text_connection.execute(
        f"ATTACH '{text_database_file_path}' AS text (READ_ONLY)"
    )

    texts_total  = 0
    words_total  = 0

    for bin_number in range(1, text_bins + 1):
        text_id_list = duckdb_text_connection.sql(
            f"""
                SELECT text_id
                FROM text.texts_bin_{str(bin_number)}
            """
        ).fetch_arrow_table().column('text_id').to_pylist()


        indexing_start = time()

        # Build comma-separated ID list for SQL IN clause
        text_id_string = ','.join(map(str, text_id_list))

        batch_table = duckdb_text_connection.sql(
            f"""
                SELECT
                    text_id,
                    text
                FROM text.texts_bin_{str(bin_number)}
                WHERE text_id IN ({str(text_id_string)})
            """
        ).fetch_arrow_table()

        text_id_list_batch = batch_table.column('text_id').to_pylist()
        text_list          = batch_table.column('text').to_pylist()

        # Delete objects that are not needed anymore and
        # perform garbage collection to prevent memory leaks:
        del text_id_list
        del text_id_string
        del batch_table
        gc.collect()

        batch_texts, batch_words = twiga_index_writer(
            index_database_file_path,
            text_id_list_batch,
            text_list,
            index_bins,
            batch_maximum
        )

        texts_total += batch_texts
        words_total += batch_words

        indexing_time = round((time() - indexing_start))
        indexing_time_string = str(timedelta(seconds=indexing_time))

        # Log batch processing data:
        message = (
            f'text bin {str(bin_number)}/{str(text_bins)}, ' +
            f'{str(batch_texts)} texts, ' +
            f'{str(batch_words)} words indexed for ' +
            f'{indexing_time_string}'
        )

        print(message, flush=True)
        logger.info(message)

        del text_id_list_batch
        del text_list
        gc.collect()

    duckdb_text_connection.close()

   # Final summary: 
    script_time = round((time() - script_start))
    script_time_string = str(timedelta(seconds=script_time))

    message = (
        f'{str(texts_total)} texts having a total of ' +
        f'{str(words_total)} words were indexed for ' +
        f'{script_time_string}'
    )

    print(message, flush=True)
    logger.info(message)

    return True


if __name__ == '__main__':
    main()
