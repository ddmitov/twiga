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
# docker run --rm -it --user $(id -u):$(id -g) \
# -v $PWD:/app \
# -v $PWD/data:/.cache \
# twiga-demo python /app/demo_indexer.py

load_dotenv(find_dotenv())


def get_all_text_ids(
    duckdb_text_connection: object,
    bin_number:             int
) -> list:
    """Get all text_ids from a bin (for fresh indexing mode)."""

    text_ids = duckdb_text_connection.sql(
        f"""
            SELECT text_id
            FROM text.texts_bin_{str(bin_number)}
        """
    ).fetch_arrow_table().column('text_id').to_pylist()

    return text_ids


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

    index_bins     = int(os.environ['INDEX_BINS'])
    text_bins      = int(os.environ['TEXT_BINS'])
    parts_per_bin  = int(os.environ['INDEXER_PARTS_PER_BIN'])
    batch_maximum  = int(os.environ['INDEXER_BATCH_MAXIMUM'])

    script_start = time()
    logger = logger_starter()

    stopwords_bg_set = set(
        [
            'а', 'ако', 'ала', 'бе', 'без', 'беше', 'би', 'бил', 'била',
            'били', 'било', 'близо', 'бъдат', 'бъде', 'бяха', 'в', 'вас',
            'ваш', 'ваша', 'вече', 'ви', 'вие', 'все', 'всеки', 'всички',
            'всичко', 'всяка', 'във', 'въпреки', 'върху', 'г', 'ги', 'го',
            'дали', 'до', 'докато', 'докога', 'дори', 'досега', 'доста',
            'друг', 'друга', 'други', 'е', 'едва', 'ето', 'за', 'зад',
            'заедно', 'заради', 'засега', 'затова', 'защо', 'защото', 'и',
            'из', 'или', 'им', 'има', 'имат', 'иска', 'й', 'как', 'каква',
            'какво', 'както', 'какъв', 'като', 'кога', 'когато', 'което',
            'които', 'кой', 'който', 'колко', 'която', 'къде', 'където',
            'към', 'ли', 'м', 'ме', 'между', 'мен', 'ми', 'му', 'н', 'на',
            'над',  'най', 'например', 'нас', 'него', 'нещо', 'нея',
            'ни', 'ние', 'никой', 'нито', 'нищо', 'но', 'някои', 'някой',
            'няколко', 'няма', 'обаче', 'около', 'освен', 'особено', 'от',
            'отгоре', 'отново', 'още', 'пак', 'по', 'повече', 'повечето',
            'под', 'поне', 'поради', 'после', 'почти', 'пред', 'преди',
            'през', 'при', 'пък', 'пъти', 'с', 'са', 'само', 'се', 'сега',
            'си', 'скоро', 'след', 'сме', 'според', 'сред', 'срещу', 'сте',
            'съм', 'със', 'също', 'т', 'тази', 'така', 'такива', 'такъв',
            'там', 'те', 'тези', 'ти', 'то', 'това', 'тогава', 'този', 'той',
            'толкова', 'точно', 'тук', 'тъй', 'тя', 'тях', 'у', 'ч', 'часа',
            'че', 'чрез', 'ще', 'щом', 'я'
        ]
    )

    stopwords_en_set = set(
        [
            'a', 'about', 'above', 'after', 'again', 'against', 'all', 'am',
            'an', 'and', 'any', 'are', 'aren', 'as', 'at', 'be', 'because',
            'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by',
            'can', 'cannot', 'could', 'couldn', 'did', 'didn', 'do', 'does',
            'doesn', 'doing', 'don', 'down', 'during', 'each', 'few', 'for',
            'from', 'further', 'had', 'hadn', 'has', 'hasn', 'have', 'having',
            'he', 'he', 'her', 'here', 'hers', 'herself', 'him', 'himself',
            'his', 'how', 'how', 'i', 'if', 'in', 'into', 'is', 'isn', 'it',
            'its', 'itself', 'll', 'me', 'more', 'most', 'mustn', 'my',
            'myself', 'nor', 'not', 'of', 'off', 'on', 'once', 'only', 'or',
            'other', 'ought', 'our', 'ours', 'ourselves', 'out', 'over', 'own',
            're', 'same', 'shan', 'she', 'should', 'shouldn', 'so', 'some',
            'such', 'than', 'that', 'the', 'their', 'theirs', 'them',
            'themselves', 'then', 'there', 'these', 'they', 'this', 'those',
            'through', 't', 'to', 'too', 'under', 'until', 'up', 've', 'very',
            'was', 'wasn', 'we', 'were', 'weren', 'what', 'when', 'where',
            'which', 'while', 'who', 'whom', 'why', 'with', 'won', 'would',
            'wouldn', 'you', 'your', 'yours', 'yourself', 'yourselves'
        ]
    )

    stopword_set = stopwords_bg_set | stopwords_en_set

    print('Preparing the databases ...', flush=True)

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
        message = 'Created new index database'
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
        text_id_list = get_all_text_ids(
            duckdb_text_connection,
            bin_number
        )

        # Split each bin into smaller chunks to limit memory usage during indexing
        bin_parts = np.array_split(text_id_list, parts_per_bin)

        for bin_part_number, bin_part in enumerate(bin_parts, 1):
            if len(bin_part) > 0:
                indexing_start = time()

                # Build comma-separated ID list for SQL IN clause
                bin_part_string = ','.join(map(str, bin_part))

                batch_table = duckdb_text_connection.sql(
                    f"""
                        SELECT
                            text_id,
                            text
                        FROM text.texts_bin_{str(bin_number)}
                        WHERE text_id IN ({str(bin_part_string)})
                    """
                ).fetch_arrow_table()

                text_id_list_batch = batch_table.column('text_id').to_pylist()
                text_list          = batch_table.column('text').to_pylist()

                # Delete objects that are not needed anymore and
                # perform garbage collection to prevent memory leaks:
                del bin_part
                del bin_part_string
                del batch_table
                gc.collect()

                batch_texts, batch_words = twiga_index_writer(
                    index_database_file_path,
                    text_id_list_batch,
                    text_list,
                    index_bins,
                    stopword_set,
                    batch_maximum
                )

                texts_total += batch_texts
                words_total += batch_words

                indexing_time = round((time() - indexing_start))
                indexing_time_string = str(timedelta(seconds=indexing_time))

                # Log batch processing data:
                message = (
                    f'text bin {str(bin_number)}/{str(text_bins)}, ' +
                    f'part {str(bin_part_number)}/{str(parts_per_bin)} - ' +
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
