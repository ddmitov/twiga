#!/usr/bin/env python3

# Core modules:
from   collections          import defaultdict
import gc
import hashlib
from   itertools            import chain
from   multiprocessing      import get_context
from   multiprocessing      import cpu_count
from   multiprocessing.pool import ThreadPool
from   typing               import List

# PIP modules:
import duckdb
import numpy      as     np
import pyarrow    as     pa
from   tokenizers import normalizers
from   tokenizers import pre_tokenizers


def twiga_list_splitter(
    input_list:   list,
    parts_number: int
) -> List[list]:
    """Split a list into N approximately equal parts."""

    parts = np.array_split(input_list, parts_number)
    list_of_lists = [list(part) for part in parts if len(part) > 0]

    return list_of_lists


def twiga_dict_splitter(
    input_dict:   dict,
    parts_number: int
) -> List[dict]:
    """Split a dictionary into N smaller dictionaries by keys."""

    key_items = list(input_dict.keys())
    key_parts = np.array_split(key_items, parts_number)

    key_batches = [list(key_part) for key_part in key_parts]

    list_of_dicts = []

    for key_batch in key_batches:
        batch_dict = {}

        for key in key_batch:
            batch_dict[key] = input_dict[key]

        list_of_dicts.append(batch_dict)

    return list_of_dicts


def twiga_hasher_error_callback(error: str) -> bool:
    """Print errors from multiprocessing pool and return True."""

    print(error, flush=True)

    return True


def twiga_index_creator(
    database_file_path: str,
    index_bins:         int
) -> bool:
    """Create the index database schema with sharded hash tables."""

    duckdb_index_connection = duckdb.connect()

    duckdb_index_connection.execute(f"ATTACH '{database_file_path}' AS index")

    duckdb_index_connection.execute(
        """
            CREATE TABLE index.word_counts (
                text_id     INTEGER PRIMARY KEY,
                words_total INTEGER
            )
        """
    )

    duckdb_index_connection.execute(
        'CREATE SEQUENCE index.hash_id_sequence START 1'
    )

    for bin_number in range(1, index_bins + 1):
        duckdb_index_connection.execute(
            f"""
                CREATE TABLE index.bin_{str(bin_number)}_hash_dict (
                    hash    VARCHAR PRIMARY KEY,
                    hash_id INTEGER UNIQUE
                )
            """
        )

        duckdb_index_connection.execute(
            f"""
                CREATE TABLE index.bin_{str(bin_number)}_hash_index (
                    hash_id   INTEGER,
                    text_id   INTEGER,
                    positions INTEGER[]
                )
            """
        )

    duckdb_index_connection.close()

    return True


def twiga_index_writer(
    index_database_file_path: str,
    text_id_list:             list,
    text_list:                list,
    bins_total:               int,
    stopword_set:             set,
    hasher_batch_maximum:     int
) -> tuple[int, int]:
    """Tokenize, hash, and write word index entries for a batch of texts."""

    normalizer = normalizers.Sequence(
        [
            normalizers.NFD(),          # Decompose Unicode characters
            normalizers.StripAccents(), # Remove accents after decomposition
            normalizers.Lowercase()     # Convert to lowercase
        ]
    )

    pre_tokenizer = pre_tokenizers.Sequence(
        [
            pre_tokenizers.Whitespace(),
            pre_tokenizers.Punctuation(behavior='removed'),
            pre_tokenizers.Digits(individual_digits=False)
        ]
    )

    normalized_texts = [
        normalizer.normalize_str(text)
        for text in text_list
    ]

    pre_tokenized_texts = [
        pre_tokenizer.pre_tokenize_str(text)
        for text in normalized_texts
    ]

    del normalized_texts
    gc.collect()

    text_words_list = [
        [
            word_tuple[0]
            for word_tuple in pre_tokenized_text
            if word_tuple[0] not in stopword_set
        ]
        for pre_tokenized_text in pre_tokenized_texts
    ]

    del pre_tokenized_texts
    gc.collect()

    # Split texts into batches by word count to control memory usage during hashing.
    # Each batch contains [text_ids[], word_lists[]] up to hasher_batch_maximum words.
    hasher_batches       = []
    current_hasher_batch = [[], []]
    current_word_count   = 0

    for text_id, word_list in zip(text_id_list, text_words_list):
        words_number = len(word_list)

        if current_word_count + words_number > hasher_batch_maximum:
            if current_hasher_batch:
                hasher_batches.append(current_hasher_batch)

            current_hasher_batch = [[], []]
            current_hasher_batch[0].append(text_id)
            current_hasher_batch[1].append(word_list)

            current_word_count = words_number
        else:
            current_hasher_batch[0].append(text_id)
            current_hasher_batch[1].append(word_list)

            current_word_count += words_number

    # Don't forget the last batch:
    if current_hasher_batch:
        hasher_batches.append(current_hasher_batch)

    del text_id_list
    del text_list
    gc.collect()

    text_hasher_arguments = [
        (
            batch[0],
            batch[1],
            index_bins
        )
        for batch in hasher_batches
    ]

    del hasher_batches
    gc.collect()

    results_data = None

    with get_context('spawn').Pool(cpu_count()) as hashing_process_pool:
        results = hashing_process_pool.starmap_async(
            twiga_index_hasher,
            text_hasher_arguments,
            error_callback=twiga_hasher_error_callback
        )

        results.wait()

        results_data = results.get()

    texts_total = sum([result[0] for result in results_data])
    words_total = sum([result[1] for result in results_data])

    hashes_list             = [result[2] for result in results_data]
    word_counts_nested_list = [result[3] for result in results_data]

    # Combine all hash dictionaries from the different threads:
    hashes_defaultdict = defaultdict(list)

    all_keys = set(chain(*[dictionary.keys() for dictionary in hashes_list]))

    for key in all_keys:
        values = chain(*[dictionary.get(key, []) for dictionary in hashes_list])
        hashes_defaultdict[key] = list(values)

    hashes = dict(hashes_defaultdict)

    word_counts = list(chain.from_iterable(word_counts_nested_list))
    word_counts_table = pa.Table.from_pylist(word_counts)

    duckdb_index_connection = duckdb.connect()

    duckdb_index_connection.execute(
        f"ATTACH '{index_database_file_path}' AS index"
    )

    duckdb_index_connection.execute("SET preserve_insertion_order = false")

    duckdb_index_connection.execute("BEGIN TRANSACTION")

    duckdb_index_connection.execute(
        """
            INSERT INTO index.word_counts
            SELECT *
            FROM word_counts_table
        """
    )

    duckdb_index_connection.execute("COMMIT")

    hashes_batch_list = twiga_dict_splitter(
        hashes,
        cpu_count()
    )

    del hashes
    gc.collect()

    index_tables_writer_arguments = [
        (
            duckdb_index_connection,
            hashes_thread_dict
        )
        for hashes_thread_dict in hashes_batch_list
    ]

    thread_pool = ThreadPool(cpu_count())

    results = thread_pool.starmap_async(
        twiga_index_table_writer,
        index_tables_writer_arguments
    )

    results.get()

    del hashes_batch_list

    duckdb_index_connection.execute("CHECKPOINT index")
    duckdb_index_connection.close()

    gc.collect()

    return texts_total, words_total


def twiga_index_hasher(
    text_id_list:    list,
    text_words_list: list,
    index_bins:      int,
) -> tuple[int, int]:
    """Hash words and group them by bin number."""

    texts_total = 0
    words_total = 0

    # Dictionary of lists of dictionaries:
    hashes = {}

    # List of dictionaries:
    word_counts = []

    # Iterate all texts in a batch:
    for text_id, word_list in zip(text_id_list, text_words_list):
        texts_total += 1

        # Hash every word:
        text_word_hash_list = []

        for word in word_list:
            word_hash = hashlib.blake2b(
                word.encode(),
                digest_size=32
            ).hexdigest()

            text_word_hash_list.append(word_hash)

        words_count_record = {}

        words_count_record['text_id']     = int(text_id)
        words_count_record['words_total'] = len(word_list)

        word_counts.append(words_count_record)

        # Dictionary of lists:
        positions = {}

        for position, hashed_word in enumerate(text_word_hash_list):
            if hashed_word not in positions:
                positions[hashed_word] = []

            positions[hashed_word].append(position)

        for hashed_word in text_word_hash_list:
            words_total += 1

            hashed_word_record = {}

            bin_number = (int(hashed_word, 16) % index_bins) + 1

            hashed_word_record['hash']      = str(hashed_word)
            hashed_word_record['text_id']   = int(text_id)
            hashed_word_record['positions'] = positions[hashed_word]

            if bin_number not in hashes:
                hashes[bin_number] = []

            hashes[bin_number].append(hashed_word_record)

    return texts_total, words_total, hashes, word_counts


def twiga_index_table_writer(
    duckdb_index_connection:  object,
    hashes_thread_dict:       dict
) -> bool:
    """Write hash entries to bin-specific index tables in a thread."""

    thread_duckdb_connection = duckdb_index_connection.cursor()

    thread_duckdb_connection.execute("SET threads TO 1")

    for bin_number, bin_data in hashes_thread_dict.items():
        bin_hashes_table = pa.Table.from_pylist(bin_data)

        thread_duckdb_connection.execute("BEGIN TRANSACTION")

        unique_bin_hashes_table = thread_duckdb_connection.sql(
            """
                SELECT hash
                FROM bin_hashes_table
                GROUP BY hash
            """
        ).fetch_arrow_table()

        unknown_bin_hashes_table = thread_duckdb_connection.sql(
            f"""
                SELECT hash
                FROM unique_bin_hashes_table
                EXCEPT
                SELECT hd.hash
                FROM
                    index.bin_{str(bin_number)}_hash_dict AS hd
                    INNER JOIN unique_bin_hashes_table AS ubht
                        ON ubht.hash = hd.hash
            """
        ).fetch_arrow_table()

        new_hashes_table = thread_duckdb_connection.sql(
            """
                SELECT
                    hash,
                    NEXTVAL('index.hash_id_sequence') AS hash_id
                FROM unknown_bin_hashes_table
            """
        ).fetch_arrow_table()

        thread_duckdb_connection.execute(
            f"""
                INSERT INTO index.bin_{str(bin_number)}_hash_dict
                SELECT *
                FROM new_hashes_table
            """
        )

        thread_duckdb_connection.execute(
            f"""
                INSERT INTO index.bin_{str(bin_number)}_hash_index
                SELECT
                    ihd.hash_id AS hash_id,
                    bht.text_id AS text_id,
                    bht.positions AS positions
                FROM
                    bin_hashes_table AS bht
                    LEFT JOIN index.bin_{str(bin_number)}_hash_dict AS ihd
                        ON ihd.hash = bht.hash
            """
        )

        thread_duckdb_connection.execute("COMMIT")

    thread_duckdb_connection.close()

    return True
