#!/usr/bin/env python3

# Core modules:
import os
import signal
import threading
import time

# PIP modules:
from   dotenv  import find_dotenv
from   dotenv  import load_dotenv
import duckdb
from   fastapi import FastAPI
import gradio  as     gr
import uvicorn

# Twiga modules:
from twiga_core_search import twiga_request_hasher
from twiga_core_search import twiga_index_reader
from twiga_core_search import twiga_single_word_searcher
from twiga_core_search import twiga_any_position_searcher
from twiga_core_search import twiga_exact_phrase_searcher
from twiga_text        import twiga_text_reader

# Start the application for local development at http://0.0.0.0:7860/ using:
# docker run --rm -it -p 7860:7860 \
# --user $(id -u):$(id -g) -v $PWD:/app \
# twiga-demo python /app/demo_searcher.py

# Global variable for scale-to-zero capability after a period of inactivity:
last_activity = None

# Global variables for DuckDB connections:
duckdb_index_connection = None
duckdb_text_connection  = None

# Load settings from .env file:
load_dotenv(find_dotenv())


def text_searcher(
    search_request: str,
    results_number: str,
    search_method:  str = 'exact_phrase'
) -> tuple[dict, dict]:
    """
    Search for texts matching a search query.

    Args:
        search_request: The search query containing one or more words.
        results_number: Maximum number of search results to return.
        search_method: either 'exact_phrase' or 'any_position'.
            'exact_phrase' finds texts with consecutive word sequences.
            'any_position' finds texts with words in any order.

    Returns:
        A tuple containing:
        - info: Dictionary with search performance metrics (timing data)
        - search_result: Dictionary with matching texts, keyed by result number
    """

    # Update the timestamp of the last activity:
    global last_activity
    last_activity = time.time()

    # Use the global DuckDB connections:
    global duckdb_index_connection
    global duckdb_text_connection

    # Start measuring the search time:
    search_start = time.time()

    # Hash the search request:
    hash_list = twiga_request_hasher(search_request)

    index_bins = int(os.environ['INDEX_BINS'])
    text_bins  = int(os.environ['TEXT_BINS'])

    text_id_table = None

    # Single-word search:
    if len(hash_list) == 1:
        text_id_table = twiga_single_word_searcher(
            duckdb_index_connection,
            index_bins,
            hash_list[0],
            results_number
        )

    hash_id_list, hash_table = twiga_index_reader(
        duckdb_index_connection,
        index_bins,
        hash_list
    )

    # Multiple words search:
    if hash_table is not None:
        if search_method == 'any_position':
            text_id_table = twiga_any_position_searcher(
                duckdb_index_connection,
                hash_table,
                hash_id_list,
                results_number
            )
        else:
            text_id_table = twiga_exact_phrase_searcher(
                duckdb_index_connection,
                hash_table,
                hash_id_list,
                results_number
            )

    search_time = round((time.time() - search_start), 3)

    # Extract all matching texts:
    text_extraction_start = time.time()

    search_result_dataframe = None

    if text_id_table is not None:
        search_result_table = twiga_text_reader(
            duckdb_text_connection,
            text_bins,
            text_id_table
        )

        search_result_dataframe = search_result_table.to_pandas()

    search_result = {}

    if search_result_dataframe is None:
        search_result['Message:'] = 'No matching texts were found.'

    # Convert dataframe rows to dict keyed by 1-based index for JSON output
    if search_result_dataframe is not None:
        search_result_index = range(1, len(search_result_dataframe) + 1)
        search_result_list = search_result_dataframe.to_dict('records')

        for index, element in zip(search_result_index, search_result_list):
            search_result[str(index)] = element

    text_extraction_time = round((time.time() - text_extraction_start), 3)
    total_time           = round((search_time + text_extraction_time), 3)

    info = {}

    info['Index Search ..... runtime in seconds'] = search_time
    info['Text Extraction .. runtime in seconds'] = text_extraction_time
    info['Total ............ runtime in seconds'] = total_time

    return info, search_result


def activity_inspector():
    """Self-updating timer terminating the app after prolonged inactivity."""

    global last_activity

    # Schedule the next check (recursive timer pattern)
    thread = threading.Timer(
        int(os.environ['INACTIVITY_CHECK_SECONDS']),
        activity_inspector
    )

    thread.daemon = True
    thread.start()

    inactivity_maximum = int(os.environ['INACTIVITY_MAXIMUM_SECONDS'])

    # Send SIGINT to gracefully shut down if idle too long (scale-to-zero)
    if time.time() - last_activity > inactivity_maximum:
        os.kill(os.getpid(), signal.SIGINT)


def main():
    """Main function to start Gradio demo application."""

    # Matplotlib writable config directory,
    # Matplotlib is a dependency of Gradio:
    os.environ['MPLCONFIGDIR'] = '/app/data/.config/matplotlib'

    # Disable Gradio telemetry:
    os.environ['GRADIO_ANALYTICS_ENABLED'] = 'False'

    # Initialize DuckDB connections:
    global duckdb_index_connection
    duckdb_index_connection = duckdb.connect('/app/data/twiga_index.duckdb')

    global duckdb_text_connection
    duckdb_text_connection = duckdb.connect('/app/data/twiga_texts.duckdb')

    # Get the total number of texts in the index:
    statistics_table = duckdb_index_connection.query(
        """
            SELECT
                COUNT(text_id)   AS texts_total,
                SUM(words_total) AS words_total
            FROM word_counts
        """
    ).fetch_arrow_table()

    texts_total = statistics_table.column('texts_total')[0].as_py()
    words_total = statistics_table.column('words_total')[0].as_py()

    # Define the Gradio user interface:
    request_box = gr.Textbox(lines=1, label='Search Request')

    results_number = gr.Dropdown(
        [10, 20, 50, 100],
        label='Maximum Number of Search Results',
        value=10
    )

    search_method_radio = gr.Radio(
        ['exact_phrase', 'any_position'],
        label='Search Method',
        value='exact_phrase'
    )

    info_box = gr.JSON(label='Search Info', show_label=True)

    results_box = gr.JSON(label='Search Results', show_label=True)

    # Dark theme by default:
    javascript_code = '''
        function refresh() {
            const url = new URL(window.location);

            if (url.searchParams.get('__theme') !== 'dark') {
                url.searchParams.set('__theme', 'dark');
                window.location.href = url.href;
            }
        }
    '''

    # CSS styling:
    css_code = '''
        a:link {
            color: white;
            text-decoration: none;
        }

        a:visited {
            color: white;
            text-decoration: none;
        }

        a:hover {
            color: white;
            text-decoration: none;
        }

        a:active {
            color: white;
            text-decoration: none;
        }

        .dark {font-size: 16px !important}
    '''

    # Initialize Gradio interface:
    gradio_interface = gr.Blocks(
        theme=gr.themes.Glass(
            font=[gr.themes.GoogleFont('Open Sans')],
            font_mono=[gr.themes.GoogleFont('Roboto Mono')]
        ),
        js=javascript_code,
        css=css_code,
        title='Twiga'
    )

    with gradio_interface:
        with gr.Row():
            gr.Markdown(
                '''
                # Twiga
                ## Lexical Search using Standard SQL Tables
                '''
            )

        with gr.Row():
            with gr.Column(scale=30):
                gr.Markdown(
                    '''
                    **Repository:** https://github.com/ddmitov/twiga  
                    **License:** Apache License 2.0.  
                    '''
                )

            with gr.Column(scale=40):
                gr.Markdown(
                    '''
                    **Dataset:** Common Crawl News  
                    https://huggingface.co/datasets/stanford-oval/ccnews  
                    '''
                )

            with gr.Column(scale=30):
                gr.Markdown(
                    f'''
                    **Total texts:** {texts_total}  
                    **Total words:** {words_total}  
                    '''
                )

        with gr.Row():
            request_box.render()

        with gr.Row():
            with gr.Column(scale=1):
                results_number.render()

            with gr.Column(scale=1):
                search_method_radio.render()

            with gr.Column(scale=3):
                gr.Examples(
                    [
                        'околна среда',
                        'international trade relations',
                        'used car market report',
                        'renewable energy sources',
                        'global economic outlook',
                        'head of the European Central Bank',
                    ],
                    fn=text_searcher,
                    inputs=request_box,
                    outputs=results_box,
                    examples_per_page=11,
                    cache_examples=False
                )

        with gr.Row():
            search_button = gr.Button('Search')

            gr.ClearButton(
                [
                    info_box,
                    request_box,
                    results_box
                ]
            )

        with gr.Row():
            info_box.render()

        with gr.Row():
            results_box.render()

        gr.on(
            triggers=[request_box.submit, search_button.click],
            fn=text_searcher,
            inputs=[request_box, results_number, search_method_radio],
            outputs=[info_box, results_box],
        )

    gradio_interface.show_api = False
    gradio_interface.queue()

    fastapi_app = FastAPI()

    fastapi_app = gr.mount_gradio_app(
        fastapi_app,
        gradio_interface,
        path='/'
    )

    # Update last activity date and time:
    global last_activity
    last_activity = time.time()

    # Start activity inspector in a separate thread
    # to implement scale-to-zero capability,
    # i.e. when there is no user activity for a predefined amount of time
    # the application will shut down.
    activity_inspector()

    try:
        uvicorn.run(
            fastapi_app,
            host = '0.0.0.0',
            port = 7860
        )
    except (KeyboardInterrupt, SystemExit):
        print('\n')

        exit(0)


if __name__ == '__main__':
    main()
