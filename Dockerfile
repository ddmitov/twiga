FROM python:3.12-slim

# Twiga dependencies:
RUN pip install --no-cache \
    numpy                  \
    duckdb                 \
    pyarrow                \
    tokenizers

# Demo-related dependencies:
RUN pip install --no-cache  \
    datasets                \
    "gradio[mcp] <= 5.34.0" \
    pandas                  \
    psutil                  \
    python-dotenv

RUN mkdir /home/twiga

# Twiga files:
COPY ./twiga_core_search.py /home/twiga/twiga_core_search.py
COPY ./twiga_text.py        /home/twiga/twiga_text.py

# Demo application files:
COPY ./.env             /home/twiga/.env
COPY ./demo_searcher.py /home/twiga/demo_searcher.py

# Start the demo application by default:
EXPOSE 7860
CMD ["python", "/home/twiga/demo_searcher.py"]

# docker build -t twiga-demo .
# docker buildx build -t twiga-demo .
