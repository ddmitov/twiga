FROM python:3.12-slim

# Twiga dependencies:
RUN pip install --no-cache \
    duckdb                 \
    pyarrow                \
    tokenizers

# Demo-related dependencies:
RUN pip install --no-cache \
    datasets               \
    "gradio <= 5.34.0"     \
    pandas                 \
    python-dotenv

RUN apt-get update && apt-get install -y curl

RUN mkdir /home/twiga

RUN curl -o /home/twiga/stopwords-iso.json \
    https://raw.githubusercontent.com/stopwords-iso/stopwords-iso/master/stopwords-iso.json

# Twiga files:
COPY ./twiga_core.py   /home/twiga/twiga_core.py
COPY ./twiga_text.py   /home/twiga/twiga_text.py

# Demo application files:
COPY ./.env             /home/twiga/.env
COPY ./demo_searcher.py /home/twiga/demo_searcher.py

# Start the demo application by default:
EXPOSE 7860
CMD ["python", "/home/twiga/demo_searcher.py"]

# docker build -t twiga-demo .
# docker buildx build -t twiga-demo .
