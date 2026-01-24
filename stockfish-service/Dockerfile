# syntax=docker/dockerfile:1
FROM python:3.11-slim

ARG STOCKFISH_VERSION=16.1
ENV STOCKFISH_URL="https://github.com/official-stockfish/Stockfish/releases/download/sf_${STOCKFISH_VERSION}/stockfish-ubuntu-x86-64-avx2.tar"

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl; \
    curl -L -o /tmp/stockfish.tar "${STOCKFISH_URL}"; \
    mkdir -p /tmp/stockfish; \
    tar -xf /tmp/stockfish.tar -C /tmp/stockfish; \
    sf_bin="$(find /tmp/stockfish -type f -name 'stockfish*' -perm /111 | head -n 1)"; \
    mv "$sf_bin" /usr/local/bin/stockfish; \
    chmod +x /usr/local/bin/stockfish; \
    rm -rf /tmp/stockfish /tmp/stockfish.tar; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir fastapi uvicorn[standard] python-chess

WORKDIR /app
COPY . /app

EXPOSE 9999
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9999"]
