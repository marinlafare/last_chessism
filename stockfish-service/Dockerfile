# syntax=docker/dockerfile:1
FROM python:3.11-slim-bookworm@sha256:2333bd330d12de02514770b3585cad313644316047cdee24a7acfdece6de6efb AS engine
COPY stockfish-batch-worker/scripts/fetch_engine.py /fetch_engine.py
RUN python /fetch_engine.py /engine

FROM python:3.11-slim-bookworm@sha256:2333bd330d12de02514770b3585cad313644316047cdee24a7acfdece6de6efb
COPY --from=engine /engine/stockfish /usr/local/bin/stockfish
COPY --from=engine /engine/source /usr/local/share/stockfish
RUN pip install --no-cache-dir fastapi uvicorn[standard] python-chess==1.999 chess==1.11.2 redis

WORKDIR /app
COPY stockfish-service /app
COPY stockfish-batch-worker/stockfish_core /app/stockfish_core

EXPOSE 9999
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9999"]
