# syntax=docker/dockerfile:1
FROM python:3.11-slim-bookworm@sha256:2333bd330d12de02514770b3585cad313644316047cdee24a7acfdece6de6efb AS build
ARG TARGETARCH
RUN test "$TARGETARCH" = amd64
WORKDIR /build
COPY requirements.txt .
RUN python -m pip wheel --no-cache-dir --wheel-dir=/wheels -r requirements.txt
COPY scripts/fetch_engine.py ./fetch_engine.py
RUN python fetch_engine.py /engine

FROM python:3.11-slim-bookworm@sha256:2333bd330d12de02514770b3585cad313644316047cdee24a7acfdece6de6efb AS runtime
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 STOCKFISH_PATH=/usr/local/bin/stockfish
WORKDIR /app
COPY requirements.txt .
RUN --mount=type=bind,from=build,source=/wheels,target=/wheels \
    python -m pip install --no-cache-dir --no-index --find-links=/wheels -r requirements.txt \
    && python -m pip check
COPY --from=build /engine/stockfish /usr/local/bin/stockfish
COPY --from=build /engine/source /usr/local/share/stockfish
COPY stockfish_batch ./stockfish_batch
COPY benchmark_fen_batches ./benchmark_fen_batches
RUN groupadd --gid 10001 worker && useradd --uid 10001 --gid worker --no-create-home worker
USER 10001:10001
ENTRYPOINT ["python", "-m", "stockfish_batch"]
