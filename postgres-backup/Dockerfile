FROM postgres:15 AS pgbackrest-base

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends pgbackrest \
    && rm -rf /var/lib/apt/lists/* \
    && install -d -o postgres -g postgres -m 0750 \
        /var/spool/pgbackrest \
        /var/spool/pgbackrest/lock \
        /var/spool/pgbackrest/log \
        /restore-test

COPY postgres-backup/pgbackrest.conf /etc/pgbackrest/pgbackrest.conf
COPY postgres-backup/archive-push.sh /usr/local/bin/pgbackrest-chessism-archive-push
RUN chmod 0755 /usr/local/bin/pgbackrest-chessism-archive-push \
    && chmod 0644 /etc/pgbackrest/pgbackrest.conf


FROM pgbackrest-base AS database


FROM pgbackrest-base AS backup-worker

USER root
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends python3 python3-venv \
    && python3 -m venv /opt/chessism-venv \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/chessism-venv/bin:${PATH}"
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py constants.py worker.py ./
COPY chessism_api/ ./chessism_api

USER postgres
CMD ["arq", "worker.WorkerSettings"]
