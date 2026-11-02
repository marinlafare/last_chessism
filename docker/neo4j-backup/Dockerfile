FROM python:3.11-slim@sha256:673719e071ecf27434fd2635bbd8d00f49f4fdd81f243143438a42ced8bedf60
RUN pip install --no-cache-dir docker==7.1.0
WORKDIR /app
COPY chessism_api/__init__.py /app/chessism_api/__init__.py
COPY chessism_api/database/__init__.py /app/chessism_api/database/__init__.py
COPY chessism_api/database/neo4j /app/chessism_api/database/neo4j
COPY chessism_api/operations/__init__.py /app/chessism_api/operations/__init__.py
COPY chessism_api/operations/backup_files.py /app/chessism_api/operations/backup_files.py
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
CMD ["python", "-m", "chessism_api.database.neo4j.backup_controller"]
