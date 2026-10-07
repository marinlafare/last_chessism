# Build from the repository root. Credentials are mounted at runtime, never copied.
FROM docker:29.8.2-cli@sha256:b1805116a6a86cc591b5d5f60a910a0715cdcc9d18d866ad68b1457ead25c35c AS docker_cli
FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:587.0.0-stable@sha256:73a97b855d386c74bee3facd59ccc261006c55a9812b7913fa6b93816010a1bc AS google_cli
FROM python:3.11-slim-bookworm@sha256:2333bd330d12de02514770b3585cad313644316047cdee24a7acfdece6de6efb

WORKDIR /app
COPY --from=docker_cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=google_cli /usr/lib/google-cloud-sdk /opt/google-cloud-sdk
ENV PATH="/opt/google-cloud-sdk/bin:${PATH}" \
    CLOUDSDK_PYTHON=/usr/local/bin/python \
    CLOUDSDK_CORE_DISABLE_USAGE_REPORTING=true \
    CLOUDSDK_COMPONENT_MANAGER_DISABLE_UPDATE_CHECK=true \
    CLOUDSDK_CORE_DISABLE_PROMPTS=true \
    PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app:/app/stockfish-batch-worker

COPY requirements.txt /app/requirements.txt
COPY stockfish-batch-worker/requirements.txt /app/worker-requirements.txt
RUN pip install --no-cache-dir -r requirements.txt -r worker-requirements.txt && pip check
COPY constants.py /app/constants.py
COPY chessism_api /app/chessism_api
COPY stockfish-batch-worker/stockfish_batch /app/stockfish-batch-worker/stockfish_batch
COPY stockfish-batch-worker/cloud_job /app/stockfish-batch-worker/cloud_job
COPY stockfish-batch-worker/cleaning_job /app/stockfish-batch-worker/cleaning_job
COPY stockfish-batch-worker/batch /app/stockfish-batch-worker/batch

# The launcher drops to the mounted config owner's UID before reading credentials
# or running the controller. SETUID/SETGID are needed for bootstrap; Compose's
# root init separately needs KILL to forward signals to the non-root child.
ENTRYPOINT ["python", "-m", "cloud_job.run_compose"]
