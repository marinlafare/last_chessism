#!/usr/bin/env bash
# Run from any directory. Explicit opt-in: these are persistent IAM grants.
set -euo pipefail
if [[ "${1:-}" != "--apply-approved-iam" ]]; then
  printf '%s\n' 'Review cloud_job/RECOVERY.md, then pass --apply-approved-iam.'
  exit 2
fi
CHESSISM_GCLOUD="${CHESSISM_GCLOUD:-gcloud}"
CHESSISM_RECOVERY_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
CHESSISM_RECOVERY_SA='chessism-batch-recovery@chessism-production.iam.gserviceaccount.com'
CHESSISM_WORKER_SA='chessism-batch-worker@chessism-production.iam.gserviceaccount.com'
CHESSISM_BUCKET='gs://chessism-batch-276704059200-us-central1'

# A failed read is not treated as absence. Only confirmed NOT_FOUND creates.
exists() {
  local result
  if result=$("$CHESSISM_GCLOUD" "$@" --project=chessism-production --format=none 2>&1); then
    return 0
  elif [[ "$result" == *NOT_FOUND* ]]; then
    return 1
  else
    printf '%s\n' "$result" >&2
    exit 1
  fi
}

"$CHESSISM_GCLOUD" services enable workflows.googleapis.com workflowexecutions.googleapis.com \
  --project=chessism-production --quiet --format=none
if ! exists iam service-accounts describe "$CHESSISM_RECOVERY_SA"; then
  "$CHESSISM_GCLOUD" iam service-accounts create chessism-batch-recovery \
    --display-name='Chessism Batch recovery supervisor' --project=chessism-production --quiet --format=none
fi
for item in 'chessismBatchRecovery:recovery-batch-role.yaml' 'chessismRecoveryStateWriter:recovery-state-role.yaml'; do
  operation=create
  if exists iam roles describe "${item%%:*}"; then operation=update; fi
  "$CHESSISM_GCLOUD" iam roles "$operation" "${item%%:*}" \
    --file="$CHESSISM_RECOVERY_DIR/${item#*:}" --project=chessism-production --quiet --format=none
done
"$CHESSISM_GCLOUD" projects add-iam-policy-binding chessism-production \
  --member="serviceAccount:$CHESSISM_RECOVERY_SA" \
  --role=projects/chessism-production/roles/chessismBatchRecovery --condition=None --quiet --format=none
"$CHESSISM_GCLOUD" iam service-accounts add-iam-policy-binding "$CHESSISM_WORKER_SA" \
  --member="serviceAccount:$CHESSISM_RECOVERY_SA" --role=roles/iam.serviceAccountUser \
  --project=chessism-production --condition=None --quiet --format=none
"$CHESSISM_GCLOUD" storage buckets add-iam-policy-binding "$CHESSISM_BUCKET" \
  --member="serviceAccount:$CHESSISM_RECOVERY_SA" --role=roles/storage.objectViewer \
  --project=chessism-production --condition=None --quiet --format=none
"$CHESSISM_GCLOUD" storage buckets add-iam-policy-binding "$CHESSISM_BUCKET" \
  --member="serviceAccount:$CHESSISM_RECOVERY_SA" \
  --role=projects/chessism-production/roles/chessismRecoveryStateWriter \
  --condition="expression=resource.name.startsWith('projects/_/buckets/chessism-batch-276704059200-us-central1/objects/inputs/ui-') && (resource.name.endsWith('/owner.json') || resource.name.endsWith('/state.json')),title=chessism_recovery_control_only" \
  --project=chessism-production --quiet --format=none
"$CHESSISM_GCLOUD" workflows deploy chessism-batch-recovery \
  --project=chessism-production --location=us-central1 \
  --source="$CHESSISM_RECOVERY_DIR/recovery.yaml" --service-account="$CHESSISM_RECOVERY_SA" \
  --labels=app=chessism,recovery_schema=1 --quiet --format='yaml(name,state,revisionId)'
