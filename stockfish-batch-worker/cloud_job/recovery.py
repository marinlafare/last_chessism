"""Cloud-resident recovery bridge; no retries depend on the host staying online."""
import json
import re

from cleaning_job.cloud import BUCKET, PROJECT, PROJECT_NUMBER, REGION
from stockfish_batch.checkpoints import digest, encode

WORKFLOW_ID = "chessism-batch-recovery"
WORKFLOW = f"projects/{PROJECT}/locations/{REGION}/workflows/{WORKFLOW_ID}"
SERVICE_ACCOUNT = f"chessism-batch-recovery@{PROJECT}.iam.gserviceaccount.com"
MAX_CONTROL_BYTES = 512 * 1024


def execution_identity(name):
    # Google returns the numeric project in execution names; our durable owner
    # documents use the human-readable project ID. Both identify the same scope.
    if isinstance(name, str):
        name = name.replace(f"projects/{PROJECT_NUMBER}/", f"projects/{PROJECT}/", 1)
    if not isinstance(name, str) or not re.fullmatch(re.escape(WORKFLOW) + r"/executions/[a-f0-9-]{36}", name):
        raise ValueError("Invalid recovery execution identity")
    return name


def control_prefix(run_id, session):
    if not re.fullmatch(r"[a-f0-9]{32}", run_id) or type(session) is not int or not 1 <= session <= 3:
        raise ValueError("Invalid recovery identity")
    return f"gs://{BUCKET}/inputs/ui-{run_id}/recovery-{session}/"


def validate_state(state, run_id, session, spec_hash):
    if (not isinstance(state, dict) or state.get("version") != 1 or state.get("run_id") != run_id
            or state.get("session") != session or state.get("spec_hash") != spec_hash
            or not re.fullmatch(re.escape(WORKFLOW) + r"/executions/[a-f0-9-]{36}", state.get("execution", ""))):
        raise ValueError("Recovery state does not match this run")
    jobs, uids = state.get("jobs"), state.get("uids")
    if (not isinstance(jobs, list) or not isinstance(uids, dict) or len(set(jobs)) != len(jobs)
            or any(job != f"chessism-ui-{run_id}-{index + 1}" for index, job in enumerate(jobs))
            or set(uids) != set(jobs) or any(not isinstance(uid, str) or not uid for uid in uids.values())
            or (state.get("current_job") is not None and state["current_job"] not in uids)):
        raise ValueError("Invalid recovery job inventory")
    if state.get("status") not in {"STARTING", "RUNNING", "RETRYING", "SUCCEEDED", "FAILED", "CANCELLED"}:
        raise ValueError("Invalid recovery status")
    for key in ("application_failures", "preemptions"):
        if type(state.get(key)) is not int or state[key] < 0:
            raise ValueError("Invalid recovery counters")
    return state


class RecoveryClient:
    def require_recovery(self):
        workflow = self.request("workflows", WORKFLOW)
        if (workflow.get("state") != "ACTIVE" or workflow.get("labels", {}).get("recovery_schema") != "1"
                or workflow.get("serviceAccount") not in {
                    SERVICE_ACCOUNT, f"projects/{PROJECT}/serviceAccounts/{SERVICE_ACCOUNT}"}):
            raise ValueError("Deploy the approved Chessism recovery workflow before creating cloud jobs")

    def recovery_document(self, run_id, session, name):
        raw = self.storage().read(control_prefix(run_id, session) + name, MAX_CONTROL_BYTES)
        return None if raw is None else json.loads(raw)

    def recovery_executions(self, run_id, session):
        return [execution for page in self.pages("executions", WORKFLOW + "/executions",
                filter=f'labels."run_id":"{run_id}" AND labels."session":"{session}"', view="FULL")
                for execution in page.get("executions", [])]

    def start_recovery(self, run_id, launch):
        session = launch["recovery_session"]
        control_prefix(run_id, session)
        spec_hash = digest(encode(launch["spec"]))
        argument = {"run_id": run_id, "session": session, "spec": launch["spec"],
                    "spec_hash": spec_hash, "prior_jobs": launch.get("prior_jobs", []),
                    "prior_uids": launch.get("prior_uids", {})}
        # A lost create response must not cause independent controllers. The
        # workflow also has a GCS create-only ownership claim as a second fence.
        existing = self.recovery_executions(run_id, session)
        for execution in existing:
            if json.loads(execution.get("argument", "null")) != argument:
                raise ValueError("Existing recovery execution has different arguments")
        if existing:
            return execution_identity(existing[0]["name"])
        execution = self.request("executions", WORKFLOW + "/executions", method="POST", body={
            "argument": json.dumps(argument), "callLogLevel": "LOG_NONE",
            "labels": {"app": "chessism", "run_id": run_id, "session": str(session)},
        })
        return execution_identity(execution["name"])

    def recovery_snapshot(self, run_id, launch):
        session, expected_hash = launch["recovery_session"], digest(encode(launch["spec"]))
        owner = self.recovery_document(run_id, session, "owner.json")
        execution_name = launch["recovery_execution"]
        if owner:
            if owner.get("spec_hash") != expected_hash:
                raise ValueError("Recovery owner belongs to a different worker contract")
            execution_name = owner.get("execution", "")
        execution_name = execution_identity(execution_name)
        execution = self.request("executions", execution_name)
        state = self.recovery_document(run_id, session, "state.json")
        if state is not None:
            validate_state(state, run_id, session, expected_hash)
            if state["execution"] != execution_name:
                raise ValueError("Recovery owner and state differ")
        if execution["state"] in {"FAILED", "CANCELLED", "UNAVAILABLE"}:
            raise ValueError("Recovery supervisor stopped unexpectedly; checkpoints retained. "
                             "Check the existing Batch job before any manual resubmission.")
        if execution["state"] == "SUCCEEDED" and (not state or state["status"] not in {"SUCCEEDED", "FAILED", "CANCELLED"}):
            raise ValueError("Recovery execution finished without a terminal state")
        return state, execution

    def cancel_recovery(self, run_id, session):
        self.storage().create(control_prefix(run_id, session) + "cancel.json",
                              encode({"run_id": run_id, "session": session, "cancelled": True}))

    def require_recovery_stopped(self, run_id, launch):
        # Must be called before planning deletion AND again on cleanup resume.
        # Do not read the state file here: a previous partial cleanup may have
        # deleted it. The verified terminal snapshot is already durable locally.
        state = launch.get("recovery_summary", {})
        validate_state(state, run_id, launch["recovery_session"], digest(encode(launch["spec"])))
        if state["status"] != "SUCCEEDED":
            raise ValueError("Recovery has not completed; refusing cleanup")
        execution = self.request("executions", state["execution"])
        if execution["state"] != "SUCCEEDED":
            raise ValueError("Recovery may still write or launch work; refusing cleanup")
        # A lost creation response can leave a second execution queued. Keep
        # owner fences until ALL sessions' duplicates have stopped; otherwise a
        # delayed duplicate could acquire a deleted fence and recreate the job.
        for session in range(1, launch["recovery_session"] + 1):
            for peer in self.recovery_executions(run_id, session):
                if peer.get("state") not in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                    raise ValueError("Recovery duplicate may still run; refusing cleanup")
