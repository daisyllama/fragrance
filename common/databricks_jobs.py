"""
Trigger a one-time Databricks notebook run via the Jobs API and wait for it
to finish. Used by streamlit_app/app.py to run
notebooks/05_generate_embeddings_for_new_frag.py after adding a fragrance,
instead of running the sentence-transformers model locally (avoids adding
torch/sentence-transformers, ~1-2GB, to the app's dependencies).

No persistent job or cluster is created: submitting a task with no cluster
spec runs on serverless job compute (billed only for the run itself), which
matches the no-standing-infra approach used throughout this project. If
serverless job compute isn't enabled for the workspace, pass cluster_id to
target an existing all-purpose cluster instead.
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import NotebookTask, Run, RunResultState, SubmitTask


def trigger_embedding_job(host: str, token: str, notebook_path: str, cluster_id: str = None) -> Run:
    """Submits notebook_path as a one-time run and blocks until it
    finishes. Raises RuntimeError if the run didn't succeed. Returns the
    completed Run."""
    client = WorkspaceClient(host=host, token=token)

    task_kwargs = {}
    if cluster_id:
        task_kwargs["existing_cluster_id"] = cluster_id

    task = SubmitTask(
        task_key="generate_embeddings",
        notebook_task=NotebookTask(notebook_path=notebook_path),
        **task_kwargs,
    )

    run = client.jobs.submit(run_name="add_fragrance_generate_embeddings", tasks=[task]).result()

    result_state = run.state.result_state if run.state else None
    if result_state != RunResultState.SUCCESS:
        message = run.state.state_message if run.state else "unknown error"
        raise RuntimeError(f"Embedding job did not succeed ({result_state}): {message}")

    return run
