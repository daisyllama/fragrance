from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk.service.jobs import RunResultState

from common.databricks_jobs import trigger_embedding_job


def _mock_client_returning(result_state, state_message="done"):
    """Builds a WorkspaceClient mock whose jobs.submit(...).result() call
    returns a Run-shaped mock with the given terminal state."""
    run = MagicMock()
    run.state.result_state = result_state
    run.state.state_message = state_message

    client = MagicMock()
    client.jobs.submit.return_value.result.return_value = run
    return client, run


class TestTriggerEmbeddingJob:
    def test_returns_the_run_on_success(self):
        client, run = _mock_client_returning(RunResultState.SUCCESS)
        with patch("common.databricks_jobs.WorkspaceClient", return_value=client):
            result = trigger_embedding_job(host="h", token="t", python_file="/f.py")
        assert result is run

    def test_raises_runtime_error_when_run_fails(self):
        client, _ = _mock_client_returning(RunResultState.FAILED, state_message="ModuleNotFoundError")
        with patch("common.databricks_jobs.WorkspaceClient", return_value=client):
            with pytest.raises(RuntimeError, match="ModuleNotFoundError"):
                trigger_embedding_job(host="h", token="t", python_file="/f.py")

    def test_raises_runtime_error_when_run_state_is_missing_entirely(self):
        run = MagicMock()
        run.state = None
        client = MagicMock()
        client.jobs.submit.return_value.result.return_value = run
        with patch("common.databricks_jobs.WorkspaceClient", return_value=client):
            with pytest.raises(RuntimeError, match="unknown error"):
                trigger_embedding_job(host="h", token="t", python_file="/f.py")

    def test_connects_with_the_given_host_and_token(self):
        client, _ = _mock_client_returning(RunResultState.SUCCESS)
        with patch("common.databricks_jobs.WorkspaceClient", return_value=client) as mock_ctor:
            trigger_embedding_job(host="my-host", token="my-token", python_file="/f.py")
        mock_ctor.assert_called_once_with(host="my-host", token="my-token")

    def test_submits_the_given_python_file_as_a_spark_python_task(self):
        client, _ = _mock_client_returning(RunResultState.SUCCESS)
        with patch("common.databricks_jobs.WorkspaceClient", return_value=client):
            trigger_embedding_job(host="h", token="t", python_file="/Workspace/jobs/x.py")
        _, kwargs = client.jobs.submit.call_args
        task = kwargs["tasks"][0]
        assert task.spark_python_task.python_file == "/Workspace/jobs/x.py"

    def test_no_cluster_id_means_no_existing_cluster_id_set_on_the_task(self):
        # This is what makes it run on serverless job compute.
        client, _ = _mock_client_returning(RunResultState.SUCCESS)
        with patch("common.databricks_jobs.WorkspaceClient", return_value=client):
            trigger_embedding_job(host="h", token="t", python_file="/f.py")
        _, kwargs = client.jobs.submit.call_args
        task = kwargs["tasks"][0]
        assert task.existing_cluster_id is None

    def test_no_cluster_id_sets_an_environment_key_and_environment_spec(self):
        # Serverless job compute requires every non-notebook task to
        # reference a defined environment, or submit fails with
        # "An environment is required for serverless task ...".
        client, _ = _mock_client_returning(RunResultState.SUCCESS)
        with patch("common.databricks_jobs.WorkspaceClient", return_value=client):
            trigger_embedding_job(host="h", token="t", python_file="/f.py")
        _, kwargs = client.jobs.submit.call_args
        task = kwargs["tasks"][0]
        assert task.environment_key is not None
        environments = kwargs["environments"]
        assert environments is not None
        assert environments[0].environment_key == task.environment_key
        assert "sentence-transformers" in environments[0].spec.dependencies

    def test_cluster_id_is_set_on_the_task_when_given(self):
        client, _ = _mock_client_returning(RunResultState.SUCCESS)
        with patch("common.databricks_jobs.WorkspaceClient", return_value=client):
            trigger_embedding_job(host="h", token="t", python_file="/f.py", cluster_id="0000-1111-abcd")
        _, kwargs = client.jobs.submit.call_args
        task = kwargs["tasks"][0]
        assert task.existing_cluster_id == "0000-1111-abcd"

    def test_cluster_id_means_no_environment_is_required(self):
        # An existing all-purpose cluster already has its own environment —
        # only the serverless path needs one defined.
        client, _ = _mock_client_returning(RunResultState.SUCCESS)
        with patch("common.databricks_jobs.WorkspaceClient", return_value=client):
            trigger_embedding_job(host="h", token="t", python_file="/f.py", cluster_id="0000-1111-abcd")
        _, kwargs = client.jobs.submit.call_args
        task = kwargs["tasks"][0]
        assert task.environment_key is None
        assert kwargs["environments"] is None

    def test_no_real_network_call_is_made(self):
        # Sanity check on the test setup: WorkspaceClient is fully replaced,
        # so nothing here can reach a real Databricks workspace.
        client, _ = _mock_client_returning(RunResultState.SUCCESS)
        with patch("common.databricks_jobs.WorkspaceClient", return_value=client) as mock_ctor:
            trigger_embedding_job(host="h", token="t", python_file="/f.py")
        assert mock_ctor.called
        client.jobs.submit.assert_called_once()
