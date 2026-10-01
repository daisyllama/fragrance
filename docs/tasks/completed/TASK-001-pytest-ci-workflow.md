# TASK-001: Run pytest in CI via GitHub Actions

- **Status:** completed
- **Created:** 2026-10-01
- **Branch:** `ci/pytest-upgrade`

## Description

The repo has a pytest suite (`tests/`, configured by `pytest.ini` with `pythonpath = .`, `testpaths = tests`) but no CI at all — there is no `.github/` directory. Tests are only run by hand. Add a GitHub Actions workflow that runs the suite on every push and pull request so regressions in `common/` (shared by the notebooks and `streamlit_app/`) are caught before merge.

## Scope

In scope:
- `.github/workflows/tests.yml` — runs `pytest` on `push` to `main` and on `pull_request` targeting `main` (plus `workflow_dispatch` for manual reruns).
- A lightweight test-dependency file (e.g. `requirements-dev.txt`) listing only what `common/` + `tests/` import, plus `pytest`.
- A short "Running tests" note in `README.md` (local command + CI badge).

Out of scope:
- Deploying anything to Databricks (`databricks bundle deploy`), running notebooks, or calling the live workspace.
- Linting/type-checking (ruff, mypy) — possible follow-up task.
- Coverage thresholds — possible follow-up.

## Context from review

- **Dependencies (⚠️ risk):** Root `requirements.txt` is the Databricks/notebook env — it pulls `pyspark` and `sentence-transformers` (multi-GB with torch), which no test needs, and it is *missing* `beautifulsoup4` (imported by `common/scraping.py`) and `pytest`. Installing it in CI would be slow and still fail. Test imports actually need: `pytest`, `numpy`, `pandas`, `rapidfuzz`, `requests`, `beautifulsoup4`, `databricks-sdk`. Use a dedicated `requirements-dev.txt` rather than root `requirements.txt`.
- **Security:** Tests are fully mocked (`unittest.mock.patch` on `requests`/`WorkspaceClient`; scraper uses an inline HTML fixture, not the gitignored `scraping/fixtures/*.html`). CI must need **no secrets** — no Databricks host/token. Set `permissions: contents: read` on the workflow. Pin actions to major versions (`actions/checkout@v4`, `actions/setup-python@v5`).
- **Performance/cost:** Enable pip caching via `setup-python`'s `cache: pip` (keyed on `requirements-dev.txt`). Add `concurrency` with `cancel-in-progress: true` for PR runs. Single Python version is sufficient for a personal project.
- **Python version:** Use 3.12 (matches local dev). ✨ Optionally verify against the Databricks serverless runtime's Python version and match it if different.
- **Architecture:** Keep `pytest.ini` as the single source of pytest config; the workflow should just call `pytest` from the repo root.

## Acceptance criteria

1. `.github/workflows/tests.yml` exists and triggers on push to `main`, PRs to `main`, and manual dispatch.
2. Workflow installs only `requirements-dev.txt` (not root `requirements.txt`) and runs `pytest` from the repo root.
3. All existing tests (`test_add_fragrance`, `test_cleaning`, `test_databricks_jobs`, `test_matching`, `test_scraping`, `test_similarity`) pass in CI with no secrets configured.
4. `pip install -r requirements-dev.txt && pytest` passes locally in a fresh venv.
5. Workflow has read-only `contents` permission, pip caching, and a concurrency group.
6. README documents how to run tests locally and shows the workflow status badge.
7. A PR from `ci/pytest-upgrade` shows a green check.

## Testing

- Local: fresh venv → install `requirements-dev.txt` → `pytest` green.
- CI: push the branch / open a PR and confirm the run passes; deliberately break one assertion once (then revert) to confirm a failure turns the check red.

## Notes for execute-task

- ⚠️ `.gitignore` contains `*.json`, which would ignore `docs/tasks/tasks.json`. Either add a `!docs/tasks/tasks.json` negation or `git add -f` it — decide and record in the decision doc.
- ✨ Follow-ups to consider queuing: ruff lint step, branch protection requiring this check, coverage report.

## Progress (2026-10-01)

- AC 1–6 done. See [decision doc](../decisions/TASK-001-pytest-ci-workflow.md).
- AC 7 met: PR #1 check passed (run 36810758556), merged to main as 5fc0817.
