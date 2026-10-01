# Decisions: TASK-001 — Run pytest in CI via GitHub Actions

Task: [TASK-001](../in-progress/TASK-001-pytest-ci-workflow.md) · Branch: `ci/pytest-upgrade` · Date: 2026-10-01

## 1. Separate `requirements-dev.txt` for test dependencies

**Decision:** CI installs a new `requirements-dev.txt` (pytest, numpy, pandas, rapidfuzz, requests, beautifulsoup4, databricks-sdk), not the root `requirements.txt`.

**Why:** Root `requirements.txt` describes the Databricks/notebook environment. It pulls `pyspark` and `sentence-transformers` (with torch, several GB), which no test imports, and it is missing `beautifulsoup4` (needed by `common/scraping.py`) and `pytest`. The dev list was derived from the actual imports in `common/*.py` and `tests/*.py`.

**Alternatives rejected:**
- *Install root `requirements.txt` + extras*: minutes of extra install time per run for unused packages, and it would still need bs4/pytest added.
- *Move deps into `pyproject.toml` with optional extras*: cleaner long-term, but the repo has no packaging setup and adding one is out of scope.

**Implications:** If `common/` gains a new import, it must be added to `requirements-dev.txt` or CI will fail with `ModuleNotFoundError`. That failure is loud and easy to diagnose. The root `requirements.txt` gap (missing bs4) is left as-is because it belongs to the notebook environment.

## 2. Workflow shape

**Decision:** One job on `ubuntu-latest`, Python 3.12, triggered on push to `main`, PRs to `main`, and `workflow_dispatch`. It has `permissions: contents: read`, pip caching keyed on `requirements-dev.txt`, and a per-ref concurrency group with `cancel-in-progress`.

**Why:** Tests are fully mocked, so no secrets or Databricks access are needed and read-only permissions are enough. A single Python version matches local dev (3.12.1) and keeps runs fast for a personal project. Concurrency cancels outdated runs when commits are pushed in quick succession.

**Alternatives rejected:**
- *Version matrix (3.10–3.13)*: no consumer needs multi-version support; it would only multiply runtime.
- *Triggering on all branches' pushes*: would run twice for PR branches (push + pull_request). PR runs cover feature branches.

**Implications:** The Databricks serverless runtime's Python version was not checked. If it differs from 3.12, change `python-version` to match.

## 3. `pytest` invoked bare from repo root

**Decision:** The workflow runs `pytest` with no arguments, so `pytest.ini` (`pythonpath = .`, `testpaths = tests`) stays the single source of config.

## 4. Track `docs/tasks/tasks.json` despite `*.json` ignore rule

**Decision:** Added `!docs/tasks/tasks.json` to `.gitignore`.

**Why:** The `*.json` rule targets generated data exports. The task index is hand-maintained and must be versioned. A negation is permanent and self-documenting.

**Alternative rejected:** `git add -f`. It works once, but the next contributor's edits would be easy to miss, and the intent is invisible.

## Verification

- Fresh venv with `requirements-dev.txt` only, then `pytest`: **74 passed** (all six test modules).
- Workflow YAML parses; triggers and steps as specified.
- Not yet verified: an actual GitHub Actions run (AC 7). Requires pushing `ci/pytest-upgrade` and opening a PR.
