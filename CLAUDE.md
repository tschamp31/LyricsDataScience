# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Dependencies are managed with **uv** (`pyproject.toml` + `uv.lock`). `Pipfile` is a legacy leftover, and the pipenv steps in README.md are stale.

```bash
uv sync                                   # install dependencies
uv run python main.py                     # run the pipeline (real DB + YouTube access, see below)
uv run python django_app/manage.py check  # Django system check (safe)
```

**Running `main.py` is not a smoke test.** With no flags it runs the Claude orchestrator against the configured SQL Server and YouTube account. Control it with environment variables:
- `USE_LEGACY_PIPELINE=1` runs the original sequential pipeline instead.
- `AGENT_GOAL="..."` sets the natural-language goal given to the orchestrator.

**Tests:**

```bash
uv run python -m unittest test_e2e_orchestrator                                              # all
uv run python -m unittest test_e2e_orchestrator.TestOrchestratorE2E.test_orchestrator_report_goal  # single
```

- `test_e2e_orchestrator.py` stubs the database and agent `run()` methods, but it still calls the real `claude` CLI, so it needs the CLI installed and authenticated and it spends tokens. It is not an offline unit test.
- `test_imports.py` and `test_json_filter.py` are print-style scripts, not unittest suites. `test_json_filter.py` is broken: it imports `json_combiner`, which does not exist in the repo.
- **Do not run `manage.py test` with `MSSQL_STRING` unset.** `django_app/lyricsdashboard/settings.py` then targets a hardcoded SQL Server host, and the test runner would create a test database there. To use the SQLite fallback, set `MSSQL_STRING` to a value that does not start with `mssql` (the Django test run has not been verified this way).

No lint or formatter config exists. `mypy` is a dependency, but no mypy config is present.

**Environment variables:** `MSSQL_STRING` (SQLAlchemy URL, `mssql+pyodbc://...`), `LYRIC_GENIUS_TOKEN`, `YOUTUBE_OAUTH_FILE` (default `browser.json`), `OLLAMA_HOST`, `OLLAMA_MODEL`.

## Architecture

**Entry point and two pipelines.** `main.py` sets up logging, opens `Database`, and calls `ensure_database_ready()`, which auto-creates missing tables. It then runs `agents.Orchestrator` by default, or the legacy sequential path when `USE_LEGACY_PIPELINE` is set. The legacy path calls the modules directly (`youtube.sync_songs`, `TextAnalyzer`, etc.).

**Database (`database.py`).** SQLAlchemy ORM models for videos, artists, albums, lyrics (Genius and YouTube), sentence and verse splits, `TextAnalysis`, and `TaskExecution`. `TaskExecution` records per-task status (completed, not_found, error, rate_limited) so runs can resume and retry. `Database` provides MERGE-based upserts and `ensure_tables`.

**Orchestrator (`agents/orchestrator.py`).** The Claude CLI is the planner. Each turn runs `claude --print --output-format json --system-prompt ...`, and later turns add `--resume <session_id>`. Claude replies with JSON tool calls, which are dispatched to `BaseAgent` subclasses. The loop ends when Claude returns `{"done": true, "summary": ...}`. Tool schemas are defined in `_TOOLS`. The four agents are `YouTubeSyncAgent`, `LyricsFetchAgent`, `TextAnalysisAgent` and `ReportAgent`, all in `agents/`.

**Agents wrap the legacy modules.** The agents delegate to `youtube.py`, `lyrics.py` and `text_analyzer.py`, which hold the actual API and LLM logic. `thread_pool.py` provides multi-threaded batch processing with scoped sessions and coordinated shutdown on rate limits.

**Django dashboard (`django_app/`).** It reads the same SQL Server tables through unmanaged models (`managed = False` in `dashboard/models.py`). `database.py` owns that schema, not Django migrations. Django also has its own tables (`AgentRun`, `AgentRunEvent`, `SyncJob`, `SyncJobLog`). Two integration points:
- `agents/django_logger.py` records each orchestrator run as `AgentRun` and `AgentRunEvent` rows. It is a silent no-op unless `DJANGO_SETTINGS_MODULE` is set in the environment. `main.py` does not set it.
- The `process_queue` management command dequeues `SyncJob` rows and runs the same agents. `--loop --interval N` keeps it polling. Django-Q appears in the logs, but no Q_CLUSTER is configured in settings.

**Logging.** `logger_config.py` writes JSON logs to `logs/app.log`, rotated at 10 MB with 5 backups.

## Gotchas

- `main.py` defaults `YOUTUBE_OAUTH_FILE` to a hardcoded `D:\LyricsDataScience\browser.json` path. `youtube.py` uses a relative `browser.json`. The README's `oauth.json` reference is stale.
- Credentials are hardcoded as fallback defaults in `settings.py` (database) and `lyrics.py` and `django_app/dashboard/management/commands/process_queue.py` (Genius). Do not copy these values anywhere, and read credentials from environment variables.
