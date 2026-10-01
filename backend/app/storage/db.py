"""SQLite schema initialization and forward-compatible upgrades."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from app.config import DATA_DIR, DB_PATH

SCHEMA_VERSION = 14

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'user',
    disabled INTEGER NOT NULL DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_users_username
    ON users (username);

CREATE TABLE IF NOT EXISTS user_sessions (
    session_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now')),
    last_seen_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_user_sessions_user
    ON user_sessions (user_id);
CREATE INDEX IF NOT EXISTS idx_user_sessions_expires
    ON user_sessions (expires_at);

CREATE TABLE IF NOT EXISTS audit_events (
    event_id TEXT PRIMARY KEY,
    occurred_at TEXT NOT NULL,
    actor_user_id TEXT NOT NULL DEFAULT '',
    actor_role TEXT NOT NULL DEFAULT '',
    action TEXT NOT NULL,
    target_type TEXT NOT NULL DEFAULT '',
    target_id TEXT NOT NULL DEFAULT '',
    source_id TEXT NOT NULL DEFAULT '',
    outcome TEXT NOT NULL DEFAULT 'success',
    summary_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_audit_events_time
    ON audit_events (occurred_at);
CREATE INDEX IF NOT EXISTS idx_audit_events_actor
    ON audit_events (actor_user_id, occurred_at);
CREATE INDEX IF NOT EXISTS idx_audit_events_target
    ON audit_events (target_type, target_id, occurred_at);

CREATE TABLE IF NOT EXISTS books (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    book_id TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    author TEXT,
    cover_url TEXT,
    intro TEXT,
    category TEXT,
    word_count TEXT,
    status TEXT,
    last_chapter TEXT,
    last_update_time TEXT,
    sources TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS chapters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    book_id TEXT NOT NULL,
    chapter_id TEXT NOT NULL,
    title TEXT NOT NULL,
    url TEXT,
    content TEXT,
    cached INTEGER DEFAULT 0,
    source TEXT,
    update_time TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    UNIQUE(book_id, chapter_id)
);

CREATE TABLE IF NOT EXISTS update_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    book_id TEXT UNIQUE NOT NULL,
    last_check_time TEXT,
    next_check_time TEXT,
    status TEXT DEFAULT 'active',
    error_count INTEGER DEFAULT 0,
    last_error TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS book_cache (
    book_id TEXT PRIMARY KEY,
    source_id TEXT,
    book_url TEXT,
    response_json TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS toc_cache (
    book_id TEXT PRIMARY KEY,
    response_json TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS chapter_cache (
    chapter_id TEXT PRIMARY KEY,
    source_id TEXT,
    chapter_url TEXT,
    response_json TEXT,
    book_id TEXT,
    book_name TEXT,
    chapter_title TEXT,
    file_path TEXT,
    content_hash TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS book_records (
    book_id TEXT PRIMARY KEY,
    name TEXT,
    author TEXT,
    merged_sources_json TEXT,
    selected_source_id TEXT,
    last_chapter TEXT,
    last_seen_at TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Search subsystem: result-oriented persistence. No process events.

CREATE TABLE IF NOT EXISTS search_jobs (
    job_id TEXT PRIMARY KEY,
    keyword TEXT NOT NULL,
    normalized_keyword TEXT NOT NULL,
    source_scope TEXT NOT NULL DEFAULT '__default__',
    page INTEGER NOT NULL,
    status TEXT NOT NULL,
    created_at REAL,
    completed_at REAL,
    result_count INTEGER DEFAULT 0,
    source_count INTEGER DEFAULT 0,
    attempted_count INTEGER DEFAULT 0,
    success_count INTEGER DEFAULT 0,
    error_count INTEGER DEFAULT 0,
    timeout_count INTEGER DEFAULT 0,
    elapsed_ms INTEGER DEFAULT 0,
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_search_jobs_normalized
    ON search_jobs (normalized_keyword, page, source_scope, status);

CREATE TABLE IF NOT EXISTS search_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_name TEXT,
    book_url TEXT NOT NULL,
    name TEXT,
    author TEXT,
    cover_url TEXT,
    intro TEXT,
    category TEXT,
    word_count TEXT,
    status TEXT,
    last_chapter TEXT,
    score INTEGER DEFAULT 0,
    raw_payload_json TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(job_id, source_id, book_url)
);

CREATE INDEX IF NOT EXISTS idx_search_results_job
    ON search_results (job_id);
CREATE INDEX IF NOT EXISTS idx_search_results_book
    ON search_results (name, author);

CREATE TABLE IF NOT EXISTS subscription_search_jobs (
    job_id TEXT PRIMARY KEY,
    owner_user_id TEXT NOT NULL,
    keyword TEXT NOT NULL,
    page INTEGER NOT NULL CHECK(page >= 1),
    status TEXT NOT NULL CHECK(status IN (
        'pending', 'running', 'completed', 'partial', 'timed_out',
        'failed', 'cancelled', 'interrupted'
    )),
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    FOREIGN KEY(owner_user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_subscription_search_jobs_owner_updated
    ON subscription_search_jobs (owner_user_id, updated_at DESC);


-- Book-centric search cache (replaces search_query_cache).
-- Truth source is book entities, not query snapshots.  TTL = 7 days.

CREATE TABLE IF NOT EXISTS book_search_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    match_mode TEXT NOT NULL,
    normalized_name TEXT NOT NULL DEFAULT '',
    normalized_author TEXT NOT NULL DEFAULT '',
    source_id TEXT NOT NULL,
    source_name TEXT DEFAULT '',
    raw_book_url TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    score INTEGER DEFAULT 0,
    first_seen_at TEXT DEFAULT (datetime('now')),
    last_seen_at TEXT DEFAULT (datetime('now')),
    expires_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_book_search_cache_title
    ON book_search_cache (match_mode, normalized_name, last_seen_at);
CREATE INDEX IF NOT EXISTS idx_book_search_cache_author
    ON book_search_cache (match_mode, normalized_author, last_seen_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_book_search_cache_unique_source_book
    ON book_search_cache (source_id, raw_book_url);

-- Live acceptance checks (debugging/diagnostics, not long-lived facts).

CREATE TABLE IF NOT EXISTS plugin_live_checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    plugin_id TEXT NOT NULL,
    keyword TEXT NOT NULL,
    status TEXT NOT NULL,
    search_count INTEGER DEFAULT 0,
    selected_name TEXT,
    selected_author TEXT,
    toc_count INTEGER DEFAULT 0,
    chapter_title TEXT,
    content_length INTEGER DEFAULT 0,
    result_json TEXT,
    error_json TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Aggregate processing tasks.

CREATE TABLE IF NOT EXISTS aggregate_book_tasks (
    aggregate_book_id TEXT PRIMARY KEY,
    canonical_name TEXT DEFAULT '',
    canonical_author TEXT DEFAULT '',
    name TEXT,
    author TEXT,
    cover_url TEXT DEFAULT '',
    intro TEXT DEFAULT '',
    word_count TEXT DEFAULT '',
    aggregate_payload_json TEXT,
    primary_book_id TEXT,
    primary_source_id TEXT,
    primary_source_name TEXT DEFAULT '',
    primary_book_url TEXT DEFAULT '',
    primary_toc_url TEXT DEFAULT '',
    added_by_user_id TEXT DEFAULT '',
    start_chapter_index INTEGER DEFAULT 1,
    total_chapters_at_subscribe INTEGER DEFAULT 0,
    initial_snapshot_last_index INTEGER DEFAULT 0,
    backfill_started INTEGER DEFAULT 0,
    auto_archive_on_complete INTEGER DEFAULT 1,
    search_visibility_status TEXT DEFAULT 'hidden',
    book_status TEXT DEFAULT 'unknown',
    total_chapters INTEGER DEFAULT 0,
    processed_chapters INTEGER DEFAULT 0,
    visible_processed_chapters INTEGER DEFAULT 0,
    failed_chapters INTEGER DEFAULT 0,
    total_tokens INTEGER DEFAULT 0,
    status TEXT DEFAULT 'active',
    settings_json TEXT DEFAULT '',
    current_policy_version INTEGER DEFAULT 1,
    interval_minutes INTEGER DEFAULT 30,
    last_check_time TEXT,
    next_check_time TEXT,
    error_count INTEGER DEFAULT 0,
    last_error TEXT,
    ai_enabled INTEGER DEFAULT 0,
    last_processed_at TEXT,
    archived_at TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS user_book_subscriptions (
    user_id TEXT NOT NULL,
    aggregate_book_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'paused', 'archived')),
    start_chapter_index INTEGER NOT NULL DEFAULT 1
        CHECK (start_chapter_index >= 1),
    auto_archive_on_complete INTEGER NOT NULL DEFAULT 1
        CHECK (auto_archive_on_complete IN (0, 1)),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (user_id, aggregate_book_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    FOREIGN KEY (aggregate_book_id)
        REFERENCES aggregate_book_tasks(aggregate_book_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_user_book_subscriptions_user_status
    ON user_book_subscriptions (user_id, status, updated_at);
CREATE INDEX IF NOT EXISTS idx_user_book_subscriptions_book
    ON user_book_subscriptions (aggregate_book_id);

CREATE TABLE IF NOT EXISTS aggregate_chapter_tasks (
    chapter_id TEXT PRIMARY KEY,
    aggregate_book_id TEXT NOT NULL,
    source_chapter_id TEXT,
    chapter_index INTEGER,
    title TEXT,
    status TEXT DEFAULT 'pending',
    placeholder INTEGER DEFAULT 0,
    content_length INTEGER DEFAULT 0,
    source_word_count INTEGER DEFAULT 0,
    preview_only INTEGER DEFAULT 0,
    primary_source_chapter_url TEXT DEFAULT '',
    processed_content TEXT,
    content_file_path TEXT DEFAULT '',
    last_processed_at TEXT,
    error TEXT,
    policy_version INTEGER DEFAULT 1,
    policy_snapshot_json TEXT DEFAULT '',
    source_snapshot_refs_json TEXT DEFAULT '',
    trace_hash TEXT DEFAULT '',
    ai_model TEXT,
    ai_prompt_tokens INTEGER DEFAULT 0,
    ai_completion_tokens INTEGER DEFAULT 0,
    ai_total_tokens INTEGER DEFAULT 0,
    ai_latency_ms INTEGER DEFAULT 0,
    deviation_score REAL DEFAULT 0.0,
    ai_self_score REAL DEFAULT 0.0,
    fallback_source_id TEXT,
    source_alignment_json TEXT,
    manual_supplement INTEGER DEFAULT 0,
    manual_supplement_json TEXT DEFAULT '',
    retry_count INTEGER DEFAULT 0,
    next_retry_time TEXT,
    last_error_code TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS aggregate_ai_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    aggregate_book_id TEXT NOT NULL,
    chapter_id TEXT NOT NULL,
    provider TEXT,
    model TEXT,
    prompt_tokens INTEGER DEFAULT 0,
    completion_tokens INTEGER DEFAULT 0,
    total_tokens INTEGER DEFAULT 0,
    latency_ms INTEGER DEFAULT 0,
    status TEXT,
    error TEXT,
    deviation_score REAL DEFAULT 0.0,
    ai_self_score REAL DEFAULT 0.0,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS aggregate_book_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    aggregate_book_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_book_id TEXT DEFAULT '',
    source_name TEXT DEFAULT '',
    source_book_url TEXT DEFAULT '',
    role TEXT NOT NULL DEFAULT 'candidate',
    score INTEGER DEFAULT 0,
    enabled INTEGER NOT NULL DEFAULT 1,
    last_seen_at TEXT DEFAULT (datetime('now')),
    last_chapter_title TEXT DEFAULT '',
    chapter_count INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    UNIQUE(aggregate_book_id, source_id, source_book_id)
);

CREATE INDEX IF NOT EXISTS idx_aggregate_book_sources_book
    ON aggregate_book_sources (aggregate_book_id, role, enabled);
CREATE INDEX IF NOT EXISTS idx_aggregate_book_sources_source_book
    ON aggregate_book_sources (source_id, source_book_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_aggregate_book_sources_identity
    ON aggregate_book_sources (source_id, source_book_id)
    WHERE source_book_id <> '';

CREATE TABLE IF NOT EXISTS aggregate_operation_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    aggregate_book_id TEXT NOT NULL,
    actor_user_id TEXT DEFAULT '',
    actor_role TEXT DEFAULT '',
    operation_type TEXT NOT NULL,
    before_json TEXT DEFAULT '',
    after_json TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_aggregate_operation_logs_book
    ON aggregate_operation_logs (aggregate_book_id, created_at);

CREATE TABLE IF NOT EXISTS aggregate_source_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    aggregate_book_id TEXT NOT NULL,
    chapter_index INTEGER NOT NULL,
    source_id TEXT NOT NULL,
    source_book_id TEXT DEFAULT '',
    source_chapter_id TEXT DEFAULT '',
    title TEXT DEFAULT '',
    source_chapter_url TEXT DEFAULT '',
    raw_content TEXT DEFAULT '',
    clean_content TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    word_count INTEGER DEFAULT 0,
    purify_audit_json TEXT DEFAULT '[]',
    classification TEXT DEFAULT '',
    fetched_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    UNIQUE(aggregate_book_id, chapter_index, source_id)
);

CREATE INDEX IF NOT EXISTS idx_aggregate_source_snapshots_book
    ON aggregate_source_snapshots (aggregate_book_id, chapter_index);

CREATE TABLE IF NOT EXISTS aggregate_source_snapshot_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    aggregate_book_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_book_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending',
    toc_hash TEXT DEFAULT '',
    total_chapters INTEGER DEFAULT 0,
    matched_chapters INTEGER DEFAULT 0,
    fetched_chapters INTEGER DEFAULT 0,
    failed_chapters INTEGER DEFAULT 0,
    last_error TEXT DEFAULT '',
    started_at TEXT DEFAULT '',
    completed_at TEXT DEFAULT '',
    updated_at TEXT DEFAULT (datetime('now')),
    UNIQUE(aggregate_book_id, source_id, source_book_id)
);

CREATE INDEX IF NOT EXISTS idx_aggregate_source_snapshot_runs_book
    ON aggregate_source_snapshot_runs (aggregate_book_id, status);
"""


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def _ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, sql_type: str) -> None:
    columns = _table_columns(conn, table_name)
    if column_name in columns:
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {sql_type}")


def _ensure_users_schema(conn: sqlite3.Connection) -> None:
    """users columns added after the initial schema shipped."""
    _ensure_column(conn, "users", "access_code_secret", "TEXT DEFAULT ''")


def _ensure_shared_library_schema(conn: sqlite3.Connection) -> None:
    if "aggregate_book_tasks" in {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }:
        book_columns = {
            "canonical_name": "TEXT DEFAULT ''",
            "canonical_author": "TEXT DEFAULT ''",
            "cover_url": "TEXT DEFAULT ''",
            "intro": "TEXT DEFAULT ''",
            "word_count": "TEXT DEFAULT ''",
            "primary_source_name": "TEXT DEFAULT ''",
            "primary_book_url": "TEXT DEFAULT ''",
            "primary_toc_url": "TEXT DEFAULT ''",
            "added_by_user_id": "TEXT DEFAULT ''",
            "start_chapter_index": "INTEGER DEFAULT 1",
            "total_chapters_at_subscribe": "INTEGER DEFAULT 0",
            "initial_snapshot_last_index": "INTEGER DEFAULT 0",
            "backfill_started": "INTEGER DEFAULT 0",
            "auto_archive_on_complete": "INTEGER DEFAULT 1",
            "search_visibility_status": "TEXT DEFAULT 'hidden'",
            "visible_processed_chapters": "INTEGER DEFAULT 0",
            "settings_json": "TEXT DEFAULT ''",
            "current_policy_version": "INTEGER DEFAULT 1",
            "last_source_chapter_title": "TEXT DEFAULT ''",
            "archived_at": "TEXT",
        }
        for name, sql_type in book_columns.items():
            _ensure_column(conn, "aggregate_book_tasks", name, sql_type)

    if "aggregate_chapter_tasks" in {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }:
        chapter_columns = {
            "placeholder": "INTEGER DEFAULT 0",
            "source_word_count": "INTEGER DEFAULT 0",
            "preview_only": "INTEGER DEFAULT 0",
            "primary_source_chapter_url": "TEXT DEFAULT ''",
            "content_file_path": "TEXT DEFAULT ''",
            "policy_version": "INTEGER DEFAULT 1",
            "policy_snapshot_json": "TEXT DEFAULT ''",
            "source_snapshot_refs_json": "TEXT DEFAULT ''",
            "trace_hash": "TEXT DEFAULT ''",
            "preview_retry_count": "INTEGER DEFAULT 0",
            "manual_supplement": "INTEGER DEFAULT 0",
            "manual_supplement_json": "TEXT DEFAULT ''",
        }
        for name, sql_type in chapter_columns.items():
            _ensure_column(conn, "aggregate_chapter_tasks", name, sql_type)

    if "aggregate_source_snapshots" in {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }:
        snapshot_columns = {
            "source_chapter_url": "TEXT DEFAULT ''",
            "raw_content": "TEXT DEFAULT ''",
            "word_count": "INTEGER DEFAULT 0",
            "purify_audit_json": "TEXT DEFAULT '[]'",
        }
        for name, sql_type in snapshot_columns.items():
            _ensure_column(conn, "aggregate_source_snapshots", name, sql_type)

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_aggregate_book_tasks_canonical
            ON aggregate_book_tasks (canonical_name, canonical_author)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_aggregate_book_tasks_added_by
            ON aggregate_book_tasks (added_by_user_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_aggregate_book_tasks_status
            ON aggregate_book_tasks (status, search_visibility_status)
        """
    )


def _migrate_book_search_cache(conn: sqlite3.Connection) -> None:
    """Collapse duplicate book cache rows and align the uniqueness rule."""
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if "book_search_cache" not in tables:
        return

    conn.execute(
        """
        DELETE FROM book_search_cache
        WHERE id IN (
            SELECT current.id
            FROM book_search_cache AS current
            WHERE EXISTS (
                SELECT 1
                FROM book_search_cache AS newer
                WHERE newer.source_id = current.source_id
                  AND newer.raw_book_url = current.raw_book_url
                  AND (
                        newer.last_seen_at > current.last_seen_at
                     OR (newer.last_seen_at = current.last_seen_at AND newer.id > current.id)
                  )
            )
        )
        """
    )
    conn.execute("DROP INDEX IF EXISTS idx_book_search_cache_unique_source_book")
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_book_search_cache_unique_source_book
            ON book_search_cache (source_id, raw_book_url)
        """
    )


def _current_schema_version(conn: sqlite3.Connection) -> int:
    table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_meta'"
    ).fetchone()
    if not table:
        return 0
    row = conn.execute(
        "SELECT value FROM schema_meta WHERE key = 'version'"
    ).fetchone()
    if not row:
        return 0
    try:
        return int(row[0])
    except (TypeError, ValueError):
        return 0


def _backfill_shared_book_creators(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        UPDATE aggregate_book_tasks
        SET added_by_user_id = (
            SELECT actor_user_id
            FROM aggregate_operation_logs
            WHERE aggregate_operation_logs.aggregate_book_id = aggregate_book_tasks.aggregate_book_id
              AND aggregate_operation_logs.operation_type = 'create'
              AND TRIM(COALESCE(aggregate_operation_logs.actor_user_id, '')) <> ''
            ORDER BY aggregate_operation_logs.id ASC
            LIMIT 1
        )
        WHERE TRIM(COALESCE(added_by_user_id, '')) = ''
          AND EXISTS (
              SELECT 1
              FROM aggregate_operation_logs
              WHERE aggregate_operation_logs.aggregate_book_id = aggregate_book_tasks.aggregate_book_id
                AND aggregate_operation_logs.operation_type = 'create'
                AND TRIM(COALESCE(aggregate_operation_logs.actor_user_id, '')) <> ''
          )
        """
    )


def _migrate_v12_to_v13(conn: sqlite3.Connection) -> None:
    """Invalidate raw sessions before v13 starts storing only token hashes."""
    conn.execute("DELETE FROM user_sessions")


def _migrate_v13_to_v14(conn: sqlite3.Connection) -> None:
    """Keep legacy clean snapshots readable while v14 starts retaining raw input."""
    conn.execute(
        """
        UPDATE aggregate_source_snapshots
        SET raw_content = clean_content
        WHERE COALESCE(raw_content, '') = '' AND COALESCE(clean_content, '') <> ''
        """
    )


def initialize_database(db_path: Path | None = None) -> str:
    path = db_path or DB_PATH
    ensure_data_dir()

    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        current_version = _current_schema_version(conn)
        if current_version > SCHEMA_VERSION:
            raise RuntimeError(
                f"database schema {current_version} is newer than supported {SCHEMA_VERSION}"
            )
        conn.executescript(f"BEGIN IMMEDIATE;\n{SCHEMA_SQL}")
        _ensure_users_schema(conn)
        _ensure_shared_library_schema(conn)
        _migrate_book_search_cache(conn)
        if current_version < 12:
            _backfill_shared_book_creators(conn)
        if current_version < 13:
            _migrate_v12_to_v13(conn)
        if current_version < 14:
            _migrate_v13_to_v14(conn)
        conn.execute(
            "INSERT OR REPLACE INTO schema_meta (key, value) VALUES (?, ?)",
            ("version", str(SCHEMA_VERSION)),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return str(path)
