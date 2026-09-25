"""Background processing for virtual aggregate books."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import sqlite3
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

from app.core.app_config import AppConfig
from app.source_plugins.id_codec import decode_chapter_id, encode_book_id, encode_chapter_id
from app.services.aggregate_virtual_source import (
    VIRTUAL_SOURCE_ID,
    make_aggregate_chapter_url,
    primary_book_id_from_payload,
    unpack_aggregate_chapter_url,
)
from app.services.aggregate_settings import (
    AI_RUNTIME_ENABLED,
    BACKLOG_CHAPTER_LIMIT,
    BACKLOG_RECHECK_MINUTES,
    CHAPTER_PARALLELISM_LIMIT,
    DEFAULT_CONTENT_WORKFLOW,
    PER_SOURCE_CONCURRENCY,
    PER_BOOK_SOURCE_CONCURRENCY,
    INITIAL_PREFETCH_GRACE_SECONDS,
    INITIAL_PREFETCH_CHAPTER_LIMIT,
    PREVIEW_RETRY_DELAYS_MINUTES,
    PROCESSING_PLACEHOLDER,
    RETRY_DELAYS_MINUTES,
    WINDOW_CHAPTER_LIMIT,
    WORD_COUNT_TOLERANCE_LOWER,
    WORD_COUNT_TOLERANCE_UPPER,
    AggregateSettingsRepository,
    shared_book_storage_contract,
)
from app.services.novel_file_cache import NovelFileCache, looks_like_garbled_text
from app.services.shared_book_errors import (
    STAGE1_NO_RETRY_ERROR_CODES,
    SharedBookRetryClass,
    classify_stage1_error,
    classify_stage1_retry,
    classify_stage2_error,
    classify_stage2_retry,
    classify_stage3_error,
    classify_stage3_retry,
)
from app.services.shared_book_runtime import (
    SharedBookProcessLogger,
    SharedBookRuntimeStore,
    SharedBookStage3DeferredItem,
)
from app.services.shared_book_storage import SharedBookStorage
from app.services.library_books import library_books_service

DEFAULT_WORKFLOW = DEFAULT_CONTENT_WORKFLOW
CROSS_SOURCE_INITIAL_COMPARE_COUNT = 3
CROSS_SOURCE_MAX_COMPARE_COUNT = 8
CANDIDATE_TITLE_MAX_INDEX_DRIFT = 30

TRACE_BLOCK_RE = re.compile(
    r"(?:\n|^)(?:<!--\s*)?LEGADOHUB_TRACE_BEGIN\s*(?:```yaml\s*)?\n.*?\n(?:\s*```\s*)?LEGADOHUB_TRACE_END(?:\s*-->)?\s*$",
    re.DOTALL,
)


def classify_error(exc: Exception) -> str:
    """Map a Stage 1 exception to the canonical shared-book error code."""
    return str(classify_stage1_error(exc))


def compute_next_retry_time(retry_count: int) -> str | None:
    """Return an ISO UTC timestamp for the next retry, or None if exhausted."""
    from app.services.aggregate_settings import RETRY_DELAYS_MINUTES

    if retry_count >= len(RETRY_DELAYS_MINUTES):
        return None
    delay = RETRY_DELAYS_MINUTES[retry_count]
    return (datetime.now(timezone.utc) + timedelta(minutes=delay)).isoformat()


def max_retries_reached(retry_count: int) -> bool:
    """True when the chapter has exhausted all automatic retries."""
    from app.services.aggregate_settings import RETRY_DELAYS_MINUTES

    return retry_count >= len(RETRY_DELAYS_MINUTES)


def compute_preview_next_retry_time(preview_retry_count: int) -> str | None:
    """Return the next UTC ISO timestamp for a preview-only fallback retry."""
    if preview_retry_count >= len(PREVIEW_RETRY_DELAYS_MINUTES):
        return None
    delay = PREVIEW_RETRY_DELAYS_MINUTES[preview_retry_count]
    return (datetime.now(timezone.utc) + timedelta(minutes=delay)).isoformat()


def max_preview_retries_reached(preview_retry_count: int) -> bool:
    """True when a preview-only fallback has exhausted automatic retries."""
    return preview_retry_count >= len(PREVIEW_RETRY_DELAYS_MINUTES)


class AggregateProcessor:
    def __init__(self, db_path: str | Path | None = None, *, ai_service: Any = None):
        from app.config import DB_PATH

        self.db_path = Path(db_path or DB_PATH)
        self._ai_service = ai_service
        self._toc_cache: dict[str, dict] = {}
        self._candidate_source_cache: dict[str, tuple[str, dict[str, Any]]] = {}
        self._official_source_flags: dict[str, bool] | None = None
        self._ai_circuit_breakers: dict[str, dict[str, Any]] = {}
        self._ai_window_events: dict[str, deque[dict[str, Any]]] = {}
        self._process_logger_cache: SharedBookProcessLogger | None = None
        self._source_sems: dict[tuple[str, str], asyncio.Semaphore] = {}
        self._browser_source_sem = asyncio.Semaphore(
            max(1, int(AppConfig.get().search.browser_source_concurrency))
        )
        self._ai_sem = asyncio.Semaphore(self._ai_concurrency_limit())
        self._toc_is_vip_cache: dict[str, dict[int, bool]] = {}
        self._free_chapter_end_index_cache: dict[str, int] = {}
        self._source_ad_patterns: dict[str, list[str]] = self._load_source_ad_patterns()
        self._browser_source_ids = self._load_browser_source_ids()
        self._conservative_source_ids = self._load_conservative_source_ids()
        self._lexicon_scanner: Any | None = self._load_lexicon_scanner()

    def _load_browser_source_ids(self) -> set[str]:
        try:
            from app.source_plugins.scheduler import get_plugin_scheduler

            return {
                plugin_id
                for plugin_id, plugin in get_plugin_scheduler()._plugins.items()
                if (getattr(plugin.metadata, "browser", {}) or {}).get("mode")
                in {"required", "optional"}
            }
        except Exception:
            logger.debug("Failed to load browser source metadata", exc_info=True)
            return set()

    def _load_conservative_source_ids(self) -> set[str]:
        """Keep official, Browser, and explicitly low-cap sources at one request per book."""
        try:
            from app.source_plugins.scheduler import get_plugin_scheduler

            result: set[str] = set()
            for plugin_id, plugin in get_plugin_scheduler()._plugins.items():
                browser_mode = (getattr(plugin.metadata, "browser", {}) or {}).get("mode")
                raw_limit = (getattr(plugin.metadata, "rate_limit", {}) or {}).get(
                    "perHostConcurrency", 0
                )
                try:
                    global_limit = int(raw_limit)
                except (TypeError, ValueError):
                    global_limit = 0
                if (
                    plugin.metadata.is_official_source()
                    or browser_mode in {"required", "optional"}
                    or global_limit < PER_SOURCE_CONCURRENCY
                ):
                    result.add(plugin_id)
            return result
        except Exception:
            logger.debug("Failed to load conservative source metadata", exc_info=True)
            return set()

    def _load_lexicon_scanner(self) -> Any | None:
        """Load the sensitive-word lexicon scanner for local masked-word recovery."""
        try:
            from app.ai.lexicon import SensitiveLexiconScanner
            from app.services.aggregate_settings import resolve_sensitive_lexicon_path

            lex_path = resolve_sensitive_lexicon_path(None)
            if not lex_path.exists():
                logger.info("Sensitive lexicon directory does not exist yet: %s", lex_path)
                return None
            scanner = SensitiveLexiconScanner.from_path(lex_path)
            logger.info("Loaded sensitive lexicon from %s: %d words", lex_path, scanner.word_count)
            return scanner
        except Exception as exc:
            logger.warning("Failed to load sensitive lexicon: %s", exc)
            return None

    def _load_source_ad_patterns(self) -> dict[str, list[str]]:
        """Build source_id -> ad pattern list from plugin metadata and optional get_ad_patterns()."""
        patterns: dict[str, list[str]] = {}
        try:
            from app.source_plugins.scheduler import get_plugin_scheduler
            scheduler = get_plugin_scheduler()
            for plugin_id, plugin in scheduler._plugins.items():
                meta_patterns = list(plugin.metadata.ad_patterns or [])
                source_patterns: list[str] = []
                if hasattr(plugin.source, "get_ad_patterns") and callable(getattr(plugin.source, "get_ad_patterns")):
                    try:
                        result = plugin.source.get_ad_patterns()
                        if isinstance(result, list):
                            source_patterns = [str(p) for p in result if isinstance(p, str)]
                    except Exception:
                        logger.debug("get_ad_patterns failed for %s", plugin_id, exc_info=True)
                combined = source_patterns + meta_patterns
                if combined:
                    patterns[plugin_id] = combined
        except Exception:
            logger.debug("Failed to load source ad patterns", exc_info=True)
        return patterns

    def _ad_patterns_for_source(self, source_id: str | None) -> list[str]:
        """Plugin-specific patterns for the given source (may be empty)."""
        if source_id and source_id in self._source_ad_patterns:
            return list(self._source_ad_patterns[source_id])
        return []

    def _purify_content(self, content: str, source_id: str | None = None) -> str:
        """Unified content purification with plugin-specific ad patterns."""
        from app.services.content_purify import purify_content

        return purify_content(content, ad_patterns=self._ad_patterns_for_source(source_id))

    def _purify_content_with_lexicon(
        self, content: str, source_id: str | None = None
    ) -> tuple[str, dict[str, Any]]:
        """Purify content then try to restore masked sensitive words via lexicon.

        Returns ``(purified_text, lexicon_result)``.  The lexicon result records
        whether masked words were found, how many, sample candidates, and whether
        any replacement was performed.
        """
        if not content:
            return content, {
                "hasMaskedWords": False,
                "maskedWordCount": 0,
                "maskedWordCandidates": [],
                "lexiconRestored": False,
            }

        purified = self._purify_content(content, source_id=source_id)
        lexicon_result: dict[str, Any] = {
            "hasMaskedWords": False,
            "maskedWordCount": 0,
            "maskedWordCandidates": [],
            "lexiconRestored": False,
        }

        if self._lexicon_scanner is not None:
            try:
                restored, candidates = self._lexicon_scanner.restore_masked(purified)
                if candidates:
                    lexicon_result = {
                        "hasMaskedWords": True,
                        "maskedWordCount": len(candidates),
                        "maskedWordCandidates": [
                            {
                                "offset": c.offset,
                                "maskedText": c.masked_text,
                                "contextBefore": c.context_before,
                                "contextAfter": c.context_after,
                                "candidate": c.candidates[0] if c.candidates else "",
                            }
                            for c in candidates
                        ],
                        "lexiconRestored": restored != purified,
                    }
                    return restored, lexicon_result
            except Exception as exc:
                logger.warning("Lexicon restore failed for source %s: %s", source_id, exc)

        return purified, lexicon_result

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=5.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def _chapter_concurrency_limit(self) -> int:
        return CHAPTER_PARALLELISM_LIMIT

    def _ai_concurrency_limit(self) -> int:
        try:
            repo = AggregateSettingsRepository(self.db_path)
            cfg = repo.aggregate_config()
            val = int(cfg.get("aiMaxConcurrency", 2))
        except Exception:
            val = 2
        return max(1, val)

    async def _get_source_sem(
        self,
        source_id: str,
        aggregate_book_id: str = "",
    ) -> asyncio.Semaphore:
        key = (str(aggregate_book_id or "*"), str(source_id or ""))
        if key not in self._source_sems:
            concurrency = (
                1
                if source_id in self._conservative_source_ids
                else min(PER_SOURCE_CONCURRENCY, PER_BOOK_SOURCE_CONCURRENCY)
            )
            self._source_sems[key] = asyncio.Semaphore(
                concurrency
            )
        return self._source_sems[key]

    @asynccontextmanager
    async def _source_slot(self, *, aggregate_book_id: str, source_id: str):
        source_sem = await self._get_source_sem(source_id, aggregate_book_id)
        if source_id in self._browser_source_ids:
            async with self._browser_source_sem:
                async with source_sem:
                    yield
            return
        async with source_sem:
            yield

    def _library_books(self):
        from app.services.library_books import LibraryBooksService

        return LibraryBooksService(
            db_path=self.db_path,
            shared_book_storage=self._shared_book_storage(),
        )

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _now_dt(self) -> datetime:
        return datetime.now(timezone.utc)

    def _workflow_settings(self) -> dict:
        return AggregateSettingsRepository(self.db_path).content_workflow()

    def _shared_book_storage_contract(self) -> dict[str, Any]:
        return shared_book_storage_contract(self._workflow_settings())

    def _book_workflow_settings(self, aggregate_book_id: str = "") -> dict:
        settings = dict(self._workflow_settings())
        if not aggregate_book_id:
            return settings
        try:
            book = self._library_books().get_book(aggregate_book_id) or {}
            raw = book.get("settingsJson", "") or ""
            per_book = json.loads(raw) if raw else {}
        except Exception:
            per_book = {}
        if not isinstance(per_book, dict):
            per_book = {}
        if "autoTrackUpdates" in per_book:
            settings["autoAggregate"] = bool(per_book["autoTrackUpdates"])
        if "updateIntervalMinutes" in per_book:
            settings["aggregateCheckIntervalMinutes"] = int(per_book["updateIntervalMinutes"] or settings.get("aggregateCheckIntervalMinutes", 30))
        if "backlogChapterLimit" in per_book:
            settings["backlogChapterLimit"] = int(per_book["backlogChapterLimit"] or BACKLOG_CHAPTER_LIMIT)
        if "primarySourceMode" in per_book:
            settings["primarySourceMode"] = per_book["primarySourceMode"]
        if "sourcePriority" in per_book:
            settings["primarySourcePriority"] = list(per_book["sourcePriority"] or [])
        if "aiAggregateEnabled" in per_book:
            settings["aiEnabled"] = bool(per_book["aiAggregateEnabled"])
        if "aiPurifyEnabled" in per_book:
            settings["blockedWordRepair"] = bool(per_book["aiPurifyEnabled"])
        return settings

    def processing_enabled(self, aggregate_book_id: str = "") -> bool:
        settings = self._book_workflow_settings(aggregate_book_id)
        return bool(settings.get("autoAggregate", True))

    def ai_aggregate_enabled(self, aggregate_book_id: str = "") -> bool:
        settings = self._book_workflow_settings(aggregate_book_id)
        return AI_RUNTIME_ENABLED and bool(settings.get("aiEnabled"))

    def purify_enabled(self, aggregate_book_id: str = "") -> bool:
        settings = self._book_workflow_settings(aggregate_book_id)
        return bool(settings.get("blockedWordRepair", True))

    def check_interval_minutes(self, aggregate_book_id: str = "") -> int:
        settings = self._book_workflow_settings(aggregate_book_id)
        value = int(settings.get("aggregateCheckIntervalMinutes") or 30)
        return min(max(value, 10), 1440)

    def backlog_chapter_limit(self, aggregate_book_id: str = "") -> int:
        settings = self._book_workflow_settings(aggregate_book_id)
        value = int(settings.get("backlogChapterLimit") or BACKLOG_CHAPTER_LIMIT)
        return min(max(value, WINDOW_CHAPTER_LIMIT), 100)

    def backlog_recheck_minutes(self, aggregate_book_id: str = "") -> int:
        settings = self._book_workflow_settings(aggregate_book_id)
        value = int(settings.get("backlogRecheckMinutes") or BACKLOG_RECHECK_MINUTES)
        return min(max(value, 1), self.check_interval_minutes(aggregate_book_id))

    def return_only_aggregate_source(self) -> bool:
        return bool(self._workflow_settings().get("returnOnlyAggregateSource", False))

    def stage3_backlog_state(self, aggregate_book_id: str) -> dict[str, Any]:
        settings = self._book_workflow_settings(aggregate_book_id)
        limit = max(0, int(settings.get("stage3MaxBacklogPerBook", 0) or 0))
        backlog = self._pending_chapter_count(aggregate_book_id)
        exceeded = limit > 0 and backlog > limit
        return {
            "bookId": aggregate_book_id,
            "backlog": backlog,
            "limit": limit,
            "enabled": limit > 0,
            "exceeded": exceeded,
        }

    def ai_circuit_breaker_state(self, aggregate_book_id: str) -> dict[str, Any]:
        settings = self._book_workflow_settings(aggregate_book_id)
        now = self._now_dt()
        self._prune_ai_window_events(aggregate_book_id, now)
        breaker = self._ai_circuit_breakers.get(aggregate_book_id) or {}
        open_until = breaker.get("openUntil")
        is_open = isinstance(open_until, datetime) and open_until > now
        if not is_open and aggregate_book_id in self._ai_circuit_breakers:
            self._ai_circuit_breakers.pop(aggregate_book_id, None)
            breaker = {}

        events = list(self._ai_window_events.get(aggregate_book_id, ()))
        total_calls = len(events)
        failed_calls = sum(1 for event in events if not bool(event.get("success", False)))
        token_usage = sum(max(0, int(event.get("tokens", 0) or 0)) for event in events)
        peak_hour_skip = self._is_stage3_peak_hour_blocked(settings=settings)
        return {
            "bookId": aggregate_book_id,
            "isOpen": is_open,
            "reason": str(breaker.get("reason", "") or ""),
            "openUntil": open_until.isoformat() if isinstance(open_until, datetime) else None,
            "totalCallsLastHour": total_calls,
            "failedCallsLastHour": failed_calls,
            "tokensLastHour": token_usage,
            "peakHourBlocked": peak_hour_skip,
            "cooldownMinutes": max(1, int(settings.get("aiCircuitBreakerCooldownMinutes", 30) or 30)),
            "failureRateThreshold": float(settings.get("aiFailureRateThreshold", 0.0) or 0.0),
            "tokenBudgetPerHour": max(0, int(settings.get("aiTokenBudgetPerHour", 0) or 0)),
        }

    def _prune_ai_window_events(self, aggregate_book_id: str, now: datetime | None = None) -> None:
        now = now or self._now_dt()
        cutoff = now - timedelta(hours=1)
        events = self._ai_window_events.get(aggregate_book_id)
        if not events:
            return
        while events and events[0]["at"] <= cutoff:
            events.popleft()
        if not events:
            self._ai_window_events.pop(aggregate_book_id, None)

    def _record_ai_window_event(
        self,
        aggregate_book_id: str,
        *,
        success: bool,
        tokens: int = 0,
        now: datetime | None = None,
    ) -> None:
        now = now or self._now_dt()
        self._prune_ai_window_events(aggregate_book_id, now)
        events = self._ai_window_events.setdefault(aggregate_book_id, deque())
        events.append({
            "at": now,
            "success": bool(success),
            "tokens": max(0, int(tokens or 0)),
        })
        self._maybe_open_ai_circuit_breaker(aggregate_book_id, now=now)

    def _is_stage3_peak_hour_blocked(self, *, settings: dict[str, Any]) -> bool:
        if not bool(settings.get("stage3PeakHourSkipEnabled", False)):
            return False
        local_hour = datetime.now().hour
        return 19 <= local_hour < 23

    def _maybe_open_ai_circuit_breaker(self, aggregate_book_id: str, *, now: datetime | None = None) -> None:
        now = now or self._now_dt()
        settings = self._book_workflow_settings(aggregate_book_id)
        self._prune_ai_window_events(aggregate_book_id, now)
        events = list(self._ai_window_events.get(aggregate_book_id, ()))
        if not events:
            return

        cooldown_minutes = max(1, int(settings.get("aiCircuitBreakerCooldownMinutes", 30) or 30))
        token_budget = max(0, int(settings.get("aiTokenBudgetPerHour", 0) or 0))
        if token_budget > 0:
            token_usage = sum(max(0, int(event.get("tokens", 0) or 0)) for event in events)
            if token_usage >= token_budget:
                self._ai_circuit_breakers[aggregate_book_id] = {
                    "reason": "token_budget_exceeded",
                    "openUntil": now + timedelta(minutes=cooldown_minutes),
                }
                return

        failure_rate_threshold = float(settings.get("aiFailureRateThreshold", 0.0) or 0.0)
        if 0.0 < failure_rate_threshold <= 1.0 and len(events) >= 3:
            failed_calls = sum(1 for event in events if not bool(event.get("success", False)))
            if failed_calls / len(events) >= failure_rate_threshold:
                self._ai_circuit_breakers[aggregate_book_id] = {
                    "reason": "failure_rate_threshold_exceeded",
                    "openUntil": now + timedelta(minutes=cooldown_minutes),
                }

    def _should_skip_ai_processing(self, aggregate_book_id: str) -> tuple[bool, str]:
        state = self.ai_circuit_breaker_state(aggregate_book_id)
        if state["peakHourBlocked"]:
            return True, "peak_hour_skip"
        if state["isOpen"]:
            return True, state["reason"] or "circuit_breaker_open"
        return False, ""

    def enqueue_book(
        self,
        aggregate_book_id: str,
        payload: dict[str, Any],
        *,
        next_check_time: str | None = None,
    ) -> dict:
        if not self.processing_enabled(aggregate_book_id):
            return {"queued": False, "reason": "aggregate processing disabled", "bookId": aggregate_book_id}

        settings = self._book_workflow_settings(aggregate_book_id)
        source_priority = settings.get("primarySourcePriority") or []
        primary_book_id = primary_book_id_from_payload(payload, source_priority=source_priority)
        if not primary_book_id:
            return {"queued": False, "reason": "aggregate source has no candidates", "bookId": aggregate_book_id}
        primary_source_id = primary_book_id.split(":", 1)[0] if ":" in primary_book_id else ""

        now = self._now()
        interval = self.check_interval_minutes(aggregate_book_id)
        scheduled_next_check = str(next_check_time or now)
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO aggregate_book_tasks
                (aggregate_book_id, name, author, aggregate_payload_json, primary_book_id, primary_source_id,
                 primary_source_name, primary_book_url, primary_toc_url, total_chapters_at_subscribe,
                 status, interval_minutes, last_check_time, next_check_time, error_count, last_error,
                 ai_enabled, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, NULL, ?, 0, '', ?, ?, ?)
                ON CONFLICT(aggregate_book_id) DO UPDATE SET
                    name = COALESCE(NULLIF(excluded.name, ''), aggregate_book_tasks.name),
                    author = COALESCE(NULLIF(excluded.author, ''), aggregate_book_tasks.author),
                    aggregate_payload_json = excluded.aggregate_payload_json,
                    primary_book_id = excluded.primary_book_id,
                    primary_source_id = excluded.primary_source_id,
                    primary_source_name = COALESCE(NULLIF(excluded.primary_source_name, ''), aggregate_book_tasks.primary_source_name),
                    primary_book_url = COALESCE(NULLIF(excluded.primary_book_url, ''), aggregate_book_tasks.primary_book_url),
                    primary_toc_url = COALESCE(NULLIF(excluded.primary_toc_url, ''), aggregate_book_tasks.primary_toc_url),
                    total_chapters_at_subscribe = CASE
                        WHEN COALESCE(aggregate_book_tasks.total_chapters_at_subscribe, 0) > 0 THEN aggregate_book_tasks.total_chapters_at_subscribe
                        ELSE excluded.total_chapters_at_subscribe
                    END,
                    status = CASE
                        WHEN aggregate_book_tasks.status IN ('archived', 'paused') THEN aggregate_book_tasks.status
                        ELSE 'active'
                    END,
                    interval_minutes = excluded.interval_minutes,
                    next_check_time = excluded.next_check_time,
                    ai_enabled = excluded.ai_enabled,
                    updated_at = excluded.updated_at
                """,
                (
                    aggregate_book_id,
                    payload.get("name", ""),
                    payload.get("author", ""),
                    json.dumps(payload, ensure_ascii=False),
                    primary_book_id,
                    primary_source_id,
                    payload.get("primarySourceName", "") or primary_source_id,
                    payload.get("primaryBookUrl", "") or "",
                    payload.get("primaryTocUrl", "") or "",
                    int(payload.get("totalChaptersAtSubscribe", 0) or 0),
                    interval,
                    scheduled_next_check,
                    int(bool(settings.get("aiEnabled", True))),
                    now,
                    now,
                ),
            )
            conn.commit()
        return {
            "queued": True,
            "bookId": aggregate_book_id,
            "primaryBookId": primary_book_id,
            "nextCheckTime": scheduled_next_check,
            "intervalMinutes": interval,
        }

    def register_toc(self, aggregate_book_id: str, payload: dict[str, Any], chapters: list[dict]) -> dict:
        if not self.processing_enabled(aggregate_book_id):
            return {"registered": False, "reason": "aggregate processing disabled", "chapterCount": 0}

        primary_book_id = primary_book_id_from_payload(payload)
        source_id = primary_book_id.split(":", 1)[0] if ":" in primary_book_id else ""
        now = self._now()
        registered = 0
        new_count = 0
        updated_count = 0

        with self._conn() as conn:
            # ── build current chapter map from DB ──────────────────────
            existing_rows = conn.execute(
                "SELECT chapter_id, source_chapter_id, chapter_index, title FROM aggregate_chapter_tasks WHERE aggregate_book_id = ?",
                (aggregate_book_id,),
            ).fetchall()
            existing_by_source: dict[str, dict] = {}
            for row in existing_rows:
                existing_by_source[row[1]] = {
                    "chapterId": row[0], "sourceChapterId": row[1],
                    "chapterIndex": row[2], "title": row[3],
                }

            incoming_source_ids: set[str] = set()

            start_row = conn.execute(
                """
                SELECT start_chapter_index, initial_snapshot_last_index,
                       (SELECT COUNT(*) FROM aggregate_chapter_tasks
                        WHERE aggregate_book_id = ? AND status IN ('processed', 'fallback')) AS processed_count
                FROM aggregate_book_tasks
                WHERE aggregate_book_id = ?
                """,
                (aggregate_book_id, aggregate_book_id),
            ).fetchone()
            start_index = int((start_row[0] if start_row else 1) or 1)
            initial_snapshot_last_index = int((start_row[1] if start_row else 0) or 0)
            processed_count = int((start_row[2] if start_row else 0) or 0)
            # Only lower start_index when no chapters have been processed yet. Once
            # shared files exist we must not shrink the visible window because of a
            # transient short TOC response.
            if chapters and start_index > len(chapters) and processed_count == 0:
                start_index = len(chapters)
                conn.execute(
                    "UPDATE aggregate_book_tasks SET start_chapter_index = ?, updated_at = ? WHERE aggregate_book_id = ?",
                    (start_index, now, aggregate_book_id),
                )

            for index, chapter in enumerate(chapters, start=1):
                raw_url = chapter.get("rawChapterUrl") or chapter.get("chapterUrl", "")
                source_chapter_id = chapter.get("chapterId") or (
                    encode_chapter_id(source_id, raw_url) if source_id and raw_url else f"{aggregate_book_id}:{index}"
                )
                incoming_source_ids.add(source_chapter_id)

                # 提取 isVip 并缓存到内存（不写 DB，后续通过 trace_meta → chapter_index.json 传递）
                is_vip = bool(chapter.get("isVip", False))
                vip_map = self._toc_is_vip_cache.setdefault(aggregate_book_id, {})
                vip_map[index] = is_vip

                aggregate_chapter_url = make_aggregate_chapter_url(
                    aggregate_book_id=aggregate_book_id,
                    source_chapter_id=source_chapter_id,
                    title=chapter.get("title", ""),
                    index=index,
                )
                chapter_id = encode_chapter_id(VIRTUAL_SOURCE_ID, aggregate_chapter_url)

                existing = existing_by_source.get(source_chapter_id)
                if existing:
                    # Chapter exists — update title/index if changed.
                    needs_update = (
                        existing["title"] != chapter.get("title", "")
                        or existing["chapterIndex"] != index
                    )
                    if needs_update:
                        conn.execute(
                            """UPDATE aggregate_chapter_tasks
                               SET title = ?, chapter_index = ?, updated_at = ?
                               WHERE chapter_id = ? AND status NOT IN ('processed', 'fallback')""",
                            (chapter.get("title", ""), index, now, existing["chapterId"]),
                        )
                        updated_count += 1
                else:
                    # New chapter.
                    initial_status = "placeholder" if index < start_index and initial_snapshot_last_index <= 0 else "pending"
                    placeholder_flag = 1 if initial_status == "placeholder" else 0
                    conn.execute(
                        """INSERT OR IGNORE INTO aggregate_chapter_tasks
                           (chapter_id, aggregate_book_id, source_chapter_id, chapter_index, title, status, placeholder, primary_source_chapter_url, created_at, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            chapter_id,
                            aggregate_book_id,
                            source_chapter_id,
                            index,
                            chapter.get("title", ""),
                            initial_status,
                            placeholder_flag,
                            raw_url,
                            now,
                            now,
                        ),
                    )
                    new_count += 1
                registered += 1

            # ── handle removed chapters ───────────────────────────────
            removed_count = 0
            for src_id, info in existing_by_source.items():
                if src_id not in incoming_source_ids:
                    # Chapter no longer in primary source TOC.
                    status_row = conn.execute(
                        "SELECT status FROM aggregate_chapter_tasks WHERE chapter_id = ?",
                        (info["chapterId"],),
                    ).fetchone()
                    if status_row and status_row[0] not in ("processed", "fallback"):
                        conn.execute(
                            "DELETE FROM aggregate_chapter_tasks WHERE chapter_id = ?",
                            (info["chapterId"],),
                        )
                        removed_count += 1

            # ── update book task stats ────────────────────────────────
            conn.execute(
                """UPDATE aggregate_book_tasks
                   SET total_chapters = ?,
                       total_chapters_at_subscribe = CASE
                           WHEN COALESCE(total_chapters_at_subscribe, 0) = 0 THEN ?
                           ELSE total_chapters_at_subscribe
                       END,
                       initial_snapshot_last_index = CASE
                           WHEN COALESCE(initial_snapshot_last_index, 0) = 0 THEN ?
                           ELSE initial_snapshot_last_index
                       END,
                       processed_chapters = (
                           SELECT COUNT(*) FROM aggregate_chapter_tasks
                           WHERE aggregate_book_id = ? AND status IN ('processed', 'fallback')
                       ),
                       failed_chapters = (
                           SELECT COUNT(*) FROM aggregate_chapter_tasks
                           WHERE aggregate_book_id = ? AND status = 'error'
                       ),
                       updated_at = ?
                   WHERE aggregate_book_id = ?""",
                (registered, registered, registered, aggregate_book_id, aggregate_book_id, now, aggregate_book_id),
            )
            conn.commit()

        # 计算 freeChapterEndIndex（最后一个 isVip=false 的章节索引）并写入 metadata.json
        vip_map = self._toc_is_vip_cache.get(aggregate_book_id, {})
        free_chapter_end_index = max(
            (idx for idx, vip in vip_map.items() if not vip),
            default=0,
        )
        self._free_chapter_end_index_cache[aggregate_book_id] = free_chapter_end_index
        self._persist_free_chapter_end_index(aggregate_book_id, payload, free_chapter_end_index)

        self._refresh_shared_book_state(aggregate_book_id)
        return {
            "registered": True, "chapterCount": registered,
            "newChapters": new_count, "updatedChapters": updated_count,
            "removedChapters": removed_count,
        }

    def _persist_free_chapter_end_index(
        self, aggregate_book_id: str, payload: dict[str, Any], free_chapter_end_index: int
    ) -> None:
        """将 freeChapterEndIndex 写入 metadata.json（书级元数据，文件系统优先）。"""
        if free_chapter_end_index <= 0:
            return
        book_name = str(payload.get("name", "") or "").strip()
        author = str(payload.get("author", "") or "").strip()
        if not book_name:
            return
        try:
            from app.services.shared_book_storage import SharedBookStorage

            storage = SharedBookStorage(root=self.db_path.parent / "library")
            storage.update_free_chapter_end_index(
                book_name=book_name, author=author,
                free_chapter_end_index=free_chapter_end_index,
            )
        except Exception:
            logger.debug("persist freeChapterEndIndex failed for %s", aggregate_book_id, exc_info=True)

    def list_due_books(self, limit: int = 10) -> list[dict]:
        now = self._now()
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT aggregate_book_id, aggregate_payload_json, primary_book_id, interval_minutes
                FROM aggregate_book_tasks
                WHERE status IN ('active', 'error') AND (next_check_time IS NULL OR next_check_time <= ?)
                ORDER BY COALESCE(next_check_time, created_at)
                LIMIT ?
                """,
                (now, limit),
            ).fetchall()
        items = []
        for aggregate_book_id, payload_json, primary_book_id, interval_minutes in rows:
            try:
                payload = json.loads(payload_json or "{}")
            except Exception:
                payload = {}
            items.append({
                "aggregateBookId": aggregate_book_id,
                "payload": payload,
                "primaryBookId": primary_book_id,
                "intervalMinutes": interval_minutes or self.check_interval_minutes(aggregate_book_id),
            })
        return [item for item in items if self.processing_enabled(item["aggregateBookId"])]

    async def run_due_once(self, limit: int = 10) -> dict:
        if not self.processing_enabled():
            settings = self._workflow_settings()
            reasons = []
            if not bool(settings.get("autoAggregate", True)):
                reasons.append("autoAggregate=false")
            if not bool(settings.get("processAggregateOnRead", True)):
                reasons.append("processAggregateOnRead=false")
            return {"enabled": False, "processedBooks": 0, "dueBooks": 0,
                    "reason": "aggregate disabled: " + ", ".join(reasons)}
        due_books = self.list_due_books(limit=limit)
        processed = []
        for item in due_books:
            processed.append(await self.run_book_task(item["aggregateBookId"]))
        return {"enabled": True, "dueBooks": len(due_books),
                "processedBooks": len(processed), "items": processed}

    async def bootstrap_book_until_visible(self, aggregate_book_id: str, max_rounds: int = 20) -> dict:
        """Run multiple task rounds after first intake.

        Search visibility is only one milestone. Initial bootstrap should keep
        running until the current pending chapter window is drained, so first
        subscription intake does not stop merely because the book became
        searchable.
        """
        bootstrap_window = max(50, self.backlog_chapter_limit(aggregate_book_id), WINDOW_CHAPTER_LIMIT)
        initial_pending = max(1, self._pending_chapter_count(aggregate_book_id))
        planned_rounds = max(int(max_rounds or 0), (initial_pending + bootstrap_window - 1) // bootstrap_window + 2)
        rounds = 0
        last_result: dict[str, Any] = {}
        ever_visible = False
        for _ in range(planned_rounds):
            rounds += 1
            last_result = await self.run_book_task(aggregate_book_id, chapter_limit=bootstrap_window)
            book = self._library_books().get_book(aggregate_book_id) or {}
            if book.get("searchVisibilityStatus") == "visible":
                ever_visible = True
            pending = self._pending_chapter_count(aggregate_book_id)
            if pending <= 0:
                break
        return {
            "bookId": aggregate_book_id,
            "rounds": rounds,
            "visible": ever_visible,
            "result": last_result,
        }

    def rebuild_book_from_snapshots(self, aggregate_book_id: str) -> dict[str, Any]:
        """Reapply current regex purification to local snapshots without network access."""
        from app.services.aggregate_alignment import build_source_alignment_json

        payload = self._load_aggregate_payload(aggregate_book_id)
        if not payload:
            return {"bookId": aggregate_book_id, "rebuilt": False, "error": "book_not_found"}
        with self._conn() as conn:
            book_row = conn.execute(
                "SELECT primary_source_id FROM aggregate_book_tasks WHERE aggregate_book_id = ?",
                (aggregate_book_id,),
            ).fetchone()
            snapshots = conn.execute(
                """
                SELECT chapter_index, source_id, source_book_id, source_chapter_id, source_chapter_url,
                       title, raw_content, classification
                FROM aggregate_source_snapshots
                WHERE aggregate_book_id = ?
                ORDER BY chapter_index ASC, source_id ASC
                """,
                (aggregate_book_id,),
            ).fetchall()
            chapters = conn.execute(
                """
                SELECT chapter_id, chapter_index, title, status, source_word_count,
                       primary_source_chapter_url, preview_only, source_alignment_json
                FROM aggregate_chapter_tasks
                WHERE aggregate_book_id = ?
                ORDER BY chapter_index ASC
                """,
                (aggregate_book_id,),
            ).fetchall()
        primary_source_id = str(
            payload.get("primarySourceId", "") or (book_row[0] if book_row else "") or ""
        )
        snapshot_classifications = {
            (int(snapshot[0] or 0), str(snapshot[1] or "")): str(snapshot[7] or "")
            for snapshot in snapshots
        }

        for snapshot in snapshots:
            self._save_source_snapshot(
                aggregate_book_id=aggregate_book_id,
                chapter_index=int(snapshot[0] or 0),
                source_id=str(snapshot[1] or ""),
                source_book_id=str(snapshot[2] or ""),
                source_chapter_id=str(snapshot[3] or ""),
                source_chapter_url=str(snapshot[4] or ""),
                title=str(snapshot[5] or ""),
                raw_content=str(snapshot[6] or ""),
                classification=str(snapshot[7] or "rebuild"),
            )

        rewritten = 0
        invalidated = 0
        for chapter in chapters:
            chapter_id = str(chapter[0] or "")
            chapter_index = int(chapter[1] or 0)
            title = str(chapter[2] or "")
            source_word_count = int(chapter[4] or 0)
            official_content = self._load_source_snapshot_content(
                aggregate_book_id=aggregate_book_id,
                chapter_index=chapter_index,
                source_id=primary_source_id,
            )
            official_classification = snapshot_classifications.get(
                (chapter_index, primary_source_id), ""
            )
            try:
                alignment = json.loads(chapter[7] or "{}")
            except (TypeError, ValueError):
                alignment = {}
            selected_source_id = primary_source_id
            preview_only = bool(chapter[6])
            status = "processed"
            content = ""

            # An official full body remains authoritative. Every other case
            # must re-evaluate third-party snapshots against the local anchor.
            if (
                official_content
                and not preview_only
                and (
                    official_classification == "full"
                    or alignment.get("selectedContentSource") == "official"
                )
            ):
                content = official_content
            else:
                candidate = self._try_snapshot_candidate_content(
                    chapter={
                        "aggregateBookId": aggregate_book_id,
                        "chapterIndex": chapter_index,
                        "title": title,
                    },
                    payload=payload,
                    primary_source_id=primary_source_id,
                    official_word_count=source_word_count,
                )
                if candidate and candidate.get("content"):
                    content = str(candidate["content"])
                    selected_source_id = str(candidate.get("source_id", "") or primary_source_id)
                    alignment = dict(candidate.get("alignment_json") or {})
                    status = "fallback"
                    preview_only = False
                elif official_content:
                    # A local official body without a qualifying replacement is
                    # safe to expose only as its existing preview fallback.
                    content = official_content
                    alignment = build_source_alignment_json(
                        selected_content_source="preview_fallback",
                        official_content_length=len(official_content),
                        alignment_passed=False,
                        alignment_reason="official_preview_saved",
                        primary_source_id=primary_source_id,
                    )
                    status = "fallback"
                    preview_only = True
                else:
                    self._invalidate_rebuild_chapter(
                        chapter_id=chapter_id,
                        aggregate_book_id=aggregate_book_id,
                        chapter_index=chapter_index,
                        title=title,
                        primary_source_id=primary_source_id,
                    )
                    invalidated += 1
                    continue
            self._write_chapter_result(
                chapter_id=chapter_id,
                aggregate_book_id=aggregate_book_id,
                title=title,
                chapter_index=chapter_index,
                status=status,
                content=content,
                alignment_json=alignment,
                fallback_source_id=selected_source_id,
                source_word_count=source_word_count,
                primary_source_chapter_url=str(chapter[5] or ""),
                preview_only=preview_only,
            )
            rewritten += 1
        self._refresh_shared_book_state(aggregate_book_id)
        return {
            "bookId": aggregate_book_id,
            "rebuilt": True,
            "networkAccessed": False,
            "snapshotCount": len(snapshots),
            "rewrittenChapters": rewritten,
            "skippedChapters": invalidated,
            "invalidatedChapters": invalidated,
        }

    def _invalidate_rebuild_chapter(
        self,
        *,
        chapter_id: str,
        aggregate_book_id: str,
        chapter_index: int,
        title: str,
        primary_source_id: str,
    ) -> None:
        """Withdraw stale published content when no current official anchor exists."""
        from app.services.aggregate_alignment import build_source_alignment_json

        alignment = build_source_alignment_json(
            selected_content_source="none",
            alignment_passed=False,
            alignment_reason="missing_official_anchor",
            primary_source_id=primary_source_id,
        )
        now = self._now()
        snapshot_refs = self._source_snapshot_refs(
            aggregate_book_id=aggregate_book_id,
            chapter_index=chapter_index,
        )
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE aggregate_chapter_tasks
                SET status = 'error', content_length = 0, processed_content = '',
                    content_file_path = '', placeholder = 0, preview_only = 0,
                    fallback_source_id = '', source_alignment_json = ?,
                    source_snapshot_refs_json = ?, trace_hash = '',
                    error = '本地重建未找到可用官方锚点',
                    last_error_code = 'rebuild_missing_official_anchor',
                    retry_count = 0, preview_retry_count = 0,
                    next_retry_time = ?, last_processed_at = ?, updated_at = ?
                WHERE chapter_id = ?
                """,
                (
                    json.dumps(alignment, ensure_ascii=False),
                    json.dumps(snapshot_refs, ensure_ascii=False),
                    now,
                    now,
                    now,
                    chapter_id,
                ),
            )
            metadata_payload = self._build_shared_metadata_payload(
                conn=conn,
                aggregate_book_id=aggregate_book_id,
                book_name=self._aggregate_book_name(conn, aggregate_book_id),
                book_author=self._aggregate_book_author(conn, aggregate_book_id),
            )
            conn.commit()

        storage = self._shared_book_storage()
        book_name = str(metadata_payload.get("name", "") or "")
        book_author = str(metadata_payload.get("author", "") or "")
        storage.update_chapter_index_entry(
            chapter_index_path=storage.chapter_index_path(book_name=book_name, author=book_author),
            metadata_path=storage.metadata_path(book_name=book_name, author=book_author),
            metadata_payload=metadata_payload,
            entry={
                "index": chapter_index,
                "title": title,
                "file": None,
                "status": "failed",
            },
            chapter_trace={},
        )

    def _job_timeout_seconds(self) -> float:
        try:
            value = float(AppConfig.get().search.aggregate_overall_timeout_seconds)
        except Exception:
            value = 180.0
        return max(30.0, value)

    async def run_book_task(self, aggregate_book_id: str, chapter_limit: int = WINDOW_CHAPTER_LIMIT) -> dict:
        batch_multiplier = max(
            1,
            (max(1, int(chapter_limit)) + WINDOW_CHAPTER_LIMIT - 1) // WINDOW_CHAPTER_LIMIT,
        )
        timeout = self._job_timeout_seconds() * batch_multiplier
        try:
            return await asyncio.wait_for(
                self._run_book_task(aggregate_book_id, chapter_limit=chapter_limit),
                timeout=timeout,
            )
        except asyncio.TimeoutError as exc:
            self.mark_running_snapshots_interrupted(aggregate_book_id, "job_timeout")
            raise TimeoutError(f"订阅任务超过 {timeout:g} 秒，已中断并等待重试") from exc

    async def _run_book_task(self, aggregate_book_id: str, chapter_limit: int = WINDOW_CHAPTER_LIMIT) -> dict:
        from app.services.book_catalog import BookCatalog

        backlog_state = self.stage3_backlog_state(aggregate_book_id)
        if backlog_state["exceeded"]:
            return {
                "bookId": aggregate_book_id,
                "success": False,
                "skipped": True,
                "reason": "stage3_backlog_limit_exceeded",
                "stage3Backlog": backlog_state["backlog"],
                "stage3BacklogLimit": backlog_state["limit"],
            }

        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT aggregate_payload_json, primary_book_id, interval_minutes
                FROM aggregate_book_tasks WHERE aggregate_book_id = ?
                """,
                (aggregate_book_id,),
            ).fetchone()
        if not row:
            return {"bookId": aggregate_book_id, "success": False, "error": "aggregate task not found"}

        payload = json.loads(row[0] or "{}")
        primary_book_id = row[1] or primary_book_id_from_payload(payload)
        interval = int(row[2] or self.check_interval_minutes(aggregate_book_id))
        now = self._now()
        next_check = (datetime.now(timezone.utc) + timedelta(minutes=interval)).isoformat()
        if not self.processing_enabled(aggregate_book_id):
            return {"bookId": aggregate_book_id, "success": False, "error": "aggregate processing disabled"}

        try:
            catalog = BookCatalog()
            try:
                detail = await catalog.book_detail(primary_book_id)
                toc = await catalog.toc(primary_book_id)
            except Exception as toc_exc:
                return self._handle_toc_fetch_failure(aggregate_book_id, toc_exc, next_check)
            toc_error = self._toc_fetch_error_message(toc)
            if toc_error:
                return self._handle_toc_fetch_failure(
                    aggregate_book_id, ValueError(toc_error), next_check
                )
            detail = detail if isinstance(detail, dict) else {}
            chapters = [dict(item) for item in toc.get("chapters", []) if isinstance(item, dict)]
            self.register_toc(aggregate_book_id, payload, chapters)
            payload = await self._ensure_candidate_sources_for_book(
                aggregate_book_id,
                payload,
            )
            chapters_to_process = self._chapters_for_processing(
                aggregate_book_id, limit=chapter_limit
            )
            snapshot_result = await self._run_initial_candidate_prefetch(
                catalog=catalog,
                aggregate_book_id=aggregate_book_id,
                payload=payload,
                official_chapters=chapters,
                chapters_to_process=chapters_to_process,
            )

            chapter_results = []
            if chapters_to_process:
                chapter_sem = asyncio.Semaphore(self._chapter_concurrency_limit())

                async def _process_one(ch: dict) -> dict:
                    async with chapter_sem:
                        return await self._process_chapter(catalog, ch)

                raw_results = await asyncio.gather(
                    *[_process_one(c) for c in chapters_to_process],
                    return_exceptions=True,
                )
                for ch, r in zip(chapters_to_process, raw_results):
                    if isinstance(r, Exception):
                        logger.error(
                            "chapter processing failed: book=%s chapter=%s err=%s",
                            ch.get("aggregateBookId", ""), ch.get("chapterId", ""), r,
                        )
                        chapter_results.append(
                            self._handle_processing_error(
                                r, ch, ch.get("sourceChapterId", ch.get("chapterId", "")),
                                stage="stage1",
                            )
                        )
                    else:
                        chapter_results.append(r)

            latest_chapter = chapters[-1].get("title", "") if chapters else ""
            detail_data = detail.get("data") or {}
            book_status = self._normalize_book_status(detail_data)
            pending_after_run = self._pending_chapter_count(aggregate_book_id)
            preview_recheck_count = 0
            if pending_after_run == 0:
                preview_recheck_count = self._schedule_initial_preview_recheck(
                    aggregate_book_id,
                    now,
                )
                if preview_recheck_count > 0:
                    next_check = now
                    self._log_chapter_step(
                        aggregate_book_id=aggregate_book_id,
                        chapter_index=None,
                        title="",
                        event="initial_preview_recheck_scheduled",
                        stage="stage2",
                        payload={
                            "step": "首次章节处理完成，正在安排预览章节复查",
                            "chapterCount": preview_recheck_count,
                        },
                    )
            if pending_after_run > 0 and len(chapter_results) >= max(1, int(chapter_limit)):
                next_check = (
                    datetime.now(timezone.utc)
                    + timedelta(minutes=self.backlog_recheck_minutes(aggregate_book_id))
                ).isoformat()
            with self._conn() as conn:
                conn.execute(
                    """
                    UPDATE aggregate_book_tasks
                    SET status = 'active', last_check_time = ?, next_check_time = ?,
                        book_status = ?,
                        primary_toc_url = COALESCE(NULLIF(?, ''), primary_toc_url),
                        primary_book_url = COALESCE(NULLIF(?, ''), primary_book_url),
                        name = COALESCE(NULLIF(?, ''), name),
                        author = COALESCE(NULLIF(?, ''), author),
                        cover_url = COALESCE(NULLIF(?, ''), cover_url),
                        intro = COALESCE(NULLIF(?, ''), intro),
                        word_count = COALESCE(NULLIF(?, ''), word_count),
                        last_source_chapter_title = ?,
                        total_chapters = ?,
                        processed_chapters = (
                            SELECT COUNT(*) FROM aggregate_chapter_tasks
                            WHERE aggregate_book_id = ? AND status IN ('processed', 'fallback')
                        ),
                        failed_chapters = (
                            SELECT COUNT(*) FROM aggregate_chapter_tasks
                            WHERE aggregate_book_id = ? AND status = 'error'
                        ),
                        error_count = 0, last_error = '', updated_at = ?
                    WHERE aggregate_book_id = ?
                    """,
                    (
                        now,
                        next_check,
                        book_status,
                        detail_data.get("rawTocUrl", "") or detail_data.get("tocUrl", ""),
                        detail_data.get("rawBookUrl", "") or detail_data.get("bookUrl", ""),
                        detail_data.get("name", ""),
                        detail_data.get("author", ""),
                        detail_data.get("coverUrl", ""),
                        detail_data.get("intro", ""),
                        detail_data.get("wordCountText") or detail_data.get("wordCount", ""),
                        latest_chapter,
                        len(chapters),
                        aggregate_book_id,
                        aggregate_book_id,
                        now,
                        aggregate_book_id,
                    ),
                )
                conn.execute(
                    """
                    UPDATE book_records
                    SET last_chapter = ?, last_seen_at = ?
                    WHERE book_id = ?
                    """,
                    (latest_chapter, now, aggregate_book_id),
                )
                conn.commit()
            self._activate_backfill_if_ready(aggregate_book_id)
            self._refresh_shared_book_state(aggregate_book_id)
            return {
                "bookId": aggregate_book_id,
                "success": True,
                "chapterCount": len(chapters),
                "snapshot": snapshot_result,
                "processedChapters": len(chapter_results),
                "pendingChapters": pending_after_run,
                "previewRecheckChapters": preview_recheck_count,
                "nextCheckTime": next_check,
            }
        except Exception as exc:
            with self._conn() as conn:
                conn.execute(
                    """
                    UPDATE aggregate_book_tasks
                    SET status = 'error', last_check_time = ?, next_check_time = ?,
                        error_count = error_count + 1, last_error = ?, updated_at = ?
                    WHERE aggregate_book_id = ?
                    """,
                    (now, next_check, str(exc), now, aggregate_book_id),
                )
                conn.commit()
            self._activate_backfill_if_ready(aggregate_book_id)
            self._refresh_shared_book_state(aggregate_book_id)

    @staticmethod
    def _toc_fetch_error_message(toc: Any) -> str:
        """Return the plugin error when a TOC response has no usable chapters."""
        if isinstance(toc, dict) and isinstance(toc.get("chapters"), list) and toc["chapters"]:
            return ""
        debug = toc.get("debug") if isinstance(toc, dict) else None
        error = debug.get("error") if isinstance(debug, dict) else None
        if isinstance(error, dict):
            code = str(error.get("code", "") or "").strip()
            message = str(error.get("message", "") or error.get("error", "") or "").strip()
            return ": ".join(part for part in (code, message) if part) or "empty or invalid TOC"
        return str(error or "").strip() or "empty or invalid TOC"

    def _handle_toc_fetch_failure(self, aggregate_book_id: str, exc: Exception, next_check: str) -> dict:
        """Record a TOC fetch failure without touching shared files or book status.

        We keep the book active so the next periodic check will retry. The existing
        shared files and chapter state remain untouched because register_toc() and
        _refresh_shared_book_state() are skipped on this path.
        """
        now = self._now()
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE aggregate_book_tasks
                SET last_check_time = ?, next_check_time = ?,
                    error_count = error_count + 1, last_error = ?, updated_at = ?
                WHERE aggregate_book_id = ?
                """,
                (now, next_check, str(exc), now, aggregate_book_id),
            )
            conn.commit()
        return {
            "bookId": aggregate_book_id,
            "success": False,
            "error": str(exc),
            "nextCheckTime": next_check,
            "tocFetchFailed": True,
        }

    async def _ensure_candidate_sources_for_book(
        self,
        aggregate_book_id: str,
        payload: dict[str, Any],
        *,
        max_candidates: int = 0,
        max_discovery_sources: int = 0,
    ) -> dict[str, Any]:
        """Discover third-party candidate sources for an official-only aggregate book.

        Subscription discovery intentionally searches only official sources. The
        long-running aggregate task owns third-party source discovery so initial
        subscription stays fast while chapter processing still has fallback
        sources.

        Discovery is cached for the current process lifetime so processing many
        preview/empty chapters does not repeatedly search every source. A changed
        payload/source map invalidates the cache; restarting the process clears it.
        """
        payload_signature = self._candidate_source_cache_signature(payload)
        cached = self._candidate_source_cache.get(aggregate_book_id)
        if cached is not None:
            cached_signature, cached_payload = cached
            if (
                cached_signature == payload_signature
                and (
                    max_candidates <= 0
                    or self._candidate_source_count(
                        cached_payload.get("sources", [])
                        if isinstance(cached_payload, dict)
                        else []
                    ) >= max_candidates
                )
            ):
                return cached_payload
            self._candidate_source_cache.pop(aggregate_book_id, None)

        sources = payload.get("sources") if isinstance(payload.get("sources"), list) else []
        normalized_sources = [dict(source) for source in sources if isinstance(source, dict)]
        candidate_count = self._candidate_source_count(normalized_sources)
        configured_limit = int(
            self._book_workflow_settings(aggregate_book_id).get("sourceCandidateLimit", max_candidates) or 0
        )
        candidate_limit = max_candidates if max_candidates > 0 else configured_limit
        if candidate_limit > 0 and candidate_count >= candidate_limit:
            self._candidate_source_cache[aggregate_book_id] = (
                payload_signature,
                payload,
            )
            return payload

        keyword = str(payload.get("name", "") or "").strip()
        author = str(payload.get("author", "") or "").strip()
        if not keyword:
            self._candidate_source_cache[aggregate_book_id] = (
                payload_signature,
                payload,
            )
            return payload

        discovered = await self._discover_third_party_candidates(
            keyword=keyword,
            author=author,
            existing_sources=normalized_sources,
            max_candidates=max(0, candidate_limit - candidate_count) if candidate_limit > 0 else 0,
            max_sources=max_discovery_sources,
        )
        if not discovered:
            self._candidate_source_cache[aggregate_book_id] = (
                payload_signature,
                payload,
            )
            return payload

        merged_sources = self._merge_payload_sources(normalized_sources, discovered)
        updated_payload = dict(payload)
        updated_payload["sources"] = merged_sources
        self._persist_candidate_sources(aggregate_book_id, updated_payload, discovered)
        self._candidate_source_cache[aggregate_book_id] = (
            self._candidate_source_cache_signature(updated_payload),
            updated_payload,
        )
        return updated_payload

    def _candidate_source_cache_signature(self, payload: dict[str, Any]) -> str:
        source_rows = payload.get("sources") if isinstance(payload.get("sources"), list) else []
        normalized_rows = []
        for source in source_rows:
            if not isinstance(source, dict):
                continue
            normalized_rows.append(
                {
                    "sourceId": str(source.get("sourceId", "") or ""),
                    "bookId": str(source.get("bookId", "") or ""),
                    "bookUrl": str(source.get("bookUrl", "") or ""),
                    "tocUrl": str(source.get("tocUrl", "") or ""),
                    "score": int(source.get("score", 0) or 0),
                    "sourceName": str(source.get("sourceName", "") or ""),
                }
            )
        payload_key = {
            "name": str(payload.get("name", "") or ""),
            "author": str(payload.get("author", "") or ""),
            "primarySourceId": str(payload.get("primarySourceId", "") or ""),
            "primaryBookId": str(payload.get("primaryBookId", "") or ""),
            "sources": normalized_rows,
        }
        raw = json.dumps(payload_key, ensure_ascii=False, sort_keys=True)
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()

    def _candidate_source_count(self, sources: list[dict[str, Any]]) -> int:
        source_ids: set[str] = set()
        for source in sources:
            source_id = str(source.get("sourceId", "") or "")
            if not source_id:
                continue
            if not self._is_official_source(source_id):
                source_ids.add(source_id)
        return len(source_ids)

    async def _discover_third_party_candidates(
        self,
        *,
        keyword: str,
        author: str,
        existing_sources: list[dict[str, Any]],
        max_candidates: int,
        max_sources: int,
    ) -> list[dict[str, Any]]:
        from app.services.live_acceptance import normalize_author_key, normalize_text
        from app.source_plugins.scheduler import get_plugin_scheduler

        try:
            scheduler = get_plugin_scheduler()
        except Exception:
            return []

        plugins = [
            plugin
            for plugin in scheduler._enabled_plugins()
            if "search" in getattr(plugin, "capabilities", [])
            and not plugin.metadata.is_official_source()
        ]
        if not plugins:
            return []

        try:
            plugins = scheduler._search_priority_plugins(plugins)
        except Exception:
            pass
        if max_sources > 0:
            plugins = plugins[:max_sources]

        existing_keys = {
            (str(source.get("sourceId", "") or ""), str(source.get("bookId", "") or ""))
            for source in existing_sources
        }
        existing_source_ids = {source_id for source_id, _ in existing_keys if source_id}
        target_name = normalize_text(keyword)
        target_author = normalize_author_key(author)
        found: list[dict[str, Any]] = []
        max_concurrency = self._positive_int(getattr(scheduler, "config", {}).get("max_concurrency"), 3)
        semaphore = asyncio.Semaphore(max_concurrency)

        async def search_plugin(plugin: Any) -> list[dict[str, Any]]:
            async with semaphore:
                source_id = plugin.metadata.id
                try:
                    result = await scheduler.search_one(source_id, keyword, 1)
                except Exception:
                    return []
                items = result.get("items") if isinstance(result, dict) else []
                candidates = []
                for raw in items or []:
                    if not isinstance(raw, dict):
                        continue
                    if normalize_text(raw.get("name", "")) != target_name:
                        continue
                    item_author = normalize_author_key(raw.get("author", ""))
                    if target_author and item_author and item_author != target_author:
                        continue
                    source_id = raw.get("sourceId", "") or plugin.metadata.id
                    raw_book_url = raw.get("rawBookUrl") or raw.get("bookUrl", "")
                    book_id = raw.get("bookId") or (encode_book_id(source_id, raw_book_url) if raw_book_url else "")
                    if not source_id or not book_id or source_id in existing_source_ids:
                        continue
                    candidates.append(
                        {
                            "bookId": book_id,
                            "sourceId": source_id,
                            "sourceName": raw.get("sourceName", "") or plugin.metadata.name,
                            "bookUrl": raw_book_url,
                            "score": int(raw.get("score", 0) or 0),
                            "lastChapter": raw.get("lastChapter", "") or "",
                            "coverUrl": raw.get("coverUrl", "") or "",
                            "intro": raw.get("intro", "") or "",
                            "wordCount": raw.get("wordCount", "") or "",
                            "author": raw.get("author", "") or author,
                            "name": raw.get("name", "") or keyword,
                        }
                    )
                return candidates

        tasks = [asyncio.create_task(search_plugin(plugin)) for plugin in plugins]
        try:
            for task in asyncio.as_completed(tasks):
                candidates = await task
                for candidate in candidates:
                    key = (candidate["sourceId"], candidate["bookId"])
                    if key in existing_keys or candidate["sourceId"] in existing_source_ids:
                        continue
                    existing_keys.add(key)
                    existing_source_ids.add(candidate["sourceId"])
                    found.append(candidate)
                    if max_candidates > 0 and len(found) >= max_candidates:
                        return found
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        return found

    def _merge_payload_sources(
        self,
        existing_sources: list[dict[str, Any]],
        discovered_sources: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        merged: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for source in [*existing_sources, *discovered_sources]:
            source_id = str(source.get("sourceId", "") or "")
            book_id = str(source.get("bookId", "") or "")
            if not source_id or not book_id:
                continue
            key = (source_id, book_id)
            if key in seen:
                continue
            seen.add(key)
            merged.append(dict(source))
        return merged

    def _persist_candidate_sources(
        self,
        aggregate_book_id: str,
        payload: dict[str, Any],
        discovered_sources: list[dict[str, Any]],
    ) -> None:
        now = self._now()
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE aggregate_book_tasks
                SET aggregate_payload_json = ?, updated_at = ?
                WHERE aggregate_book_id = ?
                """,
                (json.dumps(payload, ensure_ascii=False), now, aggregate_book_id),
            )
            for source in discovered_sources:
                conn.execute(
                    """
                    INSERT INTO aggregate_book_sources (
                        aggregate_book_id, source_id, source_book_id, source_name, source_book_url,
                        role, score, enabled, last_seen_at, last_chapter_title, chapter_count, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, 'candidate', ?, 1, ?, ?, 0, ?, ?)
                    ON CONFLICT(aggregate_book_id, source_id, source_book_id) DO UPDATE SET
                        source_name = excluded.source_name,
                        source_book_url = excluded.source_book_url,
                        role = 'candidate',
                        score = excluded.score,
                        enabled = 1,
                        last_seen_at = excluded.last_seen_at,
                        last_chapter_title = excluded.last_chapter_title,
                        updated_at = excluded.updated_at
                    """,
                    (
                        aggregate_book_id,
                        source.get("sourceId", ""),
                        source.get("bookId", ""),
                        source.get("sourceName", ""),
                        source.get("bookUrl", ""),
                        int(source.get("score", 0) or 0),
                        now,
                        source.get("lastChapter", "") or "",
                        now,
                        now,
                    ),
                )
            conn.commit()

    async def _snapshot_candidate_sources(
        self,
        *,
        catalog: Any,
        aggregate_book_id: str,
        payload: dict[str, Any],
        official_chapters: list[dict[str, Any]],
        historical_complete: bool = True,
    ) -> dict[str, Any]:
        """Capture every known third-party chapter before official content is read.

        The run table makes each source independently resumable. Successful
        snapshots are immutable until an explicit re-fetch path is introduced.
        """
        primary_source_id = str(payload.get("primarySourceId", "") or "")
        candidates = [
            item
            for item in (payload.get("sources") or [])
            if isinstance(item, dict)
            and str(item.get("sourceId", "") or "")
            and str(item.get("sourceId", "") or "") != primary_source_id
            and not self._is_official_source(str(item.get("sourceId", "") or ""))
        ]
        if not candidates:
            return {"sourceCount": 0, "captured": 0, "failed": 0}

        results = await asyncio.gather(
            *[
                self._snapshot_one_candidate_source(
                    catalog=catalog,
                    aggregate_book_id=aggregate_book_id,
                    source=item,
                    official_chapters=official_chapters,
                    historical_complete=historical_complete,
                )
                for item in candidates
            ],
            return_exceptions=True,
        )
        captured = 0
        failed = 0
        source_results: list[dict[str, Any]] = []
        for item, result in zip(candidates, results):
            if isinstance(result, Exception):
                failed += 1
                source_results.append({
                    "sourceId": str(item.get("sourceId", "") or ""),
                    "success": False,
                    "error": str(result),
                })
                continue
            source_results.append(result)
            captured += int(result.get("captured", 0) or 0)
            failed += int(result.get("failed", 0) or 0)
        return {
            "sourceCount": len(candidates),
            "captured": captured,
            "failed": failed,
            "sources": source_results,
        }

    async def _run_initial_candidate_prefetch(
        self,
        *,
        catalog: Any,
        aggregate_book_id: str,
        payload: dict[str, Any],
        official_chapters: list[dict[str, Any]],
        chapters_to_process: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Give initial third-party snapshots a bounded head start."""
        result = {"mode": "on_demand", "sourceCount": 0, "captured": 0, "failed": 0}
        if self._published_chapter_count(aggregate_book_id) > 0 or not chapters_to_process:
            return result

        state = self._library_books().source_map_refresh_state(aggregate_book_id)
        if not state.get("completed"):
            return result
        try:
            verified_at = datetime.fromisoformat(
                str(state.get("lastVerifiedAt", "")).replace("Z", "+00:00")
            )
            if verified_at.tzinfo is None:
                verified_at = verified_at.replace(tzinfo=timezone.utc)
        except (TypeError, ValueError):
            return result
        remaining = INITIAL_PREFETCH_GRACE_SECONDS - (
            self._now_dt() - verified_at.astimezone(timezone.utc)
        ).total_seconds()
        if remaining <= 0:
            return result

        pending_indexes = [
            int(item.get("chapterIndex", 0) or 0)
            for item in chapters_to_process
            if int(item.get("chapterIndex", 0) or 0) > 0
        ]
        if not pending_indexes:
            return result
        first_index = min(pending_indexes)
        targets = [
            item
            for item in official_chapters
            if int(item.get("index", 0) or 0) >= first_index
        ][:INITIAL_PREFETCH_CHAPTER_LIMIT]
        if not targets:
            return result

        try:
            prefetched = await asyncio.wait_for(
                self._snapshot_candidate_sources(
                    catalog=catalog,
                    aggregate_book_id=aggregate_book_id,
                    payload=payload,
                    official_chapters=targets,
                    historical_complete=False,
                ),
                timeout=remaining,
            )
        except asyncio.TimeoutError:
            self.mark_running_snapshots_interrupted(
                aggregate_book_id, "prefetch_grace_expired"
            )
            return {**result, "mode": "initial_prefetch_timeout"}
        return {**prefetched, "mode": "initial_prefetch"}

    async def _snapshot_one_candidate_source(
        self,
        *,
        catalog: Any,
        aggregate_book_id: str,
        source: dict[str, Any],
        official_chapters: list[dict[str, Any]],
        historical_complete: bool = True,
    ) -> dict[str, Any]:
        source_id = str(source.get("sourceId", "") or "").strip()
        source_book_id = str(source.get("bookId", "") or "").strip()
        source_name = str(source.get("sourceName", "") or source_id).strip()
        if not source_id or not source_book_id:
            return {"sourceId": source_id, "success": False, "captured": 0, "failed": 1, "error": "missing_source_book"}

        historical_complete = historical_complete or self._snapshot_run_is_complete(
            aggregate_book_id=aggregate_book_id,
            source_id=source_id,
            source_book_id=source_book_id,
        )
        started_at = self._now()
        self._upsert_snapshot_run(
            aggregate_book_id=aggregate_book_id,
            source_id=source_id,
            source_book_id=source_book_id,
            status="loading_toc",
            fetched_chapters=self._source_snapshot_count(aggregate_book_id, source_id),
            started_at=started_at,
        )
        self._log_chapter_step(
            aggregate_book_id=aggregate_book_id,
            chapter_index=None,
            title="",
            event="source_snapshot_start",
            stage="source_snapshot",
            payload={
                "step": f"开始下载第三方源：{source_name}",
                "sourceId": source_id,
                "sourceName": source_name,
            },
        )
        try:
            toc = await self._cached_toc(
                catalog,
                source_book_id,
                aggregate_book_id=aggregate_book_id,
                source_id=source_id,
            )
            source_chapters = [item for item in toc.get("chapters", []) if isinstance(item, dict)]
            if not source_chapters:
                raise ValueError("empty or invalid candidate TOC")
        except Exception as exc:
            self._upsert_snapshot_run(
                aggregate_book_id=aggregate_book_id,
                source_id=source_id,
                source_book_id=source_book_id,
                status="error",
                last_error=str(exc),
                completed_at=self._now(),
            )
            self._log_chapter_step(
                aggregate_book_id=aggregate_book_id,
                chapter_index=None,
                title="",
                event="source_snapshot_error",
                stage="source_snapshot",
                error_message=str(exc),
                payload={
                    "step": f"第三方源下载失败：{source_name}",
                    "sourceId": source_id,
                    "sourceName": source_name,
                },
            )
            return {"sourceId": source_id, "success": False, "captured": 0, "failed": 1, "error": str(exc)}

        toc_hash = hashlib.sha256(
            json.dumps(source_chapters, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        target_chapters = (
            official_chapters[:INITIAL_PREFETCH_CHAPTER_LIMIT]
            if not historical_complete
            else official_chapters
        )
        matched = 0
        captured = 0
        failed = 0
        total_targets = len(target_chapters)
        fetched_total = self._source_snapshot_count(aggregate_book_id, source_id)
        self._upsert_snapshot_run(
            aggregate_book_id=aggregate_book_id,
            source_id=source_id,
            source_book_id=source_book_id,
            status="running",
            toc_hash=toc_hash,
            total_chapters=len(source_chapters),
            fetched_chapters=fetched_total,
        )
        self._log_chapter_step(
            aggregate_book_id=aggregate_book_id,
            chapter_index=None,
            title="",
            event="source_snapshot_toc_complete",
            stage="source_snapshot",
            payload={
                "step": f"第三方源目录加载完成：{source_name}，共 {len(source_chapters)} 章",
                "sourceId": source_id,
                "sourceName": source_name,
                "total": len(source_chapters),
            },
        )

        def report_progress(processed: int, chapter_index: int, title: str) -> None:
            nonlocal fetched_total
            if processed % 10 != 0 and processed != total_targets:
                return
            fetched_total = self._source_snapshot_count(aggregate_book_id, source_id)
            percent = round(processed * 100 / total_targets) if total_targets else 100
            self._upsert_snapshot_run(
                aggregate_book_id=aggregate_book_id,
                source_id=source_id,
                source_book_id=source_book_id,
                status="running",
                toc_hash=toc_hash,
                total_chapters=len(source_chapters),
                matched_chapters=matched,
                fetched_chapters=fetched_total,
                failed_chapters=failed,
                last_error="" if failed == 0 else "chapter_snapshot_failed",
            )
            self._log_chapter_step(
                aggregate_book_id=aggregate_book_id,
                chapter_index=chapter_index,
                title=title,
                event="source_snapshot_progress",
                stage="source_snapshot",
                payload={
                    "step": f"正在下载第三方源：{source_name}（{processed}/{total_targets}）",
                    "sourceId": source_id,
                    "sourceName": source_name,
                    "processed": processed,
                    "total": total_targets,
                    "matched": matched,
                    "fetched": fetched_total,
                    "failed": failed,
                    "percent": percent,
                },
            )

        for processed, official in enumerate(target_chapters, start=1):
            chapter_index = int(official.get("index", 0) or 0)
            title = str(official.get("title", "") or "")
            candidate = self._match_snapshot_candidate_chapter(
                source_chapters=source_chapters,
                chapter_index=chapter_index,
                title=title,
            )
            if candidate is None:
                report_progress(processed, chapter_index, title)
                continue
            matched += 1
            if self._source_snapshot_exists(aggregate_book_id, chapter_index, source_id):
                report_progress(processed, chapter_index, title)
                continue
            candidate_chapter_id = str(candidate.get("chapterId", "") or "")
            if not candidate_chapter_id and candidate.get("chapterUrl"):
                candidate_chapter_id = encode_chapter_id(source_id, str(candidate["chapterUrl"]))
            if not candidate_chapter_id:
                failed += 1
                report_progress(processed, chapter_index, title)
                continue
            try:
                async with self._source_slot(
                    aggregate_book_id=aggregate_book_id,
                    source_id=source_id,
                ):
                    if self._source_snapshot_exists(aggregate_book_id, chapter_index, source_id):
                        report_progress(processed, chapter_index, title)
                        continue
                    result = await catalog.chapter(candidate_chapter_id)
                raw_content = str(result.get("content", "") or "")
                if not raw_content.strip():
                    raise ValueError("empty candidate chapter content")
                self._save_source_snapshot(
                    aggregate_book_id=aggregate_book_id,
                    chapter_index=chapter_index,
                    source_id=source_id,
                    source_book_id=source_book_id,
                    source_chapter_id=candidate_chapter_id,
                    source_chapter_url=self._extract_source_chapter_url(result, candidate_chapter_id),
                    title=str(candidate.get("title", "") or title),
                    raw_content=raw_content,
                    classification="captured",
                )
                captured += 1
            except Exception as exc:
                failed += 1
                self._log_chapter_step(
                    aggregate_book_id=aggregate_book_id,
                    chapter_index=chapter_index,
                    title=title,
                    event="candidate_snapshot_error",
                    stage="stage1",
                    error_message=str(exc),
                    payload={"step": "候补源快照抓取失败", "sourceId": source_id},
                )
            report_progress(processed, chapter_index, title)

        status = "complete" if historical_complete and failed == 0 else "partial"
        fetched_total = self._source_snapshot_count(aggregate_book_id, source_id)
        self._upsert_snapshot_run(
            aggregate_book_id=aggregate_book_id,
            source_id=source_id,
            source_book_id=source_book_id,
            status=status,
            toc_hash=toc_hash,
            total_chapters=len(source_chapters),
            matched_chapters=matched,
            fetched_chapters=fetched_total,
            failed_chapters=failed,
            last_error="" if failed == 0 else "chapter_snapshot_failed",
            completed_at=self._now() if status == "complete" else "",
        )
        self._write_source_snapshot_manifest(
            aggregate_book_id=aggregate_book_id,
            source_id=source_id,
            source_book_id=source_book_id,
            status=status,
            toc_hash=toc_hash,
            total_chapters=len(source_chapters),
            matched_chapters=matched,
            fetched_chapters=fetched_total,
            failed_chapters=failed,
        )
        self._log_chapter_step(
            aggregate_book_id=aggregate_book_id,
            chapter_index=None,
            title="",
            event="source_snapshot_complete" if failed == 0 else "source_snapshot_error",
            stage="source_snapshot",
            payload={
                "step": (
                    f"第三方源下载完成：{source_name}，已保存 {fetched_total} 章"
                    if failed == 0
                    else f"第三方源下载结束：{source_name}，失败 {failed} 章"
                ),
                "sourceId": source_id,
                "sourceName": source_name,
                "total": len(source_chapters),
                "matched": matched,
                "fetched": fetched_total,
                "failed": failed,
                "percent": 100,
                "status": status,
            },
        )
        return {"sourceId": source_id, "success": failed == 0, "captured": captured, "failed": failed, "matched": matched}

    def _match_snapshot_candidate_chapter(
        self,
        *,
        source_chapters: list[dict[str, Any]],
        chapter_index: int,
        title: str,
    ) -> dict[str, Any] | None:
        # 批量采集与实时补全必须共用同一套匹配语义：候选源目录可能插入公告章、
        # 缺章/重编号，纯位置窗口（±2）会系统性漏掉大量章节。统一交由
        # _match_candidate_toc_entries 处理同名微错位优先、序号门限与位置兜底。
        matches = self._match_candidate_toc_entries(
            cand_chapters=source_chapters,
            target_index=chapter_index,
            target_title=title,
        )
        return matches[0] if matches else None

    def _source_snapshot_exists(self, aggregate_book_id: str, chapter_index: int, source_id: str) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                """SELECT 1 FROM aggregate_source_snapshots
                   WHERE aggregate_book_id = ? AND chapter_index = ? AND source_id = ?""",
                (aggregate_book_id, chapter_index, source_id),
            ).fetchone()
        return row is not None

    def _source_snapshot_count(self, aggregate_book_id: str, source_id: str) -> int:
        with self._conn() as conn:
            row = conn.execute(
                """SELECT COUNT(*) FROM aggregate_source_snapshots
                   WHERE aggregate_book_id = ? AND source_id = ?""",
                (aggregate_book_id, source_id),
            ).fetchone()
        return int(row[0] or 0) if row else 0

    def _snapshot_run_is_complete(
        self,
        *,
        aggregate_book_id: str,
        source_id: str,
        source_book_id: str,
    ) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT status FROM aggregate_source_snapshot_runs
                WHERE aggregate_book_id = ? AND source_id = ? AND source_book_id = ?
                """,
                (aggregate_book_id, source_id, source_book_id),
            ).fetchone()
        return bool(row and row[0] == "complete")

    def mark_running_snapshots_interrupted(self, aggregate_book_id: str, reason: str) -> None:
        """Close live snapshot rows when the owning book job is interrupted."""
        now = self._now()
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE aggregate_source_snapshot_runs
                SET status = 'partial', last_error = ?, completed_at = ?, updated_at = ?
                WHERE aggregate_book_id = ? AND status IN ('running', 'loading_toc')
                """,
                (str(reason or "job_interrupted"), now, now, aggregate_book_id),
            )
            conn.commit()

    def _published_chapter_count(self, aggregate_book_id: str) -> int:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) FROM aggregate_chapter_tasks
                WHERE aggregate_book_id = ? AND status IN ('processed', 'fallback')
                """,
                (aggregate_book_id,),
            ).fetchone()
        return int(row[0] or 0) if row else 0

    def _upsert_snapshot_run(
        self,
        *,
        aggregate_book_id: str,
        source_id: str,
        source_book_id: str,
        status: str,
        toc_hash: str = "",
        total_chapters: int = 0,
        matched_chapters: int = 0,
        fetched_chapters: int = 0,
        failed_chapters: int = 0,
        last_error: str = "",
        started_at: str = "",
        completed_at: str = "",
    ) -> None:
        now = self._now()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO aggregate_source_snapshot_runs
                (aggregate_book_id, source_id, source_book_id, status, toc_hash, total_chapters,
                 matched_chapters, fetched_chapters, failed_chapters, last_error, started_at, completed_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(aggregate_book_id, source_id, source_book_id) DO UPDATE SET
                    status = excluded.status,
                    toc_hash = CASE WHEN excluded.toc_hash <> '' THEN excluded.toc_hash ELSE aggregate_source_snapshot_runs.toc_hash END,
                    total_chapters = CASE WHEN excluded.total_chapters > 0 THEN excluded.total_chapters ELSE aggregate_source_snapshot_runs.total_chapters END,
                    matched_chapters = CASE WHEN excluded.matched_chapters > 0 THEN excluded.matched_chapters ELSE aggregate_source_snapshot_runs.matched_chapters END,
                    fetched_chapters = CASE WHEN excluded.fetched_chapters > 0 THEN excluded.fetched_chapters ELSE aggregate_source_snapshot_runs.fetched_chapters END,
                    failed_chapters = excluded.failed_chapters,
                    last_error = excluded.last_error,
                    started_at = CASE WHEN excluded.started_at <> '' THEN excluded.started_at ELSE aggregate_source_snapshot_runs.started_at END,
                    completed_at = excluded.completed_at,
                    updated_at = excluded.updated_at
                """,
                (aggregate_book_id, source_id, source_book_id, status, toc_hash, total_chapters, matched_chapters,
                 fetched_chapters, failed_chapters, last_error, started_at, completed_at, now),
            )
            conn.commit()

    def _write_source_snapshot_manifest(
        self,
        *,
        aggregate_book_id: str,
        source_id: str,
        source_book_id: str,
        status: str,
        toc_hash: str,
        total_chapters: int,
        matched_chapters: int,
        fetched_chapters: int,
        failed_chapters: int,
    ) -> None:
        book_name, book_author = self._shared_book_identity(aggregate_book_id)
        if not book_name:
            return
        self._shared_book_storage().atomic_write_json(
            self._shared_book_storage().source_snapshot_manifest_path(
                book_name=book_name,
                author=book_author,
                source_id=source_id,
            ),
            {
                "schemaVersion": 1,
                "aggregateBookId": aggregate_book_id,
                "sourceId": source_id,
                "sourceBookId": source_book_id,
                "status": status,
                "tocHash": toc_hash,
                "totalChapters": total_chapters,
                "matchedChapters": matched_chapters,
                "fetchedChapters": fetched_chapters,
                "failedChapters": failed_chapters,
                "updatedAt": self._now(),
            },
        )

    def _positive_int(self, value: Any, default: int) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return default
        return parsed if parsed > 0 else default

    def _chapters_for_processing(self, aggregate_book_id: str, limit: int = WINDOW_CHAPTER_LIMIT) -> list[dict]:
        terminal_codes = tuple(sorted(str(code) for code in STAGE1_NO_RETRY_ERROR_CODES))
        placeholders = ",".join("?" for _ in terminal_codes) if terminal_codes else "''"
        now = self._now()
        with self._conn() as conn:
            requested_limit = max(1, int(limit))
            rows: list[tuple] = []
            if requested_limit > 0:
                rows.extend(
                    conn.execute(
                        f"""
                        SELECT chapter_id, source_chapter_id, title, status, chapter_index, aggregate_book_id, placeholder
                        FROM aggregate_chapter_tasks
                        WHERE aggregate_book_id = ?
                          AND placeholder = 0
                          AND COALESCE(manual_supplement, 0) = 0
                          AND (
                            status = 'pending'
                            OR (
                              status = 'error'
                              AND (next_retry_time IS NULL OR next_retry_time <= ?)
                              AND (last_error_code IS NULL OR last_error_code NOT IN ({placeholders}))
                              AND (
                                COALESCE(retry_count, 0) < ?
                                OR (COALESCE(retry_count, 0) = ? AND next_retry_time IS NOT NULL)
                              )
                            )
                          )
                        ORDER BY CASE WHEN status = 'error' THEN 0 ELSE 1 END,
                                 COALESCE(chapter_index, 999999), created_at
                        LIMIT ?
                        """,
                        (
                            aggregate_book_id,
                            now,
                            *terminal_codes,
                            len(RETRY_DELAYS_MINUTES),
                            len(RETRY_DELAYS_MINUTES),
                            requested_limit,
                        ),
                    ).fetchall()
                )
            remaining_limit = max(0, requested_limit - len(rows))
            if remaining_limit > 0:
                rows.extend(
                    conn.execute(
                        f"""
                        SELECT chapter_id, source_chapter_id, title, status, chapter_index, aggregate_book_id, placeholder
                        FROM aggregate_chapter_tasks
                        WHERE aggregate_book_id = ?
                          AND placeholder = 1
                          AND COALESCE(manual_supplement, 0) = 0
                          AND (
                            status = 'pending'
                            OR (
                              status = 'error'
                              AND (next_retry_time IS NULL OR next_retry_time <= ?)
                              AND (last_error_code IS NULL OR last_error_code NOT IN ({placeholders}))
                              AND (
                                COALESCE(retry_count, 0) < ?
                                OR (COALESCE(retry_count, 0) = ? AND next_retry_time IS NOT NULL)
                              )
                            )
                          )
                        ORDER BY CASE WHEN status = 'error' THEN 0 ELSE 1 END,
                                 COALESCE(chapter_index, 999999), created_at
                        LIMIT ?
                        """,
                        (
                            aggregate_book_id,
                            now,
                            *terminal_codes,
                            len(RETRY_DELAYS_MINUTES),
                            len(RETRY_DELAYS_MINUTES),
                            remaining_limit,
                        ),
                    ).fetchall()
                )
            remaining_limit = max(0, requested_limit - len(rows))
            if remaining_limit > 0:
                due_stage3_ids = self._due_stage3_deferred_chapter_ids(aggregate_book_id)
                existing_ids = {row[0] for row in rows}
                due_stage3_ids = [chapter_id for chapter_id in due_stage3_ids if chapter_id not in existing_ids]
                if due_stage3_ids:
                    placeholders = ",".join("?" for _ in due_stage3_ids)
                    rows.extend(
                        conn.execute(
                            f"""
                            SELECT chapter_id, source_chapter_id, title, status, chapter_index, aggregate_book_id, placeholder
                            FROM aggregate_chapter_tasks
                            WHERE aggregate_book_id = ?
                              AND chapter_id IN ({placeholders})
                              AND COALESCE(manual_supplement, 0) = 0
                              AND status = 'fallback'
                              AND content_file_path IS NOT NULL
                              AND content_file_path != ''
                            ORDER BY COALESCE(chapter_index, 999999), created_at
                            LIMIT ?
                            """,
                            (aggregate_book_id, *due_stage3_ids, remaining_limit),
                        ).fetchall()
                    )
            remaining_limit = max(0, requested_limit - len(rows))
            if remaining_limit > 0:
                existing_ids = {row[0] for row in rows}
                placeholders = ",".join("?" for _ in existing_ids) if existing_ids else "''"
                rows.extend(
                    conn.execute(
                        f"""
                        SELECT chapter_id, source_chapter_id, title, status, chapter_index, aggregate_book_id, placeholder
                        FROM aggregate_chapter_tasks
                        WHERE aggregate_book_id = ?
                          AND status = 'fallback'
                          AND preview_only = 1
                          AND COALESCE(manual_supplement, 0) = 0
                          AND (next_retry_time IS NULL OR next_retry_time <= ?)
                          AND (preview_retry_count IS NULL OR preview_retry_count <= ?)
                          AND chapter_id NOT IN ({placeholders})
                        ORDER BY COALESCE(chapter_index, 999999), created_at
                        LIMIT ?
                        """,
                        (aggregate_book_id, now, len(PREVIEW_RETRY_DELAYS_MINUTES), *existing_ids, remaining_limit),
                    ).fetchall()
                )
        return [
            {
                "chapterId": row[0],
                "sourceChapterId": row[1],
                "title": row[2],
                "status": row[3],
                "chapterIndex": row[4],
                "aggregateBookId": row[5],
                "placeholder": bool(row[6]),
            }
            for row in rows
        ]

    # ── helpers for _process_chapter ──────────────────────────────────────

    def _load_aggregate_payload(self, aggregate_book_id: str) -> dict[str, Any]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT aggregate_payload_json FROM aggregate_book_tasks WHERE aggregate_book_id = ?",
                (aggregate_book_id,),
            ).fetchone()
        if not row:
            return {}
        try:
            return json.loads(row[0] or "{}")
        except Exception:
            return {}

    def _is_official_source(self, source_id: str) -> bool:
        from app.source_plugins.loader import PluginLoader

        if self._official_source_flags is None:
            try:
                plugins = PluginLoader().load_all()
            except Exception:
                return False
            self._official_source_flags = {
                plugin_id: bool(plugin.metadata.is_official_source())
                for plugin_id, plugin in plugins.items()
            }
        return self._official_source_flags.get(source_id, False)

    def _book_has_official_source(self, aggregate_book_id: str, payload: dict) -> bool:
        """检查聚合书籍是否有任何官方源（主源或候选源）。"""
        primary_source_id = payload.get("primarySourceId", "")
        if primary_source_id and self._is_official_source(primary_source_id):
            return True
        sources = payload.get("sources") or []
        for src in sources:
            sid = src.get("sourceId", "") if isinstance(src, dict) else ""
            if sid and self._is_official_source(sid):
                return True
        return False

    def _candidate_sources_from_payload(
        self,
        payload: dict,
        primary_source_id: str,
        aggregate_book_id: str = "",
        *,
        include_expansion: bool = False,
    ) -> list[dict]:
        from app.services.shared_book_source_map import SharedBookSourceMapService

        service = SharedBookSourceMapService(library_books=self._library_books())
        candidates = service.load_current_source_map_refs(
            aggregate_book_id,
            payload=payload,
            primary_source_id=primary_source_id,
        )
        # Candidate sources are always third-party content mirrors.  A second
        # official plugin may be present in an older source map, but must not
        # be used as a cross-source fallback for the primary official source.
        candidates = [
            item for item in candidates
            if not self._is_official_source(str(item.get("sourceId", "") or ""))
        ]
        settings = self._book_workflow_settings(aggregate_book_id)
        priority = [str(x) for x in settings.get("candidateSourcePriority", []) or []]
        if priority:
            order_map = {sid: idx for idx, sid in enumerate(priority)}
            candidates = [item for item in candidates if item.get("sourceId", "") in order_map]
            candidates.sort(
                key=lambda s: (
                    order_map.get(s.get("sourceId", ""), 9999),
                    -int(s.get("priority", s.get("score", 0)) or 0),
                )
            )
        else:
            # Browser sources remain available as a last resort, but must not
            # occupy the batch queue before ordinary HTTP candidates.
            candidates.sort(
                key=lambda s: (
                    str(s.get("sourceId", "") or "") in self._browser_source_ids,
                    -int(s.get("priority", s.get("score", 0)) or 0),
                )
            )
        try:
            limit = int(settings.get("sourceCandidateLimit", 0) or 0)
        except (TypeError, ValueError):
            limit = 0
        if limit > 0:
            return candidates[:limit]
        # Start with three mirrors.  A chapter that cannot form a three-source
        # consensus may inspect the remaining mapped HTTP mirrors, but never
        # performs an unbounded crawl.
        max_count = (
            CROSS_SOURCE_MAX_COMPARE_COUNT
            if include_expansion
            else CROSS_SOURCE_INITIAL_COMPARE_COUNT
        )
        return candidates[:max_count]

    def _get_ai_service(self, aggregate_book_id: str = ""):
        if not self.ai_aggregate_enabled(aggregate_book_id):
            return None
        if self._ai_service is not None:
            return self._ai_service

        # Build production AI service from settings only when enabled & configured.
        workflow = self._book_workflow_settings(aggregate_book_id)

        provider_config = AggregateSettingsRepository(self.db_path).ai_provider_config()
        base_url = str(provider_config.get("baseUrl") or "").strip()
        api_key = str(provider_config.get("apiKey") or "").strip()
        model = str(provider_config.get("model") or "").strip()
        if not base_url or not api_key or not model:
            return None

        try:
            from app.ai.client import OpenAICompatibleClient
            from app.services.aggregate_ai_service import AggregateAIService

            lexicon = None
            if bool(workflow.get("sensitiveLexiconEnabled", True)):
                raw_lex_path = workflow.get("sensitiveLexiconPath")
                if raw_lex_path:
                    from app.services.aggregate_settings import resolve_sensitive_lexicon_path

                    lex_path = resolve_sensitive_lexicon_path(raw_lex_path)
                    if not lex_path.exists():
                        lex_path.mkdir(parents=True, exist_ok=True)
                        logger.info(
                            "Created empty sensitive lexicon directory: %s (resolved from %s)",
                            lex_path,
                            raw_lex_path,
                        )
                    else:
                        try:
                            from app.ai.lexicon import SensitiveLexiconScanner

                            lexicon = SensitiveLexiconScanner.from_path(lex_path)
                            logger.debug(
                                "Loaded sensitive lexicon from %s: %d words",
                                lex_path,
                                lexicon.word_count,
                            )
                        except Exception as exc:
                            logger.warning(
                                "Failed to load sensitive lexicon from %s: %s",
                                lex_path,
                                exc,
                            )
                            logger.debug("Lexicon load traceback", exc_info=True)

            client = OpenAICompatibleClient(provider_config)
            self._ai_service = AggregateAIService(client=client, lexicon=lexicon)
        except Exception:
            return None

        return self._ai_service

    async def _cached_toc(
        self,
        catalog,
        book_id: str,
        *,
        aggregate_book_id: str = "",
        source_id: str = "",
        on_fetch_start: Any = None,
    ) -> dict:
        if book_id in self._toc_cache:
            return self._toc_cache[book_id]
        if aggregate_book_id and source_id:
            async with self._source_slot(
                aggregate_book_id=aggregate_book_id,
                source_id=source_id,
            ):
                if book_id in self._toc_cache:
                    return self._toc_cache[book_id]
                if callable(on_fetch_start):
                    on_fetch_start()
                toc = await catalog.toc(book_id)
        else:
            if callable(on_fetch_start):
                on_fetch_start()
            toc = await catalog.toc(book_id)
        self._toc_cache[book_id] = toc
        return toc

    def invalidate_candidate_toc_cache(self, aggregate_book_id: str) -> None:
        payload = self._load_aggregate_payload(aggregate_book_id)
        for source in payload.get("sources", []):
            if isinstance(source, dict):
                self._toc_cache.pop(str(source.get("bookId", "") or ""), None)

    def _safe_log_text(self, value: Any, *, max_length: int = 300) -> str:
        text = str(value or "")
        text = re.sub(r"https?://\S+", "[url]", text)
        return text[:max_length]

    def _log_chapter_step(
        self,
        *,
        aggregate_book_id: str,
        chapter_index: int | None,
        title: str,
        event: str,
        stage: str,
        payload: dict[str, Any] | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        contract = self._shared_book_storage_contract()
        if not contract.get("useSharedBookStorage"):
            return
        try:
            book_name = self._aggregate_book_name_from_cache(aggregate_book_id)
            book_author = self._aggregate_book_author_from_cache(aggregate_book_id)
            if not book_name:
                return
            safe_payload = dict(payload or {})
            if title and "title" not in safe_payload:
                safe_payload["title"] = title
            if error_message:
                error_message = self._safe_log_text(error_message)
            self._shared_book_process_logger().append(
                book_name=book_name,
                author=book_author,
                event=event,
                book_id=aggregate_book_id,
                chapter_index=chapter_index,
                stage=stage,
                error_code=error_code,
                error_message=error_message,
                payload=safe_payload,
            )
        except Exception:
            logger.debug("Failed to write chapter step log", exc_info=True)

    # ── core chapter processing state machine ────────────────────────────

    def _validate_word_count(
        self, content: str, official_word_count: int, *,
        enforce: bool = False, aggregate_book_id: str = "",
        chapter_index: int | None = None, source_id: str = "",
    ) -> dict[str, Any]:
        """校验净化后正文长度与官方字数是否匹配。

        initial 阶段 enforce=False，仅记录偏差不拒绝。
        返回 dict: {passed, actual, expected, ratio, lowerBound, upperBound}。
        """
        result: dict[str, Any] = {
            "passed": True, "actual": 0, "expected": 0,
            "ratio": 0.0, "lowerBound": WORD_COUNT_TOLERANCE_LOWER,
            "upperBound": WORD_COUNT_TOLERANCE_UPPER, "enforced": enforce,
        }
        if not official_word_count or official_word_count <= 0:
            return result
        actual = len((content or "").replace("\n", "").replace(" ", "").replace("\r", ""))
        expected = official_word_count
        ratio = actual / expected if expected else 0.0
        result["actual"] = actual
        result["expected"] = expected
        result["ratio"] = round(ratio, 4)
        passed = WORD_COUNT_TOLERANCE_LOWER <= ratio <= WORD_COUNT_TOLERANCE_UPPER
        result["passed"] = passed
        if not passed:
            self._log_chapter_step(
                aggregate_book_id=aggregate_book_id,
                chapter_index=chapter_index,
                title="",
                event="word_count_deviation",
                stage="stage1",
                payload={
                    "step": "字数校验偏差",
                    "sourceId": source_id,
                    "actual": actual,
                    "expected": expected,
                    "ratio": round(ratio, 4),
                    "lowerBound": WORD_COUNT_TOLERANCE_LOWER,
                    "upperBound": WORD_COUNT_TOLERANCE_UPPER,
                    "enforced": enforce,
                },
            )
            if enforce:
                result["passed"] = False
            else:
                result["passed"] = True
        return result

    async def _process_chapter(self, catalog, chapter: dict) -> dict:
        """Process a single chapter through the 3-path pipeline.

        Path 1: Content is full → purify → optional AI → processed
        Path 2: Content is preview → try candidates → fallback
        Path 3: Content is empty → try candidates → error
        """
        from app.services.aggregate_alignment import (
            build_source_alignment_json,
            classify_source_content,
        )

        chapter_id = chapter["chapterId"]
        source_chapter_id = chapter.get("sourceChapterId") or chapter_id
        aggregate_book_id = chapter.get("aggregateBookId", "")
        title = chapter.get("title", "")
        chapter_index = chapter.get("chapterIndex")

        payload = self._load_aggregate_payload(aggregate_book_id)
        primary_source_id = payload.get("primarySourceId", "")
        if not primary_source_id:
            primary_source_id = source_chapter_id.split(":")[0] if ":" in source_chapter_id else ""

        current_stage = "stage1"
        try:
            self._log_chapter_step(
                aggregate_book_id=aggregate_book_id,
                chapter_index=chapter_index,
                title=title,
                event="chapter_start",
                stage="stage1",
                payload={"step": "开始处理章节", "sourceId": primary_source_id},
            )
            self._log_chapter_step(
                aggregate_book_id=aggregate_book_id,
                chapter_index=chapter_index,
                title=title,
                event="official_fetch_start",
                stage="stage1",
                payload={"step": "正在拉取官方源", "sourceId": primary_source_id},
            )
            async with self._source_slot(
                aggregate_book_id=aggregate_book_id,
                source_id=primary_source_id,
            ):
                result = await catalog.chapter(source_chapter_id)
            content = result.get("content", "")
            source_word_count = self._extract_source_word_count(result)
            primary_source_chapter_url = self._extract_source_chapter_url(result, source_chapter_id)
            self._log_chapter_step(
                aggregate_book_id=aggregate_book_id,
                chapter_index=chapter_index,
                title=title,
                event="official_fetch_complete",
                stage="stage1",
                payload={
                    "step": "官方源拉取完成",
                    "sourceId": primary_source_id,
                    "contentLength": len(content or ""),
                    "sourceWordCount": source_word_count,
                    "fromSnapshot": False,
                },
            )
            fetched_content = content
            if not content:
                content = self._load_source_snapshot_content(
                    aggregate_book_id=aggregate_book_id,
                    chapter_index=chapter_index or 0,
                    source_id=primary_source_id,
                )
                if content:
                    self._log_chapter_step(
                        aggregate_book_id=aggregate_book_id,
                        chapter_index=chapter_index,
                        title=title,
                        event="official_snapshot_used",
                        stage="stage1",
                        payload={
                            "step": "官方源为空，使用本地快照",
                            "sourceId": primary_source_id,
                            "contentLength": len(content or ""),
                            "fromSnapshot": True,
                        },
                    )
            is_official = self._is_official_source(primary_source_id)
            book_has_official = self._book_has_official_source(aggregate_book_id, payload)
            toc_vip_map = self._toc_is_vip_cache.get(aggregate_book_id, {})
            chapter_is_vip = bool(toc_vip_map.get(chapter_index or 0, False))
            classification = classify_source_content(
                content,
                source_id=primary_source_id,
                is_official=is_official,
                source_word_count=source_word_count,
                preview_only_hint=self._extract_preview_only(result),
                extra=result.get("extra") if isinstance(result.get("extra"), dict) else {},
                is_paid=self._extract_is_paid(result),
                is_vip=chapter_is_vip,
            )
            if fetched_content:
                self._save_source_snapshot(
                    aggregate_book_id=aggregate_book_id,
                    chapter_index=chapter_index or 0,
                    source_id=primary_source_id,
                    source_book_id=self._source_book_id_from_payload(payload, primary_source_id),
                    source_chapter_id=source_chapter_id,
                    title=title,
                    raw_content=fetched_content,
                    source_chapter_url=primary_source_chapter_url,
                    classification=str(classification.get("classification", "unknown") or "unknown"),
                )
            self._log_chapter_step(
                aggregate_book_id=aggregate_book_id,
                chapter_index=chapter_index,
                title=title,
                event="official_classified",
                stage="stage1",
                payload={
                    "step": "官方源内容识别完成",
                    "sourceId": primary_source_id,
                    "classification": classification.get("classification", ""),
                    "contentLength": len(content or ""),
                    "previewOnly": classification.get("classification") == "preview",
                },
            )

            # ── Path 1: full content ────────────────────────────────────
            if classification["classification"] == "full":
                return await self._process_full_content(
                    catalog=catalog, chapter=chapter, content=content,
                    classification=classification, primary_source_id=primary_source_id,
                    payload=payload,
                    source_word_count=source_word_count,
                    primary_source_chapter_url=primary_source_chapter_url,
                )

            # ── Path 2: preview content → save preview first, then try candidates ──
            current_stage = "stage2"
            if classification["classification"] == "preview":
                if not book_has_official:
                    # 无官方源降级：不保存为 "preview_fallback"（那是官方预览专属标记），
                    # 直接尝试候选源补全。候选成功 → processed；全失败但有短内容 → suspect。
                    self._log_chapter_step(
                        aggregate_book_id=aggregate_book_id,
                        chapter_index=chapter_index,
                        title=title,
                        event="candidate_discovery_start",
                        stage="stage2",
                        payload={"step": "无官方源，第三方内容偏短，正在准备候选源"},
                    )
                    payload = await self._ensure_candidate_sources_for_book(
                        aggregate_book_id, payload,
                    )
                    self._log_chapter_step(
                        aggregate_book_id=aggregate_book_id,
                        chapter_index=chapter_index,
                        title=title,
                        event="candidate_merge_start",
                        stage="stage2",
                        payload={"step": "正在从候选源补全章节"},
                    )
                    candidate_result = await self._try_candidate_content(
                        catalog, chapter, payload, primary_source_id,
                        official_word_count=source_word_count,
                    )
                    if candidate_result and candidate_result.get("content"):
                        self._log_chapter_step(
                            aggregate_book_id=aggregate_book_id,
                            chapter_index=chapter_index,
                            title=title,
                            event="candidate_selected",
                            stage="stage2",
                            payload={
                                "step": "候选源补全成功",
                                "sourceId": candidate_result.get("source_id", ""),
                                "contentLength": len(candidate_result.get("content", "") or ""),
                            },
                        )
                        return await self._process_full_content(
                            catalog=catalog, chapter=chapter,
                            content=candidate_result["content"],
                            classification={
                                **classification,
                                "alignmentJson": candidate_result.get("alignment_json") or {},
                            },
                            primary_source_id=candidate_result["source_id"],
                            payload=payload,
                            fallback_source_id=candidate_result["source_id"],
                            source_word_count=source_word_count,
                            primary_source_chapter_url=primary_source_chapter_url,
                            cross_source_selected=True,
                        )
                    # 候选源全失败 → 有短内容标记 suspect，无内容标记 error
                    if content:
                        if self.purify_enabled(aggregate_book_id):
                            purified_short, _ = self._purify_content_with_lexicon(
                                content, source_id=primary_source_id
                            )
                        else:
                            purified_short = self._normalize_content_light(content)
                        suspect_alignment = build_source_alignment_json(
                            selected_content_source="third_party_suspect",
                            alignment_passed=False,
                            alignment_reason="no_official_baseline_short_content",
                            primary_source_id=primary_source_id,
                        )
                        self._log_chapter_step(
                            aggregate_book_id=aggregate_book_id,
                            chapter_index=chapter_index,
                            title=title,
                            event="suspect_content_saved",
                            stage="stage2",
                            payload={
                                "step": "无官方源且候选源未补全，短内容标记为存疑",
                                "contentLength": len(purified_short or ""),
                            },
                        )
                        self._write_chapter_result(
                            chapter_id=chapter_id, aggregate_book_id=aggregate_book_id,
                            title=title, chapter_index=chapter_index,
                            status="fallback", content=purified_short,
                            alignment_json=suspect_alignment,
                            fallback_source_id=primary_source_id,
                            source_word_count=source_word_count,
                            primary_source_chapter_url=primary_source_chapter_url,
                        )
                        return {"chapterId": chapter_id, "success": True,
                                "contentLength": len(purified_short), "fallback": True}
                    raise ValueError("empty chapter content from all sources (no official baseline)")

                # 有官方源：先保存官方预览到本地（即使后续全失败，预览不丢）
                if self.purify_enabled(aggregate_book_id):
                    purified_preview, _ = self._purify_content_with_lexicon(
                        content, source_id=primary_source_id
                    )
                else:
                    purified_preview = self._normalize_content_light(content)
                preview_alignment = build_source_alignment_json(
                    selected_content_source="preview_fallback",
                    alignment_passed=False,
                    alignment_reason="official_preview_saved",
                    primary_source_id=primary_source_id,
                )
                self._log_chapter_step(
                    aggregate_book_id=aggregate_book_id,
                    chapter_index=chapter_index,
                    title=title,
                    event="preview_saved_first",
                    stage="stage2",
                    payload={
                        "step": "官方预览已先写入本地",
                        "sourceId": primary_source_id,
                        "contentLength": len(purified_preview or ""),
                    },
                )
                self._write_chapter_result(
                    chapter_id=chapter_id, aggregate_book_id=aggregate_book_id,
                    title=title, chapter_index=chapter_index,
                    status="fallback", content=purified_preview,
                    alignment_json=preview_alignment,
                    fallback_source_id=primary_source_id,
                    source_word_count=source_word_count,
                    primary_source_chapter_url=primary_source_chapter_url,
                    preview_only=True,
                )

                # Step 2: 遍历第三方候选源
                self._log_chapter_step(
                    aggregate_book_id=aggregate_book_id,
                    chapter_index=chapter_index,
                    title=title,
                    event="candidate_discovery_start",
                    stage="stage2",
                    payload={"step": "官方源是预览，正在准备候选源"},
                )
                payload = await self._ensure_candidate_sources_for_book(
                    aggregate_book_id,
                    payload,
                )
                self._log_chapter_step(
                    aggregate_book_id=aggregate_book_id,
                    chapter_index=chapter_index,
                    title=title,
                    event="candidate_merge_start",
                    stage="stage2",
                    payload={"step": "正在从候选源补全章节"},
                )
                candidate_result = await self._try_candidate_content(
                    catalog, chapter, payload, primary_source_id,
                    official_word_count=source_word_count,
                )
                if candidate_result and candidate_result.get("content"):
                    self._log_chapter_step(
                        aggregate_book_id=aggregate_book_id,
                        chapter_index=chapter_index,
                        title=title,
                        event="candidate_selected",
                        stage="stage2",
                        payload={
                            "step": "候选源补全成功",
                            "sourceId": candidate_result.get("source_id", ""),
                            "contentLength": len(candidate_result.get("content", "") or ""),
                        },
                    )
                    # 候选源补全成功 → 用候选内容覆盖预览
                    return await self._process_full_content(
                        catalog=catalog, chapter=chapter,
                        content=candidate_result["content"],
                        classification={
                            **classification,
                            "alignmentJson": candidate_result.get("alignment_json") or {},
                        },
                        primary_source_id=candidate_result["source_id"],
                        payload=payload,
                        fallback_source_id=candidate_result["source_id"],
                        source_word_count=source_word_count,
                        primary_source_chapter_url=primary_source_chapter_url,
                        cross_source_selected=True,
                    )

                # Step 3: 全部候选源失败，预览已保存
                self._log_chapter_step(
                    aggregate_book_id=aggregate_book_id,
                    chapter_index=chapter_index,
                    title=title,
                    event="preview_fallback_all_candidates_failed",
                    stage="stage2",
                    payload={
                        "step": "所有候选源未补全，保留官方预览",
                        "contentLength": len(purified_preview or ""),
                    },
                )
                return {"chapterId": chapter_id, "success": True,
                        "contentLength": len(purified_preview), "fallback": True}

            # ── Path 3: empty → try candidates ──────────────────────────
            current_stage = "stage2"
            self._log_chapter_step(
                aggregate_book_id=aggregate_book_id,
                chapter_index=chapter_index,
                title=title,
                event="candidate_discovery_start",
                stage="stage2",
                payload={"step": "官方源为空，正在准备候选源"},
            )
            payload = await self._ensure_candidate_sources_for_book(
                aggregate_book_id,
                payload,
            )
            self._log_chapter_step(
                aggregate_book_id=aggregate_book_id,
                chapter_index=chapter_index,
                title=title,
                event="candidate_merge_start",
                stage="stage2",
                payload={"step": "正在从候选源补全章节"},
            )
            candidate_result = await self._try_candidate_content(
                catalog, chapter, payload, primary_source_id,
                official_word_count=source_word_count,
            )
            if candidate_result and candidate_result.get("content"):
                self._log_chapter_step(
                    aggregate_book_id=aggregate_book_id,
                    chapter_index=chapter_index,
                    title=title,
                    event="candidate_selected",
                    stage="stage2",
                    payload={
                        "step": "候选源补全成功",
                        "sourceId": candidate_result.get("source_id", ""),
                        "contentLength": len(candidate_result.get("content", "") or ""),
                    },
                )
                return await self._process_full_content(
                    catalog=catalog, chapter=chapter,
                    content=candidate_result["content"],
                    classification={
                        **classification,
                        "alignmentJson": candidate_result.get("alignment_json") or {},
                    },
                    primary_source_id=candidate_result["source_id"],
                    payload=payload,
                    fallback_source_id=candidate_result["source_id"],
                    source_word_count=source_word_count,
                    primary_source_chapter_url=primary_source_chapter_url,
                    cross_source_selected=True,
                )
            if candidate_result and candidate_result.get("all_sources_failed"):
                raise ValueError("empty supplement content from all current sources")
            raise ValueError("empty chapter content from all sources")

        except Exception as exc:
            return self._handle_processing_error(exc, chapter, source_chapter_id, stage=current_stage)

    async def _process_full_content(
        self, *, catalog, chapter: dict, content: str,
        classification: dict, primary_source_id: str, payload: dict,
        fallback_source_id: str = "",
        source_word_count: int = 0,
        primary_source_chapter_url: str = "",
        cross_source_selected: bool = False,
        manual_meta: dict[str, Any] | None = None,
    ) -> dict:
        """Path 1: Content is full. Normalize → write result. (AI 已抽离)"""
        from app.services.aggregate_alignment import build_source_alignment_json

        chapter_id = chapter["chapterId"]
        aggregate_book_id = chapter.get("aggregateBookId", "")
        title = chapter.get("title", "")
        chapter_index = chapter.get("chapterIndex")

        # Cross-source selection is the watermark defense: do not subsequently
        # delete prose by a phrase rule after that evidence-based choice.
        if cross_source_selected:
            selected_content = self._normalize_content_light(content)
            lexicon_result = {
                "hasMaskedWords": False,
                "maskedWordCount": 0,
                "maskedWordCandidates": [],
                "lexiconRestored": False,
            }
        elif self.purify_enabled(aggregate_book_id):
            selected_content, lexicon_result = self._purify_content_with_lexicon(
                content, source_id=primary_source_id
            )
        else:
            selected_content = self._normalize_content_light(content)
            lexicon_result = {
                "hasMaskedWords": False,
                "maskedWordCount": 0,
                "maskedWordCandidates": [],
                "lexiconRestored": False,
            }

        # 若本章有「作家说」，去掉正文末尾与之重叠的镜像附加（避免与评论系统重复）
        author_strip = await self._strip_embedded_author_say(
            catalog=catalog,
            chapter=chapter,
            content=selected_content,
            aggregate_book_id=aggregate_book_id,
            chapter_index=chapter_index,
        )
        selected_content = author_strip["content"]
        if author_strip.get("stripped"):
            lexicon_result = {
                **lexicon_result,
                "authorSayStripped": True,
                "authorSayRemovedChars": author_strip.get("removedChars", 0),
            }

        # 官方直出：仅记录偏差。第三方补全：按官方字数硬门禁，不达标拒绝写入。
        # 人工手动采纳时用户已自行确认内容，跳过硬门禁但仍记录偏差供审计。
        enforce_word_count = (
            bool(fallback_source_id)
            and int(source_word_count or 0) > 0
            and not manual_meta
        )
        word_count_result = self._validate_word_count(
            selected_content, source_word_count,
            enforce=enforce_word_count,
            aggregate_book_id=aggregate_book_id,
            chapter_index=chapter_index,
            source_id=fallback_source_id or primary_source_id,
        )
        if enforce_word_count and not word_count_result.get("passed"):
            raise ValueError(
                "word_count_mismatch "
                f"actual={word_count_result.get('actual')} "
                f"expected={word_count_result.get('expected')} "
                f"ratio={word_count_result.get('ratio')}"
            )

        # 写入结果
        status = "fallback" if fallback_source_id else "processed"
        selected_content_source = fallback_source_id or primary_source_id or "unknown"
        if not fallback_source_id and self._is_official_source(primary_source_id):
            selected_content_source = "official"
        elif not fallback_source_id:
            selected_content_source = "third_party"
        alignment_json = build_source_alignment_json(
            selected_content_source=selected_content_source,
            alignment_passed=True,
            alignment_reason="full_content_processed",
            primary_source_id=primary_source_id,
            candidate_source_id=fallback_source_id or "",
        )
        if isinstance(classification.get("alignmentJson"), dict) and classification.get("alignmentJson"):
            alignment_json = dict(classification["alignmentJson"])
        self._log_chapter_step(
            aggregate_book_id=aggregate_book_id,
            chapter_index=chapter_index,
            title=title,
            event="chapter_write_start",
            stage="stage2" if fallback_source_id else "stage1",
            payload={
                "step": "正在写入处理结果",
                "status": status,
                "sourceId": fallback_source_id or primary_source_id,
                "contentLength": len(selected_content or ""),
                "wordCountPassed": word_count_result.get("passed"),
                "wordCountRatio": word_count_result.get("ratio"),
                "hasMaskedWords": lexicon_result.get("hasMaskedWords"),
                "lexiconRestored": lexicon_result.get("lexiconRestored"),
            },
        )
        self._write_chapter_result(
            chapter_id=chapter_id, aggregate_book_id=aggregate_book_id,
            title=title, chapter_index=chapter_index,
            status=status, content=selected_content,
            alignment_json=alignment_json,
            fallback_source_id=fallback_source_id or primary_source_id,
            source_word_count=source_word_count,
            primary_source_chapter_url=primary_source_chapter_url,
            word_count_validation=word_count_result,
            lexicon_result=lexicon_result,
            manual_meta=manual_meta,
        )
        return {"chapterId": chapter_id, "success": True,
                "contentLength": len(selected_content),
                "fallback": bool(fallback_source_id)}

    def _apply_line_consensus_purification(
        self,
        *,
        selected_candidate: dict[str, Any],
        accepted_candidates: list[dict[str, Any]],
        consensus: list[dict[str, Any]],
        official_preview: str,
        official_word_count: int,
        primary_source_id: str,
        chapter_title: str,
        aggregate_book_id: str = "",
        chapter_index: int | None = None,
    ) -> dict[str, Any]:
        """Apply strict multi-source cleanup and roll it back if official gates regress."""
        from app.services.aggregate_alignment import align_candidate_chapter
        from app.services.aggregate_line_consensus import (
            purify_by_line_consensus,
            removed_segments_overlap_content,
        )

        selected_source_id = str(selected_candidate.get("source_id", "") or "")
        selected_consensus = next(
            (item for item in consensus if item.get("sourceId") == selected_source_id),
            {},
        )
        supporting_source_ids = set(selected_consensus.get("supportingSourceIds", []) or [])
        eligible_candidates = [
            candidate
            for candidate in accepted_candidates
            if not bool(
                ((candidate.get("alignment_json") or {}).get("sentenceOrder") or {}).get("rejected")
            )
        ]
        if len(eligible_candidates) <= 2:
            comparison_candidates = eligible_candidates
        else:
            comparison_candidates = [
                candidate
                for candidate in eligible_candidates
                if candidate.get("source_id") == selected_source_id
                or candidate.get("source_id") in supporting_source_ids
            ]

        original_content = str(selected_candidate.get("content", "") or "")
        cleaned_content, audit = purify_by_line_consensus(
            original_content,
            comparison_candidates,
            selected_source_id=selected_source_id,
            chapter_title=chapter_title,
        )
        alignment_json = dict(selected_candidate.get("alignment_json") or {})
        alignment_json["majorityDeletionAllowed"] = int(audit.get("sourceCount", 0) or 0) >= 3
        if cleaned_content == original_content:
            audit["revalidation"] = {
                "attempted": False,
                "passed": True,
                "reason": "not_required",
            }
            alignment_json["lineConsensus"] = audit
            selected_candidate["alignment_json"] = alignment_json
            return selected_candidate

        preview_gate = (
            align_candidate_chapter(
                official_preview=official_preview,
                candidate_title=chapter_title,
                candidate_content=cleaned_content,
                expected_title=chapter_title,
            )
            if official_preview
            else {"alignmentPassed": True, "alignmentReason": "not_available"}
        )
        word_gate = self._validate_word_count(
            cleaned_content,
            official_word_count,
            enforce=True,
            aggregate_book_id=aggregate_book_id,
            chapter_index=chapter_index,
            source_id=selected_source_id,
        )
        original_actual = len(
            original_content.replace("\n", "").replace(" ", "").replace("\r", "")
        )
        cleaned_actual = int(word_gate.get("actual", 0) or 0)
        word_count_not_worse = not official_word_count or (
            abs(cleaned_actual - official_word_count)
            <= abs(original_actual - official_word_count)
        )
        official_preview_overlap = removed_segments_overlap_content(audit, official_preview)
        official_anchor_trusted = self._is_official_source(primary_source_id)
        revalidation_passed = bool(preview_gate.get("alignmentPassed")) and bool(
            word_gate.get("passed")
        ) and word_count_not_worse and not official_preview_overlap and bool(
            official_anchor_trusted and official_preview and official_word_count
        )
        audit["revalidation"] = {
            "attempted": True,
            "passed": revalidation_passed,
            "preview": preview_gate,
            "wordCount": word_gate,
            "wordCountDistanceBefore": (
                abs(original_actual - official_word_count) if official_word_count else None
            ),
            "wordCountDistanceAfter": (
                abs(cleaned_actual - official_word_count) if official_word_count else None
            ),
            "wordCountNotWorse": word_count_not_worse,
            "officialPreviewOverlap": official_preview_overlap,
            "officialAnchorTrusted": official_anchor_trusted,
        }
        if not revalidation_passed:
            audit.update({
                "applied": False,
                "reason": "official_revalidation_failed",
                "rollback": True,
                "proposedRemovedCount": audit.get("removedCount", 0),
                "proposedRemovedLines": audit.get("removedLines", []),
                "removedCount": 0,
                "removedLines": [],
                "cleanedLength": len(original_content),
            })
        else:
            audit["rollback"] = False
            selected_candidate["content"] = cleaned_content
            alignment_json["candidateContentLength"] = len(cleaned_content)

        alignment_json["lineConsensus"] = audit
        selected_candidate["alignment_json"] = alignment_json
        return selected_candidate

    async def _try_candidate_content(
        self,
        catalog,
        chapter: dict,
        payload: dict,
        primary_source_id: str,
        *,
        official_word_count: int = 0,
        snapshot_only: bool = False,
    ) -> dict | None:
        """Try to get full content from candidate sources.

        For preview chapters, prefer title-fuzzy matching + preview alignment.
        For empty chapters, fall back to chapter-index/title matching.
        When official_word_count is known, candidate body must pass the same
        90%–120% word-count gate used after purify (补全后校对).
        """
        from app.services.aggregate_alignment import (
            align_candidate_chapter,
            build_source_alignment_json,
            chapter_title_similarity,
            classify_source_content,
        )

        if snapshot_only:
            return self._try_snapshot_candidate_content(
                chapter=chapter,
                payload=payload,
                primary_source_id=primary_source_id,
                official_word_count=official_word_count,
            )

        candidates = self._candidate_sources_from_payload(
            payload,
            primary_source_id,
            chapter.get("aggregateBookId", ""),
            include_expansion=True,
        )
        target_index = chapter.get("chapterIndex", 1)
        target_title = chapter.get("title", "")
        aggregate_book_id = chapter.get("aggregateBookId", "")
        official_snapshot = self._load_source_snapshot_content(
            aggregate_book_id=aggregate_book_id,
            chapter_index=target_index or 0,
            source_id=primary_source_id,
        )
        official_preview = str(official_snapshot or "").strip()
        try:
            official_word_count = int(official_word_count or 0)
        except (TypeError, ValueError):
            official_word_count = 0

        snapshot_candidate = self._try_snapshot_candidate_content(
            chapter=chapter,
            payload=payload,
            primary_source_id=primary_source_id,
            official_word_count=official_word_count,
        )
        if snapshot_candidate and snapshot_candidate.get("content"):
            return snapshot_candidate

        attempted_source_ids: list[str] = []
        accepted_candidates: list[dict[str, Any]] = []
        selected_candidate: dict[str, Any] | None = None
        consensus: list[dict[str, Any]] = []
        consensus_mode = ""
        expansion_started = False
        browser_is_default_fallback = not bool(
            self._book_workflow_settings(aggregate_book_id).get(
                "candidateSourcePriority", []
            )
        )

        for cand in candidates:
            cand_source_id = cand.get("sourceId", "")
            cand_book_id = cand.get("bookId", "")
            if not cand_source_id or not cand_book_id:
                continue
            if cand_source_id in attempted_source_ids:
                continue
            if (
                cand_source_id in self._browser_source_ids
                and accepted_candidates
                and browser_is_default_fallback
            ):
                # A validated HTTP candidate can use the documented degraded
                # consensus. Do not serialize a whole batch behind Browser.
                continue
            attempted_source_ids.append(cand_source_id)
            source_lag_info = self._candidate_source_lag_info(cand, target_title)
            if source_lag_info:
                self._log_chapter_step(
                    aggregate_book_id=aggregate_book_id,
                    chapter_index=target_index,
                    title=target_title,
                    event="candidate_source_behind_target",
                    stage="stage2",
                    payload={
                        "step": "候选源最新章节落后，跳过",
                        "sourceId": cand_source_id,
                        **source_lag_info,
                    },
                )
                continue
            toc_fetched = False

            def log_toc_fetch_start() -> None:
                nonlocal toc_fetched
                toc_fetched = True
                self._log_chapter_step(
                    aggregate_book_id=aggregate_book_id,
                    chapter_index=target_index,
                    title=target_title,
                    event="candidate_toc_start",
                    stage="stage2",
                    payload={
                        "step": "正在加载候选源目录",
                        "sourceId": cand_source_id,
                    },
                )

            try:
                cand_toc = await self._cached_toc(
                    catalog,
                    cand_book_id,
                    aggregate_book_id=aggregate_book_id,
                    source_id=cand_source_id,
                    on_fetch_start=log_toc_fetch_start,
                )
                cand_chapters = [c for c in cand_toc.get("chapters", []) if isinstance(c, dict)]
                if toc_fetched:
                    self._log_chapter_step(
                        aggregate_book_id=aggregate_book_id,
                        chapter_index=target_index,
                        title=target_title,
                        event="candidate_toc_complete",
                        stage="stage2",
                        payload={
                            "step": "候选源目录加载完成",
                            "sourceId": cand_source_id,
                            "chapterCount": len(cand_chapters),
                        },
                    )
            except Exception as exc:
                self._log_chapter_step(
                    aggregate_book_id=aggregate_book_id,
                    chapter_index=target_index,
                    title=target_title,
                    event="candidate_toc_error",
                    stage="stage2",
                    error_message=str(exc),
                    payload={
                        "step": "候选源目录加载失败",
                        "sourceId": cand_source_id,
                    },
                )
                continue

            lag_info = self._candidate_toc_lag_info(cand_chapters, target_title)
            if lag_info:
                self._log_chapter_step(
                    aggregate_book_id=aggregate_book_id,
                    chapter_index=target_index,
                    title=target_title,
                    event="candidate_toc_behind_target",
                    stage="stage2",
                    payload={
                        "step": "候选源章节落后，跳过",
                        "sourceId": cand_source_id,
                        **lag_info,
                    },
                )
                continue

            matched_candidates = self._match_candidate_toc_entries(
                cand_chapters=cand_chapters,
                target_index=target_index,
                target_title=target_title,
            )
            if not matched_candidates:
                self._log_chapter_step(
                    aggregate_book_id=aggregate_book_id,
                    chapter_index=target_index,
                    title=target_title,
                    event="candidate_no_match",
                    stage="stage2",
                    payload={
                        "step": "候选源没有匹配章节",
                        "sourceId": cand_source_id,
                    },
                )
                continue
            self._log_chapter_step(
                aggregate_book_id=aggregate_book_id,
                chapter_index=target_index,
                title=target_title,
                event="candidate_match_found",
                stage="stage2",
                payload={
                    "step": "候选源找到可能章节",
                    "sourceId": cand_source_id,
                    "matchedCount": len(matched_candidates),
                },
            )

            for matched_ch in matched_candidates:
                cand_chapter_id = matched_ch.get("chapterId", "")
                if not cand_chapter_id and matched_ch.get("chapterUrl"):
                    from app.source_plugins.id_codec import encode_chapter_id

                    cand_chapter_id = encode_chapter_id(cand_source_id, matched_ch["chapterUrl"])
                if not cand_chapter_id:
                    continue

                try:
                    cand_content = self._load_source_snapshot_content(
                        aggregate_book_id=aggregate_book_id,
                        chapter_index=target_index or 0,
                        source_id=cand_source_id,
                        expected_title=target_title,
                        source_chapter_id=cand_chapter_id,
                    )
                    cand_result: dict[str, Any] = {}
                    fetched_from_network = False
                    if cand_content and (official_preview or official_word_count > 0):
                        self._log_chapter_step(
                            aggregate_book_id=aggregate_book_id,
                            chapter_index=target_index,
                            title=target_title,
                            event="candidate_snapshot_used",
                            stage="stage2",
                            payload={
                                "step": "使用已保存的候选章节",
                                "sourceId": cand_source_id,
                                "contentLength": len(cand_content),
                            },
                        )
                    else:
                        self._log_chapter_step(
                            aggregate_book_id=aggregate_book_id,
                            chapter_index=target_index,
                            title=target_title,
                            event="candidate_fetch_start",
                            stage="stage2",
                            payload={
                                "step": "正在拉取候选章节",
                                "sourceId": cand_source_id,
                                "candidateTitle": matched_ch.get("title", "") or "",
                            },
                        )
                        async with self._source_slot(
                            aggregate_book_id=aggregate_book_id,
                            source_id=cand_source_id,
                        ):
                            cand_content = self._load_source_snapshot_content(
                                aggregate_book_id=aggregate_book_id,
                                chapter_index=target_index or 0,
                                source_id=cand_source_id,
                                expected_title=target_title,
                                source_chapter_id=cand_chapter_id,
                            )
                            if cand_content:
                                if not (official_preview or official_word_count > 0):
                                    continue
                            else:
                                cand_result = await catalog.chapter(cand_chapter_id)
                                cand_content = cand_result.get("content", "")
                                fetched_from_network = True
                    is_official = self._is_official_source(cand_source_id)
                    cls: dict[str, Any] = {}
                    for _attempt in range(2):
                        candidate_word_count = self._extract_source_word_count(cand_result)
                        candidate_preview_only = self._extract_preview_only(cand_result)
                        candidate_is_paid = self._extract_is_paid(cand_result)
                        # Snapshot persistence happens only after the candidate
                        # passes validation below: caching a rejected chapter at
                        # this position would poison every later candidate match.
                        # Prefer official baseline so short third-party bodies cannot
                        # pass as "full" merely because the mirror omits wordCount.
                        gate_word_count = (
                            official_word_count
                            if official_word_count > 0
                            else candidate_word_count
                        )
                        cls = classify_source_content(
                            cand_content,
                            source_id=cand_source_id,
                            is_official=is_official,
                            source_word_count=gate_word_count,
                            preview_only_hint=candidate_preview_only,
                            extra=cand_result.get("extra") if isinstance(cand_result.get("extra"), dict) else {},
                            is_paid=candidate_is_paid,
                        )
                        if cls["classification"] == "full":
                            break
                        if (
                            _attempt == 0
                            and not fetched_from_network
                            and not is_official
                            and cand_content
                        ):
                            # Snapshot hit but its content failed classification:
                            # the cached row is likely a truncated legacy entry.
                            # Treat the snapshot as stale and refetch once.
                            self._log_chapter_step(
                                aggregate_book_id=aggregate_book_id,
                                chapter_index=target_index,
                                title=target_title,
                                event="candidate_snapshot_stale",
                                stage="stage2",
                                payload={
                                    "step": "快照内容不完整，回源重新拉取",
                                    "sourceId": cand_source_id,
                                    "contentLength": len(cand_content),
                                    "classification": cls.get("classification", ""),
                                },
                            )
                            async with self._source_slot(
                                aggregate_book_id=aggregate_book_id,
                                source_id=cand_source_id,
                            ):
                                cand_result = await catalog.chapter(cand_chapter_id)
                                cand_content = cand_result.get("content", "")
                            fetched_from_network = True
                            continue
                        break
                    if cls["classification"] != "full":
                        self._log_chapter_step(
                            aggregate_book_id=aggregate_book_id,
                            chapter_index=target_index,
                            title=target_title,
                            event="candidate_rejected",
                            stage="stage2",
                            payload={
                                "step": "候选章节不是完整正文",
                                "sourceId": cand_source_id,
                                "classification": cls.get("classification", ""),
                                "contentLength": len(cand_content or ""),
                                "officialWordCount": official_word_count,
                                "gateWordCount": gate_word_count,
                            },
                        )
                        continue

                    if official_preview and len(cand_content or "") <= len(official_preview):
                        self._log_chapter_step(
                            aggregate_book_id=aggregate_book_id,
                            chapter_index=target_index,
                            title=target_title,
                            event="candidate_rejected",
                            stage="stage2",
                            payload={
                                "step": "候选章节长度未超过官方预览，无法补全",
                                "sourceId": cand_source_id,
                                "candidateContentLength": len(cand_content or ""),
                                "officialPreviewLength": len(official_preview),
                            },
                        )
                        continue

                    if official_word_count > 0:
                        word_gate = self._validate_word_count(
                            cand_content,
                            official_word_count,
                            enforce=True,
                            aggregate_book_id=aggregate_book_id,
                            chapter_index=target_index,
                            source_id=cand_source_id,
                        )
                        if not word_gate.get("passed"):
                            self._log_chapter_step(
                                aggregate_book_id=aggregate_book_id,
                                chapter_index=target_index,
                                title=target_title,
                                event="candidate_rejected",
                                stage="stage2",
                                payload={
                                    "step": "候选章节与官方字数不符",
                                    "sourceId": cand_source_id,
                                    "actual": word_gate.get("actual"),
                                    "expected": word_gate.get("expected"),
                                    "ratio": word_gate.get("ratio"),
                                    "lowerBound": word_gate.get("lowerBound"),
                                    "upperBound": word_gate.get("upperBound"),
                                },
                            )
                            continue

                    alignment_json = build_source_alignment_json(
                        selected_content_source="candidate",
                        official_content_length=len(official_preview),
                        candidate_content_length=len(cand_content or ""),
                        candidate_source_id=cand_source_id,
                        primary_source_id=primary_source_id,
                    )
                    if official_preview:
                        aligned = align_candidate_chapter(
                            official_preview=official_preview,
                            candidate_title=matched_ch.get("title", "") or target_title,
                            candidate_content=cand_content,
                            expected_title=target_title,
                        )
                        alignment_json = build_source_alignment_json(
                            selected_content_source="candidate",
                            official_content_length=len(official_preview),
                            candidate_content_length=len(cand_content or ""),
                            title_similarity=aligned.get("titleSimilarity", 0.0),
                            preview_similarity=aligned.get("previewSimilarity", 0.0),
                            head_preview_similarity=aligned.get("headPreviewSimilarity", 0.0),
                            alignment_passed=bool(aligned.get("alignmentPassed")),
                            alignment_reason=aligned.get("alignmentReason", ""),
                            candidate_source_id=cand_source_id,
                            primary_source_id=primary_source_id,
                        )
                        if not aligned.get("alignmentPassed"):
                            self._log_chapter_step(
                                aggregate_book_id=aggregate_book_id,
                                chapter_index=target_index,
                                title=target_title,
                                event="candidate_rejected",
                                stage="stage2",
                                payload={
                                    "step": "候选章节校验未通过",
                                    "sourceId": cand_source_id,
                                    "reason": aligned.get("alignmentReason", ""),
                                    "titleSimilarity": aligned.get("titleSimilarity", 0.0),
                                    "previewSimilarity": aligned.get("previewSimilarity", 0.0),
                                },
                            )
                            continue

                    accepted_candidates.append({
                        "content": cand_content,
                        "source_id": cand_source_id,
                        "alignment_json": alignment_json,
                    })
                    if fetched_from_network:
                        self._save_source_snapshot(
                            aggregate_book_id=aggregate_book_id,
                            chapter_index=target_index or 0,
                            source_id=cand_source_id,
                            source_book_id=cand.get("bookId", ""),
                            source_chapter_id=cand_chapter_id,
                            title=matched_ch.get("title", "") or target_title,
                            raw_content=cand_content,
                            source_chapter_url=self._extract_source_chapter_url(cand_result, cand_chapter_id),
                            classification=str(cls.get("classification", "") or "unknown"),
                        )
                        self._upsert_snapshot_run(
                            aggregate_book_id=aggregate_book_id,
                            source_id=cand_source_id,
                            source_book_id=str(cand.get("bookId", "") or ""),
                            status="partial",
                            total_chapters=len(cand_chapters),
                            fetched_chapters=self._source_snapshot_count(aggregate_book_id, cand_source_id),
                            failed_chapters=0,
                            last_error="",
                        )
                    self._log_chapter_step(
                        aggregate_book_id=aggregate_book_id,
                        chapter_index=target_index,
                        title=target_title,
                        event="candidate_validated",
                        stage="stage2",
                        payload={
                            "step": "候选章节通过单源校验，等待交叉比对",
                            "sourceId": cand_source_id,
                            "contentLength": len(cand_content or ""),
                        },
                    )
                    if len(accepted_candidates) >= CROSS_SOURCE_INITIAL_COMPARE_COUNT:
                        current_selection, current_consensus = self._select_consistent_candidate(
                            accepted_candidates
                        )
                        if len(accepted_candidates) == CROSS_SOURCE_INITIAL_COMPARE_COUNT:
                            stable = self._has_stable_candidate_consensus(
                                current_consensus,
                                candidate_count=len(accepted_candidates),
                                require_unanimous=True,
                            )
                            if not stable:
                                expansion_started = True
                        else:
                            stable = self._has_stable_candidate_consensus(
                                current_consensus,
                                candidate_count=len(accepted_candidates),
                                require_unanimous=False,
                            )
                        if current_selection and stable:
                            selected_candidate = current_selection
                            consensus = current_consensus
                            consensus_mode = (
                                "expanded"
                                if expansion_started
                                else "three_source"
                            )
                    break
                except Exception as exc:
                    self._log_chapter_step(
                        aggregate_book_id=aggregate_book_id,
                        chapter_index=target_index,
                        title=target_title,
                        event="candidate_fetch_error",
                        stage="stage2",
                        error_message=str(exc),
                        payload={
                            "step": "候选章节拉取失败",
                            "sourceId": cand_source_id,
                        },
                    )
                    continue

            if selected_candidate:
                break

        if not selected_candidate:
            selected_candidate, consensus = self._select_consistent_candidate(
                accepted_candidates,
                allow_degraded=len(accepted_candidates) in (1, 2),
            )
            if len(accepted_candidates) == 1 and selected_candidate:
                consensus_mode = "single_source"
            elif len(accepted_candidates) == 2 and selected_candidate:
                consensus_mode = "two_source"
            elif len(accepted_candidates) >= CROSS_SOURCE_INITIAL_COMPARE_COUNT:
                stable = self._has_stable_candidate_consensus(
                    consensus,
                    candidate_count=len(accepted_candidates),
                    require_unanimous=not expansion_started,
                )
                if stable:
                    consensus_mode = "expanded" if expansion_started else "three_source"
                else:
                    selected_candidate = None
        if selected_candidate:
            alignment_json = dict(selected_candidate["alignment_json"])
            alignment_json["crossSourceConsensus"] = consensus
            alignment_json.update({
                "crossSourceConsensusMode": consensus_mode,
                "crossSourceRequestedCount": CROSS_SOURCE_INITIAL_COMPARE_COUNT,
                "crossSourceAcceptedCount": len(accepted_candidates),
                "crossSourceAttemptedCount": len(attempted_source_ids),
                "crossSourceExpanded": expansion_started,
                "majorityDeletionAllowed": len(accepted_candidates) >= 3,
            })
            selected_candidate["alignment_json"] = alignment_json
            selected_candidate = self._apply_line_consensus_purification(
                selected_candidate=selected_candidate,
                accepted_candidates=accepted_candidates,
                consensus=consensus,
                official_preview=official_preview,
                official_word_count=official_word_count,
                primary_source_id=primary_source_id,
                chapter_title=target_title,
                aggregate_book_id=aggregate_book_id,
                chapter_index=target_index,
            )
            self._log_chapter_step(
                aggregate_book_id=aggregate_book_id,
                chapter_index=target_index,
                title=target_title,
                event="candidate_accepted",
                stage="stage2",
                payload={
                    "step": (
                        "候选章节按可用源降级选用"
                        if consensus_mode in {"single_source", "two_source"}
                        else "候选章节通过交叉比对"
                    ),
                    "sourceId": selected_candidate["source_id"],
                    "contentLength": len(selected_candidate["content"] or ""),
                    "consensusMode": consensus_mode,
                    "consensus": consensus,
                },
            )
            return selected_candidate
        if accepted_candidates:
            self._log_chapter_step(
                aggregate_book_id=aggregate_book_id,
                chapter_index=target_index,
                title=target_title,
                event="candidate_consensus_failed",
                stage="stage2",
                payload={
                    "step": "候选正文未形成稳定多数，拒绝发布",
                    "sourceIds": [item["source_id"] for item in accepted_candidates],
                    "consensus": consensus,
                },
            )
        if attempted_source_ids:
            self._log_chapter_step(
                aggregate_book_id=aggregate_book_id,
                chapter_index=target_index,
                title=target_title,
                event="candidate_all_failed",
                stage="stage2",
                payload={
                    "step": "所有候选源都未补全",
                    "attemptedSourceIds": attempted_source_ids,
                },
            )
            return {
                "all_sources_failed": True,
                "attempted_source_ids": attempted_source_ids,
            }
        self._log_chapter_step(
            aggregate_book_id=aggregate_book_id,
            chapter_index=target_index,
            title=target_title,
            event="candidate_none",
            stage="stage2",
            payload={"step": "没有可用候选源"},
        )
        return None

    # ── 手动选源补全（人工兜底）─────────────────────────────────────────────

    MANUAL_SCAN_ENTRIES_PER_SOURCE = 2
    MANUAL_PREVIEW_HEAD_CHARS = 120
    MANUAL_PREVIEW_TAIL_CHARS = 120

    def _source_display_name(self, source_id: str, aggregate_book_id: str = "") -> str:
        """Resolve a candidate source id to its human-readable plugin name."""
        if aggregate_book_id:
            with self._conn() as conn:
                row = conn.execute(
                    """
                    SELECT source_name FROM aggregate_book_sources
                    WHERE aggregate_book_id = ? AND source_id = ?
                      AND COALESCE(source_name, '') != ''
                    LIMIT 1
                    """,
                    (aggregate_book_id, source_id),
                ).fetchone()
            if row and row[0]:
                return str(row[0])
        try:
            from app.source_plugins.scheduler import get_plugin_scheduler

            plugin = get_plugin_scheduler()._plugins.get(source_id)
            if plugin is not None:
                return str(getattr(plugin.metadata, "name", "") or source_id)
        except Exception:
            pass
        return source_id

    @staticmethod
    def _resolve_manual_chapter_id(cand_source_id: str, matched_ch: dict[str, Any]) -> str:
        chapter_id = str(matched_ch.get("chapterId", "") or "")
        if not chapter_id and matched_ch.get("chapterUrl"):
            chapter_id = encode_chapter_id(cand_source_id, str(matched_ch["chapterUrl"]))
        return chapter_id

    async def scan_manual_candidates(self, catalog, chapter: dict[str, Any]) -> dict[str, Any]:
        """只读扫描该章在各候选源上的匹配章节，返回内容预览与自动校验数据。

        与自动补全共用同一套发现/匹配/分类/对齐原语，但不应用任何拒绝门、
        不写库——被自动流程拒绝的候选（如字数偏差 5%）同样返回，供人工判断。
        """
        from app.services.aggregate_alignment import (
            align_candidate_chapter,
            classify_source_content,
        )

        aggregate_book_id = str(chapter.get("aggregateBookId", "") or "")
        target_index = chapter.get("chapterIndex", 0)
        target_title = str(chapter.get("title", "") or "")
        try:
            official_word_count = int(chapter.get("sourceWordCount", 0) or 0)
        except (TypeError, ValueError):
            official_word_count = 0

        payload = self._load_aggregate_payload(aggregate_book_id)
        primary_source_id = str(payload.get("primarySourceId", "") or "")
        payload = await self._ensure_candidate_sources_for_book(aggregate_book_id, payload)
        candidates = self._candidate_sources_from_payload(
            payload, primary_source_id, aggregate_book_id, include_expansion=True,
        )
        official_preview = str(
            self._load_source_snapshot_content(
                aggregate_book_id=aggregate_book_id,
                chapter_index=target_index or 0,
                source_id=primary_source_id,
            ) or ""
        ).strip()

        self._log_chapter_step(
            aggregate_book_id=aggregate_book_id,
            chapter_index=target_index,
            title=target_title,
            event="manual_scan_start",
            stage="stage2",
            payload={"step": "手动选源：正在扫描候选源"},
        )

        items: list[dict[str, Any]] = []
        source_errors: list[dict[str, str]] = []
        seen_sources: set[str] = set()
        for cand in candidates:
            cand_source_id = str(cand.get("sourceId", "") or "")
            cand_book_id = str(cand.get("bookId", "") or "")
            if not cand_source_id or not cand_book_id or cand_source_id in seen_sources:
                continue
            seen_sources.add(cand_source_id)
            source_name = self._source_display_name(cand_source_id, aggregate_book_id)
            try:
                cand_toc = await self._cached_toc(
                    catalog,
                    cand_book_id,
                    aggregate_book_id=aggregate_book_id,
                    source_id=cand_source_id,
                )
                cand_chapters = [c for c in cand_toc.get("chapters", []) if isinstance(c, dict)]
            except Exception as exc:
                source_errors.append({"sourceId": cand_source_id, "sourceName": source_name,
                                      "error": str(exc) or "目录加载失败"})
                continue

            matched = self._match_candidate_toc_entries(
                cand_chapters=cand_chapters,
                target_index=target_index,
                target_title=target_title,
            )[: self.MANUAL_SCAN_ENTRIES_PER_SOURCE]
            for matched_ch in matched:
                cand_chapter_id = self._resolve_manual_chapter_id(cand_source_id, matched_ch)
                if not cand_chapter_id:
                    continue
                candidate_title = str(matched_ch.get("title", "") or target_title)
                item: dict[str, Any] = {
                    "sourceId": cand_source_id,
                    "sourceName": source_name,
                    "sourceChapterId": cand_chapter_id,
                    "candidateTitle": candidate_title,
                    "officialWordCount": official_word_count,
                    "contentLength": 0,
                    "classification": "empty",
                    "wordCount": {"passed": False, "actual": 0, "expected": official_word_count},
                    "alignment": {"passed": False, "titleSimilarity": 0.0,
                                  "previewSimilarity": 0.0, "headPreviewSimilarity": 0.0,
                                  "reason": ""},
                    "autoAcceptable": False,
                    "head": "",
                    "tail": "",
                    "error": "",
                }
                try:
                    async with self._source_slot(
                        aggregate_book_id=aggregate_book_id,
                        source_id=cand_source_id,
                    ):
                        cand_result = await catalog.chapter(cand_chapter_id)
                    cand_content = str(cand_result.get("content", "") or "")
                except Exception as exc:
                    item["error"] = str(exc) or "正文拉取失败"
                    items.append(item)
                    continue

                item["contentLength"] = len(cand_content)
                is_official = self._is_official_source(cand_source_id)
                candidate_word_count = self._extract_source_word_count(cand_result)
                gate_word_count = official_word_count if official_word_count > 0 else candidate_word_count
                cls = classify_source_content(
                    cand_content,
                    source_id=cand_source_id,
                    is_official=is_official,
                    source_word_count=gate_word_count,
                    preview_only_hint=self._extract_preview_only(cand_result),
                    extra=cand_result.get("extra") if isinstance(cand_result.get("extra"), dict) else {},
                    is_paid=self._extract_is_paid(cand_result),
                )
                item["classification"] = str(cls.get("classification", "") or "unknown")

                word_gate = {"passed": True, "actual": len(cand_content),
                             "expected": official_word_count, "ratio": None}
                if official_word_count > 0:
                    word_gate = self._validate_word_count(
                        cand_content, official_word_count, enforce=True,
                        aggregate_book_id=aggregate_book_id,
                        chapter_index=target_index, source_id=cand_source_id,
                    )
                item["wordCount"] = {
                    "passed": bool(word_gate.get("passed")),
                    "actual": word_gate.get("actual", len(cand_content)),
                    "expected": official_word_count,
                    "ratio": word_gate.get("ratio"),
                }

                aligned = {"alignmentPassed": False, "titleSimilarity": 0.0,
                           "previewSimilarity": 0.0, "headPreviewSimilarity": 0.0,
                           "alignmentReason": "no_preview_available"}
                if official_preview:
                    aligned = align_candidate_chapter(
                        official_preview=official_preview,
                        candidate_title=candidate_title,
                        candidate_content=cand_content,
                        expected_title=target_title,
                    )
                item["alignment"] = {
                    "passed": bool(aligned.get("alignmentPassed")),
                    "titleSimilarity": aligned.get("titleSimilarity", 0.0),
                    "previewSimilarity": aligned.get("previewSimilarity", 0.0),
                    "headPreviewSimilarity": aligned.get("headPreviewSimilarity", 0.0),
                    "reason": aligned.get("alignmentReason", ""),
                }

                longer_than_preview = (
                    not official_preview or len(cand_content) > len(official_preview)
                )
                item["autoAcceptable"] = bool(
                    item["classification"] == "full"
                    and item["wordCount"]["passed"]
                    and longer_than_preview
                    and (not official_preview or item["alignment"]["passed"])
                )
                if cand_content:
                    item["head"] = cand_content[: self.MANUAL_PREVIEW_HEAD_CHARS]
                    tail_keep = self.MANUAL_PREVIEW_TAIL_CHARS
                    item["tail"] = (
                        cand_content[-tail_keep:] if len(cand_content) > tail_keep * 2 else ""
                    )
                items.append(item)

        self._log_chapter_step(
            aggregate_book_id=aggregate_book_id,
            chapter_index=target_index,
            title=target_title,
            event="manual_scan_complete",
            stage="stage2",
            payload={"step": "手动选源扫描完成",
                     "candidateCount": len(items), "sourceErrorCount": len(source_errors)},
        )
        return {
            "targetTitle": target_title,
            "chapterIndex": target_index,
            "officialPreviewLength": len(official_preview),
            "officialWordCount": official_word_count,
            "items": items,
            "sourceErrors": source_errors,
        }

    async def apply_manual_candidate(
        self,
        catalog,
        chapter: dict[str, Any],
        source_id: str,
        source_chapter_id: str,
    ) -> dict[str, Any]:
        """人工采纳指定源的指定章节：跳过自动校验门，但净化/快照/审计不跳。"""
        from app.services.aggregate_alignment import (
            align_candidate_chapter,
            build_source_alignment_json,
            classify_source_content,
        )

        source_id = str(source_id or "")
        source_chapter_id = str(source_chapter_id or "")
        if not source_id or not source_chapter_id:
            raise ValueError("manual_source_or_chapter_missing")

        aggregate_book_id = str(chapter.get("aggregateBookId", "") or "")
        target_index = chapter.get("chapterIndex", 0)
        target_title = str(chapter.get("title", "") or "")
        try:
            official_word_count = int(chapter.get("sourceWordCount", 0) or 0)
        except (TypeError, ValueError):
            official_word_count = 0

        payload = self._load_aggregate_payload(aggregate_book_id)
        primary_source_id = str(payload.get("primarySourceId", "") or "")
        payload = await self._ensure_candidate_sources_for_book(aggregate_book_id, payload)
        candidates = self._candidate_sources_from_payload(
            payload, primary_source_id, aggregate_book_id, include_expansion=True,
        )
        cand_entry = next(
            (c for c in candidates if str(c.get("sourceId", "") or "") == source_id),
            None,
        )
        if cand_entry is None:
            raise ValueError("manual_source_not_available")
        cand_book_id = str(cand_entry.get("bookId", "") or "")

        official_preview = str(
            self._load_source_snapshot_content(
                aggregate_book_id=aggregate_book_id,
                chapter_index=target_index or 0,
                source_id=primary_source_id,
            ) or ""
        ).strip()

        cand_toc = await self._cached_toc(
            catalog,
            cand_book_id,
            aggregate_book_id=aggregate_book_id,
            source_id=source_id,
        )
        cand_chapters = [c for c in cand_toc.get("chapters", []) if isinstance(c, dict)]
        matched_chapter: dict[str, Any] | None = None
        for matched in self._match_candidate_toc_entries(
            cand_chapters=cand_chapters,
            target_index=target_index,
            target_title=target_title,
        ):
            if self._resolve_manual_chapter_id(source_id, matched) == source_chapter_id:
                matched_chapter = matched
                break
        if matched_chapter is None:
            # 安全闸：只允许采纳该章实际匹配到的候选，杜绝任意章节注入。
            raise ValueError("manual_target_not_matched")
        candidate_title = str(matched_chapter.get("title", "") or target_title)

        self._log_chapter_step(
            aggregate_book_id=aggregate_book_id,
            chapter_index=target_index,
            title=target_title,
            event="manual_apply_start",
            stage="stage2",
            payload={"step": "手动选源：正在拉取并采纳",
                     "sourceId": source_id, "candidateTitle": candidate_title},
        )
        async with self._source_slot(
            aggregate_book_id=aggregate_book_id,
            source_id=source_id,
        ):
            cand_result = await catalog.chapter(source_chapter_id)
        cand_content = str(cand_result.get("content", "") or "")
        if not cand_content.strip():
            raise ValueError("manual_candidate_empty")

        is_official = self._is_official_source(source_id)
        candidate_word_count = self._extract_source_word_count(cand_result)
        gate_word_count = official_word_count if official_word_count > 0 else candidate_word_count
        cls = classify_source_content(
            cand_content,
            source_id=source_id,
            is_official=is_official,
            source_word_count=gate_word_count,
            preview_only_hint=self._extract_preview_only(cand_result),
            extra=cand_result.get("extra") if isinstance(cand_result.get("extra"), dict) else {},
            is_paid=self._extract_is_paid(cand_result),
        )
        word_gate = self._validate_word_count(
            cand_content, official_word_count,
            enforce=bool(official_word_count > 0),
            aggregate_book_id=aggregate_book_id,
            chapter_index=target_index, source_id=source_id,
        )
        aligned = {"alignmentPassed": False, "titleSimilarity": 0.0,
                   "previewSimilarity": 0.0, "headPreviewSimilarity": 0.0,
                   "alignmentReason": "no_preview_available"}
        if official_preview:
            aligned = align_candidate_chapter(
                official_preview=official_preview,
                candidate_title=candidate_title,
                candidate_content=cand_content,
                expected_title=target_title,
            )

        source_chapter_url = self._extract_source_chapter_url(cand_result, source_chapter_id)
        self._save_source_snapshot(
            aggregate_book_id=aggregate_book_id,
            chapter_index=target_index or 0,
            source_id=source_id,
            source_book_id=cand_book_id,
            source_chapter_id=source_chapter_id,
            title=candidate_title,
            raw_content=cand_content,
            source_chapter_url=source_chapter_url,
            classification="full",
        )

        alignment_json = build_source_alignment_json(
            selected_content_source="candidate",
            official_content_length=len(official_preview),
            candidate_content_length=len(cand_content),
            title_similarity=aligned.get("titleSimilarity", 0.0),
            preview_similarity=aligned.get("previewSimilarity", 0.0),
            head_preview_similarity=aligned.get("headPreviewSimilarity", 0.0),
            alignment_passed=bool(aligned.get("alignmentPassed")),
            alignment_reason=f"manual_override:{aligned.get('alignmentReason', '')}",
            candidate_source_id=source_id,
            primary_source_id=primary_source_id,
        )
        manual_meta = {
            "chosenAt": self._now(),
            "sourceId": source_id,
            "sourceName": self._source_display_name(source_id, aggregate_book_id),
            "sourceChapterId": source_chapter_id,
            "candidateTitle": candidate_title,
            "contentLength": len(cand_content),
            "officialWordCount": official_word_count,
            "classification": str(cls.get("classification", "") or ""),
            "wordCount": {
                "passed": bool(word_gate.get("passed")),
                "actual": word_gate.get("actual"),
                "expected": official_word_count,
                "ratio": word_gate.get("ratio"),
            },
            "alignment": {
                "passed": bool(aligned.get("alignmentPassed")),
                "titleSimilarity": aligned.get("titleSimilarity", 0.0),
                "previewSimilarity": aligned.get("previewSimilarity", 0.0),
                "headPreviewSimilarity": aligned.get("headPreviewSimilarity", 0.0),
                "reason": aligned.get("alignmentReason", ""),
            },
        }
        alignment_json["manualSupplement"] = manual_meta
        alignment_json["crossSourceConsensusMode"] = "manual"

        result = await self._process_full_content(
            catalog=catalog,
            chapter=chapter,
            content=cand_content,
            classification={"classification": "full", "alignmentJson": alignment_json},
            # 与自动候选路径一致：净化/词库按采纳源执行；真正的官方源 id
            # 已显式写入 alignmentJson.primarySourceId（下方 classification 注入
            # 会覆盖默认 alignment 构造）。
            primary_source_id=source_id,
            payload=payload,
            fallback_source_id=source_id,
            source_word_count=official_word_count,
            primary_source_chapter_url=source_chapter_url,
            manual_meta=manual_meta,
        )
        self._log_chapter_step(
            aggregate_book_id=aggregate_book_id,
            chapter_index=target_index,
            title=target_title,
            event="manual_apply_complete",
            stage="stage2",
            payload={"step": "手动选源采纳完成", "sourceId": source_id,
                     "contentLength": result.get("contentLength", len(cand_content))},
        )
        return {"ok": True, **result, "manual": manual_meta}

    def _try_snapshot_candidate_content(
        self,
        *,
        chapter: dict[str, Any],
        payload: dict[str, Any],
        primary_source_id: str,
        official_word_count: int,
    ) -> dict[str, Any] | None:
        """Select a publishable candidate exclusively from previously captured snapshots."""
        from app.services.aggregate_alignment import (
            align_candidate_chapter,
            build_source_alignment_json,
            classify_source_content,
        )
        aggregate_book_id = str(chapter.get("aggregateBookId", "") or "")
        chapter_index = int(chapter.get("chapterIndex", 0) or 0)
        target_title = str(chapter.get("title", "") or "")
        candidate_sources = self._candidate_sources_from_payload(
            payload,
            primary_source_id,
            aggregate_book_id,
            include_expansion=True,
        )
        allowed_source_ids = {
            str(item.get("sourceId", "") or "")
            for item in candidate_sources
            if str(item.get("sourceId", "") or "")
        }
        if not aggregate_book_id or not chapter_index or not allowed_source_ids:
            return None
        placeholders = ",".join("?" for _ in allowed_source_ids)
        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT source_id, title, clean_content
                FROM aggregate_source_snapshots
                WHERE aggregate_book_id = ? AND chapter_index = ?
                  AND source_id IN ({placeholders})
                ORDER BY source_id ASC
                """,
                (aggregate_book_id, chapter_index, *sorted(allowed_source_ids)),
            ).fetchall()
        official_preview = self._load_source_snapshot_content(
            aggregate_book_id=aggregate_book_id,
            chapter_index=chapter_index,
            source_id=primary_source_id,
        )
        if not official_preview and not official_word_count:
            # A stored third-party body is not self-authenticating. Without an
            # official body or word-count anchor it remains a snapshot only.
            return {"all_sources_failed": bool(rows), "attempted_source_ids": sorted(allowed_source_ids)}
        accepted: list[dict[str, Any]] = []
        for source_id, candidate_title, content in rows:
            candidate_content = str(content or "")
            classification = classify_source_content(
                candidate_content,
                source_id=str(source_id or ""),
                source_word_count=int(official_word_count or 0),
            )
            if classification.get("classification") != "full":
                continue
            if official_preview and len(candidate_content) <= len(official_preview):
                continue
            if official_word_count:
                word_gate = self._validate_word_count(
                    candidate_content,
                    official_word_count,
                    enforce=True,
                    aggregate_book_id=aggregate_book_id,
                    chapter_index=chapter_index,
                    source_id=str(source_id or ""),
                )
                if not word_gate.get("passed"):
                    continue
            aligned = (
                align_candidate_chapter(
                    official_preview=official_preview,
                    candidate_title=str(candidate_title or target_title),
                    candidate_content=candidate_content,
                    expected_title=target_title,
                )
                if official_preview
                else {"alignmentPassed": True, "alignmentReason": "no_official_preview"}
            )
            if not aligned.get("alignmentPassed"):
                continue
            accepted.append({
                "source_id": str(source_id or ""),
                "content": candidate_content,
                "alignment_json": build_source_alignment_json(
                    selected_content_source="candidate",
                    official_content_length=len(official_preview),
                    candidate_content_length=len(candidate_content),
                    title_similarity=float(aligned.get("titleSimilarity", 0.0) or 0.0),
                    preview_similarity=float(aligned.get("previewSimilarity", 0.0) or 0.0),
                    head_preview_similarity=float(aligned.get("headPreviewSimilarity", 0.0) or 0.0),
                    alignment_passed=True,
                    alignment_reason=str(aligned.get("alignmentReason", "") or ""),
                    candidate_source_id=str(source_id or ""),
                    primary_source_id=primary_source_id,
                ),
            })
        selected, consensus = self._select_consistent_candidate(
            accepted,
            allow_degraded=len(accepted) in (1, 2),
        )
        if selected is None:
            return {"all_sources_failed": bool(rows), "attempted_source_ids": sorted(allowed_source_ids)} if rows else None

        alignment_json = dict(selected["alignment_json"])
        alignment_json["crossSourceConsensus"] = consensus
        alignment_json["crossSourceConsensusMode"] = (
            "single_source" if len(accepted) == 1 else "two_source" if len(accepted) == 2 else "multi_source"
        )
        alignment_json.update({
            "crossSourceRequestedCount": CROSS_SOURCE_INITIAL_COMPARE_COUNT,
            "crossSourceAcceptedCount": len(accepted),
            "crossSourceAttemptedCount": len(allowed_source_ids),
            "crossSourceExpanded": len(allowed_source_ids) > CROSS_SOURCE_INITIAL_COMPARE_COUNT,
            "majorityDeletionAllowed": False,
        })
        selected["alignment_json"] = alignment_json
        return self._apply_line_consensus_purification(
            selected_candidate=selected,
            accepted_candidates=accepted,
            consensus=consensus,
            official_preview=official_preview,
            official_word_count=int(official_word_count or 0),
            primary_source_id=primary_source_id,
            chapter_title=target_title,
            aggregate_book_id=aggregate_book_id,
            chapter_index=chapter_index,
        )

    def _select_consistent_candidate(
        self,
        candidates: list[dict[str, Any]],
        *,
        allow_degraded: bool = False,
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        """Select the best supported candidate, optionally degrading to priority order."""
        from app.services.aggregate_alignment import (
            CROSS_SOURCE_CONTENT_SIMILARITY_THRESHOLD,
            analyze_sentence_order_consensus,
            cross_source_content_similarity,
        )

        consensus: list[dict[str, Any]] = []
        from app.services.aggregate_line_consensus import direct_consensus_gap_count

        sentence_order_audits = analyze_sentence_order_consensus(candidates)
        eligible_candidates: list[dict[str, Any]] = []
        for candidate in candidates:
            source_id = str(candidate.get("source_id", "") or "")
            sentence_order = sentence_order_audits.get(source_id, {})
            alignment_json = dict(candidate.get("alignment_json") or {})
            alignment_json["sentenceOrder"] = sentence_order
            candidate["alignment_json"] = alignment_json
            if not sentence_order.get("rejected"):
                eligible_candidates.append(candidate)

        ranked: list[tuple[int, int, float, int, dict[str, Any]]] = []
        for index, candidate in enumerate(candidates):
            source_id = str(candidate.get("source_id", "") or "")
            sentence_order = sentence_order_audits.get(source_id, {})
            if sentence_order.get("rejected"):
                consensus.append({
                    "sourceId": source_id,
                    "supportingSourceIds": [],
                    "supportCount": 0,
                    "averageSimilarity": 0.0,
                    "sentenceOrderStatus": "rejected_order_mismatch",
                    "sentenceOrder": sentence_order,
                })
                continue
            scores: list[float] = []
            peers: list[str] = []
            for peer in eligible_candidates:
                if peer is candidate:
                    continue
                score = cross_source_content_similarity(
                    candidate.get("content", ""), peer.get("content", "")
                )
                if score >= CROSS_SOURCE_CONTENT_SIMILARITY_THRESHOLD:
                    scores.append(score)
                    peers.append(str(peer.get("source_id", "")))
            consensus.append({
                "sourceId": candidate.get("source_id", ""),
                "supportingSourceIds": peers,
                "supportCount": len(peers),
                "averageSimilarity": round(sum(scores) / len(scores), 4) if scores else 0.0,
                "sentenceOrderStatus": sentence_order.get("status", "insufficient_evidence"),
            })
            if scores:
                direct_gap_count = direct_consensus_gap_count(
                    str(candidate.get("content", "") or ""),
                    eligible_candidates,
                    selected_source_id=str(candidate.get("source_id", "") or ""),
                )
                consensus[-1]["directConsensusGapCount"] = direct_gap_count
                ranked.append((
                    len(scores),
                    direct_gap_count,
                    sum(scores) / len(scores),
                    index,
                    candidate,
                ))
        if not ranked:
            if allow_degraded and len(eligible_candidates) in (1, 2):
                return eligible_candidates[0], consensus
            return None, consensus
        ranked.sort(key=lambda item: (-item[0], item[1], -item[2], item[3]))
        return ranked[0][4], consensus

    @staticmethod
    def _has_stable_candidate_consensus(
        consensus: list[dict[str, Any]],
        *,
        candidate_count: int,
        require_unanimous: bool,
    ) -> bool:
        """Require three-source unanimity first, then a strict majority after expansion."""
        active_consensus = [
            item for item in consensus
            if item.get("sentenceOrderStatus") != "rejected_order_mismatch"
        ]
        active_count = len(active_consensus)
        if active_count < 2:
            return False
        if require_unanimous:
            return all(
                int(item.get("supportCount", 0) or 0) == active_count - 1
                for item in active_consensus
            )
        return any(
            (int(item.get("supportCount", 0) or 0) + 1) >= CROSS_SOURCE_INITIAL_COMPARE_COUNT
            and (int(item.get("supportCount", 0) or 0) + 1) * 2 > active_count
            for item in active_consensus
        )

    def _match_candidate_toc_entries(
        self,
        *,
        cand_chapters: list[dict[str, Any]],
        target_index: int,
        target_title: str,
    ) -> list[dict[str, Any]]:
        title_matches: list[tuple[tuple[int, float, int], int, dict[str, Any]]] = []
        index_fallbacks: list[tuple[int, dict[str, Any]]] = []
        target_ordinal = self._chapter_ordinal_from_title(target_title)
        for position, ch in enumerate(cand_chapters, start=1):
            if not isinstance(ch, dict):
                continue
            ch_index = int(ch.get("index") or position)
            index_gap = abs(ch_index - int(target_index or 0))
            if target_index and index_gap > CANDIDATE_TITLE_MAX_INDEX_DRIFT:
                continue
            title = str(ch.get("title", "") or "")
            rank = self._candidate_title_match_rank(target_title, title)
            if rank is not None:
                title_matches.append((rank, index_gap, ch))
                continue
            candidate_ordinal = self._chapter_ordinal_from_title(title)
            if (
                ch_index
                and target_index
                and index_gap <= 2
                and (target_ordinal is None or candidate_ordinal is None)
            ):
                index_fallbacks.append((index_gap, ch))
        title_matches.sort(
            key=lambda item: (
                item[0][0],
                -item[0][1],
                item[1],
                item[0][2],
            )
        )
        if title_matches:
            return [item[2] for item in title_matches[:3]]
        index_fallbacks.sort(key=lambda item: item[0])
        return [item[1] for item in index_fallbacks[:3]]

    def _candidate_title_match_rank(
        self,
        target_title: str,
        candidate_title: str,
    ) -> tuple[int, float, int] | None:
        from app.services.aggregate_alignment import (
            chapter_name_similarity,
            chapter_title_similarity,
        )

        if not target_title or not candidate_title:
            return None
        similarity = chapter_title_similarity(target_title, candidate_title)
        target_ordinal = self._chapter_ordinal_from_title(target_title)
        candidate_ordinal = self._chapter_ordinal_from_title(candidate_title)
        if target_ordinal is not None and candidate_ordinal is not None:
            ordinal_gap = abs(target_ordinal - candidate_ordinal)
            # A mirror may number chapters across the whole book while the
            # official TOC restarts at each volume. The TOC index bounds this
            # name match before we use it as a snapshot or live candidate.
            name_similarity = chapter_name_similarity(target_title, candidate_title)
            if name_similarity >= 0.8:
                return (0, name_similarity, ordinal_gap)
            if similarity >= 0.75 and ordinal_gap <= 5:
                return (1, similarity, ordinal_gap)
            if ordinal_gap <= 2:
                return (2, similarity, ordinal_gap)
            return None
        if similarity >= 0.75:
            return (0, similarity, 0)
        return None

    def _candidate_source_lag_info(
        self,
        candidate: dict[str, Any],
        target_title: str,
    ) -> dict[str, int] | None:
        last_chapter = str(candidate.get("lastChapter", "") or "").strip()
        if not last_chapter:
            return None
        return self._candidate_toc_lag_info([{"title": last_chapter}], target_title)

    def _candidate_toc_lag_info(
        self,
        cand_chapters: list[dict[str, Any]],
        target_title: str,
    ) -> dict[str, int] | None:
        target_number = self._chapter_ordinal_from_title(target_title)
        if not target_number:
            return None
        candidate_numbers = [
            number
            for chapter in cand_chapters
            if isinstance(chapter, dict)
            for number in [self._chapter_ordinal_from_title(str(chapter.get("title", "") or ""))]
            if number
        ]
        if not candidate_numbers:
            return None
        latest_number = max(candidate_numbers)
        if latest_number >= target_number:
            return None
        return {
            "targetChapterNumber": target_number,
            "latestCandidateChapterNumber": latest_number,
        }

    def _chapter_ordinal_from_title(self, title: str) -> int | None:
        from app.services.aggregate_alignment import _parse_chinese_number

        match = re.search(r"第\s*([零〇一二两三四五六七八九十百千万\d]+)\s*[章节回卷篇部集]", str(title or ""))
        if not match:
            return None
        return _parse_chinese_number(match.group(1))

    def _handle_processing_error(
        self,
        exc: Exception,
        chapter: dict,
        source_chapter_id: str,
        *,
        stage: str = "stage1",
    ) -> dict:
        from app.services.aggregate_alignment import build_source_alignment_json

        now = self._now()
        chapter_id = chapter["chapterId"]
        aggregate_book_id = chapter.get("aggregateBookId", "")

        if stage == "stage2":
            error_code = str(classify_stage2_error(exc))
            retry_class = classify_stage2_retry(error_code)
        elif stage == "stage3":
            error_code = str(classify_stage3_error(exc))
            retry_class = classify_stage3_retry(error_code)
        else:
            error_code = classify_error(exc)
            retry_class = classify_stage1_retry(error_code)

        alignment_json = build_source_alignment_json(
            selected_content_source="none",
            alignment_passed=False,
            alignment_reason=error_code,
        )
        with self._conn() as conn:
            row = conn.execute(
                "SELECT retry_count FROM aggregate_chapter_tasks WHERE chapter_id = ?",
                (chapter_id,),
            ).fetchone()
            current_retry = (row[0] if row else 0) or 0

            if retry_class == SharedBookRetryClass.NO_RETRY:
                conn.execute(
                    """UPDATE aggregate_chapter_tasks
                       SET status = 'error', error = ?, last_error_code = ?,
                           source_alignment_json = ?, next_retry_time = NULL, updated_at = ?
                       WHERE chapter_id = ?""",
                    (str(exc), error_code, json.dumps(alignment_json, ensure_ascii=False), now, chapter_id),
                )
            elif max_retries_reached(current_retry):
                periodic_retry_time = (
                    self._now_dt()
                    + timedelta(minutes=self.check_interval_minutes(aggregate_book_id))
                ).isoformat()
                conn.execute(
                    """UPDATE aggregate_chapter_tasks
                       SET status = 'error', error = ?, last_error_code = ?,
                           source_alignment_json = ?, next_retry_time = ?, updated_at = ?
                       WHERE chapter_id = ?""",
                    (
                        str(exc),
                        error_code,
                        json.dumps(alignment_json, ensure_ascii=False),
                        periodic_retry_time,
                        now,
                        chapter_id,
                    ),
                )
            elif retry_class == SharedBookRetryClass.LONG_RETRY_SCAN:
                conn.execute(
                    """UPDATE aggregate_chapter_tasks
                       SET status = 'error', error = ?, last_error_code = ?,
                           source_alignment_json = ?, next_retry_time = NULL, updated_at = ?
                       WHERE chapter_id = ?""",
                    (str(exc), error_code, json.dumps(alignment_json, ensure_ascii=False), now, chapter_id),
                )
            else:
                next_time = compute_next_retry_time(current_retry)
                conn.execute(
                    """UPDATE aggregate_chapter_tasks
                       SET status = 'error', error = ?, last_error_code = ?,
                           retry_count = ?, next_retry_time = ?,
                           source_alignment_json = ?, updated_at = ?
                       WHERE chapter_id = ?""",
                    (str(exc), error_code, current_retry + 1, next_time,
                     json.dumps(alignment_json, ensure_ascii=False), now, chapter_id),
                )
            conn.commit()
        self._log_chapter_step(
            aggregate_book_id=aggregate_book_id,
            chapter_index=chapter.get("chapterIndex"),
            title=chapter.get("title", ""),
            event="chapter_error",
            stage=stage,
            error_code=error_code,
            error_message=str(exc),
            payload={
                "step": "章节处理失败",
                "sourceChapterId": source_chapter_id,
                "retryClass": str(retry_class),
                "retryCount": current_retry,
            },
        )
        return {"chapterId": chapter_id, "success": False, "error": str(exc), "errorCode": error_code}

    def _write_chapter_result(
        self, *, chapter_id: str, aggregate_book_id: str, title: str,
        chapter_index: int | None, status: str, content: str,
        alignment_json: dict, fallback_source_id: str = "",
        source_word_count: int = 0,
        primary_source_chapter_url: str = "",
        preview_only: bool = False,
        word_count_validation: dict[str, Any] | None = None,
        lexicon_result: dict[str, Any] | None = None,
        manual_meta: dict[str, Any] | None = None,
    ) -> None:
        if self._looks_like_garbled_text(content):
            raise ValueError("garbled chapter content")
        now = self._now()
        is_manual = bool(manual_meta)
        manual_meta_json = json.dumps(manual_meta or {}, ensure_ascii=False)
        trace_meta = self._build_trace_meta(
            aggregate_book_id=aggregate_book_id,
            chapter_index=chapter_index,
            title=title,
            alignment_json=alignment_json,
            fallback_source_id=fallback_source_id,
            content=content,
            source_word_count=source_word_count,
            primary_source_chapter_url=primary_source_chapter_url,
            preview_only=preview_only,
            status=status,
            word_count_validation=word_count_validation,
            lexicon_result=lexicon_result,
        )
        trace_hash = hashlib.sha256(
            json.dumps(trace_meta, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        snapshot_refs = self._source_snapshot_refs(
            aggregate_book_id=aggregate_book_id,
            chapter_index=int(chapter_index or 0),
        )
        with self._conn() as conn:
            conn.execute(
                """UPDATE aggregate_chapter_tasks
                   SET status = ?, content_length = ?, processed_content = '', last_processed_at = ?,
                       placeholder = 0,
                       error = '', last_error_code = '', retry_count = 0, next_retry_time = NULL,
                       trace_hash = ?, policy_snapshot_json = ?, policy_version = COALESCE(policy_version, 1),
                       source_snapshot_refs_json = ?,
                       source_alignment_json = ?, fallback_source_id = ?,
                       manual_supplement = ?, manual_supplement_json = ?,
                       ai_model = '', deviation_score = 0, ai_self_score = 0,
                       ai_prompt_tokens = 0, ai_completion_tokens = 0,
                       source_word_count = ?, primary_source_chapter_url = ?, preview_only = ?,
                       ai_total_tokens = 0, ai_latency_ms = 0,
                       updated_at = ?
                   WHERE chapter_id = ?""",
                (status, len(content), now,
                 trace_hash,
                 json.dumps(trace_meta, ensure_ascii=False),
                 json.dumps(snapshot_refs, ensure_ascii=False),
                 json.dumps(alignment_json, ensure_ascii=False),
                 fallback_source_id,
                 1 if is_manual else 0, manual_meta_json,
                 int(source_word_count or 0), primary_source_chapter_url or "", 1 if preview_only else 0,
                 now, chapter_id),
            )
            file_result = self._write_aggregate_chapter_file(
                conn, chapter_id=chapter_id, aggregate_book_id=aggregate_book_id,
                title=title, content=content, chapter_index=chapter_index,
                trace_meta=trace_meta,
            )
            if file_result and file_result.get("filePath"):
                conn.execute(
                    """
                    UPDATE aggregate_chapter_tasks
                    SET content_file_path = ?, updated_at = ?
                    WHERE chapter_id = ?
                    """,
                    (
                        file_result["filePath"],
                        now,
                        chapter_id,
                    ),
                )
            if status == "fallback" and preview_only:
                self._schedule_preview_retry(conn, chapter_id, now)
            else:
                conn.execute(
                    """
                    UPDATE aggregate_chapter_tasks
                    SET preview_retry_count = 0, next_retry_time = NULL, updated_at = ?
                    WHERE chapter_id = ?
                    """,
                    (now, chapter_id),
                )
            conn.commit()
        self._refresh_shared_book_state(aggregate_book_id)
        self._log_chapter_result(
            aggregate_book_id=aggregate_book_id,
            chapter_index=chapter_index,
            title=title,
            status=status,
            alignment_json=alignment_json,
            fallback_source_id=fallback_source_id,
            preview_only=preview_only,
        )

    def _source_snapshot_refs(self, *, aggregate_book_id: str, chapter_index: int) -> list[dict[str, Any]]:
        if not aggregate_book_id or chapter_index <= 0:
            return []
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT source_id, source_book_id, source_chapter_id, content_hash, classification, fetched_at
                FROM aggregate_source_snapshots
                WHERE aggregate_book_id = ? AND chapter_index = ?
                ORDER BY source_id ASC
                """,
                (aggregate_book_id, chapter_index),
            ).fetchall()
        return [
            {
                "sourceId": row[0],
                "sourceBookId": row[1],
                "sourceChapterId": row[2],
                "contentHash": row[3],
                "classification": row[4],
                "fetchedAt": row[5],
            }
            for row in rows
        ]

    def _log_chapter_result(
        self,
        *,
        aggregate_book_id: str,
        chapter_index: int | None,
        title: str,
        status: str,
        alignment_json: dict,
        fallback_source_id: str,
        preview_only: bool,
    ) -> None:
        contract = self._shared_book_storage_contract()
        if not contract.get("useSharedBookStorage"):
            return
        try:
            book_name = self._aggregate_book_name_from_cache(aggregate_book_id)
            book_author = self._aggregate_book_author_from_cache(aggregate_book_id)
            if not book_name:
                return
        except Exception:
            return

        event = "chapter_write"
        error_code = None
        error_message = None
        if status in {"error", "failed"}:
            event = "chapter_error"
            error_code = "S3_PROCESSING_FAILED"
            error_message = "chapter processing failed"

        stage = "stage1"
        if fallback_source_id:
            stage = "stage2"

        primary_source_id = ""
        if isinstance(alignment_json, dict):
            primary_source_id = alignment_json.get("primarySourceId") or alignment_json.get("selectedSource") or ""

        try:
            self._shared_book_process_logger().append(
                book_name=book_name,
                author=book_author,
                event=event,
                book_id=aggregate_book_id,
                chapter_index=chapter_index,
                stage=stage,
                error_code=error_code,
                error_message=error_message,
                payload={
                    "title": title,
                    "status": status,
                    "previewOnly": preview_only,
                    "primarySourceId": primary_source_id,
                    "supplementSourceId": fallback_source_id or None,
                    "aiModel": None,
                },
            )
        except Exception:
            logger.debug("Failed to write chapter process log", exc_info=True)

    def _aggregate_book_name_from_cache(self, aggregate_book_id: str) -> str:
        with self._conn() as conn:
            return self._aggregate_book_name(conn, aggregate_book_id)

    def _aggregate_book_author_from_cache(self, aggregate_book_id: str) -> str:
        with self._conn() as conn:
            return self._aggregate_book_author(conn, aggregate_book_id)

    def aggregate_chapter_response(self, chapter_url: str, chapter_id: str = "") -> dict:
        try:
            payload = unpack_aggregate_chapter_url(chapter_url)
        except Exception as exc:
            return {
                "implemented": True,
                "chapterId": chapter_id,
                "title": "",
                "content": PROCESSING_PLACEHOLDER,
                "debug": {"aggregate": True, "status": "pending", "error": str(exc)},
            }
        aggregate_chapter_id = chapter_id or encode_chapter_id(VIRTUAL_SOURCE_ID, chapter_url)
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT title, status, content_file_path, error
                FROM aggregate_chapter_tasks
                WHERE chapter_id = ?
                """,
                (aggregate_chapter_id,),
            ).fetchone()
        title = (row[0] if row else "") or payload.get("title", "")
        status = row[1] if row else "pending"
        if row and status in ("processed", "fallback") and row[2]:
            content = self._read_chapter_content_from_file(row[2])
            if content:
                return {
                    "implemented": True,
                    "chapterId": aggregate_chapter_id,
                    "title": title,
                    "content": content,
                    "debug": {"aggregate": True, "status": status},
                }
        return {
            "implemented": True,
            "chapterId": aggregate_chapter_id,
            "title": title,
            "content": PROCESSING_PLACEHOLDER,
            "debug": {
                "aggregate": True,
                "status": status,
                "sourceChapterId": payload.get("sourceChapterId", ""),
                "error": row[3] if row else "",
            },
        }

    def _strip_trace_block(self, content: str) -> str:
        if not content:
            return content
        cleaned = TRACE_BLOCK_RE.sub("", str(content))
        return cleaned.rstrip()

    def _looks_like_garbled_text(self, content: str) -> bool:
        return looks_like_garbled_text(content)

    def _read_chapter_content_from_file(self, content_file_path: str) -> str:
        """从 .md 文件读取章节正文，去除 trace block 和标题行。"""
        if not content_file_path:
            return ""
        try:
            path = Path(content_file_path)
            if not path.exists():
                return ""
            text = path.read_text(encoding="utf-8")
            cleaned = self._strip_trace_block(text).strip()
            if cleaned.startswith("# "):
                lines = cleaned.split("\n", 1)
                cleaned = lines[1] if len(lines) > 1 else ""
            if self._looks_like_garbled_text(cleaned):
                logger.warning("Skipped garbled local chapter file: %s", content_file_path)
                return ""
            return cleaned.strip()
        except Exception:
            logger.debug("Failed to read chapter file: %s", content_file_path, exc_info=True)
            return ""


    def _normalize_content_light(self, content: str) -> str:
        """Lightweight normalization without aggressive purification."""
        if not content:
            return content
        text = content.replace("\r\n", "\n").replace("\r", "\n")
        text = re.sub(r"[ \t]+\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _write_aggregate_chapter_file(
        self,
        conn: sqlite3.Connection,
        *,
        chapter_id: str,
        aggregate_book_id: str,
        title: str,
        content: str,
        chapter_index: int | None = None,
        chapter_url: str = "",
        trace_meta: dict | None = None,
    ) -> dict[str, Any] | None:
        """Persist a processed chapter to the shared library files."""
        contract = self._shared_book_storage_contract()
        if not contract.get("useSharedBookStorage"):
            return None
        book_name = self._aggregate_book_name(conn, aggregate_book_id)
        book_author = self._aggregate_book_author(conn, aggregate_book_id)
        storage = self._shared_book_storage()
        shared_path = storage.chapter_markdown_path(
            book_name=book_name,
            author=book_author,
            chapter_index=int(chapter_index or 0),
            title=title,
        )
        storage.write_chapter_file(
            path=shared_path,
            title=title,
            body=content,
            trace_payload=trace_meta or {},
        )
        conn.execute(
            """
            UPDATE aggregate_chapter_tasks
            SET content_file_path = ?, updated_at = ?
            WHERE chapter_id = ?
            """,
            (str(shared_path), self._now(), chapter_id),
        )
        metadata_payload = self._build_shared_metadata_payload(
            aggregate_book_id=aggregate_book_id,
            book_name=book_name,
            book_author=book_author,
            conn=conn,
        )
        storage.update_chapter_index_entry(
            chapter_index_path=storage.chapter_index_path(book_name=book_name, author=book_author),
            metadata_path=storage.metadata_path(book_name=book_name, author=book_author),
            metadata_payload=metadata_payload,
            entry=self._build_chapter_index_entry(
                chapter_index=int(chapter_index or 0),
                title=title,
                file=f"chapters/{shared_path.name}",
                trace_payload=trace_meta or {},
            ),
            chapter_trace=trace_meta or {},
        )
        return {"filePath": str(shared_path), "written": shared_path.exists()}

    def _shared_book_storage(self) -> SharedBookStorage:
        return SharedBookStorage(root=self.db_path.parent / "library")

    def _shared_book_process_logger(self) -> SharedBookProcessLogger:
        if self._process_logger_cache is None:
            self._process_logger_cache = SharedBookProcessLogger(
                storage=self._shared_book_storage()
            )
        return self._process_logger_cache

    def _build_chapter_index_entry(
        self,
        *,
        chapter_index: int,
        title: str,
        file: str | None,
        trace_payload: dict[str, Any],
    ) -> dict[str, Any]:
        alignment = trace_payload.get("alignment") if isinstance(trace_payload.get("alignment"), dict) else {}
        primary = trace_payload.get("primarySource") if isinstance(trace_payload.get("primarySource"), dict) else {}
        supplement = trace_payload.get("supplementSource") if isinstance(trace_payload.get("supplementSource"), dict) else {}
        source_id = str(
            trace_payload.get("fallbackSourceId")
            or supplement.get("sourceId", "")
            or trace_payload.get("selectedSource")
            or primary.get("sourceId", "")
            or trace_payload.get("primarySourceId", "")
            or ""
        )
        return {
            "index": int(chapter_index or 0),
            "title": title,
            "file": file,
            "status": trace_payload.get("chapterStatus", "") or "unknown",
            "isVip": bool(trace_payload.get("isVip", False)),
            "officialWordCount": int(trace_payload.get("officialWordCount", 0) or 0),
            "officialPreviewWords": trace_payload.get("officialPreviewWords"),
            "sourceId": source_id,
            "sourceChapterId": str(primary.get("chapterId", "") or ""),
            "alignedWith": trace_payload.get("alignedWith", "") or trace_payload.get("selectedContentSource", "") or "",
            "alignmentScore": alignment.get("previewSimilarity"),
        }

    def _shared_book_runtime_store(self) -> SharedBookRuntimeStore:
        return SharedBookRuntimeStore(storage=self._shared_book_storage())

    def _shared_book_identity(self, aggregate_book_id: str) -> tuple[str, str]:
        with self._conn() as conn:
            return (
                self._aggregate_book_name(conn, aggregate_book_id),
                self._aggregate_book_author(conn, aggregate_book_id),
            )

    def _load_stage3_deferred_state(self, aggregate_book_id: str):
        book_name, author = self._shared_book_identity(aggregate_book_id)
        if not book_name:
            return None, None, None
        store = self._shared_book_runtime_store()
        return store, book_name, store.load_state(book_name=book_name, author=author)

    def _stage3_deferred_attempt(self, aggregate_book_id: str, chapter_id: str) -> int:
        _, _, state = self._load_stage3_deferred_state(aggregate_book_id)
        if state is None:
            return 0
        for item in state.stage3Deferred:
            if item.chapterId == chapter_id:
                return int(item.attempt or 0)
        return 0

    def _mark_stage3_deferred(
        self,
        *,
        aggregate_book_id: str,
        chapter_id: str,
        reason: str,
        retry_not_before: str,
        attempt: int = 0,
    ) -> None:
        store, book_name, state = self._load_stage3_deferred_state(aggregate_book_id)
        if store is None or state is None or not book_name:
            return
        book_name, author = self._shared_book_identity(aggregate_book_id)
        items = [item for item in state.stage3Deferred if item.chapterId != chapter_id]
        items.append(
            SharedBookStage3DeferredItem(
                chapterId=chapter_id,
                reason=reason,
                retryNotBefore=retry_not_before,
                updatedAt=self._now(),
                attempt=max(0, int(attempt or 0)),
            )
        )
        state = state.model_copy(update={"updatedAt": self._now(), "stage3Deferred": items})
        store.save_state(book_name=book_name, author=author, state=state)

    def _clear_stage3_deferred(self, aggregate_book_id: str, chapter_id: str) -> None:
        store, book_name, state = self._load_stage3_deferred_state(aggregate_book_id)
        if store is None or state is None or not book_name:
            return
        book_name, author = self._shared_book_identity(aggregate_book_id)
        items = [item for item in state.stage3Deferred if item.chapterId != chapter_id]
        if len(items) == len(state.stage3Deferred):
            return
        state = state.model_copy(update={"updatedAt": self._now(), "stage3Deferred": items})
        store.save_state(book_name=book_name, author=author, state=state)

    def _due_stage3_deferred_chapter_ids(self, aggregate_book_id: str) -> list[str]:
        _, _, state = self._load_stage3_deferred_state(aggregate_book_id)
        if state is None:
            return []
        now = self._now_dt()
        due: list[str] = []
        for item in state.stage3Deferred:
            try:
                not_before = datetime.fromisoformat(item.retryNotBefore)
            except Exception:
                not_before = now
            if not_before <= now:
                due.append(item.chapterId)
        return due

    def _stage3_retry_not_before(self, aggregate_book_id: str, reason: str, attempt: int = 0) -> str:
        if reason in {"peak_hour_skip", "token_budget_exceeded", "failure_rate_threshold_exceeded"}:
            breaker_state = self.ai_circuit_breaker_state(aggregate_book_id)
            if breaker_state.get("openUntil"):
                return str(breaker_state["openUntil"])
            return (self._now_dt() + timedelta(minutes=self.check_interval_minutes(aggregate_book_id))).isoformat()
        if reason in {"content_candidate_untrusted", "long_retry_scan"}:
            return (self._now_dt() + timedelta(minutes=self.check_interval_minutes(aggregate_book_id))).isoformat()
        next_time = compute_next_retry_time(max(0, int(attempt or 0)))
        if next_time:
            return next_time
        return (self._now_dt() + timedelta(minutes=self.check_interval_minutes(aggregate_book_id))).isoformat()

    def _shared_stage1_status(
        self,
        *,
        status: str,
        preview_only: bool,
        trace_meta: dict[str, Any],
        ai_model: str = "",
        ai_total_tokens: int = 0,
    ) -> str:
        normalized = str(status or "").strip().lower()
        has_ai = (
            bool(ai_model)
            or int(ai_total_tokens or 0) > 0
            or bool(trace_meta.get("aiModel"))
            or bool((trace_meta.get("aiCheck") or {}).get("model"))
            or bool(trace_meta.get("stage3Attempted"))
        )
        if normalized == "processed":
            return "proofread_complete" if has_ai else "supplemented"
        if normalized == "fallback":
            if has_ai and not preview_only:
                return "suspect"
            return "fetched" if preview_only else "readable"
        if normalized == "error":
            return "failed"
        if normalized in {"pending", "placeholder"}:
            return "fetched" if preview_only else "processing"
        return normalized or "unknown"

    def _build_shared_trace_payload(
        self,
        *,
        conn: sqlite3.Connection,
        aggregate_book_id: str,
        chapter_row: dict[str, Any],
        trace_meta: dict[str, Any],
    ) -> dict[str, Any]:
        alignment = {}
        try:
            alignment = json.loads(chapter_row["source_alignment_json"] or "{}")
        except Exception:
            alignment = {}
        snapshot_refs = []
        try:
            payload = json.loads(chapter_row["source_snapshot_refs_json"] or "[]")
            if isinstance(payload, list):
                snapshot_refs = payload
        except Exception:
            snapshot_refs = []
        chapter_status = self._shared_stage1_status(
            status=chapter_row["status"] or "",
            preview_only=bool(chapter_row["preview_only"]),
            trace_meta=trace_meta,
            ai_model=chapter_row["ai_model"] or "",
            ai_total_tokens=int(chapter_row["ai_total_tokens"] or 0),
        )
        trace_primary = trace_meta.get("primarySource")
        primary_source_id = (
            alignment.get("primarySourceId")
            or trace_meta.get("primarySourceId")
            or ((trace_primary or {}).get("sourceId", "") if isinstance(trace_primary, dict) else trace_primary)
            or ""
        )
        source_word_count = int(chapter_row["source_word_count"] or trace_meta.get("sourceWordCount", 0) or 0)
        fetched_word_count = len(self._read_chapter_content_from_file(chapter_row["content_file_path"] or ""))
        ai_check = {
            "model": chapter_row["ai_model"] or trace_meta.get("aiModel", "") or "",
            "deviationScore": float(chapter_row["deviation_score"] or 0.0),
            "selfScore": float(chapter_row["ai_self_score"] or 0.0),
            "promptTokens": int(chapter_row["ai_prompt_tokens"] or 0),
            "completionTokens": int(chapter_row["ai_completion_tokens"] or 0),
            "totalTokens": int(chapter_row["ai_total_tokens"] or 0),
            "latencyMs": int(chapter_row["ai_latency_ms"] or 0),
        }
        trace_payload: dict[str, Any] = {
            "schemaVersion": 2,
            "aggregateBookId": aggregate_book_id,
            "chapterId": chapter_row["chapter_id"] or "",
            "chapterIndex": int(chapter_row["chapter_index"] or 0),
            "chapterTitle": chapter_row["title"] or "",
            "chapterStatus": chapter_status,
            "proofreadComplete": chapter_status == "proofread_complete",
            "previewOnly": bool(chapter_row["preview_only"]),
            "primarySource": {
                "sourceId": primary_source_id,
                "chapterId": chapter_row["source_chapter_id"] or "",
                "wordCount": source_word_count,
            },
            "primarySourceId": primary_source_id,
            "officialWordCount": source_word_count,
            "officialPreviewWords": source_word_count if bool(chapter_row["preview_only"]) else None,
            "isVip": bool(trace_meta.get("isVip", chapter_row["preview_only"])),
            "fetchedWordCount": fetched_word_count,
            "selectedContentSource": alignment.get("selectedContentSource", "") or trace_meta.get("selectedContentSource", ""),
            "processedAt": chapter_row["last_processed_at"] or trace_meta.get("processedAt", "") or "",
            "legacy": {
                "legacyStatus": chapter_row["status"] or "",
                "traceHash": chapter_row["trace_hash"] or "",
                "contentFilePath": chapter_row["content_file_path"] or "",
            },
        }
        if any(ai_check.values()):
            trace_payload["aiCheck"] = ai_check
            trace_payload["aiModel"] = ai_check["model"]
            trace_payload["aiTokens"] = ai_check["totalTokens"]
        if alignment:
            trace_payload["alignment"] = alignment
        supplement_source_id = chapter_row["fallback_source_id"] or alignment.get("candidateSourceId") or ""
        if supplement_source_id and supplement_source_id != primary_source_id:
            trace_payload["supplementSource"] = {
                "sourceId": supplement_source_id,
                "selected": True,
            }
        if snapshot_refs:
            trace_payload["sourceSnapshotRefs"] = snapshot_refs
        return trace_payload

    def _build_shared_metadata_payload(
        self,
        *,
        conn: sqlite3.Connection,
        aggregate_book_id: str,
        book_name: str,
        book_author: str,
    ) -> dict[str, Any]:
        row = conn.execute(
            """
            SELECT aggregate_payload_json, cover_url, intro, word_count, primary_book_id,
                   primary_source_id, primary_source_name, total_chapters_at_subscribe,
                   book_status, status, search_visibility_status, visible_processed_chapters,
                   last_check_time, total_chapters
            FROM aggregate_book_tasks
            WHERE aggregate_book_id = ?
            """,
            (aggregate_book_id,),
        ).fetchone()
        payload_json = row[0] if row else ""
        try:
            payload = json.loads(payload_json or "{}")
        except Exception:
            payload = {}
        chapter_stats = conn.execute(
            """
            SELECT status, preview_only, ai_model
            FROM aggregate_chapter_tasks
            WHERE aggregate_book_id = ?
            """,
            (aggregate_book_id,),
        ).fetchall()
        total_count = len(chapter_stats)
        processed_count = sum(1 for s, _, _ in chapter_stats if s in {"processed", "fallback"})
        failed_count = sum(1 for s, _, _ in chapter_stats if s == "error")
        preview_count = sum(
            1 for s, p, _ in chapter_stats
            if s in {"processed", "fallback"} and p
        )
        proofread_count = sum(
            1 for s, _, m in chapter_stats
            if s in {"processed", "fallback"} and bool(m)
        )
        snapshot_stats = conn.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT source_id)
            FROM aggregate_source_snapshots
            WHERE aggregate_book_id = ?
            """,
            (aggregate_book_id,),
        ).fetchone()
        snapshot_failed = conn.execute(
            """SELECT COALESCE(SUM(failed_chapters), 0)
               FROM aggregate_source_snapshot_runs
               WHERE aggregate_book_id = ?""",
            (aggregate_book_id,),
        ).fetchone()
        latest_index = 0
        latest_title = ""
        if chapter_stats:
            latest = conn.execute(
                """
                SELECT chapter_index, title
                FROM aggregate_chapter_tasks
                WHERE aggregate_book_id = ?
                  AND status IN ('processed', 'fallback')
                ORDER BY chapter_index DESC
                LIMIT 1
                """,
                (aggregate_book_id,),
            ).fetchone()
            if latest:
                latest_index = int(latest[0] or 0)
                latest_title = str(latest[1] or "")
        payload = {
            "candidateId": aggregate_book_id,
            "name": payload.get("name") or book_name,
            "author": payload.get("author") or book_author,
            "coverUrl": (row[1] if row else "") or payload.get("coverUrl") or "",
            "intro": (row[2] if row else "") or payload.get("intro") or "",
            "bookStatus": (row[8] if row else "") or payload.get("bookStatus") or "",
            "wordCount": (row[3] if row else "") or payload.get("wordCount") or "",
            "totalChaptersAtSubscribe": int((row[7] if row else 0) or payload.get("totalChaptersAtSubscribe", 0) or 0),
            "primaryBookId": (row[4] if row else "") or payload.get("primaryBookId") or "",
            "primarySourceId": (row[5] if row else "") or payload.get("primarySourceId") or "",
            "primarySourceName": (row[6] if row else "") or payload.get("primarySourceName") or "",
            "sources": payload.get("sources") or [],
            "bookState": {
                "status": (row[9] if row else "") or "active",
                "searchVisibilityStatus": (row[10] if row else "") or "hidden",
                "chapterCount": total_count,
                "processedChapterCount": processed_count,
                "readableChapterCount": max(0, processed_count - preview_count),
                "previewChapterCount": preview_count,
                "proofreadCompleteCount": proofread_count,
                "suspectChapterCount": 0,
                "failedChapterCount": failed_count,
                "sourceSnapshotChapterCount": int((snapshot_stats or [0])[0] or 0),
                "sourceSnapshotSourceCount": int((snapshot_stats or [0, 0])[1] or 0),
                "sourceSnapshotFailedCount": int((snapshot_failed or [0])[0] or 0),
                "latestChapterIndex": latest_index,
                "latestChapterTitle": latest_title,
                "lastUpdateCheckAt": (row[12] if row else "") or "",
            },
        }
        metadata = self._shared_book_storage().build_shared_metadata(payload)
        storage = self._shared_book_storage()
        existing_path = storage.metadata_path(book_name=book_name, author=book_author)
        existing_metadata: dict[str, Any] = {}
        if existing_path.exists():
            try:
                existing_payload = json.loads(existing_path.read_text(encoding="utf-8"))
                if isinstance(existing_payload, dict):
                    existing_metadata = existing_payload
            except Exception:
                existing_metadata = {}

        source_map = existing_metadata.get("sourceMap") if isinstance(existing_metadata.get("sourceMap"), dict) else {}
        if source_map:
            metadata["sourceMap"] = source_map
            if isinstance(source_map.get("summary"), list):
                metadata["sourceMapSummary"] = source_map["summary"]
        elif isinstance(existing_metadata.get("sourceMapSummary"), list):
            metadata["sourceMapSummary"] = existing_metadata["sourceMapSummary"]
        return metadata

    def _write_shared_book_stage1_bundle(
        self,
        conn: sqlite3.Connection,
        *,
        aggregate_book_id: str,
        title: str,
        chapter_index: int | None,
        content: str,
        trace_meta: dict[str, Any],
    ) -> None:
        contract = self._shared_book_storage_contract()
        if not contract.get("useSharedBookStorage"):
            return
        if not aggregate_book_id or not chapter_index:
            return

        book_name = self._aggregate_book_name(conn, aggregate_book_id)
        book_author = self._aggregate_book_author(conn, aggregate_book_id)
        if not book_name:
            return
        storage = self._shared_book_storage()

        chapter_rows = conn.execute(
            """
            SELECT chapter_id, source_chapter_id, chapter_index, title, status, source_word_count,
                   preview_only, primary_source_chapter_url, content_file_path,
                   last_processed_at, source_snapshot_refs_json, fallback_source_id, source_alignment_json,
                   trace_hash, ai_model, ai_prompt_tokens, ai_completion_tokens, ai_total_tokens,
                   ai_latency_ms, deviation_score, ai_self_score
            FROM aggregate_chapter_tasks
            WHERE aggregate_book_id = ?
            ORDER BY chapter_index ASC, created_at ASC
            """,
            (aggregate_book_id,),
        ).fetchall()
        chapter_files: list[tuple[Path, str]] = []
        chapter_entries: list[dict[str, Any]] = []
        for row in chapter_rows:
            row_obj = {
                "chapter_id": row[0],
                "source_chapter_id": row[1],
                "chapter_index": row[2],
                "title": row[3],
                "status": row[4],
                "source_word_count": row[5],
                "preview_only": row[6],
                "primary_source_chapter_url": row[7],
                "content_file_path": row[8],
                "last_processed_at": row[9],
                "source_snapshot_refs_json": row[10],
                "fallback_source_id": row[11],
                "source_alignment_json": row[12],
                "trace_hash": row[13],
                "ai_model": row[14],
                "ai_prompt_tokens": row[15],
                "ai_completion_tokens": row[16],
                "ai_total_tokens": row[17],
                "ai_latency_ms": row[18],
                "deviation_score": row[19],
                "ai_self_score": row[20],
            }
            chapter_title = row[3] or title or f"第{int(row[2] or 0)}章"
            has_content = str(row[4]) in {"processed", "fallback"}
            if has_content:
                row_trace_meta = self._load_policy_snapshot(conn, row[0])
                trace_payload = self._build_shared_trace_payload(
                    conn=conn,
                    aggregate_book_id=aggregate_book_id,
                    chapter_row=row_obj,
                    trace_meta=row_trace_meta,
                )
                clean_body = self._read_chapter_content_from_file(row[8] or "")
                chapter_path = storage.chapter_markdown_path(
                    book_name=book_name,
                    author=book_author,
                    chapter_index=int(row[2] or 0),
                    title=chapter_title,
                )
                markdown = storage.render_chapter_markdown(
                    title=chapter_title,
                    body=clean_body,
                    trace_payload=trace_payload,
                )
                chapter_files.append((chapter_path, markdown))
                chapter_entries.append(
                    self._build_chapter_index_entry(
                        chapter_index=int(row[2] or 0),
                        title=chapter_title,
                        file=f"chapters/{chapter_path.name}",
                        trace_payload=trace_payload,
                    )
                )
            else:
                chapter_entries.append(
                    {
                        "index": int(row[2] or 0),
                        "title": chapter_title,
                        "file": None,
                        "status": self._shared_stage1_status(
                            status=str(row[4]),
                            preview_only=bool(row[6]),
                            trace_meta={},
                        ),
                    }
                )

        metadata_payload = self._build_shared_metadata_payload(
            conn=conn,
            aggregate_book_id=aggregate_book_id,
            book_name=book_name,
            book_author=book_author,
        )
        chapter_index_payload = {
            "schemaVersion": 2,
            "bookId": aggregate_book_id,
            "chapters": chapter_entries,
        }
        storage.write_book_bundle(
            metadata_path=storage.metadata_path(book_name=book_name, author=book_author),
            metadata_payload=metadata_payload,
            chapter_index_path=storage.chapter_index_path(book_name=book_name, author=book_author),
            chapter_index_payload=chapter_index_payload,
            chapter_files=chapter_files,
        )

    def _dual_verify_chapter_output(
        self,
        conn: sqlite3.Connection,
        *,
        chapter_id: str,
        aggregate_book_id: str,
        title: str,
        content: str,
    ) -> None:
        contract = self._shared_book_storage_contract()
        if not contract.get("shouldCompareReads"):
            return
        row = conn.execute(
            """
            SELECT chapter_index
            FROM aggregate_chapter_tasks
            WHERE chapter_id = ?
            """,
            (chapter_id,),
        ).fetchone()
        if not row or not row[0]:
            return
        storage = self._shared_book_storage()
        book_name = self._aggregate_book_name(conn, aggregate_book_id)
        book_author = self._aggregate_book_author(conn, aggregate_book_id)
        shared_path = storage.chapter_markdown_path(
            book_name=book_name,
            author=book_author,
            chapter_index=int(row[0] or 0),
            title=title,
        )
        if not shared_path.exists():
            logger.warning(
                "shared-book dual_verify mismatch for %s: shared chapter missing at %s",
                chapter_id,
                shared_path,
            )
            return
        shared_markdown = shared_path.read_text(encoding="utf-8")
        shared_content = self._strip_trace_block(shared_markdown)
        legacy_content = self._strip_trace_block(content)
        mismatches: list[str] = []
        if shared_content != legacy_content:
            mismatches.append("content")
        try:
            shared_trace = storage.parse_trace_block(shared_markdown)
            shared_status = str(shared_trace.get("chapterStatus", "") or "")
            if not shared_status:
                mismatches.append("trace.chapterStatus")
        except Exception as exc:
            mismatches.append(f"trace:{exc}")
        if mismatches:
            logger.warning(
                "shared-book dual_verify mismatch for %s: %s",
                chapter_id,
                ", ".join(mismatches),
            )

    def _source_book_id_from_payload(self, payload: dict[str, Any], source_id: str) -> str:
        for source in payload.get("sources", []) if isinstance(payload.get("sources"), list) else []:
            if isinstance(source, dict) and source.get("sourceId") == source_id:
                return source.get("bookId", "") or ""
        return ""

    def _save_source_snapshot(
        self,
        *,
        aggregate_book_id: str,
        chapter_index: int,
        source_id: str,
        source_book_id: str,
        source_chapter_id: str,
        title: str,
        raw_content: str = "",
        classification: str = "unknown",
        source_chapter_url: str = "",
        clean_content: str = "",
    ) -> None:
        raw_content = raw_content or clean_content
        if not aggregate_book_id or not chapter_index or not source_id or not raw_content:
            return
        if self._looks_like_garbled_text(raw_content):
            raise ValueError("garbled source snapshot content")
        from app.services.content_purify import purify_content_with_audit

        clean_content, purify_audit = purify_content_with_audit(
            raw_content,
            ad_patterns=self._ad_patterns_for_source(source_id),
            chapter_title=title,
        )
        if self._looks_like_garbled_text(clean_content):
            raise ValueError("garbled purified source snapshot content")
        content_hash = hashlib.sha256(clean_content.encode("utf-8")).hexdigest()
        now = self._now()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO aggregate_source_snapshots
                (aggregate_book_id, chapter_index, source_id, source_book_id, source_chapter_id, title,
                 source_chapter_url, raw_content, clean_content, content_hash, word_count,
                 purify_audit_json, classification, fetched_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(aggregate_book_id, chapter_index, source_id) DO UPDATE SET
                    source_book_id = excluded.source_book_id,
                    source_chapter_id = excluded.source_chapter_id,
                    title = excluded.title,
                    source_chapter_url = excluded.source_chapter_url,
                    raw_content = excluded.raw_content,
                    clean_content = excluded.clean_content,
                    content_hash = excluded.content_hash,
                    word_count = excluded.word_count,
                    purify_audit_json = excluded.purify_audit_json,
                    classification = excluded.classification,
                    fetched_at = excluded.fetched_at,
                    updated_at = excluded.updated_at
                """,
                (
                    aggregate_book_id,
                    chapter_index,
                    source_id,
                    source_book_id,
                    source_chapter_id,
                    title,
                    source_chapter_url,
                    raw_content,
                    clean_content,
                    content_hash,
                    len(clean_content),
                    json.dumps(purify_audit, ensure_ascii=False),
                    classification,
                    now,
                    now,
                ),
            )
            conn.commit()
        self._write_source_snapshot_file(
            aggregate_book_id=aggregate_book_id,
            chapter_index=chapter_index,
            source_id=source_id,
            source_book_id=source_book_id,
            source_chapter_id=source_chapter_id,
            source_chapter_url=source_chapter_url,
            title=title,
            raw_content=raw_content,
            clean_content=clean_content,
            content_hash=content_hash,
            purify_audit=purify_audit,
            classification=classification,
            fetched_at=now,
        )

    def _write_source_snapshot_file(
        self,
        *,
        aggregate_book_id: str,
        chapter_index: int,
        source_id: str,
        source_book_id: str,
        source_chapter_id: str,
        source_chapter_url: str,
        title: str,
        raw_content: str,
        clean_content: str,
        content_hash: str,
        purify_audit: list[dict[str, object]],
        classification: str,
        fetched_at: str,
    ) -> None:
        book_name, book_author = self._shared_book_identity(aggregate_book_id)
        if not book_name:
            return
        storage = self._shared_book_storage()
        storage.atomic_write_json(
            storage.source_snapshot_path(
                book_name=book_name,
                author=book_author,
                source_id=source_id,
                chapter_index=chapter_index,
            ),
            {
                "schemaVersion": 1,
                "aggregateBookId": aggregate_book_id,
                "chapterIndex": chapter_index,
                "title": title,
                "sourceId": source_id,
                "sourceBookId": source_book_id,
                "sourceChapterId": source_chapter_id,
                "sourceChapterUrl": source_chapter_url,
                "fetchedAt": fetched_at,
                "rawContent": raw_content,
                "content": clean_content,
                "contentHash": content_hash,
                "wordCount": len(clean_content),
                "purifyAudit": purify_audit,
                "classification": classification,
            },
        )

    def _load_source_snapshot_content(
        self,
        *,
        aggregate_book_id: str,
        chapter_index: int,
        source_id: str,
        expected_title: str = "",
        source_chapter_id: str = "",
    ) -> str:
        if not aggregate_book_id or not chapter_index or not source_id:
            return ""
        with self._conn() as conn:
            if source_chapter_id:
                # When a specific TOC match is requested the position row may
                # hold a different chapter (mirrors renumber chapters); never
                # serve a snapshot captured for another chapter of the same
                # source at the same aggregate position.
                row = conn.execute(
                    """
                    SELECT title, clean_content
                    FROM aggregate_source_snapshots
                    WHERE aggregate_book_id = ? AND chapter_index = ? AND source_id = ?
                      AND source_chapter_id = ?
                    LIMIT 1
                    """,
                    (aggregate_book_id, chapter_index, source_id, source_chapter_id),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT title, clean_content
                    FROM aggregate_source_snapshots
                    WHERE aggregate_book_id = ? AND chapter_index = ? AND source_id = ?
                    LIMIT 1
                    """,
                    (aggregate_book_id, chapter_index, source_id),
                ).fetchone()
        if not row:
            return ""
        snapshot_title = str(row[0] or "")
        content = str(row[1] or "")
        if expected_title and self._candidate_title_match_rank(expected_title, snapshot_title) is None:
            logger.info(
                "Skipped mismatched source snapshot: book=%s chapter=%s source=%s expected=%r actual=%r",
                aggregate_book_id,
                chapter_index,
                source_id,
                expected_title,
                snapshot_title,
            )
            return ""
        if self._looks_like_garbled_text(content):
            logger.warning(
                "Skipped garbled source snapshot: book=%s chapter=%s source=%s",
                aggregate_book_id,
                chapter_index,
                source_id,
            )
            return ""
        return content

    def _load_policy_snapshot(self, conn: sqlite3.Connection, chapter_id: str) -> dict:
        row = conn.execute(
            "SELECT policy_snapshot_json FROM aggregate_chapter_tasks WHERE chapter_id = ?",
            (chapter_id,),
        ).fetchone()
        if not row or not row[0]:
            return {}
        try:
            return json.loads(row[0])
        except Exception:
            return {}

    def _build_trace_meta(
        self,
        *,
        aggregate_book_id: str,
        chapter_index: int | None,
        title: str,
        alignment_json: dict,
        fallback_source_id: str,
        content: str,
        source_word_count: int = 0,
        primary_source_chapter_url: str = "",
        preview_only: bool = False,
        status: str = "",
        word_count_validation: dict[str, Any] | None = None,
        lexicon_result: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        book = self._library_books().get_book(aggregate_book_id) or {}
        candidate_sources = []
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT source_id, source_book_id, role, score
                FROM aggregate_book_sources
                WHERE aggregate_book_id = ?
                ORDER BY CASE role WHEN 'primary' THEN 0 ELSE 1 END, score DESC, source_id ASC
                """,
                (aggregate_book_id,),
            ).fetchall()
            candidate_sources = [
                {
                    "sourceId": row[0],
                    "sourceBookId": row[1],
                    "role": row[2],
                    "score": int(row[3] or 0),
                }
                for row in rows
            ]
        modification_trail = [
            {
                "step": "selected_source",
                "selectedContentSource": alignment_json.get("selectedContentSource", ""),
                "primarySourceId": alignment_json.get("primarySourceId", ""),
                "candidateSourceId": alignment_json.get("candidateSourceId", ""),
                "fallbackSourceId": fallback_source_id,
            },
            {
                "step": "processing",
                "alignmentReason": alignment_json.get("alignmentReason", ""),
            },
        ]
        if fallback_source_id and fallback_source_id != alignment_json.get("primarySourceId", ""):
            modification_trail.insert(
                1,
                {
                    "step": "candidate_completion",
                    "candidateSourceId": fallback_source_id,
                    "titleSimilarity": alignment_json.get("titleSimilarity", 0.0),
                    "previewSimilarity": alignment_json.get("previewSimilarity", 0.0),
                    "headPreviewSimilarity": alignment_json.get("headPreviewSimilarity", 0.0),
                    "alignmentPassed": bool(alignment_json.get("alignmentPassed")),
                    "alignmentReason": alignment_json.get("alignmentReason", ""),
                },
            )
        manual_supplement = alignment_json.get("manualSupplement")
        if isinstance(manual_supplement, dict) and manual_supplement:
            modification_trail.append({
                "step": "manual_override",
                "candidateSourceId": manual_supplement.get("sourceId", ""),
                "sourceChapterId": manual_supplement.get("sourceChapterId", ""),
                "chosenAt": manual_supplement.get("chosenAt", ""),
                "classification": manual_supplement.get("classification", ""),
                "wordCountPassed": bool(
                    (manual_supplement.get("wordCount") or {}).get("passed")
                ),
                "alignmentPassed": bool(
                    (manual_supplement.get("alignment") or {}).get("passed")
                ),
            })
        primary_source_id = alignment_json.get("primarySourceId", "") or book.get("primarySourceId", "")
        selected_source_id = alignment_json.get("candidateSourceId", "") or primary_source_id
        # 从 TOC 缓存读取真实 is_vip（不依赖 preview_only 推断）
        toc_vip_map = self._toc_is_vip_cache.get(aggregate_book_id, {})
        is_vip = toc_vip_map.get(chapter_index or 0, bool(preview_only))
        source_hashes: list[dict[str, Any]] = []
        if chapter_index:
            with self._conn() as conn:
                rows = conn.execute(
                    """
                    SELECT source_id, content_hash, classification, fetched_at
                    FROM aggregate_source_snapshots
                    WHERE aggregate_book_id = ? AND chapter_index = ?
                    ORDER BY source_id ASC
                    """,
                    (aggregate_book_id, chapter_index),
                ).fetchall()
            source_hashes = [
                {
                    "sourceId": row[0],
                    "contentHash": row[1],
                    "classification": row[2],
                    "fetchedAt": row[3],
                }
                for row in rows
            ]
        chapter_status = (
            "fetched" if status == "fallback" and preview_only else
            "readable" if status == "fallback" else
            "supplemented" if status == "processed" and fallback_source_id else
            "readable" if status == "processed" else
            "fetched" if preview_only else
            status or "unknown"
        )
        # 从 selectedContentSource + alignment 推导语义化 alignedWith
        scs = alignment_json.get("selectedContentSource", "")
        if scs == "official":
            aligned_with = "official_full"
        elif scs == "candidate" and alignment_json.get("alignmentPassed"):
            aligned_with = "official_preview"
        elif scs == "preview_fallback":
            aligned_with = "official_preview_only"
        elif scs == "third_party":
            aligned_with = "third_party"
        elif scs == "third_party_suspect":
            aligned_with = "suspect"
        else:
            aligned_with = scs
        return {
            "schemaVersion": 2,
            "aggregateBookId": aggregate_book_id,
            "chapterIndex": chapter_index,
            "chapterTitle": title,
            "chapterStatus": chapter_status,
            "proofreadComplete": False,
            "stage3Verdict": "",
            "stage3Reason": "",
            "policyVersion": book.get("currentPolicyVersion", 1),
            "processedAt": self._now(),
            "startChapterIndex": book.get("startChapterIndex", 1),
            "primarySource": {
                "sourceId": primary_source_id,
                "chapterId": "",
                "wordCount": int(source_word_count or 0),
            },
            "primarySourceChapterUrl": primary_source_chapter_url,
            "stage3Attempted": False,
            "candidateSources": candidate_sources,
            "selectedSource": selected_source_id,
            "selectedContentSource": alignment_json.get("selectedContentSource", ""),
            "alignedWith": aligned_with,
            "sourceWordCount": int(source_word_count or 0),
            "officialWordCount": int(source_word_count or 0),
            "officialPreviewWords": int(source_word_count or 0) if (preview_only or is_vip) else None,
            "isVip": is_vip,
            "fetchedWordCount": len(content or ""),
            "previewOnly": bool(preview_only),
            "fallbackSourceId": fallback_source_id,
            "primarySourceId": primary_source_id,
            "aiAggregateEnabled": False,
            "aiPurifyEnabled": False,
            "aiModel": "",
            "modificationTrail": modification_trail,
            "sourceHashes": source_hashes,
            "finalContentHash": hashlib.sha256((content or "").encode("utf-8")).hexdigest() if content else "",
            "alignment": alignment_json,
            "wordCountValidation": word_count_validation or {},
            "lexiconResult": lexicon_result or {
                "hasMaskedWords": False,
                "maskedWordCount": 0,
                "maskedWordCandidates": [],
                "lexiconRestored": False,
            },
        }

    async def _strip_embedded_author_say(
        self,
        *,
        catalog,
        chapter: dict,
        content: str,
        aggregate_book_id: str = "",
        chapter_index: int | None = None,
    ) -> dict[str, Any]:
        """Drop mirror-appended 作家说 that already exists in authorReviews."""
        from app.services.author_say_strip import (
            extract_author_say_texts,
            strip_overlapping_author_say,
        )
        from app.services.reading_reviews import chapter_review_cache

        original = content or ""
        if not original.strip():
            return {"content": original, "stripped": False, "removedChars": 0}

        review_chapter_id = str(
            chapter.get("sourceChapterId") or chapter.get("chapterId") or ""
        ).strip()
        if not review_chapter_id:
            return {"content": original, "stripped": False, "removedChars": 0}

        reviews = chapter_review_cache.get(review_chapter_id)
        if reviews is None and catalog is not None:
            try:
                reviews = await catalog.chapter_reviews(review_chapter_id)
                if isinstance(reviews, dict):
                    chapter_review_cache.set(review_chapter_id, reviews)
            except Exception as exc:
                self._log_chapter_step(
                    aggregate_book_id=aggregate_book_id,
                    chapter_index=chapter_index,
                    title=str(chapter.get("title", "") or ""),
                    event="author_say_fetch_failed",
                    stage="stage1",
                    error_message=str(exc),
                    payload={"step": "作家说拉取失败，跳过正文去重", "chapterId": review_chapter_id},
                )
                return {"content": original, "stripped": False, "removedChars": 0}

        author_texts = extract_author_say_texts(reviews)
        if not author_texts:
            return {"content": original, "stripped": False, "removedChars": 0}

        cleaned = strip_overlapping_author_say(original, author_texts)
        removed = max(0, len(original) - len(cleaned))
        if removed <= 0 or cleaned == original:
            return {"content": original, "stripped": False, "removedChars": 0}

        self._log_chapter_step(
            aggregate_book_id=aggregate_book_id,
            chapter_index=chapter_index,
            title=str(chapter.get("title", "") or ""),
            event="author_say_stripped",
            stage="stage1",
            payload={
                "step": "正文已去除与作家说重叠内容",
                "chapterId": review_chapter_id,
                "authorSayCount": len(author_texts),
                "removedChars": removed,
            },
        )
        return {"content": cleaned, "stripped": True, "removedChars": removed}

    def _extract_source_word_count(self, chapter_result: dict[str, Any]) -> int:
        if not isinstance(chapter_result, dict):
            return 0
        for key in ("sourceWordCount", "wordCount", "wordsCount", "WordsCnt"):
            value = chapter_result.get(key)
            try:
                if value is not None and int(value) > 0:
                    return int(value)
            except (TypeError, ValueError):
                continue
        extra = chapter_result.get("extra")
        if isinstance(extra, dict):
            for key in ("sourceWordCount", "wordCount", "wordsCount", "WordsCnt", "actualWords"):
                value = extra.get(key)
                try:
                    if value is not None and int(value) > 0:
                        return int(value)
                except (TypeError, ValueError):
                    continue
        debug = chapter_result.get("debug")
        if isinstance(debug, dict):
            for key in ("sourceWordCount", "wordCount", "wordsCount", "WordsCnt"):
                value = debug.get(key)
                try:
                    if value is not None and int(value) > 0:
                        return int(value)
                except (TypeError, ValueError):
                    continue
        return 0

    def _extract_source_chapter_url(self, chapter_result: dict[str, Any], source_chapter_id: str) -> str:
        if isinstance(chapter_result, dict):
            for key in ("primarySourceChapterUrl", "sourceChapterUrl", "rawChapterUrl", "url", "chapterUrl"):
                value = chapter_result.get(key)
                if value:
                    return str(value)
            debug = chapter_result.get("debug")
            if isinstance(debug, dict):
                for key in ("primarySourceChapterUrl", "sourceChapterUrl", "rawChapterUrl", "url", "chapterUrl"):
                    value = debug.get(key)
                    if value:
                        return str(value)
        try:
            source_id, raw_url = decode_chapter_id(source_chapter_id)
            if raw_url and source_id != VIRTUAL_SOURCE_ID:
                return raw_url
        except Exception:
            pass
        return ""

    def _extract_preview_only(self, chapter_result: dict[str, Any]) -> bool:
        if not isinstance(chapter_result, dict):
            return False
        for key in ("previewOnly", "isPreview", "preview_only"):
            if key in chapter_result:
                return bool(chapter_result.get(key))
        extra = chapter_result.get("extra")
        if isinstance(extra, dict):
            for key in ("previewOnly", "isPreview", "preview_only", "isLocked"):
                if key in extra:
                    return bool(extra.get(key))
        debug = chapter_result.get("debug")
        if isinstance(debug, dict):
            for key in ("previewOnly", "isPreview", "preview_only"):
                if key in debug:
                    return bool(debug.get(key))
        return False

    def _extract_is_paid(self, chapter_result: dict[str, Any]) -> bool:
        if not isinstance(chapter_result, dict):
            return False
        for key in ("isPaid", "paid", "authRequired"):
            if key in chapter_result:
                return bool(chapter_result.get(key))
        extra = chapter_result.get("extra")
        if isinstance(extra, dict):
            for key in ("isPaid", "paid", "authRequired"):
                if key in extra:
                    return bool(extra.get(key))
        debug = chapter_result.get("debug")
        if isinstance(debug, dict):
            for key in ("isPaid", "paid", "authRequired"):
                if key in debug:
                    return bool(debug.get(key))
        return False

    def _normalize_book_status(self, detail_data: dict[str, Any]) -> str:
        if not isinstance(detail_data, dict):
            return "unknown"
        raw = str(
            detail_data.get("status")
            or detail_data.get("bookStatus")
            or detail_data.get("bookStatusText")
            or detail_data.get("kindStatus")
            or detail_data.get("kind")
            or ""
        ).strip().lower()
        if any(key in raw for key in ("完结", "completed", "finished", "已完结", "完本")):
            return "completed"
        if raw:
            return "ongoing"
        return "unknown"

    def _visible_processed_count(self, conn: sqlite3.Connection, aggregate_book_id: str, start_index: int) -> int:
        rows = conn.execute(
            """
            SELECT chapter_index, status, preview_only
            FROM aggregate_chapter_tasks
            WHERE aggregate_book_id = ? AND chapter_index >= ?
            ORDER BY chapter_index ASC
            """,
            (aggregate_book_id, start_index),
        ).fetchall()
        count = 0
        expected = start_index
        for row in rows:
            chapter_index = int(row[0] or 0)
            if chapter_index != expected:
                break
            if row[1] not in ("processed", "fallback"):
                break
            # Preview-only chapters are not counted toward the discovery threshold.
            if bool(row[2]):
                break
            count += 1
            expected += 1
        return count

    def _activate_backfill_if_ready(self, aggregate_book_id: str) -> None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT start_chapter_index, initial_snapshot_last_index, backfill_started
                FROM aggregate_book_tasks
                WHERE aggregate_book_id = ?
                """,
                (aggregate_book_id,),
            ).fetchone()
            if not row:
                return
            start_index = int(row[0] or 1)
            initial_last = int(row[1] or 0)
            backfill_started = bool(row[2])
            if backfill_started or start_index <= 1 or initial_last < start_index:
                return
            pending = conn.execute(
                """
                SELECT COUNT(*)
                FROM aggregate_chapter_tasks
                WHERE aggregate_book_id = ?
                  AND chapter_index >= ?
                  AND chapter_index <= ?
                  AND status NOT IN ('processed', 'fallback')
                """,
                (aggregate_book_id, start_index, initial_last),
            ).fetchone()[0]
            if int(pending or 0) > 0:
                return
            conn.execute(
                """
                UPDATE aggregate_book_tasks
                SET backfill_started = 1, updated_at = ?
                WHERE aggregate_book_id = ?
                """,
                (self._now(), aggregate_book_id),
            )
            conn.execute(
                """
                UPDATE aggregate_chapter_tasks
                SET status = 'pending', updated_at = ?
                WHERE aggregate_book_id = ?
                  AND placeholder = 1
                  AND status = 'placeholder'
                """,
                (self._now(), aggregate_book_id),
            )
            conn.commit()

    def _schedule_preview_retry(self, conn: sqlite3.Connection, chapter_id: str, now: str) -> None:
        """Increment preview retry count and schedule the next long-period retry.

        The chapter just became (or stayed) a preview-only fallback. We count this
        as one retry attempt, then schedule the next attempt after the delay that
        corresponds to the *old* count:

          old_count=0 -> schedule after delay[0] (30m)
          old_count=4 -> schedule after delay[4] (480m)
          old_count=5 -> delay list exhausted, no more retries

        Once the count exceeds the delay-list length the row remains a stable
        preview fallback.
        """
        row = conn.execute(
            "SELECT COALESCE(preview_retry_count, 0) FROM aggregate_chapter_tasks WHERE chapter_id = ?",
            (chapter_id,),
        ).fetchone()
        old_count = (row[0] if row else 0) or 0
        new_count = old_count + 1
        # compute_preview_next_retry_time() returns None once old_count is past the
        # final delay entry, so a simple None check schedules every valid delay.
        next_time = compute_preview_next_retry_time(old_count)
        if next_time:
            conn.execute(
                """
                UPDATE aggregate_chapter_tasks
                SET preview_retry_count = ?, next_retry_time = ?, updated_at = ?
                WHERE chapter_id = ?
                """,
                (new_count, next_time, now, chapter_id),
            )
        else:
            conn.execute(
                """
                UPDATE aggregate_chapter_tasks
                SET preview_retry_count = ?, next_retry_time = NULL, updated_at = ?
                WHERE chapter_id = ?
                """,
                (new_count, now, chapter_id),
            )

    def _schedule_initial_preview_recheck(self, aggregate_book_id: str, now: str) -> int:
        """Make the first preview retry due once the initial backlog is empty."""
        with self._conn() as conn:
            cursor = conn.execute(
                """
                UPDATE aggregate_chapter_tasks
                SET next_retry_time = ?, updated_at = ?
                WHERE aggregate_book_id = ?
                  AND status = 'fallback'
                  AND preview_only = 1
                  AND COALESCE(preview_retry_count, 0) = 1
                """,
                (now, now, aggregate_book_id),
            )
            conn.commit()
            return max(0, int(cursor.rowcount or 0))

    def _refresh_shared_book_state(self, aggregate_book_id: str) -> None:
        should_archive_subscriptions = False
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT start_chapter_index, total_chapters, book_status, status
                FROM aggregate_book_tasks
                WHERE aggregate_book_id = ?
                """,
                (aggregate_book_id,),
            ).fetchone()
            if not row:
                return
            start_index = int(row[0] or 1)
            total_chapters = int(row[1] or 0)
            book_status = str(row[2] or "unknown")
            current_status = str(row[3] or "active")
            visible_count = self._visible_processed_count(conn, aggregate_book_id, start_index)
            unfinished_count = int(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM aggregate_chapter_tasks
                    WHERE aggregate_book_id = ?
                      AND status NOT IN ('processed', 'fallback')
                    """,
                    (aggregate_book_id,),
                ).fetchone()[0]
                or 0
            )
            preview_retry_count = int(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM aggregate_chapter_tasks
                    WHERE aggregate_book_id = ?
                      AND status = 'fallback'
                      AND preview_only = 1
                      AND next_retry_time IS NOT NULL
                    """,
                    (aggregate_book_id,),
                ).fetchone()[0]
                or 0
            )
            required = max(0, total_chapters - start_index + 1)
            configured_threshold = self._library_books().discovery_min_readable_chapters()
            threshold = min(configured_threshold, required) if required > 0 else 0
            search_visibility = "visible" if visible_count >= threshold and threshold > 0 else "hidden"
            next_status = current_status
            if current_status not in {"paused", "archived"}:
                next_status = (
                    "archived"
                    if unfinished_count == 0 and preview_retry_count == 0 and book_status == "completed"
                    else "active"
                )
            should_archive_subscriptions = next_status == "archived"
            conn.execute(
                """
                UPDATE aggregate_book_tasks
                SET visible_processed_chapters = ?,
                    search_visibility_status = ?,
                    status = ?,
                    next_check_time = CASE WHEN ? = 'archived' THEN NULL ELSE next_check_time END,
                    archived_at = CASE
                        WHEN ? = 'archived' AND archived_at IS NULL THEN ?
                        WHEN ? != 'archived' THEN archived_at
                        ELSE archived_at
                    END,
                    updated_at = ?
                WHERE aggregate_book_id = ?
                """,
                (
                    visible_count,
                    search_visibility,
                    next_status,
                    next_status,
                    next_status,
                    self._now(),
                    next_status,
                    self._now(),
                    aggregate_book_id,
                ),
            )
            conn.commit()
        if should_archive_subscriptions:
            from app.services.user_subscriptions import UserSubscriptionsService

            UserSubscriptionsService(self.db_path).archive_completed_for_book(aggregate_book_id)

    def _pending_chapter_count(self, aggregate_book_id: str) -> int:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM aggregate_chapter_tasks
                WHERE aggregate_book_id = ?
                  AND status IN ('pending', 'placeholder')
                """,
                (aggregate_book_id,),
            ).fetchone()
        return int(row[0] or 0)

    def _aggregate_book_name(self, conn: sqlite3.Connection, aggregate_book_id: str) -> str:
        row = conn.execute(
            "SELECT name FROM aggregate_book_tasks WHERE aggregate_book_id = ?",
            (aggregate_book_id,),
        ).fetchone()
        if row and row[0]:
            return row[0]
        row = conn.execute("SELECT name FROM book_records WHERE book_id = ?", (aggregate_book_id,)).fetchone()
        return row[0] if row and row[0] else ""

    def _aggregate_book_author(self, conn: sqlite3.Connection, aggregate_book_id: str) -> str:
        row = conn.execute(
            "SELECT author FROM aggregate_book_tasks WHERE aggregate_book_id = ?",
            (aggregate_book_id,),
        ).fetchone()
        if row and row[0]:
            return row[0]
        row = conn.execute("SELECT author FROM book_records WHERE book_id = ?", (aggregate_book_id,)).fetchone()
        return row[0] if row and row[0] else ""

    def _aggregate_book_meta(self, conn: sqlite3.Connection, aggregate_book_id: str) -> dict[str, Any]:
        """Return the book-level metadata needed to rebuild a library card."""
        row = conn.execute(
            """
            SELECT cover_url, intro, word_count, primary_book_id, primary_source_id,
                   primary_source_name, primary_book_url, primary_toc_url,
                   start_chapter_index, total_chapters, book_status
            FROM aggregate_book_tasks
            WHERE aggregate_book_id = ?
            """,
            (aggregate_book_id,),
        ).fetchone()
        if not row:
            return {}
        return {
            "coverUrl": row[0] or "",
            "intro": row[1] or "",
            "wordCount": row[2] or "",
            "primaryBookId": row[3] or "",
            "primarySourceId": row[4] or "",
            "primarySourceName": row[5] or "",
            "primaryBookUrl": row[6] or "",
            "primaryTocUrl": row[7] or "",
            "startChapterIndex": row[8] or 1,
            "totalChapters": row[9] or 0,
            "bookStatus": row[10] or "unknown",
        }

    async def run_forever(self, stop_event: asyncio.Event, poll_seconds: int = 60) -> None:
        logger.info("Aggregate processor started, pollSeconds=%d", poll_seconds)
        cleanup_counter = 0
        while not stop_event.is_set():
            try:
                result = await self.run_due_once(limit=5)
                if result.get("processedBooks", 0) > 0:
                    logger.info(
                        "Aggregate run completed: processedBooks=%d, dueBooks=%d",
                        result.get("processedBooks", 0),
                        result.get("dueBooks", 0),
                    )
            except Exception:
                logger.warning("Aggregate run_due_once failed", exc_info=True)

            cleanup_counter += 1
            if cleanup_counter >= 10:
                cleanup_counter = 0
                try:
                    await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: NovelFileCache(
                            root=self.db_path.parent / "cache"
                        ).cleanup_temp_cache(max_age_hours=24),
                    )
                except Exception:
                    logger.warning("Novel file cache cleanup failed", exc_info=True)

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=poll_seconds)
            except asyncio.TimeoutError:
                continue
        logger.info("Aggregate processor stopped")
