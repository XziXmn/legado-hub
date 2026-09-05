"""Console API endpoints for plugin governance, testing, search jobs, explore, books, and configuration."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from starlette.concurrency import run_in_threadpool

from app.config import APP_PHASE, APP_VERSION
from app.core.app_config import AppConfig
from app.core.aggregate_config import load_aggregate_config, update_progress
from app.services.catalog import Catalog
from app.services.book_catalog import BookCatalog
from app.services.cookie_store import CookieStore
from app.services.search_jobs import SearchJobService
from app.services.live_acceptance import LiveAcceptanceService
from app.services.live_check_repository import LiveCheckRepository
from app.services.update_scheduler import UpdateScheduler
from app.services.cache import Cache
from app.services.source_ping import SourcePingService
from app.services.login_browser_service import login_browser_service
from app.services.official_auth.manager import official_auth_manager
from app.services.aggregate_processor import AggregateProcessor
from app.services.aggregate_settings import AI_RUNTIME_ENABLED, AggregateSettingsRepository
from app.services.audit import audit_service
from app.services.lexicon_updater import LexiconUpdater
from app.services.library_books import library_books_service
from app.services.shared_book_lock import SharedBookLockService
from app.services.shared_book_scheduler import SharedBookScheduler
from app.services.shared_book_storage import TRACE_BEGIN
from app.source_plugins.loader import PluginLoader
from app.source_plugins.scheduler import PluginScheduler, get_plugin_scheduler
from app.source_plugins.id_codec import encode_chapter_id
from app.services.aggregate_virtual_source import VIRTUAL_SOURCE_ID, make_aggregate_chapter_url
from app.services.user_auth import auth_service

logger = logging.getLogger(__name__)

console_router = APIRouter(prefix="/api/console")
_CONSOLE_STARTED_AT = time.monotonic()
_DELETE_LEASE_WAIT_SECONDS = 12.0


def console_route(method: str, path: str, *, access: str = "admin", **kwargs):
    """Register a console route with an explicit server-side access policy."""
    if access not in {"admin", "user"}:
        raise ValueError(f"unsupported console route access: {access}")
    dependency = auth_service.require_admin if access == "admin" else auth_service.require_user
    dependencies = [*kwargs.pop("dependencies", []), Depends(dependency)]
    return getattr(console_router, method)(path, dependencies=dependencies, **kwargs)


def _read_json(path: Path, default: dict) -> dict:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _aggregate_book_settings(payload: str) -> dict:
    try:
        return json.loads(payload or "{}")
    except (json.JSONDecodeError, TypeError):
        logger.warning("Ignoring malformed aggregate book settings", exc_info=True)
        return {}


def _reject_unknown_fields(payload: dict, allowed: set[str], *, label: str = "请求") -> None:
    unknown = sorted(str(key) for key in payload if key not in allowed)
    if unknown:
        raise HTTPException(
            status_code=422,
            detail=f"{label}包含不支持的字段: {', '.join(unknown)}",
        )


def _parse_int_field(
    value: Any,
    *,
    field: str,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    if isinstance(value, bool):
        raise HTTPException(status_code=422, detail=f"{field} 必须是整数")
    try:
        if isinstance(value, float) and not value.is_integer():
            raise ValueError
        parsed = int(value)
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail=f"{field} 必须是整数")
    if minimum is not None and parsed < minimum:
        raise HTTPException(status_code=422, detail=f"{field} 不能小于 {minimum}")
    if maximum is not None and parsed > maximum:
        raise HTTPException(status_code=422, detail=f"{field} 不能大于 {maximum}")
    return parsed


def _parse_float_field(
    value: Any,
    *,
    field: str,
    minimum: float | None = None,
) -> float:
    if isinstance(value, bool):
        raise HTTPException(status_code=422, detail=f"{field} 必须是数字")
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail=f"{field} 必须是数字")
    if not math.isfinite(parsed):
        raise HTTPException(status_code=422, detail=f"{field} 必须是有限数字")
    if minimum is not None and parsed < minimum:
        raise HTTPException(status_code=422, detail=f"{field} 不能小于 {minimum:g}")
    return parsed


def _parse_bool_field(value: Any, *, field: str) -> bool:
    if not isinstance(value, bool):
        raise HTTPException(status_code=422, detail=f"{field} 必须是布尔值")
    return value


def _shared_book_processing_settings(book: dict) -> dict:
    settings = _aggregate_book_settings(str(book.get("settingsJson", "") or ""))
    try:
        interval_minutes = int(book.get("intervalMinutes") or settings.get("updateIntervalMinutes") or 60)
    except (TypeError, ValueError):
        interval_minutes = 60
    try:
        backlog_limit = int(settings.get("backlogChapterLimit", 25) or 25)
    except (TypeError, ValueError):
        backlog_limit = 25
    return {
        "updateIntervalMinutes": min(1440, max(10, interval_minutes)),
        "backlogChapterLimit": min(100, max(5, backlog_limit)),
    }


_LIBRARY_BOOK_SETTING_FIELDS = {
    "autoTrackUpdates",
    "updateIntervalMinutes",
    "backlogChapterLimit",
    "aiAggregateEnabled",
    "aiPurifyEnabled",
    "primarySourceMode",
    "sourcePriorityMode",
    "primarySourcePriority",
    "sourcePriority",
}


def _validate_library_book_settings(payload: dict) -> dict:
    _reject_unknown_fields(payload, _LIBRARY_BOOK_SETTING_FIELDS, label="单书设置")
    if not payload:
        raise HTTPException(status_code=422, detail="单书设置不能为空")
    normalized: dict[str, Any] = {}
    for field_name in ("autoTrackUpdates", "aiAggregateEnabled", "aiPurifyEnabled"):
        if field_name in payload:
            normalized[field_name] = _parse_bool_field(payload[field_name], field=field_name)
    if "updateIntervalMinutes" in payload:
        normalized["updateIntervalMinutes"] = _parse_int_field(
            payload["updateIntervalMinutes"],
            field="updateIntervalMinutes",
            minimum=10,
            maximum=1440,
        )
    if "backlogChapterLimit" in payload:
        normalized["backlogChapterLimit"] = _parse_int_field(
            payload["backlogChapterLimit"],
            field="backlogChapterLimit",
            minimum=5,
            maximum=100,
        )
    if "primarySourceMode" in payload:
        value = str(payload["primarySourceMode"] or "").strip()
        if value not in {"official", "best_progress", "best_score"}:
            raise HTTPException(status_code=422, detail="primarySourceMode 值无效")
        normalized["primarySourceMode"] = value
    if "sourcePriorityMode" in payload:
        value = str(payload["sourcePriorityMode"] or "").strip()
        if value not in {"auto", "manual"}:
            raise HTTPException(status_code=422, detail="sourcePriorityMode 值无效")
        normalized["sourcePriorityMode"] = value
    if "primarySourcePriority" in payload and "sourcePriority" in payload:
        raise HTTPException(status_code=422, detail="不能同时提交 primarySourcePriority 和 sourcePriority")
    priority_field = "primarySourcePriority" if "primarySourcePriority" in payload else "sourcePriority"
    if priority_field in payload:
        priority = payload[priority_field]
        if not isinstance(priority, list) or any(not isinstance(item, str) for item in priority):
            raise HTTPException(status_code=422, detail=f"{priority_field} 必须是字符串数组")
        normalized["sourcePriority"] = list(dict.fromkeys(item.strip() for item in priority if item.strip()))
        normalized["sourcePriorityMode"] = "manual" if normalized["sourcePriority"] else "auto"
    return normalized


def _apply_book_source_priority_settings(settings: dict, payload: dict) -> bool:
    changed = False
    if "sourcePriorityMode" in payload:
        settings["sourcePriorityMode"] = str(payload["sourcePriorityMode"] or "auto")
        changed = True
    priority = payload.get("primarySourcePriority", payload.get("sourcePriority"))
    if isinstance(priority, list):
        settings["sourcePriority"] = [str(x) for x in priority]
        settings["sourcePriorityMode"] = "manual" if priority else "auto"
        changed = True
    return changed


def _shared_book_storage_read_mode() -> str:
    workflow = AggregateSettingsRepository().content_workflow()
    if not bool(workflow.get("useSharedBookStorage", False)):
        return "legacy"
    return str(workflow.get("sharedBookStorageReadMode", "shared") or "shared").strip().lower()


def _is_admin_role(user) -> bool:
    return str(getattr(user, "role", "") or "").strip().lower() == "admin"


def _explicit_auth_identity(payload: dict | None) -> str:
    """Return an explicit account name or phone identity from auth output."""
    if not isinstance(payload, dict):
        return ""
    for key in (
        "accountName",
        "nickName",
        "userName",
        "mobilePhone",
        "mobile",
        "phone",
        "bindPhone",
        "phoneNumber",
        "phoneMasked",
        "mobileMasked",
    ):
        value = str(payload.get(key, "") or "").strip()
        if value:
            return value
    return ""


def _record_official_login_audit(
    admin,
    plugin_id: str,
    method: str,
    *,
    result: dict | None = None,
    error_code: str = "",
) -> None:
    authenticated = bool(result and result.get("authenticated") and _explicit_auth_identity(result))
    audit_service.record(
        action="official_source.login",
        actor_user_id=admin.user_id,
        actor_role=admin.role,
        target_type="official_source",
        target_id=plugin_id,
        source_id=plugin_id,
        outcome="success" if authenticated else "failure",
        summary={
            "method": method,
            "authenticated": authenticated,
            "errorCode": error_code,
        },
    )


def _normalize_auth_identity(payload: dict | None) -> dict:
    """Reject authenticated results that do not carry an explicit identity."""
    result = dict(payload or {})
    identity = _explicit_auth_identity(result)
    if identity and not str(result.get("accountName", "") or "").strip():
        result["accountName"] = identity
    if result.get("authenticated") and not identity:
        result["authenticated"] = False
        result["authStatus"] = "pending"
        result["requiredActions"] = result.get("requiredActions") or ["check_auth_status"]
        result["message"] = "登录态未返回明确账号或手机号，暂不判定为成功"
    return result


def _display_book_status(value: object) -> str:
    raw = str(value or "").strip()
    normalized = raw.lower()
    if not normalized:
        return "未知"
    if any(key in normalized for key in ("completed", "finished", "finish", "done", "end")):
        return "已完结"
    if any(key in raw for key in ("完结", "完本", "已完结")):
        return "已完结"
    if any(key in normalized for key in ("ongoing", "serial", "updating", "active")):
        return "连载中"
    if any(key in raw for key in ("连载", "连载中", "更新中")):
        return "连载中"
    if any(key in normalized for key in ("paused", "hiatus", "stopped")):
        return "暂停"
    if any(key in raw for key in ("暂停", "停更", "断更")):
        return "暂停"
    if normalized == "unknown":
        return "未知"
    return raw


def _sanitize_source_map_summary(items: list[dict] | None) -> list[dict]:
    sanitized = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        last_chapter = item.get("lastChapter", "") or ""
        chapter_count = int(item.get("chapterCount", 0) or 0)
        if chapter_count <= 0:
            import re

            match = re.search(r"第\s*(\d+)\s*章", str(last_chapter))
            if match:
                chapter_count = int(match.group(1))
        sanitized.append(
            {
                "sourceId": item.get("sourceId", "") or "",
                "sourceName": item.get("sourceName", "") or "",
                "score": int(item.get("score", 0) or 0),
                "chapterCount": chapter_count,
                "lastChapter": last_chapter,
                "bookStatus": _display_book_status(item.get("bookStatus", "")),
                "name": item.get("name", "") or "",
                "author": item.get("author", "") or "",
            }
        )
    return sanitized


def _sanitize_trace_summary(summary: dict | None) -> dict:
    payload = summary if isinstance(summary, dict) else {}
    return {
        "stage": payload.get("stage", "") or "",
        "currentStep": payload.get("currentStep", "") or "",
        "nextStep": payload.get("nextStep", "") or "",
        "chapterStatus": payload.get("chapterStatus", "") or "",
        "selectedSource": payload.get("selectedSource", "") or "",
        "selectedContentSource": payload.get("selectedContentSource", "") or "",
        "fallbackSourceId": payload.get("fallbackSourceId", "") or "",
        "alignmentPassed": payload.get("alignmentPassed"),
        "alignmentReason": payload.get("alignmentReason", "") or "",
        "titleSimilarity": payload.get("titleSimilarity"),
        "previewSimilarity": payload.get("previewSimilarity"),
        "aiModel": payload.get("aiModel", "") or "",
        "aiTokens": int(payload.get("aiTokens", 0) or 0),
        "processedAt": payload.get("processedAt", "") or "",
        "traceHash": payload.get("traceHash", "") or "",
        "stage3Verdict": payload.get("stage3Verdict", "") or "",
        "stage3Reason": payload.get("stage3Reason", "") or "",
        "currentChapterIndex": payload.get("currentChapterIndex"),
        "currentChapterTitle": payload.get("currentChapterTitle", "") or "",
        "nextChapterIndex": payload.get("nextChapterIndex"),
        "nextChapterTitle": payload.get("nextChapterTitle", "") or "",
    }


def _load_shared_library_book_summary(book_id: str, *, admin_view: bool = False) -> dict:
    book = library_books_service.get_book(book_id)
    if not book:
        return {"bookId": book_id, "found": False}
    shared_metadata = library_books_service.load_shared_metadata(book_id)
    source_map = shared_metadata.get("sourceMap") if isinstance(shared_metadata.get("sourceMap"), dict) else {}
    health = source_map.get("health") if isinstance(source_map.get("health"), dict) else {}
    source_summary = _sanitize_source_map_summary(
        library_books_service.build_source_map_summary(shared_metadata)
    )
    payload = {
        "bookId": book_id,
        "found": True,
        "book": book,
        "bookState": library_books_service.build_book_state_summary(shared_metadata),
        "sourceMap": {
            "summary": source_summary,
            "health": {
                "status": str(health.get("status", "") or ""),
                "lastVerifiedAt": str(health.get("lastVerifiedAt", "") or ""),
                "missingCriticalSource": bool(health.get("missingCriticalSource")),
            },
        },
        "sourceMapSummary": source_summary,
        "sourceMapRefresh": library_books_service.source_map_refresh_state(book_id),
        "sourceSnapshotProgress": library_books_service.source_snapshot_progress(book_id),
        "freeChapterEndIndex": int(shared_metadata.get("freeChapterEndIndex", 0) or 0),
    }
    if admin_view:
        payload["processingSettings"] = _shared_book_processing_settings(book)
        payload["currentPolicyVersion"] = int(book.get("currentPolicyVersion", 1) or 1)
        payload["intervalMinutes"] = payload["processingSettings"]["updateIntervalMinutes"]
        payload["payload"] = library_books_service.load_payload(book_id)
    return payload


def _list_shared_library_book_chapters(
    book_id: str,
    *,
    page: int = 1,
    pageSize: int = 50,
    status: str = "all",
    keyword: str = "",
) -> dict:
    from app.services.library_books import library_books_service

    book = library_books_service.get_book(book_id)
    if not book:
        return {"items": [], "page": page, "pageSize": pageSize, "total": 0}

    page = _bounded_page(page, 1, 1000000)
    page_size = _bounded_page(pageSize, 50, 200)
    storage = library_books_service.shared_book_storage
    book_name = str(book.get("name", "") or "").strip()
    author = str(book.get("author", "") or "").strip()
    chapter_index_payload = storage._read_json(storage.chapter_index_path(book_name=book_name, author=author)) or {}
    chapter_entries = chapter_index_payload.get("chapters")
    if not isinstance(chapter_entries, list):
        chapter_entries = []

    normalized_status = str(status or "all").strip().lower()
    normalized_keyword = str(keyword or "").strip().lower()
    db_rows: dict[int, dict[str, Any]] = {}
    try:
        import sqlite3
        from app.config import DB_PATH
        from app.storage.db import initialize_database

        initialize_database(DB_PATH)
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            for row in conn.execute(
                """
                SELECT chapter_id, chapter_index, title, status, content_length,
                       source_word_count, preview_only, last_processed_at, updated_at,
                       error
                FROM aggregate_chapter_tasks
                WHERE aggregate_book_id = ?
                """,
                (book_id,),
            ).fetchall():
                db_rows[int(row["chapter_index"] or 0)] = dict(row)
    except Exception:
        db_rows = {}

    items: list[dict[str, Any]] = []
    entries_by_index = {
        int(entry.get("index", 0) or 0): entry
        for entry in chapter_entries
        if isinstance(entry, dict) and int(entry.get("index", 0) or 0) > 0
    }
    for chapter_index in sorted(set(entries_by_index) | set(db_rows)):
        entry = entries_by_index.get(chapter_index, {})
        db_row = db_rows.get(chapter_index, {})
        if not isinstance(entry, dict):
            entry = {}
        if not isinstance(db_row, dict):
            db_row = {}
        if not entry and not db_row:
            continue
        task_status = str(db_row.get("status") or "pending")
        chapter_status = str(entry.get("status") or task_status)
        chapter_title = str(entry.get("title") or db_row.get("title") or "")
        if normalized_keyword and normalized_keyword not in chapter_title.lower():
            continue

        file_name = str(entry.get("file", "") or "").strip()
        trace = {}
        content_length = int(db_row.get("content_length") or 0)
        has_content = False
        preview_only = bool(db_row.get("preview_only") or False)
        source_word_count = int(db_row.get("source_word_count") or 0)
        processed_at = str(db_row.get("last_processed_at") or db_row.get("updated_at") or "")
        if file_name:
            chapter_path = storage.shared_book_dir(book_name=book_name, author=author) / file_name
            if chapter_path.exists():
                markdown = chapter_path.read_text(encoding="utf-8")
                has_content = True
                body, _, _ = markdown.partition(f"<!-- {TRACE_BEGIN}")
                content_length = content_length or len(body.replace(f"# {chapter_title}", "", 1).strip())
                try:
                    trace = storage.parse_trace_block(markdown)
                except ValueError:
                    trace = {}
        preview_only = preview_only or bool(trace.get("previewOnly", False))
        if preview_only and chapter_status == "supplemented":
            chapter_status = "fetched"
        if normalized_status != "all" and chapter_status != normalized_status:
            continue
        source_word_count = source_word_count or int(trace.get("sourceWordCount", 0) or 0)
        processed_at = processed_at or str(trace.get("processedAt", "") or "")
        is_vip = bool(entry.get("isVip", False))
        source_id = str(
            entry.get("sourceId")
            or trace.get("fallbackSourceId")
            or ((trace.get("supplementSource") or {}).get("sourceId") if isinstance(trace.get("supplementSource"), dict) else "")
            or trace.get("primarySourceId")
            or ((trace.get("primarySource") or {}).get("sourceId") if isinstance(trace.get("primarySource"), dict) else "")
            or ""
        )
        aligned_with = str(entry.get("alignedWith") or trace.get("alignedWith") or trace.get("selectedContentSource") or "")
        task_db_chapter_id = str(db_row.get("chapter_id") or "")
        if task_db_chapter_id:
            read_chapter_id = task_db_chapter_id
        elif entry.get("sourceChapterId"):
            source_chapter_id = str(entry["sourceChapterId"])
            agg_url = make_aggregate_chapter_url(
                aggregate_book_id=book_id,
                source_chapter_id=source_chapter_id,
                title=chapter_title,
                index=chapter_index,
            )
            read_chapter_id = encode_chapter_id(VIRTUAL_SOURCE_ID, agg_url)
        else:
            read_chapter_id = ""
        items.append(
            {
                "chapterId": str(chapter_index),
                "taskChapterId": task_db_chapter_id,
                "sourceChapterId": str(entry.get("sourceChapterId") or ""),
                "readChapterId": read_chapter_id,
                "chapterIndex": chapter_index,
                "title": chapter_title,
                "status": chapter_status,
                "taskStatus": task_status,
                "sourceId": source_id,
                "alignedWith": aligned_with,
                "placeholder": False,
                "contentLength": content_length,
                "hasContent": has_content,
                "processedAt": processed_at,
                "sourceWordCount": source_word_count,
                "previewOnly": preview_only,
                "isVip": is_vip,
                "file": file_name or None,
                "error": db_row.get("error") or "",
            }
        )

    total = len(items)
    offset = (page - 1) * page_size
    return {"items": items[offset: offset + page_size], "page": page, "pageSize": page_size, "total": total}


def _list_library_book_logs(book_id: str, *, limit: int = 50, offset: int = 0, admin_view: bool = False) -> dict:
    from app.services.library_books import library_books_service
    from app.services.shared_book_runtime import SharedBookProcessLogger

    book = library_books_service.get_book(book_id)
    if not book:
        return {"bookId": book_id, "items": [], "limit": limit, "offset": offset, "total": 0}

    logger = SharedBookProcessLogger(library_books_service.shared_book_storage)
    result = logger.read(
        book_name=str(book.get("name", "") or ""),
        author=str(book.get("author", "") or ""),
        limit=limit,
        offset=offset,
    )
    return {
        "bookId": book_id,
        "items": result["items"],
        "limit": result["limit"],
        "offset": result["offset"],
        "total": result["total"],
    }


def _load_library_book_chapter_progress(book_id: str, chapter_id: str) -> dict:
    from app.services.library_books import library_books_service
    from app.services.shared_book_runtime import SharedBookProcessLogger, build_chapter_progress_payload

    book = library_books_service.get_book(book_id)
    if not book:
        return {"bookId": book_id, "chapterId": chapter_id, "found": False}

    book_name = str(book.get("name", "") or "").strip()
    author = str(book.get("author", "") or "").strip()
    storage = library_books_service.shared_book_storage

    chapter_index_path = storage.chapter_index_path(book_name=book_name, author=author)
    chapter_index = storage._read_json(chapter_index_path) or {"chapters": []}
    target_entry: dict[str, Any] | None = None
    for entry in chapter_index.get("chapters", []):
        if not isinstance(entry, dict):
            continue
        if entry.get("index") == int(chapter_id) or str(entry.get("index", "")) == chapter_id:
            target_entry = entry
            break

    if target_entry is None:
        return {"bookId": book_id, "chapterId": chapter_id, "found": False}

    chapter_index_value = int(target_entry.get("index", 0) or 0)
    chapter_title = str(target_entry.get("title", "") or "").strip()
    file_name = str(target_entry.get("file", "") or "").strip()
    chapter_path = storage.shared_book_dir(book_name=book_name, author=author) / file_name if file_name else None

    trace: dict[str, Any] | None = None
    markdown = ""
    if chapter_path and chapter_path.exists():
        try:
            markdown = chapter_path.read_text(encoding="utf-8")
            trace = storage.parse_trace_block(markdown)
        except Exception:
            trace = None

    logger = SharedBookProcessLogger(storage)
    logs = logger.read(
        book_name=book_name,
        author=author,
        chapter_index=chapter_index_value,
        limit=20,
    )["items"]

    payload = build_chapter_progress_payload(
        book_id=book_id,
        chapter_index=chapter_index_value,
        chapter_title=chapter_title,
        chapter_trace=trace,
        logs=logs,
    )
    trace_summary = {
        **(trace or {}),
        "stage": payload.get("stage", ""),
        "currentStep": payload.get("currentStep", ""),
        "nextStep": payload.get("nextStep", ""),
        "currentChapterIndex": payload.get("currentChapterIndex"),
        "currentChapterTitle": payload.get("currentChapterTitle", ""),
        "nextChapterIndex": payload.get("nextChapterIndex"),
        "nextChapterTitle": payload.get("nextChapterTitle", ""),
    }
    body = markdown.partition(f"<!-- {TRACE_BEGIN}")[0]
    content_length = len(body.replace(f"# {chapter_title}", "", 1).strip()) if body else 0
    payload.update(
        {
            "found": True,
            "chapterId": chapter_id,
            "title": chapter_title,
            "status": payload.get("chapterStatus", "pending"),
            "previewOnly": bool((trace or {}).get("previewOnly", False)),
            "contentLength": content_length,
            "sourceWordCount": int((trace or {}).get("sourceWordCount", 0) or 0),
            "traceSummary": trace_summary,
        }
    )
    return _sanitize_chapter_progress_payload(payload)


def _reprocess_library_book_chapter(book_id: str, chapter_id: str) -> dict:
    import sqlite3

    from app.services.aggregate_processor import AggregateProcessor
    from app.services.library_books import library_books_service

    book = library_books_service.get_book(book_id)
    if not book:
        return {"ok": False, "bookId": book_id, "chapterId": chapter_id, "error": "书籍不存在"}

    book_name = str(book.get("name", "") or "").strip()
    author = str(book.get("author", "") or "").strip()
    storage = library_books_service.shared_book_storage
    chapter_index_path = storage.chapter_index_path(book_name=book_name, author=author)
    chapter_index = storage._read_json(chapter_index_path) or {"chapters": []}
    target_entry = None
    for entry in chapter_index.get("chapters", []):
        if not isinstance(entry, dict):
            continue
        if entry.get("index") == int(chapter_id) or str(entry.get("index", "")) == chapter_id:
            target_entry = entry
            break
    if target_entry is None:
        return {"ok": False, "bookId": book_id, "chapterId": chapter_id, "error": "章节不存在"}

    processor = AggregateProcessor()
    chapter_row = None
    with processor._conn() as conn:
        conn.row_factory = sqlite3.Row
        chapter_row = conn.execute(
            """
            SELECT chapter_id, source_chapter_id, chapter_index, title, aggregate_book_id
            FROM aggregate_chapter_tasks
            WHERE aggregate_book_id = ? AND chapter_index = ?
            """,
            (book_id, int(target_entry.get("index", 0) or 0)),
        ).fetchone()
    if not chapter_row:
        return {"ok": False, "bookId": book_id, "chapterId": chapter_id, "error": "章节任务不存在"}

    from app.services.book_catalog import BookCatalog

    catalog = BookCatalog()
    result = asyncio.run(processor._process_chapter(catalog, dict(chapter_row)))
    return {"ok": True, "bookId": book_id, "chapterId": chapter_id, "result": result}


def _sanitize_chapter_progress_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Remove private source URLs from shared chapter progress payload."""
    payload = dict(payload)
    trace = payload.get("chapterTrace") or payload.get("traceSummary") or {}
    if isinstance(trace, dict):
        trace.pop("primarySourceUrl", None)
        trace.pop("primarySourceChapterUrl", None)
        primary = trace.get("primarySource")
        if isinstance(primary, dict):
            primary.pop("chapterUrl", None)
            primary.pop("bookUrl", None)
            primary.pop("tocUrl", None)
        supplement = trace.get("supplementSource")
        if isinstance(supplement, dict):
            supplement.pop("chapterUrl", None)
            supplement.pop("bookUrl", None)
            supplement.pop("tocUrl", None)
    sanitized_logs = []
    for log in payload.get("logs", []):
        if not isinstance(log, dict):
            sanitized_logs.append(log)
            continue
        safe_log = dict(log)
        safe_payload = safe_log.get("payload")
        if isinstance(safe_payload, dict):
            for key in list(safe_payload.keys()):
                if "url" in key.lower() or "chapterurl" in key.lower():
                    safe_payload.pop(key, None)
        sanitized_logs.append(safe_log)
    payload["logs"] = sanitized_logs
    return payload


def _build_chapter_runtime_summary(book_id: str, chapter_index: int, chapter_title: str) -> dict[str, Any]:
    from app.services.library_books import library_books_service
    from app.services.shared_book_runtime import SharedBookProcessLogger

    book = library_books_service.get_book(book_id)
    if not book:
        return {}
    storage = library_books_service.shared_book_storage
    logger = SharedBookProcessLogger(storage)
    logs = logger.read(
        book_name=str(book.get("name", "") or ""),
        author=str(book.get("author", "") or ""),
        chapter_index=chapter_index,
        limit=20,
    )["items"]
    last_event = logs[-1] if logs else {}
    next_step = "等待下一轮调度"
    current_step = "待处理"
    stage = ""
    if isinstance(last_event, dict):
        event = str(last_event.get("event", "") or "")
        stage = str(last_event.get("stage", "") or "")
        payload = last_event.get("payload") if isinstance(last_event.get("payload"), dict) else {}
        title = str(payload.get("title", "") or chapter_title).strip()
        status = str(payload.get("status", "") or "").strip()
        if event == "job_start":
            trigger = str((last_event.get("payload") or {}).get("trigger", "") or "")
            if trigger == "book_source_map_refresh":
                current_step = "正在刷新源映射"
                next_step = "继续处理章节队列"
            elif trigger == "book_bootstrap":
                current_step = "正在补齐首批章节"
                next_step = "继续处理下一章"
            elif trigger == "book_update_check":
                current_step = "正在检查更新"
                next_step = "继续处理下一章"
            else:
                current_step = "正在处理章节"
                next_step = "继续处理下一章"
        elif event == "job_complete":
            current_step = "章节处理完成"
            next_step = "等待下一轮调度"
        elif event == "job_error":
            current_step = "处理失败"
            next_step = "等待修复后重试"
        elif event == "job_skipped":
            current_step = "处理被跳过"
            next_step = "等待锁释放"
        elif event == "chapter_write":
            current_step = f"已写入 {title or chapter_title}"
            next_step = "继续处理下一章"
        elif event == "chapter_error":
            current_step = f"{title or chapter_title} 处理失败"
            next_step = "等待重试"
        elif status:
            current_step = status
    return {
        "stage": stage,
        "currentStep": current_step,
        "nextStep": next_step,
        "currentChapterIndex": chapter_index,
        "currentChapterTitle": chapter_title,
        "nextChapterIndex": chapter_index + 1,
        "nextChapterTitle": "",
    }


def _library_processing_event_message(record: dict[str, Any]) -> str:
    payload = record.get("payload") if isinstance(record.get("payload"), dict) else {}
    step = str(payload.get("step", "") or "").strip()
    if step:
        return step
    labels = {
        "chapter_start": "开始处理章节",
        "official_fetch_start": "正在拉取官方源",
        "official_fetch_complete": "官方源拉取完成",
        "official_snapshot_used": "使用官方源本地快照",
        "official_classified": "官方源内容识别完成",
        "candidate_discovery_start": "正在准备候选源",
        "candidate_merge_start": "正在从候选源补全章节",
        "candidate_toc_start": "正在加载候选源目录",
        "candidate_toc_complete": "候选源目录加载完成",
        "candidate_toc_error": "候选源目录加载失败",
        "candidate_source_behind_target": "候选源最新章节落后，已跳过",
        "candidate_toc_behind_target": "候选源目录落后，已跳过",
        "candidate_no_match": "候选源没有匹配章节",
        "candidate_match_found": "候选源找到可能章节",
        "candidate_fetch_start": "正在拉取候选章节",
        "candidate_fetch_error": "候选章节拉取失败",
        "candidate_snapshot_used": "使用候选源本地快照",
        "candidate_rejected": "候选章节未通过",
        "candidate_accepted": "候选章节通过校验",
        "candidate_selected": "候选源补全成功",
        "candidate_all_failed": "所有候选源都未补全",
        "candidate_none": "没有可用候选源",
        "initial_preview_recheck_scheduled": "正在安排预览章节复查",
        "preview_fallback": "写入官方预览内容",
        "ai_proofread_start": "正在校对正文",
        "ai_proofread_complete": "AI 校对完成",
        "ai_proofread_error": "AI 校对失败",
        "ai_deferred": "AI 校对暂缓",
        "chapter_write_start": "正在写入处理结果",
        "chapter_write": "章节写入完成",
        "chapter_error": "章节处理失败",
        "source_snapshot_start": "开始下载第三方源",
        "source_snapshot_toc_complete": "第三方源目录加载完成",
        "source_snapshot_progress": "正在下载第三方章节",
        "source_snapshot_complete": "第三方源下载完成",
        "source_snapshot_error": "第三方源下载失败",
        "job_timeout": "订阅任务超时，等待重试",
    }
    return labels.get(str(record.get("event", "") or ""), str(record.get("event", "") or ""))


def _sanitize_library_processing_event(record: dict[str, Any]) -> dict[str, Any]:
    payload = record.get("payload") if isinstance(record.get("payload"), dict) else {}
    return {
        "ts": record.get("ts", "") or "",
        "event": record.get("event", "") or "",
        "stage": record.get("stage", "") or "",
        "chapterIndex": record.get("chapterIndex"),
        "title": payload.get("title", "") or "",
        "message": _library_processing_event_message(record),
        "sourceId": (
            payload.get("sourceId", "")
            or payload.get("primarySourceId", "")
            or payload.get("supplementSourceId", "")
            or ""
        ),
        "status": payload.get("status", "") or "",
        "classification": payload.get("classification", "") or "",
        "reason": payload.get("reason", "") or record.get("errorCode", "") or "",
        "error": record.get("errorMessage", "") or "",
        "contentLength": int(payload.get("contentLength", 0) or 0),
        "aiModel": payload.get("aiModel", "") or "",
        "aiTokens": int(payload.get("aiTokens", 0) or 0),
        "targetChapterNumber": payload.get("targetChapterNumber"),
        "latestCandidateChapterNumber": payload.get("latestCandidateChapterNumber"),
        "attemptedSourceIds": payload.get("attemptedSourceIds", []),
    }


async def _manual_source_map_refresh(book_id: str, payload: dict | None = None) -> dict:
    book = library_books_service.get_book(book_id)
    if not book:
        return {"ok": False, "bookId": book_id, "error": "书籍不存在"}
    scheduler = SharedBookScheduler()
    result = await scheduler.run_source_map_refresh_now(
        book_id,
        payload=library_books_service.load_payload(book_id),
        force=True if payload is None else bool(payload.get("force", True)),
    )
    return {"ok": bool(result.get("success")), "bookId": book_id, "result": result}


def _manual_library_book_repair(book_id: str, payload: dict | None = None) -> dict:
    del payload
    book = library_books_service.get_book(book_id)
    if not book:
        return {"ok": False, "bookId": book_id, "error": "书籍不存在"}
    book_name = str(book.get("name", "") or "").strip()
    author = str(book.get("author", "") or "").strip()
    storage = library_books_service.shared_book_storage
    metadata_path = storage.metadata_path(book_name=book_name, author=author)
    chapter_index_path = storage.chapter_index_path(book_name=book_name, author=author)
    if not metadata_path.exists() or not chapter_index_path.exists():
        return {"ok": False, "bookId": book_id, "error": "shared_metadata_missing"}
    metadata_payload = _read_json(metadata_path, {})
    chapter_index_payload = _read_json(chapter_index_path, {})
    chapter_traces = {}
    for item in chapter_index_payload.get("chapters", []) if isinstance(chapter_index_payload, dict) else []:
        if not isinstance(item, dict):
            continue
        file_name = str(item.get("file", "") or "").strip()
        if not file_name:
            continue
        chapter_path = metadata_path.parent / file_name
        if not chapter_path.exists():
            continue
        try:
            trace = storage.parse_trace_block(chapter_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        chapter_index = int(trace.get("chapterIndex", 0) or 0)
        if chapter_index > 0:
            chapter_traces[chapter_index] = trace
    repaired = storage.rebuild_metadata_summary(
        metadata_payload,
        chapter_index_payload=chapter_index_payload if isinstance(chapter_index_payload, dict) else {},
        chapter_traces=chapter_traces,
    )
    storage.atomic_write_json(metadata_path, repaired)
    return {
        "ok": True,
        "bookId": book_id,
        "bookState": repaired.get("bookState", {}),
        "sourceMapSummary": _sanitize_source_map_summary(repaired.get("sourceMapSummary", [])),
    }


def _manual_library_integrity_repair(payload: dict | None = None) -> dict:
    requested = (payload or {}).get("bookIds")
    book_ids = (
        {str(book_id).strip() for book_id in requested if str(book_id).strip()}
        if isinstance(requested, list)
        else None
    )
    before = library_books_service.scan_integrity(book_ids=book_ids)
    processor = AggregateProcessor()
    results = []
    local_rebuild_codes = {
        "metadata_missing",
        "metadata_invalid",
        "chapter_index_missing",
        "chapter_index_invalid",
        "chapter_index_count_mismatch",
        "chapter_files_missing",
        "chapter_traces_invalid",
        "chapter_paths_invalid",
        "source_snapshot_files_missing",
        "source_snapshot_files_invalid",
        "source_snapshot_manifests_missing",
        "source_snapshot_manifests_invalid",
    }
    for item in before["books"]:
        if item["status"] == "healthy" or not item.get("repairable"):
            continue
        book_id = str(item["bookId"])
        book = library_books_service.get_book(book_id) or {}
        actions: list[str] = []
        removed_tmp = library_books_service.shared_book_storage.cleanup_tmp_files(
            book_name=str(book.get("name", "") or ""),
            author=str(book.get("author", "") or ""),
        )
        if removed_tmp:
            actions.append("cleanup_tmp")

        issue_codes = {str(issue.get("code", "") or "") for issue in item.get("issues", [])}
        if issue_codes & local_rebuild_codes:
            rebuilt = processor.rebuild_book_from_snapshots(book_id)
            if rebuilt.get("rebuilt") and (
                int(rebuilt.get("snapshotCount", 0) or 0) > 0
                or int(rebuilt.get("rewrittenChapters", 0) or 0) > 0
            ):
                actions.append("rebuild_from_snapshots")

        repaired_metadata = _manual_library_book_repair(book_id)
        if repaired_metadata.get("ok"):
            actions.append("rebuild_metadata")

        current = library_books_service.scan_integrity(book_ids={book_id})
        current_book = current["books"][0] if current["books"] else item
        queued = False
        if current_book["status"] != "healthy" and str(book.get("status", "")) not in {"paused", "archived"}:
            aggregate_payload = library_books_service.load_payload(book_id)
            if aggregate_payload:
                queued = bool(processor.enqueue_book(book_id, aggregate_payload).get("queued"))
                if queued:
                    actions.append("queue_recovery")
        results.append(
            {
                "bookId": book_id,
                "actions": actions,
                "queued": queued,
                "status": current_book["status"],
            }
        )

    after = library_books_service.scan_integrity(book_ids=book_ids)
    return {
        "ok": True,
        "before": before["summary"],
        "after": after,
        "repairedBooks": len(results),
        "queuedBooks": sum(bool(item["queued"]) for item in results),
        "results": results,
    }


async def _manual_library_book_update_check(book_id: str) -> dict:
    payload = library_books_service.load_payload(book_id)
    if not payload:
        return {"bookId": book_id, "success": False, "error": "书籍不存在或没有订阅载荷"}
    processor = AggregateProcessor()
    processor.enqueue_book(book_id, payload)
    asyncio.create_task(processor.run_book_task(book_id))
    return {"bookId": book_id, "success": True, "queued": True}


# ---- Plugins ----

_plugin_loader = PluginLoader()
_plugin_scheduler = get_plugin_scheduler()


def _plugin_access_type(plugin) -> str:
    browser_mode = (plugin.metadata.browser or {}).get("mode", "none")
    return "Browser" if browser_mode == "required" else "HTTP"


def _plugin_source_type(plugin) -> str:
    return _plugin_access_type(plugin)


def _plugin_last_modified(plugin) -> str:
    return getattr(plugin.source, "last_modified", "") if plugin.source else ""


def _plugin_health(plugin_id: str) -> dict:
    from app.services.plugin_runtime_state import get_runtime_state

    state = get_runtime_state().get_state(plugin_id)
    last_ping = state.get("lastPing") or {}
    last_error = state.get("lastError") or {}
    return {
        "pingStatus": last_ping.get("status", "unknown"),
        "pingLatencyMs": last_ping.get("latencyMs", 0),
        "pingTimestamp": last_ping.get("timestamp"),
        "lastError": last_error.get("message", ""),
        "lastErrorTimestamp": last_error.get("timestamp"),
    }


@console_route("get", "/plugins")
def list_plugins():
    plugins = _plugin_scheduler._plugins
    return {
        "items": [
            {
                "pluginId": p.metadata.id,
                "name": p.metadata.name,
                "author": p.metadata.author,
                "version": p.metadata.version,
                "enabled": p.metadata.enabled,
                "official": p.metadata.is_official_source(),
                "capabilities": p.capabilities,
                "domains": p.metadata.domains,
                "tags": p.metadata.tags,
                "auth": p.metadata.auth,
                "content": p.metadata.content,
                "accessType": _plugin_access_type(p),
                "sourceType": _plugin_source_type(p),
                "proxyRequired": bool((p.metadata.proxy or {}).get("required")),
                "proxyMode": (p.metadata.proxy or {}).get("mode", "auto"),
                "browser": p.metadata.browser,
                "lastModified": _plugin_last_modified(p),
                "health": _plugin_health(p.metadata.id),
            }
            for p in plugins.values()
        ],
        "total": len(plugins),
    }


@console_route("get", "/official-sources")
async def list_official_sources(request: Request):
    auth_service.require_admin(request)
    plugins = _plugin_scheduler._plugins
    cookie_store = CookieStore()
    items = []
    for plugin in plugins.values():
        auth_mode = (plugin.metadata.auth or {}).get("mode", "none")
        if not plugin.metadata.is_official_source() and auth_mode == "none":
            continue
        payload = cookie_store.load(plugin.metadata.id)
        has_cookies = bool(payload) and (
            bool(payload.get("cookies")) if isinstance(payload, dict) else True
        )
        auth_status = {
            "authenticated": False,
            "authStatus": "anonymous" if not has_cookies else "unknown",
            "accountName": "",
            "message": "",
            "requiredActions": ["check_auth_status"] if has_cookies else [],
            "hasCookies": has_cookies,
            "cookieDomains": sorted((payload.get("cookies") or {}).keys()) if isinstance(payload, dict) and isinstance(payload.get("cookies"), dict) else [],
        }
        if "auth" in plugin.capabilities:
            ctx = _plugin_scheduler._make_ctx(plugin.metadata.id)
            try:
                result = _normalize_auth_identity(await _plugin_scheduler._call_plugin(
                    plugin,
                    lambda: plugin.source.auth_status(ctx),
                    timeout=None,
                ))
                auth_status = {
                    **auth_status,
                    **result,
                    "hasCookies": has_cookies,
                    "cookieDomains": auth_status["cookieDomains"],
                }
            except Exception as exc:
                auth_status = {
                    **auth_status,
                    "message": str(exc),
                }
            finally:
                await ctx._fetcher.close()

        items.append({
            "pluginId": plugin.metadata.id,
            "name": plugin.metadata.name,
            "author": plugin.metadata.author,
            "version": plugin.metadata.version,
            "enabled": plugin.metadata.enabled,
            "domains": plugin.metadata.domains,
            "baseUrls": plugin.metadata.base_urls,
            "tags": plugin.metadata.tags,
            "auth": plugin.metadata.auth,
            "content": plugin.metadata.content,
            "lastModified": _plugin_last_modified(plugin),
            "browser": plugin.metadata.browser,
            "official": plugin.metadata.is_official_source(),
            "hasCookies": has_cookies,
            "authStatus": auth_status,
        })
    items.sort(key=lambda item: (not item["official"], item["name"], item["pluginId"]))
    return {"items": items, "total": len(items)}


@console_route("get", "/plugins/{plugin_id}")
def get_plugin(plugin_id: str):
    plugin = _plugin_scheduler._plugins.get(plugin_id)
    if not plugin:
        raise HTTPException(status_code=404, detail="插件不存在")
    return {
        "pluginId": plugin.metadata.id,
        "name": plugin.metadata.name,
        "author": plugin.metadata.author,
        "version": plugin.metadata.version,
        "enabled": plugin.metadata.enabled,
        "official": plugin.metadata.is_official_source(),
        "capabilities": plugin.capabilities,
        "domains": plugin.metadata.domains,
        "baseUrls": plugin.metadata.base_urls,
        "tags": plugin.metadata.tags,
        "auth": plugin.metadata.auth,
        "content": plugin.metadata.content,
        "accessType": _plugin_access_type(plugin),
        "sourceType": _plugin_source_type(plugin),
        "proxyRequired": bool((plugin.metadata.proxy or {}).get("required")),
        "proxyMode": (plugin.metadata.proxy or {}).get("mode", "auto"),
        "browser": plugin.metadata.browser,
        "rateLimit": plugin.metadata.rate_limit,
        "proxy": plugin.metadata.proxy,
        "sourceSeed": plugin.metadata.source_seed,
        "lastModified": _plugin_last_modified(plugin),
        "health": _plugin_health(plugin_id),
    }


@console_route("get", "/plugins/{plugin_id}/attempts")
def get_plugin_attempts(plugin_id: str, limit: int = 20):
    plugin = _plugin_scheduler._plugins.get(plugin_id)
    if not plugin:
        raise HTTPException(status_code=404, detail="插件不存在")
    from app.services.plugin_runtime_state import get_runtime_state

    attempts = get_runtime_state().get_attempts(plugin_id, limit=limit)
    return {"pluginId": plugin_id, "attempts": attempts}


@console_route("post", "/plugins/reload")
def reload_plugins():
    _plugin_scheduler.reload()
    return {"reloaded": True, "count": len(_plugin_scheduler._plugins)}


@console_route("post", "/plugins/{plugin_id}/enable")
def enable_plugin(plugin_id: str, payload: dict):
    plugin = _plugin_scheduler._plugins.get(plugin_id)
    if not plugin:
        raise HTTPException(status_code=404, detail="插件不存在")
    enabled = payload.get("enabled", True)
    plugin.metadata.enabled = enabled
    AppConfig.get().set_plugin_enabled(plugin_id, enabled)
    return {"pluginId": plugin_id, "enabled": enabled}


@console_route("post", "/plugins/batch-enable")
def batch_enable_plugins(payload: dict):
    plugin_ids = payload.get("pluginIds", [])
    enabled = payload.get("enabled", True)
    cfg = AppConfig.get()
    results = []
    for plugin_id in plugin_ids:
        plugin = _plugin_scheduler._plugins.get(plugin_id)
        if plugin:
            plugin.metadata.enabled = enabled
            cfg.set_plugin_enabled(plugin_id, enabled)
            results.append({"pluginId": plugin_id, "enabled": enabled})
        else:
            results.append({"pluginId": plugin_id, "error": "插件不存在"})
    return {"results": results}


@console_route("post", "/plugins/ping")
async def ping_all_plugins(payload: dict | None = None):
    payload = payload or {}
    plugin_ids = payload.get("pluginIds")
    if plugin_ids:
        plugin_ids = [pid for pid in plugin_ids if pid in _plugin_scheduler._plugins]
    service = SourcePingService(scheduler=_plugin_scheduler)
    results = await service.ping_all(plugin_ids)
    return {"results": results}


@console_route("post", "/plugins/{plugin_id}/ping")
async def ping_one_plugin(plugin_id: str):
    if plugin_id not in _plugin_scheduler._plugins:
        raise HTTPException(status_code=404, detail="插件不存在")
    service = SourcePingService(scheduler=_plugin_scheduler)
    result = await service.ping_one(plugin_id)
    return result


@console_route("get", "/plugins/{plugin_id}/auth")
async def get_plugin_auth(plugin_id: str, request: Request):
    auth_service.require_admin(request)
    plugin = _plugin_scheduler._plugins.get(plugin_id)
    if not plugin:
        raise HTTPException(status_code=404, detail="插件不存在")
    cookie_store = CookieStore()
    payload = cookie_store.load(plugin_id)
    has_cookies = bool(payload) and (
        bool(payload.get("cookies")) if isinstance(payload, dict) else True
    )
    cookie_domains: list[str] = []
    if isinstance(payload, dict):
        cookies = payload.get("cookies")
        if isinstance(cookies, dict):
            cookie_domains = sorted(cookies.keys())

    auth_meta = plugin.metadata.auth or {}
    if auth_meta.get("mode", "none") == "none":
        browser_meta = plugin.metadata.browser or {}
        if browser_meta.get("mode") == "required":
            return {
                "sourceId": plugin_id,
                "mode": "browser_bypass",
                "authenticated": False,
                "accountName": "",
                "expiresAt": "",
                "message": (
                    "已保存 Cookie，可用于后端模拟访问。"
                    if has_cookies
                    else "该插件无需账号登录；如遇 Cloudflare/浏览器挑战，后续按绕过策略处理，不再提供手动验证链路。"
                ),
                "requiredActions": ["retry_live_check"] if has_cookies else ["bypass_required"],
                "hasCookies": has_cookies,
                "cookieDomains": cookie_domains,
                "verificationStatus": "cookies_saved" if has_cookies else "bypass_required",
            }
        return {
            "sourceId": plugin_id,
            "mode": "none",
            "authenticated": False,
            "accountName": "",
            "expiresAt": "",
            "message": "该插件无需登录",
            "requiredActions": [],
            "hasCookies": has_cookies,
            "cookieDomains": cookie_domains,
        }
    if "auth" not in plugin.capabilities:
        return {
            "sourceId": plugin_id,
            "mode": auth_meta.get("mode", "optional"),
            "authenticated": False,
            "accountName": "",
            "expiresAt": "",
            "message": "该插件尚未实现登录检测方法",
            "requiredActions": ["manual_login"] if auth_meta.get("loginUrl") else [],
            "hasCookies": has_cookies,
            "cookieDomains": cookie_domains,
        }
    ctx = _plugin_scheduler._make_ctx(plugin_id)
    try:
        result = _normalize_auth_identity(await _plugin_scheduler._call_plugin(
            plugin,
            lambda: plugin.source.auth_status(ctx),
            timeout=None,
        ))
        result.setdefault("mode", auth_meta.get("mode", "optional"))
        if not result.get("authenticated") and has_cookies:
            result.setdefault("requiredActions", ["check_auth_status"])
            if not result.get("message"):
                result["message"] = "Cookie 已保存，但远程登录态校验未通过"
    except Exception as exc:
        result = {
            "sourceId": plugin_id,
            "mode": auth_meta.get("mode", "optional"),
            "authenticated": False,
            "accountName": "",
            "expiresAt": "",
            "message": str(exc),
            "requiredActions": ["check_auth_status"] if has_cookies else [],
            "hasCookies": has_cookies,
            "cookieDomains": cookie_domains,
        }
    finally:
        await ctx._fetcher.close()
    return result


@console_route("post", "/plugins/{plugin_id}/login")
async def prepare_plugin_login(plugin_id: str, request: Request):
    auth_service.require_admin(request)
    plugin = _plugin_scheduler._plugins.get(plugin_id)
    if not plugin:
        raise HTTPException(status_code=404, detail="插件不存在")
    if hasattr(plugin.source, "prepare_login") and callable(getattr(plugin.source, "prepare_login")):
        ctx = _plugin_scheduler._make_ctx(plugin_id)
        try:
            return await _plugin_scheduler._call_plugin(
                plugin,
                lambda: plugin.source.prepare_login(ctx),
                timeout=None,
            )
        finally:
            await ctx._fetcher.close()
    auth = plugin.metadata.auth
    login_url = auth.get("loginUrl", plugin.metadata.base_urls[0] if plugin.metadata.base_urls else "")
    cookie_domains = auth.get("cookieDomains", plugin.metadata.domains)
    return {
        "sourceId": plugin_id,
        "mode": "manual_browser",
        "loginUrl": login_url,
        "instructions": "在打开的浏览器中完成登录，然后回到后台点击检测登录状态。",
        "cookieDomains": cookie_domains,
    }


@console_route("post", "/plugins/{plugin_id}/auth/check")
async def check_plugin_auth(plugin_id: str, request: Request):
    return await get_plugin_auth(plugin_id, request)


@console_route("post", "/plugins/{plugin_id}/cookies/clear")
def clear_plugin_cookies(plugin_id: str, request: Request):
    admin = auth_service.require_admin(request)
    plugin = _plugin_scheduler._plugins.get(plugin_id)
    if not plugin:
        raise HTTPException(status_code=404, detail="插件不存在")
    ctx = _plugin_scheduler._make_ctx(plugin_id)
    ctx.cookies.clear()
    CookieStore().clear(plugin_id)
    audit_service.record(
        action="official_source.cookies.clear",
        actor_user_id=admin.user_id,
        actor_role=admin.role,
        target_type="official_source",
        target_id=plugin_id,
        source_id=plugin_id,
    )
    return {"cleared": True, "pluginId": plugin_id}


@console_route("post", "/plugins/{plugin_id}/login-browser")
async def start_login_browser(plugin_id: str, request: Request):
    """Launch a headed browser window for the user to complete manual login.

    The browser opens the plugin's configured login URL. The user interacts
    with the page (SMS code, captcha, etc.) manually. The backend polls for
    login-success indicators and extracts cookies automatically.
    """
    auth_service.require_admin(request)
    plugin = _plugin_scheduler._plugins.get(plugin_id)
    if not plugin:
        raise HTTPException(status_code=404, detail="插件不存在")

    # Resolve login URL and cookie domains (same logic as prepare_login)
    auth = plugin.metadata.auth or {}
    login_url = auth.get("loginUrl", "")
    if not login_url and plugin.metadata.base_urls:
        login_url = plugin.metadata.base_urls[0]
    cookie_domains = auth.get("cookieDomains", plugin.metadata.domains or ["qidian.com"])

    session = await login_browser_service.start(
        plugin_id=plugin_id,
        login_url=login_url or "https://passport.qidian.com/",
        cookie_domains=cookie_domains,
    )
    return {
        "pluginId": plugin_id,
        "status": "running" if session.status == "pending" else session.status,
        "message": session.message or "正在启动浏览器...",
    }


@console_route("get", "/plugins/{plugin_id}/login-browser/status")
async def get_login_browser_status(plugin_id: str, request: Request):
    """Poll the status of an active login-browser session."""
    admin = auth_service.require_admin(request)
    session = await login_browser_service.get(plugin_id)
    if not session:
        return {"pluginId": plugin_id, "status": "none", "message": "没有活跃的登录会话"}

    result = {
        "pluginId": plugin_id,
        "status": "running" if session.status == "pending" else session.status,
        "message": session.message,
        "authenticated": False,
        "accountName": "",
        "hasCookies": session.status == "success" and bool(session.cookies),
        "cookieDomains": list(session.cookies.keys()) if session.cookies else [],
    }

    # If completed, persist cookies and clean up
    if session.status in ("success", "failed", "timeout", "cancelled"):
        try:
            if session.status == "success" and session.cookies:
                probe = _normalize_auth_identity(
                    await official_auth_manager.save_cookies_and_probe(plugin_id, session.cookies)
                )
                account_name = _explicit_auth_identity(probe)
                authenticated = bool(probe.get("authenticated")) and bool(account_name)
                result.update({
                    "status": "success" if authenticated else "pending",
                    "message": probe.get("message") or session.message,
                    "authenticated": authenticated,
                    "accountName": account_name,
                    "authStatus": probe.get("authStatus", "unknown"),
                    "requiredActions": probe.get("requiredActions", []),
                })
            elif session.status == "success":
                result.update({"status": "failed", "message": "登录完成，但未提取到 Cookie"})
        except Exception as exc:
            result.update({"status": "failed", "message": f"登录态校验失败: {exc}"})
        finally:
            await login_browser_service.cleanup(plugin_id)
        _record_official_login_audit(
            admin,
            plugin_id,
            "browser",
            result=result,
            error_code="" if result.get("authenticated") else str(session.status or "failed"),
        )

    return result


@console_route("delete", "/plugins/{plugin_id}/login-browser")
async def cancel_login_browser(plugin_id: str, request: Request):
    """Cancel an active login-browser session."""
    auth_service.require_admin(request)
    ok = await login_browser_service.cancel(plugin_id)
    await login_browser_service.cleanup(plugin_id)
    return {"pluginId": plugin_id, "cancelled": ok}


# ---- Official Source Login (通用登录协议层) ----

@console_route("get", "/official-sources/{plugin_id}/login-capabilities")
def get_login_capabilities(plugin_id: str, request: Request):
    """Get available login methods for an official source."""
    auth_service.require_admin(request)
    return official_auth_manager.capabilities(plugin_id)


@console_route("post", "/official-sources/{plugin_id}/login/phone/request-code")
async def official_login_phone_request(plugin_id: str, payload: dict, request: Request):
    """Step 1 of phone login: request SMS verification code.

    Payload: {"phone": "13800138000", "sessionId": "", "challengeToken": "", "challengeRandstr": ""}
    Full payload is forwarded to private auth_api, including challenge params.
    """
    auth_service.require_admin(request)
    if not payload.get("phone"):
        raise HTTPException(status_code=400, detail="缺少手机号")
    return official_auth_manager.request_phone_code(plugin_id, payload)


@console_route("post", "/official-sources/{plugin_id}/login/phone/verify")
async def official_login_phone_verify(plugin_id: str, payload: dict, request: Request):
    """Step 2 of phone login: verify SMS code and complete login.

    Payload: {"sessionId": "xxx", "phone": "13800138000", "code": "123456", "challengeToken": ""}
    """
    admin = auth_service.require_admin(request)
    try:
        result = _normalize_auth_identity(
            await official_auth_manager.verify_phone_code(plugin_id, payload)
        )
    except Exception as exc:
        _record_official_login_audit(
            admin, plugin_id, "phone", error_code=type(exc).__name__
        )
        raise
    _record_official_login_audit(admin, plugin_id, "phone", result=result)
    return result


@console_route("post", "/official-sources/{plugin_id}/login/cookie/verify")
async def official_login_cookie_verify(plugin_id: str, payload: dict, request: Request):
    """Verify pasted cookies for an official source.

    Payload: {"cookieText": "ywguid=...; ywkey=..."}
    """
    admin = auth_service.require_admin(request)
    cookie_text = payload.get("cookieText", "")
    if not cookie_text:
        raise HTTPException(status_code=400, detail="缺少 Cookie 文本")
    try:
        result = _normalize_auth_identity(
            await official_auth_manager.verify_cookie(plugin_id, cookie_text)
        )
    except Exception as exc:
        _record_official_login_audit(
            admin, plugin_id, "cookie", error_code=type(exc).__name__
        )
        raise
    _record_official_login_audit(admin, plugin_id, "cookie", result=result)
    return result


@console_route("post", "/official-sources/{plugin_id}/login/logout")
async def official_login_logout(plugin_id: str, request: Request):
    """Clear auth state for an official source."""
    admin = auth_service.require_admin(request)
    result = official_auth_manager.logout(plugin_id)
    audit_service.record(
        action="official_source.logout",
        actor_user_id=admin.user_id,
        actor_role=admin.role,
        target_type="official_source",
        target_id=plugin_id,
        source_id=plugin_id,
    )
    return result


@console_route("get", "/official-sources/{plugin_id}/login/debug-trace")
def official_login_debug_trace(plugin_id: str, request: Request):
    """Return recent login step traces for an official source.

    Sensitive request and result values are redacted at the trace-store boundary.
    """
    auth_service.require_admin(request)
    from app.services.official_auth.sessions import login_trace_store
    from app.services.official_auth.manager import official_auth_manager

    return {
        "pluginId": plugin_id,
        "traces": login_trace_store.get(plugin_id),
        "session": _get_active_login_session(plugin_id),
    }


def _get_active_login_session(plugin_id: str) -> dict | None:
    """Best-effort snapshot of the most recent active login session."""
    from app.services.official_auth.sessions import session_store

    # Find the most recent non-expired session for this plugin.
    candidate = None
    for session in list(session_store._sessions.values()):
        if session.plugin_id == plugin_id and not session.expired():
            if candidate is None or session.created_at > candidate.created_at:
                candidate = session
    return candidate.to_dict() if candidate else None


# ---- Sources ----

@console_route("get", "/sources")
def list_sources(
    enabled_only: bool = False,
    limit: int = 100,
    offset: int = 0,
):
    all_plugins = list(_plugin_scheduler._plugins.values())
    if enabled_only:
        all_plugins = [p for p in all_plugins if p.metadata.enabled]
    total = len(all_plugins)
    page_plugins = all_plugins[offset : offset + limit]
    items = []
    for plugin in page_plugins:
        items.append({
            "pluginId": plugin.metadata.id,
            "name": plugin.metadata.name,
            "author": plugin.metadata.author,
            "version": plugin.metadata.version,
            "enabled": plugin.metadata.enabled,
            "official": plugin.metadata.is_official_source(),
            "capabilities": plugin.capabilities,
            "domains": plugin.metadata.domains,
            "baseUrls": plugin.metadata.base_urls,
            "tags": plugin.metadata.tags,
            "auth": plugin.metadata.auth,
            "content": plugin.metadata.content,
            "proxyMode": (plugin.metadata.proxy or {}).get("mode", "auto"),
            "proxyRequired": bool((plugin.metadata.proxy or {}).get("required")),
            "browser": plugin.metadata.browser,
            "lastModified": _plugin_last_modified(plugin),
        })
    stats = {
        "total": len(_plugin_scheduler._plugins),
        "enabled": sum(1 for p in _plugin_scheduler._plugins.values() if p.metadata.enabled),
        "filtered": total,
    }
    return {"items": items, "limit": limit, "offset": offset, "total": total, "stats": stats}


@console_route("get", "/sources/{source_id}")
def get_source(source_id: str):
    plugin = _plugin_scheduler._plugins.get(source_id)
    if not plugin:
        raise HTTPException(status_code=404, detail="书源不存在")
    return {
        "source": {
            "pluginId": plugin.metadata.id,
            "name": plugin.metadata.name,
            "author": plugin.metadata.author,
            "version": plugin.metadata.version,
            "enabled": plugin.metadata.enabled,
            "official": plugin.metadata.is_official_source(),
            "capabilities": plugin.capabilities,
            "domains": plugin.metadata.domains,
            "baseUrls": plugin.metadata.base_urls,
            "tags": plugin.metadata.tags,
            "auth": plugin.metadata.auth,
            "content": plugin.metadata.content,
            "proxyMode": (plugin.metadata.proxy or {}).get("mode", "auto"),
            "proxyRequired": bool((plugin.metadata.proxy or {}).get("required")),
            "browser": plugin.metadata.browser,
            "lastModified": _plugin_last_modified(plugin),
        }
    }


@console_route("get", "/search/stream")
async def stream_search(keyword: str = "", page: int = 1, limit: int | None = None):
    catalog = Catalog()

    async def event_generator():
        async for event in catalog.stream_search(keyword=keyword, page=page, max_sources_override=limit):
            yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@console_route("post", "/sources/{source_id}/enable")
def enable_source(source_id: str, payload: dict):
    plugin = _plugin_scheduler._plugins.get(source_id)
    enabled = payload.get("enabled", True)
    if plugin:
        plugin.metadata.enabled = enabled
    AppConfig.get().set_plugin_enabled(source_id, enabled)
    return {"sourceId": source_id, "enabled": enabled}


# ---- Search Jobs ----

_search_service = SearchJobService()
_live_check_repository = LiveCheckRepository()
_live_acceptance_service = LiveAcceptanceService(
    scheduler=_plugin_scheduler,
    repository=_live_check_repository,
)


def _schedule_console_search(job_id: str) -> None:
    """Schedule a search job to run in the background event loop."""
    _search_service.schedule_job(job_id)


@console_route("post", "/search-jobs")
async def create_search_job(request: Request, payload: dict):
    """Create a source search job.  Always starts a live search."""
    auth_service.require_admin(request)
    keyword = payload.get("keyword", "")
    page = payload.get("page", 1)
    limit = payload.get("limit")
    source_ids = payload.get("sourceIds")

    job = _search_service.create_job(
        keyword=keyword, page=page, limit=limit,
        source_ids=source_ids, search_mode="source",
    )

    return {
        "jobId": job.job_id,
        "status": "running",
        "keyword": job.keyword,
        "page": job.page,
        "searchMode": "source",
        "sourceCount": len(job.sources),
        "completedCount": 0,
        "successCount": 0,
        "errorCount": 0,
        "timeoutCount": 0,
        "elapsedMs": 0,
        "result": {"items": [], "candidateGroups": []},
        "candidateGroups": [],
        "events": _search_service.get_events(job.job_id),
        "liveSearchPending": True,
    }


@console_route("get", "/search-jobs")
def list_search_jobs(request: Request, limit: int = 20):
    auth_service.require_admin(request)
    return {"items": _search_service.list_jobs(limit=limit)}


@console_route("get", "/search-jobs/{job_id}")
def get_search_job(request: Request, job_id: str):
    """Get search job status and results using the session model."""
    auth_service.require_admin(request)
    # Try session snapshot first (in-memory, includes merged items).
    snapshot = _search_service.session_snapshot(
        job_id, base_api=None, include_official_sources=True
    )
    if snapshot:
        # Apply score filter to items.
        items = snapshot.get("items", [])
        if items:
            filtered_items, score_filter, filtered_count = _search_service._apply_score_filter(items)
            if filtered_count > 0:
                snapshot["items"] = filtered_items
                from app.services.live_acceptance import group_candidates
                snapshot["candidateGroups"] = group_candidates(
                    filtered_items, snapshot.get("keyword", "")
                )
            debug = dict(snapshot.get("debug") or {})
            debug["scoreFilter"] = score_filter
            debug["filteredCount"] = filtered_count
            snapshot["debug"] = debug
        return snapshot

    # Fallback: try DB for historical job.
    job = _search_service.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="任务不存在")
    result = job.result or {}
    items = result.get("items", [])
    candidate_groups = job.candidate_groups or []
    if items:
        filtered_items, score_filter, filtered_count = _search_service._apply_score_filter(items)
        if filtered_count > 0:
            items = filtered_items
            from app.services.live_acceptance import group_candidates
            candidate_groups = group_candidates(items, job.keyword)
        debug = dict(result.get("debug", {}))
        debug["scoreFilter"] = score_filter
        debug["filteredCount"] = filtered_count
        result = {**result, "items": items, "debug": debug}
    # When loaded from DB, job.sources is empty. Use source_count from the
    # result debug or fall back to len(job.sources).
    db_source_count = (
        result.get("debug", {}).get("sourceCount")
        or len(job.sources)
        or 0
    )
    return {
        "jobId": job.job_id,
        "status": job.status,
        "keyword": job.keyword,
        "page": job.page,
        "sourceCount": db_source_count,
        "completedCount": job.completed_count,
        "successCount": job.success_count,
        "errorCount": job.error_count,
        "timeoutCount": job.timeout_count,
        "elapsedMs": job.elapsed_ms,
        "result": result,
        "candidateGroups": candidate_groups,
        "liveSearchPending": job.status in {"running", "pending"},
    }


@console_route("get", "/search-jobs/{job_id}/events")
def get_search_job_events(request: Request, job_id: str, after: int = 0):
    auth_service.require_admin(request)
    if not _search_service.get_job(job_id):
        raise HTTPException(status_code=404, detail="任务不存在")
    events = _search_service.get_events(job_id, after_index=after)
    return {"jobId": job_id, "events": events, "nextAfter": after + len(events)}


@console_route("get", "/search-jobs/{job_id}/candidates")
def get_search_job_candidates(request: Request, job_id: str):
    auth_service.require_admin(request)
    job = _search_service.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="任务不存在")
    candidates = _search_service.get_candidates(job_id)
    # Apply score filter to candidate groups
    score_filter = _search_service._get_score_filter()
    filtered_candidates = []
    for group in candidates:
        items = [item for item in group.get("items", []) if item.get("score", 0) >= score_filter]
        if items:
            filtered_group = dict(group)
            filtered_group["items"] = items
            filtered_candidates.append(filtered_group)
    return {"jobId": job_id, "items": filtered_candidates, "scoreFilter": score_filter}


@console_route("post", "/search-jobs/{job_id}/candidates/{candidate_id}/verify")
async def verify_search_job_candidate(request: Request, job_id: str, candidate_id: str, payload: dict | None = None):
    auth_service.require_admin(request)
    candidate = _search_service.find_candidate(job_id, candidate_id)
    if not candidate:
        raise HTTPException(status_code=404, detail="候选不存在")
    job = _search_service.get_job(job_id)
    payload = payload or {}
    chapter_index = payload.get("chapterIndex", 0)
    include_reviews = payload.get("includeReviews", True)
    result = await _live_acceptance_service.verify_candidate(
        candidate,
        keyword=job.keyword if job else "",
        chapter_index=chapter_index,
        include_reviews=bool(include_reviews),
    )
    return {"jobId": job_id, "candidateId": candidate_id, "result": result}


@console_route("post", "/search-jobs/{job_id}/candidates/{candidate_id}/reviews")
async def fetch_search_job_candidate_reviews(request: Request, job_id: str, candidate_id: str, payload: dict | None = None):
    """Fetch chapter reviews independently of chapter content.

    Useful for VIP chapters where the main text is only a preview but reviews
    are still available. This endpoint intentionally allows a longer timeout
    so it can retrieve all review pages asynchronously from the frontend.
    """
    auth_service.require_admin(request)
    candidate = _search_service.find_candidate(job_id, candidate_id)
    if not candidate:
        raise HTTPException(status_code=404, detail="候选不存在")
    payload = payload or {}
    chapter_index = payload.get("chapterIndex", 0)
    # Allow the caller to cap the backend timeout; default to a generous limit
    # so review pagination can complete without blocking chapter content.
    timeout = payload.get("timeout")
    if timeout is not None:
        try:
            timeout = float(timeout)
        except (TypeError, ValueError):
            timeout = None
    result = await _live_acceptance_service.fetch_reviews(
        candidate,
        chapter_index=chapter_index,
        timeout=timeout,
    )
    return {"jobId": job_id, "candidateId": candidate_id, "result": result}


@console_route("post", "/search-jobs/{job_id}/cancel")
def cancel_search_job(request: Request, job_id: str):
    auth_service.require_admin(request)
    if not _search_service.get_job(job_id):
        raise HTTPException(status_code=404, detail="任务不存在")
    ok = _search_service.cancel_job(job_id)
    return {"jobId": job_id, "cancelled": ok}


@console_route("post", "/search-jobs/{job_id}/subscribe")
async def subscribe_from_search_job(request: Request, job_id: str, payload: dict):
    """Admin shortcut: add a candidate group to the shared library."""
    admin = auth_service.require_admin(request)
    candidate_id = str(payload.get("candidateId", "")).strip()
    if not candidate_id:
        raise HTTPException(status_code=400, detail="缺少 candidateId")
    group = _search_service.find_candidate_group(job_id, candidate_id)
    if not group:
        raise HTTPException(status_code=404, detail="候选书籍不存在")
    created = await library_books_service.create_or_get_shared_book(
        group,
        actor_user_id=admin.user_id,
    )
    if created.get("created"):
        processor = AggregateProcessor()
        book_id = created["book"]["aggregateBookId"]
        initial_next_check = (
            datetime.now(timezone.utc)
            + timedelta(minutes=processor.check_interval_minutes(book_id))
        ).isoformat()
        processor.enqueue_book(book_id, created["payload"], next_check_time=initial_next_check)
        scheduler = SharedBookScheduler(processor=processor)
        scheduler.enqueue_initial_subscription(
            book_id,
            payload=created["payload"],
            book_name=created["book"].get("name", ""),
            author=created["book"].get("author", ""),
        )
        asyncio.create_task(scheduler.run_periodic_once(wait_for_recovery=False, include_due_books=False))
    return {
        "ok": True,
        "created": bool(created.get("created")),
        "book": created.get("book"),
    }


# ---- Explore ----

@console_route("get", "/explore/sources")
async def list_explore_sources():
    groups = await _plugin_scheduler.explore_groups()
    source_map: dict[str, dict] = {}
    for group in groups.get("groups", []):
        source_id = group.get("sourceId", "")
        if not source_id:
            continue
        source = source_map.setdefault(
            source_id,
            {
                "sourceId": source_id,
                "name": group.get("sourceName", ""),
                "groupCount": 0,
                "groups": [],
            },
        )
        source["groupCount"] += 1
        source["groups"].append(group)
    return {
        "items": list(source_map.values()),
        "total": len(source_map),
        "debug": groups.get("debug", {}),
    }


@console_route("get", "/explore/sources/{source_id}/groups")
async def get_explore_groups(source_id: str):
    return await _plugin_scheduler.explore_groups(source_id)


@console_route("post", "/explore/sources/{source_id}/items")
async def explore_items(source_id: str, payload: dict):
    group_id = payload.get("groupId") or payload.get("kind")
    page = int(payload.get("page", 1) or 1)
    return await Catalog().explore(source_id=source_id, group_id=group_id, page=page)


# ---- Books ----

@console_route("get", "/books")
def list_books(limit: int = 100, offset: int = 0):
    import sqlite3
    from app.config import DB_PATH
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT book_id, name, author, last_chapter, last_seen_at FROM book_records ORDER BY last_seen_at DESC LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
    return {
        "items": [
            {"bookId": r[0], "name": r[1], "author": r[2], "lastChapter": r[3], "lastSeenAt": r[4]}
            for r in rows
        ],
        "limit": limit,
        "offset": offset,
    }


@console_route("get", "/books/{book_id}")
async def get_book(book_id: str):
    catalog = BookCatalog()
    detail = await catalog.book_detail(book_id)
    sources = catalog.get_book_sources(book_id)
    return {"bookId": book_id, "detail": detail, "sources": sources}


@console_route("get", "/books/{book_id}/toc")
async def get_book_toc(book_id: str):
    catalog = BookCatalog()
    return await catalog.toc(book_id)


@console_route("get", "/chapter/{chapter_id}")
async def get_chapter(chapter_id: str):
    catalog = BookCatalog()
    return await catalog.chapter(chapter_id)


@console_route("get", "/chapter/{chapter_id}/fallback")
async def get_chapter_fallback(chapter_id: str, source_ids: str = ""):
    catalog = BookCatalog()
    fallback_ids = [s.strip() for s in source_ids.split(",") if s.strip()]
    return await catalog.chapter_with_fallback(chapter_id, fallback_ids or None)


@console_route("get", "/books/{book_id}/chapters/{chapter_id}/navigation")
def get_chapter_navigation(book_id: str, chapter_id: str):
    catalog = BookCatalog()
    return catalog.get_chapter_navigation(book_id, chapter_id)


# ---- Update Tasks ----

_scheduler = UpdateScheduler()


@console_route("get", "/update-tasks")
def list_update_tasks(limit: int = 100, offset: int = 0):
    return {"items": _scheduler.list_tasks(limit=limit, offset=offset)}


@console_route("post", "/update-tasks/{book_id}/enable")
def enable_update_task(book_id: str):
    return _scheduler.enable_tracking(book_id)


@console_route("post", "/update-tasks/{book_id}/disable")
def disable_update_task(book_id: str):
    return _scheduler.disable_tracking(book_id)


@console_route("post", "/update-tasks/{book_id}/run")
async def run_update_task(book_id: str):
    return await _scheduler.run_check(book_id)


# ---- Cache ----

@console_route("get", "/cache")
def get_cache():
    import sqlite3
    from app.config import DB_PATH
    with sqlite3.connect(DB_PATH) as conn:
        search_count = conn.execute("SELECT COUNT(*) FROM book_search_cache").fetchone()[0]
        book_count = conn.execute("SELECT COUNT(*) FROM book_cache").fetchone()[0]
        toc_count = conn.execute("SELECT COUNT(*) FROM toc_cache").fetchone()[0]
        chapter_count = conn.execute("SELECT COUNT(*) FROM chapter_cache").fetchone()[0]
    return {
        "searchCache": search_count,
        "bookCache": book_count,
        "tocCache": toc_count,
        "chapterCache": chapter_count,
    }


def _json_payload(value: str | None) -> dict:
    if not value:
        return {}
    try:
        payload = json.loads(value)
        return payload if isinstance(payload, dict) else {}
    except (json.JSONDecodeError, TypeError):
        logger.warning("Ignoring malformed cached JSON payload", exc_info=True)
        return {}


@console_route("get", "/cache/items")
def list_cache_items(limit: int = 50):
    import sqlite3
    from app.config import DB_PATH

    limit = max(1, min(int(limit or 50), 200))
    with sqlite3.connect(DB_PATH) as conn:
        search_rows = conn.execute(
            """
            SELECT match_mode, normalized_name, source_id, source_name,
                   raw_book_url, score, first_seen_at, last_seen_at, expires_at
            FROM book_search_cache
            ORDER BY last_seen_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        book_rows = conn.execute(
            """
            SELECT book_id, source_id, book_url, response_json, created_at
            FROM book_cache
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        toc_rows = conn.execute(
            """
            SELECT book_id, response_json, created_at
            FROM toc_cache
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        chapter_rows = conn.execute(
            """
            SELECT chapter_id, source_id, chapter_url, response_json, created_at
            FROM chapter_cache
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    searches = []
    for (match_mode, norm_name, source_id, source_name,
         raw_book_url, score, first_seen, last_seen, expires_at) in search_rows:
        searches.append({
            "matchMode": match_mode,
            "normalizedName": norm_name,
            "sourceId": source_id,
            "sourceName": source_name,
            "rawBookUrl": raw_book_url,
            "score": score,
            "firstSeenAt": first_seen,
            "lastSeenAt": last_seen,
            "expiresAt": expires_at,
        })

    books = []
    for book_id, source_id, book_url, response_json, created_at in book_rows:
        payload = _json_payload(response_json)
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        books.append({
            "bookId": book_id,
            "sourceId": source_id,
            "bookUrl": book_url,
            "name": data.get("name", ""),
            "author": data.get("author", ""),
            "lastChapter": data.get("lastChapter", ""),
            "createdAt": created_at,
        })

    tocs = []
    for book_id, response_json, created_at in toc_rows:
        payload = _json_payload(response_json)
        chapters = payload.get("chapters") if isinstance(payload.get("chapters"), list) else []
        first = chapters[0] if chapters else {}
        last = chapters[-1] if chapters else {}
        tocs.append({
            "bookId": book_id,
            "chapterCount": len(chapters),
            "firstTitle": first.get("title", "") if isinstance(first, dict) else "",
            "lastTitle": last.get("title", "") if isinstance(last, dict) else "",
            "createdAt": created_at,
        })

    chapters = []
    for chapter_id, source_id, chapter_url, response_json, created_at in chapter_rows:
        payload = _json_payload(response_json)
        content = payload.get("content", "")
        chapters.append({
            "chapterId": chapter_id,
            "sourceId": source_id,
            "chapterUrl": chapter_url,
            "title": payload.get("title", ""),
            "contentLength": len(content) if isinstance(content, str) else 0,
            "createdAt": created_at,
        })

    return {
        "searches": searches,
        "books": books,
        "tocs": tocs,
        "chapters": chapters,
        "limit": limit,
    }


@console_route("delete", "/cache")
def clear_cache():
    import sqlite3
    from app.config import DB_PATH
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM book_search_cache")
        conn.execute("DELETE FROM book_cache")
        conn.execute("DELETE FROM toc_cache")
        conn.execute("DELETE FROM chapter_cache")
        conn.commit()
    return {"cleared": True}


@console_route("post", "/cache/clear")
def clear_cache_post(payload: dict):
    cache_type = payload.get("type", "all")
    import sqlite3
    from app.config import DB_PATH
    with sqlite3.connect(DB_PATH) as conn:
        if cache_type in ("all", "search"):
            conn.execute("DELETE FROM book_search_cache")
        if cache_type in ("all", "book"):
            conn.execute("DELETE FROM book_cache")
        if cache_type in ("all", "toc"):
            conn.execute("DELETE FROM toc_cache")
        if cache_type in ("all", "chapter"):
            conn.execute("DELETE FROM chapter_cache")
        conn.commit()
    return {"cleared": True, "type": cache_type}


def _source_pool_from_config(cfg: AppConfig) -> dict:
    return {
        "proxy": {
            "enabled": cfg.proxy.enabled,
            "url": cfg.proxy.url,
            "allowAutoRetry": cfg.proxy.allow_auto_retry,
        },
        "max_concurrency": cfg.search.global_source_concurrency,
        "source_timeout_seconds": cfg.search.source_timeout_seconds,
        "overall_search_timeout_seconds": cfg.search.overall_timeout_seconds,
        "browser_source_timeout_seconds": cfg.search.browser_source_timeout_seconds,
        "browser_search_timeout_seconds": cfg.search.browser_search_timeout_seconds,
        "default_user_agent": cfg.search.default_user_agent,
        "officialSourceInNormalSearch": cfg.search.official_source_in_normal_search,
    }


_SOURCE_POOL_FIELDS = {
    "proxy",
    "max_concurrency",
    "source_timeout_seconds",
    "overall_search_timeout_seconds",
    "browser_source_timeout_seconds",
    "browser_search_timeout_seconds",
    "default_user_agent",
    "officialSourceInNormalSearch",
}
_SOURCE_POOL_PROXY_FIELDS = {"enabled", "url", "allowAutoRetry"}
_SEARCH_CONFIG_FIELDS = {
    "overallTimeoutSeconds",
    "firstResultTimeoutSeconds",
    "sourceTimeoutSeconds",
    "cacheTtlSeconds",
}
_SUBSCRIPTION_SETTING_FIELDS = {
    "maxActivePerUser",
    "maxNewSharedBooksPerDay",
    "maxGlobalProvisioningBooks",
    "rateLimitWindowSeconds",
    "searchRateLimitPerWindow",
    "createRateLimitPerWindow",
    "updateRateLimitPerWindow",
}
_CHAPTER_COMMENT_SETTING_FIELDS = {
    "segmentEnabled",
    "pageEnabled",
    "chapterEnabled",
}
_READING_ACCESS_SETTING_FIELDS = {
    "publicBaseUrl",
}
_SETTINGS_FIELDS = {
    "sourcePool",
    "searchScoreFilter",
    "searchConfig",
    "contentWorkflow",
    "subscription",
    "chapterComment",
    "readingAccess",
}


def _apply_source_pool_to_config(cfg: AppConfig, sp: dict) -> None:
    _reject_unknown_fields(sp, _SOURCE_POOL_FIELDS, label="sourcePool")
    if "proxy" in sp:
        proxy = sp["proxy"]
        if not isinstance(proxy, dict):
            raise HTTPException(status_code=422, detail="sourcePool.proxy 必须是对象")
        _reject_unknown_fields(proxy, _SOURCE_POOL_PROXY_FIELDS, label="sourcePool.proxy")
        if "enabled" in proxy:
            cfg.set("proxy.enabled", _parse_bool_field(proxy["enabled"], field="sourcePool.proxy.enabled"))
        if "url" in proxy:
            cfg.set("proxy.url", str(proxy["url"] or ""))
        if "allowAutoRetry" in proxy:
            cfg.set(
                "proxy.allowAutoRetry",
                _parse_bool_field(proxy["allowAutoRetry"], field="sourcePool.proxy.allowAutoRetry"),
            )
    if "max_concurrency" in sp:
        cfg.set(
            "search.globalSourceConcurrency",
            _parse_int_field(sp["max_concurrency"], field="sourcePool.max_concurrency", minimum=1),
        )
    for field_name, config_key in (
        ("source_timeout_seconds", "search.sourceTimeoutSeconds"),
        ("overall_search_timeout_seconds", "search.overallTimeoutSeconds"),
        ("browser_source_timeout_seconds", "search.browserSourceTimeoutSeconds"),
        ("browser_search_timeout_seconds", "search.browserSearchTimeoutSeconds"),
    ):
        if field_name in sp:
            cfg.set(
                config_key,
                _parse_float_field(sp[field_name], field=f"sourcePool.{field_name}", minimum=0),
            )
    if "default_user_agent" in sp:
        cfg.set("search.defaultUserAgent", str(sp["default_user_agent"] or ""))
    if "officialSourceInNormalSearch" in sp:
        cfg.set(
            "search.officialSourceInNormalSearch",
            _parse_bool_field(
                sp["officialSourceInNormalSearch"],
                field="sourcePool.officialSourceInNormalSearch",
            ),
        )


def _apply_search_config(cfg: AppConfig, search_config: dict) -> None:
    _reject_unknown_fields(search_config, _SEARCH_CONFIG_FIELDS, label="searchConfig")
    for field_name, config_key, integer in (
        ("overallTimeoutSeconds", "search.overallTimeoutSeconds", False),
        ("firstResultTimeoutSeconds", "search.firstResultTimeoutSeconds", False),
        ("sourceTimeoutSeconds", "search.sourceTimeoutSeconds", False),
        ("cacheTtlSeconds", "search.cacheTtlSeconds", True),
    ):
        if field_name not in search_config:
            continue
        if integer:
            value = _parse_int_field(search_config[field_name], field=f"searchConfig.{field_name}", minimum=0)
        else:
            value = _parse_float_field(search_config[field_name], field=f"searchConfig.{field_name}", minimum=0)
        cfg.set(config_key, value)


def _subscription_settings_from_config(cfg: AppConfig) -> dict:
    subscription = cfg.subscription
    return {
        "maxActivePerUser": subscription.max_active_per_user,
        "maxNewSharedBooksPerDay": subscription.max_new_shared_books_per_day,
        "maxGlobalProvisioningBooks": subscription.max_global_provisioning_books,
        "rateLimitWindowSeconds": subscription.rate_limit_window_seconds,
        "searchRateLimitPerWindow": subscription.search_rate_limit_per_window,
        "createRateLimitPerWindow": subscription.create_rate_limit_per_window,
        "updateRateLimitPerWindow": subscription.update_rate_limit_per_window,
    }


def _chapter_comment_settings_from_config(cfg: AppConfig) -> dict:
    chapter_comment = cfg.chapter_comment
    return {
        "segmentEnabled": chapter_comment.segment_enabled,
        "pageEnabled": chapter_comment.page_enabled,
        "chapterEnabled": chapter_comment.chapter_enabled,
    }


def _reading_access_settings_from_config(cfg: AppConfig) -> dict:
    return {
        "publicBaseUrl": cfg.reading_access.public_base_url,
    }


def _parse_public_base_url_field(value: object, *, field: str) -> str:
    """Accept a single public book-source origin (no multi-line list)."""
    if value is None:
        return ""
    if not isinstance(value, str):
        raise HTTPException(status_code=422, detail=f"{field} 必须是字符串")
    text = value.strip().rstrip("/")
    if not text:
        return ""
    if any(sep in text for sep in ("\n", "\r", ",", ";")) or " " in text.strip():
        raise HTTPException(status_code=422, detail=f"{field} 只支持单个地址，请勿填写多个")
    from app.core.public_security import normalize_public_base_url

    try:
        return normalize_public_base_url(text)
    except RuntimeError as exc:
        raise HTTPException(status_code=422, detail=f"{field} 无效：{exc}") from exc


# ---- Settings ----

@console_route("get", "/settings")
def get_settings():
    cfg = AppConfig.get()
    settings = {
        "sourcePool": _source_pool_from_config(cfg),
        "searchScoreFilter": cfg.search.score_filter,
        "searchConfig": {
            "overallTimeoutSeconds": cfg.search.overall_timeout_seconds,
            "firstResultTimeoutSeconds": cfg.search.first_result_timeout_seconds,
            "sourceTimeoutSeconds": cfg.search.source_timeout_seconds,
            "cacheTtlSeconds": cfg.search.cache_ttl_seconds,
        },
        "contentWorkflow": cfg.aggregate.content_workflow,
        "subscription": _subscription_settings_from_config(cfg),
        "chapterComment": _chapter_comment_settings_from_config(cfg),
        "readingAccess": _reading_access_settings_from_config(cfg),
    }
    return settings


@console_route("post", "/settings")
def update_settings(payload: dict):
    _reject_unknown_fields(payload, _SETTINGS_FIELDS, label="settings")
    active_config = AppConfig.get()
    # ponytail: process-local lock matches the supported single-process deployment.
    with AppConfig._lock:
        cfg = AppConfig(active_config.path)
        if "searchConfig" in payload:
            if not isinstance(payload["searchConfig"], dict):
                raise HTTPException(status_code=422, detail="searchConfig 必须是对象")
            _apply_search_config(cfg, payload["searchConfig"])
        if "sourcePool" in payload:
            if not isinstance(payload["sourcePool"], dict):
                raise HTTPException(status_code=422, detail="sourcePool 必须是对象")
            _apply_source_pool_to_config(cfg, payload["sourcePool"])
        if "searchScoreFilter" in payload:
            cfg.set(
                "search.scoreFilter",
                _parse_int_field(payload["searchScoreFilter"], field="searchScoreFilter", minimum=0),
            )
        if "contentWorkflow" in payload:
            if not isinstance(payload["contentWorkflow"], dict):
                raise HTTPException(status_code=422, detail="contentWorkflow 必须是对象")
            cfg.set("aggregate.contentWorkflow", payload["contentWorkflow"])
        if "subscription" in payload:
            subscription = payload["subscription"]
            if not isinstance(subscription, dict):
                raise HTTPException(status_code=422, detail="subscription 必须是对象")
            _reject_unknown_fields(subscription, _SUBSCRIPTION_SETTING_FIELDS, label="subscription")
            for field_name in _SUBSCRIPTION_SETTING_FIELDS:
                if field_name in subscription:
                    cfg.set(
                        f"subscription.{field_name}",
                        _parse_int_field(subscription[field_name], field=f"subscription.{field_name}", minimum=1),
                    )
        if "chapterComment" in payload:
            chapter_comment = payload["chapterComment"]
            if not isinstance(chapter_comment, dict):
                raise HTTPException(status_code=422, detail="chapterComment 必须是对象")
            _reject_unknown_fields(
                chapter_comment,
                _CHAPTER_COMMENT_SETTING_FIELDS,
                label="chapterComment",
            )
            for field_name in _CHAPTER_COMMENT_SETTING_FIELDS:
                if field_name in chapter_comment:
                    cfg.set(
                        f"chapterComment.{field_name}",
                        _parse_bool_field(
                            chapter_comment[field_name],
                            field=f"chapterComment.{field_name}",
                        ),
                    )
        if "readingAccess" in payload:
            reading_access = payload["readingAccess"]
            if not isinstance(reading_access, dict):
                raise HTTPException(status_code=422, detail="readingAccess 必须是对象")
            _reject_unknown_fields(
                reading_access,
                _READING_ACCESS_SETTING_FIELDS,
                label="readingAccess",
            )
            if "publicBaseUrl" in reading_access:
                cfg.set(
                    "readingAccess.publicBaseUrl",
                    _parse_public_base_url_field(
                        reading_access["publicBaseUrl"],
                        field="readingAccess.publicBaseUrl",
                    ),
                )
        cfg.save()
        active_config.reload()
    _plugin_scheduler.refresh_config()
    _search_service.scheduler.refresh_config()
    return {
        "saved": True,
        "subscription": _subscription_settings_from_config(active_config),
        "chapterComment": _chapter_comment_settings_from_config(active_config),
        "readingAccess": _reading_access_settings_from_config(active_config),
    }


# ---- Aggregate Settings ----

@console_route("get", "/aggregate-settings")
def get_aggregate_settings():
    from app.config import DB_PATH
    from app.storage.db import initialize_database

    initialize_database(DB_PATH)
    return AggregateSettingsRepository(DB_PATH).get_settings()


@console_route("post", "/aggregate-settings")
def update_aggregate_settings(payload: dict):
    from app.config import DB_PATH
    from app.storage.db import initialize_database

    initialize_database(DB_PATH)
    settings = AggregateSettingsRepository(DB_PATH).save_settings(payload)
    return {"saved": True, **settings}


@console_route("post", "/aggregate-settings/test-provider")
async def test_aggregate_provider(payload: dict):
    if not AI_RUNTIME_ENABLED:
        return {"ok": False, "status": "disabled", "message": "AI 链路已暂时停用"}

    from app.ai.client import OpenAICompatibleClient

    config = payload.get("aiProviderConfig") if isinstance(payload.get("aiProviderConfig"), dict) else payload
    if not config or not config.get("baseUrl") or not config.get("apiKey"):
        return {"ok": False, "status": "not_configured", "message": "baseUrl and apiKey are required"}

    client = OpenAICompatibleClient(config)
    return await client.test_connectivity()


@console_route("post", "/aggregate-settings/fetch-models")
async def fetch_aggregate_models(payload: dict):
    if not AI_RUNTIME_ENABLED:
        return {"ok": False, "models": [], "status": "disabled", "message": "AI 链路已暂时停用"}

    from app.ai.client import OpenAICompatibleClient

    config = payload.get("aiProviderConfig") if isinstance(payload.get("aiProviderConfig"), dict) else payload
    if not config or not config.get("baseUrl") or not config.get("apiKey"):
        return {"ok": False, "status": "not_configured", "models": [], "message": "baseUrl and apiKey are required"}

    client = OpenAICompatibleClient(config)
    try:
        models = await client.list_models()
        return {"ok": True, "models": models, "count": len(models)}
    except Exception as exc:
        return {"ok": False, "models": [], "status": "error", "message": str(exc)}


# ---- Aggregate Books ----

def _bounded_page(value: int | str | None, default: int, maximum: int) -> int:
    try:
        parsed = int(value) if value is not None else default
    except (TypeError, ValueError):
        parsed = default
    return min(max(parsed, 1), maximum)


def _write_aggregate_operation_log(
    conn,
    *,
    book_id: str,
    actor_user_id: str,
    actor_role: str,
    operation_type: str,
    before: dict,
    after: dict,
) -> None:
    conn.execute(
        """
        INSERT INTO aggregate_operation_logs
        (aggregate_book_id, actor_user_id, actor_role, operation_type, before_json, after_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            book_id,
            actor_user_id,
            actor_role,
            operation_type,
            json.dumps(before, ensure_ascii=False),
            json.dumps(after, ensure_ascii=False),
            _now(),
        ),
    )


def _aggregate_book_delete_snapshot(conn, book_id: str, book: dict) -> dict:
    subscription_rows = conn.execute(
        """
        SELECT status, COUNT(*)
        FROM user_book_subscriptions
        WHERE aggregate_book_id = ?
        GROUP BY status
        """,
        (book_id,),
    ).fetchall()
    subscription_counts = {str(status): int(count or 0) for status, count in subscription_rows}
    chapter_count = int(
        conn.execute(
            "SELECT COUNT(*) FROM aggregate_chapter_tasks WHERE aggregate_book_id = ?",
            (book_id,),
        ).fetchone()[0]
        or 0
    )
    return {
        "book": {
            "aggregateBookId": book_id,
            "name": str(book.get("name", "") or ""),
            "author": str(book.get("author", "") or ""),
            "status": str(book.get("status", "") or ""),
        },
        "subscriptionCounts": subscription_counts,
        "chapterCount": chapter_count,
    }


def _delete_aggregate_book_impl(
    book_id: str,
    *,
    actor_user_id: str = "",
    actor_role: str = "admin",
    lease_wait_seconds: float = _DELETE_LEASE_WAIT_SECONDS,
) -> dict:
    import sqlite3
    from app.config import DB_PATH
    from app.storage.db import initialize_database

    initialize_database(DB_PATH)
    book = library_books_service.get_book(book_id)
    if not book:
        return {"bookId": book_id, "deleted": False}

    lock_service = SharedBookLockService(storage=library_books_service.shared_book_storage)
    lease = lock_service.acquire(aggregate_book_id=book_id)
    if lease is None:
        with sqlite3.connect(DB_PATH, timeout=5.0) as conn:
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute(
                "UPDATE aggregate_book_tasks SET status = 'archived', updated_at = datetime('now') "
                "WHERE aggregate_book_id = ?",
                (book_id,),
            )
            conn.commit()
        lock_service.request_stop(aggregate_book_id=book_id)
        deadline = time.monotonic() + max(0.0, float(lease_wait_seconds))
        while lease is None and time.monotonic() < deadline:
            time.sleep(0.05)
            lease = lock_service.acquire(aggregate_book_id=book_id)

    if lease is None:
        with sqlite3.connect(DB_PATH, timeout=5.0) as conn:
            before = _aggregate_book_delete_snapshot(conn, book_id, book)
            _write_aggregate_operation_log(
                conn,
                book_id=book_id,
                actor_user_id=actor_user_id,
                actor_role=actor_role,
                operation_type="delete.rejected",
                before=before,
                after={"deleted": False, "reason": "aggregate_book_busy"},
            )
            audit_service.record(
                action="shared_book.delete",
                actor_user_id=actor_user_id,
                actor_role=actor_role,
                target_type="shared_book",
                target_id=book_id,
                outcome="rejected",
                summary={"deleted": False, "errorCode": "aggregate_book_busy"},
                conn=conn,
            )
            conn.commit()
        raise HTTPException(
            status_code=409,
            detail={
                "code": "aggregate_book_busy",
                "message": "已停止后续处理，当前任务仍在退出，请稍后重试删除",
                "retryable": True,
            },
        )

    try:
        with sqlite3.connect(DB_PATH, timeout=5.0) as conn:
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute("BEGIN IMMEDIATE")
            before = _aggregate_book_delete_snapshot(conn, book_id, book)
            conn.execute("DELETE FROM aggregate_source_snapshots WHERE aggregate_book_id = ?", (book_id,))
            conn.execute("DELETE FROM aggregate_book_sources WHERE aggregate_book_id = ?", (book_id,))
            conn.execute("DELETE FROM aggregate_ai_usage WHERE aggregate_book_id = ?", (book_id,))
            conn.execute("DELETE FROM aggregate_chapter_tasks WHERE aggregate_book_id = ?", (book_id,))
            cursor = conn.execute("DELETE FROM aggregate_book_tasks WHERE aggregate_book_id = ?", (book_id,))
            deleted = cursor.rowcount > 0
            _write_aggregate_operation_log(
                conn,
                book_id=book_id,
                actor_user_id=actor_user_id,
                actor_role=actor_role,
                operation_type="delete",
                before=before,
                after={"deleted": deleted},
            )
            audit_service.record(
                action="shared_book.delete",
                actor_user_id=actor_user_id,
                actor_role=actor_role,
                target_type="shared_book",
                target_id=book_id,
                summary={"deleted": deleted},
                conn=conn,
            )
            conn.commit()

        # Clean up both the shared library directory and the private runtime directory.
        storage = library_books_service.shared_book_storage
        book_name = str(book.get("name", "") or "").strip()
        author = str(book.get("author", "") or "").strip()
        if book_name:
            shared_dir = storage.shared_book_dir(book_name=book_name, author=author)
            private_dir = storage.runtime_dir(book_name=book_name, author=author).parent
            if shared_dir.exists():
                shutil.rmtree(shared_dir, ignore_errors=True)
            if private_dir.exists():
                shutil.rmtree(private_dir, ignore_errors=True)

        # Legacy path cleanup (no longer the active storage location).
        legacy_dir = Path(DB_PATH).parent / "novels" / "legadohub_ai_aggregate" / book_id
        if legacy_dir.exists():
            shutil.rmtree(legacy_dir, ignore_errors=True)
        return {"bookId": book_id, "deleted": deleted}
    finally:
        lease.release()


def _update_aggregate_book_status(book_id: str, status: str, *, actor_user_id: str = "", actor_role: str = "admin"):
    import sqlite3
    from app.config import DB_PATH
    from app.storage.db import initialize_database

    initialize_database(DB_PATH)
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT status FROM aggregate_book_tasks WHERE aggregate_book_id = ?",
            (book_id,),
        ).fetchone()
        if not row:
            return {"bookId": book_id, "status": status, "updated": False}
        before = {"status": str(row[0] or "")}
        cursor = conn.execute(
            """
            UPDATE aggregate_book_tasks
            SET status = ?,
                archived_at = CASE
                        WHEN ? = 'archived' THEN COALESCE(archived_at, datetime('now'))
                    WHEN ? = 'active' THEN NULL
                    WHEN ? != 'archived' THEN archived_at
                    ELSE archived_at
                END,
                updated_at = datetime('now')
            WHERE aggregate_book_id = ?
            """,
            (status, status, status, status, book_id),
        )
        _write_aggregate_operation_log(
            conn,
            book_id=book_id,
            actor_user_id=actor_user_id,
            actor_role=actor_role,
            operation_type=f"set_status:{status}",
            before=before,
            after={"status": status},
        )
        audit_service.record(
            action="shared_book.status.update",
            actor_user_id=actor_user_id,
            actor_role=actor_role,
            target_type="shared_book",
            target_id=book_id,
            summary={"previousStatus": before["status"], "status": status},
            conn=conn,
        )
        conn.commit()
    archived_subscriptions = 0
    if status == "archived" and cursor.rowcount > 0:
        from app.services.user_subscriptions import UserSubscriptionsService

        archived_subscriptions = UserSubscriptionsService(DB_PATH).archive_completed_for_book(book_id)
    return {
        "bookId": book_id,
        "status": status,
        "updated": cursor.rowcount > 0,
        "archivedSubscriptions": archived_subscriptions,
    }


def delete_aggregate_book(request: Request, book_id: str):
    admin = auth_service.require_admin(request)
    return _delete_aggregate_book_impl(
        book_id,
        actor_user_id=admin.user_id,
        actor_role=admin.role,
    )


# ---- Shared Library / Users ----

@console_route("get", "/library-books")
def list_library_books(request: Request, keyword: str = ""):
    auth_service.require_admin(request)
    items = library_books_service.list_books(keyword=keyword, include_hidden=True)
    return {"items": items, "total": len(items)}


@console_route("get", "/library-integrity")
def scan_library_integrity_console(request: Request):
    auth_service.require_admin(request)
    return library_books_service.scan_integrity()


@console_route("post", "/library-integrity/repair")
def repair_library_integrity_console(request: Request, payload: dict | None = None):
    admin = auth_service.require_admin(request)
    result = _manual_library_integrity_repair(payload)
    audit_service.record(
        action="shared_book.integrity.repair",
        actor_user_id=admin.user_id,
        actor_role=admin.role,
        target_type="shared_book_library",
        target_id="global",
        summary={
            "repairedBooks": result.get("repairedBooks", 0),
            "queuedBooks": result.get("queuedBooks", 0),
        },
    )
    return result


@console_route("get", "/library-books/{book_id}/summary")
def get_library_book_summary(request: Request, book_id: str):
    auth_service.require_admin(request)
    payload = _load_shared_library_book_summary(book_id, admin_view=True)
    payload["mode"] = "shared"
    return payload


@console_route("get", "/library-books/{book_id}")
def get_library_book_admin(request: Request, book_id: str):
    auth_service.require_admin(request)
    payload = _load_shared_library_book_summary(book_id, admin_view=True)
    payload["mode"] = "shared"
    return payload


@console_route("get", "/library-books/{book_id}/chapters")
def list_library_book_chapters_admin(
    request: Request,
    book_id: str,
    page: int = 1,
    pageSize: int = 50,
    status: str = "all",
    keyword: str = "",
):
    auth_service.require_admin(request)
    payload = _list_shared_library_book_chapters(
        book_id,
        page=page,
        pageSize=pageSize,
        status=status,
        keyword=keyword,
    )
    payload["mode"] = "shared"
    payload["adminView"] = True
    return payload


@console_route("get", "/library-books/{book_id}/logs")
def list_library_book_logs(request: Request, book_id: str, limit: int = 50, offset: int = 0):
    auth_service.require_admin(request)
    return _list_library_book_logs(book_id, limit=limit, offset=offset, admin_view=True)


@console_route("get", "/library-books/{book_id}/chapters/{chapter_id}/progress")
def get_library_book_chapter_progress(request: Request, book_id: str, chapter_id: str):
    auth_service.require_admin(request)
    payload = _load_library_book_chapter_progress(book_id, chapter_id)
    if payload.get("found", True) and isinstance(payload.get("traceSummary"), dict):
        payload["traceSummary"] = _sanitize_trace_summary(payload["traceSummary"])
    return payload


@console_route("post", "/library-books/{book_id}/chapters/{chapter_id}/process")
def process_library_book_chapter(request: Request, book_id: str, chapter_id: str):
    auth_service.require_admin(request)
    return _reprocess_library_book_chapter(book_id, chapter_id)


@console_route("post", "/library-books/{book_id}/pause")
def pause_library_book(request: Request, book_id: str):
    admin = auth_service.require_admin(request)
    return _update_aggregate_book_status(
        book_id, "paused", actor_user_id=admin.user_id, actor_role=admin.role
    )


@console_route("post", "/library-books/{book_id}/resume")
def resume_library_book(request: Request, book_id: str):
    admin = auth_service.require_admin(request)
    return _update_aggregate_book_status(
        book_id, "active", actor_user_id=admin.user_id, actor_role=admin.role
    )


@console_route("post", "/library-books/{book_id}/archive")
def archive_library_book(request: Request, book_id: str):
    admin = auth_service.require_admin(request)
    return _update_aggregate_book_status(
        book_id, "archived", actor_user_id=admin.user_id, actor_role=admin.role
    )


@console_route("delete", "/library-books/{book_id}")
def delete_library_book(request: Request, book_id: str):
    auth_service.require_admin(request)
    return delete_aggregate_book(request, book_id)


@console_route("get", "/library-books/{book_id}/settings")
def get_library_book_settings(request: Request, book_id: str):
    auth_service.require_admin(request)
    book = library_books_service.get_book(book_id)
    if not book:
        raise HTTPException(status_code=404, detail="书籍不存在")
    processing_settings = _shared_book_processing_settings(book)
    return {
        "bookId": book_id,
        "settings": processing_settings,
        "currentPolicyVersion": int(book.get("currentPolicyVersion", 1) or 1),
        "intervalMinutes": processing_settings["updateIntervalMinutes"],
    }


@console_route("post", "/library-books/{book_id}/settings")
def update_library_book_settings(request: Request, book_id: str, payload: dict):
    admin = auth_service.require_admin(request)
    normalized_payload = _validate_library_book_settings(payload)
    import sqlite3
    from app.config import DB_PATH
    from app.storage.db import initialize_database

    initialize_database(DB_PATH)
    now = _now()
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT settings_json, current_policy_version, interval_minutes, next_check_time
            FROM aggregate_book_tasks
            WHERE aggregate_book_id = ?
            """,
            (book_id,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="书籍不存在")

        settings = _aggregate_book_settings(row[0] or "")
        before_settings = dict(settings)
        current_policy_version = int(row[1] or 1)
        interval_minutes = int(row[2] or settings.get("updateIntervalMinutes", 60) or 60)
        next_check_time = str(row[3] or "")
        policy_changed = False

        for field_name in ("autoTrackUpdates", "aiAggregateEnabled", "aiPurifyEnabled"):
            if field_name in normalized_payload:
                value = normalized_payload[field_name]
                policy_changed = policy_changed or settings.get(field_name) != value
                settings[field_name] = value
        if "updateIntervalMinutes" in normalized_payload:
            settings["updateIntervalMinutes"] = normalized_payload["updateIntervalMinutes"]
            interval_minutes = settings["updateIntervalMinutes"]
            next_check_time = (
                datetime.now(timezone.utc) + timedelta(minutes=interval_minutes)
            ).isoformat()
        if "backlogChapterLimit" in normalized_payload:
            settings["backlogChapterLimit"] = normalized_payload["backlogChapterLimit"]
        for field_name in ("primarySourceMode", "sourcePriorityMode", "sourcePriority"):
            if field_name in normalized_payload:
                value = normalized_payload[field_name]
                policy_changed = policy_changed or settings.get(field_name) != value
                settings[field_name] = value
        next_policy_version = current_policy_version + 1 if policy_changed else current_policy_version
        conn.execute(
            """
            UPDATE aggregate_book_tasks
            SET settings_json = ?, interval_minutes = ?, current_policy_version = ?,
                next_check_time = ?, updated_at = ?
            WHERE aggregate_book_id = ?
            """,
            (
                json.dumps(settings, ensure_ascii=False),
                interval_minutes,
                next_policy_version,
                next_check_time or None,
                now,
                book_id,
            ),
        )
        conn.execute(
            """
            INSERT INTO aggregate_operation_logs
            (aggregate_book_id, actor_user_id, actor_role, operation_type, before_json, after_json, created_at)
            VALUES (?, ?, 'admin', 'update_settings', ?, ?, ?)
            """,
            (
                book_id,
                admin.user_id,
                json.dumps(before_settings, ensure_ascii=False),
                json.dumps(normalized_payload, ensure_ascii=False),
                now,
            ),
        )
        audit_service.record(
            action="shared_book.settings.update",
            actor_user_id=admin.user_id,
            actor_role=admin.role,
            target_type="shared_book",
            target_id=book_id,
            summary={
                "updateIntervalMinutes": interval_minutes,
                "backlogChapterLimit": int(settings.get("backlogChapterLimit", 25) or 25),
                "policyChanged": policy_changed,
                "currentPolicyVersion": next_policy_version,
            },
            conn=conn,
        )
        conn.commit()

    processing_settings = {
        "updateIntervalMinutes": interval_minutes,
        "backlogChapterLimit": int(settings.get("backlogChapterLimit", 25) or 25),
    }
    return {
        "bookId": book_id,
        "updated": True,
        "policyChanged": policy_changed,
        "currentPolicyVersion": next_policy_version,
        "intervalMinutes": interval_minutes,
        "nextCheckTime": next_check_time,
        "settings": processing_settings,
    }


@console_route("post", "/library-books/{book_id}/rebuild")
async def rebuild_library_book(request: Request, book_id: str, payload: dict | None = None):
    admin = auth_service.require_admin(request)
    import sqlite3
    from app.config import DB_PATH
    from app.storage.db import initialize_database

    initialize_database(DB_PATH)
    payload = payload or {}
    if not bool(payload.get("refetchSources", False)):
        rebuilt = AggregateProcessor().rebuild_book_from_snapshots(book_id)
        if not rebuilt.get("rebuilt"):
            raise HTTPException(status_code=404, detail="书籍不存在")
        audit_service.record(
            action="shared_book.rebuild",
            actor_user_id=admin.user_id,
            actor_role=admin.role,
            target_type="shared_book",
            target_id=book_id,
            summary={"mode": "local_snapshot", "rewrittenChapters": rebuilt.get("rewrittenChapters", 0)},
        )
        return {"bookId": book_id, "rebuilt": True, "mode": "local_snapshot", "result": rebuilt}
    now = _now()
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT aggregate_payload_json, settings_json, current_policy_version, start_chapter_index,
                   primary_book_id, primary_source_id
            FROM aggregate_book_tasks
            WHERE aggregate_book_id = ?
            """,
            (book_id,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="书籍不存在")

        (
            aggregate_payload_json,
            settings_json,
            current_policy_version,
            current_start_index,
            primary_book_id,
            primary_source_id,
        ) = row
        settings = _aggregate_book_settings(settings_json or "")
        new_start_index = max(1, int(payload.get("startChapterIndex", current_start_index or 1) or 1))
        settings["startChapterIndex"] = new_start_index
        if "aiAggregateEnabled" in payload:
            settings["aiAggregateEnabled"] = bool(payload["aiAggregateEnabled"])
        if "aiPurifyEnabled" in payload:
            settings["aiPurifyEnabled"] = bool(payload["aiPurifyEnabled"])
        if "primarySourceMode" in payload:
            settings["primarySourceMode"] = str(payload["primarySourceMode"] or "official")
        _apply_book_source_priority_settings(settings, payload)

        try:
            aggregate_payload = json.loads(aggregate_payload_json or "{}")
        except (json.JSONDecodeError, TypeError) as exc:
            raise HTTPException(status_code=409, detail=f"聚合配置损坏，已保留现有数据：{exc}") from exc
        primary_book_id = str(primary_book_id or aggregate_payload.get("primaryBookId") or "").strip()
        if not primary_book_id:
            raise HTTPException(status_code=409, detail="缺少主书源，已保留现有数据")
        primary_source_id = str(
            primary_source_id or aggregate_payload.get("primarySourceId") or primary_book_id.split(":", 1)[0]
        ).strip()
        try:
            preflight_toc = await BookCatalog().toc(primary_book_id)
            toc_error = AggregateProcessor._toc_fetch_error_message(preflight_toc)
        except Exception as exc:
            toc_error = str(exc)
        if toc_error:
            raise HTTPException(status_code=409, detail=f"目录获取失败，已保留现有数据：{toc_error}")

        sources = aggregate_payload.get("sources") if isinstance(aggregate_payload.get("sources"), list) else []
        aggregate_payload["sources"] = [
            dict(source)
            for source in sources
            if isinstance(source, dict)
            and (
                str(source.get("sourceId", "") or "") == str(primary_source_id or "")
                or library_books_service._is_official(str(source.get("sourceId", "") or ""))
            )
        ]
        conn.execute("DELETE FROM aggregate_chapter_tasks WHERE aggregate_book_id = ?", (book_id,))
        conn.execute("DELETE FROM aggregate_source_snapshots WHERE aggregate_book_id = ?", (book_id,))
        conn.execute("DELETE FROM aggregate_source_snapshot_runs WHERE aggregate_book_id = ?", (book_id,))
        conn.execute(
            "DELETE FROM aggregate_book_sources WHERE aggregate_book_id = ? AND role = 'candidate'",
            (book_id,),
        )
        conn.execute(
            """
            UPDATE aggregate_book_tasks
            SET start_chapter_index = ?, initial_snapshot_last_index = 0, backfill_started = 0,
                total_chapters = 0, processed_chapters = 0, visible_processed_chapters = 0, failed_chapters = 0,
                search_visibility_status = 'hidden', status = 'active', archived_at = NULL,
                settings_json = ?, current_policy_version = ?, auto_archive_on_complete = 0,
                aggregate_payload_json = ?, error_count = 0, last_error = '',
                next_check_time = ?, last_processed_at = NULL, updated_at = ?
            WHERE aggregate_book_id = ?
            """,
            (
                new_start_index,
                json.dumps(settings, ensure_ascii=False),
                int(current_policy_version or 1) + 1,
                json.dumps(aggregate_payload, ensure_ascii=False),
                now,
                now,
                book_id,
            ),
        )
        conn.execute(
            """
            INSERT INTO aggregate_operation_logs
            (aggregate_book_id, actor_user_id, actor_role, operation_type, before_json, after_json, created_at)
            VALUES (?, ?, 'admin', 'rebuild', '', ?, ?)
            """,
            (book_id, admin.user_id, json.dumps(payload, ensure_ascii=False), now),
        )
        audit_service.record(
            action="shared_book.rebuild",
            actor_user_id=admin.user_id,
            actor_role=admin.role,
            target_type="shared_book",
            target_id=book_id,
            summary={"startChapterIndex": new_start_index},
            conn=conn,
        )
        conn.commit()

    book = library_books_service.get_book(book_id)
    if book:
        storage = library_books_service.shared_book_storage
        book_name = str(book.get("name", "") or "").strip()
        author = str(book.get("author", "") or "").strip()
        if book_name:
            metadata = library_books_service.load_shared_metadata(book_id)
            if metadata:
                metadata.pop("sourceMap", None)
                metadata.pop("sourceMapSummary", None)
                storage.atomic_write_json(
                    storage.metadata_path(book_name=book_name, author=author),
                    metadata,
                )
            source_refs_path = storage.source_refs_path(book_name=book_name, author=author)
            if source_refs_path.exists():
                source_refs_path.unlink()

    try:
        source_map_refresh = await _manual_source_map_refresh(book_id, {"force": True})
    except Exception as exc:
        logger.warning("Failed to refresh source map before rebuilding %s", book_id, exc_info=True)
        source_map_refresh = {"ok": False, "bookId": book_id, "error": str(exc)}

    refreshed_payload = library_books_service.load_payload(book_id)
    if refreshed_payload:
        aggregate_payload = refreshed_payload

    # Clear stale chapter files; identity metadata and user subscriptions stay intact.
    if book:
        if book_name:
            shared_dir = storage.shared_book_dir(book_name=book_name, author=author)
            chapters_dir = shared_dir / "chapters"
            sources_dir = shared_dir / "sources"
            chapter_index_path = shared_dir / "chapter_index.json"
            if chapters_dir.exists():
                shutil.rmtree(chapters_dir, ignore_errors=True)
            if sources_dir.exists():
                shutil.rmtree(sources_dir, ignore_errors=True)
            if chapter_index_path.exists():
                chapter_index_path.unlink()

    # Legacy path cleanup (no longer the active storage location).
    legacy_dir = Path(DB_PATH).parent / "novels" / "legadohub_ai_aggregate" / book_id
    if legacy_dir.exists():
        shutil.rmtree(legacy_dir, ignore_errors=True)

    processor = AggregateProcessor()
    processor.enqueue_book(book_id, aggregate_payload)
    bootstrap = await processor.bootstrap_book_until_visible(book_id)
    return {
        "bookId": book_id,
        "rebuilt": True,
        "sourceMapRefresh": source_map_refresh,
        "bootstrap": bootstrap,
    }


@console_route("post", "/library-books/{book_id}/source-map/refresh")
async def refresh_library_book_source_map_console(request: Request, book_id: str, payload: dict | None = None):
    admin = auth_service.require_admin(request)
    result = _manual_source_map_refresh(book_id, payload=payload)
    if asyncio.iscoroutine(result):
        result = await result
    audit_service.record(
        action="shared_book.source_map.refresh",
        actor_user_id=admin.user_id,
        actor_role=admin.role,
        target_type="shared_book",
        target_id=book_id,
        outcome="success" if result.get("ok", True) else "failure",
        summary={"errorCode": result.get("error", "")},
    )
    return result


@console_route("post", "/library-books/{book_id}/repair")
async def repair_library_book_console(request: Request, book_id: str, payload: dict | None = None):
    admin = auth_service.require_admin(request)
    result = _manual_library_book_repair(book_id, payload=payload)
    if asyncio.iscoroutine(result):
        result = await result
    audit_service.record(
        action="shared_book.repair",
        actor_user_id=admin.user_id,
        actor_role=admin.role,
        target_type="shared_book",
        target_id=book_id,
        outcome="success" if result.get("ok", True) else "failure",
        summary={"errorCode": result.get("error", "")},
    )
    return result


@console_route("post", "/library-books/{book_id}/update-check")
async def run_library_book_update_check_console(request: Request, book_id: str):
    admin = auth_service.require_admin(request)
    result = _manual_library_book_update_check(book_id)
    if asyncio.iscoroutine(result):
        result = await result
    audit_service.record(
        action="shared_book.update_check",
        actor_user_id=admin.user_id,
        actor_role=admin.role,
        target_type="shared_book",
        target_id=book_id,
        outcome="success" if result.get("ok", True) else "failure",
        summary={"errorCode": result.get("error", "")},
    )
    return result


@console_route("get", "/library-books/{book_id}/processing-logs")
def list_library_book_processing_logs(
    request: Request,
    book_id: str,
    limit: int = 50,
    offset: int = 0,
):
    """Return recent chapter processing events for a shared book.

    This is the backend feed consumed by the console "subscription processing
    log" panel. It surfaces per-chapter status, selected source, AI usage and
    alignment metadata without leaking raw chapter text.
    """
    auth_service.require_admin(request)
    import sqlite3
    from app.config import DB_PATH
    from app.storage.db import initialize_database
    from app.services.shared_book_runtime import SharedBookProcessLogger

    initialize_database(DB_PATH)
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        book = cur.execute(
            """
            SELECT status, search_visibility_status, name, author
            FROM aggregate_book_tasks
            WHERE aggregate_book_id = ?
            """,
            (book_id,),
        ).fetchone()
        if book is None:
            raise HTTPException(status_code=404, detail="书籍不存在")

        stats = cur.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status = 'processed' THEN 1 ELSE 0 END) AS processed,
                SUM(CASE WHEN status IN ('processed', 'fallback') THEN 1 ELSE 0 END) AS completed,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN status = 'fallback' THEN 1 ELSE 0 END) AS fallback,
                SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS failed
            FROM aggregate_chapter_tasks
            WHERE aggregate_book_id = ?
            """,
            (book_id,),
        ).fetchone()

        rows = cur.execute(
            """
            SELECT
                chapter_id,
                chapter_index,
                title,
                status,
                preview_only,
                content_length,
                source_word_count,
                primary_source_chapter_url,
                fallback_source_id,
                ai_model,
                (ai_prompt_tokens + ai_completion_tokens) AS ai_tokens,
                last_processed_at,
                updated_at,
                error,
                source_alignment_json
            FROM aggregate_chapter_tasks
            WHERE aggregate_book_id = ?
            ORDER BY
                last_processed_at DESC,
                updated_at DESC
            LIMIT ? OFFSET ?
            """,
            (book_id, limit, offset),
        ).fetchall()

        items = []
        for row in rows:
            alignment = {}
            try:
                alignment = json.loads(row["source_alignment_json"] or "{}")
            except Exception:
                pass

            source = row["fallback_source_id"] or "primary"
            if alignment.get("selectedSource"):
                source = alignment["selectedSource"]

            items.append(
                {
                    "chapterId": row["chapter_id"],
                    "chapterIndex": row["chapter_index"],
                    "title": row["title"],
                    "status": row["status"],
                    "previewOnly": bool(row["preview_only"]),
                    "wordCount": row["source_word_count"] or row["content_length"] or 0,
                    "source": source,
                    "aiModel": row["ai_model"] or "",
                    "aiTokens": row["ai_tokens"] or 0,
                    "processedAt": row["last_processed_at"] or row["updated_at"],
                    "error": row["error"] or "",
                    "alignment": {
                        "passed": alignment.get("alignmentPassed"),
                        "reason": alignment.get("alignmentReason"),
                        "titleSimilarity": alignment.get("titleSimilarity"),
                        "previewSimilarity": alignment.get("previewSimilarity"),
                    },
                }
            )

        current = cur.execute(
            """
            SELECT chapter_index, title, status, error, updated_at
            FROM aggregate_chapter_tasks
            WHERE aggregate_book_id = ?
              AND status IN ('pending', 'error')
            ORDER BY chapter_index ASC
            LIMIT 1
            """,
            (book_id,),
        ).fetchone()
        latest = cur.execute(
            """
            SELECT chapter_index, title, status, updated_at
            FROM aggregate_chapter_tasks
            WHERE aggregate_book_id = ?
              AND status IN ('processed', 'fallback')
            ORDER BY chapter_index DESC
            LIMIT 1
            """,
            (book_id,),
        ).fetchone()

        recent_events: list[dict[str, Any]] = []
        latest_event: dict[str, Any] | None = None
        try:
            logger = SharedBookProcessLogger(library_books_service.shared_book_storage)
            raw_events = logger.read(
                book_name=str(book["name"] or ""),
                author=str(book["author"] or ""),
                limit=max(1, min(int(limit or 50), 200)),
                offset=max(0, int(offset or 0)),
                newest_first=True,
            )["items"]
            recent_events = [_sanitize_library_processing_event(item) for item in raw_events]
            latest_event = recent_events[0] if recent_events else None
        except Exception:
            recent_events = []
            latest_event = None

        current_step = (
            "处理失败，等待重试或手动处理"
            if current and current["status"] == "error"
            else ("等待处理" if current else "当前没有待处理章节")
        )
        current_index = current["chapter_index"] if current else None
        current_title = current["title"] if current else ""
        current_status = current["status"] if current else "idle"
        current_error = current["error"] if current else ""
        current_updated_at = current["updated_at"] if current else ""
        if latest_event:
            current_step = latest_event.get("message") or current_step
            current_index = latest_event.get("chapterIndex") or current_index
            current_title = latest_event.get("title") or current_title
            current_status = latest_event.get("status") or current_status
            current_error = latest_event.get("error") or current_error
            current_updated_at = latest_event.get("ts") or current_updated_at

        return {
            "bookId": book_id,
            "bookStatus": book["status"],
            "searchVisibilityStatus": book["search_visibility_status"],
            "current": {
                "chapterIndex": current_index,
                "title": current_title,
                "status": current_status,
                "step": current_step,
                "error": current_error,
                "updatedAt": current_updated_at,
            },
            "next": {
                "chapterIndex": current["chapter_index"] if current else None,
                "title": current["title"] if current else "",
            },
            "latestCompleted": {
                "chapterIndex": latest["chapter_index"] if latest else None,
                "title": latest["title"] if latest else "",
                "status": latest["status"] if latest else "",
                "updatedAt": latest["updated_at"] if latest else "",
            },
            "stats": {
                "total": stats["total"] or 0,
                "processed": stats["processed"] or 0,
                "completed": stats["completed"] or 0,
                "pending": stats["pending"] or 0,
                "fallback": stats["fallback"] or 0,
                "failed": stats["failed"] or 0,
            },
            "items": items,
            "recentEvents": recent_events,
            "limit": limit,
            "offset": offset,
        }


@console_route("get", "/library-books/{book_id}/logs/stream")
async def stream_library_book_logs(request: Request, book_id: str):
    """SSE stream of shared-book processing logs (tail -f style)."""
    auth_service.require_admin(request)
    from app.services.shared_book_runtime import SharedBookProcessLogger

    book = library_books_service.get_book(book_id)
    if not book:
        raise HTTPException(status_code=404, detail="书籍不存在")

    book_name = str(book.get("name", "") or "").strip()
    author = str(book.get("author", "") or "").strip()
    storage = library_books_service.shared_book_storage
    logger = SharedBookProcessLogger(storage)

    async def event_generator():
        async for record in logger.tail_stream(book_name=book_name, author=author):
            yield f"data: {json.dumps(record, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@console_route("get", "/users")
def list_users(request: Request):
    auth_service.require_admin(request)
    items = auth_service.list_users()
    return {"items": items, "total": len(items)}


def _subscription_links_for_access_code(access_code: str, request: Request) -> dict:
    """Attach personal source/subscription URLs for a just-issued access code."""
    from app.core.public_security import (
        effective_public_base_url,
        ensure_reader_entrypoint_origin,
        get_public_base_url,
        is_lan_reading_base,
    )

    public = ensure_reader_entrypoint_origin(effective_public_base_url())
    try:
        request_base = ensure_reader_entrypoint_origin(get_public_base_url(request), request=request)
    except Exception:
        request_base = ""
    lan = ""
    if request_base and is_lan_reading_base(request_base):
        lan = request_base
    if not public and request_base and not is_lan_reading_base(request_base):
        public = request_base
    return auth_service.build_access_subscription_links(
        access_code,
        public_base=public,
        lan_base=lan,
    )


@console_route("post", "/users")
def create_user(request: Request, payload: dict):
    admin = auth_service.require_admin(request)
    _reject_unknown_fields(payload, {"username", "password", "role"}, label="创建用户")
    role = str(payload.get("role", "user"))
    username = str(payload.get("username", "")).strip()
    if role == "user":
        if str(payload.get("password", "")):
            raise HTTPException(status_code=422, detail="普通用户由系统生成授权码，不能提交密码")
        result = auth_service.create_access_user(
            username=username,
            actor_user_id=admin.user_id,
            actor_role=admin.role,
        )
        code = str(result.get("accessCode") or "")
        if code:
            result.update(_subscription_links_for_access_code(code, request))
        return result
    return auth_service.create_user(
        username=username,
        password=str(payload.get("password", "")),
        role=role,
        actor_user_id=admin.user_id,
        actor_role=admin.role,
    )


@console_route("post", "/users/{user_id}/reset-password")
def reset_user_password(request: Request, user_id: str, payload: dict):
    admin = auth_service.require_admin(request)
    _reject_unknown_fields(payload, {"password"}, label="重置密码")
    target = auth_service.get_user(user_id)
    if target and not target.is_admin:
        raise HTTPException(status_code=400, detail="普通用户必须重置授权码")
    if target and target.user_id == admin.user_id:
        raise HTTPException(status_code=409, detail="当前账户请在账户安全中修改密码")
    return auth_service.reset_password(
        user_id,
        str(payload.get("password", "")),
        actor_user_id=admin.user_id,
        actor_role=admin.role,
    )


@console_route("post", "/users/{user_id}/reset-access-code")
def reset_user_access_code(request: Request, user_id: str, payload: dict | None = None):
    admin = auth_service.require_admin(request)
    _reject_unknown_fields(payload or {}, set(), label="重置授权码")
    result = auth_service.reset_access_code(
        user_id,
        actor_user_id=admin.user_id,
        actor_role=admin.role,
    )
    code = str(result.get("accessCode") or "")
    if code:
        result.update(_subscription_links_for_access_code(code, request))
    return result


@console_route("post", "/users/{user_id}/revoke-sessions")
def revoke_user_sessions(request: Request, user_id: str, payload: dict | None = None):
    admin = auth_service.require_admin(request)
    _reject_unknown_fields(payload or {}, set(), label="撤销会话")
    return auth_service.revoke_user_sessions(
        user_id,
        actor_user_id=admin.user_id,
        actor_role=admin.role,
    )


@console_route("post", "/users/{user_id}/disable")
def disable_user(request: Request, user_id: str, payload: dict | None = None):
    admin = auth_service.require_admin(request)
    payload = payload or {}
    _reject_unknown_fields(payload, {"disabled"}, label="用户状态")
    disabled = True if "disabled" not in payload else _parse_bool_field(payload["disabled"], field="disabled")
    return auth_service.set_disabled(
        user_id,
        disabled,
        actor_user_id=admin.user_id,
        actor_role=admin.role,
    )


@console_route("delete", "/users/{user_id}")
def delete_user(request: Request, user_id: str):
    admin = auth_service.require_admin(request)
    return auth_service.delete_user(
        user_id,
        actor_user_id=admin.user_id,
    )


# ---- Progress ----

@console_route("get", "/progress")
def get_progress():
    from app.services.plugin_runtime_state import get_runtime_state

    runtime_state = get_runtime_state()
    plugin_count = len(_plugin_scheduler._plugins)
    enabled_plugin_count = sum(1 for p in _plugin_scheduler._plugins.values() if p.metadata.enabled)
    proxy_needed_count = sum(
        1 for p in _plugin_scheduler._plugins.values()
        if bool((p.metadata.proxy or {}).get("required"))
    )
    healthy_count = 0
    for p in _plugin_scheduler._plugins.values():
        state = runtime_state.get_state(p.metadata.id)
        last_ping = state.get("lastPing") or {}
        if last_ping.get("status") == "reachable":
            healthy_count += 1
    plugin_stats = {
        "total": plugin_count,
        "enabled": enabled_plugin_count,
        "healthy": healthy_count,
        "disabled": plugin_count - enabled_plugin_count,
        "proxyNeeded": proxy_needed_count,
    }
    progress = {
        "pluginStats": plugin_stats,
        "configured_sources": plugin_count,
        "enabled_sources": enabled_plugin_count,
        "healthy_sources": healthy_count,
        "proxy_sources": proxy_needed_count,
        "unsupported_sources": 0,
        "plugin_count": plugin_count,
        "enabled_plugin_count": enabled_plugin_count,
    }
    update_progress(progress)
    config = load_aggregate_config()
    return {
        "aggregate": config.get("parser_progress", {}),
        "pluginStats": plugin_stats,
        "sources": plugin_stats,
        "plugins": {
            "total": plugin_count,
            "enabled": enabled_plugin_count,
            "healthy": healthy_count,
        },
    }


# ---- Rule Engines ----


@console_route("get", "/rule-engines")
def list_rule_engines():
    # Return plugin capabilities instead of legacy engine report
    plugins = _plugin_scheduler._plugins
    return {
        "engines": [
            {
                "id": p.metadata.id,
                "name": p.metadata.name,
                "author": p.metadata.author,
                "capabilities": p.capabilities,
                "contractVersion": p.metadata.contract_version,
            }
            for p in plugins.values()
        ]
    }


# ---- Lexicon ----

@console_route("post", "/lexicon/update")
async def update_lexicon(request: Request):
    """Download/update the sensitive-word lexicon from the upstream repository."""
    auth_service.require_admin(request)
    updater = LexiconUpdater()
    result = await run_in_threadpool(updater.check_and_update)
    return {
        "success": result.success,
        "fileCount": result.file_count,
        "wordCount": result.word_count,
        "commitSha": result.commit_sha,
        "error": result.error,
    }


@console_route("get", "/lexicon/status")
async def get_lexicon_status(request: Request):
    """Return the currently installed sensitive-word lexicon metadata."""
    auth_service.require_admin(request)
    meta = LexiconUpdater().load_meta()
    return {
        "sourceRepo": meta.source_repo,
        "branch": meta.branch,
        "commitSha": meta.commit_sha,
        "updatedAt": meta.updated_at,
        "fileCount": meta.file_count,
        "wordCount": meta.word_count,
        "lastError": meta.last_error,
    }


# ---- Status (for console dashboard) ----

@console_route("get", "/status")
def get_status():
    # Count from actually loaded plugins so numbers always match the plugin list
    plugins = _plugin_scheduler._plugins
    total = len(plugins)
    enabled_plugins = [plugin for plugin in plugins.values() if plugin.metadata.enabled]
    enabled = len(enabled_plugins)
    disabled = total - enabled
    from app.services.plugin_runtime_state import get_runtime_state

    runtime_state = get_runtime_state()
    healthy = 0
    unhealthy = 0
    for p in enabled_plugins:
        state = runtime_state.get_state(p.metadata.id)
        last_ping = state.get("lastPing") or {}
        if last_ping.get("status") == "reachable":
            healthy += 1
        elif last_ping.get("status") == "unreachable":
            unhealthy += 1
    checked = healthy + unhealthy
    unknown = enabled - checked
    health = (
        "degraded"
        if unhealthy > 0
        else "pending"
        if unknown > 0
        else "idle"
        if enabled == 0
        else "healthy"
    )
    plugin_stats = {
        "total": total,
        "enabled": enabled,
        "disabled": disabled,
        "healthy": healthy,
        "unhealthy": unhealthy,
        "checked": checked,
        "unknown": unknown,
    }
    return {
        "health": health,
        "uptimeSeconds": max(0, int(time.monotonic() - _CONSOLE_STARTED_AT)),
        "pluginStats": plugin_stats,
        "sourceStats": plugin_stats,  # compatibility alias
        "plugins": {  # compatibility alias
            "total": total,
            "enabled": enabled,
            "disabled": disabled,
            "healthy": healthy,
            "unhealthy": unhealthy,
            "checked": checked,
            "unknown": unknown,
        },
        "version": APP_VERSION,
        "phase": APP_PHASE,
    }


def _now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()
