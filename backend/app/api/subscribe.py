"""Subscription/library APIs for shared aggregate books.

Copyright (c) 2026 moo. All rights reserved.
"""

from __future__ import annotations

import asyncio
import logging
import re
import sqlite3
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Request

from app.core.legado_source import generate_legado_source
from app.services.aggregate_processor import AggregateProcessor
from app.services.aggregate_virtual_source import VIRTUAL_SOURCE_ID, unpack_aggregate_chapter_url
from app.services.library_books import library_books_service
from app.services.shared_book_job_types import SharedBookJobType
from app.services.shared_book_scheduler import SharedBookScheduler
from app.services.subscription_search import subscription_search_service
from app.services.user_subscriptions import (
    SubscriptionLimitError,
    subscription_rate_limiter,
    user_subscriptions_service,
)
from app.services.user_auth import auth_service
from app.source_plugins.id_codec import decode_book_id, decode_chapter_id
from app.core.public_security import get_public_base_url, reading_network_lane
from app.services.reading_limits import reading_access_limiter
from app.services.search_jobs import SearchJobService

router = APIRouter(prefix="/api/subscribe")
public_router = APIRouter(prefix="/api/subscribe")
logger = logging.getLogger(__name__)
_shared_book_creation_lock = threading.Lock()
_MAX_SUBSCRIPTION_SEARCH_PAGE = 1000
_MAX_LIBRARY_CHAPTER_PAGE = 100_000
_MAX_LIBRARY_CHAPTER_PAGE_SIZE = 200
_LEGADO_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")
_TERMINAL_SEARCH_STATUSES = {"completed", "partial", "timed_out", "failed", "cancelled"}
# Legado book-source search is one HTTP call per page. Progressive UX:
# page1 short-wait → return library + first remotes; page2+ short-wait → new remotes;
# job hard-stop at 120s. Do not hold a single page for the full 120s.
_READING_SEARCH_TIMEOUT_MS = 120_000
_READING_SEARCH_PAGE1_WAIT_MS = 6_000
_READING_SEARCH_FOLLOW_WAIT_MS = 20_000
_READING_SEARCH_POLL_SECONDS = 0.1
_MAX_READING_SEARCH_OWNERS = 1024
_legado_search_service: SearchJobService | None = None
_legado_search_service_init_token = None
_legado_search_owners: dict[str, set[str]] = {}
_legado_search_owners_lock = threading.Lock()
# user_id + keyword -> progressive third-party job state
_legado_search_progress: dict[str, dict[str, Any]] = {}
_legado_search_progress_lock = threading.Lock()
_LIBRARY_CHAPTER_STATUSES = {
    "all",
    "pending",
    "placeholder",
    "processing",
    "unknown",
    "readable",
    "supplemented",
    "proofread_complete",
    "fetched",
    "preview",
    "suspect",
    "failed",
    "error",
    "fallback",
    "processed",
    "skipped",
}


@asynccontextmanager
async def _shared_book_creation_guard():
    # ponytail: process-wide polling lock; replace with a DB reservation for multi-process deployment.
    while not _shared_book_creation_lock.acquire(blocking=False):
        await asyncio.sleep(0.01)
    try:
        yield
    finally:
        _shared_book_creation_lock.release()

_USER_BOOK_FIELDS = {
    "aggregateBookId",
    "name",
    "author",
    "coverUrl",
    "intro",
    "wordCount",
    "bookStatus",
    "totalChapters",
    "processedChapters",
    "visibleProcessedChapters",
    "failedChapters",
    "status",
    "searchVisibilityStatus",
    "lastSourceChapterTitle",
    "lastLocalChapterTitle",
    "createdAt",
    "updatedAt",
}


def _limit_detail(exc: SubscriptionLimitError) -> dict:
    detail = {"code": exc.code, "message": str(exc), "retryable": exc.retryable}
    if exc.retry_after_seconds is not None:
        detail["retryAfterSeconds"] = exc.retry_after_seconds
    return detail


def _limit_exception(exc: SubscriptionLimitError) -> HTTPException:
    headers = (
        {"Retry-After": str(exc.retry_after_seconds)}
        if exc.retry_after_seconds is not None
        else None
    )
    return HTTPException(status_code=429, detail=_limit_detail(exc), headers=headers)


def _start_chapter_index(payload: dict, default: int = 1) -> int:
    value = payload.get("startChapterIndex", default)
    if isinstance(value, bool):
        raise HTTPException(status_code=422, detail="startChapterIndex 必须是大于等于 1 的整数")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and value.strip().isdigit():
        parsed = int(value.strip())
    else:
        raise HTTPException(status_code=422, detail="startChapterIndex 必须是大于等于 1 的整数")
    if parsed < 1:
        raise HTTPException(status_code=422, detail="startChapterIndex 必须是大于等于 1 的整数")
    return parsed


def _auto_archive_on_complete(payload: dict, default: bool = True) -> bool:
    value = payload.get("autoArchiveOnComplete", default)
    if not isinstance(value, bool):
        raise HTTPException(status_code=422, detail="autoArchiveOnComplete 必须是布尔值")
    return value


def _search_page(payload: dict) -> int:
    value = payload.get("page", 1)
    if value is None or value == "":
        return 1
    if isinstance(value, bool):
        raise HTTPException(status_code=422, detail=f"page 必须是 1 到 {_MAX_SUBSCRIPTION_SEARCH_PAGE} 的整数")
    if isinstance(value, int):
        page = value
    elif isinstance(value, str) and value.strip().isdigit():
        page = int(value.strip())
    else:
        raise HTTPException(status_code=422, detail=f"page 必须是 1 到 {_MAX_SUBSCRIPTION_SEARCH_PAGE} 的整数")
    if not 1 <= page <= _MAX_SUBSCRIPTION_SEARCH_PAGE:
        raise HTTPException(status_code=422, detail=f"page 必须是 1 到 {_MAX_SUBSCRIPTION_SEARCH_PAGE} 的整数")
    return page


def _reject_legado_query_anomalies(request: Request, allowed: set[str]) -> None:
    unknown = set(request.query_params.keys()) - allowed
    repeated = {key for key in allowed if len(request.query_params.getlist(key)) > 1}
    if unknown or repeated:
        raise HTTPException(status_code=422, detail="查询参数无效")


def _validated_legado_text(value: str, *, field: str, max_length: int) -> str:
    normalized = str(value or "").strip()
    if len(normalized) > max_length or any(ord(character) < 32 for character in normalized):
        raise HTTPException(status_code=422, detail=f"{field} 长度或内容无效")
    return normalized


def _validated_legado_identifier(value: str, *, field: str, allow_empty: bool = False) -> str:
    normalized = str(value or "").strip()
    if not normalized and allow_empty:
        return ""
    if not normalized or len(normalized) > 128 or not _LEGADO_IDENTIFIER_PATTERN.fullmatch(normalized):
        raise HTTPException(status_code=422, detail=f"{field} 无效")
    return normalized


def _legado_query_int(value: str, *, field: str, minimum: int, maximum: int) -> int:
    normalized = str(value or "").strip()
    if not normalized.isascii() or not normalized.isdigit():
        raise HTTPException(status_code=422, detail=f"{field} 必须是整数")
    parsed = int(normalized)
    if not minimum <= parsed <= maximum:
        raise HTTPException(status_code=422, detail=f"{field} 超出允许范围")
    return parsed


def _get_legado_search_service() -> SearchJobService:
    global _legado_search_service, _legado_search_service_init_token
    init_token = SearchJobService.__init__
    if _legado_search_service is None or _legado_search_service_init_token is not init_token:
        _legado_search_service = SearchJobService()
        _legado_search_service_init_token = init_token
        with _legado_search_owners_lock:
            _legado_search_owners.clear()
    return _legado_search_service


def _third_party_search_source_ids(search_service: SearchJobService) -> list[str]:
    scheduler = search_service.scheduler
    plugins = scheduler._search_priority_plugins(scheduler._enabled_plugins())
    return [
        plugin.metadata.id
        for plugin in plugins
        if "search" in plugin.capabilities and not plugin.metadata.is_official_source()
    ]


def _remember_legado_search_owner(job_id: str, user_id: str) -> None:
    with _legado_search_owners_lock:
        owners = _legado_search_owners.setdefault(job_id, set())
        owners.add(user_id)
        while len(_legado_search_owners) > _MAX_READING_SEARCH_OWNERS:
            oldest_job_id = next(iter(_legado_search_owners))
            if oldest_job_id == job_id and len(_legado_search_owners) == 1:
                break
            _legado_search_owners.pop(oldest_job_id, None)


def _owns_legado_search(job_id: str, user_id: str) -> bool:
    with _legado_search_owners_lock:
        return user_id in _legado_search_owners.get(job_id, set())


def _public_search_text(value: object, *, max_length: int) -> str:
    return str(value or "").strip()[:max_length]


def _lane_source_name(source_name: str, *, lane: str, is_aggregate: bool) -> str:
    """Keep LAN aggregate results visually distinct from the public channel.

    Third-party plugin names stay unchanged; only Hub library/aggregate rows get
    the ·内网 mark so multi-source UI does not look like one fused catalog.
    """
    name = (source_name or "").strip()
    if lane != "lan" or not is_aggregate:
        return name
    if "·内网" in name or name.endswith("内网"):
        return name
    return f"{name}·内网" if name else "LegadoHub·内网"


def _public_legado_search_item(
    item: dict,
    *,
    base_api: str,
    allowed_source_ids: set[str],
) -> dict | None:
    source_id = _public_search_text(item.get("sourceId"), max_length=128)
    if source_id != VIRTUAL_SOURCE_ID and source_id not in allowed_source_ids:
        return None
    book_id = _public_search_text(item.get("bookId"), max_length=8192)
    try:
        encoded_source_id, _ = decode_book_id(book_id)
    except Exception:
        return None
    if encoded_source_id != source_id:
        return None

    lane = reading_network_lane(base_api)
    is_aggregate = source_id == VIRTUAL_SOURCE_ID
    source_name = _lane_source_name(
        _public_search_text(
            item.get("readingSourceName") or item.get("sourceName") or source_id,
            max_length=200,
        ),
        lane=lane,
        is_aggregate=is_aggregate,
    )
    last_chapter = _public_search_text(item.get("lastChapter"), max_length=500)
    reading_last_chapter = _public_search_text(
        item.get("readingLastChapter")
        or " · ".join(part for part in (source_name, last_chapter) if part),
        max_length=800,
    )
    # Lane-scoped bookUrl host already differs; for multi-source fusion, also
    # stamp a stable lane token into the public bookUrl query so Reading does
    # not collapse LAN/public rows that share the same bookId path.
    book_url = f"{base_api}/api/legado/book/{book_id}"
    if lane == "lan":
        book_url = f"{book_url}?lane=lan"
    public_item = {
        "displayType": "aggregate" if is_aggregate else "source",
        "resultKind": "aggregate" if is_aggregate else "source",
        "sourceId": source_id,
        "sourceName": source_name,
        "readingSourceName": source_name,
        "name": _public_search_text(item.get("name"), max_length=500),
        "author": _public_search_text(item.get("author"), max_length=300),
        "coverUrl": _public_search_text(item.get("coverUrl"), max_length=4096),
        "intro": _public_search_text(item.get("intro"), max_length=6000),
        "kind": _public_search_text(item.get("kind"), max_length=500),
        "lastChapter": last_chapter,
        "readingLastChapter": reading_last_chapter,
        "wordCount": item.get("wordCount", ""),
        "bookId": book_id,
        "bookUrl": book_url,
        "networkLane": lane,
    }
    if source_id == VIRTUAL_SOURCE_ID:
        for key in (
            "aggregateBookId",
            "libraryStatus",
            "searchVisibilityStatus",
            "processedChapters",
            "visibleProcessedChapters",
            "totalChapters",
        ):
            if key in item:
                public_item[key] = item[key]
    return public_item


def _collect_library_search_items(
    *,
    keyword: str,
    page: int,
    base_api: str,
    user_id: str,
) -> list[dict]:
    """Subscription library first, then published shared books (same keyword).

    Only used on Legado search page=1 so later pages can carry third-party hits
    without re-emitting the same library rows.
    """
    raw_items: list[dict] = []
    if not keyword.strip() or page > 1:
        return raw_items
    try:
        subscribed_books = user_subscriptions_service.list_books(
            user_id,
            library_books_service,
            keyword=keyword,
        )
        raw_items.extend(
            library_books_service.build_search_injected_item(book, base_api=base_api)
            for book in subscribed_books
            if isinstance(book.get("subscription"), dict)
            and book["subscription"].get("status") in {"active", "paused"}
            and book.get("searchVisibilityStatus") == "visible"
            and int(book.get("visibleProcessedChapters", 0) or 0) > 0
        )
    except Exception:
        logger.exception("Reading subscribed-book search failed")

    try:
        published = library_books_service.page_published_books(
            keyword=keyword,
            page=1,
            page_size=20,
        )
        raw_items.extend(
            library_books_service.build_search_injected_item(book, base_api=base_api)
            for book in published["items"]
        )
    except Exception:
        logger.exception("Reading published-book search failed")
    return raw_items


def _publicize_search_items(
    raw_items: list[dict],
    *,
    base_api: str,
    allowed_source_ids: set[str],
) -> list[dict]:
    items: list[dict] = []
    seen_book_ids: set[str] = set()
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        public_item = _public_legado_search_item(
            item,
            base_api=base_api,
            allowed_source_ids=allowed_source_ids,
        )
        if public_item is None:
            continue
        book_id = public_item["bookId"]
        if book_id in seen_book_ids:
            continue
        seen_book_ids.add(book_id)
        items.append(public_item)
    return items


def _legado_search_payload(
    *,
    keyword: str,
    page: int,
    base_api: str,
    user_id: str,
    allowed_source_ids: set[str],
    snapshot: dict | None = None,
    library_items: list[dict] | None = None,
    third_party_items: list[dict] | None = None,
) -> dict:
    if third_party_items is None:
        snapshot_items = (snapshot or {}).get("items", [])
        third_party_items = [
            item
            for item in snapshot_items
            if isinstance(item, dict) and item.get("sourceId") != VIRTUAL_SOURCE_ID
        ] if isinstance(snapshot_items, list) else []
    raw_items: list[dict] = []
    if library_items is not None:
        raw_items.extend(library_items)
    elif keyword.strip() and page <= 1:
        raw_items.extend(
            _collect_library_search_items(
                keyword=keyword,
                page=1,
                base_api=base_api,
                user_id=user_id,
            )
        )

    raw_items.extend(third_party_items or [])
    items = _publicize_search_items(
        raw_items,
        base_api=base_api,
        allowed_source_ids=allowed_source_ids,
    )

    return {
        "implemented": True,
        "keyword": keyword,
        "page": page,
        "items": items,
        "jobId": str((snapshot or {}).get("jobId", "") or ""),
        "status": str((snapshot or {}).get("status", "completed") or "completed"),
        "liveSearchPending": bool((snapshot or {}).get("liveSearchPending", False)),
    }


def _progress_key(user_id: str, keyword: str, *, lane: str = "public") -> str:
    """Per-user, per-keyword, per-network-lane search pagination state.

    LAN and public book sources must not share page2 continuation / emitted_ids,
    or dual-import multi-source search fuses progressive batches.
    """
    lane_key = "lan" if str(lane or "").strip().lower() == "lan" else "public"
    return f"{user_id}\n{keyword.strip().casefold()}\n{lane_key}"


def _legado_search_mode_for_lane(lane: str) -> str:
    """Isolate SearchCoordinator sessions by network lane without changing plugins."""
    return "source_lan" if str(lane or "").strip().lower() == "lan" else "source"


def _third_party_snapshot_items(
    search_service: SearchJobService,
    job_id: str,
    *,
    base_api: str,
) -> tuple[list[dict], dict | None]:
    snapshot = search_service.session_snapshot(
        job_id,
        base_api=base_api,
        include_official_sources=False,
    )
    if not isinstance(snapshot, dict):
        return [], None
    items = [
        item
        for item in (snapshot.get("items") or [])
        if isinstance(item, dict) and item.get("sourceId") != VIRTUAL_SOURCE_ID
    ]
    return items, snapshot


def _item_book_id(item: dict, *, base_api: str, allowed_source_ids: set[str]) -> str:
    public_item = _public_legado_search_item(
        item,
        base_api=base_api,
        allowed_source_ids=allowed_source_ids,
    )
    return str((public_item or {}).get("bookId") or "")


async def _wait_for_legado_search(
    search_service: SearchJobService,
    job_id: str,
    wait_ms: int,
    *,
    stop_on_first_result: bool = False,
    known_book_ids: set[str] | None = None,
    base_api: str = "",
    allowed_source_ids: set[str] | None = None,
) -> str:
    """Wait briefly for third-party progress on one Legado search page.

    Returns:
    - ``ready``: new book ids appeared (or first hit when known_book_ids empty)
    - ``terminal``: job finished
    - ``deadline``: page wait elapsed
    """
    known = known_book_ids or set()
    allowed = allowed_source_ids or set()
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(0, wait_ms) / 1000
    while True:
        session = search_service.get_session(job_id)
        if session is None or session.status in _TERMINAL_SEARCH_STATUSES:
            return "terminal"
        if stop_on_first_result:
            live = getattr(session, "live_items", None) or []
            if isinstance(live, list) and live:
                if not known:
                    return "ready"
                for item in live:
                    if not isinstance(item, dict):
                        continue
                    book_id = _item_book_id(
                        item, base_api=base_api, allowed_source_ids=allowed
                    )
                    if book_id and book_id not in known:
                        return "ready"
        remaining = deadline - loop.time()
        if remaining <= 0:
            return "deadline"
        await asyncio.sleep(min(_READING_SEARCH_POLL_SECONDS, remaining))


async def _enforce_legado_search_deadline(
    search_service: SearchJobService,
    job_id: str,
    wait_ms: int,
) -> None:
    try:
        await asyncio.sleep(max(0, wait_ms) / 1000)
        session = search_service.get_session(job_id)
        if session is None or session.status in _TERMINAL_SEARCH_STATUSES:
            return
        search_service.cancel_job(job_id)
    except Exception:
        logger.debug("Failed to enforce reading search deadline for %s", job_id, exc_info=True)


def _track_legado_deadline_task(task: asyncio.Task[None]) -> None:
    # Keep a strong ref so the hard-stop task is not GC'd mid-flight.
    if not hasattr(_track_legado_deadline_task, "_tasks"):
        _track_legado_deadline_task._tasks = set()  # type: ignore[attr-defined]
    tasks: set[asyncio.Task[None]] = _track_legado_deadline_task._tasks  # type: ignore[attr-defined]
    tasks.add(task)
    task.add_done_callback(tasks.discard)


def _validated_aggregate_book_id(value: str) -> str:
    return _validated_legado_identifier(value, field="aggregateBookId")


async def _create_or_get_book_for_subscription(group: dict, user_id: str, existing: dict | None) -> dict:
    current_subscription = (
        user_subscriptions_service.get(user_id, existing["aggregateBookId"])
        if existing else None
    )
    if not current_subscription or current_subscription.get("status") == "archived":
        user_subscriptions_service.check_capacity(
            user_id, creates_shared_book=existing is None
        )
    try:
        return await library_books_service.create_or_get_shared_book(
            group, actor_user_id=user_id
        )
    except sqlite3.IntegrityError:
        raced_book = library_books_service.find_existing_book(group)
        if not raced_book:
            raise
        return {"created": False, "book": raced_book}


def _safe_user_book(book: dict | None) -> dict:
    source = book or {}
    result = {key: source[key] for key in _USER_BOOK_FIELDS if key in source}
    for key in ("subscription", "personalProgress", "provisioning", "bookState"):
        value = source.get(key)
        if isinstance(value, dict):
            result[key] = dict(value)
    return result


def _require_book_access(user, aggregate_book_id: str) -> dict | None:
    if user.role == "admin":
        return user_subscriptions_service.get(user.user_id, aggregate_book_id)
    subscription = user_subscriptions_service.get(user.user_id, aggregate_book_id)
    if not subscription:
        raise HTTPException(status_code=404, detail="书籍不存在")
    return subscription


def _book_needs_subscription_wake(
    book: dict,
    provisioning: dict,
) -> bool:
    shared_status = str(book.get("status", "") or "active").lower()
    if shared_status in {"paused", "archived"}:
        return False
    return bool(
        book.get("searchVisibilityStatus") != "visible"
        or shared_status == "error"
        or not provisioning.get("firstReadableChapter")
    )


async def _run_subscription_wake(scheduler: SharedBookScheduler) -> None:
    try:
        await scheduler.run_manual_until_idle()
    except Exception:
        logger.warning("Subscription provisioning runner failed", exc_info=True)


def _schedule_subscription_wake(
    book: dict,
    *,
    payload: dict | None,
    provisioning: dict,
) -> bool:
    """Wake shared processing without changing any personal subscription setting."""
    if not _book_needs_subscription_wake(book, provisioning):
        return False
    aggregate_book_id = str(book.get("aggregateBookId", "") or "")
    processing_payload = (
        dict(payload)
        if isinstance(payload, dict) and payload
        else library_books_service.load_payload(aggregate_book_id)
    )
    if not aggregate_book_id or not processing_payload:
        logger.warning(
            "Skipped subscription wake because processing payload is missing: bookId=%s",
            aggregate_book_id,
        )
        return False
    try:
        processor = AggregateProcessor()
        enqueue_result = processor.enqueue_book(
            aggregate_book_id,
            processing_payload,
            next_check_time=datetime.now(timezone.utc).isoformat(),
        )
        if not enqueue_result.get("queued"):
            return False
        scheduler = SharedBookScheduler(processor=processor)
        if (
            book.get("searchVisibilityStatus") != "visible"
            or not provisioning.get("firstReadableChapter")
        ):
            scheduler.enqueue_initial_subscription(
                aggregate_book_id,
                payload=processing_payload,
                book_name=book.get("name", ""),
                author=book.get("author", ""),
            )
        else:
            scheduler.enqueue_manual_update(
                aggregate_book_id,
                reason=SharedBookJobType.BOOK_UPDATE_CHECK.value,
                payload=processing_payload,
                book_name=book.get("name", ""),
                author=book.get("author", ""),
            )
        asyncio.create_task(_run_subscription_wake(scheduler))
        return True
    except Exception:
        logger.warning(
            "Failed to wake shared processing for %s",
            aggregate_book_id,
            exc_info=True,
        )
        return False


@router.post("/search")
@public_router.post("/search")
async def subscription_search(request: Request, payload: dict):
    user = auth_service.require_user(request)
    unknown = set(payload) - {"keyword", "page"}
    if unknown:
        raise HTTPException(status_code=422, detail=f"不支持的字段: {', '.join(sorted(unknown))}")
    raw_keyword = payload.get("keyword", "")
    if not isinstance(raw_keyword, str):
        raise HTTPException(status_code=422, detail="keyword 必须是字符串")
    keyword = raw_keyword.strip()
    if len(keyword) > 200 or any(ord(character) < 32 for character in keyword):
        raise HTTPException(status_code=422, detail="keyword 长度或内容无效")
    page = _search_page(payload)
    if not keyword:
        return {
            "implemented": True,
            "jobId": "",
            "keyword": keyword,
            "page": page,
            "cards": [],
            "status": "completed",
            "liveSearchPending": False,
        }
    try:
        subscription_rate_limiter.check(user.user_id, "search")
    except SubscriptionLimitError as exc:
        raise _limit_exception(exc) from exc

    job = subscription_search_service.create_job(
        keyword=keyword, page=page, owner_user_id=user.user_id
    )
    snapshot = subscription_search_service.snapshot(job.job_id)
    snapshot["viewer"] = {"userId": user.user_id, "role": user.role}
    return snapshot


@router.get("/search/{job_id}")
@public_router.get("/search/{job_id}")
def get_subscription_search(request: Request, job_id: str):
    user = auth_service.require_user(request)
    if not subscription_search_service.get_job_for_user(job_id, user.user_id):
        raise HTTPException(status_code=404, detail="搜索任务不存在")
    return subscription_search_service.snapshot(job_id)


@router.post("/search/{job_id}/cards/{candidate_id}/subscribe")
@public_router.post("/search/{job_id}/cards/{candidate_id}/subscribe")
async def subscribe_candidate(request: Request, job_id: str, candidate_id: str, payload: dict | None = None):
    user = auth_service.require_user(request)
    group = subscription_search_service.find_card_group_for_user(
        job_id, candidate_id, user.user_id
    )
    if not group:
        raise HTTPException(status_code=404, detail="候选书籍不存在")
    payload = payload or {}
    unknown = set(payload) - {"startChapterIndex", "autoArchiveOnComplete"}
    if unknown:
        raise HTTPException(status_code=422, detail=f"不支持的字段: {', '.join(sorted(unknown))}")
    start_chapter_index = _start_chapter_index(payload)
    auto_archive = _auto_archive_on_complete(payload)
    try:
        subscription_rate_limiter.check(user.user_id, "create")
    except SubscriptionLimitError as exc:
        raise _limit_exception(exc) from exc
    existing = library_books_service.find_existing_book(group)
    previous_subscription = (
        user_subscriptions_service.get(user.user_id, existing["aggregateBookId"])
        if existing
        else None
    )
    try:
        if existing is None:
            async with _shared_book_creation_guard():
                existing = library_books_service.find_existing_book(group)
                created = await _create_or_get_book_for_subscription(
                    group, user.user_id, existing
                )
                subscription, subscription_created = user_subscriptions_service.ensure(
                    user.user_id,
                    created["book"]["aggregateBookId"],
                    start_chapter_index=start_chapter_index,
                    auto_archive_on_complete=auto_archive,
                )
        else:
            created = await _create_or_get_book_for_subscription(
                group, user.user_id, existing
            )
            subscription, subscription_created = user_subscriptions_service.ensure(
                user.user_id,
                created["book"]["aggregateBookId"],
                start_chapter_index=start_chapter_index,
                auto_archive_on_complete=auto_archive,
            )
    except SubscriptionLimitError as exc:
        raise _limit_exception(exc) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    book = created["book"]
    safe_book = _safe_user_book(book)
    provisioning = library_books_service.provisioning_summary(
        book["aggregateBookId"],
        start_chapter_index=subscription["startChapterIndex"],
    )
    subscription_activated = bool(
        created.get("created")
        or subscription_created
        or not previous_subscription
        or previous_subscription.get("status") != "active"
    )
    wake_requested = bool(
        subscription_activated
        and _schedule_subscription_wake(
            book,
            payload=created.get("payload"),
            provisioning=provisioning,
        )
    )
    return {
        "ok": True,
        "created": bool(created.get("created")),
        "sharedBookCreated": bool(created.get("created")),
        "subscriptionCreated": subscription_created,
        "book": safe_book,
        "subscription": subscription,
        "provisioning": provisioning,
        "processingWakeRequested": wake_requested,
        "subscriptionConfig": {
            "coverUrl": book.get("coverUrl", ""),
            "name": book.get("name", ""),
            "author": book.get("author", ""),
            "intro": book.get("intro", ""),
            "bookStatus": book.get("bookStatus", ""),
            "totalChaptersAtSubscribe": book.get("totalChaptersAtSubscribe", 0),
            "startChapterIndex": subscription["startChapterIndex"],
            "autoArchiveOnComplete": subscription["autoArchiveOnComplete"],
            "primarySourceName": book.get("primarySourceName", ""),
        },
    }


@router.post("/books/{aggregate_book_id}/source-map/refresh")
async def refresh_library_book_source_map(request: Request, aggregate_book_id: str, payload: dict | None = None):
    auth_service.require_admin(request)
    _reject_legado_query_anomalies(request, set())
    aggregate_book_id = _validated_aggregate_book_id(aggregate_book_id)
    from app.api.console import _manual_source_map_refresh

    result = _manual_source_map_refresh(aggregate_book_id, payload=payload)
    if asyncio.iscoroutine(result):
        result = await result
    if not result.get("ok") and result.get("error") == "书籍不存在":
        raise HTTPException(status_code=404, detail="书籍不存在")
    return result


@router.get("/library")
def list_library(request: Request, keyword: str = ""):
    auth_service.require_admin(request)
    _reject_legado_query_anomalies(request, {"keyword"})
    keyword = _validated_legado_text(keyword, field="keyword", max_length=200)
    items = library_books_service.list_books(keyword=keyword, include_hidden=True)
    return {"items": items, "total": len(items)}


@router.get("/library/mine")
@public_router.get("/library/mine")
def list_my_library(request: Request, keyword: str = ""):
    user = auth_service.require_user(request)
    _reject_legado_query_anomalies(request, {"keyword"})
    keyword = _validated_legado_text(keyword, field="keyword", max_length=200)
    items = user_subscriptions_service.list_books(
        user.user_id, library_books_service, keyword=keyword
    )
    return {"items": [_safe_user_book(item) for item in items], "total": len(items)}


@router.get("/books/{aggregate_book_id}")
@public_router.get("/books/{aggregate_book_id}")
def get_library_book(request: Request, aggregate_book_id: str):
    user = auth_service.require_user(request)
    _reject_legado_query_anomalies(request, set())
    aggregate_book_id = _validated_aggregate_book_id(aggregate_book_id)
    subscription = _require_book_access(user, aggregate_book_id)
    detail = library_books_service.get_shared_book_detail(aggregate_book_id)
    if not detail.get("found"):
        raise HTTPException(status_code=404, detail="书籍不存在")
    detail = {
        "bookId": aggregate_book_id,
        "found": True,
        "book": _safe_user_book(detail.get("book")),
        "bookState": dict(detail.get("bookState") or {}),
        "sourceSnapshotProgress": detail.get("sourceSnapshotProgress"),
    }
    detail["subscription"] = subscription
    if subscription:
        delivery = library_books_service.subscription_delivery_summary(
            aggregate_book_id,
            start_chapter_index=subscription.get("startChapterIndex", 1),
        )
        detail["personalProgress"] = delivery["personalProgress"]
        detail["provisioning"] = delivery["provisioning"]
    return detail


@router.get("/books/{aggregate_book_id}/chapters")
@public_router.get("/books/{aggregate_book_id}/chapters")
def list_library_book_chapters(
    request: Request,
    aggregate_book_id: str,
    page: int = 1,
    pageSize: int = 200,
    status: str = "all",
    keyword: str = "",
):
    user = auth_service.require_user(request)
    _reject_legado_query_anomalies(request, {"page", "pageSize", "status", "keyword"})
    aggregate_book_id = _validated_aggregate_book_id(aggregate_book_id)
    if not 1 <= page <= _MAX_LIBRARY_CHAPTER_PAGE:
        raise HTTPException(status_code=422, detail="page 超出允许范围")
    if not 1 <= pageSize <= _MAX_LIBRARY_CHAPTER_PAGE_SIZE:
        raise HTTPException(status_code=422, detail="pageSize 超出允许范围")
    status = _validated_legado_text(status, field="status", max_length=32).lower() or "all"
    if status not in _LIBRARY_CHAPTER_STATUSES:
        raise HTTPException(status_code=422, detail="status 无效")
    keyword = _validated_legado_text(keyword, field="keyword", max_length=200)
    _require_book_access(user, aggregate_book_id)
    return library_books_service.list_shared_chapters(
        aggregate_book_id,
        page=page,
        pageSize=pageSize,
        status=status,
        keyword=keyword,
    )


@router.get("/books/{aggregate_book_id}/subscription")
@public_router.get("/books/{aggregate_book_id}/subscription")
def get_subscription(request: Request, aggregate_book_id: str):
    user = auth_service.require_user(request)
    _reject_legado_query_anomalies(request, set())
    aggregate_book_id = _validated_aggregate_book_id(aggregate_book_id)
    subscription = user_subscriptions_service.get(user.user_id, aggregate_book_id)
    if not subscription:
        raise HTTPException(status_code=404, detail="订阅不存在")
    return {"subscription": subscription}


@router.put("/books/{aggregate_book_id}/subscription")
@public_router.put("/books/{aggregate_book_id}/subscription")
async def put_subscription(request: Request, aggregate_book_id: str, payload: dict | None = None):
    user = auth_service.require_user(request)
    _reject_legado_query_anomalies(request, set())
    aggregate_book_id = _validated_aggregate_book_id(aggregate_book_id)
    payload = payload or {}
    unknown = set(payload) - {"startChapterIndex", "autoArchiveOnComplete"}
    if unknown:
        raise HTTPException(status_code=422, detail=f"不支持的字段: {', '.join(sorted(unknown))}")
    book = library_books_service.get_book(aggregate_book_id)
    if not book:
        raise HTTPException(status_code=404, detail="书籍不存在")
    previous_subscription = user_subscriptions_service.get(user.user_id, aggregate_book_id)
    start_chapter_index = _start_chapter_index(payload)
    auto_archive = _auto_archive_on_complete(payload)
    try:
        subscription_rate_limiter.check(user.user_id, "create")
    except SubscriptionLimitError as exc:
        raise _limit_exception(exc) from exc
    try:
        subscription, created = user_subscriptions_service.ensure(
            user.user_id,
            aggregate_book_id,
            start_chapter_index=start_chapter_index,
            auto_archive_on_complete=auto_archive,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="书籍不存在") from exc
    except SubscriptionLimitError as exc:
        raise _limit_exception(exc) from exc
    provisioning = library_books_service.provisioning_summary(
        aggregate_book_id,
        start_chapter_index=subscription["startChapterIndex"],
    )
    subscription_activated = bool(
        created
        or not previous_subscription
        or previous_subscription.get("status") != "active"
    )
    wake_requested = bool(
        subscription_activated
        and _schedule_subscription_wake(
            book,
            payload=None,
            provisioning=provisioning,
        )
    )
    return {
        "created": created,
        "subscription": subscription,
        "provisioning": provisioning,
        "processingWakeRequested": wake_requested,
    }


@router.patch("/books/{aggregate_book_id}/subscription")
@public_router.patch("/books/{aggregate_book_id}/subscription")
async def patch_subscription(request: Request, aggregate_book_id: str, payload: dict | None = None):
    user = auth_service.require_user(request)
    _reject_legado_query_anomalies(request, set())
    aggregate_book_id = _validated_aggregate_book_id(aggregate_book_id)
    payload = payload or {}
    unknown = set(payload) - {"status", "startChapterIndex", "autoArchiveOnComplete"}
    if unknown:
        raise HTTPException(status_code=422, detail=f"不支持的字段: {', '.join(sorted(unknown))}")
    if "startChapterIndex" in payload:
        payload["startChapterIndex"] = _start_chapter_index(payload)
    if "autoArchiveOnComplete" in payload:
        payload["autoArchiveOnComplete"] = _auto_archive_on_complete(payload)
    previous_subscription = user_subscriptions_service.get(user.user_id, aggregate_book_id)
    try:
        subscription = user_subscriptions_service.update(
            user.user_id,
            aggregate_book_id,
            payload,
            before_change=lambda: subscription_rate_limiter.check(user.user_id, "update"),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="订阅不存在") from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except SubscriptionLimitError as exc:
        raise _limit_exception(exc) from exc
    provisioning = library_books_service.provisioning_summary(
        aggregate_book_id,
        start_chapter_index=subscription["startChapterIndex"],
    )
    subscription_activated = bool(
        subscription.get("status") == "active"
        and previous_subscription
        and previous_subscription.get("status") != "active"
    )
    book = library_books_service.get_book(aggregate_book_id) or {}
    wake_requested = bool(
        subscription_activated
        and _schedule_subscription_wake(
            book,
            payload=None,
            provisioning=provisioning,
        )
    )
    return {
        "subscription": subscription,
        "provisioning": provisioning,
        "processingWakeRequested": wake_requested,
    }


@router.delete("/books/{aggregate_book_id}/subscription")
@public_router.delete("/books/{aggregate_book_id}/subscription")
def delete_subscription(request: Request, aggregate_book_id: str):
    user = auth_service.require_user(request)
    _reject_legado_query_anomalies(request, set())
    aggregate_book_id = _validated_aggregate_book_id(aggregate_book_id)
    try:
        return user_subscriptions_service.remove(user.user_id, aggregate_book_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="订阅不存在") from exc


@router.get("/chapters/{chapter_id}")
@public_router.get("/chapters/{chapter_id}")
async def get_subscribed_chapter(request: Request, chapter_id: str):
    user = auth_service.require_user(request)
    _reject_legado_query_anomalies(request, set())
    if not chapter_id or len(chapter_id) > 4096 or any(ord(character) < 32 for character in chapter_id):
        raise HTTPException(status_code=404, detail="章节不存在")
    try:
        source_id, chapter_url = decode_chapter_id(chapter_id)
        payload = unpack_aggregate_chapter_url(chapter_url)
    except Exception as exc:
        raise HTTPException(status_code=404, detail="章节不存在") from exc
    if source_id != VIRTUAL_SOURCE_ID:
        raise HTTPException(status_code=404, detail="章节不存在")
    _require_book_access(user, str(payload.get("aggregateBookId", "") or ""))
    result = library_books_service.read_shared_chapter(
        chapter_id,
        published_only=False,
    )
    if result is None:
        raise HTTPException(status_code=404, detail="章节尚未就绪")
    return {
        "implemented": bool(result.get("implemented", True)),
        "chapterId": str(result.get("chapterId", chapter_id) or chapter_id),
        "title": str(result.get("title", "") or ""),
        "content": str(result.get("content", "") or ""),
        "authRequired": bool(result.get("authRequired", False)),
        "isPaid": bool(result.get("isPaid", False)),
        "isVip": bool(result.get("isVip", False)),
        "previewOnly": bool(result.get("previewOnly", False)),
        "extra": dict(result.get("extra") or {}) if isinstance(result.get("extra"), dict) else {},
    }


# ---- Legado virtual source (migrated from /api/legado) ----

@router.get("/legado/source")
@public_router.get("/legado/source")
def get_legado_source(request: Request, code: str = "", reader: str = "x") -> list[dict]:
    """Personal book-source JSON bound to a user access code.

    Requires ``?code=`` (no anonymous/public source export). Invalid codes
    return 401 (same family as redeem). ``reader=max`` serves the Legado Max
    flavour (review bubbles instead of the chapterComment rule); the default
    is the Legado-X protocol source. One link always yields exactly one
    source entry.
    """
    from app.core.public_security import reading_base_url, request_client_ip
    from app.services.user_auth import AuthRateLimitError, auth_rate_limiter

    access_code = str(code or "").strip()
    if not access_code:
        raise HTTPException(
            status_code=401,
            detail="请使用管理员发放的专属书源链接（需带 code 参数）",
        )
    identifier = auth_service.access_code_identifier(access_code)
    keys = (f"access:ip:{request_client_ip(request)}", f"access:id:{identifier}")
    try:
        auth_rate_limiter.check(*keys)
    except AuthRateLimitError as exc:
        raise HTTPException(
            status_code=429,
            headers={"Retry-After": str(exc.retry_after_seconds)},
            detail="认证尝试过于频繁",
        ) from exc
    try:
        auth_service.authenticate_access_code(access_code)
    except HTTPException as exc:
        if exc.status_code in {401, 403}:
            auth_rate_limiter.record_failure(*keys)
        raise

    # Always bake the reader entrypoint (8765), never admin (8766) — access/enter
    # and Reading APIs only exist on the public listener.
    if reader not in ("x", "max"):
        raise HTTPException(status_code=422, detail="reader 无效")
    return generate_legado_source(
        reading_base_url(request),
        access_code=access_code,
        reader=reader,
    )


@router.get("/legado/search")
@public_router.get("/legado/search")
async def legado_search(
    request: Request,
    keyword: str = "",
    page: str = "1",
    waitMs: str = "0",
) -> dict:
    """Legado book-source search.

    ``waitMs`` is optional and only caps the **per-page** short wait (not the
    120s job lifetime). Default 0 uses built-in page1/follow waits.
    """
    user = auth_service.require_reading_user(request, touch=False)
    _reject_legado_query_anomalies(request, {"keyword", "page", "waitMs"})
    keyword = _validated_legado_text(keyword, field="keyword", max_length=200)
    parsed_page = _legado_query_int(page, field="page", minimum=1, maximum=1000)
    parsed_wait_ms = _legado_query_int(
        waitMs,
        field="waitMs",
        minimum=0,
        maximum=_READING_SEARCH_TIMEOUT_MS,
    )
    with reading_access_limiter.guard(user.user_id, "search"):
        return await _legado_search_response(
            keyword=keyword,
            page=parsed_page,
            wait_ms=parsed_wait_ms,
            base_api=get_public_base_url(request),
            user_id=user.user_id,
        )


async def _legado_search_response(
    *,
    keyword: str,
    page: int,
    wait_ms: int,
    base_api: str,
    user_id: str,
) -> dict:
    """Progressive Legado search via page flips.

    page=1: library/aggregate first + short wait for first third-party hits.
    page=2+: reuse the same job, short-wait for *new* third-party hits only.
    Job hard-stops at 120s from page1 start.
    """
    if not keyword.strip():
        return _legado_search_payload(
            keyword=keyword,
            page=page,
            base_api=base_api,
            user_id=user_id,
            allowed_source_ids=set(),
            snapshot={},
            library_items=[],
            third_party_items=[],
        )

    search_service = _get_legado_search_service()
    source_ids = _third_party_search_source_ids(search_service)
    allowed_source_ids = set(source_ids)
    lane = reading_network_lane(base_api)
    search_mode = _legado_search_mode_for_lane(lane)

    # Library only on page 1 so later pages can continue with remote-only batches.
    library_items = _collect_library_search_items(
        keyword=keyword,
        page=page,
        base_api=base_api,
        user_id=user_id,
    )

    if not source_ids:
        return _legado_search_payload(
            keyword=keyword,
            page=page,
            base_api=base_api,
            user_id=user_id,
            allowed_source_ids=allowed_source_ids,
            library_items=library_items,
            third_party_items=[],
        )

    progress_key = _progress_key(user_id, keyword, lane=lane)
    page1_wait = _READING_SEARCH_PAGE1_WAIT_MS
    follow_wait = _READING_SEARCH_FOLLOW_WAIT_MS
    if wait_ms > 0:
        # Optional override: apply to this page's short wait only.
        if page <= 1:
            page1_wait = wait_ms
        else:
            follow_wait = wait_ms

    try:
        if page <= 1:
            job = search_service.create_job(
                keyword=keyword,
                page=1,
                source_ids=source_ids,
                search_mode=search_mode,
            )
            _remember_legado_search_owner(job.job_id, user_id)
            with _legado_search_progress_lock:
                _legado_search_progress[progress_key] = {
                    "job_id": job.job_id,
                    "started_at": time.time(),
                    "emitted_ids": set(),
                }
            deadline_task = asyncio.create_task(
                _enforce_legado_search_deadline(
                    search_service, job.job_id, _READING_SEARCH_TIMEOUT_MS
                )
            )
            _track_legado_deadline_task(deadline_task)

            await _wait_for_legado_search(
                search_service,
                job.job_id,
                page1_wait,
                stop_on_first_result=True,
                known_book_ids=set(),
                base_api=base_api,
                allowed_source_ids=allowed_source_ids,
            )
            third_party_all, snapshot = _third_party_snapshot_items(
                search_service, job.job_id, base_api=base_api
            )
            batch = third_party_all
            emitted: set[str] = set()
            for item in batch:
                book_id = _item_book_id(
                    item, base_api=base_api, allowed_source_ids=allowed_source_ids
                )
                if book_id:
                    emitted.add(book_id)
            with _legado_search_progress_lock:
                state = _legado_search_progress.get(progress_key)
                if state and state.get("job_id") == job.job_id:
                    state["emitted_ids"] = emitted

            session = search_service.get_session(job.job_id)
            pending = bool(
                session is not None and session.status not in _TERMINAL_SEARCH_STATUSES
            )
            if isinstance(snapshot, dict):
                snapshot = {
                    **snapshot,
                    "jobId": job.job_id,
                    "status": (
                        str(session.status) if session is not None else str(snapshot.get("status") or "running")
                    ),
                    "liveSearchPending": pending,
                }
            else:
                snapshot = {
                    "jobId": job.job_id,
                    "status": "running" if pending else "completed",
                    "liveSearchPending": pending,
                }

            return _legado_search_payload(
                keyword=keyword,
                page=page,
                base_api=base_api,
                user_id=user_id,
                allowed_source_ids=allowed_source_ids,
                snapshot=snapshot,
                library_items=library_items,
                third_party_items=batch,
            )

        # page >= 2: continue the same job, return only *new* third-party hits.
        with _legado_search_progress_lock:
            state = dict(_legado_search_progress.get(progress_key) or {})
        job_id = str(state.get("job_id") or "")
        started_at = float(state.get("started_at") or 0)
        emitted_ids: set[str] = set(state.get("emitted_ids") or set())

        if not job_id or not _owns_legado_search(job_id, user_id):
            return _legado_search_payload(
                keyword=keyword,
                page=page,
                base_api=base_api,
                user_id=user_id,
                allowed_source_ids=allowed_source_ids,
                library_items=[],
                third_party_items=[],
                snapshot={"jobId": "", "status": "completed", "liveSearchPending": False},
            )

        # Job hard deadline (page flips must not exceed overall budget).
        if started_at and (time.time() - started_at) * 1000 >= _READING_SEARCH_TIMEOUT_MS:
            session = search_service.get_session(job_id)
            if session is not None and session.status not in _TERMINAL_SEARCH_STATUSES:
                search_service.cancel_job(job_id)
            return _legado_search_payload(
                keyword=keyword,
                page=page,
                base_api=base_api,
                user_id=user_id,
                allowed_source_ids=allowed_source_ids,
                library_items=[],
                third_party_items=[],
                snapshot={
                    "jobId": job_id,
                    "status": "timed_out",
                    "liveSearchPending": False,
                },
            )

        await _wait_for_legado_search(
            search_service,
            job_id,
            follow_wait,
            stop_on_first_result=True,
            known_book_ids=emitted_ids,
            base_api=base_api,
            allowed_source_ids=allowed_source_ids,
        )
        third_party_all, snapshot = _third_party_snapshot_items(
            search_service, job_id, base_api=base_api
        )
        batch = []
        new_emitted = set(emitted_ids)
        for item in third_party_all:
            book_id = _item_book_id(
                item, base_api=base_api, allowed_source_ids=allowed_source_ids
            )
            if not book_id or book_id in emitted_ids:
                continue
            batch.append(item)
            new_emitted.add(book_id)
        with _legado_search_progress_lock:
            state_now = _legado_search_progress.get(progress_key)
            if state_now and state_now.get("job_id") == job_id:
                state_now["emitted_ids"] = new_emitted

        session = search_service.get_session(job_id)
        pending = bool(
            session is not None and session.status not in _TERMINAL_SEARCH_STATUSES
        )
        # If still running but this page has no new items, return empty so Legado
        # stops paging — only after we already waited follow_wait for growth.
        if isinstance(snapshot, dict):
            snapshot = {
                **snapshot,
                "jobId": job_id,
                "status": (
                    str(session.status)
                    if session is not None
                    else str(snapshot.get("status") or "completed")
                ),
                "liveSearchPending": pending and bool(batch),
            }
        else:
            snapshot = {
                "jobId": job_id,
                "status": "running" if pending else "completed",
                "liveSearchPending": pending and bool(batch),
            }

        return _legado_search_payload(
            keyword=keyword,
            page=page,
            base_api=base_api,
            user_id=user_id,
            allowed_source_ids=allowed_source_ids,
            snapshot=snapshot,
            library_items=[],
            third_party_items=batch,
        )
    except Exception:
        logger.exception("Reading third-party search failed")
        return _legado_search_payload(
            keyword=keyword,
            page=page,
            base_api=base_api,
            user_id=user_id,
            allowed_source_ids=allowed_source_ids,
            library_items=library_items if page <= 1 else [],
            third_party_items=[],
            snapshot={"status": "failed", "liveSearchPending": False},
        )


@router.get("/legado/search/{job_id}")
@public_router.get("/legado/search/{job_id}")
async def get_legado_search_status(request: Request, job_id: str) -> dict:
    user = auth_service.require_reading_user(request, touch=False)
    _reject_legado_query_anomalies(request, set())
    job_id = _validated_legado_identifier(job_id, field="jobId")
    with reading_access_limiter.guard(user.user_id, "search"):
        if not _owns_legado_search(job_id, user.user_id):
            raise HTTPException(status_code=404, detail="搜索任务不存在")
        search_service = _get_legado_search_service()
        snapshot = search_service.session_snapshot(
            job_id,
            base_api=get_public_base_url(request),
            include_official_sources=False,
        )
        if snapshot is None:
            raise HTTPException(status_code=404, detail="搜索任务不存在")
        source_ids = set(_third_party_search_source_ids(search_service))
        return _legado_search_payload(
            keyword=str(snapshot.get("keyword", "") or ""),
            page=int(snapshot.get("page", 1) or 1),
            base_api=get_public_base_url(request),
            user_id=user.user_id,
            allowed_source_ids=source_ids,
            snapshot=snapshot,
        )


@router.post("/legado/search/{job_id}/cancel")
async def cancel_legado_search(request: Request, job_id: str) -> dict:
    auth_service.require_admin(request)
    _reject_legado_query_anomalies(request, set())
    job_id = _validated_legado_identifier(job_id, field="jobId")
    cancelled = _get_legado_search_service().cancel_job(job_id)
    return {"jobId": job_id, "cancelled": cancelled}


@router.get("/legado/explore")
@public_router.get("/legado/explore")
async def legado_explore(request: Request, sourceId: str = "", groupId: str = "", page: str = "1") -> dict:
    user = auth_service.require_reading_user(request, touch=False)
    _reject_legado_query_anomalies(request, {"sourceId", "groupId", "page"})
    source_id = _validated_legado_identifier(sourceId, field="sourceId", allow_empty=True)
    group_id = _validated_legado_text(groupId, field="groupId", max_length=128)
    parsed_page = _legado_query_int(page, field="page", minimum=1, maximum=1000)
    with reading_access_limiter.guard(user.user_id, "search"):
        base_api = get_public_base_url(request)
        published = library_books_service.page_published_books(page=parsed_page, page_size=20)
        items = [
            library_books_service.build_search_injected_item(book, base_api=base_api)
            for book in published["items"]
        ]
        return {
            "implemented": True,
            "sourceId": source_id,
            "groupId": group_id or "published",
            "page": published["page"],
            "items": items,
        }
