"""Legado-facing endpoints for shared library book/toc/chapter access.

The old aggregate search/explore/source endpoints were removed; only the
book reader contract remains, backed by the shared library storage.
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse

from app.services.catalog import Catalog
from app.services.library_books import format_reading_update_time, library_books_service
from app.services.legado_max_bubbles import decorate_legado_max_content
from app.services.aggregate_virtual_source import VIRTUAL_SOURCE_ID
from app.source_plugins.id_codec import (
    decode_book_id,
    decode_chapter_id,
    encode_chapter_id,
)
from app.services.reading_reviews import (
    chapter_review_cache,
    render_chapter_reviews_html,
)
from app.services.user_auth import auth_service
from app.core.public_security import get_public_base_url
from app.services.reading_limits import reading_access_limiter

router = APIRouter(prefix="/api/legado")
logger = logging.getLogger(__name__)
_EXTERNAL_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+:[A-Za-z0-9_-]+$")
_MAX_EXTERNAL_ID_LENGTH = 8192
_MAX_NUMERIC_ID = 2**63 - 1


def _validated_external_id(value: str, *, label: str) -> str:
    normalized = str(value or "")
    if (
        not normalized
        or len(normalized) > _MAX_EXTERNAL_ID_LENGTH
        or not _EXTERNAL_ID_PATTERN.fullmatch(normalized)
    ):
        raise HTTPException(status_code=404, detail=f"{label}不存在")
    return normalized


def _reject_query_anomalies(request: Request, allowed: set[str]) -> None:
    unknown = set(request.query_params.keys()) - allowed
    repeated = {key for key in allowed if len(request.query_params.getlist(key)) > 1}
    if unknown or repeated:
        raise HTTPException(status_code=422, detail="查询参数无效")


def _query_int(
    value: str | None,
    *,
    field: str,
    minimum: int,
    maximum: int,
    default: int | None = None,
) -> int | None:
    if value is None or value == "":
        return default
    if not value.isascii() or not value.isdigit():
        if not (minimum < 0 and value.startswith("-") and value[1:].isascii() and value[1:].isdigit()):
            raise HTTPException(status_code=422, detail=f"{field} 必须是整数")
    parsed = int(value)
    if not minimum <= parsed <= maximum:
        raise HTTPException(status_code=422, detail=f"{field} 超出允许范围")
    return parsed


def _decode_book_identity(book_id: str) -> tuple[str, str]:
    try:
        return decode_book_id(book_id)
    except Exception as exc:
        raise HTTPException(status_code=404, detail="书籍不存在") from exc


def _decode_chapter_identity(chapter_id: str) -> tuple[str, str]:
    try:
        return decode_chapter_id(chapter_id)
    except Exception as exc:
        raise HTTPException(status_code=404, detail="章节不存在") from exc


def _require_third_party_plugin(
    catalog: Catalog,
    source_id: str,
    capability: str,
    *,
    label: str,
    target_url: str,
) -> None:
    plugin = catalog.scheduler._plugins.get(source_id)
    if (
        not plugin
        or not plugin.metadata.enabled
        or plugin.metadata.is_official_source()
        or capability not in plugin.capabilities
    ):
        raise HTTPException(status_code=404, detail=f"{label}不存在")
    parsed = urlparse(target_url)
    hostname = str(parsed.hostname or "").lower().rstrip(".")
    allowed_hosts = {
        str(domain or "").lower().lstrip(".").rstrip(".")
        for domain in plugin.metadata.domains
        if str(domain or "").strip()
    }
    for base_url in plugin.metadata.base_urls:
        base_hostname = str(urlparse(str(base_url or "")).hostname or "").lower().rstrip(".")
        if base_hostname:
            allowed_hosts.add(base_hostname)
    if (
        parsed.scheme not in {"http", "https"}
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or not any(hostname == allowed or hostname.endswith(f".{allowed}") for allowed in allowed_hosts)
    ):
        raise HTTPException(status_code=404, detail=f"{label}不存在")


def _public_text(value: Any, *, max_length: int) -> str:
    return str(value or "").strip()[:max_length]


def _public_book_response(
    data: dict,
    *,
    source_id: str,
    book_id: str,
    base_api: str,
) -> dict:
    return {
        "implemented": True,
        "data": {
            "sourceId": source_id,
            "bookId": book_id,
            "name": _public_text(data.get("name"), max_length=500),
            "author": _public_text(data.get("author"), max_length=300),
            "coverUrl": _public_text(data.get("coverUrl"), max_length=4096),
            "intro": _public_text(data.get("intro"), max_length=12000),
            "kind": _public_text(data.get("kind"), max_length=500),
            "lastChapter": _public_text(data.get("lastChapter"), max_length=500),
            "wordCount": data.get("wordCount", ""),
            "status": _public_text(data.get("status"), max_length=100),
            "updateTime": format_reading_update_time(data.get("updateTime", "")),
            "bookUrl": f"{base_api}/api/legado/book/{book_id}",
            "tocUrl": f"{base_api}/api/legado/book/{book_id}/toc",
        },
    }


def _public_toc_response(
    result: dict,
    *,
    source_id: str,
    book_id: str,
    base_api: str,
) -> dict:
    chapters = []
    for position, raw in enumerate(result.get("chapters", []) or [], start=1):
        if not isinstance(raw, dict):
            continue
        chapter_id = _public_text(raw.get("chapterId"), max_length=_MAX_EXTERNAL_ID_LENGTH)
        if source_id != VIRTUAL_SOURCE_ID:
            raw_chapter_url = _public_text(
                raw.get("rawChapterUrl") or raw.get("chapterUrl"),
                max_length=16_384,
            )
            if raw_chapter_url:
                chapter_id = encode_chapter_id(source_id, raw_chapter_url)
        try:
            encoded_source_id, _ = decode_chapter_id(chapter_id)
        except Exception:
            continue
        if encoded_source_id != source_id or len(chapter_id) > _MAX_EXTERNAL_ID_LENGTH:
            continue
        try:
            chapter_index = int(raw.get("index", position) or position)
        except (TypeError, ValueError):
            chapter_index = position
        preview_only = bool(raw.get("previewOnly", False))
        is_vip = bool(raw.get("isVip", False))
        is_paid = bool(raw.get("isPaid", is_vip))
        chapters.append(
            {
                "sourceId": source_id,
                "chapterId": chapter_id,
                "index": chapter_index,
                "title": _public_text(raw.get("title"), max_length=1000),
                "chapterUrl": f"{base_api}/api/legado/chapter/{chapter_id}",
                "updateTime": format_reading_update_time(raw.get("updateTime", "")),
                "isVip": is_vip,
                "isPaid": is_paid,
                "isPay": bool(raw.get("isPay", is_vip and not preview_only)),
                "previewOnly": preview_only,
            }
        )
    return {
        "implemented": True,
        "bookId": book_id,
        "chapters": chapters,
    }


def _public_chapter_response(result: dict, *, chapter_id: str) -> dict:
    extra = result.get("extra") if isinstance(result.get("extra"), dict) else {}
    preview_only = bool(result.get("previewOnly", extra.get("previewOnly", False)))
    is_vip = bool(result.get("isVip", extra.get("isVip", result.get("isPaid", False))))
    safe_extra = {
        key: extra[key]
        for key in ("previewOnly", "isVip", "contentAccess")
        if key in extra and isinstance(extra[key], (str, int, float, bool, type(None)))
    }
    return {
        "implemented": True,
        "chapterId": chapter_id,
        "title": _public_text(result.get("title"), max_length=1000),
        "content": str(result.get("content", "") or ""),
        "authRequired": bool(result.get("authRequired", False)),
        "isVip": is_vip,
        "isPaid": bool(result.get("isPaid", is_vip)),
        "isPay": bool(result.get("isPay", is_vip and not preview_only)),
        "previewOnly": preview_only,
        "extra": safe_extra,
    }


async def _chapter_reviews(chapter_id: str, *, catalog: Catalog | None = None) -> dict:
    cached = chapter_review_cache.get(chapter_id)
    if cached is not None:
        return cached
    reviews = await (catalog or Catalog()).chapter_reviews(chapter_id)
    chapter_review_cache.set(chapter_id, reviews)
    return reviews


def _purify_chapter_content_for_reading(
    content: str,
    *,
    source_id: str | None = None,
) -> str:
    """Read-path ad/watermark gate (same pure rules as write-time purify).

    Covers already-stored library chapters and third-party direct reads without
    waiting for reprocessing. Safe/no-op when patterns do not match.
    """
    from app.services.content_purify import purify_for_reading

    return purify_for_reading(content, source_id=source_id)


async def _strip_author_say_from_chapter_content(
    *,
    chapter_id: str,
    content: str,
    catalog: Catalog | None = None,
) -> str:
    """Fetch chapter reviews and strip embedded 作家说 from the body when present.

    Reading path always tries the reviews API (cached via chapter_review_cache).
    Subscription processing still does durable strip at write time; this covers
    already-stored chapters and third-party bodies that still carry author notes.
    """
    from app.services.author_say_strip import (
        extract_author_say_texts,
        strip_overlapping_author_say,
    )

    if not content or not content.strip():
        return content
    try:
        reviews = await _chapter_reviews(chapter_id, catalog=catalog)
    except Exception:
        logger.debug(
            "author-say strip skipped; reviews unavailable for %s",
            chapter_id,
            exc_info=True,
        )
        return content
    if not isinstance(reviews, dict):
        return content
    author_texts = extract_author_say_texts(reviews)
    if not author_texts:
        return content
    return strip_overlapping_author_say(content, author_texts)


async def _apply_reading_content_gates(
    *,
    chapter_id: str,
    content: str,
    source_id: str | None = None,
    catalog: Catalog | None = None,
    apply_purify: bool = True,
) -> str:
    """Reading delivery gates: optional ad purify, then 作家说 strip.

    Direct source reads use purify → author-say. Already-selected aggregate
    chapters skip phrase-based purify and only apply the author-say boundary.
    """
    cleaned = (
        _purify_chapter_content_for_reading(content, source_id=source_id)
        if apply_purify
        else content
    )
    return await _strip_author_say_from_chapter_content(
        chapter_id=chapter_id,
        content=cleaned,
        catalog=catalog,
    )


@router.get("/book/{book_id}")
async def get_book(request: Request, book_id: str) -> dict:
    user = auth_service.require_reading_user(request, touch=False)
    # ``lane`` is an optional dual-source disambiguator from search bookUrl; ignored.
    _reject_query_anomalies(request, {"lane"})
    book_id = _validated_external_id(book_id, label="书籍")
    source_id, book_url = _decode_book_identity(book_id)
    with reading_access_limiter.guard(user.user_id, "metadata"):
        base_api = get_public_base_url(request)
        if source_id == VIRTUAL_SOURCE_ID:
            shared = library_books_service.legado_book_detail(book_id, base_api=base_api)
            if shared is None:
                raise HTTPException(status_code=404, detail="书籍尚未发布")
            return _public_book_response(
                dict(shared.get("data") or {}),
                source_id=source_id,
                book_id=book_id,
                base_api=base_api,
            )
        catalog = Catalog(base_api=base_api)
        _require_third_party_plugin(
            catalog,
            source_id,
            "detail",
            label="书籍",
            target_url=book_url,
        )
        result = await catalog.book_detail(book_id)
        data = result.get("data") if isinstance(result, dict) else None
        if not isinstance(data, dict):
            raise HTTPException(status_code=404, detail="书籍读取失败")
        return _public_book_response(
            data,
            source_id=source_id,
            book_id=book_id,
            base_api=base_api,
        )


@router.get("/book/{book_id}/toc")
async def get_toc(request: Request, book_id: str) -> dict:
    user = auth_service.require_reading_user(request, touch=False)
    _reject_query_anomalies(request, {"lane"})
    book_id = _validated_external_id(book_id, label="书籍")
    source_id, book_url = _decode_book_identity(book_id)
    with reading_access_limiter.guard(user.user_id, "metadata"):
        base_api = get_public_base_url(request)
        if source_id == VIRTUAL_SOURCE_ID:
            shared = library_books_service.legado_toc(book_id, base_api=base_api)
            if shared is None:
                raise HTTPException(status_code=404, detail="书籍尚未发布")
            return _public_toc_response(
                shared,
                source_id=source_id,
                book_id=book_id,
                base_api=base_api,
            )
        catalog = Catalog(base_api=base_api)
        _require_third_party_plugin(
            catalog,
            source_id,
            "toc",
            label="书籍",
            target_url=book_url,
        )
        result = await catalog.toc(book_id)
        return _public_toc_response(
            result,
            source_id=source_id,
            book_id=book_id,
            base_api=base_api,
        )


@router.get("/chapter/{chapter_id}")
async def get_chapter(request: Request, chapter_id: str, reviewBubbles: str | None = None) -> dict:
    user = auth_service.require_reading_user(request, touch=False)
    _reject_query_anomalies(request, {"lane", "reviewBubbles"})
    if reviewBubbles not in (None, "1"):
        raise HTTPException(status_code=422, detail="reviewBubbles 无效")
    chapter_id = _validated_external_id(chapter_id, label="章节")
    source_id, chapter_url = _decode_chapter_identity(chapter_id)
    with reading_access_limiter.guard(user.user_id, "chapter"):
        catalog = Catalog(base_api=get_public_base_url(request))
        if source_id == VIRTUAL_SOURCE_ID:
            shared = library_books_service.legado_chapter(chapter_id)
            if shared is None:
                raise HTTPException(status_code=404, detail="章节尚未发布")
            content = await _apply_reading_content_gates(
                chapter_id=chapter_id,
                content=str(shared.get("content", "") or ""),
                source_id=source_id,
                catalog=catalog,
                apply_purify=False,
            )
            shared = {**shared, "content": content}
            result = shared
        else:
            _require_third_party_plugin(
                catalog,
                source_id,
                "chapter",
                label="章节",
                target_url=chapter_url,
            )
            result = await catalog.chapter(chapter_id)
            content = await _apply_reading_content_gates(
                chapter_id=chapter_id,
                content=str(result.get("content", "") or ""),
                source_id=source_id,
                catalog=catalog,
            )
            result = {**result, "content": content}
        response = _public_chapter_response(result, chapter_id=chapter_id)
        if reviewBubbles == "1":
            try:
                with reading_access_limiter.guard(user.user_id, "reviews"):
                    reviews = await _chapter_reviews(chapter_id, catalog=catalog)
                view_url = f"{get_public_base_url(request)}/api/legado/chapter/{chapter_id}/reviews/view"
                response["content"] = decorate_legado_max_content(
                    response["content"], reviews, view_url=view_url
                )
            except Exception:
                logger.warning("Unable to decorate Legado Max reviews for %s", chapter_id, exc_info=True)
        return response


@router.get("/chapter/{chapter_id}/reviews")
async def get_chapter_reviews(request: Request, chapter_id: str) -> dict:
    user = auth_service.require_reading_user(request, touch=False)
    _reject_query_anomalies(request, {"lane"})
    chapter_id = _validated_external_id(chapter_id, label="章节")
    source_id, chapter_url = _decode_chapter_identity(chapter_id)
    with reading_access_limiter.guard(user.user_id, "reviews"):
        catalog = Catalog(base_api=get_public_base_url(request))
        if source_id == VIRTUAL_SOURCE_ID:
            if library_books_service.legado_chapter(chapter_id) is None:
                raise HTTPException(status_code=404, detail="章节尚未发布")
        else:
            _require_third_party_plugin(
                catalog,
                source_id,
                "chapter_reviews",
                label="章节",
                target_url=chapter_url,
            )
        return await _chapter_reviews(chapter_id, catalog=catalog)


@router.get("/chapter/{chapter_id}/reviews/view", response_class=HTMLResponse)
async def get_chapter_review_view(
    request: Request,
    chapter_id: str,
    tab: str = "chapter",
    paragraphId: str | None = None,
    paragraphIds: str | None = None,
    rootReviewId: str | None = None,
    page: str = "1",
    pageSize: str = "10",
    cursorId: str = "0",
) -> HTMLResponse:
    user = auth_service.require_reading_user(request, touch=False)
    _reject_query_anomalies(
        request,
        {
            "tab",
            "paragraphId",
            "paragraphIds",
            "rootReviewId",
            "page",
            "pageSize",
            "cursorId",
            "lane",
        },
    )
    chapter_id = _validated_external_id(chapter_id, label="章节")
    source_id, chapter_url = _decode_chapter_identity(chapter_id)
    if tab not in {"chapter", "paragraph"}:
        raise HTTPException(status_code=422, detail="tab 无效")
    parsed_paragraph_id = _query_int(
        paragraphId, field="paragraphId", minimum=-1, maximum=_MAX_NUMERIC_ID
    )
    parsed_root_review_id = _query_int(
        rootReviewId, field="rootReviewId", minimum=1, maximum=_MAX_NUMERIC_ID
    )
    parsed_page = _query_int(page, field="page", minimum=1, maximum=1000, default=1) or 1
    page_size = _query_int(pageSize, field="pageSize", minimum=1, maximum=50, default=10) or 10
    cursor_id = _query_int(cursorId, field="cursorId", minimum=0, maximum=_MAX_NUMERIC_ID, default=0) or 0
    if paragraphIds is not None and (len(paragraphIds) > 1024 or any(ord(char) < 32 for char in paragraphIds)):
        raise HTTPException(status_code=422, detail="paragraphIds 无效")

    with reading_access_limiter.guard(user.user_id, "reviews"):
        catalog = Catalog(base_api=get_public_base_url(request))
        if source_id == VIRTUAL_SOURCE_ID:
            chapter = library_books_service.legado_chapter(chapter_id)
            if chapter is None:
                raise HTTPException(status_code=404, detail="章节尚未发布")
        else:
            _require_third_party_plugin(
                catalog,
                source_id,
                "chapter",
                label="章节",
                target_url=chapter_url,
            )
            _require_third_party_plugin(
                catalog,
                source_id,
                "chapter_reviews",
                label="章节",
                target_url=chapter_url,
            )
            chapter = await catalog.chapter(chapter_id)
        reviews = await _chapter_reviews(chapter_id, catalog=catalog)
        paragraph_detail = None
        page_hot_detail = None
        chapter_detail = None
        reply_detail = None
        if parsed_root_review_id is not None:
            reply_detail = await catalog.review_replies(
                chapter_id,
                parsed_root_review_id,
                page=parsed_page,
                page_size=page_size,
                cursor_id=cursor_id,
            )
            tab = "paragraph"
        elif paragraphIds:
            try:
                parsed_ids = list(dict.fromkeys(
                    int(value.strip())
                    for value in paragraphIds.split(",")
                    if value.strip()
                ))
            except ValueError as exc:
                raise HTTPException(status_code=422, detail="paragraphIds 必须是逗号分隔的非负整数") from exc
            if (
                not parsed_ids
                or len(parsed_ids) > 50
                or any(value < 0 or value > _MAX_NUMERIC_ID for value in parsed_ids)
            ):
                raise HTTPException(status_code=422, detail="paragraphIds 数量或取值无效")
            page_hot_detail = await catalog.page_hot_reviews(
                chapter_id,
                parsed_ids,
                page=parsed_page,
                page_size=page_size,
            )
            tab = "paragraph"
        elif parsed_paragraph_id is not None:
            paragraph_detail = await catalog.paragraph_reviews(
                chapter_id,
                parsed_paragraph_id,
                page=parsed_page,
                page_size=page_size,
            )
            tab = "paragraph"
        elif tab == "chapter" and parsed_page > 1:
            chapter_detail = await catalog.chapter_say(
                chapter_id,
                page=parsed_page,
                page_size=page_size,
            )
        base_api = get_public_base_url(request)
        review_view_url = f"{base_api}/api/legado/chapter/{chapter_id}/reviews/view"
        return HTMLResponse(
            render_chapter_reviews_html(
                chapter_title=str(chapter.get("title") or "本章评论"),
                reviews=reviews,
                review_view_url=review_view_url,
                active_tab=tab,
                selected_paragraph_id=parsed_paragraph_id,
                paragraph_detail=paragraph_detail,
                page_hot_detail=page_hot_detail,
                chapter_detail=chapter_detail,
                reply_detail=reply_detail,
            ),
            headers={
                "X-Frame-Options": "SAMEORIGIN",
                "Content-Security-Policy": "frame-ancestors 'self'; base-uri 'self'; object-src 'none'",
            },
        )
