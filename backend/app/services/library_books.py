"""Shared aggregate library services.

Copyright (c) 2026 moo. All rights reserved.
"""

from __future__ import annotations

import json
import re
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from app.config import DB_PATH, HOST, PORT
from app.services.aggregate_alignment import chapter_name_similarity
from app.services.aggregate_settings import AggregateSettingsRepository
from app.services.aggregate_virtual_source import (
    VIRTUAL_SOURCE_ID,
    VIRTUAL_SOURCE_NAME,
    make_aggregate_chapter_url,
    primary_book_id_from_payload,
    unpack_aggregate_book_url,
    unpack_aggregate_chapter_url,
)
from app.services.live_acceptance import (
    QIDIAN_SOURCE_IDS,
    normalize_author_key,
    normalize_text,
    qidian_book_identity,
)
from app.services.audit import audit_service
from app.services.shared_book_storage import CHAPTER_STATUS_PROCESSED, SharedBookStorage, TRACE_BEGIN
from app.source_plugins.id_codec import (
    decode_book_id,
    decode_chapter_id,
    encode_book_id,
    encode_chapter_id,
)
from app.source_plugins.loader import PluginLoader
from app.storage.db import initialize_database


def make_library_aggregate_book_url(aggregate_book_id: str) -> str:
    return f"legadohub://aggregate/library/{aggregate_book_id}"


def make_library_book_id(aggregate_book_id: str) -> str:
    return encode_book_id(VIRTUAL_SOURCE_ID, make_library_aggregate_book_url(aggregate_book_id))


def format_reading_update_time(value: Any) -> str:
    """Format ISO timestamps for the compact Reading chapter list."""
    normalized = str(value or "").strip()
    if not normalized or len(normalized) <= 10:
        return normalized
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return normalized
    return parsed.strftime("%Y-%m-%d %H:%M")


def strip_leading_duplicate_chapter_title(content: str, title: str) -> str:
    """Drop a source heading when its name matches the published chapter title."""
    first_line, separator, remainder = content.partition("\n")
    if not separator or not remainder.strip():
        return content
    heading = first_line.strip().removeprefix("#").strip()
    published = str(title or "").strip()
    if not heading or not published:
        return content
    exact = re.sub(r"\s+", "", heading) == re.sub(r"\s+", "", published)
    if exact or chapter_name_similarity(published, heading) == 1.0:
        return remainder.lstrip("\n")
    return content


def _normalize_book_status_text(*values: object) -> str:
    for value in values:
        raw = str(value or "").strip().lower()
        if not raw:
            continue
        if any(key in raw for key in ("完结", "completed", "finished", "已完结", "完本")):
            return "completed"
        return "ongoing"
    return ""


class LibraryBooksService:
    def __init__(self, db_path=DB_PATH, *, shared_book_storage: SharedBookStorage | None = None):
        self.db_path = db_path
        self.shared_book_storage = shared_book_storage or SharedBookStorage()

    def _conn(self) -> sqlite3.Connection:
        initialize_database(self.db_path)
        conn = sqlite3.connect(self.db_path, timeout=5.0)
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _plugins(self) -> dict[str, Any]:
        try:
            from app.source_plugins.scheduler import get_plugin_scheduler

            return get_plugin_scheduler()._plugins
        except Exception:
            return {}

    def _is_official(self, source_id: str) -> bool:
        plugin = self._plugins().get(source_id)
        return bool(plugin and plugin.metadata.is_official_source())

    def _merged_book_settings(self) -> dict[str, Any]:
        workflow = AggregateSettingsRepository(self.db_path).content_workflow()
        merged = {
            "autoTrackUpdates": True,
            "updateIntervalMinutes": 60,
            "primarySourceMode": "official",
            "aiAggregateEnabled": False,
            "aiPurifyEnabled": True,
            "sourcePriorityMode": "auto",
            "sourcePriority": [],
            "minReadableChaptersForDiscovery": 50,
        }
        merged.update(
            {
                "autoTrackUpdates": bool(workflow.get("autoAggregate", True)),
                "updateIntervalMinutes": int(workflow.get("aggregateCheckIntervalMinutes") or 60),
                "primarySourceMode": workflow.get("primarySourceMode", "official"),
                "aiAggregateEnabled": bool(workflow.get("aiEnabled", False)),
                "aiPurifyEnabled": bool(workflow.get("blockedWordRepair", True)),
                "sourcePriorityMode": "manual" if workflow.get("primarySourcePriority") else "auto",
                "sourcePriority": list(workflow.get("primarySourcePriority") or []),
                "minReadableChaptersForDiscovery": max(
                    0,
                    int(workflow.get("minReadableChaptersForDiscovery", 50) or 50),
                ),
            }
        )
        return merged

    def discovery_min_readable_chapters(self) -> int:
        return int(self._merged_book_settings().get("minReadableChaptersForDiscovery", 50) or 50)

    def _canonical_name(self, value: str) -> str:
        return normalize_text(value or "")

    def _canonical_author(self, value: str) -> str:
        return normalize_author_key(value or "")

    def _normalize_source_urls(self, source_id: str, raw_book_url: str) -> tuple[str, str]:
        """Return normalized (book_url, toc_url) for sources that need a canonical URL shape."""
        book_url = str(raw_book_url or "").strip()
        toc_url = book_url
        if source_id in {"qidian_com_web", "qidian_com_app"} and book_url:
            parsed = urlparse(book_url)
            match = re.search(r"/book/(\d+)/?", parsed.path or "")
            if match:
                book_id = match.group(1)
                book_url = f"https://m.qidian.com/book/{book_id}/"
                toc_url = f"https://m.qidian.com/book/{book_id}/catalog/"
        return book_url, toc_url

    def _payload_from_group(self, group: dict[str, Any]) -> dict[str, Any]:
        items = [dict(item) for item in group.get("items", []) if isinstance(item, dict)]
        sources = []
        for item in items:
            source_id = item.get("sourceId", "")
            raw_book_url = item.get("rawBookUrl") or item.get("bookUrl", "")
            normalized_book_url, normalized_toc_url = self._normalize_source_urls(source_id, raw_book_url)
            book_id = item.get("bookId", "")
            if source_id and normalized_book_url:
                book_id = encode_book_id(source_id, normalized_book_url)
            if not source_id or not raw_book_url or not book_id:
                continue
            sources.append(
                {
                    "bookId": book_id,
                    "sourceId": source_id,
                    "sourceName": item.get("sourceName", ""),
                    "bookUrl": normalized_book_url,
                    "tocUrl": item.get("tocUrl", "") or normalized_toc_url,
                    "score": int(item.get("score", 0) or 0),
                    "lastChapter": item.get("lastChapter", "") or "",
                    "coverUrl": item.get("coverUrl", "") or "",
                    "intro": item.get("intro", "") or "",
                    "wordCount": item.get("wordCount", "") or "",
                    "chapterCount": int(item.get("chapterCount", 0) or 0),
                    "bookStatus": item.get("status", "") or item.get("bookStatus", "") or "",
                    "author": item.get("author", "") or "",
                    "name": item.get("name", "") or "",
                }
            )
        return {
            "candidateId": group.get("candidateId", ""),
            "name": group.get("name", "") or (sources[0].get("name", "") if sources else ""),
            "author": group.get("author", "") or (sources[0].get("author", "") if sources else ""),
            "coverUrl": group.get("coverUrl", "") or (sources[0].get("coverUrl", "") if sources else ""),
            "intro": group.get("intro", "") or (sources[0].get("intro", "") if sources else ""),
            "bookStatus": group.get("bookStatus", "") or (sources[0].get("bookStatus", "") if sources else ""),
            "totalChaptersAtSubscribe": int(group.get("chapterCount", 0) or (sources[0].get("chapterCount", 0) if sources else 0) or 0),
            "sources": sources,
        }

    def _display_item_for_group(self, group: dict[str, Any]) -> dict[str, Any]:
        items = [dict(item) for item in group.get("items", []) if isinstance(item, dict)]
        if not items:
            return {}
        official_items = [item for item in items if self._is_official(item.get("sourceId", ""))]
        ranked = official_items or sorted(items, key=lambda item: -int(item.get("score", 0) or 0))
        return dict(ranked[0]) if ranked else {}

    def _primary_source_payload(self, payload: dict[str, Any], primary_book_id: str) -> dict[str, Any]:
        sources = payload.get("sources") if isinstance(payload.get("sources"), list) else []
        for source in sources:
            if isinstance(source, dict) and source.get("bookId", "") == primary_book_id:
                return dict(source)
        return {}

    def build_source_map_summary(self, shared_metadata: dict[str, Any]) -> list[dict[str, Any]]:
        """Return API-facing source summary from shared metadata only."""
        source_map = shared_metadata.get("sourceMap")
        items = None
        if isinstance(source_map, dict):
            items = source_map.get("summary")
        if not isinstance(items, list):
            items = shared_metadata.get("sourceMapSummary")
        if not isinstance(items, list):
            return []
        summary: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            if self._is_official(str(item.get("sourceId", "") or "")):
                continue
            summary.append(
                {
                    "sourceId": item.get("sourceId", "") or "",
                    "sourceName": item.get("sourceName", "") or "",
                    "score": int(item.get("score", 0) or 0),
                    "chapterCount": int(item.get("chapterCount", 0) or 0),
                    "lastChapter": item.get("lastChapter", "") or "",
                    "bookStatus": item.get("bookStatus", "") or "",
                    "name": item.get("name", "") or "",
                    "author": item.get("author", "") or "",
                }
            )
        return summary

    def load_shared_metadata(self, aggregate_book_id: str) -> dict[str, Any]:
        book = self.get_book(aggregate_book_id) or {}
        book_name = str(book.get("name", "") or "").strip()
        author = str(book.get("author", "") or "").strip()
        if not book_name:
            return {}
        path = self.shared_book_storage.metadata_path(book_name=book_name, author=author)
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    def source_map_refresh_state(self, aggregate_book_id: str) -> dict[str, Any]:
        """Return the persisted source-map refresh state for one shared book."""
        metadata = self.load_shared_metadata(aggregate_book_id)
        source_map = metadata.get("sourceMap") if isinstance(metadata.get("sourceMap"), dict) else {}
        health = source_map.get("health") if isinstance(source_map.get("health"), dict) else {}
        last_verified_at = str(health.get("lastVerifiedAt", "") or "").strip()
        status = str(health.get("status", "") or "").strip()
        return {
            "completed": bool(last_verified_at),
            "lastVerifiedAt": last_verified_at,
            "status": status,
            "missingCriticalSource": bool(health.get("missingCriticalSource")),
        }

    def source_snapshot_progress(self, aggregate_book_id: str) -> dict[str, Any] | None:
        """Return URL-free third-party snapshot progress for detail pages."""
        payload = self.load_payload(aggregate_book_id)
        source_names = {
            str(item.get("sourceId", "") or ""): str(item.get("sourceName", "") or item.get("sourceId", "") or "")
            for item in payload.get("sources", [])
            if isinstance(item, dict) and item.get("sourceId")
        }
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT source_id, status, total_chapters, matched_chapters,
                       fetched_chapters, failed_chapters, updated_at
                FROM aggregate_source_snapshot_runs
                WHERE aggregate_book_id = ?
                ORDER BY updated_at DESC, source_id ASC
                """,
                (aggregate_book_id,),
            ).fetchall()
        if not rows:
            return None

        sources = []
        seen_source_ids: set[str] = set()
        for row in rows:
            source_id = str(row[0] or "")
            if source_id in seen_source_ids:
                continue
            seen_source_ids.add(source_id)
            status = str(row[1] or "pending")
            total = max(0, int(row[2] or 0))
            fetched = max(0, int(row[4] or 0))
            percent = (
                100
                if status == "complete"
                else 0
                if status == "loading_toc"
                else min(99, round(fetched * 100 / total))
                if total
                else 0
            )
            sources.append({
                "sourceId": source_id,
                "sourceName": source_names.get(source_id) or source_id,
                "status": status,
                "totalChapters": total,
                "matchedChapters": max(0, int(row[3] or 0)),
                "fetchedChapters": fetched,
                "failedChapters": max(0, int(row[5] or 0)),
                "percent": percent,
                "updatedAt": str(row[6] or ""),
            })

        running = sum(item["status"] in {"running", "loading_toc"} for item in sources)
        completed = sum(item["status"] == "complete" for item in sources)
        failed = sum(item["status"] == "error" or item["failedChapters"] > 0 for item in sources)
        total_chapters = sum(item["totalChapters"] for item in sources)
        fetched_chapters = sum(item["fetchedChapters"] for item in sources)
        completed_units = sum(
            item["totalChapters"] if item["status"] == "complete" else min(item["fetchedChapters"], item["totalChapters"])
            for item in sources
        )
        if running:
            status = "running"
        elif failed:
            status = "error"
        elif completed == len(sources):
            status = "complete"
        else:
            status = "partial"
        return {
            "status": status,
            "sourceCount": len(sources),
            "completedSourceCount": completed,
            "runningSourceCount": running,
            "failedSourceCount": failed,
            "totalChapters": total_chapters,
            "fetchedChapters": fetched_chapters,
            "failedChapters": sum(item["failedChapters"] for item in sources),
            "percent": (
                min(100, round(completed_units * 100 / total_chapters))
                if total_chapters
                else 100 if completed == len(sources) else 0
            ),
            "sources": sources,
        }

    def get_shared_book_detail(self, aggregate_book_id: str) -> dict[str, Any]:
        """Return the shared-file truth view for a single library book.

        This intentionally does not expose private source URLs; it returns the
        sanitized source-map summary and shared bookState only.
        """
        book = self.get_book(aggregate_book_id)
        if not book:
            return {"bookId": aggregate_book_id, "found": False}
        shared_metadata = self.load_shared_metadata(aggregate_book_id)
        source_summary = self.build_source_map_summary(shared_metadata)
        source_map = shared_metadata.get("sourceMap") if isinstance(shared_metadata.get("sourceMap"), dict) else {}
        health = source_map.get("health") if isinstance(source_map.get("health"), dict) else {}
        return {
            "bookId": aggregate_book_id,
            "found": True,
            "book": book,
            "bookState": self.build_book_state_summary(shared_metadata),
            "sources": source_summary,
            "sourceMap": {
                "summary": source_summary,
                "health": {
                    "status": str(health.get("status", "") or ""),
                    "lastVerifiedAt": str(health.get("lastVerifiedAt", "") or ""),
                    "missingCriticalSource": bool(health.get("missingCriticalSource")),
                },
            },
            "sourceMapRefresh": self.source_map_refresh_state(aggregate_book_id),
            "sourceSnapshotProgress": self.source_snapshot_progress(aggregate_book_id),
        }

    def _safe_shared_chapter_path(self, book_dir: Path, file_name: str) -> Path | None:
        """Resolve one index-owned chapter path without allowing directory escape."""
        if not file_name:
            return None
        try:
            resolved_book_dir = book_dir.resolve()
            chapter_path = (resolved_book_dir / file_name).resolve()
        except OSError:
            return None
        if chapter_path == resolved_book_dir or resolved_book_dir not in chapter_path.parents:
            return None
        return chapter_path

    def _shared_chapter_catalog(
        self,
        aggregate_book_id: str,
        *,
        book: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, Path | None, list[dict[str, Any]]]:
        """Load the shared chapter index and its DB projection exactly once."""
        book = book or self.get_book(aggregate_book_id)
        if not book:
            return None, None, []

        book_name = str(book.get("name", "") or "").strip()
        author = str(book.get("author", "") or "").strip()
        if not book_name:
            return book, None, []
        storage = self.shared_book_storage
        book_dir = storage.shared_book_dir(book_name=book_name, author=author)
        chapter_index_payload = storage._read_json(
            storage.chapter_index_path(book_name=book_name, author=author)
        ) or {}
        chapter_entries = chapter_index_payload.get("chapters")
        if not isinstance(chapter_entries, list):
            chapter_entries = []

        with self._conn() as conn:
            db_chapters = {
                int(row[2] or 0): {
                    "chapterId": str(row[0] or ""),
                    "sourceChapterId": str(row[1] or ""),
                    "contentLength": int(row[3] or 0),
                    "sourceWordCount": int(row[4] or 0),
                    "previewOnly": bool(row[5]),
                    "processedAt": str(row[6] or ""),
                    "volumeName": str(row[7] or "").strip(),
                }
                for row in conn.execute(
                    """
                    SELECT chapter_id, source_chapter_id, chapter_index,
                           content_length, source_word_count, preview_only,
                           last_processed_at, volume_name
                    FROM aggregate_chapter_tasks
                    WHERE aggregate_book_id = ?
                    """,
                    (aggregate_book_id,),
                ).fetchall()
            }

        records: list[dict[str, Any]] = []
        for entry in chapter_entries:
            if not isinstance(entry, dict):
                continue
            chapter_index = int(entry.get("index", 0) or 0)
            chapter_title = str(entry.get("title", "") or "")
            chapter_status = str(entry.get("status", "") or "pending")
            file_name = str(entry.get("file", "") or "").strip()
            chapter_path = self._safe_shared_chapter_path(book_dir, file_name)
            db_chapter = db_chapters.get(chapter_index, {})
            volume_name = str(
                db_chapter.get("volumeName") or entry.get("volumeName") or ""
            ).strip()
            db_chapter_id = str(db_chapter.get("chapterId", "") or "")
            source_chapter_id = str(
                entry.get("sourceChapterId")
                or db_chapter.get("sourceChapterId")
                or ""
            )
            if db_chapter_id.startswith(f"{VIRTUAL_SOURCE_ID}:"):
                read_chapter_id = db_chapter_id
            elif source_chapter_id:
                read_chapter_id = encode_chapter_id(
                    VIRTUAL_SOURCE_ID,
                    make_aggregate_chapter_url(
                        aggregate_book_id=aggregate_book_id,
                        source_chapter_id=source_chapter_id,
                        title=chapter_title,
                        index=chapter_index,
                    ),
                )
            else:
                read_chapter_id = ""
            preview_only = bool(
                db_chapter.get("previewOnly", chapter_status == "fetched")
            )
            is_vip = bool(entry.get("isVip", preview_only))
            records.append(
                {
                    "chapterId": str(chapter_index),
                    "chapterIndex": chapter_index,
                    "readChapterId": read_chapter_id,
                    "title": chapter_title,
                    "volumeName": volume_name,
                    "status": chapter_status,
                    "contentLength": int(db_chapter.get("contentLength", 0) or 0),
                    "hasContent": bool(chapter_path and chapter_path.is_file()),
                    "processedAt": str(db_chapter.get("processedAt", "") or ""),
                    "sourceWordCount": int(db_chapter.get("sourceWordCount", 0) or 0),
                    "isVip": is_vip,
                    "isPaid": is_vip,
                    "isPay": is_vip and not preview_only,
                    "previewOnly": preview_only,
                    "file": file_name or None,
                    "_chapterPath": chapter_path,
                    "_hasDbProjection": bool(db_chapter),
                }
            )
        return book, book_dir, records

    def _hydrate_shared_chapter_record(
        self,
        aggregate_book_id: str,
        record: dict[str, Any],
    ) -> dict[str, Any]:
        """Read one chapter file to provide current-page content metrics."""
        item = dict(record)
        chapter_path = item.pop("_chapterPath", None)
        item.pop("_hasDbProjection", None)
        if not isinstance(chapter_path, Path) or not chapter_path.is_file():
            item["hasContent"] = False
            return item
        try:
            markdown = chapter_path.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            item["hasContent"] = False
            return item

        item["hasContent"] = True
        body, _, _ = markdown.partition(f"<!-- {TRACE_BEGIN}")
        item["contentLength"] = len(
            body.replace(f"# {item.get('title', '')}", "", 1).strip()
        )
        try:
            trace = self.shared_book_storage.parse_trace_block(markdown)
        except ValueError:
            trace = {}
        item["previewOnly"] = bool(trace.get("previewOnly", item.get("previewOnly", False)))
        item["isVip"] = bool(item.get("isVip", trace.get("isVip", item["previewOnly"])))
        item["isPaid"] = item["isVip"]
        item["isPay"] = item["isVip"] and not item["previewOnly"]
        item["sourceWordCount"] = int(
            item.get("sourceWordCount", 0) or trace.get("sourceWordCount", 0) or 0
        )
        item["processedAt"] = str(
            item.get("processedAt", "") or trace.get("processedAt", "") or ""
        )
        if not item.get("readChapterId"):
            primary = trace.get("primarySource")
            source_chapter_id = str(
                primary.get("chapterId", "") if isinstance(primary, dict) else ""
            )
            if source_chapter_id:
                item["readChapterId"] = encode_chapter_id(
                    VIRTUAL_SOURCE_ID,
                    make_aggregate_chapter_url(
                        aggregate_book_id=aggregate_book_id,
                        source_chapter_id=source_chapter_id,
                        title=str(item.get("title", "") or ""),
                        index=int(item.get("chapterIndex", 0) or 0),
                    ),
                )
        return item

    def list_shared_chapters(
        self,
        aggregate_book_id: str,
        *,
        page: int = 1,
        pageSize: int = 50,
        status: str = "all",
        keyword: str = "",
    ) -> dict[str, Any]:
        """List chapters from shared-file truth, reading only the requested page."""
        book, _book_dir, records = self._shared_chapter_catalog(aggregate_book_id)
        if not book:
            return {"items": [], "page": page, "pageSize": pageSize, "total": 0}

        page = max(1, int(page or 1))
        page_size = max(1, min(int(pageSize or 50), 200))
        normalized_status = str(status or "all").strip().lower()
        normalized_keyword = str(keyword or "").strip().lower()
        filtered = [
            record
            for record in records
            if (normalized_status == "all" or record.get("status") == normalized_status)
            and (
                not normalized_keyword
                or normalized_keyword in str(record.get("title", "") or "").lower()
            )
        ]
        total = len(filtered)
        offset = (page - 1) * page_size
        items = [
            self._hydrate_shared_chapter_record(aggregate_book_id, record)
            for record in filtered[offset : offset + page_size]
        ]
        return {"items": items, "page": page, "pageSize": page_size, "total": total}

    def subscription_delivery_summary(
        self,
        aggregate_book_id: str,
        *,
        start_chapter_index: int = 1,
    ) -> dict[str, Any]:
        """Build personal progress and first-readable state without scanning bodies."""
        book, _book_dir, records = self._shared_chapter_catalog(aggregate_book_id)
        start = max(1, int(start_chapter_index or 1))
        if not book:
            return {
                "personalProgress": {
                    "rangeStartIndex": start,
                    "rangeEndIndex": start - 1,
                    "fullCount": 0,
                    "previewCount": 0,
                    "failedCount": 0,
                    "pendingCount": 0,
                    "continuousReadableThroughIndex": start - 1,
                    "coverageRatio": 0.0,
                },
                "provisioning": {
                    "state": "error",
                    "readableChapterCount": 0,
                    "previewChapterCount": 0,
                    "pendingChapterCount": 0,
                    "firstReadableChapter": None,
                },
            }

        delivery_records: list[dict[str, Any]] = []
        for record in records:
            if int(record.get("chapterIndex", 0) or 0) < start:
                continue
            if record.get("hasContent") and not record.get("_hasDbProjection"):
                item = self._hydrate_shared_chapter_record(aggregate_book_id, record)
            else:
                item = dict(record)
                item.pop("_chapterPath", None)
                item.pop("_hasDbProjection", None)
            delivery_records.append(item)
        delivery_records.sort(key=lambda item: int(item.get("chapterIndex", 0) or 0))

        full = 0
        preview = 0
        failed = 0
        first_readable: dict[str, Any] | None = None
        continuous = start - 1
        expected_index = start
        continuous_blocked = False
        failed_statuses = {"error", "failed", "rejected"}
        for item in delivery_records:
            chapter_index = int(item.get("chapterIndex", 0) or 0)
            read_chapter_id = str(item.get("readChapterId", "") or "")
            has_readable_content = bool(item.get("hasContent") and read_chapter_id)
            preview_only = bool(item.get("previewOnly"))
            status = str(item.get("status", "") or "").lower()
            if has_readable_content and preview_only:
                preview += 1
            elif has_readable_content:
                full += 1
            elif status in failed_statuses:
                failed += 1

            if has_readable_content and first_readable is None:
                first_readable = {
                    "chapterId": read_chapter_id,
                    "chapterIndex": chapter_index,
                    "title": str(item.get("title", "") or ""),
                    "contentAccess": "preview" if preview_only else "full",
                }

            if chapter_index != expected_index:
                continuous_blocked = True
            if not continuous_blocked and has_readable_content:
                continuous = chapter_index
                expected_index = chapter_index + 1
            else:
                continuous_blocked = True

        range_end = max(
            [int(item.get("chapterIndex", 0) or 0) for item in delivery_records]
            + [int(book.get("totalChapters", 0) or 0), start - 1]
        )
        total = max(0, range_end - start + 1)
        pending = max(0, total - full - preview - failed)
        available = full + preview
        shared_status = str(book.get("status", "") or "active").lower()
        if first_readable is not None:
            provisioning_state = "ready"
        elif shared_status in {"paused", "archived"}:
            provisioning_state = "paused"
        elif shared_status == "error":
            provisioning_state = "error"
        else:
            provisioning_state = "processing"
        return {
            "personalProgress": {
                "rangeStartIndex": start,
                "rangeEndIndex": range_end,
                "fullCount": full,
                "previewCount": preview,
                "failedCount": failed,
                "pendingCount": pending,
                "continuousReadableThroughIndex": continuous,
                "coverageRatio": round(available / total, 4) if total else 0.0,
            },
            "provisioning": {
                "state": provisioning_state,
                "readableChapterCount": full,
                "previewChapterCount": preview,
                "pendingChapterCount": pending,
                "firstReadableChapter": first_readable,
            },
        }

    def provisioning_summary(
        self,
        aggregate_book_id: str,
        *,
        start_chapter_index: int = 1,
    ) -> dict[str, Any]:
        return self.subscription_delivery_summary(
            aggregate_book_id,
            start_chapter_index=start_chapter_index,
        )["provisioning"]

    def save_payload_sources(self, aggregate_book_id: str, sources: list[dict[str, Any]]) -> None:
        payload = self.load_payload(aggregate_book_id)
        payload["sources"] = [dict(item) for item in sources if isinstance(item, dict)]
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE aggregate_book_tasks
                SET aggregate_payload_json = ?, updated_at = datetime('now')
                WHERE aggregate_book_id = ?
                """,
                (json.dumps(payload, ensure_ascii=False), aggregate_book_id),
            )
            conn.commit()

    def build_book_state_summary(self, shared_metadata: dict[str, Any]) -> dict[str, Any]:
        """Return API-facing bookState summary from shared metadata only."""
        chapter_count = 0
        raw_state = shared_metadata.get("bookState")
        if isinstance(raw_state, dict):
            chapter_count = int(raw_state.get("chapterCount", 0) or 0)
        return self.shared_book_storage.build_book_state_summary(raw_state, chapter_count=chapter_count)

    async def _hydrate_primary_source_payload(
        self,
        payload: dict[str, Any],
        primary_book_id: str,
        primary_source_id: str,
        primary_source: dict[str, Any],
    ) -> dict[str, Any]:
        """Best-effort fill of subscription fields from the resolved primary source.

        Phase 1 must not block subscription intake on primary-source detail/toc
        enrichment. If the live book-detail or toc path is unavailable, we keep
        the search-card values and let the first background processing round
        refresh them from the real source.
        """
        hydrated = dict(payload)
        hydrated["primarySourceId"] = primary_source_id
        hydrated["primarySourceName"] = primary_source.get("sourceName", "") or primary_source_id
        hydrated["primaryBookId"] = primary_book_id
        hydrated["primaryBookUrl"] = primary_source.get("bookUrl", "") or ""
        hydrated["primaryTocUrl"] = primary_source.get("tocUrl", "") or primary_source.get("bookUrl", "") or ""
        hydrated["supplementSourceConfig"] = hydrated.get("supplementSourceConfig") or {}

        try:
            from app.services.book_catalog import BookCatalog

            catalog = BookCatalog()
            detail = await catalog.book_detail(primary_book_id)
            detail_data = detail.get("data") if isinstance(detail, dict) else {}
            if isinstance(detail_data, dict):
                hydrated["name"] = detail_data.get("name", "") or hydrated.get("name", "")
                hydrated["author"] = detail_data.get("author", "") or hydrated.get("author", "")
                hydrated["coverUrl"] = detail_data.get("coverUrl", "") or hydrated.get("coverUrl", "")
                hydrated["intro"] = detail_data.get("intro", "") or hydrated.get("intro", "")
                hydrated["bookStatus"] = _normalize_book_status_text(
                    detail_data.get("status", "")
                    or detail_data.get("bookStatus", "")
                    or detail_data.get("bookStatusText", "")
                    or detail_data.get("kindStatus", "")
                    or detail_data.get("kind", "")
                    or hydrated.get("bookStatus", "")
                )
                hydrated["wordCount"] = (
                    detail_data.get("wordCountText", "")
                    or detail_data.get("wordCount", "")
                    or hydrated.get("wordCount", "")
                )
                hydrated["primaryBookUrl"] = detail_data.get("rawBookUrl", "") or detail_data.get("bookUrl", "") or hydrated.get("primaryBookUrl", "")
                hydrated["primaryTocUrl"] = detail_data.get("rawTocUrl", "") or detail_data.get("tocUrl", "") or hydrated.get("primaryTocUrl", "")

            toc = await catalog.toc(primary_book_id)
            chapters = toc.get("chapters") if isinstance(toc, dict) else []
            if isinstance(chapters, list):
                hydrated["totalChaptersAtSubscribe"] = len(chapters)
                for source in hydrated.get("sources", []):
                    if isinstance(source, dict) and source.get("bookId", "") == primary_book_id:
                        source["chapterCount"] = len(chapters)
                        if chapters:
                            source["lastChapter"] = chapters[-1].get("title", "") or source.get("lastChapter", "")
                        break
        except Exception:
            # Use the search-card payload when live enrichment is unavailable.
            pass

        return hydrated

    def _aggregate_book_row_to_dict(self, row: sqlite3.Row | tuple | None) -> dict[str, Any] | None:
        if not row:
            return None
        if isinstance(row, sqlite3.Row):
            row = tuple(row)
        return {
            "aggregateBookId": row[0],
            "canonicalName": row[1] or "",
            "canonicalAuthor": row[2] or "",
            "name": row[3] or "",
            "author": row[4] or "",
            "coverUrl": row[5] or "",
            "intro": row[6] or "",
            "wordCount": row[7] or "",
            "primaryBookId": row[8] or "",
            "primarySourceId": row[9] or "",
            "primarySourceName": row[10] or "",
            "primaryBookUrl": row[11] or "",
            "primaryTocUrl": row[12] or "",
            "addedByUserId": row[13] or "",
            "startChapterIndex": int(row[14] or 1),
            "totalChaptersAtSubscribe": int(row[15] or 0),
            "initialSnapshotLastIndex": int(row[16] or 0),
            "backfillStarted": bool(row[17]),
            "autoArchiveOnComplete": bool(row[18]),
            "searchVisibilityStatus": row[19] or "hidden",
            "bookStatus": row[20] or "unknown",
            "totalChapters": int(row[21] or 0),
            "processedChapters": int(row[22] or 0),
            "visibleProcessedChapters": int(row[23] or 0),
            "failedChapters": int(row[24] or 0),
            "status": row[25] or "active",
            "settingsJson": row[26] or "",
            "currentPolicyVersion": int(row[27] or 1),
            "lastSourceChapterTitle": row[28] or "",
            "lastLocalChapterTitle": row[29] or "",
            "lastCheckTime": row[30] or "",
            "nextCheckTime": row[31] or "",
            "lastError": row[32] or "",
            "archivedAt": row[33] or "",
            "createdAt": row[34] or "",
            "updatedAt": row[35] or "",
            "intervalMinutes": int(row[36] or 60),
        }

    def _book_lookup_row(self, aggregate_book_id: str) -> dict[str, Any] | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT aggregate_book_id, canonical_name, canonical_author, name, author,
                       cover_url, intro, word_count, primary_book_id, primary_source_id,
                       primary_source_name, primary_book_url, primary_toc_url, added_by_user_id,
                       start_chapter_index, total_chapters_at_subscribe, initial_snapshot_last_index,
                       backfill_started, auto_archive_on_complete, search_visibility_status,
                       book_status, total_chapters, processed_chapters, visible_processed_chapters,
                       failed_chapters, status, settings_json, current_policy_version,
                       last_source_chapter_title, '', last_check_time, next_check_time, last_error,
                       archived_at, created_at, updated_at, interval_minutes
                FROM aggregate_book_tasks
                WHERE aggregate_book_id = ?
                """,
                (aggregate_book_id,),
            ).fetchone()
        data = self._aggregate_book_row_to_dict(row)
        if data:
            data["addedByUsername"] = self.username_for_user_id(data.get("addedByUserId", ""))
        return data

    def find_existing_book(self, group: dict[str, Any], *, visible_only: bool = False) -> dict[str, Any] | None:
        payload = self._payload_from_group(group)
        with self._conn() as conn:
            for source in payload.get("sources", []):
                source_id = str(source.get("sourceId", "") or "")
                source_book_id = str(source.get("bookId", "") or "")
                identities = [(source_id, source_book_id)]
                qidian_work_id = qidian_book_identity(source_id, source.get("bookUrl", ""))
                if qidian_work_id:
                    canonical_url = f"https://m.qidian.com/book/{qidian_work_id}/"
                    identities.extend(
                        (variant_id, encode_book_id(variant_id, canonical_url))
                        for variant_id in sorted(QIDIAN_SOURCE_IDS)
                        if variant_id != source_id
                    )
                for identity_source_id, identity_book_id in identities:
                    row = conn.execute(
                        """
                        SELECT aggregate_book_id
                        FROM aggregate_book_sources
                        WHERE source_id = ? AND source_book_id = ?
                        ORDER BY id ASC
                        LIMIT 1
                        """,
                        (identity_source_id, identity_book_id),
                    ).fetchone()
                    if row:
                        book = self._book_lookup_row(row[0])
                        if not book:
                            continue
                        if visible_only and book.get("searchVisibilityStatus") != "visible":
                            continue
                        return book
        return None

    def build_subscription_card(self, group: dict[str, Any]) -> dict[str, Any]:
        payload = self._payload_from_group(group)
        display = self._display_item_for_group(group)
        existing = self.find_existing_book(group, visible_only=False)
        items = [dict(item) for item in group.get("items", []) if isinstance(item, dict)]
        source_summary = [
            {
                "sourceId": item.get("sourceId", ""),
                "sourceName": item.get("sourceName", "") or item.get("sourceId", ""),
                "score": int(item.get("score", 0) or 0),
                "official": self._is_official(item.get("sourceId", "")),
            }
            for item in sorted(items, key=lambda x: -int(x.get("score", 0) or 0))
        ]
        return {
            "cardId": group.get("candidateId", ""),
            "candidateId": group.get("candidateId", ""),
            "name": payload.get("name", "") or display.get("name", ""),
            "author": payload.get("author", "") or display.get("author", ""),
            "coverUrl": display.get("coverUrl", ""),
            "intro": display.get("intro", ""),
            "wordCount": display.get("wordCount", ""),
            "lastChapter": display.get("lastChapter", ""),
            "bookStatus": display.get("status", "") or display.get("bookStatus", ""),
            "sourceCount": len(source_summary),
            "sourceSummary": source_summary,
            "hasOfficialSource": any(x["official"] for x in source_summary),
            "officialSourceIds": [x["sourceId"] for x in source_summary if x["official"]],
            "alreadyInLibrary": bool(existing),
            "aggregateBookId": existing.get("aggregateBookId", "") if existing else "",
            "addedByUserId": existing.get("addedByUserId", "") if existing else "",
            "addedByUsername": existing.get("addedByUsername", "") if existing else "",
            "progress": {
                "processedChapters": existing.get("processedChapters", 0) if existing else 0,
                "visibleProcessedChapters": existing.get("visibleProcessedChapters", 0) if existing else 0,
                "totalChapters": existing.get("totalChapters", 0) if existing else 0,
                "status": existing.get("status", "") if existing else "",
                "searchVisibilityStatus": existing.get("searchVisibilityStatus", "") if existing else "",
            },
            "debug": {
                "payload": payload,
                "displaySourceId": display.get("sourceId", ""),
            },
        }

    async def create_or_get_shared_book(
        self,
        group: dict[str, Any],
        *,
        actor_user_id: str,
    ) -> dict[str, Any]:
        existing = self.find_existing_book(group)
        if existing:
            return {"created": False, "book": existing}

        payload = self._payload_from_group(group)
        if not payload.get("sources"):
            raise ValueError("candidate group has no valid sources")
        plugins = self._plugins()
        settings = self._merged_book_settings()
        primary_book_id = primary_book_id_from_payload(
            payload,
            plugins=plugins,
            source_priority=settings.get("sourcePriority") or [],
        )
        primary_source_id = primary_book_id.split(":", 1)[0] if ":" in primary_book_id else ""
        if not primary_book_id or not primary_source_id:
            if settings.get("sourcePriority"):
                raise ValueError("配置的主源当前不可用，请重新搜索或调整主源优先级")
            raise ValueError("failed to resolve primary source")
        primary_source = self._primary_source_payload(payload, primary_book_id)
        display = self._display_item_for_group(group)
        payload = await self._hydrate_primary_source_payload(
            payload,
            primary_book_id,
            primary_source_id,
            primary_source,
        )
        payload["coverUrl"] = payload.get("coverUrl", "") or display.get("coverUrl", "")
        payload["intro"] = payload.get("intro", "") or display.get("intro", "")
        payload["bookStatus"] = (
            _normalize_book_status_text(payload.get("bookStatus", ""), display.get("status", ""))
            or "unknown"
        )
        payload["totalChaptersAtSubscribe"] = int(payload.get("totalChaptersAtSubscribe", 0) or 0)
        payload["startChapterIndex"] = 1
        payload["autoArchiveOnComplete"] = False

        aggregate_book_id = uuid.uuid4().hex
        canonical_name = self._canonical_name(payload.get("name", ""))
        canonical_author = self._canonical_author(payload.get("author", ""))
        now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()

        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO aggregate_book_tasks (
                    aggregate_book_id, canonical_name, canonical_author, name, author,
                    cover_url, intro, word_count, aggregate_payload_json, primary_book_id,
                    primary_source_id, primary_source_name, primary_book_url, primary_toc_url,
                    added_by_user_id, start_chapter_index, total_chapters_at_subscribe,
                    initial_snapshot_last_index, backfill_started, auto_archive_on_complete,
                    search_visibility_status, book_status, total_chapters, processed_chapters,
                    visible_processed_chapters, failed_chapters, total_tokens, status,
                    settings_json, current_policy_version, interval_minutes, last_check_time,
                    next_check_time, error_count, last_error, ai_enabled, last_processed_at,
                    archived_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, 'hidden', ?, 0, 0, 0, 0, 0, 'active', ?, 1, ?, NULL, NULL, 0, '', ?, NULL, NULL, ?, ?)
                """,
                (
                    aggregate_book_id,
                    canonical_name,
                    canonical_author,
                    payload.get("name", "") or display.get("name", ""),
                    payload.get("author", "") or display.get("author", ""),
                    payload.get("coverUrl", "") or display.get("coverUrl", ""),
                    payload.get("intro", "") or display.get("intro", ""),
                    payload.get("wordCount", "") or display.get("wordCount", ""),
                    json.dumps(payload, ensure_ascii=False),
                    primary_book_id,
                    primary_source_id,
                    payload.get("primarySourceName", "") or primary_source_id,
                    payload.get("primaryBookUrl", "") or "",
                    payload.get("primaryTocUrl", "") or "",
                    actor_user_id,
                    1,
                    int(payload.get("totalChaptersAtSubscribe", 0) or 0),
                    0,
                    payload.get("bookStatus", "") or display.get("status", "") or "unknown",
                    json.dumps(
                        {
                            **settings,
                            "supplementSourceConfig": payload.get("supplementSourceConfig", {}),
                        },
                        ensure_ascii=False,
                    ),
                    int(settings.get("updateIntervalMinutes", 60) or 60),
                    1 if settings.get("aiAggregateEnabled", True) else 0,
                    now,
                    now,
                ),
            )
            for source in payload.get("sources", []):
                conn.execute(
                    """
                    INSERT INTO aggregate_book_sources (
                        aggregate_book_id, source_id, source_book_id, source_name, source_book_url,
                        role, score, enabled, last_seen_at, last_chapter_title, chapter_count, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, 0, ?, ?)
                    """,
                    (
                        aggregate_book_id,
                        source.get("sourceId", ""),
                        source.get("bookId", ""),
                        source.get("sourceName", ""),
                        source.get("bookUrl", ""),
                        "primary" if source.get("sourceId", "") == primary_source_id else "candidate",
                        int(source.get("score", 0) or 0),
                        now,
                        source.get("lastChapter", "") or "",
                        now,
                        now,
                    ),
                )
            conn.execute(
                """
                INSERT INTO aggregate_operation_logs (
                    aggregate_book_id, actor_user_id, actor_role, operation_type, before_json, after_json, created_at
                ) VALUES (?, ?, 'user', 'create', '', ?, ?)
                """,
                (
                    aggregate_book_id,
                    actor_user_id,
                    json.dumps(
                        {
                            "primarySourceId": primary_source_id,
                            "primaryBookUrl": primary_source.get("bookUrl", "") or "",
                        },
                        ensure_ascii=False,
                    ),
                    now,
                ),
            )
            audit_service.record(
                action="shared_book.create",
                actor_user_id=actor_user_id,
                actor_role="user",
                target_type="shared_book",
                target_id=aggregate_book_id,
                source_id=primary_source_id,
                summary={"sharedBookCreated": True, "status": "active"},
                conn=conn,
            )
            conn.commit()

        book = self._book_lookup_row(aggregate_book_id)
        return {"created": True, "book": book, "payload": payload}

    def _attach_book_state_summary(self, item: dict[str, Any]) -> None:
        """Hydrate item with shared bookState counts from filesystem metadata."""
        name = str(item.get("name", "") or "").strip()
        author = str(item.get("author", "") or "").strip()
        if not name:
            return
        path = self.shared_book_storage.metadata_path(book_name=name, author=author)
        if not path.exists():
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return
        if not isinstance(payload, dict):
            return
        item["bookState"] = self.build_book_state_summary(payload)

    def list_books(self, *, added_by_user_id: str | None = None, keyword: str = "", include_hidden: bool = True) -> list[dict[str, Any]]:
        where = []
        params: list[Any] = []
        if added_by_user_id:
            where.append("added_by_user_id = ?")
            params.append(added_by_user_id)
        if keyword:
            where.append("(name LIKE ? OR author LIKE ?)")
            params.extend([f"%{keyword}%", f"%{keyword}%"])
        if not include_hidden:
            where.append("search_visibility_status = 'visible'")
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT aggregate_book_id, canonical_name, canonical_author, name, author,
                       cover_url, intro, word_count, primary_book_id, primary_source_id,
                       primary_source_name, primary_book_url, primary_toc_url, added_by_user_id,
                       start_chapter_index, total_chapters_at_subscribe, initial_snapshot_last_index,
                       backfill_started, auto_archive_on_complete, search_visibility_status,
                       book_status, total_chapters, processed_chapters, visible_processed_chapters,
                       failed_chapters, status, settings_json, current_policy_version,
                       last_source_chapter_title, '', last_check_time, next_check_time, last_error,
                       archived_at, created_at, updated_at, interval_minutes
                FROM aggregate_book_tasks
                {where_sql}
                ORDER BY updated_at DESC, created_at DESC
                """,
                params,
            ).fetchall()
        items = []
        for row in rows:
            if not row:
                continue
            item = self._aggregate_book_row_to_dict(row)
            if item:
                item["addedByUsername"] = self.username_for_user_id(item.get("addedByUserId", ""))
                self._attach_book_state_summary(item)
                items.append(item)
        return items

    def scan_integrity(self, *, book_ids: set[str] | None = None) -> dict[str, Any]:
        """Check DB-backed shared books against their file-backed runtime state."""
        books = self.list_books(include_hidden=True)
        if book_ids is not None:
            books = [book for book in books if book.get("aggregateBookId") in book_ids]
        results = [self._scan_book_integrity(book) for book in books]
        summary = {
            "totalBooks": len(results),
            "healthyBooks": sum(item["status"] == "healthy" for item in results),
            "repairableBooks": sum(item["status"] == "repairable" for item in results),
            "brokenBooks": sum(item["status"] == "broken" for item in results),
            "issueCount": sum(len(item["issues"]) for item in results),
        }
        return {
            "checkedAt": datetime.now().astimezone().isoformat(),
            "summary": summary,
            "books": results,
        }

    def _scan_book_integrity(self, book: dict[str, Any]) -> dict[str, Any]:
        storage = self.shared_book_storage
        book_id = str(book.get("aggregateBookId", "") or "")
        book_name = str(book.get("name", "") or "").strip()
        author = str(book.get("author", "") or "").strip()
        book_dir = storage.shared_book_dir(book_name=book_name, author=author)
        issues: list[dict[str, Any]] = []

        def add_issue(
            code: str,
            message: str,
            *,
            repairable: bool = True,
            count: int = 1,
            samples: list[Any] | None = None,
        ) -> None:
            issue: dict[str, Any] = {
                "code": code,
                "message": message,
                "repairable": repairable,
                "count": max(1, int(count or 1)),
            }
            if samples:
                issue["samples"] = samples[:20]
            issues.append(issue)

        payload = self.load_payload(book_id)
        if not payload or not (payload.get("primaryBookId") or payload.get("sources")):
            add_issue("aggregate_payload_missing", "订阅载荷缺失或损坏", repairable=False)

        if not book_name:
            add_issue("book_identity_missing", "书籍名称缺失，无法定位共享目录", repairable=False)
        elif not book_dir.exists():
            add_issue("book_directory_missing", "共享书籍目录不存在")

        if book_dir.exists():
            stale_tmp = list(book_dir.rglob("*.tmp"))
            if stale_tmp:
                add_issue("stale_tmp_files", "存在未清理的临时文件", count=len(stale_tmp))

        metadata_path = storage.metadata_path(book_name=book_name, author=author)
        chapter_index_path = storage.chapter_index_path(book_name=book_name, author=author)
        metadata_payload: dict[str, Any] | None = None
        chapter_index_payload: dict[str, Any] | None = None
        if metadata_path.exists():
            try:
                metadata_payload = storage._read_json(metadata_path)
            except (OSError, ValueError, json.JSONDecodeError):
                add_issue("metadata_invalid", "metadata.json 无法解析")
        elif book_dir.exists():
            add_issue("metadata_missing", "metadata.json 不存在")

        if chapter_index_path.exists():
            try:
                chapter_index_payload = storage._read_json(chapter_index_path)
            except (OSError, ValueError, json.JSONDecodeError):
                add_issue("chapter_index_invalid", "chapter_index.json 无法解析")
        elif book_dir.exists():
            add_issue("chapter_index_missing", "chapter_index.json 不存在")

        chapter_traces: dict[int, dict[str, Any]] = {}
        chapter_entries = (
            chapter_index_payload.get("chapters", [])
            if isinstance(chapter_index_payload, dict)
            else []
        )
        if not isinstance(chapter_entries, list):
            add_issue("chapter_index_invalid", "chapter_index.json 的章节列表无效")
            chapter_entries = []

        missing_chapters: list[int] = []
        broken_chapters: list[int] = []
        invalid_paths: list[int] = []
        for item in chapter_entries:
            if not isinstance(item, dict):
                continue
            chapter_index = int(item.get("index", 0) or 0)
            file_name = str(item.get("file", "") or "").strip()
            status = str(item.get("status", "") or "").strip()
            if not file_name:
                if status in CHAPTER_STATUS_PROCESSED:
                    missing_chapters.append(chapter_index)
                continue
            chapter_path = self._safe_shared_chapter_path(book_dir, file_name)
            if chapter_path is None:
                invalid_paths.append(chapter_index)
                continue
            if not chapter_path.exists():
                missing_chapters.append(chapter_index)
                continue
            try:
                trace = storage.parse_trace_block(chapter_path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, ValueError):
                broken_chapters.append(chapter_index)
                continue
            if chapter_index > 0:
                chapter_traces[chapter_index] = trace

        if missing_chapters:
            add_issue(
                "chapter_files_missing",
                "已处理章节的正文文件缺失",
                count=len(missing_chapters),
                samples=missing_chapters,
            )
        if broken_chapters:
            add_issue(
                "chapter_traces_invalid",
                "章节 trace 损坏或正文编码无效",
                count=len(broken_chapters),
                samples=broken_chapters,
            )
        if invalid_paths:
            add_issue(
                "chapter_paths_invalid",
                "章节索引包含越界路径",
                count=len(invalid_paths),
                samples=invalid_paths,
            )

        if metadata_payload is not None and chapter_index_payload is not None:
            if storage.book_state_needs_rebuild(
                metadata_payload=metadata_payload,
                chapter_index_payload=chapter_index_payload,
                chapter_traces=chapter_traces,
            ):
                add_issue("book_state_drift", "metadata.json 的章节统计与文件不一致")

        with self._conn() as conn:
            db_chapter_count = int(
                conn.execute(
                    "SELECT COUNT(*) FROM aggregate_chapter_tasks WHERE aggregate_book_id = ?",
                    (book_id,),
                ).fetchone()[0]
                or 0
            )
            snapshot_rows = conn.execute(
                """
                SELECT chapter_index, source_id, content_hash
                FROM aggregate_source_snapshots
                WHERE aggregate_book_id = ?
                ORDER BY chapter_index, source_id
                """,
                (book_id,),
            ).fetchall()
            completed_snapshot_sources = conn.execute(
                """
                SELECT DISTINCT source_id
                FROM aggregate_source_snapshot_runs
                WHERE aggregate_book_id = ? AND status = 'complete'
                """,
                (book_id,),
            ).fetchall()

        if chapter_index_payload is not None and db_chapter_count != len(chapter_entries):
            add_issue(
                "chapter_index_count_mismatch",
                "数据库章节数与 chapter_index.json 不一致",
                count=abs(db_chapter_count - len(chapter_entries)) or 1,
            )

        missing_snapshots: list[str] = []
        invalid_snapshots: list[str] = []
        for chapter_index, source_id, content_hash in snapshot_rows:
            snapshot_path = storage.source_snapshot_path(
                book_name=book_name,
                author=author,
                source_id=str(source_id or ""),
                chapter_index=int(chapter_index or 0),
            )
            sample = f"{source_id}:{int(chapter_index or 0)}"
            if not snapshot_path.exists():
                missing_snapshots.append(sample)
                continue
            try:
                snapshot_payload = storage._read_json(snapshot_path) or {}
            except (OSError, ValueError, json.JSONDecodeError):
                invalid_snapshots.append(sample)
                continue
            if (
                str(snapshot_payload.get("aggregateBookId", "") or "") != book_id
                or str(snapshot_payload.get("sourceId", "") or "") != str(source_id or "")
                or int(snapshot_payload.get("chapterIndex", 0) or 0) != int(chapter_index or 0)
                or str(snapshot_payload.get("contentHash", "") or "") != str(content_hash or "")
            ):
                invalid_snapshots.append(sample)

        if missing_snapshots:
            add_issue(
                "source_snapshot_files_missing",
                "数据库快照缺少对应文件",
                count=len(missing_snapshots),
                samples=missing_snapshots,
            )
        if invalid_snapshots:
            add_issue(
                "source_snapshot_files_invalid",
                "来源快照文件损坏或与数据库不一致",
                count=len(invalid_snapshots),
                samples=invalid_snapshots,
            )

        missing_manifests: list[str] = []
        invalid_manifests: list[str] = []
        for row in completed_snapshot_sources:
            source_id = str(row[0] or "")
            manifest_path = storage.source_snapshot_manifest_path(
                book_name=book_name,
                author=author,
                source_id=source_id,
            )
            if not manifest_path.exists():
                missing_manifests.append(source_id)
                continue
            try:
                manifest = storage._read_json(manifest_path) or {}
            except (OSError, ValueError, json.JSONDecodeError):
                invalid_manifests.append(source_id)
                continue
            if (
                str(manifest.get("aggregateBookId", "") or "") != book_id
                or str(manifest.get("sourceId", "") or "") != source_id
            ):
                invalid_manifests.append(source_id)

        if missing_manifests:
            add_issue(
                "source_snapshot_manifests_missing",
                "已完成来源快照缺少 manifest.json",
                count=len(missing_manifests),
                samples=missing_manifests,
            )
        if invalid_manifests:
            add_issue(
                "source_snapshot_manifests_invalid",
                "来源快照 manifest.json 损坏或身份不一致",
                count=len(invalid_manifests),
                samples=invalid_manifests,
            )

        if not issues:
            status = "healthy"
        elif any(not issue["repairable"] for issue in issues):
            status = "broken"
        else:
            status = "repairable"
        return {
            "bookId": book_id,
            "name": book_name,
            "author": author,
            "bookStatus": str(book.get("status", "") or ""),
            "status": status,
            "repairable": status == "repairable",
            "issues": issues,
        }

    def _discovery_readable_chapter_count(self, book: dict[str, Any]) -> int:
        if "visibleProcessedChapters" in book:
            try:
                return max(0, int(book.get("visibleProcessedChapters", 0) or 0))
            except (TypeError, ValueError):
                return 0
        book_state = book.get("bookState")
        if isinstance(book_state, dict):
            try:
                readable = int(book_state.get("readableChapterCount", 0) or 0)
            except (TypeError, ValueError):
                return 0
            return max(0, readable)
        try:
            return max(0, int(book.get("processedChapters", 0) or 0))
        except (TypeError, ValueError):
            return 0

    def search_visible_books(
        self,
        keyword: str,
        *,
        min_readable_chapters: int | None = None,
    ) -> list[dict[str, Any]]:
        normalized = self._canonical_name(keyword)
        author_norm = self._canonical_author(keyword)
        if not normalized and not author_norm:
            return []
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT aggregate_book_id, canonical_name, canonical_author, name, author,
                       cover_url, intro, word_count, primary_book_id, primary_source_id,
                       primary_source_name, primary_book_url, primary_toc_url, added_by_user_id,
                       start_chapter_index, total_chapters_at_subscribe, initial_snapshot_last_index,
                       backfill_started, auto_archive_on_complete, search_visibility_status,
                       book_status, total_chapters, processed_chapters, visible_processed_chapters,
                       failed_chapters, status, settings_json, current_policy_version,
                       last_source_chapter_title, '', last_check_time, next_check_time, last_error,
                       archived_at, created_at, updated_at, interval_minutes
                FROM aggregate_book_tasks
                WHERE canonical_name LIKE '%' || ? || '%'
                   OR canonical_author LIKE '%' || ? || '%'
                ORDER BY updated_at DESC, created_at DESC
                """,
                (normalized, author_norm or normalized),
            ).fetchall()
        items = []
        for row in rows:
            item = self._aggregate_book_row_to_dict(row)
            if item and self._is_injectable_book(item, min_readable_chapters=min_readable_chapters):
                item["addedByUsername"] = self.username_for_user_id(item.get("addedByUserId", ""))
                self._attach_book_state_summary(item)
                items.append(item)
        return items

    def _is_injectable_book(
        self,
        book: dict[str, Any],
        *,
        min_readable_chapters: int | None = None,
    ) -> bool:
        threshold = self.discovery_min_readable_chapters() if min_readable_chapters is None else max(0, int(min_readable_chapters or 0))
        return self._discovery_readable_chapter_count(book) >= threshold

    def library_match_score(self, book: dict[str, Any], keyword: str, *, bonus: int = 50) -> int:
        kw = normalize_text(keyword or "")
        name = normalize_text(book.get("name", "") or book.get("canonicalName", ""))
        author = normalize_author_key(book.get("author", "") or book.get("canonicalAuthor", ""))
        score = 0
        title_hit = False
        author_hit = False
        if kw:
            if kw == name:
                score += 200
                title_hit = True
            elif kw in name:
                score += 100
                title_hit = True
            if author:
                if kw == author:
                    score += 80
                    author_hit = True
                elif kw in author:
                    score += 40
                    author_hit = True
        if title_hit and author_hit:
            score += 50
        if book.get("author"):
            score += 10
        if book.get("lastSourceChapterTitle") or book.get("lastLocalChapterTitle"):
            score += 5
        if book.get("intro"):
            score += 3
        if book.get("coverUrl"):
            score += 3
        if book.get("wordCount"):
            score += 2
        score += int(bonus or 0)
        return score

    def get_book(self, aggregate_book_id: str) -> dict[str, Any] | None:
        return self._book_lookup_row(aggregate_book_id)

    def match_books_for_candidate_groups(self, groups: list[dict[str, Any]], *, visible_only: bool = True) -> list[dict[str, Any]]:
        matched: list[dict[str, Any]] = []
        seen: set[str] = set()
        for group in groups:
            row = self.find_existing_book(group, visible_only=visible_only)
            if row and row["aggregateBookId"] not in seen:
                seen.add(row["aggregateBookId"])
                matched.append(row)
        return matched

    def build_search_injected_item(self, book: dict[str, Any], *, base_api: str | None = None, score: int = 0) -> dict[str, Any]:
        base_api = base_api or f"http://{HOST}:{PORT}"
        aggregate_book_id = book["aggregateBookId"]
        raw_book_url = make_library_aggregate_book_url(aggregate_book_id)
        book_id = make_library_book_id(aggregate_book_id)
        return {
            "displayType": "aggregate",
            "resultKind": "aggregate",
            "sourceId": VIRTUAL_SOURCE_ID,
            "sourceName": VIRTUAL_SOURCE_NAME,
            "bookId": book_id,
            "rawBookUrl": raw_book_url,
            "bookUrl": f"{base_api}/api/legado/book/{book_id}",
            "name": book.get("name", ""),
            "author": book.get("author", ""),
            "coverUrl": book.get("coverUrl", ""),
            "intro": book.get("intro", ""),
            "lastChapter": book.get("lastSourceChapterTitle", "") or book.get("lastLocalChapterTitle", ""),
            "wordCount": book.get("wordCount", ""),
            "aggregateBookId": aggregate_book_id,
            "searchVisibilityStatus": book.get("searchVisibilityStatus", ""),
            "libraryStatus": book.get("status", ""),
            "processedChapters": book.get("processedChapters", 0),
            "visibleProcessedChapters": book.get("visibleProcessedChapters", 0),
            "totalChapters": book.get("totalChapters", 0),
            "score": int(score or 0),
        }

    def list_published_books(self, *, keyword: str = "") -> list[dict[str, Any]]:
        """Return only shared books that are explicitly published and readable."""
        return self.list_books(keyword=keyword, include_hidden=False)

    def page_published_books(
        self,
        *,
        keyword: str = "",
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        """Page published books in SQLite without per-book filesystem hydration."""
        page = max(1, int(page or 1))
        page_size = max(1, min(int(page_size or 20), 100))
        normalized_keyword = str(keyword or "").strip()
        where = [
            "search_visibility_status = 'visible'",
            "visible_processed_chapters > 0",
        ]
        params: list[Any] = []
        if normalized_keyword:
            where.append("(name LIKE ? COLLATE NOCASE OR author LIKE ? COLLATE NOCASE)")
            pattern = f"%{normalized_keyword}%"
            params.extend([pattern, pattern])
        where_sql = " AND ".join(where)
        offset = (page - 1) * page_size
        with self._conn() as conn:
            total = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM aggregate_book_tasks WHERE {where_sql}",
                    params,
                ).fetchone()[0]
                or 0
            )
            rows = conn.execute(
                f"""
                SELECT aggregate_book_id, canonical_name, canonical_author, name, author,
                       cover_url, intro, word_count, primary_book_id, primary_source_id,
                       primary_source_name, primary_book_url, primary_toc_url, added_by_user_id,
                       start_chapter_index, total_chapters_at_subscribe, initial_snapshot_last_index,
                       backfill_started, auto_archive_on_complete, search_visibility_status,
                       book_status, total_chapters, processed_chapters, visible_processed_chapters,
                       failed_chapters, status, settings_json, current_policy_version,
                       last_source_chapter_title, '', last_check_time, next_check_time, last_error,
                       archived_at, created_at, updated_at, interval_minutes
                FROM aggregate_book_tasks
                WHERE {where_sql}
                ORDER BY updated_at DESC, created_at DESC
                LIMIT ? OFFSET ?
                """,
                [*params, page_size, offset],
            ).fetchall()
        return {
            "items": [
                item
                for row in rows
                if (item := self._aggregate_book_row_to_dict(row)) is not None
            ],
            "page": page,
            "pageSize": page_size,
            "total": total,
        }

    def search_published_books(self, keyword: str) -> list[dict[str, Any]]:
        """Search the explicit Reading publication set without changing state."""
        normalized_keyword = str(keyword or "").strip().lower()
        if not normalized_keyword:
            return []
        return [
            book
            for book in self.list_books(keyword=keyword, include_hidden=False)
            if normalized_keyword in str(book.get("name", "") or "").lower()
            or normalized_keyword in str(book.get("author", "") or "").lower()
        ]

    def _decode_library_book_id(self, book_id: str) -> str:
        try:
            source_id, book_url = decode_book_id(book_id)
            payload = unpack_aggregate_book_url(book_url)
        except Exception:
            return ""
        if source_id != VIRTUAL_SOURCE_ID or not payload.get("library"):
            return ""
        return str(payload.get("aggregateBookId", "") or "")

    def is_virtual_book_id(self, book_id: str) -> bool:
        try:
            source_id, _ = decode_book_id(book_id)
        except Exception:
            return False
        return source_id == VIRTUAL_SOURCE_ID

    def _published_book(self, aggregate_book_id: str) -> dict[str, Any] | None:
        book = self.get_book(aggregate_book_id)
        if not book or book.get("searchVisibilityStatus") != "visible":
            return None
        return book

    def legado_book_detail(self, book_id: str, *, base_api: str) -> dict[str, Any] | None:
        """Map one published shared book to the stable Legado detail contract."""
        aggregate_book_id = self._decode_library_book_id(book_id)
        book = self._published_book(aggregate_book_id) if aggregate_book_id else None
        if not book:
            return None
        return {
            "implemented": True,
            "data": {
                "sourceId": VIRTUAL_SOURCE_ID,
                "bookId": book_id,
                "name": book.get("name", ""),
                "author": book.get("author", ""),
                "coverUrl": book.get("coverUrl", ""),
                "intro": book.get("intro", ""),
                "kind": "共享书库",
                "lastChapter": book.get("lastSourceChapterTitle", "")
                or book.get("lastLocalChapterTitle", ""),
                "wordCount": book.get("wordCount", ""),
                "status": book.get("bookStatus", ""),
                "bookUrl": f"{base_api}/api/legado/book/{book_id}",
                "tocUrl": f"{base_api}/api/legado/book/{book_id}/toc",
            },
            "debug": {"aggregate": True, "published": True},
        }

    def legado_toc(self, book_id: str, *, base_api: str) -> dict[str, Any] | None:
        """Return only chapter entries that already have published shared content."""
        aggregate_book_id = self._decode_library_book_id(book_id)
        book = self._published_book(aggregate_book_id) if aggregate_book_id else None
        if not book:
            return None
        _book, _book_dir, records = self._shared_chapter_catalog(
            aggregate_book_id,
            book=book,
        )
        chapters: list[dict[str, Any]] = []
        for record in records:
            item = record
            read_chapter_id = str(item.get("readChapterId", "") or "")
            if item.get("hasContent") and not read_chapter_id:
                item = self._hydrate_shared_chapter_record(aggregate_book_id, record)
                read_chapter_id = str(item.get("readChapterId", "") or "")
            if not item.get("hasContent") or not read_chapter_id:
                continue
            chapters.append(
                {
                    "sourceId": VIRTUAL_SOURCE_ID,
                    "chapterId": read_chapter_id,
                    "index": int(item.get("chapterIndex", 0) or 0),
                    "title": item.get("title", ""),
                    "volumeName": item.get("volumeName", ""),
                    "chapterUrl": f"{base_api}/api/legado/chapter/{read_chapter_id}",
                    "updateTime": format_reading_update_time(item.get("processedAt", "")),
                    "isVip": bool(item.get("isVip")),
                    "isPaid": bool(item.get("isPaid")),
                    "isPay": bool(item.get("isPay")),
                    "previewOnly": bool(item.get("previewOnly")),
                }
            )
        return {
            "implemented": True,
            "bookId": book_id,
            "chapters": chapters,
            "debug": {"aggregate": True, "published": True},
        }

    def read_shared_chapter(
        self,
        chapter_id: str,
        *,
        published_only: bool,
    ) -> dict[str, Any] | None:
        """Read one shared UTF-8 chapter without invoking processing services."""
        try:
            source_id, chapter_url = decode_chapter_id(chapter_id)
            payload = unpack_aggregate_chapter_url(chapter_url)
        except Exception:
            return None
        if source_id != VIRTUAL_SOURCE_ID:
            return None
        aggregate_book_id = str(payload.get("aggregateBookId", "") or "")
        book = (
            self._published_book(aggregate_book_id)
            if published_only
            else self.get_book(aggregate_book_id)
        )
        if not book:
            return None

        _book, _book_dir, records = self._shared_chapter_catalog(
            aggregate_book_id,
            book=book,
        )
        payload_index = int(payload.get("index", 0) or 0)
        target = next(
            (
                item
                for item in records
                if int(item.get("chapterIndex", 0) or 0) == payload_index
            ),
            None,
        )
        if target is None:
            target = next(
                (item for item in records if item.get("readChapterId") == chapter_id),
                None,
            )
        if not target or not target.get("hasContent"):
            return None

        expected_chapter_id = str(target.get("readChapterId", "") or "")
        if not expected_chapter_id:
            source_chapter_id = str(payload.get("sourceChapterId", "") or "")
            if source_chapter_id:
                expected_chapter_id = encode_chapter_id(
                    VIRTUAL_SOURCE_ID,
                    make_aggregate_chapter_url(
                        aggregate_book_id=aggregate_book_id,
                        source_chapter_id=source_chapter_id,
                        title=str(target.get("title", "") or ""),
                        index=int(target.get("chapterIndex", 0) or 0),
                    ),
                )
        if expected_chapter_id != chapter_id:
            return None

        chapter_path = target.get("_chapterPath")
        if not isinstance(chapter_path, Path):
            return None
        try:
            markdown = chapter_path.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            return None
        body, _, _ = markdown.partition(f"<!-- {TRACE_BEGIN}")
        body = body.strip()
        if body.startswith("# "):
            lines = body.split("\n", 1)
            body = lines[1].strip() if len(lines) > 1 else ""
        body = strip_leading_duplicate_chapter_title(body, str(target.get("title", "") or ""))
        if not body or "\ufffd" in body or "\x00" in body:
            return None

        try:
            trace = self.shared_book_storage.parse_trace_block(markdown)
        except ValueError:
            trace = {}
        preview_only = bool(trace.get("previewOnly", target.get("previewOnly", False)))
        is_vip = bool(target.get("isVip", trace.get("isVip", preview_only)))
        return {
            "implemented": True,
            "chapterId": chapter_id,
            "title": target.get("title", ""),
            "content": body,
            "authRequired": preview_only,
            "isVip": is_vip,
            "isPaid": is_vip,
            "isPay": is_vip and not preview_only,
            "previewOnly": preview_only,
            "extra": {
                "previewOnly": preview_only,
                "isVip": is_vip,
                "contentAccess": "preview" if preview_only else "full",
            },
            "debug": {"aggregate": True, "published": True},
        }

    def legado_chapter(self, chapter_id: str) -> dict[str, Any] | None:
        """Read one published chapter for the Reading/Legado data plane."""
        return self.read_shared_chapter(chapter_id, published_only=True)

    def source_snapshot_content(
        self,
        aggregate_book_id: str,
        chapter_index: int,
        source_ids: list[str],
    ) -> tuple[str, str]:
        """Load the first usable official snapshot in explicit priority order."""
        ordered_source_ids = list(dict.fromkeys(source_id for source_id in source_ids if source_id))
        if not aggregate_book_id or chapter_index <= 0 or not ordered_source_ids:
            return "", ""
        with self._conn() as conn:
            for source_id in ordered_source_ids:
                row = conn.execute(
                    """
                    SELECT clean_content
                    FROM aggregate_source_snapshots
                    WHERE aggregate_book_id = ? AND chapter_index = ? AND source_id = ?
                    LIMIT 1
                    """,
                    (aggregate_book_id, chapter_index, source_id),
                ).fetchone()
                content = str(row[0] or "") if row else ""
                if content and "\ufffd" not in content and "\x00" not in content:
                    return content, source_id
        return "", ""

    def build_search_injected_items_for_groups(
        self,
        groups: list[dict[str, Any]],
        *,
        base_api: str | None = None,
        score_bonus: int = 50,
        min_readable_chapters: int | None = None,
    ) -> list[dict[str, Any]]:
        seen: set[str] = set()
        items: list[dict[str, Any]] = []
        for group in groups:
            book = self.find_existing_book(group, visible_only=False)
            if not book or not self._is_injectable_book(book, min_readable_chapters=min_readable_chapters):
                continue
            aggregate_book_id = book["aggregateBookId"]
            if aggregate_book_id in seen:
                continue
            seen.add(aggregate_book_id)
            base_score = int(group.get("score", 0) or 0)
            injected = self.build_search_injected_item(
                book,
                base_api=base_api,
                score=base_score + int(score_bonus or 0),
            )
            injected["candidateId"] = group.get("candidateId", "")
            items.append(injected)
        return items

    def build_search_injected_items_for_keyword(
        self,
        keyword: str,
        *,
        base_api: str | None = None,
        score_bonus: int = 50,
        min_readable_chapters: int | None = None,
    ) -> list[dict[str, Any]]:
        books = self.search_visible_books(keyword, min_readable_chapters=min_readable_chapters)
        items = []
        for book in books:
            score = self.library_match_score(book, keyword, bonus=score_bonus)
            items.append(self.build_search_injected_item(book, base_api=base_api, score=score))
        return items

    def load_payload(self, aggregate_book_id: str) -> dict[str, Any]:
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

    def username_for_user_id(self, user_id: str) -> str:
        if not user_id:
            return ""
        with self._conn() as conn:
            row = conn.execute("SELECT username FROM users WHERE user_id = ?", (user_id,)).fetchone()
        return row[0] if row and row[0] else ""


library_books_service = LibraryBooksService()
