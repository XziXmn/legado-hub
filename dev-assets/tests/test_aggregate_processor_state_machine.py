"""Tests for AggregateProcessor state machine: preview / third-party / fallback paths."""

import asyncio
import json
import re
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from app.services.aggregate_processor import AggregateProcessor
from app.services.aggregate_alignment import align_candidate_chapter
from app.services.aggregate_line_consensus import purify_by_line_consensus
from app.services.aggregate_settings import PROCESSING_PLACEHOLDER
from app.services.catalog import Catalog
from app.services.novel_file_cache import NovelFileCache
from app.services.shared_book_runtime import SharedBookRuntimeStore
from app.services.shared_book_storage import SharedBookStorage
from app.source_plugins.id_codec import decode_chapter_id, encode_chapter_id
from app.storage.db import initialize_database
from app.services.aggregate_virtual_source import make_aggregate_chapter_url
from app.services.library_books import LibraryBooksService


# ── helpers ──────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _disable_real_candidate_discovery(monkeypatch):
    scheduler = SimpleNamespace(_enabled_plugins=lambda: [])
    monkeypatch.setattr(
        "app.source_plugins.scheduler.get_plugin_scheduler",
        lambda: scheduler,
    )


def _setup_db(tmp_path, *, ai_enabled=True, purify="conservative", ai_provider=None):
    db_path = tmp_path / "test.db"
    initialize_database(db_path)

    # The product code now reads workflow settings from backend/config/app_config.json
    # instead of the legacy admin_settings table. Write an isolated config per test.
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "app_config.json"
    workflow = {
        "autoAggregate": True,
        "processAggregateOnRead": True,
        "aggregateCheckIntervalMinutes": 10,
        "purifyMode": purify,
        "aiEnabled": ai_enabled,
        "useSharedBookStorage": True,
        "sharedBookStorageDualWrite": False,
    }
    config_data: dict[str, Any] = {"aggregate": {"contentWorkflow": workflow}}
    if ai_provider is not None:
        config_data["ai"] = {"provider": ai_provider}
    config_path.write_text(json.dumps(config_data, ensure_ascii=False), encoding="utf-8")

    import app.core.app_config as _app_config_module

    _app_config_module.APP_CONFIG_PATH = config_path
    _app_config_module.AppConfig.reset()
    return db_path


def _insert_book(db_path, book_id, *, primary_source_id="official_src",
                 aggregate_payload=None):
    payload = aggregate_payload or {
        "name": "测试书", "author": "作者",
        "sources": [
            {"bookId": f"official_src:{book_id}", "sourceId": "official_src",
             "score": 100, "bookUrl": f"https://official.example/book/{book_id}"},
            {"bookId": f"candidate_src:{book_id}", "sourceId": "candidate_src",
             "score": 80, "bookUrl": f"https://candidate.example/book/{book_id}"},
        ],
    }
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """INSERT OR REPLACE INTO aggregate_book_tasks
               (aggregate_book_id, name, author, primary_book_id, primary_source_id,
                aggregate_payload_json, status, ai_enabled, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, 'active', 1, datetime('now'), datetime('now'))""",
            (book_id, payload.get("name", ""), payload.get("author", ""),
             f"{primary_source_id}:{book_id}", primary_source_id,
             json.dumps(payload, ensure_ascii=False)),
        )
        conn.commit()


def _insert_chapter(db_path, book_id, index=1, *, status="pending"):
    ch_id = f"{book_id}:ch{index}"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """INSERT OR REPLACE INTO aggregate_chapter_tasks
               (chapter_id, aggregate_book_id, source_chapter_id, chapter_index, title, status,
                created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))""",
            (ch_id, book_id, f"official_src:ch{index}", index, f"第{index}章", status),
        )
        conn.commit()
    return ch_id


def test_read_chapter_content_skips_garbled_local_file(tmp_path):
    processor = AggregateProcessor(db_path=tmp_path / "test.db")
    normal = tmp_path / "normal.md"
    garbled = tmp_path / "garbled.md"
    normal.write_text("# 第一章\n\n这是正常中文正文。\n", encoding="utf-8")
    garbled.write_text("# 第一章\n\njF9@\ufffd\ufffd\ufffd\u0011\u0000\ufffd" * 20, encoding="utf-8")

    assert processor._read_chapter_content_from_file(str(normal)) == "这是正常中文正文。"
    assert processor._read_chapter_content_from_file(str(garbled)) == ""


def test_processor_library_service_uses_its_shared_storage_root(tmp_path):
    processor = AggregateProcessor(db_path=tmp_path / "data" / "app.db")

    assert processor._library_books().shared_book_storage.root == tmp_path / "data" / "library"


@pytest.mark.asyncio
async def test_book_catalog_forwards_chapter_reviews(monkeypatch):
    from app.services.book_catalog import BookCatalog

    expected = {"authorReviews": [{"content": "作家说"}]}

    async def fake_chapter_reviews(_catalog, chapter_id: str, user_agent: str = ""):
        assert chapter_id == "official:chapter"
        return expected

    monkeypatch.setattr(Catalog, "chapter_reviews", fake_chapter_reviews)
    catalog = object.__new__(BookCatalog)
    catalog.repo = None
    catalog.cache = None

    assert await catalog.chapter_reviews("official:chapter") == expected


@pytest.mark.asyncio
async def test_aggregate_reading_keeps_cross_source_selected_prose(monkeypatch):
    import app.api.legado as legado_api

    content = "他打开百度搜索，查询起点评论。"
    strip_author_say = AsyncMock(return_value=content)
    monkeypatch.setattr(legado_api, "_strip_author_say_from_chapter_content", strip_author_say)

    result = await legado_api._apply_reading_content_gates(
        chapter_id="legadohub_ai_aggregate:test",
        content=content,
        source_id="legadohub_ai_aggregate",
        apply_purify=False,
    )

    assert result == content
    assert strip_author_say.await_args.kwargs["content"] == content


def test_garbled_content_is_not_persisted_or_reused(tmp_path):
    db_path = _setup_db(tmp_path)
    processor = AggregateProcessor(db_path=db_path)
    garbled = "jF9@\ufffd\ufffd\ufffd\u0011\u0000\ufffd" * 20

    with pytest.raises(ValueError, match="garbled"):
        processor._save_source_snapshot(
            aggregate_book_id="book-1",
            chapter_index=1,
            source_id="official_src",
            source_book_id="source-book-1",
            source_chapter_id="source-chapter-1",
            title="第一章",
            clean_content=garbled,
            classification="unknown",
        )

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """INSERT INTO aggregate_source_snapshots
               (aggregate_book_id, chapter_index, source_id, clean_content, content_hash)
               VALUES (?, ?, ?, ?, ?)""",
            ("book-1", 1, "official_src", garbled, "legacy-bad-hash"),
        )
        conn.commit()

    assert processor._load_source_snapshot_content(
        aggregate_book_id="book-1",
        chapter_index=1,
        source_id="official_src",
    ) == ""

    with pytest.raises(ValueError, match="garbled"):
        processor._write_chapter_result(
            chapter_id="book-1:ch1",
            aggregate_book_id="book-1",
            title="第一章",
            chapter_index=1,
            status="processed",
            content=garbled,
            alignment_json={},
        )

    with sqlite3.connect(db_path) as conn, pytest.raises(ValueError, match="garbled"):
        NovelFileCache(root=tmp_path / "cache").write_chapter(
            conn=conn,
            chapter_id="official_src:chapter-1",
            source_id="official_src",
            chapter_url="https://example.com/chapter/1",
            title="第一章",
            content=garbled,
        )


def test_catalog_rejects_garbled_plugin_chapter():
    garbled = "jF9@\ufffd\ufffd\ufffd\u0011\u0000\ufffd" * 20

    class FakeScheduler:
        async def chapter(self, source_id, chapter_url):
            return {"title": "第一章", "content": garbled, "debug": {}}

    class FakeCache:
        def get_chapter(self, chapter_id):
            return None

        def set_chapter(self, *args, **kwargs):
            raise AssertionError("garbled content must not enter chapter_cache")

    catalog = object.__new__(Catalog)
    catalog.scheduler = FakeScheduler()
    catalog.cache = FakeCache()
    chapter_id = encode_chapter_id("official_src", "https://example.com/chapter/1")

    result = asyncio.run(catalog.chapter(chapter_id))

    assert result["content"] == ""
    assert result["debug"]["error"] == "garbled chapter content"


def test_catalog_preserves_paid_chapter_flags():
    cached: list[dict] = []

    class FakeScheduler:
        async def chapter(self, source_id, chapter_url):
            return {
                "title": "付费章节",
                "content": "付费章节预览",
                "authRequired": True,
                "isPaid": True,
                "extra": {"previewOnly": True},
                "debug": {},
            }

    class FakeCache:
        def get_chapter(self, chapter_id):
            return None

        def set_chapter(self, chapter_id, source_id, chapter_url, data):
            cached.append(data)

    catalog = object.__new__(Catalog)
    catalog.scheduler = FakeScheduler()
    catalog.cache = FakeCache()
    chapter_id = encode_chapter_id("official_src", "https://example.com/chapter/paid")

    result = asyncio.run(catalog.chapter(chapter_id))

    assert result["authRequired"] is True
    assert result["isPaid"] is True
    assert cached[0]["authRequired"] is True
    assert cached[0]["isPaid"] is True


def test_candidate_sources_respect_source_candidate_limit(tmp_path, monkeypatch):
    db_path = _setup_db(tmp_path)
    processor = AggregateProcessor(db_path=db_path)
    monkeypatch.setattr(
        processor,
        "_book_workflow_settings",
        lambda _book_id: {"sourceCandidateLimit": 2},
    )

    class FakeSourceMapService:
        def __init__(self, *args, **kwargs):
            pass

        def load_current_source_map_refs(self, *args, **kwargs):
            return [
                {"sourceId": "s1", "bookId": "s1:b", "score": 100},
                {"sourceId": "s2", "bookId": "s2:b", "score": 90},
                {"sourceId": "s3", "bookId": "s3:b", "score": 80},
            ]

    monkeypatch.setattr(
        "app.services.shared_book_source_map.SharedBookSourceMapService",
        FakeSourceMapService,
    )

    candidates = processor._candidate_sources_from_payload({}, "official", "book-1")

    assert [item["sourceId"] for item in candidates] == ["s1", "s2"]


def test_candidate_sources_default_to_three_sources(tmp_path, monkeypatch):
    db_path = _setup_db(tmp_path)
    processor = AggregateProcessor(db_path=db_path)
    monkeypatch.setattr(processor, "_book_workflow_settings", lambda _book_id: {})

    class FakeSourceMapService:
        def __init__(self, *args, **kwargs):
            pass

        def load_current_source_map_refs(self, *args, **kwargs):
            return [
                {"sourceId": f"s{index}", "bookId": f"s{index}:b", "score": 100 - index}
                for index in range(1, 5)
            ]

    monkeypatch.setattr(
        "app.services.shared_book_source_map.SharedBookSourceMapService",
        FakeSourceMapService,
    )

    candidates = processor._candidate_sources_from_payload({}, "official", "book-1")

    assert [item["sourceId"] for item in candidates] == ["s1", "s2", "s3"]


def test_candidate_sources_expand_to_eight_only_for_consensus_retry(tmp_path, monkeypatch):
    db_path = _setup_db(tmp_path)
    processor = AggregateProcessor(db_path=db_path)
    monkeypatch.setattr(processor, "_book_workflow_settings", lambda _book_id: {})

    class FakeSourceMapService:
        def __init__(self, *args, **kwargs):
            pass

        def load_current_source_map_refs(self, *args, **kwargs):
            return [
                {"sourceId": f"s{index}", "bookId": f"s{index}:b", "score": 100 - index}
                for index in range(1, 10)
            ]

    monkeypatch.setattr(
        "app.services.shared_book_source_map.SharedBookSourceMapService",
        FakeSourceMapService,
    )

    candidates = processor._candidate_sources_from_payload(
        {}, "official", "book-1", include_expansion=True
    )

    assert [item["sourceId"] for item in candidates] == [f"s{index}" for index in range(1, 9)]


def test_candidate_sources_exclude_all_official_plugins(tmp_path, monkeypatch):
    db_path = _setup_db(tmp_path)
    processor = AggregateProcessor(db_path=db_path)
    monkeypatch.setattr(processor, "_book_workflow_settings", lambda _book_id: {})
    monkeypatch.setattr(
        processor,
        "_is_official_source",
        lambda source_id: source_id in {"qidian_com_app", "qidian_com_web"},
    )

    class FakeSourceMapService:
        def __init__(self, *args, **kwargs):
            pass

        def load_current_source_map_refs(self, *args, **kwargs):
            return [
                {"sourceId": "qidian_com_web", "bookId": "web:b", "score": 100},
                {"sourceId": "mirror_a", "bookId": "a:b", "score": 90},
                {"sourceId": "mirror_b", "bookId": "b:b", "score": 80},
            ]

    monkeypatch.setattr(
        "app.services.shared_book_source_map.SharedBookSourceMapService",
        FakeSourceMapService,
    )

    candidates = processor._candidate_sources_from_payload({}, "qidian_com_app", "book-1")

    assert [item["sourceId"] for item in candidates] == ["mirror_a", "mirror_b"]


def test_candidate_sources_respect_candidate_source_priority(tmp_path, monkeypatch):
    db_path = _setup_db(tmp_path)
    processor = AggregateProcessor(db_path=db_path)
    monkeypatch.setattr(
        processor,
        "_book_workflow_settings",
        lambda _book_id: {
            "sourceCandidateLimit": 10,
            "candidateSourcePriority": ["s3", "s1"],
        },
    )

    class FakeSourceMapService:
        def __init__(self, *args, **kwargs):
            pass

        def load_current_source_map_refs(self, *args, **kwargs):
            return [
                {"sourceId": "s1", "bookId": "s1:b", "score": 100},
                {"sourceId": "s2", "bookId": "s2:b", "score": 90},
                {"sourceId": "s3", "bookId": "s3:b", "score": 80},
            ]

    monkeypatch.setattr(
        "app.services.shared_book_source_map.SharedBookSourceMapService",
        FakeSourceMapService,
    )

    candidates = processor._candidate_sources_from_payload({}, "official", "book-1")

    assert [item["sourceId"] for item in candidates] == ["s3", "s1"]


def test_candidate_sources_try_browser_sources_after_http_sources(tmp_path, monkeypatch):
    db_path = _setup_db(tmp_path)
    processor = AggregateProcessor(db_path=db_path)
    processor._browser_source_ids = {"slow_browser"}
    monkeypatch.setattr(processor, "_book_workflow_settings", lambda _book_id: {})

    class FakeSourceMapService:
        def __init__(self, *args, **kwargs):
            pass

        def load_current_source_map_refs(self, *args, **kwargs):
            return [
                {"sourceId": "slow_browser", "bookId": "browser:b", "score": 100},
                {"sourceId": "http_source", "bookId": "http:b", "score": 90},
            ]

    monkeypatch.setattr(
        "app.services.shared_book_source_map.SharedBookSourceMapService",
        FakeSourceMapService,
    )

    candidates = processor._candidate_sources_from_payload({}, "official", "book-1")

    assert [item["sourceId"] for item in candidates] == ["http_source", "slow_browser"]


class _FakeCatalog:
    """Returns different content depending on source_chapter_id prefix."""

    def __init__(self, *, official_content="", candidate_content="",
                 official_fail=False, candidate_fail=False,
                 official_extra=None, candidate_extra=None):
        self._official = official_content
        self._candidate = candidate_content
        self._official_fail = official_fail
        self._candidate_fail = candidate_fail
        self._official_extra = official_extra
        self._candidate_extra = candidate_extra

    async def chapter(self, chapter_id: str) -> dict:
        if chapter_id.startswith("official_src"):
            if self._official_fail:
                raise RuntimeError("official source failed")
            result = {"content": self._official, "title": "第1章"}
            if self._official_extra is not None:
                result["extra"] = self._official_extra
            return result
        if chapter_id.startswith("candidate_src"):
            if self._candidate_fail:
                raise RuntimeError("candidate source failed")
            result = {"content": self._candidate, "title": "第1章"}
            if self._candidate_extra is not None:
                result["extra"] = self._candidate_extra
            return result
        # Fallback: try official
        if self._official_fail:
            raise RuntimeError("source fetch failed")
        result = {"content": self._official, "title": "第1章"}
        if self._official_extra is not None:
            result["extra"] = self._official_extra
        return result

    async def toc(self, book_id: str) -> dict:
        """Return a fake TOC with one chapter per book."""
        source_id = book_id.split(":")[0] if ":" in book_id else book_id
        from app.source_plugins.id_codec import encode_chapter_id as enc
        ch_url = f"https://{source_id}.example/ch1.html"
        return {"chapters": [
            {"chapterId": enc(source_id, ch_url), "title": "第1章",
             "chapterUrl": ch_url, "index": 1}
        ]}


class _FakeAIService:
    """Records calls and returns canned results."""

    def __init__(self, *, content="AI 聚合正文", fail=False, error=None, self_score=1.0):
        self._content = content
        self._fail = fail
        self._error = error
        self._self_score = self_score
        self.calls: list[dict] = []

    async def process_official_full(self, **kwargs):
        self.calls.append({"method": "official_full", **kwargs})
        if self._fail:
            raise self._error or RuntimeError("AI aggregation failed")
        return {"status": "processed", "content": self._content,
                "selfScore": self._self_score,
                "aiModel": "official-fake-model", "promptTokens": 120, "completionTokens": 60,
                "totalTokens": 180, "latencyMs": 250, "plannedAnalysis": True}

    async def process_with_candidates(self, **kwargs):
        self.calls.append({"method": "with_candidates", **kwargs})
        if self._fail:
            raise self._error or RuntimeError("AI aggregation failed")
        return {"status": "processed", "content": self._content,
                "selfScore": self._self_score,
                "aiModel": "fake-model", "promptTokens": 100, "completionTokens": 50,
                "totalTokens": 150, "latencyMs": 200, "plannedAnalysis": False}

    async def process_third_party_primary(self, **kwargs):
        self.calls.append({"method": "third_party_primary", **kwargs})
        if self._fail:
            return {"status": "fallback", "content": kwargs.get("content", ""),
                    "selfScore": 0.0,
                    "aiModel": "", "error": str(self._error or "AI failed"),
                    "promptTokens": 0, "completionTokens": 0,
                    "totalTokens": 0, "latencyMs": 0, "plannedAnalysis": False}
        return {"status": "processed", "content": self._content,
                "selfScore": self._self_score,
                "aiModel": "fake-model", "promptTokens": 100, "completionTokens": 50,
                "totalTokens": 150, "latencyMs": 200, "plannedAnalysis": False}


def _chapter_dict(ch_id, book_id, index=1):
    return {"chapterId": ch_id, "sourceChapterId": f"official_src:ch{index}",
            "title": f"第{index}章", "chapterIndex": index, "aggregateBookId": book_id}


def _get_chapter_row(db_path, ch_id):
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT status, content_file_path, source_alignment_json, fallback_source_id, last_error_code, preview_only "
            "FROM aggregate_chapter_tasks WHERE chapter_id = ?", (ch_id,),
        ).fetchone()
    if not row:
        return None
    alignment = {}
    try:
        alignment = json.loads(row[2] or "{}")
    except Exception:
        pass
    content = ""
    if row[1]:
        try:
            content = Path(row[1]).read_text(encoding="utf-8")
            TRACE_RE = re.compile(
                r"(?:\n|^)(?:<!--\s*)?LEGADOHUB_TRACE_BEGIN\s*(?:```yaml\s*)?\n.*?\n(?:\s*```\s*)?LEGADOHUB_TRACE_END(?:\s*-->)?\s*$",
                re.DOTALL,
            )
            content = TRACE_RE.sub("", content).strip()
            if content.startswith("# "):
                lines = content.split("\n", 1)
                content = lines[1] if len(lines) > 1 else ""
            content = content.strip()
        except Exception:
            pass
    return {"status": row[0], "content": content, "alignment": alignment,
            "fallbackSourceId": row[3] or "", "lastErrorCode": row[4] or "",
            "previewOnly": bool(row[5]), "contentFilePath": row[1] or ""}


def _get_policy_snapshot(db_path, ch_id):
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT policy_snapshot_json FROM aggregate_chapter_tasks WHERE chapter_id = ?",
            (ch_id,),
        ).fetchone()
    if not row or not row[0]:
        return {}
    return json.loads(row[0])


# ── background candidate discovery ───────────────────────────────────────────


def test_ensure_candidate_sources_discovers_and_persists_third_party_matches(tmp_path, monkeypatch):
    """A newly subscribed official-only book should get third-party candidates during aggregation."""
    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:auto_candidates"
    payload = {
        "name": "测试书",
        "author": "作者",
        "sources": [
            {
                "bookId": "official_src:1",
                "sourceId": "official_src",
                "sourceName": "官方源",
                "bookUrl": "https://official.example/book/1",
                "score": 100,
            }
        ],
    }
    _insert_book(db_path, book_id, primary_source_id="official_src", aggregate_payload=payload)

    class Metadata:
        id = "candidate_src"
        name = "候选源"
        priority = 50

        def is_official_source(self):
            return False

    plugin = SimpleNamespace(metadata=Metadata(), capabilities=["search"])

    discovery_limits: list[int] = []

    class Scheduler:
        config = {"max_concurrency": 2}

        def _enabled_plugins(self):
            return [plugin]

        def _search_priority_plugins(self, plugins):
            return plugins

        async def search_one(self, source_id, keyword, page):
            assert source_id == "candidate_src"
            assert keyword == "测试书"
            return {
                "items": [
                    {
                        "sourceId": "candidate_src",
                        "sourceName": "候选源",
                        "name": "测试书",
                        "author": "作者",
                        "bookUrl": "https://candidate.example/book/1",
                        "lastChapter": "第十章",
                        "score": 80,
                    },
                    {
                        "sourceId": "candidate_src",
                        "sourceName": "候选源",
                        "name": "别的书",
                        "author": "作者",
                        "bookUrl": "https://candidate.example/book/2",
                    },
                ],
                "error": None,
            }

    monkeypatch.setattr(
        "app.source_plugins.scheduler.get_plugin_scheduler",
        lambda: Scheduler(),
    )

    processor = AggregateProcessor(db_path)
    monkeypatch.setattr(processor, "_is_official_source", lambda sid: sid == "official_src")
    original_discover = processor._discover_third_party_candidates

    async def _capture_discovery_limit(**kwargs):
        discovery_limits.append(kwargs["max_candidates"])
        return await original_discover(**kwargs)

    monkeypatch.setattr(processor, "_discover_third_party_candidates", _capture_discovery_limit)
    updated = asyncio.run(processor._ensure_candidate_sources_for_book(book_id, payload))

    assert [src["sourceId"] for src in updated["sources"]] == ["official_src", "candidate_src"]
    assert discovery_limits == [0]
    candidate = updated["sources"][1]
    assert candidate["bookId"].startswith("candidate_src:")
    assert candidate["bookUrl"] == "https://candidate.example/book/1"

    with sqlite3.connect(db_path) as conn:
        source_row = conn.execute(
            """
            SELECT source_id, source_book_id, role, enabled, last_chapter_title
            FROM aggregate_book_sources
            WHERE aggregate_book_id = ? AND source_id = ?
            """,
            (book_id, "candidate_src"),
        ).fetchone()
        payload_row = conn.execute(
            "SELECT aggregate_payload_json FROM aggregate_book_tasks WHERE aggregate_book_id = ?",
            (book_id,),
        ).fetchone()

    assert source_row is not None
    assert source_row[2] == "candidate"
    assert source_row[3] == 1
    assert source_row[4] == "第十章"
    persisted = json.loads(payload_row[0])
    assert [src["sourceId"] for src in persisted["sources"]] == ["official_src", "candidate_src"]


def test_candidate_discovery_includes_browser_sources_without_losing_http_results(tmp_path, monkeypatch):
    processor = AggregateProcessor(tmp_path / "test.db")

    class Metadata:
        def __init__(self, source_id, browser):
            self.id = source_id
            self.name = source_id
            self.priority = 50
            self.browser = browser

        def is_official_source(self):
            return False

    browser = SimpleNamespace(metadata=Metadata("browser_source", {"mode": "required"}), capabilities=["search"])
    optional = SimpleNamespace(metadata=Metadata("optional_source", {"mode": "optional"}), capabilities=["search"])
    http = SimpleNamespace(metadata=Metadata("http_source", {}), capabilities=["search"])

    class Scheduler:
        config = {"max_concurrency": 2}

        def __init__(self):
            self.calls = []

        def _enabled_plugins(self):
            return [browser, optional, http]

        def _search_priority_plugins(self, plugins):
            return plugins

        async def search_one(self, source_id, keyword, page):
            self.calls.append(source_id)
            if source_id == "browser_source":
                raise RuntimeError("browser unavailable")
            return {
                "items": [{
                    "sourceId": source_id,
                    "name": "测试书",
                    "author": "作者",
                    "bookUrl": f"https://{source_id}.test/book",
                }],
                "error": None,
            }

    scheduler = Scheduler()
    monkeypatch.setattr("app.source_plugins.scheduler.get_plugin_scheduler", lambda: scheduler)

    discovered = asyncio.run(processor._discover_third_party_candidates(
        keyword="测试书",
        author="作者",
        existing_sources=[],
        max_candidates=0,
        max_sources=8,
    ))

    assert {item["sourceId"] for item in discovered} == {"http_source", "optional_source"}
    assert set(scheduler.calls) == {"browser_source", "optional_source", "http_source"}


def test_processing_enabled_does_not_use_process_aggregate_on_read_for_background_subscription(tmp_path):
    """Disabling reading-triggered aggregation must not disable shared-subscription background jobs."""
    db_path = _setup_db(tmp_path, ai_enabled=False)

    config_dir = tmp_path / "config"
    config_path = config_dir / "app_config.json"
    config_data = json.loads(config_path.read_text(encoding="utf-8"))
    config_data["aggregate"]["contentWorkflow"]["processAggregateOnRead"] = False
    config_path.write_text(json.dumps(config_data, ensure_ascii=False), encoding="utf-8")

    import app.core.app_config as _app_config_module

    _app_config_module.AppConfig.reset()

    processor = AggregateProcessor(db_path)
    assert processor.processing_enabled() is True
    assert processor.processing_enabled("book:any") is True


def test_processing_enabled_respects_auto_aggregate_for_background_subscription(tmp_path):
    """The real background subscription kill switch remains autoAggregate."""
    db_path = _setup_db(tmp_path, ai_enabled=False)

    config_dir = tmp_path / "config"
    config_path = config_dir / "app_config.json"
    config_data = json.loads(config_path.read_text(encoding="utf-8"))
    config_data["aggregate"]["contentWorkflow"]["autoAggregate"] = False
    config_path.write_text(json.dumps(config_data, ensure_ascii=False), encoding="utf-8")

    import app.core.app_config as _app_config_module

    _app_config_module.AppConfig.reset()

    processor = AggregateProcessor(db_path)
    assert processor.processing_enabled() is False
    assert processor.processing_enabled("book:any") is False


def test_run_due_once_still_runs_when_process_aggregate_on_read_disabled(tmp_path, monkeypatch):
    """Background subscription scheduler must still run when only reading-trigger aggregation is disabled."""
    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:due_processes"
    _insert_book(db_path, book_id)

    config_dir = tmp_path / "config"
    config_path = config_dir / "app_config.json"
    config_data = json.loads(config_path.read_text(encoding="utf-8"))
    config_data["aggregate"]["contentWorkflow"]["processAggregateOnRead"] = False
    config_path.write_text(json.dumps(config_data, ensure_ascii=False), encoding="utf-8")

    import app.core.app_config as _app_config_module

    _app_config_module.AppConfig.reset()

    processor = AggregateProcessor(db_path)

    async def _fake_run_book_task(aggregate_book_id: str):
        return {"bookId": aggregate_book_id, "success": True}

    monkeypatch.setattr(processor, "run_book_task", _fake_run_book_task)

    result = asyncio.run(processor.run_due_once(limit=5))

    assert result["enabled"] is True
    assert result["dueBooks"] == 1
    assert result["processedBooks"] == 1


# ── Test A: preview must NOT be marked processed ─────────────────────────────


def test_preview_content_is_not_marked_processed_without_candidate(tmp_path):
    """VIP preview (~30 chars) without aligned candidate → must NOT be 'processed'."""
    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:preview_no_cand"
    _insert_book(db_path, book_id)
    ch_id = _insert_chapter(db_path, book_id)

    preview = "少年站在山巅望着远方，他知道这一切才刚刚开始。"  # < 200 chars
    catalog = _FakeCatalog(official_content=preview)
    # No AI service needed — preview without candidate should not reach AI.
    processor = AggregateProcessor(db_path)

    result = asyncio.run(processor._process_chapter(catalog, _chapter_dict(ch_id, book_id)))

    row = _get_chapter_row(db_path, ch_id)
    assert row["status"] != "processed", \
        f"Preview content must NOT be 'processed', got '{row['status']}'"
    assert row["alignment"]["selectedContentSource"] != "official", \
        "Preview-only must not claim selectedContentSource='official'"


# ── Test B: fallback chapter response returns content ────────────────────────


def test_fallback_chapter_response_returns_fallback_content(tmp_path):
    """A chapter with status='fallback' and content_file_path must return file content."""
    from app.services.aggregate_virtual_source import (
        make_aggregate_chapter_url, VIRTUAL_SOURCE_ID,
    )
    from app.source_plugins.id_codec import encode_chapter_id

    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:fallback_resp"
    _insert_book(db_path, book_id)
    ch_id = _insert_chapter(db_path, book_id)

    storage = SharedBookStorage(root=db_path.parent / "library")
    md_path = storage.chapter_markdown_path(
        book_name="测试书", author="作者", chapter_index=1, title="第1章",
    )
    md_path.parent.mkdir(parents=True, exist_ok=True)
    trace_block = storage.render_trace_block({"chapterId": ch_id, "status": "fallback"})
    md_content = storage.render_chapter_markdown(
        title="第1章", body="这是 fallback 正文内容", trace_payload={"chapterId": ch_id, "status": "fallback"},
    )
    md_path.write_text(md_content, encoding="utf-8")

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE aggregate_chapter_tasks SET status='fallback', "
            "content_file_path=? WHERE chapter_id=?",
            (str(md_path), ch_id),
        )
        conn.commit()

    chapter_url = make_aggregate_chapter_url(book_id, f"official_src:ch1", title="第1章", index=1)
    processor = AggregateProcessor(db_path)
    resp = processor.aggregate_chapter_response(chapter_url, chapter_id=ch_id)

    assert resp["content"] == "这是 fallback 正文内容"
    assert resp["debug"]["status"] == "fallback"


# ── Test C: third-party primary not marked as official ───────────────────────


def test_third_party_primary_not_marked_as_official(tmp_path):
    """When primary_source_id is NOT an official source, selectedContentSource ≠ 'official'."""
    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:tp_primary"
    _insert_book(db_path, book_id, primary_source_id="candidate_src")
    ch_id = _insert_chapter(db_path, book_id)

    full_content = "这是一段第三方源的完整正文，超过两百个字的阈值。" * 15
    catalog = _FakeCatalog(official_content=full_content)
    ai_service = _FakeAIService(content="AI 整理后的第三方正文")
    processor = AggregateProcessor(db_path, ai_service=ai_service)

    result = asyncio.run(processor._process_chapter(catalog, _chapter_dict(ch_id, book_id)))

    row = _get_chapter_row(db_path, ch_id)
    assert row["alignment"]["selectedContentSource"] != "official", \
        "Third-party primary must NOT get selectedContentSource='official'"
    assert "official" not in row["alignment"]["selectedContentSource"] or \
           "third_party" in row["alignment"]["selectedContentSource"]


# ── Test D: preview + aligned candidate calls AI service ─────────────────────


def test_is_official_source_returns_false_for_unknown(tmp_path, monkeypatch):
    """Unknown source_id → not official."""
    db_path = _setup_db(tmp_path, ai_enabled=False)
    processor = AggregateProcessor(db_path)
    monkeypatch.setattr(
        "app.source_plugins.loader.PluginLoader.load_all",
        lambda self: {},
    )
    assert processor._is_official_source("nonexistent_src") is False


def test_is_official_source_returns_true_for_official(tmp_path, monkeypatch):
    """Source marked as official → True."""
    db_path = _setup_db(tmp_path, ai_enabled=False)
    processor = AggregateProcessor(db_path)
    load_calls = 0

    class FakePlugin:
        def __init__(self, official: bool):
            self.metadata = type("M", (), {"is_official_source": lambda self: official})()

    def load_plugins(_self):
        nonlocal load_calls
        load_calls += 1
        return {"qidian_com": FakePlugin(True), "example_com": FakePlugin(False)}

    monkeypatch.setattr(
        "app.source_plugins.loader.PluginLoader.load_all",
        load_plugins,
    )
    assert processor._is_official_source("qidian_com") is True
    assert processor._is_official_source("example_com") is False
    assert processor._is_official_source("missing_com") is False
    assert load_calls == 1


# ── TOC cache ────────────────────────────────────────────────────────────────


def test_toc_cache_avoids_repeated_calls(tmp_path):
    """_cached_toc should return cached result on second call without hitting catalog."""
    db_path = _setup_db(tmp_path, ai_enabled=False)
    processor = AggregateProcessor(db_path)

    call_count = 0
    class CountingCatalog:
        async def toc(self, book_id):
            nonlocal call_count
            call_count += 1
            return {"chapters": [{"chapterId": "x:1", "title": "ch1", "index": 1}]}

    catalog = CountingCatalog()
    asyncio.run(processor._cached_toc(catalog, "book_a"))
    asyncio.run(processor._cached_toc(catalog, "book_a"))
    asyncio.run(processor._cached_toc(catalog, "book_b"))

    assert call_count == 2  # book_a once, book_b once


def test_candidate_toc_cache_is_invalidated_for_scheduled_refresh(tmp_path):
    db_path = _setup_db(tmp_path, ai_enabled=False)
    _insert_book(db_path, "book")
    processor = AggregateProcessor(db_path)
    processor._toc_cache["candidate_src:book"] = {"chapters": []}
    processor._toc_cache["another-book"] = {"chapters": []}

    processor.invalidate_candidate_toc_cache("book")

    assert "candidate_src:book" not in processor._toc_cache
    assert "another-book" in processor._toc_cache


# ── previous chapter context ─────────────────────────────────────────────────


def test_ai_output_deviation_error_code():
    from app.ai.client import AIProviderHTTPError
    from app.services.aggregate_processor import classify_error
    # After the shared-subscription refactor, classify_error maps Stage 1 fetch failures.
    # A generic ValueError (including the legacy AI_OUTPUT_DEVIATION message) is no longer
    # a dedicated error code and falls back to S1_SOURCE_FETCH_FAILED.
    assert classify_error(ValueError("AI_OUTPUT_DEVIATION: score 0.3 < threshold 0.9")) == "S1_SOURCE_FETCH_FAILED"


# ── production default AI service ────────────────────────────────────────────


def _patch_ai_provider_config(monkeypatch, config: dict | None):
    """Monkeypatch AggregateSettingsRepository.ai_provider_config for tests."""
    from app.services.aggregate_settings import AggregateSettingsRepository

    def _fake(self):
        from app.services.aggregate_settings import _merge_defaults, DEFAULT_AI_PROVIDER_CONFIG
        if config is None:
            return _merge_defaults(DEFAULT_AI_PROVIDER_CONFIG, {})
        return _merge_defaults(DEFAULT_AI_PROVIDER_CONFIG, config)

    monkeypatch.setattr(AggregateSettingsRepository, "ai_provider_config", _fake)


def test_default_processor_does_not_build_ai_service_while_runtime_disabled(tmp_path, monkeypatch):
    db_path = _setup_db(tmp_path)
    _patch_ai_provider_config(monkeypatch, {
        "baseUrl": "https://api.example.com/v1",
        "apiKey": "sk-test",
        "model": "mimo-v2.5",
    })
    processor = AggregateProcessor(db_path)

    service = processor._get_ai_service()

    assert service is None


def test_candidate_chapter_uses_candidate_toc_url_not_guessed_book_url(tmp_path, monkeypatch):
    """Preview candidate must fetch chapter via toc's chapterId/chapterUrl, not bookUrl/{index}.html."""
    db_path = _setup_db(tmp_path, ai_enabled=False)
    _patch_ai_provider_config(monkeypatch, None)
    book_id = "book:toc_url"
    _insert_book(db_path, book_id, aggregate_payload={
        "name": "测试书", "author": "作者",
        "sources": [
            {"bookId": "official_src:1", "sourceId": "official_src", "score": 100, "bookUrl": "https://official.example/book/1"},
            {"bookId": "candidate_src:1", "sourceId": "candidate_src", "score": 80, "bookUrl": "https://candidate.example/book/1"},
        ],
    })
    ch_id = _insert_chapter(db_path, book_id, index=3)

    preview = "少年站在山巅，望着远方的云海。" * 10
    real_candidate_url = "https://candidate.example/read/3.html"
    real_candidate_id = encode_chapter_id("candidate_src", real_candidate_url)
    candidate = ("【小说网】" + preview + "后续正文内容扩充了很多。" * 30)

    class TOCOnlyCatalog(_FakeCatalog):
        async def toc(self, book_id: str) -> dict:
            source_id = book_id.split(":")[0]
            if source_id == "candidate_src":
                return {"chapters": [
                    {"chapterId": encode_chapter_id("candidate_src", "https://candidate.example/read/1.html"), "title": "第1章", "index": 1},
                    {"chapterId": encode_chapter_id("candidate_src", "https://candidate.example/read/2.html"), "title": "第2章", "index": 2},
                    {"chapterId": real_candidate_id, "title": "第3章", "index": 3, "chapterUrl": real_candidate_url},
                ]}
            return await super().toc(book_id)

        async def chapter(self, chapter_id: str) -> dict:
            if chapter_id == real_candidate_id:
                return {"content": candidate, "title": "第3章"}
            return await super().chapter(chapter_id)

    catalog = TOCOnlyCatalog(official_content=preview, candidate_content=candidate,
                             official_extra={"previewOnly": True})
    processor = AggregateProcessor(db_path)
    monkeypatch.setattr(processor, "_is_official_source", lambda sid: sid == "official_src")

    result = asyncio.run(processor._process_chapter(catalog, _chapter_dict(ch_id, book_id, index=3)))

    row = _get_chapter_row(db_path, ch_id)
    assert row["status"] == "fallback"


def test_candidate_toc_search_uses_index_window(tmp_path, monkeypatch):
    """For target index=N, only N-2..N+2 window chapters should be fetched and aligned."""
    db_path = _setup_db(tmp_path, ai_enabled=False)
    _patch_ai_provider_config(monkeypatch, None)
    book_id = "book:window"
    _insert_book(db_path, book_id, aggregate_payload={
        "name": "测试书", "author": "作者",
        "sources": [
            {"bookId": "official_src:1", "sourceId": "official_src", "score": 100},
            {"bookId": "candidate_src:1", "sourceId": "candidate_src", "score": 80},
        ],
    })
    ch_id = _insert_chapter(db_path, book_id, index=5)

    preview = "少年站在山巅，望着远方的云海。" * 10

    fetched_chapter_ids: list[str] = []

    class WindowCatalog(_FakeCatalog):
        async def toc(self, book_id: str) -> dict:
            source_id = book_id.split(":")[0]
            if source_id == "candidate_src":
                return {"chapters": [
                    {"chapterId": encode_chapter_id("candidate_src", f"https://c.example/{i}.html"), "title": f"第{i}章", "index": i}
                    for i in range(1, 21)
                ]}
            return await super().toc(book_id)

        async def chapter(self, chapter_id: str) -> dict:
            # Only intercept candidate chapters; let official fall through to _FakeCatalog.
            if not chapter_id.startswith("candidate_src"):
                return await super().chapter(chapter_id)
            _, url = decode_chapter_id(chapter_id)
            fetched_chapter_ids.append(chapter_id)
            # Make chapter 5 align, others mismatch.
            if "5.html" in url:
                return {"content": "【小说网】" + preview + "后续正文内容扩充了很多。" * 30, "title": "第5章"}
            return {"content": "错误的章节内容，与预览完全不同。" * 50, "title": "其他章"}

    catalog = WindowCatalog(official_content=preview, official_extra={"previewOnly": True})
    processor = AggregateProcessor(db_path)
    monkeypatch.setattr(processor, "_is_official_source", lambda sid: sid == "official_src")

    result = asyncio.run(processor._snapshot_one_candidate_source(
        catalog=catalog,
        aggregate_book_id=book_id,
        source={"sourceId": "candidate_src", "bookId": "candidate_src:1"},
        official_chapters=[{"index": 5, "title": "第5章"}],
    ))

    decoded_urls = [decode_chapter_id(cid)[1] for cid in fetched_chapter_ids]
    assert result["captured"] == 1
    # Only window chapters 3..7 should be fetched (target index 5 -> 3,4,5,6,7).
    assert len(fetched_chapter_ids) <= 5, f"Expected <=5 fetches, got {len(fetched_chapter_ids)}: {fetched_chapter_ids}"
    assert any("5.html" in url for url in decoded_urls)
    assert not any(f"{i}.html" in url for url in decoded_urls for i in [1, 2, 8, 9, 10, 20])


def test_stage2_stops_after_three_valid_sources_reach_unanimous_consensus(tmp_path, monkeypatch):
    """Stage 2 should not fetch a fourth source after three validated bodies agree."""
    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:stage2_first_source"
    _insert_book(db_path, book_id, aggregate_payload={
        "name": "测试书", "author": "作者",
        "sources": [
            {"bookId": "official_src:1", "sourceId": "official_src", "score": 100, "bookUrl": "https://official.example/book/1"},
            {"bookId": "candidate_a:1", "sourceId": "candidate_a", "score": 95, "bookUrl": "https://candidate-a.example/book/1"},
            {"bookId": "candidate_b:1", "sourceId": "candidate_b", "score": 80, "bookUrl": "https://candidate-b.example/book/1"},
            {"bookId": "candidate_c:1", "sourceId": "candidate_c", "score": 70, "bookUrl": "https://candidate-c.example/book/1"},
            {"bookId": "candidate_d:1", "sourceId": "candidate_d", "score": 60, "bookUrl": "https://candidate-d.example/book/1"},
            {"bookId": "candidate_e:1", "sourceId": "candidate_e", "score": 50, "bookUrl": "https://candidate-e.example/book/1"},
        ],
    })
    ch_id = _insert_chapter(db_path, book_id, index=1)

    preview = "少年站在山巅，望着远方的云海，心中涌起一股莫名的悸动。他知道这一切才刚刚开始，未来路还很长。"
    shared_content = (
        "少年站在山巅，望着远方的云海，心中涌起一股莫名的悸动。"
        "\n\n他知道这一切才刚刚开始，未来路还很长。后续正文内容扩充了很多。" * 10
    ) + "\n\n他打开百度搜索，查询起点评论。"
    selected_content = shared_content.replace(
        "他知道这一切才刚刚开始",
        "GOOGLE搜索TWKAN\n\n他知道这一切才刚刚开始",
        1,
    )
    fetched_candidate_ids: list[str] = []

    class MultiSourceCatalog(_FakeCatalog):
        async def toc(self, book_id: str) -> dict:
            source_id = book_id.split(":")[0]
            return {"chapters": [
                {"chapterId": encode_chapter_id(source_id, f"https://{source_id}.example/ch1.html"), "title": "第1章", "index": 1}
            ]}

        async def chapter(self, chapter_id: str) -> dict:
            if chapter_id.startswith("official_src"):
                return {"content": preview, "title": "第1章", "extra": {"previewOnly": True}}
            fetched_candidate_ids.append(chapter_id)
            if chapter_id.startswith("candidate_a"):
                return {"content": selected_content, "title": "第1章"}
            if chapter_id.startswith(("candidate_b", "candidate_c")):
                return {"content": shared_content, "title": "第1章"}
            if chapter_id.startswith(("candidate_d", "candidate_e")):
                raise AssertionError("extra candidate should not be fetched")
            return await super().chapter(chapter_id)

    processor = AggregateProcessor(db_path)
    monkeypatch.setattr(processor, "_is_official_source", lambda sid: sid == "official_src")
    monkeypatch.setattr(
        "app.services.aggregate_alignment.cross_source_content_similarity",
        lambda left, right: 0.95,
    )

    processor._save_source_snapshot(
        aggregate_book_id=book_id, chapter_index=1, source_id="official_src",
        source_book_id="official_src:1", source_chapter_id="official_src:ch1",
        title="第1章", raw_content=preview, classification="preview",
    )
    candidate = asyncio.run(processor._try_candidate_content(
        MultiSourceCatalog(), _chapter_dict(ch_id, book_id),
        processor._load_aggregate_payload(book_id), "official_src",
    ))

    assert candidate is not None
    assert candidate["source_id"] == "candidate_b"
    assert any(chapter_id.startswith("candidate_a") for chapter_id in fetched_candidate_ids)
    assert any(chapter_id.startswith("candidate_b") for chapter_id in fetched_candidate_ids)
    assert any(chapter_id.startswith("candidate_c") for chapter_id in fetched_candidate_ids)
    assert not any(chapter_id.startswith("candidate_d") for chapter_id in fetched_candidate_ids)
    assert not any(chapter_id.startswith("candidate_e") for chapter_id in fetched_candidate_ids)
    assert candidate["alignment_json"]["crossSourceConsensusMode"] == "three_source"
    assert candidate["alignment_json"]["crossSourceAcceptedCount"] == 3
    assert candidate["alignment_json"]["crossSourceExpanded"] is False
    assert candidate["alignment_json"]["lineConsensus"]["removedCount"] == 0


def test_stage2_expands_after_initial_three_sources_disagree(tmp_path, monkeypatch):
    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:stage2_expand_sources"
    sources = [
        {"bookId": "official_src:1", "sourceId": "official_src", "score": 100},
        *[
            {"bookId": f"candidate_{name}:1", "sourceId": f"candidate_{name}", "score": score}
            for name, score in zip(("a", "b", "c", "d", "e"), (95, 90, 85, 80, 75))
        ],
    ]
    _insert_book(db_path, book_id, aggregate_payload={
        "name": "测试书", "author": "作者", "sources": sources,
    })
    ch_id = _insert_chapter(db_path, book_id, index=1)
    preview = "少年站在山巅，望着远方的云海，心中涌起一股莫名的悸动。他知道这一切才刚刚开始，未来路还很长。"
    majority = preview + "后续正文沿着山路展开，众人继续向前。" * 30
    divergent = preview + "另一条完全不同的叙事线从城中展开。" * 30
    fetched_candidate_ids: list[str] = []

    class ExpandingCatalog(_FakeCatalog):
        async def toc(self, book_id: str) -> dict:
            source_id = book_id.split(":")[0]
            return {"chapters": [{
                "chapterId": encode_chapter_id(source_id, f"https://{source_id}.example/ch1.html"),
                "title": "第1章",
                "index": 1,
            }]}

        async def chapter(self, chapter_id: str) -> dict:
            if chapter_id.startswith("official_src"):
                return {"content": preview, "title": "第1章", "extra": {"previewOnly": True}}
            fetched_candidate_ids.append(chapter_id)
            if chapter_id.startswith(("candidate_a", "candidate_b", "candidate_d")):
                return {"content": majority, "title": "第1章"}
            if chapter_id.startswith("candidate_c"):
                return {"content": divergent, "title": "第1章"}
            if chapter_id.startswith("candidate_e"):
                raise AssertionError("fifth candidate should not be fetched after a 3/4 majority")
            return await super().chapter(chapter_id)

    processor = AggregateProcessor(db_path)
    monkeypatch.setattr(processor, "_is_official_source", lambda sid: sid == "official_src")

    processor._save_source_snapshot(
        aggregate_book_id=book_id, chapter_index=1, source_id="official_src",
        source_book_id="official_src:1", source_chapter_id="official_src:ch1",
        title="第1章", raw_content=preview, classification="preview",
    )
    candidate = asyncio.run(processor._try_candidate_content(
        ExpandingCatalog(), _chapter_dict(ch_id, book_id),
        processor._load_aggregate_payload(book_id), "official_src",
    ))

    assert candidate is not None
    assert candidate["source_id"] == "candidate_a"
    assert any(chapter_id.startswith("candidate_d") for chapter_id in fetched_candidate_ids)
    assert not any(chapter_id.startswith("candidate_e") for chapter_id in fetched_candidate_ids)
    assert candidate["alignment_json"]["crossSourceConsensusMode"] == "expanded"
    assert candidate["alignment_json"]["crossSourceAcceptedCount"] == 4
    assert candidate["alignment_json"]["crossSourceExpanded"] is True


def test_stage2_keeps_preview_when_expanded_sources_end_in_tie(tmp_path, monkeypatch):
    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:stage2_expanded_tie"
    _insert_book(db_path, book_id, aggregate_payload={
        "name": "测试书",
        "author": "作者",
        "sources": [
            {"bookId": "official_src:1", "sourceId": "official_src", "score": 100},
            *[
                {"bookId": f"candidate_{name}:1", "sourceId": f"candidate_{name}", "score": score}
                for name, score in zip(("a", "b", "c", "d"), (95, 90, 85, 80))
            ],
        ],
    })
    ch_id = _insert_chapter(db_path, book_id, index=1)
    preview = "少年站在山巅，望着远方的云海，心中涌起一股莫名的悸动。他知道这一切才刚刚开始，未来路还很长。"
    branch_a = preview + "众人沿着山路继续向前，直到晨光照亮群峰。" * 30
    branch_b = preview + "城中骤然响起钟声，另一队人转身奔向北门。" * 30

    class TiedCatalog(_FakeCatalog):
        async def toc(self, book_id: str) -> dict:
            source_id = book_id.split(":")[0]
            return {"chapters": [{
                "chapterId": encode_chapter_id(source_id, f"https://{source_id}.example/ch1.html"),
                "title": "第1章",
                "index": 1,
            }]}

        async def chapter(self, chapter_id: str) -> dict:
            if chapter_id.startswith("official_src"):
                return {"content": preview, "title": "第1章", "extra": {"previewOnly": True}}
            if chapter_id.startswith(("candidate_a", "candidate_b")):
                return {"content": branch_a, "title": "第1章"}
            if chapter_id.startswith(("candidate_c", "candidate_d")):
                return {"content": branch_b, "title": "第1章"}
            return await super().chapter(chapter_id)

    processor = AggregateProcessor(db_path)
    monkeypatch.setattr(processor, "_is_official_source", lambda sid: sid == "official_src")

    asyncio.run(processor._process_chapter(TiedCatalog(), _chapter_dict(ch_id, book_id)))

    row = _get_chapter_row(db_path, ch_id)
    assert row["previewOnly"] is True
    assert row["fallbackSourceId"] == "official_src"
    assert row["alignment"]["candidateSourceId"] == ""
    assert row["content"] == preview


def test_stage2_does_not_publish_unvalidated_snapshot_after_fetch_error(tmp_path, monkeypatch):
    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:stage2_snapshot_bypass"
    _insert_book(db_path, book_id, aggregate_payload={
        "name": "测试书",
        "author": "作者",
        "sources": [
            {"bookId": "official_src:1", "sourceId": "official_src", "score": 100},
            {"bookId": "candidate_src:1", "sourceId": "candidate_src", "score": 90},
        ],
    })
    ch_id = _insert_chapter(db_path, book_id, index=1)
    snapshot = "这份快照没有经过本次单源校验，因此不能直接发布。" * 30

    class FailingCatalog(_FakeCatalog):
        async def chapter(self, chapter_id: str) -> dict:
            if chapter_id.startswith("official_src"):
                return {"content": "", "title": "第1章"}
            raise RuntimeError("candidate fetch failed")

    processor = AggregateProcessor(db_path)
    monkeypatch.setattr(processor, "_is_official_source", lambda sid: sid == "official_src")
    processor._save_source_snapshot(
        aggregate_book_id=book_id,
        chapter_index=1,
        source_id="candidate_src",
        source_book_id="candidate_src:1",
        source_chapter_id="candidate_src:ch1",
        title="第1章",
        clean_content=snapshot,
        classification="unknown",
    )

    asyncio.run(processor._process_chapter(FailingCatalog(), _chapter_dict(ch_id, book_id)))

    row = _get_chapter_row(db_path, ch_id)
    assert row["content"] == ""
    assert row["fallbackSourceId"] != "candidate_src"


def test_candidate_snapshot_retains_raw_clean_audit_and_resume_state(tmp_path, monkeypatch):
    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:snapshot_audit"
    _insert_book(db_path, book_id)

    class CountingCatalog(_FakeCatalog):
        calls = 0

        async def chapter(self, chapter_id: str) -> dict:
            type(self).calls += 1
            return {
                "content": "第1章\n\n" + ("这是可保留的正文内容。" * 20) + "\n\n请收藏本站。",
                "title": "第1章",
            }

    processor = AggregateProcessor(db_path)
    monkeypatch.setattr(processor, "_is_official_source", lambda sid: sid == "official_src")
    catalog = CountingCatalog()
    source = {"sourceId": "candidate_src", "bookId": "candidate_src:1"}
    official_chapters = [{"index": 1, "title": "第1章"}]

    first = asyncio.run(processor._snapshot_one_candidate_source(
        catalog=catalog,
        aggregate_book_id=book_id,
        source=source,
        official_chapters=official_chapters,
    ))
    second = asyncio.run(processor._snapshot_one_candidate_source(
        catalog=catalog,
        aggregate_book_id=book_id,
        source=source,
        official_chapters=official_chapters,
    ))

    assert first == {"sourceId": "candidate_src", "success": True, "captured": 1, "failed": 0, "matched": 1}
    assert second["captured"] == 0
    assert CountingCatalog.calls == 1
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """SELECT raw_content, clean_content, purify_audit_json
               FROM aggregate_source_snapshots
               WHERE aggregate_book_id = ? AND chapter_index = 1 AND source_id = 'candidate_src'""",
            (book_id,),
        ).fetchone()
        run = conn.execute(
            """SELECT status, fetched_chapters, failed_chapters
               FROM aggregate_source_snapshot_runs
               WHERE aggregate_book_id = ? AND source_id = 'candidate_src'""",
            (book_id,),
        ).fetchone()
    assert row is not None
    assert "请收藏本站" in row[0]
    assert "第1章" not in row[1]
    assert "请收藏本站" not in row[1]
    assert json.loads(row[2])
    assert run == ("complete", 1, 0)
    snapshot_file = tmp_path / "library" / "测试书_作者" / "sources" / "candidate_src" / "chapters" / "000001.json"
    assert json.loads(snapshot_file.read_text(encoding="utf-8"))["rawContent"] == row[0]


def test_candidate_snapshot_reports_live_progress_and_sanitized_summary(tmp_path, monkeypatch):
    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:snapshot_progress"
    _insert_book(db_path, book_id, aggregate_payload={
        "name": "测试书",
        "author": "作者",
        "sources": [
            {"bookId": "official_src:1", "sourceId": "official_src", "sourceName": "官方源"},
            {"bookId": "candidate_src:1", "sourceId": "candidate_src", "sourceName": "候补书源"},
        ],
    })

    class ProgressCatalog(_FakeCatalog):
        async def toc(self, book_id: str) -> dict:
            return {"chapters": [
                {"chapterId": f"candidate_src:ch{i}", "title": f"第{i}章", "index": i}
                for i in range(1, 13)
            ]}

        async def chapter(self, chapter_id: str) -> dict:
            return {"content": f"{chapter_id} 正文内容。" * 20, "title": chapter_id}

    processor = AggregateProcessor(db_path)
    monkeypatch.setattr(processor, "_is_official_source", lambda sid: sid == "official_src")
    events: list[str] = []
    original_log = processor._log_chapter_step

    def capture_log(**kwargs):
        events.append(str(kwargs.get("event", "")))
        original_log(**kwargs)

    monkeypatch.setattr(processor, "_log_chapter_step", capture_log)
    result = asyncio.run(processor._snapshot_one_candidate_source(
        catalog=ProgressCatalog(),
        aggregate_book_id=book_id,
        source={"sourceId": "candidate_src", "sourceName": "候补书源", "bookId": "candidate_src:1"},
        official_chapters=[{"index": i, "title": f"第{i}章"} for i in range(1, 13)],
    ))

    assert result["captured"] == 12
    assert "source_snapshot_start" in events
    assert "source_snapshot_toc_complete" in events
    assert events.count("source_snapshot_progress") == 2
    assert events[-1] == "source_snapshot_complete"

    summary = LibraryBooksService(db_path).source_snapshot_progress(book_id)
    assert summary is not None
    assert summary["status"] == "complete"
    assert summary["percent"] == 100
    assert summary["sources"][0]["sourceName"] == "候补书源"
    assert summary["sources"][0]["fetchedChapters"] == 12
    assert "sourceBookId" not in summary["sources"][0]
    assert not any("Url" in key for key in summary["sources"][0])

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """UPDATE aggregate_source_snapshot_runs
               SET status = 'error', failed_chapters = 1
               WHERE aggregate_book_id = ? AND source_id = 'candidate_src'""",
            (book_id,),
        )
        conn.commit()
    ended_with_error = LibraryBooksService(db_path).source_snapshot_progress(book_id)
    assert ended_with_error is not None
    assert ended_with_error["status"] == "error"
    assert ended_with_error["percent"] == 100

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """INSERT INTO aggregate_source_snapshot_runs
               (aggregate_book_id, source_id, source_book_id, status,
                total_chapters, fetched_chapters, updated_at)
               VALUES (?, 'candidate_src', 'candidate_src:obsolete', 'partial',
                       100, 1, '2000-01-01T00:00:00+00:00')""",
            (book_id,),
        )
        conn.commit()
    latest_only = LibraryBooksService(db_path).source_snapshot_progress(book_id)
    assert latest_only is not None
    assert latest_only["sourceCount"] == 1
    assert latest_only["sources"][0]["totalChapters"] == 12


def test_snapshot_progress_reports_partial_completion_ratio(tmp_path):
    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:partial_snapshot_progress"
    _insert_book(db_path, book_id)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """INSERT INTO aggregate_source_snapshot_runs
               (aggregate_book_id, source_id, source_book_id, status,
                total_chapters, fetched_chapters, updated_at)
               VALUES (?, 'candidate_src', 'candidate_src:1', 'partial',
                       10, 2, datetime('now'))""",
            (book_id,),
        )
        conn.commit()

    progress = LibraryBooksService(db_path).source_snapshot_progress(book_id)

    assert progress is not None
    assert progress["status"] == "partial"
    assert progress["percent"] == 20


def test_run_book_task_timeout_closes_running_snapshots(tmp_path, monkeypatch):
    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:task_timeout"
    _insert_book(db_path, book_id)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """INSERT INTO aggregate_source_snapshot_runs
               (aggregate_book_id, source_id, source_book_id, status, updated_at)
               VALUES (?, 'candidate_src', 'candidate_src:1', 'running', datetime('now'))""",
            (book_id,),
        )
        conn.commit()

    processor = AggregateProcessor(db_path)
    monkeypatch.setattr(processor, "_job_timeout_seconds", lambda: 0.01)

    async def block_forever(*args, **kwargs):
        await asyncio.Event().wait()

    monkeypatch.setattr(processor, "_run_book_task", block_forever)
    with pytest.raises(TimeoutError, match="已中断并等待重试"):
        asyncio.run(processor.run_book_task(book_id))

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """SELECT status, last_error FROM aggregate_source_snapshot_runs
               WHERE aggregate_book_id = ? AND source_id = 'candidate_src'""",
            (book_id,),
        ).fetchone()
    assert row == ("partial", "job_timeout")


@pytest.mark.asyncio
async def test_run_book_task_scales_timeout_with_chapter_batch(tmp_path, monkeypatch):
    processor = AggregateProcessor(_setup_db(tmp_path, ai_enabled=False))
    monkeypatch.setattr(processor, "_job_timeout_seconds", lambda: 0.05)

    async def finish_within_scaled_budget(*_args, **_kwargs):
        await asyncio.sleep(0.06)
        return {"success": True}

    monkeypatch.setattr(processor, "_run_book_task", finish_within_scaled_budget)

    assert await processor.run_book_task("book", chapter_limit=10) == {"success": True}


@pytest.mark.asyncio
async def test_source_slots_apply_per_book_and_conservative_limits(tmp_path):
    processor = AggregateProcessor(tmp_path / "test.db")

    async def peak_for(work: list[tuple[str, str]]) -> int:
        active = 0
        peak = 0

        async def run(book_id: str, source_id: str) -> None:
            nonlocal active, peak
            async with processor._source_slot(
                aggregate_book_id=book_id,
                source_id=source_id,
            ):
                active += 1
                peak = max(peak, active)
                await asyncio.sleep(0.01)
                active -= 1

        await asyncio.gather(*(run(book_id, source_id) for book_id, source_id in work))
        return peak

    assert await peak_for([("book", "source_a")] * 4) == 3
    assert await peak_for([("book", "source_a"), ("book", "source_b")]) == 2

    processor._conservative_source_ids = {"limited_source"}
    assert await peak_for([("book", "limited_source")] * 3) == 1

    processor._browser_source_ids = {"browser_a", "browser_b", "browser_c", "browser_d"}
    processor._conservative_source_ids.update(processor._browser_source_ids)
    assert await peak_for([
        ("book_a", "browser_a"),
        ("book_b", "browser_b"),
        ("book_c", "browser_c"),
        ("book_d", "browser_d"),
    ]) == 3


def test_candidate_toc_matching_prefers_title_over_wrong_index(tmp_path):
    processor = AggregateProcessor(tmp_path / "test.db")
    matches = processor._match_candidate_toc_entries(
        cand_chapters=[
            {"index": 363, "title": "第六百六十九章 工作不够 兼职来凑"},
            {"index": 350, "title": "第356章 小心没用，白天也滑"},
            {"index": 362, "title": "第361章 道左相逢"},
        ],
        target_index=363,
        target_title="第三百五十四章 小心没用，白天也滑",
    )

    assert [item["title"] for item in matches] == ["第356章 小心没用，白天也滑"]


@pytest.mark.parametrize(
    ("target_index", "candidate_index"),
    [(245, 244), (502, 501), (678, 677)],
)
def test_candidate_toc_matches_renumbered_volume_opening(
    tmp_path, target_index, candidate_index,
):
    processor = AggregateProcessor(tmp_path / "test.db")
    chapters = [
        {"index": 1, "title": "第1章 卷首", "chapterId": "first-volume"},
        {"index": candidate_index, "title": f"第{candidate_index}章 卷首", "chapterId": "right-volume"},
        {"index": target_index, "title": f"第{target_index}章 另一件事", "chapterId": "wrong-name"},
    ]

    matches = processor._match_candidate_toc_entries(
        cand_chapters=chapters,
        target_index=target_index,
        target_title="第1章 卷首",
    )

    assert matches[0]["chapterId"] == "right-volume"
    assert processor._match_snapshot_candidate_chapter(
        source_chapters=chapters,
        chapter_index=target_index,
        title="第1章 卷首",
    )["chapterId"] == "right-volume"


def test_candidate_toc_rejects_distant_duplicate_without_nearby_match(tmp_path):
    processor = AggregateProcessor(tmp_path / "test.db")

    assert processor._match_candidate_toc_entries(
        cand_chapters=[{"index": 1, "title": "第1章 卷首"}],
        target_index=245,
        target_title="第1章 卷首",
    ) == []


def test_candidate_alignment_accepts_same_name_after_volume_renumbering():
    preview = "天地玄黄宇宙洪荒" * 25
    content = preview[:150] + "山" * 50 + "后续正文" * 60

    result = align_candidate_chapter(
        official_preview=preview,
        candidate_title="第244章 卷首",
        candidate_content=content,
        expected_title="第1章 卷首",
    )

    assert result["alignmentPassed"] is True
    assert result["titleSimilarity"] == 1.0
    assert result["alignmentReason"] == "title_and_preview_matched"
    assert result["previewSimilarity"] < 0.88


def test_candidate_snapshot_rejects_mismatched_saved_title(tmp_path):
    db_path = _setup_db(tmp_path)
    processor = AggregateProcessor(db_path)
    processor._save_source_snapshot(
        aggregate_book_id="book-1",
        chapter_index=363,
        source_id="candidate_src",
        source_book_id="candidate-book",
        source_chapter_id="candidate-chapter",
        title="第六百六十九章 工作不够 兼职来凑",
        raw_content="错误候选正文。" * 40,
    )

    assert processor._load_source_snapshot_content(
        aggregate_book_id="book-1",
        chapter_index=363,
        source_id="candidate_src",
        expected_title="第三百五十四章 小心没用，白天也滑",
    ) == ""


@pytest.mark.asyncio
async def test_initial_prefetch_is_bounded_and_timeout_does_not_block_processing(tmp_path, monkeypatch):
    import app.services.aggregate_processor as processor_module

    processor = AggregateProcessor(tmp_path / "test.db")
    monkeypatch.setattr(processor, "_published_chapter_count", lambda _book_id: 0)
    monkeypatch.setattr(
        processor,
        "_library_books",
        lambda: SimpleNamespace(
            source_map_refresh_state=lambda _book_id: {
                "completed": True,
                "lastVerifiedAt": datetime.now().astimezone().isoformat(),
            }
        ),
    )
    captured: dict[str, Any] = {}

    async def capture_prefetch(**kwargs):
        captured.update(kwargs)
        return {"sourceCount": 2, "captured": 40, "failed": 0}

    monkeypatch.setattr(processor, "_snapshot_candidate_sources", capture_prefetch)
    result = await processor._run_initial_candidate_prefetch(
        catalog=object(),
        aggregate_book_id="book",
        payload={},
        official_chapters=[{"index": index, "title": f"第{index}章"} for index in range(1, 31)],
        chapters_to_process=[{"chapterIndex": 3}],
    )

    assert result["mode"] == "initial_prefetch"
    assert [item["index"] for item in captured["official_chapters"]] == list(range(3, 23))
    assert captured["historical_complete"] is False

    interrupted: list[tuple[str, str]] = []

    async def block_prefetch(**_kwargs):
        await asyncio.Event().wait()

    monkeypatch.setattr(processor_module, "INITIAL_PREFETCH_GRACE_SECONDS", 0.01)
    monkeypatch.setattr(processor, "_snapshot_candidate_sources", block_prefetch)
    monkeypatch.setattr(
        processor,
        "mark_running_snapshots_interrupted",
        lambda book_id, reason: interrupted.append((book_id, reason)),
    )
    timed_out = await processor._run_initial_candidate_prefetch(
        catalog=object(),
        aggregate_book_id="book",
        payload={},
        official_chapters=[{"index": 1, "title": "第1章"}],
        chapters_to_process=[{"chapterIndex": 1}],
    )

    assert timed_out["mode"] == "initial_prefetch_timeout"
    assert interrupted == [("book", "prefetch_grace_expired")]


def test_snapshot_progress_distinguishes_toc_loading_from_chapter_download(tmp_path):
    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:toc_loading"
    _insert_book(db_path, book_id)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """INSERT INTO aggregate_source_snapshot_runs
               (aggregate_book_id, source_id, source_book_id, status,
                total_chapters, fetched_chapters, started_at, updated_at)
               VALUES (?, 'candidate_src', 'candidate_src:1', 'loading_toc',
                       1700, 600, datetime('now'), datetime('now'))""",
            (book_id,),
        )
        conn.commit()

    progress = LibraryBooksService(db_path).source_snapshot_progress(book_id)

    assert progress is not None
    assert progress["status"] == "running"
    assert progress["runningSourceCount"] == 1
    assert progress["sources"][0]["status"] == "loading_toc"
    assert progress["sources"][0]["percent"] == 0


def test_partial_snapshot_run_defers_history_without_downgrading_completed_source(tmp_path, monkeypatch):
    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:snapshot_incremental"
    _insert_book(db_path, book_id)

    class TwoChapterCatalog(_FakeCatalog):
        async def toc(self, book_id: str) -> dict:
            return {"chapters": [
                {"chapterId": "candidate_src:ch1", "title": "第1章", "index": 1},
                {"chapterId": "candidate_src:ch2", "title": "第2章", "index": 2},
            ]}

        async def chapter(self, chapter_id: str) -> dict:
            return {"content": f"{chapter_id} 正文内容。" * 20, "title": chapter_id}

    processor = AggregateProcessor(db_path)
    monkeypatch.setattr(processor, "_is_official_source", lambda sid: sid == "official_src")
    source = {"sourceId": "candidate_src", "bookId": "candidate_src:1"}
    catalog = TwoChapterCatalog()

    partial = asyncio.run(processor._snapshot_one_candidate_source(
        catalog=catalog,
        aggregate_book_id=book_id,
        source=source,
        official_chapters=[{"index": 2, "title": "第2章"}],
        historical_complete=False,
    ))
    with sqlite3.connect(db_path) as conn:
        partial_status = conn.execute(
            """
            SELECT status, fetched_chapters FROM aggregate_source_snapshot_runs
            WHERE aggregate_book_id = ? AND source_id = 'candidate_src'
            """,
            (book_id,),
        ).fetchone()
    complete = asyncio.run(processor._snapshot_one_candidate_source(
        catalog=catalog,
        aggregate_book_id=book_id,
        source=source,
        official_chapters=[{"index": 1, "title": "第1章"}, {"index": 2, "title": "第2章"}],
    ))
    incremental = asyncio.run(processor._snapshot_one_candidate_source(
        catalog=catalog,
        aggregate_book_id=book_id,
        source=source,
        official_chapters=[{"index": 2, "title": "第2章"}],
        historical_complete=False,
    ))

    assert partial["success"] is True
    assert partial_status == ("partial", 1)
    assert complete["success"] is True
    assert incremental["success"] is True
    with sqlite3.connect(db_path) as conn:
        status = conn.execute(
            """
            SELECT status, fetched_chapters FROM aggregate_source_snapshot_runs
            WHERE aggregate_book_id = ? AND source_id = 'candidate_src'
            """,
            (book_id,),
        ).fetchone()
    assert status == ("complete", 2)


def test_local_rebuild_uses_snapshots_without_catalog_access(tmp_path, monkeypatch):
    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:local_rebuild"
    _insert_book(db_path, book_id)
    chapter_id = _insert_chapter(db_path, book_id)
    processor = AggregateProcessor(db_path)
    monkeypatch.setattr(processor, "_is_official_source", lambda sid: sid == "official_src")
    raw = "第1章\n\n" + ("这是保存在本地快照中的正文。" * 15) + "\n\n请收藏本站。"
    processor._save_source_snapshot(
        aggregate_book_id=book_id,
        chapter_index=1,
        source_id="official_src",
        source_book_id="official_src:1",
        source_chapter_id="official_src:ch1",
        title="第1章",
        raw_content=raw,
        classification="full",
    )
    processor._write_chapter_result(
        chapter_id=chapter_id,
        aggregate_book_id=book_id,
        title="第1章",
        chapter_index=1,
        status="processed",
        content=processor._load_source_snapshot_content(
            aggregate_book_id=book_id,
            chapter_index=1,
            source_id="official_src",
        ),
        alignment_json={"selectedContentSource": "official", "primarySourceId": "official_src"},
        fallback_source_id="official_src",
    )

    result = processor.rebuild_book_from_snapshots(book_id)

    assert result["networkAccessed"] is False
    assert result["rewrittenChapters"] == 1
    row = _get_chapter_row(db_path, chapter_id)
    assert row["content"] and "请收藏本站" not in row["content"]


def test_local_rebuild_reselects_candidate_from_local_snapshots(tmp_path, monkeypatch):
    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:local_rebuild_candidate"
    _insert_book(db_path, book_id)
    chapter_id = _insert_chapter(db_path, book_id, status="fallback")
    processor = AggregateProcessor(db_path)
    monkeypatch.setattr(processor, "_is_official_source", lambda sid: sid == "official_src")
    processor._save_source_snapshot(
        aggregate_book_id=book_id,
        chapter_index=1,
        source_id="official_src",
        source_book_id="official_src:1",
        source_chapter_id="official_src:ch1",
        title="第1章",
        raw_content="官方预览正文。",
        classification="preview",
    )
    processor._save_source_snapshot(
        aggregate_book_id=book_id,
        chapter_index=1,
        source_id="candidate_src",
        source_book_id="candidate_src:1",
        source_chapter_id="candidate_src:ch1",
        title="第1章",
        raw_content="候补源完整正文。" * 30,
        classification="captured",
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE aggregate_chapter_tasks
            SET preview_only = 1, source_alignment_json = ?, source_word_count = 100
            WHERE chapter_id = ?
            """,
            (json.dumps({"selectedContentSource": "preview_fallback"}), chapter_id),
        )
        conn.commit()

    calls = []

    def select_from_snapshots(**kwargs):
        calls.append(kwargs)
        return {
            "source_id": "candidate_src",
            "content": processor._load_source_snapshot_content(
                aggregate_book_id=book_id, chapter_index=1, source_id="candidate_src"
            ),
            "alignment_json": {
                "selectedContentSource": "candidate",
                "primarySourceId": "official_src",
                "candidateSourceId": "candidate_src",
                "alignmentPassed": True,
            },
        }

    monkeypatch.setattr(processor, "_try_snapshot_candidate_content", select_from_snapshots)

    result = processor.rebuild_book_from_snapshots(book_id)

    assert result["networkAccessed"] is False
    assert calls and calls[0]["official_word_count"] == 100
    row = _get_chapter_row(db_path, chapter_id)
    assert row["status"] == "fallback"
    assert row["previewOnly"] is False
    assert row["fallbackSourceId"] == "candidate_src"
    assert "候补源完整正文" in row["content"]


def test_local_rebuild_downgrades_stale_candidate_to_official_preview(tmp_path, monkeypatch):
    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:local_rebuild_preview"
    _insert_book(db_path, book_id)
    chapter_id = _insert_chapter(db_path, book_id, status="fallback")
    processor = AggregateProcessor(db_path)
    monkeypatch.setattr(processor, "_is_official_source", lambda sid: sid == "official_src")
    processor._save_source_snapshot(
        aggregate_book_id=book_id,
        chapter_index=1,
        source_id="official_src",
        source_book_id="official_src:1",
        source_chapter_id="official_src:ch1",
        title="第1章",
        raw_content="官方预览正文。" * 20,
        classification="preview",
    )
    processor._write_chapter_result(
        chapter_id=chapter_id,
        aggregate_book_id=book_id,
        title="第1章",
        chapter_index=1,
        status="fallback",
        content="旧候补全文。" * 60,
        alignment_json={"selectedContentSource": "candidate", "alignmentPassed": True},
        fallback_source_id="removed_candidate",
    )

    result = processor.rebuild_book_from_snapshots(book_id)

    assert result["invalidatedChapters"] == 0
    row = _get_chapter_row(db_path, chapter_id)
    assert row["previewOnly"] is True
    assert row["fallbackSourceId"] == "official_src"
    assert row["alignment"]["selectedContentSource"] == "preview_fallback"
    assert row["alignment"]["alignmentReason"] == "official_preview_saved"
    assert "官方预览正文" in row["content"]
    assert "旧候补全文" not in row["content"]


def test_local_rebuild_withdraws_unanchored_stale_candidate(tmp_path, monkeypatch):
    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:local_rebuild_unanchored"
    payload = {
        "name": "测试书",
        "author": "作者",
        "primarySourceId": "official_src",
        "sources": [
            {"sourceId": "official_src", "bookId": "official_src:1"},
            {"sourceId": "candidate_src", "bookId": "candidate_src:1"},
        ],
    }
    _insert_book(db_path, book_id, aggregate_payload=payload)
    chapter_id = _insert_chapter(db_path, book_id, status="fallback")
    processor = AggregateProcessor(db_path)
    monkeypatch.setattr(processor, "_is_official_source", lambda sid: sid == "official_src")
    processor._save_source_snapshot(
        aggregate_book_id=book_id,
        chapter_index=1,
        source_id="candidate_src",
        source_book_id="candidate_src:1",
        source_chapter_id="candidate_src:ch1",
        title="第1章",
        raw_content="没有官方锚点的旧候补全文。" * 40,
        classification="captured",
    )
    processor._write_chapter_result(
        chapter_id=chapter_id,
        aggregate_book_id=book_id,
        title="第1章",
        chapter_index=1,
        status="fallback",
        content="没有官方锚点的旧候补全文。" * 40,
        alignment_json={"selectedContentSource": "candidate", "alignmentPassed": False},
        fallback_source_id="candidate_src",
    )

    result = processor.rebuild_book_from_snapshots(book_id)

    assert result["invalidatedChapters"] == 1
    row = _get_chapter_row(db_path, chapter_id)
    assert row["status"] == "error"
    assert row["contentFilePath"] == ""
    assert row["fallbackSourceId"] == ""
    assert row["lastErrorCode"] == "rebuild_missing_official_anchor"
    assert row["alignment"]["selectedContentSource"] == "none"
    chapter_index = json.loads(
        (db_path.parent / "library" / "测试书_作者" / "chapter_index.json").read_text(encoding="utf-8")
    )
    assert chapter_index["chapters"][0]["status"] == "failed"
    assert chapter_index["chapters"][0]["file"] is None


def test_official_snapshot_records_content_classification(tmp_path, monkeypatch):
    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:official_snapshot_classification"
    _insert_book(db_path, book_id)
    chapter_id = _insert_chapter(db_path, book_id)
    processor = AggregateProcessor(db_path)
    monkeypatch.setattr(processor, "_is_official_source", lambda sid: sid == "official_src")

    asyncio.run(processor._process_chapter(
        _FakeCatalog(official_content="官方免费正文。" * 40),
        _chapter_dict(chapter_id, book_id),
    ))

    with sqlite3.connect(db_path) as conn:
        classification = conn.execute(
            """
            SELECT classification FROM aggregate_source_snapshots
            WHERE aggregate_book_id = ? AND chapter_index = 1 AND source_id = 'official_src'
            """,
            (book_id,),
        ).fetchone()
    assert classification == ("full",)


def test_candidate_consensus_rejects_out_of_order_body(tmp_path):
    processor = AggregateProcessor(db_path=tmp_path / "test.db")
    opening = "冬日阴云，寒风阵阵。季觉站在寥落破败的厂区前面，鼓起勇气发问。"
    paragraphs = [
        opening,
        "中年人即答，告诉他欢迎进厂。",
        "而季觉的神情顿时一言难尽起来。",
        "延建带着大家召开了第一次重组会议。",
        "所有人开始讨论生产线的具体问题。",
    ]
    ordered = "\n\n".join(paragraphs * 12)
    disordered = "\n\n".join([
        paragraphs[0], paragraphs[3], paragraphs[1], paragraphs[4], paragraphs[2],
    ] * 12)

    selected, consensus = processor._select_consistent_candidate([
        {"source_id": "scrambled", "content": disordered, "alignment_json": {}},
        {"source_id": "normal_a", "content": ordered, "alignment_json": {}},
        {"source_id": "normal_b", "content": ordered, "alignment_json": {}},
    ])

    assert selected and selected["source_id"] == "normal_a"
    assert next(item for item in consensus if item["sourceId"] == "scrambled")["supportCount"] == 0


def _sentence_order_bodies():
    sentences = [
        f"第{index}道工序完成后，记录员把编号{index}写进当天的生产日志。"
        for index in range(1, 25)
    ]
    ordered = "".join(sentences)
    disordered = "".join(
        sentences[:4] + sentences[12:18] + sentences[4:12] + sentences[18:]
    )
    return sentences, ordered, disordered


def test_sentence_order_consensus_rejects_only_supported_outlier(tmp_path, monkeypatch):
    import app.services.aggregate_alignment as alignment

    _sentences, ordered, disordered = _sentence_order_bodies()
    monkeypatch.setattr(alignment, "cross_source_content_similarity", lambda left, right: 0.99)
    candidates = [
        {"source_id": "scrambled", "content": disordered, "alignment_json": {}},
        {"source_id": "normal_a", "content": ordered, "alignment_json": {}},
        {"source_id": "normal_b", "content": ordered, "alignment_json": {}},
    ]

    processor = AggregateProcessor(db_path=tmp_path / "test.db")
    selected, consensus = processor._select_consistent_candidate(candidates)

    assert selected and selected["source_id"] == "normal_a"
    rejected = next(item for item in consensus if item["sourceId"] == "scrambled")
    assert rejected["sentenceOrderStatus"] == "rejected_order_mismatch"
    assert rejected["sentenceOrder"]["referenceSourceIds"] == ["normal_a", "normal_b"]
    assert candidates[0]["alignment_json"]["sentenceOrder"]["rejected"] is True
    assert processor._has_stable_candidate_consensus(
        consensus, candidate_count=3, require_unanimous=True
    ) is True


def test_sentence_order_consensus_does_not_assign_blame_with_two_sources(tmp_path, monkeypatch):
    import app.services.aggregate_alignment as alignment

    _sentences, ordered, disordered = _sentence_order_bodies()
    monkeypatch.setattr(alignment, "cross_source_content_similarity", lambda left, right: 0.99)
    candidates = [
        {"source_id": "scrambled", "content": disordered, "alignment_json": {}},
        {"source_id": "normal", "content": ordered, "alignment_json": {}},
    ]

    _selected, consensus = AggregateProcessor(
        db_path=tmp_path / "test.db"
    )._select_consistent_candidate(candidates, allow_degraded=True)

    assert all(item["sentenceOrderStatus"] != "rejected_order_mismatch" for item in consensus)
    assert all(
        not candidate["alignment_json"]["sentenceOrder"]["rejected"]
        for candidate in candidates
    )


def test_sentence_order_consensus_tolerates_insertions_traditional_and_line_wraps(tmp_path):
    from app.services.text_convert import to_traditional

    sentences, ordered, _disordered = _sentence_order_bodies()
    inserted = "".join(sentences[:8] + ["本页内容由镜像站点整理后提供。"] + sentences[8:])
    traditional = str(to_traditional(ordered))
    wrapped_traditional = "\n".join(
        traditional[index : index + 17]
        for index in range(0, len(traditional), 17)
    )
    candidates = [
        {"source_id": "inserted", "content": inserted, "alignment_json": {}},
        {"source_id": "clean", "content": ordered, "alignment_json": {}},
        {"source_id": "traditional", "content": wrapped_traditional, "alignment_json": {}},
    ]

    AggregateProcessor(db_path=tmp_path / "test.db")._select_consistent_candidate(candidates)

    assert all(
        not candidate["alignment_json"]["sentenceOrder"]["rejected"]
        for candidate in candidates
    )
    assert candidates[0]["alignment_json"]["sentenceOrder"]["status"] == "order_consistent"


def test_local_snapshot_rebuild_rejects_sentence_order_outlier(tmp_path, monkeypatch):
    sentences, ordered, disordered = _sentence_order_bodies()
    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:sentence_order_rebuild"
    payload = {
        "name": "测试书",
        "author": "作者",
        "primarySourceId": "official_src",
        "sources": [
            {"sourceId": "official_src", "bookId": "official_src:1"},
            {"sourceId": "scrambled", "bookId": "scrambled:1"},
            {"sourceId": "normal_a", "bookId": "normal_a:1"},
            {"sourceId": "normal_b", "bookId": "normal_b:1"},
        ],
    }
    _insert_book(db_path, book_id, aggregate_payload=payload)
    chapter_id = _insert_chapter(db_path, book_id, status="fallback")
    processor = AggregateProcessor(db_path)
    monkeypatch.setattr(processor, "_is_official_source", lambda sid: sid == "official_src")
    for source_id, content, classification in (
        ("official_src", "".join(sentences[:4]), "preview"),
        ("scrambled", disordered, "captured"),
        ("normal_a", ordered, "captured"),
        ("normal_b", ordered, "captured"),
    ):
        processor._save_source_snapshot(
            aggregate_book_id=book_id,
            chapter_index=1,
            source_id=source_id,
            source_book_id=f"{source_id}:1",
            source_chapter_id=f"{source_id}:ch1",
            title="第1章",
            raw_content=content,
            classification=classification,
        )
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE aggregate_chapter_tasks SET preview_only = 1 WHERE chapter_id = ?",
            (chapter_id,),
        )
        conn.commit()

    result = processor.rebuild_book_from_snapshots(book_id)

    assert result["networkAccessed"] is False
    row = _get_chapter_row(db_path, chapter_id)
    assert row["fallbackSourceId"] == "normal_a"
    rejected = next(
        item
        for item in row["alignment"]["crossSourceConsensus"]
        if item["sourceId"] == "scrambled"
    )
    assert rejected["sentenceOrderStatus"] == "rejected_order_mismatch"


def test_candidate_consensus_prefers_fewer_direct_paragraph_gaps(tmp_path, monkeypatch):
    import app.services.aggregate_alignment as alignment

    clean = "第一段这是足够长的正文内容。\n\n第二段这是足够长的正文内容。"
    inserted = "第一段这是足够长的正文内容。\n\n未知来源插入的内容。\n\n第二段这是足够长的正文内容。"

    def similarity(left, right):
        return 0.99 if inserted in (left, right) else 0.95

    monkeypatch.setattr(alignment, "cross_source_content_similarity", similarity)
    selected, consensus = AggregateProcessor(db_path=tmp_path / "test.db")._select_consistent_candidate([
        {"source_id": "inserted", "content": inserted, "alignment_json": {}},
        {"source_id": "clean_a", "content": clean, "alignment_json": {}},
        {"source_id": "clean_b", "content": clean, "alignment_json": {}},
    ])

    assert selected and selected["source_id"] == "clean_a"
    gaps = {item["sourceId"]: item["directConsensusGapCount"] for item in consensus}
    assert gaps["inserted"] > gaps["clean_a"]


def test_line_consensus_removes_anchor_bounded_source_unique_content():
    paragraphs = [
        f"第{index}段真实正文内容足够长，人物继续沿着既定方向行动。"
        for index in range(1, 31)
    ]
    clean = "\n\n".join(paragraphs)
    selected = "\n\n".join([*paragraphs[:15], "甲站随机混淆插入段", *paragraphs[15:]])
    peers = [
        {"source_id": "selected", "content": selected},
        {"source_id": "peer_a", "content": clean},
        {"source_id": "peer_b", "content": clean.replace("继续", "繼續")},
    ]

    cleaned, audit = purify_by_line_consensus(
        selected,
        peers,
        selected_source_id="selected",
        chapter_title="第325章",
    )

    assert cleaned == clean
    assert audit["sourceCount"] == 3
    assert audit["majorityCount"] == 2
    assert audit["removalPolicy"] == "strict_three_source_anchor_bounded_unique"
    assert audit["removedCount"] == 1
    assert audit["removedLines"][0]["content"] == "甲站随机混淆插入段"
    assert audit["removedLines"][0]["anchorSourceIds"] == ["peer_a", "peer_b"]
    assert audit["removalRatio"] <= 0.02


def test_processor_line_consensus_revalidates_and_preserves_source_snapshot(tmp_path, monkeypatch):
    db_path = _setup_db(tmp_path, ai_enabled=False)
    processor = AggregateProcessor(db_path)
    monkeypatch.setattr(processor, "_is_official_source", lambda source_id: source_id == "official_src")
    paragraphs = [
        f"第{index}段真实正文内容足够长，人物继续沿着既定方向行动。"
        for index in range(1, 31)
    ]
    clean = "\n\n".join(paragraphs)
    selected = "\n\n".join([*paragraphs[:15], "甲站随机混淆插入段", *paragraphs[15:]])
    official_preview = "\n\n".join(paragraphs[:3])
    consensus = [{
        "sourceId": "selected",
        "supportingSourceIds": ["peer_a", "peer_b"],
        "supportCount": 2,
    }]

    def candidates() -> list[dict[str, Any]]:
        return [
            {"source_id": "selected", "content": selected, "alignment_json": {}},
            {"source_id": "peer_a", "content": clean, "alignment_json": {}},
            {"source_id": "peer_b", "content": clean, "alignment_json": {}},
        ]

    processor._save_source_snapshot(
        aggregate_book_id="book:consensus",
        chapter_index=1,
        source_id="selected",
        source_book_id="selected:book",
        source_chapter_id="selected:chapter",
        title="第一章",
        raw_content=selected,
        classification="captured",
    )
    accepted = candidates()
    result = processor._apply_line_consensus_purification(
        selected_candidate=accepted[0],
        accepted_candidates=accepted,
        consensus=consensus,
        official_preview=official_preview,
        official_word_count=len(clean.replace("\n", "").replace(" ", "")),
        primary_source_id="official_src",
        chapter_title="第一章",
        aggregate_book_id="book:consensus",
        chapter_index=1,
    )

    assert result["content"] == clean
    audit = result["alignment_json"]["lineConsensus"]
    assert audit["applied"] is True
    assert audit["revalidation"]["passed"] is True
    with sqlite3.connect(db_path) as conn:
        raw_content, clean_content = conn.execute(
            """SELECT raw_content, clean_content FROM aggregate_source_snapshots
               WHERE aggregate_book_id = ? AND chapter_index = ? AND source_id = ?""",
            ("book:consensus", 1, "selected"),
        ).fetchone()
    assert "甲站随机混淆插入段" in raw_content
    assert "甲站随机混淆插入段" in clean_content

    accepted = candidates()
    rollback = processor._apply_line_consensus_purification(
        selected_candidate=accepted[0],
        accepted_candidates=accepted,
        consensus=consensus,
        official_preview=official_preview,
        official_word_count=len(selected.replace("\n", "").replace(" ", "")),
        primary_source_id="official_src",
        chapter_title="第一章",
        aggregate_book_id="book:consensus",
        chapter_index=1,
    )
    rollback_audit = rollback["alignment_json"]["lineConsensus"]
    assert rollback["content"] == selected
    assert rollback_audit["applied"] is False
    assert rollback_audit["rollback"] is True
    assert rollback_audit["reason"] == "official_revalidation_failed"
    assert rollback_audit["proposedRemovedLines"][0]["content"] == "甲站随机混淆插入段"

    preview_selected = "\n\n".join([
        paragraphs[0],
        "甲站随机混淆插入段",
        *paragraphs[1:],
    ])
    preview_candidates = [
        {"source_id": "selected", "content": preview_selected, "alignment_json": {}},
        {"source_id": "peer_a", "content": clean, "alignment_json": {}},
        {"source_id": "peer_b", "content": clean, "alignment_json": {}},
    ]
    overlap = processor._apply_line_consensus_purification(
        selected_candidate=preview_candidates[0],
        accepted_candidates=preview_candidates,
        consensus=consensus,
        official_preview="\n\n".join(preview_selected.split("\n\n")[:4]),
        official_word_count=len(clean.replace("\n", "").replace(" ", "")),
        primary_source_id="official_src",
        chapter_title="第一章",
        aggregate_book_id="book:consensus",
        chapter_index=1,
    )
    overlap_audit = overlap["alignment_json"]["lineConsensus"]
    assert overlap["content"] == preview_selected
    assert overlap_audit["rollback"] is True
    assert overlap_audit["revalidation"]["officialPreviewOverlap"] is True

    accepted = candidates()
    untrusted = processor._apply_line_consensus_purification(
        selected_candidate=accepted[0],
        accepted_candidates=accepted,
        consensus=consensus,
        official_preview=official_preview,
        official_word_count=len(clean.replace("\n", "").replace(" ", "")),
        primary_source_id="peer_a",
        chapter_title="第一章",
        aggregate_book_id="book:consensus",
        chapter_index=1,
    )
    untrusted_audit = untrusted["alignment_json"]["lineConsensus"]
    assert untrusted["content"] == selected
    assert untrusted_audit["rollback"] is True
    assert untrusted_audit["revalidation"]["officialAnchorTrusted"] is False


def test_line_consensus_preserves_different_two_source_watermarks():
    paragraphs = [f"第{i}段正文内容足够长，确保段落对齐稳定。" for i in range(1, 13)]
    selected = "\n\n".join([*paragraphs[:6], "甲站未知插入标记", *paragraphs[6:]])
    peer = "\n\n".join([*paragraphs[:6], "乙站完全不同的插入标记", *paragraphs[6:]])

    cleaned, audit = purify_by_line_consensus(
        selected,
        [
            {"source_id": "selected", "content": selected},
            {"source_id": "peer", "content": peer},
        ],
        selected_source_id="selected",
        chapter_title="第一章",
    )

    assert cleaned == selected
    assert audit["sourceCount"] == 2
    assert audit["applied"] is False
    assert audit["reason"] == "insufficient_sources"
    assert audit["removedLines"] == []


def test_line_consensus_requires_two_peers_to_support_both_anchors():
    paragraphs = [f"第{i}段正文内容足够长，确保段落对齐稳定。" for i in range(1, 21)]
    selected = "\n\n".join([*paragraphs[:10], "仅主源存在的额外段", *paragraphs[10:]])
    peer_a = "\n\n".join(paragraphs)
    unrelated = "\n\n".join(f"无关段落{i}完全不能形成正文锚点。" for i in range(1, 21))

    cleaned, audit = purify_by_line_consensus(
        selected,
        [
            {"source_id": "selected", "content": selected},
            {"source_id": "peer_a", "content": peer_a},
            {"source_id": "peer_b", "content": unrelated},
        ],
        selected_source_id="selected",
        chapter_title="第一章",
    )

    assert cleaned == selected
    assert audit["applied"] is False
    assert audit["reason"] == "no_removable_segments"


def test_line_consensus_preserves_blocks_longer_than_three_paragraphs():
    paragraphs = [f"第{i}段正文内容足够长，确保段落对齐稳定。" for i in range(1, 21)]
    extras = [f"额外差异段{i}" for i in range(1, 5)]
    selected = "\n\n".join([*paragraphs[:10], *extras, *paragraphs[10:]])
    peer = "\n\n".join(paragraphs)

    cleaned, audit = purify_by_line_consensus(
        selected,
        [
            {"source_id": "selected", "content": selected},
            {"source_id": "peer_a", "content": peer},
            {"source_id": "peer_b", "content": peer},
        ],
        selected_source_id="selected",
        chapter_title="第一章",
    )

    assert cleaned == selected
    assert audit["removedCount"] == 0
    assert audit["reason"] == "no_removable_segments"


def test_line_consensus_preserves_span_merged_unknown_content():
    selected = "第一段这是足够长的正文内容。\n\n6=9+\n\n第二段这是足够长的正文内容。"
    peer = "第一段这是足够长的正文内容。\n\n第二段这是足够长的正文内容。"

    cleaned, audit = purify_by_line_consensus(
        selected,
        [
            {"source_id": "selected", "content": selected},
            {"source_id": "peer_a", "content": peer},
            {"source_id": "peer_b", "content": peer},
        ],
        selected_source_id="selected",
        chapter_title="第一章",
    )

    assert "6=9+" in cleaned
    assert "第二段这是足够长的正文内容。" in cleaned
    assert audit["removedLines"] == []


def test_line_consensus_preserves_unverified_minority_prose():
    selected = "第一段真实正文。\n\n仅此来源存在的叙事补充。\n\n第二段真实正文。"
    peer = "第一段真实正文。\n\n第二段真实正文。"

    cleaned, audit = purify_by_line_consensus(
        selected,
        [
            {"source_id": "selected", "content": selected},
            {"source_id": "peer_a", "content": peer},
            {"source_id": "peer_b", "content": peer},
        ],
        selected_source_id="selected",
        chapter_title="第一章",
    )

    assert cleaned == selected
    assert audit["removedCount"] == 0
    assert audit["reason"] == "no_removable_segments"


def test_line_consensus_rejects_cleanup_over_two_percent():
    paragraphs = [f"第{i}段正文内容足够长，确保段落对齐稳定。" for i in range(1, 21)]
    extra = "这一整段来源独有内容很长，超过正文自动删除比例限制。" * 3
    selected = "\n\n".join([*paragraphs[:10], extra, *paragraphs[10:]])
    peer = "\n\n".join(paragraphs)

    cleaned, audit = purify_by_line_consensus(
        selected,
        [
            {"source_id": "selected", "content": selected},
            {"source_id": "peer_a", "content": peer},
            {"source_id": "peer_b", "content": peer},
        ],
        selected_source_id="selected",
        chapter_title="第一章",
    )

    assert cleaned == selected
    assert audit["applied"] is False
    assert audit["reason"] == "removal_ratio_exceeded"
    assert audit["proposedRemovedCount"] == 1
    assert audit["proposedRemovedLines"][0]["content"] == extra


def test_line_consensus_preserves_all_unverified_content_for_audit():
    watermarks = [
        f"无错版本在读！6=9+书吧首发本小说。{index:02d}-{'x' * 120}"
        for index in range(25)
    ]
    shared_paragraph = "Verified shared body paragraph. " * 300
    selected = "\n\n".join([shared_paragraph, shared_paragraph, *watermarks])
    peer = f"{shared_paragraph}\n\n{shared_paragraph}"

    cleaned, audit = purify_by_line_consensus(
        selected,
        [
            {"source_id": "selected", "content": selected},
            {"source_id": "peer_a", "content": peer},
            {"source_id": "peer_b", "content": peer},
        ],
        selected_source_id="selected",
        chapter_title="第一章",
    )

    assert all(watermark in cleaned for watermark in watermarks)
    assert audit["removedCount"] == 0
    assert audit["suspiciousCount"] == len(watermarks)
    assert audit["removedLines"] == []
    assert len(audit["suspiciousLines"]) == 20


def test_line_consensus_aligns_split_and_merged_paragraphs():
    selected = "第一段正文内容。\n\n第二段正文内容。\n\n第三段正文内容。"
    peers = [
        {"source_id": "selected", "content": selected},
        {"source_id": "merged", "content": "第一段正文内容。第二段正文内容。\n\n第三段正文内容。"},
        {"source_id": "split", "content": selected},
        {
            "source_id": "window_shift",
            "content": "噪声甲\n\n噪声乙\n\n噪声丙\n\n" + selected,
        },
    ]

    cleaned, audit = purify_by_line_consensus(
        selected,
        peers,
        selected_source_id="selected",
        chapter_title="第一章",
    )

    assert cleaned == selected
    assert audit["removedCount"] == 0
    assert audit["suspiciousCount"] == 0


def test_line_consensus_normalizes_traditional_unicode_space_and_punctuation():
    selected = "網際網路　連線正常！\n\n第二段正文。"
    peers = [
        {"source_id": "selected", "content": selected},
        {"source_id": "simplified", "content": "网际网路连线正常\n\n第二段正文"},
        {"source_id": "traditional", "content": "網際網路 連線正常。\n\n第二段正文。"},
    ]

    cleaned, audit = purify_by_line_consensus(
        selected,
        peers,
        selected_source_id="selected",
        chapter_title="第一章",
    )

    assert cleaned == selected
    assert audit["removedCount"] == 0
    assert audit["suspiciousCount"] == 0


def test_line_consensus_keeps_near_identical_mirror_wording():
    selected = "前一段正文内容。\n\n除了西河区边缘的一家开在旧厂房的新型网际网路公司。\n\n后一段正文内容。"
    peer = "前一段正文内容。\n\n除了西河区边缘的一家开在旧厂房的新型互联网公司。\n\n后一段正文内容。"

    cleaned, audit = purify_by_line_consensus(
        selected,
        [
            {"source_id": "selected", "content": selected},
            {"source_id": "peer", "content": peer},
        ],
        selected_source_id="selected",
        chapter_title="第一章",
    )

    assert cleaned == selected
    assert audit["removedCount"] == 0


def test_line_consensus_keeps_short_synonym_mirror_wording():
    selected = "前一段正文内容。\n\n然后，又甩上了一个链接。\n\n后一段正文内容。"
    peer = "前一段正文内容。\n\n然后，又甩上了一个连结。\n\n后一段正文内容。"

    cleaned, audit = purify_by_line_consensus(
        selected,
        [
            {"source_id": "selected", "content": selected},
            {"source_id": "peer", "content": peer},
        ],
        selected_source_id="selected",
        chapter_title="第一章",
    )

    assert cleaned == selected
    assert audit["removedCount"] == 0


@pytest.mark.parametrize("watermark", [
    "记住首发网站域名𝕥𝕨𝕜𝕒𝕟.𝕔𝕠𝕞",
    "本书由𝕥𝕨𝕜𝕒𝕟.𝕔𝕠𝕞全网首发",
    "Wωω ▪TTκan ▪co",
    "wωw ⊙тt kān ⊙￠o",
    "Wωω☢ ttKan☢ CO",
    "☢т tκa n ☢co",
    "щшш ●ttka n ●￠ ○",
    "WWW¤ttKan¤c o",
    "</ins>",
    "</div>",
])
def test_line_consensus_preserves_unknown_obfuscated_content(watermark):
    selected = f"第一段真实正文。\n\n{watermark}\n\n第二段真实正文。"
    peer = "第一段真实正文。\n\n第二段真实正文。"

    cleaned, audit = purify_by_line_consensus(
        selected,
        [
            {"source_id": "selected", "content": selected},
            {"source_id": "peer_a", "content": peer},
            {"source_id": "peer_b", "content": peer},
        ],
        selected_source_id="selected",
        chapter_title="第一章",
    )

    assert watermark in cleaned
    assert audit["removedCount"] == 0
    assert audit["reason"] == "no_removable_segments"


def test_line_consensus_keeps_two_source_difference_without_enough_anchors():
    selected = "第一段真实正文。\n\nGOOGLE搜索TWKAN\n\n第二段真实正文。"

    cleaned, audit = purify_by_line_consensus(
        selected,
        [
            {"source_id": "selected", "content": selected},
            {"source_id": "peer", "content": "第一段真实正文。\n\n第二段真实正文。"},
        ],
        selected_source_id="selected",
        chapter_title="第一章",
    )

    assert cleaned == selected
    assert audit["applied"] is False
    assert audit["reason"] == "insufficient_sources"
    assert audit["removedCount"] == 0


def test_candidate_consensus_can_degrade_to_two_sources_below_ninety_percent(tmp_path, monkeypatch):
    import app.services.aggregate_alignment as alignment

    processor = AggregateProcessor(db_path=tmp_path / "test.db")
    monkeypatch.setattr(alignment, "cross_source_content_similarity", lambda left, right: 0.85)

    selected, consensus = processor._select_consistent_candidate([
        {"source_id": "candidate_a", "content": "正文甲", "alignment_json": {}},
        {"source_id": "candidate_b", "content": "正文乙", "alignment_json": {}},
    ], allow_degraded=True)

    assert selected and selected["source_id"] == "candidate_a"
    assert all(item["supportCount"] == 0 for item in consensus)


def test_stage2_marks_two_valid_sources_as_degraded(tmp_path, monkeypatch):
    import app.services.aggregate_alignment as alignment

    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:stage2_two_source_degraded"
    _insert_book(db_path, book_id, aggregate_payload={
        "name": "测试书",
        "author": "作者",
        "sources": [
            {"bookId": "official_src:1", "sourceId": "official_src", "score": 100},
            {"bookId": "candidate_a:1", "sourceId": "candidate_a", "score": 95},
            {"bookId": "candidate_b:1", "sourceId": "candidate_b", "score": 85},
            {"bookId": "slow_browser:1", "sourceId": "slow_browser", "score": 80},
        ],
    })
    ch_id = _insert_chapter(db_path, book_id, index=1)
    preview = "少年站在山巅，望着远方的云海，心中涌起一股莫名的悸动。他知道这一切才刚刚开始，未来路还很长。"
    body_paragraphs = [
        f"第{index}段后续正文内容扩充了很多，确保段落锚点稳定。"
        for index in range(1, 13)
    ]
    candidate_a_content = "\n\n".join([
        preview,
        *body_paragraphs[:6],
        "甲站未知插入标记",
        *body_paragraphs[6:],
    ])
    candidate_b_content = "\n\n".join([
        preview,
        *body_paragraphs[:6],
        "乙站完全不同的插入标记",
        *body_paragraphs[6:],
    ])

    class TwoSourceCatalog(_FakeCatalog):
        async def toc(self, book_id: str) -> dict:
            source_id = book_id.split(":")[0]
            return {"chapters": [{
                "chapterId": encode_chapter_id(source_id, f"https://{source_id}.example/ch1.html"),
                "title": "第1章",
                "index": 1,
            }]}

        async def chapter(self, chapter_id: str) -> dict:
            if chapter_id.startswith("official_src"):
                return {"content": preview, "title": "第1章", "extra": {"previewOnly": True}}
            if chapter_id.startswith("candidate_a"):
                return {"content": candidate_a_content, "title": "第1章"}
            if chapter_id.startswith("candidate_b"):
                return {"content": candidate_b_content, "title": "第1章"}
            if chapter_id.startswith("slow_browser"):
                raise AssertionError("two HTTP candidates must avoid Browser fallback")
            return await super().chapter(chapter_id)

    processor = AggregateProcessor(db_path)
    processor._browser_source_ids = {"slow_browser"}
    monkeypatch.setattr(processor, "_is_official_source", lambda sid: sid == "official_src")
    monkeypatch.setattr(processor, "_discover_third_party_candidates", AsyncMock(return_value=[]))
    monkeypatch.setattr(alignment, "cross_source_content_similarity", lambda left, right: 0.85)
    for source_id, content in (("candidate_a", candidate_a_content), ("candidate_b", candidate_b_content)):
        processor._save_source_snapshot(
            aggregate_book_id=book_id,
            chapter_index=1,
            source_id=source_id,
            source_book_id=f"{source_id}:1",
            source_chapter_id=f"{source_id}:ch1",
            title="第1章",
            raw_content=content,
            classification="captured",
        )

    asyncio.run(processor._process_chapter(TwoSourceCatalog(), _chapter_dict(ch_id, book_id)))

    row = _get_chapter_row(db_path, ch_id)
    assert row["fallbackSourceId"] == "candidate_a"
    assert row["alignment"]["crossSourceConsensusMode"] == "two_source"
    assert row["alignment"]["crossSourceAcceptedCount"] == 2
    assert row["alignment"]["majorityDeletionAllowed"] is False
    assert row["alignment"]["lineConsensus"]["sourceCount"] == 2
    assert row["alignment"]["lineConsensus"]["removedCount"] == 0
    assert "甲站未知插入标记" in row["content"]


def test_stage2_uses_one_valid_candidate_as_explicit_degraded_fallback(tmp_path, monkeypatch):
    """A lone fully validated supplement is usable but cannot enable majority deletion."""
    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:stage2_second_source"
    _insert_book(db_path, book_id, aggregate_payload={
        "name": "测试书", "author": "作者",
        "sources": [
            {"bookId": "official_src:1", "sourceId": "official_src", "score": 100, "bookUrl": "https://official.example/book/1"},
            {"bookId": "candidate_a:1", "sourceId": "candidate_a", "score": 95, "bookUrl": "https://candidate-a.example/book/1"},
            {"bookId": "candidate_b:1", "sourceId": "candidate_b", "score": 85, "bookUrl": "https://candidate-b.example/book/1"},
            {"bookId": "slow_browser:1", "sourceId": "slow_browser", "score": 80, "bookUrl": "https://browser.example/book/1"},
        ],
    })
    ch_id = _insert_chapter(db_path, book_id, index=1)

    preview = "少年站在山巅，望着远方的云海，心中涌起一股莫名的悸动。他知道这一切才刚刚开始，未来路还很长。"
    candidate_b_content = ("【B站】少年站在山巅，望着远方的云海，心中涌起一股莫名的悸动。"
                           "他知道这一切才刚刚开始，未来路还很长。后续正文内容扩充了很多。" * 10)
    fetched_candidate_ids: list[str] = []

    class MultiSourceCatalog(_FakeCatalog):
        async def toc(self, book_id: str) -> dict:
            source_id = book_id.split(":")[0]
            return {"chapters": [
                {"chapterId": encode_chapter_id(source_id, f"https://{source_id}.example/ch1.html"), "title": "第1章", "index": 1}
            ]}

        async def chapter(self, chapter_id: str) -> dict:
            if chapter_id.startswith("official_src"):
                return {"content": preview, "title": "第1章", "extra": {"previewOnly": True}}
            fetched_candidate_ids.append(chapter_id)
            if chapter_id.startswith("candidate_a"):
                return {"content": "", "title": "第1章"}
            if chapter_id.startswith("candidate_b"):
                return {"content": candidate_b_content, "title": "第1章"}
            if chapter_id.startswith("slow_browser"):
                raise AssertionError("validated HTTP candidate must avoid Browser fallback")
            return await super().chapter(chapter_id)

    processor = AggregateProcessor(db_path)
    processor._browser_source_ids = {"slow_browser"}
    monkeypatch.setattr(processor, "_is_official_source", lambda sid: sid == "official_src")
    monkeypatch.setattr(processor, "_discover_third_party_candidates", AsyncMock(return_value=[]))
    processor._save_source_snapshot(
        aggregate_book_id=book_id,
        chapter_index=1,
        source_id="candidate_b",
        source_book_id="candidate_b:1",
        source_chapter_id="candidate_b:ch1",
        title="第1章",
        raw_content=candidate_b_content,
        classification="captured",
    )

    asyncio.run(processor._process_chapter(MultiSourceCatalog(), _chapter_dict(ch_id, book_id)))

    row = _get_chapter_row(db_path, ch_id)
    assert row["status"] == "fallback"
    assert row["content"] != ""
    assert row["alignment"]["candidateSourceId"] == "candidate_b"
    assert row["alignment"]["crossSourceConsensusMode"] == "single_source"
    assert row["alignment"]["crossSourceAcceptedCount"] == 1
    assert row["alignment"]["majorityDeletionAllowed"] is False
    assert row["alignment"]["lineConsensus"]["applied"] is False
    assert row["alignment"]["lineConsensus"]["reason"] == "insufficient_sources"
    assert row["previewOnly"] is False
    assert row["fallbackSourceId"] == "candidate_b"
    assert fetched_candidate_ids == []


def test_stage2_uses_precaptured_candidate_after_preview_alignment(tmp_path, monkeypatch):
    """Publishing reads the local candidate snapshot after preview-head validation."""
    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:stage2_nearby_index_drift"
    _insert_book(db_path, book_id, aggregate_payload={
        "name": "测试书", "author": "作者",
        "sources": [
            {"bookId": "official_src:1", "sourceId": "official_src", "score": 100, "bookUrl": "https://official.example/book/1"},
            {"bookId": "candidate_src:1", "sourceId": "candidate_src", "score": 80, "bookUrl": "https://candidate.example/book/1"},
        ],
    })
    ch_id = _insert_chapter(db_path, book_id, index=1)

    preview = "少年站在山巅，望着远方的云海，心中涌起一股莫名的悸动。他知道这一切才刚刚开始，未来路还很长。"
    matched_candidate = ("【候选源】" + preview + "后续正文内容扩充了很多。" * 20)

    class DriftCatalog(_FakeCatalog):
        async def toc(self, book_id: str) -> dict:
            source_id = book_id.split(":")[0]
            if source_id == "candidate_src":
                return {"chapters": [
                    {"chapterId": encode_chapter_id("candidate_src", "https://candidate.example/wrong.html"), "title": "山风起时", "index": 1},
                    {"chapterId": encode_chapter_id("candidate_src", "https://candidate.example/right.html"), "title": "云海翻腾", "index": 2},
                    {"chapterId": encode_chapter_id("candidate_src", "https://candidate.example/other.html"), "title": "夜雨将至", "index": 3},
                ]}
            return await super().toc(book_id)

        async def chapter(self, chapter_id: str) -> dict:
            _, chapter_url = decode_chapter_id(chapter_id)
            if chapter_url.endswith("wrong.html"):
                return {"content": "完全不相关的错误正文。" * 30, "title": "山风起时"}
            if chapter_url.endswith("right.html"):
                return {"content": matched_candidate, "title": "云海翻腾"}
            if chapter_url.endswith("other.html"):
                return {"content": "另一段错误正文。" * 30, "title": "夜雨将至"}
            return await super().chapter(chapter_id)

    processor = AggregateProcessor(db_path)
    monkeypatch.setattr(processor, "_is_official_source", lambda sid: sid == "official_src")
    processor._save_source_snapshot(
        aggregate_book_id=book_id,
        chapter_index=1,
        source_id="candidate_src",
        source_book_id="candidate_src:1",
        source_chapter_id=encode_chapter_id("candidate_src", "https://candidate.example/right.html"),
        title="第1章",
        raw_content=matched_candidate,
        classification="captured",
    )

    asyncio.run(processor._process_chapter(DriftCatalog(official_content=preview, official_extra={"previewOnly": True}), _chapter_dict(ch_id, book_id)))

    row = _get_chapter_row(db_path, ch_id)
    assert row["status"] == "fallback"
    assert row["alignment"]["candidateSourceId"] == "candidate_src"
    assert row["alignment"]["crossSourceConsensusMode"] == "single_source"
    assert row["alignment"]["majorityDeletionAllowed"] is False
    assert row["previewOnly"] is False
    assert row["fallbackSourceId"] == "candidate_src"
    assert preview in row["content"]


def test_stage2_rejects_candidate_that_is_not_longer_than_official_preview(tmp_path, monkeypatch):
    """A candidate that is still shorter than the official preview should not replace the official preview."""
    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:stage2_shorter_candidate"
    _insert_book(db_path, book_id, aggregate_payload={
        "name": "测试书", "author": "作者",
        "sources": [
            {"bookId": "official_src:1", "sourceId": "official_src", "score": 100, "bookUrl": "https://official.example/book/1"},
            {"bookId": "candidate_src:1", "sourceId": "candidate_src", "score": 80, "bookUrl": "https://candidate.example/book/1"},
        ],
    })
    ch_id = _insert_chapter(db_path, book_id, index=1)

    official_preview = ("少年站在山巅，望着远方的云海，心中涌起一股莫名的悸动。"
                        "他知道这一切才刚刚开始，未来路还很长。") * 3
    shorter_candidate = official_preview[: len(official_preview) - 40]

    class PreviewVsShorterCandidateCatalog(_FakeCatalog):
        async def toc(self, book_id: str) -> dict:
            source_id = book_id.split(":")[0]
            if source_id == "candidate_src":
                return {"chapters": [
                    {"chapterId": encode_chapter_id("candidate_src", "https://candidate.example/ch1.html"), "title": "第1章", "index": 1}
                ]}
            return await super().toc(book_id)

        async def chapter(self, chapter_id: str) -> dict:
            if chapter_id.startswith("official_src"):
                return {
                    "content": official_preview,
                    "title": "第1章",
                    "extra": {"previewOnly": True},
                }
            if chapter_id.startswith("candidate_src"):
                return {"content": shorter_candidate, "title": "第1章"}
            return await super().chapter(chapter_id)

    processor = AggregateProcessor(db_path)
    monkeypatch.setattr(processor, "_is_official_source", lambda sid: sid == "official_src")

    result = asyncio.run(processor._process_chapter(PreviewVsShorterCandidateCatalog(), _chapter_dict(ch_id, book_id)))

    row = _get_chapter_row(db_path, ch_id)
    assert result.get("fallback") is True
    assert row["status"] == "fallback"
    assert row["previewOnly"] == 1
    assert row["fallbackSourceId"] == "official_src"
    assert row["alignment"]["selectedContentSource"] == "preview_fallback"
    assert official_preview[:80] in row["content"]


def test_stage2_falls_back_to_official_preview_when_candidates_fail(tmp_path, monkeypatch):
    """When every candidate source fails, Stage 2 should still keep the official preview as a readable fallback."""
    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:stage2_wait"
    _insert_book(db_path, book_id, aggregate_payload={
        "name": "测试书", "author": "作者",
        "sources": [
            {"bookId": "official_src:1", "sourceId": "official_src", "score": 100, "bookUrl": "https://official.example/book/1"},
            {"bookId": "candidate_a:1", "sourceId": "candidate_a", "score": 95, "bookUrl": "https://candidate-a.example/book/1"},
            {"bookId": "candidate_b:1", "sourceId": "candidate_b", "score": 85, "bookUrl": "https://candidate-b.example/book/1"},
        ],
    })
    ch_id = _insert_chapter(db_path, book_id, index=1)

    preview = "少年站在山巅，望着远方的云海，心中涌起一股莫名的悸动。他知道这一切才刚刚开始。"

    class MultiSourceCatalog(_FakeCatalog):
        async def toc(self, book_id: str) -> dict:
            source_id = book_id.split(":")[0]
            return {"chapters": [
                {"chapterId": encode_chapter_id(source_id, f"https://{source_id}.example/ch1.html"), "title": "第1章", "index": 1}
            ]}

        async def chapter(self, chapter_id: str) -> dict:
            if chapter_id.startswith("official_src"):
                return {"content": preview, "title": "第1章"}
            if chapter_id.startswith("candidate_a"):
                return {"content": "", "title": "第1章"}
            if chapter_id.startswith("candidate_b"):
                return {"content": "这是一段很短的无效内容。", "title": "第1章"}
            return await super().chapter(chapter_id)

    processor = AggregateProcessor(db_path)
    monkeypatch.setattr(processor, "_is_official_source", lambda sid: sid == "official_src")

    result = asyncio.run(processor._process_chapter(MultiSourceCatalog(), _chapter_dict(ch_id, book_id)))

    row = _get_chapter_row(db_path, ch_id)
    assert result["success"] is True
    assert result.get("fallback") is True
    assert row["status"] == "fallback"
    assert row["previewOnly"] == 1
    assert preview in row["content"]


def test_refresh_shared_book_state_uses_configured_min_readable_threshold(tmp_path):
    db_path = _setup_db(tmp_path, ai_enabled=False)
    book_id = "book:visibility_threshold"
    _insert_book(db_path, book_id)
    for index in range(1, 71):
        _insert_chapter(db_path, book_id, index=index, status="processed")

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE aggregate_book_tasks
            SET total_chapters = 200,
                start_chapter_index = 1,
                processed_chapters = 70,
                visible_processed_chapters = 0,
                search_visibility_status = 'hidden'
            WHERE aggregate_book_id = ?
            """,
            (book_id,),
        )
        conn.commit()

    config_path = tmp_path / "config" / "app_config.json"
    config_data = json.loads(config_path.read_text(encoding="utf-8"))
    config_data["aggregate"]["contentWorkflow"]["minReadableChaptersForDiscovery"] = 80
    config_path.write_text(json.dumps(config_data, ensure_ascii=False), encoding="utf-8")

    processor = AggregateProcessor(db_path)
    processor._refresh_shared_book_state(book_id)

    with sqlite3.connect(db_path) as conn:
        refreshed = conn.execute(
            """
            SELECT visible_processed_chapters, search_visibility_status
            FROM aggregate_book_tasks
            WHERE aggregate_book_id = ?
            """,
            (book_id,),
        ).fetchone()

    assert refreshed[0] == 70
    assert refreshed[1] == "hidden"


class TestSharedBookProcessLoggerTailStream:
    """Tail -f style log streaming for the subscription processing panel."""

    @pytest.mark.asyncio
    async def test_tail_stream_yields_appended_records(self, tmp_path):
        from app.services.shared_book_storage import SharedBookStorage
        from app.services.shared_book_runtime import SharedBookProcessLogger

        storage = SharedBookStorage(root=tmp_path / "library")
        logger = SharedBookProcessLogger(storage)
        book_name = "测试日志书"
        author = "测试作者"

        records = []
        tail_task = asyncio.create_task(
            self._collect(logger.tail_stream(book_name=book_name, author=author, poll_interval_seconds=0.05), records)
        )

        await asyncio.sleep(0.02)
        logger.append(book_name=book_name, author=author, event="chapter_write", book_id="b1", chapter_index=1, stage="stage1", payload={"title": "第一章"})
        logger.append(book_name=book_name, author=author, event="chapter_error", book_id="b1", chapter_index=2, stage="stage1", error_code="E1", error_message="boom")

        await asyncio.sleep(0.1)
        tail_task.cancel()
        try:
            await tail_task
        except asyncio.CancelledError:
            pass

        assert len(records) >= 2
        assert records[0]["event"] == "chapter_write"
        assert records[0]["chapterIndex"] == 1
        assert records[1]["event"] == "chapter_error"
        assert records[1]["errorCode"] == "E1"

    async def _collect(self, gen, out):
        async for record in gen:
            out.append(record)
