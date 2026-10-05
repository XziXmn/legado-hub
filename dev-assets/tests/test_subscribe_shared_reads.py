"""Tests that /api/subscribe/books/* reads from shared files, not DB truth."""

import json
import os
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "backend"))

from app.main import app
from app.services.library_books import LibraryBooksService
from app.services.shared_book_storage import SharedBookStorage
from app.services.user_subscriptions import UserSubscriptionsService
from app.storage.db import initialize_database


_PRIVATE_RESPONSE_KEYS = {
    "addedbyuserid",
    "addedbyusername",
    "aggregatepayloadjson",
    "authorization",
    "cookie",
    "cookies",
    "debug",
    "filepath",
    "password",
    "passwordhash",
    "path",
    "primarybookurl",
    "primarytocurl",
    "settingsjson",
    "sourcemap",
    "sourcemaprefresh",
    "sourcemapsummary",
    "sources",
    "trace",
}


@pytest.fixture
def client(tmp_path, monkeypatch):
    db = tmp_path / "test.db"
    monkeypatch.setattr("app.config.DB_PATH", db)
    monkeypatch.setattr("app.services.cache.DB_PATH", db)
    initialize_database(db)

    # Ensure shared storage writes under the temp directory.
    storage = SharedBookStorage(root=tmp_path / "library")
    service = LibraryBooksService(db_path=db, shared_book_storage=storage)
    subscription_service = UserSubscriptionsService(db_path=db)
    monkeypatch.setattr("app.api.subscribe.library_books_service", service)
    monkeypatch.setattr("app.api.legado.library_books_service", service)
    monkeypatch.setattr("app.api.subscribe.user_subscriptions_service", subscription_service)
    monkeypatch.setattr("app.services.library_books.library_books_service", service)

    class EmptyReadingScheduler:
        @staticmethod
        def _enabled_plugins():
            return []

        @staticmethod
        def _search_priority_plugins(plugins):
            return plugins

    class EmptyReadingSearchService:
        scheduler = EmptyReadingScheduler()

    monkeypatch.setattr(
        "app.api.subscribe._get_legado_search_service",
        lambda: EmptyReadingSearchService(),
    )

    tc = TestClient(app)
    login = tc.post("/api/auth/login", json={"username": "admin", "password": "admin123"})
    if login.status_code != 200:
        pytest.skip(f"admin login unavailable: {login.status_code} {login.text}")
    return tc


def test_legado_manifest_is_anonymous_but_reading_requires_valid_bearer(client):
    import hashlib
    import sqlite3
    import uuid

    from app.services.user_auth import auth_service

    anonymous = TestClient(app)
    assert anonymous.get("/api/subscribe/legado/source").status_code == 401
    created = auth_service.create_access_user(f"source-{uuid.uuid4().hex[:8]}")
    manifest = anonymous.get(
        "/api/subscribe/legado/source",
        params={"code": created["accessCode"]},
    )
    assert manifest.status_code == 200
    assert manifest.json()[0]["loginUi"]

    protected_paths = [
        "/api/subscribe/legado/search?keyword=test",
        "/api/subscribe/legado/search/missing-job",
        "/api/subscribe/legado/explore",
        "/api/legado/book/not-a-book-id",
        "/api/legado/chapter/not-a-chapter-id",
        "/api/legado/chapter/not-a-chapter-id/reviews",
        "/api/legado/chapter/not-a-chapter-id/reviews/view",
    ]
    for path in protected_paths:
        response = anonymous.get(path)
        assert response.status_code == 401
        assert response.json() == {"detail": "当前未登陆，请登陆后使用。"}

    console_response = anonymous.get("/api/console/chapter/invalid")
    assert console_response.status_code == 401
    assert console_response.json() == {"detail": "请先登录"}

    assert client.get(
        "/api/subscribe/legado/search?keyword=test",
        headers={"Authorization": "Bearer invalid-session-token"},
    ).status_code == 401

    created = auth_service.create_access_user(f"reading-{uuid.uuid4().hex[:10]}")
    redeemed = anonymous.post(
        "/api/auth/access/redeem",
        json={"accessCode": created["accessCode"]},
    )
    assert redeemed.status_code == 200
    token = redeemed.json()["token"]
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    with sqlite3.connect(auth_service.db_path) as conn:
        before_last_seen = conn.execute(
            "SELECT last_seen_at FROM user_sessions WHERE session_id = ?",
            (token_hash,),
        ).fetchone()[0]

    bearer = TestClient(app)
    headers = {"Authorization": f"Bearer {token}"}
    assert bearer.get(
        "/api/subscribe/legado/search?keyword=test",
        headers=headers,
    ).status_code == 200
    assert bearer.get("/api/subscribe/legado/explore", headers=headers).status_code == 200

    with sqlite3.connect(auth_service.db_path) as conn:
        after_last_seen = conn.execute(
            "SELECT last_seen_at FROM user_sessions WHERE session_id = ?",
            (token_hash,),
        ).fetchone()[0]
    assert after_last_seen == before_last_seen


def test_legado_source_release_ignores_stale_runtime_version_and_updates_client_marker(monkeypatch):
    import app.core.legado_source as legado_source

    monkeypatch.setattr(
        legado_source,
        "load_aggregate_config",
        lambda: {"version": "0.0.1"},
    )

    # testserver is treated as LAN/local (same as localhost).
    source = legado_source.generate_legado_source("http://testserver")[0]

    assert source["bookSourceName"] == (
        f"LegadoHub 聚合·内网({legado_source._READER_RULE_VERSION})"
    )
    assert source["bookSourceUrl"] == "LegadoHub-LAN"
    assert "内网" in source["bookSourceGroup"]
    assert source["lastUpdateTime"] >= legado_source._reader_rule_version_stamp()
    assert f"规则版本 {legado_source._READER_RULE_VERSION}" in source["bookSourceComment"]
    assert "LegadoHub-LAN" in source["bookSourceComment"]
    assert (
        f'LEGADOHUB_RULE_VERSION = "{legado_source._READER_RULE_VERSION}"' in source["jsLib"]
    )
    assert source["lastUpdateTime"] > 1_700_000_000_000

    public = legado_source.generate_legado_source("https://books.example.com")[0]
    assert public["bookSourceName"] == f"LegadoHub 聚合({legado_source._READER_RULE_VERSION})"
    assert public["bookSourceUrl"] == "LegadoHub"
    assert public["bookSourceUrl"] != source["bookSourceUrl"]
    assert "内网" not in public["bookSourceName"]
    assert "公网书源" in public["bookSourceComment"]

    lan_ip = legado_source.generate_legado_source("http://192.168.1.10:8765")[0]
    assert lan_ip["bookSourceUrl"] == "LegadoHub-LAN"
    assert "·内网" in lan_ip["bookSourceName"]


def test_legado_source_update_marker_advances_with_comment_settings(tmp_path, monkeypatch):
    import app.core.legado_source as legado_source
    from app.core.app_config import AppConfig

    config_path = tmp_path / "app_config.json"
    config_path.write_text(
        json.dumps({"chapterComment": {"segmentEnabled": True}}),
        encoding="utf-8",
    )
    runtime_config = AppConfig(config_path)
    monkeypatch.setattr(
        legado_source.AppConfig,
        "get",
        staticmethod(lambda: runtime_config),
    )

    before = legado_source.generate_legado_source("http://testserver")[0]
    next_update_time = before["lastUpdateTime"] + 2_000
    config_path.write_text(
        json.dumps({"chapterComment": {"segmentEnabled": False}}),
        encoding="utf-8",
    )
    os.utime(
        config_path,
        ns=(next_update_time * 1_000_000, next_update_time * 1_000_000),
    )
    runtime_config.reload()

    after = legado_source.generate_legado_source("http://testserver")[0]

    assert after["lastUpdateTime"] == next_update_time
    assert after["lastUpdateTime"] > before["lastUpdateTime"]
    assert after["ruleContent"]["chapterComment"]["display"]["segment"]["enabled"] is False


@pytest.mark.parametrize("page", [True, "invalid", 1.5, 0, -1, 1001])
def test_subscription_search_rejects_invalid_page(client, page):
    response = client.post("/api/subscribe/search", json={"keyword": "页码测试", "page": page})

    assert response.status_code == 422
    assert response.json()["detail"] == "page 必须是 1 到 1000 的整数"


def _insert_book(
    db_path: Path,
    aggregate_book_id: str,
    name: str,
    author: str,
    *,
    published: bool = False,
    chapter_count: int = 0,
    visible_chapter_count: int = 0,
) -> None:
    import sqlite3

    with sqlite3.connect(db_path) as conn:
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
            ) VALUES (?, ?, ?, ?, ?, '', '', '', ?, 'src:book', 'src', 'src', '', '',
                      'user-1', 1, 10, 0, 0, 1, 'hidden', 'ongoing', 0, 0, 0, 0, 0, 'active',
                      '{}', 1, 60, NULL, NULL, 0, '', 1, NULL, NULL, datetime('now'), datetime('now'))
            """,
            (
                aggregate_book_id,
                name,
                author,
                name,
                author,
                json.dumps({"name": name, "author": author, "primarySourceId": "src"}, ensure_ascii=False),
            ),
        )
        if published:
            conn.execute(
                """
                UPDATE aggregate_book_tasks
                SET search_visibility_status = 'visible', total_chapters = ?,
                    processed_chapters = ?, visible_processed_chapters = ?
                WHERE aggregate_book_id = ?
                """,
                (chapter_count, chapter_count, visible_chapter_count, aggregate_book_id),
            )
        conn.commit()


def _write_shared_files(
    storage: SharedBookStorage,
    aggregate_book_id: str,
    name: str,
    author: str,
    *,
    chapter_specs: list[tuple[int, str, bool]] | None = None,
    include_source_ids: bool = True,
) -> None:
    specs = chapter_specs or [(1, "第一章", False), (2, "第二章", False)]
    preview_count = sum(1 for _, _, preview_only in specs if preview_only)
    metadata = storage.build_shared_metadata(
        {
            "name": name,
            "author": author,
            "primarySourceId": "src",
            "primarySourceName": "Src",
            "sources": [
                {
                    "sourceId": "src",
                    "sourceName": "Source",
                    "bookId": "src:book",
                    "bookUrl": "https://example.com/book",
                    "tocUrl": "https://example.com/toc",
                    "score": 100,
                }
            ],
            "bookState": {
                "chapterCount": len(specs),
                "readableChapterCount": len(specs),
                "previewChapterCount": preview_count,
                "processedChapterCount": len(specs),
            },
        }
    )
    metadata["sourceMap"] = {
        "summary": metadata.get("sourceMapSummary", []),
        "health": {"lastVerifiedAt": "2026-06-26T00:00:00+00:00", "status": "healthy", "missingCriticalSource": False},
    }

    chapter_files = []
    chapter_entries = []
    for idx, title, preview_only in specs:
        ch_id = f"src:ch{idx}"
        trace = {
            "schemaVersion": 1,
            "chapterIndex": idx,
            "chapterTitle": title,
            "chapterStatus": "readable",
            "previewOnly": preview_only,
            "isVip": preview_only,
            "primarySource": {"sourceId": "src", "chapterId": ch_id, "wordCount": 100},
        }
        body = f"{title} {'付费预览正文' if preview_only else '完整免费正文'}"
        markdown = storage.render_chapter_markdown(title=title, body=body, trace_payload=trace)
        path = storage.chapter_markdown_path(book_name=name, author=author, chapter_index=idx, title=title)
        chapter_files.append((path, markdown))
        chapter_entries.append(
            {
                "index": idx,
                "title": title,
                "file": f"chapters/{path.name}",
                "status": "readable",
                "sourceChapterId": ch_id if include_source_ids else "",
                "isVip": preview_only,
            }
        )

    chapter_index = {"schemaVersion": 1, "bookId": aggregate_book_id, "chapters": chapter_entries}
    storage.write_book_bundle(
        metadata_path=storage.metadata_path(book_name=name, author=author),
        metadata_payload=metadata,
        chapter_index_path=storage.chapter_index_path(book_name=name, author=author),
        chapter_index_payload=chapter_index,
        chapter_files=chapter_files,
    )


def _reader_client(db_path: Path, username: str) -> tuple[TestClient, dict]:
    import sqlite3

    from app.services.user_auth import auth_service

    password = f"{username}-password"
    user = auth_service.get_user_by_username(username)
    if not user:
        user = auth_service.create_user(username, password, role="user")
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO users (user_id, username, password_hash, role)
            VALUES (?, ?, 'test-hash', 'user')
            """,
            (user["userId"], username),
        )
        conn.commit()
    reader = TestClient(app)
    login = reader.post(
        "/api/auth/access/redeem",
        json={"accessCode": auth_service.build_access_code(username, password)},
    )
    assert login.status_code == 200
    return reader, user


def _assert_no_private_response_fields(payload) -> None:
    if isinstance(payload, dict):
        for key, value in payload.items():
            assert key.casefold() not in _PRIVATE_RESPONSE_KEYS, f"private response field: {key}"
            _assert_no_private_response_fields(value)
    elif isinstance(payload, list):
        for item in payload:
            _assert_no_private_response_fields(item)

    serialized = json.dumps(payload, ensure_ascii=False)
    assert "https://example.com/book" not in serialized
    assert "https://example.com/toc" not in serialized


def test_get_library_book_reads_shared_metadata_not_db_sources(client, tmp_path, monkeypatch):
    db = tmp_path / "test.db"
    storage = SharedBookStorage(root=tmp_path / "library")
    service = LibraryBooksService(db_path=db, shared_book_storage=storage)
    monkeypatch.setattr("app.api.subscribe.library_books_service", service)

    book_id = "book-shared-read-1"
    _insert_book(db, book_id, "测试书", "作者")
    _write_shared_files(storage, book_id, "测试书", "作者")

    response = client.get(f"/api/subscribe/books/{book_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["found"] is True
    assert data["book"]["aggregateBookId"] == book_id
    assert "primaryBookUrl" not in data["book"]
    assert "primaryTocUrl" not in data["book"]
    assert "settingsJson" not in data["book"]
    assert "addedByUserId" not in data["book"]
    assert "bookState" in data
    assert data["bookState"]["readableChapterCount"] == 2

    assert "sources" not in data
    assert "sourceMap" not in data
    assert "sourceMapRefresh" not in data
    payload = data.get("payload")
    assert payload is None, "private payload must not be exposed to subscribe endpoint"


def test_list_library_book_chapters_reads_shared_files(client, tmp_path, monkeypatch):
    db = tmp_path / "test.db"
    storage = SharedBookStorage(root=tmp_path / "library")
    service = LibraryBooksService(db_path=db, shared_book_storage=storage)
    monkeypatch.setattr("app.api.subscribe.library_books_service", service)

    book_id = "book-shared-read-2"
    _insert_book(db, book_id, "测试书二", "作者二")
    _write_shared_files(storage, book_id, "测试书二", "作者二")

    response = client.get(f"/api/subscribe/books/{book_id}/chapters")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 2
    items = data["items"]
    assert len(items) == 2
    assert items[0]["chapterIndex"] == 1
    assert items[0]["hasContent"] is True
    assert items[0]["status"] == "readable"
    assert items[0]["readChapterId"].startswith("legadohub_ai_aggregate:"), f"readChapterId must be encoded aggregate ID, got: {items[0]['readChapterId']}"
    assert not items[0]["readChapterId"].isdigit(), "readChapterId must not be a numeric chapter index"
    assert "sourceChapterId" not in items[0]
    assert "primarySourceChapterUrl" not in items[0]
    assert "processed_content" not in items[0]

    filtered = client.get(
        f"/api/subscribe/books/{book_id}/chapters",
        params={"keyword": "第二"},
    )
    assert filtered.status_code == 200
    assert [item["title"] for item in filtered.json()["items"]] == ["第二章"]


def test_chapter_list_reads_only_requested_page_files(client, tmp_path, monkeypatch):
    import app.api.subscribe as subscribe_api

    db = tmp_path / "test.db"
    storage = SharedBookStorage(root=tmp_path / "library")
    service = LibraryBooksService(db_path=db, shared_book_storage=storage)
    monkeypatch.setattr(subscribe_api, "library_books_service", service)

    book_id = "book-page-read-count"
    _insert_book(db, book_id, "分页读取测试书", "作者")
    _write_shared_files(
        storage,
        book_id,
        "分页读取测试书",
        "作者",
        chapter_specs=[
            (1, "第一章", False),
            (2, "第二章", False),
            (3, "第三章", False),
        ],
    )

    original_read_text = Path.read_text
    markdown_reads: list[Path] = []

    def tracked_read_text(path: Path, *args, **kwargs):
        if path.suffix == ".md":
            markdown_reads.append(path)
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", tracked_read_text)
    response = client.get(
        f"/api/subscribe/books/{book_id}/chapters",
        params={"page": 2, "pageSize": 1},
    )

    assert response.status_code == 200
    assert [item["title"] for item in response.json()["items"]] == ["第二章"]
    assert len(markdown_reads) == 1
    assert markdown_reads[0].suffix == ".md"


def test_legado_published_search_pages_in_database_without_metadata_reads(
    client, tmp_path, monkeypatch
):
    import app.api.subscribe as subscribe_api

    service = subscribe_api.library_books_service
    for index in range(25):
        _insert_book(
            service.db_path,
            f"book-paged-{index:02d}",
            f"分页书 {index:02d}",
            "分页作者",
            published=True,
            chapter_count=1,
            visible_chapter_count=1,
        )

    monkeypatch.setattr(
        service,
        "_attach_book_state_summary",
        lambda _item: pytest.fail("published paging must not read per-book metadata"),
    )
    first = client.get(
        "/api/subscribe/legado/search",
        params={"keyword": "分页书", "page": 1},
    )
    second = client.get(
        "/api/subscribe/legado/search",
        params={"keyword": "分页书", "page": 2},
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert len(first.json()["items"]) == 20
    assert len(second.json()["items"]) == 0
    assert "debug" not in first.json()
    assert "debug" not in second.json()
    first_ids = {item["aggregateBookId"] for item in first.json()["items"]}
    assert first_ids


def test_legado_search_includes_published_book_when_third_party_snapshot_is_empty(client):
    import app.api.subscribe as subscribe_api

    aggregate_book_id = "book-empty-third-party-search"
    _insert_book(
        subscribe_api.library_books_service.db_path,
        aggregate_book_id,
        "空快照聚合书",
        "聚合作者",
        published=True,
        chapter_count=1,
        visible_chapter_count=1,
    )

    result = subscribe_api._legado_search_payload(
        keyword="空快照聚合书",
        page=1,
        base_api="http://testserver",
        user_id="reader-empty-snapshot",
        allowed_source_ids={"fixture_thirdparty"},
        snapshot={
            "jobId": "empty-third-party-job",
            "status": "completed",
            "items": [],
        },
    )

    assert [item["aggregateBookId"] for item in result["items"]] == [aggregate_book_id]
    assert result["items"][0]["sourceId"] == "legadohub_ai_aggregate"



@pytest.mark.asyncio
async def test_legado_search_wait_page_window(monkeypatch):
    import app.api.subscribe as subscribe_api

    class RunningSession:
        status = "running"
        live_items: list = []

    class SearchService:
        @staticmethod
        def get_session(_job_id):
            return RunningSession()

    class Clock:
        now = 0.0

        def time(self):
            return self.now

    clock = Clock()

    async def advance_clock(seconds):
        clock.now += seconds

    monkeypatch.setattr(subscribe_api.asyncio, "get_running_loop", lambda: clock)
    monkeypatch.setattr(subscribe_api.asyncio, "sleep", advance_clock)

    completed = await subscribe_api._wait_for_legado_search(
        SearchService(),
        "slow-reader-job",
        650,
        stop_on_first_result=True,
    )
    assert completed == "deadline"
    assert 0.65 <= clock.now < 0.66


@pytest.mark.asyncio
async def test_legado_search_wait_stops_on_first_third_party_item(monkeypatch):
    import app.api.subscribe as subscribe_api
    from app.source_plugins.id_codec import encode_book_id

    book_id = encode_book_id(
        "fixture_thirdparty",
        "https://example.com/book/first-hit",
    )

    class Session:
        def __init__(self):
            self.status = "running"
            self.live_items: list[dict] = []

    session = Session()

    class SearchService:
        @staticmethod
        def get_session(_job_id):
            return session

    class Clock:
        now = 0.0

        def time(self):
            return self.now

    clock = Clock()
    polls = {"n": 0}

    async def advance_clock(seconds):
        clock.now += seconds
        polls["n"] += 1
        if polls["n"] >= 2:
            session.live_items = [
                {
                    "sourceId": "fixture_thirdparty",
                    "name": "命中",
                    "bookId": book_id,
                }
            ]

    monkeypatch.setattr(subscribe_api.asyncio, "get_running_loop", lambda: clock)
    monkeypatch.setattr(subscribe_api.asyncio, "sleep", advance_clock)

    completed = await subscribe_api._wait_for_legado_search(
        SearchService(),
        "first-hit-job",
        120_000,
        stop_on_first_result=True,
        known_book_ids=set(),
        base_api="http://testserver",
        allowed_source_ids={"fixture_thirdparty"},
    )
    assert completed == "ready"
    assert clock.now < 1.0


@pytest.mark.asyncio
async def test_legado_search_page1_short_wait_keeps_job_running(monkeypatch):
    """page=1 returns after short wait; does not cancel the third-party job."""
    import app.api.subscribe as subscribe_api
    from app.source_plugins.id_codec import encode_book_id

    partial_book_id = encode_book_id(
        "fixture_thirdparty",
        "https://example.com/book/early-ready",
    )

    class Job:
        job_id = "reader-page1-job"

    class SearchService:
        cancelled_job_ids: list[str] = []

        @staticmethod
        def create_job(*, keyword, page, source_ids, search_mode):
            assert page == 1
            return Job()

        @classmethod
        def cancel_job(cls, job_id):
            cls.cancelled_job_ids.append(job_id)
            return True

        @staticmethod
        def get_session(_job_id):
            class Running:
                status = "running"
                live_items = [{"name": "hit"}]

            return Running()

        @staticmethod
        def session_snapshot(job_id, *, base_api, include_official_sources):
            return {
                "jobId": job_id,
                "keyword": "分页续搜",
                "page": 1,
                "status": "running",
                "liveSearchPending": True,
                "items": [
                    {
                        "sourceId": "fixture_thirdparty",
                        "sourceName": "Fixture Third Party",
                        "name": "首个结果",
                        "author": "测试作者",
                        "bookId": partial_book_id,
                    }
                ],
            }

    async def ready_wait(*_args, **_kwargs):
        return "ready"

    async def no_deadline(*_args, **_kwargs):
        return None

    service = SearchService()
    monkeypatch.setattr(subscribe_api, "_get_legado_search_service", lambda: service)
    monkeypatch.setattr(
        subscribe_api,
        "_third_party_search_source_ids",
        lambda _service: ["fixture_thirdparty"],
    )
    monkeypatch.setattr(subscribe_api, "_wait_for_legado_search", ready_wait)
    monkeypatch.setattr(subscribe_api, "_enforce_legado_search_deadline", no_deadline)
    monkeypatch.setattr(subscribe_api, "_legado_search_owners", {})
    monkeypatch.setattr(subscribe_api, "_legado_search_progress", {})
    monkeypatch.setattr(
        subscribe_api,
        "_collect_library_search_items",
        lambda **_kwargs: [],
    )

    result = await subscribe_api._legado_search_response(
        keyword="分页续搜",
        page=1,
        wait_ms=0,
        base_api="http://testserver",
        user_id="reader-early",
    )

    assert service.cancelled_job_ids == []
    assert result["liveSearchPending"] is True
    assert result["status"] == "running"
    assert result["jobId"] == Job.job_id
    assert [item["bookId"] for item in result["items"]] == [partial_book_id]


@pytest.mark.asyncio
async def test_legado_search_page2_returns_only_new_third_party_items(monkeypatch):
    import app.api.subscribe as subscribe_api
    from app.source_plugins.id_codec import encode_book_id

    first_id = encode_book_id(
        "fixture_thirdparty", "https://example.com/book/one"
    )
    second_id = encode_book_id(
        "fixture_thirdparty", "https://example.com/book/two"
    )

    class SearchService:
        @staticmethod
        def get_session(_job_id):
            class Running:
                status = "running"
                live_items = []

            return Running()

        @staticmethod
        def session_snapshot(job_id, *, base_api, include_official_sources):
            return {
                "jobId": job_id,
                "status": "running",
                "items": [
                    {
                        "sourceId": "fixture_thirdparty",
                        "name": "第一本",
                        "bookId": first_id,
                    },
                    {
                        "sourceId": "fixture_thirdparty",
                        "name": "第二本",
                        "bookId": second_id,
                    },
                ],
            }

        @staticmethod
        def cancel_job(_job_id):
            return True

    async def ready_wait(*_args, **_kwargs):
        return "ready"

    service = SearchService()
    monkeypatch.setattr(subscribe_api, "_get_legado_search_service", lambda: service)
    monkeypatch.setattr(
        subscribe_api,
        "_third_party_search_source_ids",
        lambda _service: ["fixture_thirdparty"],
    )
    monkeypatch.setattr(subscribe_api, "_wait_for_legado_search", ready_wait)
    monkeypatch.setattr(
        subscribe_api,
        "_legado_search_owners",
        {"job-continue": {"reader-page2"}},
    )
    monkeypatch.setattr(
        subscribe_api,
        "_legado_search_progress",
        {
            "reader-page2\n分页续搜\nlan": {
                "job_id": "job-continue",
                "started_at": __import__("time").time(),
                "emitted_ids": {first_id},
            }
        },
    )

    result = await subscribe_api._legado_search_response(
        keyword="分页续搜",
        page=2,
        wait_ms=0,
        base_api="http://testserver",
        user_id="reader-page2",
    )
    assert [item["bookId"] for item in result["items"]] == [second_id]
    assert result["jobId"] == "job-continue"


def test_legado_search_wait_ms_bounds(client, monkeypatch):
    import app.api.subscribe as subscribe_api

    observed: list[int] = []

    async def fake_search_response(**kwargs):
        observed.append(kwargs["wait_ms"])
        return {"items": [], "status": "completed", "liveSearchPending": False}

    monkeypatch.setattr(subscribe_api, "_legado_search_response", fake_search_response)

    default_response = client.get(
        "/api/subscribe/legado/search",
        params={"keyword": "默认超时边界"},
    )
    exact_response = client.get(
        "/api/subscribe/legado/search",
        params={"keyword": "精确超时边界", "waitMs": 120_000},
    )
    too_long_response = client.get(
        "/api/subscribe/legado/search",
        params={"keyword": "超时边界", "waitMs": 120_001},
    )

    assert default_response.status_code == 200
    assert exact_response.status_code == 200
    assert observed == [0, 120_000]
    assert too_long_response.status_code == 422
    assert too_long_response.json()["detail"] == "waitMs 超出允许范围"


def test_legado_search_prioritizes_current_reader_subscription_without_private_fields(
    client, monkeypatch
):
    import sqlite3

    import app.api.subscribe as subscribe_api
    from app.source_plugins.id_codec import encode_book_id

    service = subscribe_api.library_books_service
    published_id = "book-merged-search"
    other_published_id = "book-merged-search-other"
    unreadable_published_id = "book-merged-search-unreadable"
    _insert_book(
        service.db_path,
        published_id,
        "合并搜索测试书 已订阅",
        "聚合作者",
        published=True,
        chapter_count=1,
        visible_chapter_count=1,
    )
    _insert_book(
        service.db_path,
        other_published_id,
        "合并搜索测试书 其他共享书",
        "聚合作者",
        published=True,
        chapter_count=1,
        visible_chapter_count=1,
    )
    _insert_book(
        service.db_path,
        unreadable_published_id,
        "合并搜索测试书 尚无可读章节",
        "聚合作者",
        published=True,
        chapter_count=1,
        visible_chapter_count=0,
    )

    current_user = client.get("/api/auth/me").json()["user"]
    other_user_id = "reader-other-subscription"
    with sqlite3.connect(service.db_path) as conn:
        conn.executemany(
            """
            INSERT OR IGNORE INTO users (user_id, username, password_hash, role)
            VALUES (?, ?, 'test-hash', 'user')
            """,
            [
                (current_user["userId"], current_user["username"]),
                (other_user_id, "reader-other-subscription"),
            ],
        )
        conn.commit()
    subscribe_api.user_subscriptions_service.ensure(current_user["userId"], published_id)
    subscribe_api.user_subscriptions_service.ensure(
        current_user["userId"], unreadable_published_id
    )
    subscribe_api.user_subscriptions_service.ensure(other_user_id, other_published_id)

    class Metadata:
        id = "fixture_thirdparty"
        name = "Fixture Third Party"
        enabled = True

        @staticmethod
        def is_official_source():
            return False

    class Plugin:
        metadata = Metadata()
        capabilities = ["search", "detail", "toc", "chapter"]

    class Scheduler:
        @staticmethod
        def _enabled_plugins():
            return [Plugin()]

        @staticmethod
        def _search_priority_plugins(plugins):
            return plugins

    class Job:
        job_id = "reader-job"

    class Session:
        status = "completed"

    class SearchService:
        scheduler = Scheduler()

        @staticmethod
        def create_job(*, keyword, page, source_ids, search_mode):
            assert keyword == "合并搜索测试书"
            assert page == 1
            assert source_ids == ["fixture_thirdparty"]
            assert search_mode == "source_lan"
            return Job()

        @staticmethod
        def get_session(job_id):
            assert job_id == "reader-job"
            return Session()

        @staticmethod
        def session_snapshot(job_id, *, base_api, include_official_sources):
            assert job_id == "reader-job"
            assert include_official_sources is False
            published_book = service.page_published_books(
                keyword="合并搜索测试书",
                page=1,
                page_size=20,
            )["items"][0]
            aggregate_item = service.build_search_injected_item(
                published_book,
                base_api=base_api,
            )
            aggregate_item["score"] = 200
            third_party_book_id = encode_book_id(
                "fixture_thirdparty",
                "https://example.com/book/merged",
            )
            official_book_id = encode_book_id(
                "qidian_com_app",
                "https://m.qidian.com/book/1",
            )
            return {
                "jobId": job_id,
                "keyword": "合并搜索测试书",
                "page": 1,
                "status": "completed",
                "liveSearchPending": False,
                "items": [
                    {
                        "sourceId": "fixture_thirdparty",
                        "sourceName": "Fixture Third Party",
                        "name": "合并搜索测试书",
                        "author": "第三方作者",
                        "lastChapter": "第三章",
                        "bookId": third_party_book_id,
                        "bookUrl": "https://example.com/book/merged",
                        "rawBookUrl": "https://example.com/book/merged",
                        "score": 250,
                        "debug": {"cookie": "must-not-leak"},
                        "path": "must-not-leak",
                    },
                    aggregate_item,
                    {
                        "sourceId": "qidian_com_app",
                        "sourceName": "Official",
                        "name": "不应出现",
                        "bookId": official_book_id,
                    },
                ],
            }

    monkeypatch.setattr(subscribe_api, "_get_legado_search_service", lambda: SearchService())
    monkeypatch.setattr(subscribe_api, "_legado_search_owners", {})
    with sqlite3.connect(service.db_path) as conn:
        before_aggregate = conn.execute("SELECT COUNT(*) FROM aggregate_book_tasks").fetchone()[0]
        before_subscriptions = conn.execute("SELECT COUNT(*) FROM user_book_subscriptions").fetchone()[0]

    response = client.get(
        "/api/subscribe/legado/search",
        params={"keyword": "合并搜索测试书", "waitMs": 0},
    )

    assert response.status_code == 200
    items = response.json()["items"]
    assert [item["sourceId"] for item in items] == [
        "legadohub_ai_aggregate",
        "legadohub_ai_aggregate",
        "fixture_thirdparty",
    ]
    assert [item["aggregateBookId"] for item in items[:2]] == [
        published_id,
        other_published_id,
    ]
    assert unreadable_published_id not in {
        item.get("aggregateBookId", "") for item in items
    }
    assert items[2]["readingLastChapter"] == "Fixture Third Party · 第三章"
    assert items[2]["bookUrl"].startswith("http://testserver/api/legado/book/")
    assert "rawBookUrl" not in items[2]
    assert "debug" not in items[2]
    assert "path" not in items[2]

    other_reader_result = subscribe_api._legado_search_payload(
        keyword="合并搜索测试书",
        page=1,
        base_api="http://testserver",
        user_id=other_user_id,
        allowed_source_ids={"fixture_thirdparty"},
        snapshot=SearchService.session_snapshot(
            "reader-job",
            base_api="http://testserver",
            include_official_sources=False,
        ),
    )
    assert [item["aggregateBookId"] for item in other_reader_result["items"][:2]] == [
        other_published_id,
        published_id,
    ]
    with sqlite3.connect(service.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM aggregate_book_tasks").fetchone()[0] == before_aggregate
        assert conn.execute("SELECT COUNT(*) FROM user_book_subscriptions").fetchone()[0] == before_subscriptions


def test_legado_reads_only_published_shared_content_without_db_side_effects(
    client, tmp_path, monkeypatch
):
    import sqlite3
    import app.api.legado as legado_api

    monkeypatch.delenv("LEGADOHUB_PUBLIC_BASE_URL", raising=False)

    from app.core.legado_source import generate_legado_source
    from app.services.library_books import make_library_book_id

    db = tmp_path / "test.db"
    storage = SharedBookStorage(root=tmp_path / "library")
    service = LibraryBooksService(db_path=db, shared_book_storage=storage)
    monkeypatch.setattr("app.api.subscribe.library_books_service", service)
    monkeypatch.setattr("app.api.legado.library_books_service", service)

    async def empty_reviews(_chapter_id: str, **_kwargs) -> dict:
        return {
            "authorReviews": [],
            "chapterEnd": [],
            "hotParagraphReviews": [],
            "summary": {"totalReviews": 0, "chapterEndCount": 0},
        }

    monkeypatch.setattr(legado_api, "_chapter_reviews", empty_reviews)

    hidden_id = "book-reading-hidden"
    published_id = "book-reading-published"
    _insert_book(db, hidden_id, "阅读契约测试书隐藏", "作者")
    _insert_book(
        db,
        published_id,
        "阅读契约测试书",
        "作者",
        published=True,
        chapter_count=4,
        visible_chapter_count=3,
    )
    _write_shared_files(
        storage,
        published_id,
        "阅读契约测试书",
        "作者",
        chapter_specs=[
            (1, "第一章", False),
            (2, "第二章", False),
            (3, "第三章", False),
            (4, "第四章 付费", True),
        ],
        include_source_ids=False,
    )
    from app.services.aggregate_virtual_source import (
        VIRTUAL_SOURCE_ID,
        make_aggregate_chapter_url,
    )
    from app.source_plugins.id_codec import encode_chapter_id

    with sqlite3.connect(db) as conn:
        for index, title in enumerate(("第一章", "第二章", "第三章", "第四章 付费"), start=1):
            source_chapter_id = f"src:ch{index}"
            aggregate_url = make_aggregate_chapter_url(
                aggregate_book_id=published_id,
                source_chapter_id=source_chapter_id,
                title=title,
                index=index,
            )
            conn.execute(
                """
                INSERT INTO aggregate_chapter_tasks (
                    chapter_id, aggregate_book_id, source_chapter_id,
                    chapter_index, title, status, preview_only, volume_name
                ) VALUES (?, ?, ?, ?, ?, 'processed', ?, ?)
                """,
                (
                    encode_chapter_id(VIRTUAL_SOURCE_ID, aggregate_url),
                    published_id,
                    source_chapter_id,
                    index,
                    title,
                    int(index == 4),
                    "第一卷" if index <= 2 else "第二卷",
                ),
            )
        conn.execute(
            """
            UPDATE aggregate_chapter_tasks
            SET last_processed_at = '2026-07-21T03:28:03.696059+00:00'
            WHERE aggregate_book_id = ? AND chapter_index = 1
            """,
            (published_id,),
        )
        conn.commit()

    source = generate_legado_source("http://testserver")[0]
    max_source = generate_legado_source("http://testserver", reader="max")[0]
    assert len(generate_legado_source("http://testserver")) == 1
    assert max_source["bookSourceUrl"] == "LegadoHub-LAN-Max"
    assert "·Max" in max_source["bookSourceName"]
    assert "Legado-Max" in max_source["bookSourceGroup"]
    assert "reviewBubbles=1" in max_source["ruleContent"]["content"]
    assert "reviewBubbles=1" not in source["ruleContent"]["content"]
    assert "chapterComment" not in max_source["ruleContent"]
    assert max_source["loginUrl"] == source["loginUrl"]
    assert source["searchUrl"].startswith("@js:")
    assert 'var _base = "http://testserver"' in source["searchUrl"]
    assert "/api/subscribe/legado/search" in source["searchUrl"]
    assert "waitMs" not in source["searchUrl"]
    assert source["exploreUrl"].startswith("已发布书库::@js:")
    assert "/api/subscribe/legado/explore" in source["exploreUrl"]
    assert source["ruleToc"]["isVip"] == "$.isVip"
    assert source["ruleToc"]["isPay"] == "$.isPay"
    assert "isVolume" not in source["ruleToc"]
    assert "isVolume" in max_source["ruleToc"]
    assert "reader=max" in max_source["ruleBookInfo"]["tocUrl"]
    assert "result.isVolume" in max_source["ruleToc"]["chapterUrl"]
    assert "reader=max" not in source["ruleBookInfo"]["tocUrl"]
    login_ui = json.loads(source["loginUi"])
    assert [item["name"] for item in login_ui] == ["订阅", "书库"]
    assert "/api/auth/access/redeem" in source["loginUrl"]
    assert "/api/auth/access/me" in source["loginUrl"]
    assert "/api/auth/access/logout" in source["loginUrl"]
    assert 'typeof result !== "undefined"' in source["loginUrl"]
    assert source["loginUrl"].index("var mapped = info.get(name)") < source["loginUrl"].index("info.containsKey(name)")
    assert source["loginUrl"].index("var direct = info[name]") < source["loginUrl"].index("info.containsKey(name)")
    assert "typeof info.get" not in source["loginUrl"]
    assert "info.containsKey(name)" in source["loginUrl"]
    assert "function login()" in source["loginUrl"]
    assert "source.putLoginInfo(\"{}\")" in source["loginUrl"]
    assert "java.ajax(LEGADOHUB_BASE + path" in source["loginUrl"]
    assert "response.body()" not in source["loginUrl"]
    assert "response.code()" not in source["loginUrl"]
    assert "java.log" not in source["loginUrl"]
    assert "data:contentUrl;base64" in source["ruleToc"]["chapterUrl"]
    assert "type: 'legadoHub'" in source["ruleToc"]["chapterUrl"]
    assert "qingci" not in source["ruleToc"]["chapterUrl"].lower()
    assert "java.hexDecodeToString(payload)" in source["ruleContent"]["content"]
    assert "legadoHubAjax(contentUrl)" in source["ruleContent"]["content"]
    assert "java.ajax(contentUrl)" in source["ruleContent"]["content"]
    assert 'legadoHubReviewRoot(contentUrl) + "/reviews"' not in source["ruleContent"]["content"]
    assert "hasReaderCapability" not in source["ruleContent"]["content"]
    assert "legadoHubDecorateChapterReviewOnly" not in source["jsLib"]
    assert "legadoHubDecorateReviews" not in source["jsLib"]
    assert "function legadoHubChapterEndReviewCount" in source["jsLib"]
    assert "function legadoHubSourceBase" in source["jsLib"]
    assert "function legadoHubRewriteApiUrl" in source["jsLib"]
    assert "legadoHubRewriteApiUrl(contentUrl)" in source["ruleContent"]["content"]
    assert "legadoHubRewriteApiUrl(contentUrl)" in source["ruleToc"]["chapterUrl"]
    chapter_comment = source["ruleContent"]["chapterComment"]
    assert chapter_comment["protocolVersion"] == 2
    assert chapter_comment["display"]["segment"]["enabled"] is True
    assert chapter_comment["display"]["segment"]["preset"] == "count"
    assert chapter_comment["display"]["page"]["enabled"] is True
    assert chapter_comment["display"]["chapter"]["enabled"] is True
    assert "matchedParagraphIndex" in chapter_comment["data"]
    assert "matchedParagraphCount" in chapter_comment["data"]
    assert "pageEligible: true" in chapter_comment["data"]
    assert "chapterEndHot" in chapter_comment["data"]
    assert "chapterHot.concat(chapterEnd)" in chapter_comment["data"]
    assert "authorReviews" in chapter_comment["data"]
    assert "badge: '作家说'" in chapter_comment["data"]
    assert "author: author" in chapter_comment["data"]
    assert "version: 2" in chapter_comment["data"]
    assert "version: 1" not in chapter_comment["data"]
    assert "preview:" not in chapter_comment["data"]
    assert "previews: chapterPreviews" in chapter_comment["data"]
    assert "chapterPreviews.length >= 3" in chapter_comment["data"]
    assert "var key = '$' + String" in chapter_comment["data"]
    assert "legadoHubChapterEndReviewCount(reviews)" in chapter_comment["data"]
    assert chapter_comment["url"].startswith("@js:")
    assert chapter_comment["data"].startswith("@js:")
    assert chapter_comment["action"].startswith("@js:")
    assert "commentScope === 'page'" in chapter_comment["action"]
    assert "commentScope === 'segment'" in chapter_comment["action"]
    assert "commentScope === 'chapter'" in chapter_comment["action"]
    assert "chapter.getAbsoluteURL" in chapter_comment["action"]
    assert "typeof baseCandidate !== 'function'" in chapter_comment["action"]
    assert "legadoHubRewriteApiUrl(contentUrl)" in chapter_comment["action"]
    assert "legadoHubRewriteApiUrl(contentUrl)" in chapter_comment["url"]
    assert "sourceWebView" in chapter_comment["action"]
    assert "paragraphIds=" in chapter_comment["action"]
    assert "Authorization" not in chapter_comment["action"]
    assert "legadoHubPageHotReviewEntry" not in source["jsLib"]
    assert "legadoHubReviewTheme" not in source["jsLib"]
    assert "legadoHubOpenReviews" not in source["jsLib"]
    assert "legadohub_session" not in source["jsLib"]
    assert "heightPercentage: 0.78" not in source["jsLib"]
    assert "heightRatio: 0.78" in chapter_comment["action"]
    assert "ruleReview" not in source
    assert "LH1." not in json.dumps(source, ensure_ascii=False)

    search = client.get("/api/subscribe/legado/search", params={"keyword": "阅读契约测试书"})
    assert search.status_code == 200
    search_data = search.json()
    assert search_data["status"] == "completed"
    assert search_data["jobId"] == ""
    assert [item["aggregateBookId"] for item in search_data["items"]] == [published_id]
    assert "addedByUserId" not in search_data["items"][0]
    assert "addedByUsername" not in search_data["items"][0]

    book_id = make_library_book_id(published_id)
    with sqlite3.connect(db) as conn:
        before = {
            "books": conn.execute("SELECT COUNT(*) FROM aggregate_book_tasks").fetchone()[0],
            "chapters": conn.execute("SELECT COUNT(*) FROM aggregate_chapter_tasks").fetchone()[0],
            "logs": conn.execute("SELECT COUNT(*) FROM aggregate_operation_logs").fetchone()[0],
        }

    detail = client.get(f"/api/legado/book/{book_id}")
    assert detail.status_code == 200
    detail_data = detail.json()["data"]
    assert detail_data["name"] == "阅读契约测试书"
    assert "primaryBookUrl" not in detail_data
    assert "sources" not in detail_data

    original_read_text = Path.read_text
    markdown_reads: list[Path] = []

    def tracked_read_text(path: Path, *args, **kwargs):
        if path.suffix == ".md":
            markdown_reads.append(path)
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", tracked_read_text)
    delivery = service.subscription_delivery_summary(
        published_id,
        start_chapter_index=1,
    )
    assert markdown_reads == []
    assert delivery["personalProgress"]["fullCount"] == 3
    assert delivery["personalProgress"]["previewCount"] == 1
    assert delivery["provisioning"]["state"] == "ready"
    assert delivery["provisioning"]["firstReadableChapter"]["chapterIndex"] == 1
    assert delivery["provisioning"]["firstReadableChapter"]["contentAccess"] == "full"

    toc = client.get(f"/api/legado/book/{book_id}/toc")
    assert toc.status_code == 200
    assert markdown_reads == []
    chapters = toc.json()["chapters"]
    assert len(chapters) == 4
    assert chapters[0]["isVip"] is False
    assert chapters[0]["isPay"] is False
    assert chapters[0]["previewOnly"] is False
    assert chapters[0]["updateTime"] == "2026-07-21 03:28"
    assert chapters[-1]["isVip"] is True
    assert chapters[-1]["isPay"] is False
    assert chapters[-1]["previewOnly"] is True

    max_toc = client.get(f"/api/legado/book/{book_id}/toc?reader=max")
    assert max_toc.status_code == 200
    assert [item["title"] for item in max_toc.json()["chapters"]] == [
        "第一卷", "第一章", "第二章", "第二卷", "第三章", "第四章 付费",
    ]
    assert [item["chapterUrl"] for item in max_toc.json()["chapters"] if item.get("isVolume")] == ["", ""]

    free = client.get(chapters[0]["chapterUrl"].replace("http://testserver", ""))
    assert free.status_code == 200
    assert "完整免费正文" in free.json()["content"]
    assert free.json()["extra"]["contentAccess"] == "full"
    assert free.json()["isVip"] is False
    assert len(markdown_reads) == 1

    async def one_review(_chapter_id: str, **_kwargs) -> dict:
        return {
            "hotParagraphReviews": [{
                "paragraphId": 12,
                "matchedParagraphIndex": 0,
                "matchedParagraphCount": 1,
                "matchedText": "完整免费正文",
                "commentCount": 7,
            }],
            "chapterEnd": [],
            "chapterEndHot": [],
            "summary": {},
        }

    monkeypatch.setattr(legado_api, "_chapter_reviews", one_review)
    max_free = client.get(
        chapters[0]["chapterUrl"].replace("http://testserver", ""),
        params={"reviewBubbles": "1"},
    )
    assert max_free.status_code == 200
    assert "bubble://paragraph?num=7&status=normal&v=1" in max_free.json()["content"]
    assert "paragraphId=12" in max_free.json()["content"]
    assert "bubble://" not in free.json()["content"]
    assert client.get(
        chapters[0]["chapterUrl"].replace("http://testserver", ""),
        params={"reviewBubbles": "wrong"},
    ).status_code == 422
    monkeypatch.setattr(legado_api, "_chapter_reviews", empty_reviews)

    preview = client.get(chapters[-1]["chapterUrl"].replace("http://testserver", ""))
    assert preview.status_code == 200
    assert "付费预览正文" in preview.json()["content"]
    assert preview.json()["extra"]["contentAccess"] == "preview"
    assert preview.json()["isVip"] is True
    assert preview.json()["isPay"] is False
    assert preview.json()["extra"]["previewOnly"] is True
    assert preview.json()["previewOnly"] is True
    assert len(markdown_reads) == 3

    with sqlite3.connect(db) as conn:
        after = {
            "books": conn.execute("SELECT COUNT(*) FROM aggregate_book_tasks").fetchone()[0],
            "chapters": conn.execute("SELECT COUNT(*) FROM aggregate_chapter_tasks").fetchone()[0],
            "logs": conn.execute("SELECT COUNT(*) FROM aggregate_operation_logs").fetchone()[0],
        }
    assert after == before

    hidden_book_id = make_library_book_id(hidden_id)
    assert client.get(f"/api/legado/book/{hidden_book_id}").status_code == 404


def test_known_official_free_short_chapter_is_not_misclassified_as_preview():
    from app.services.aggregate_alignment import classify_source_content

    result = classify_source_content(
        "欢迎收藏本书",
        source_id="qidian_com_app",
        is_official=True,
        preview_only_hint=True,
        is_paid=True,
        is_vip=False,
    )

    assert result["classification"] == "full"


def test_shared_chapter_strips_only_matching_leading_source_title():
    from app.services.library_books import strip_leading_duplicate_chapter_title

    body = "第477章 看不见的手\n\n短暂晕眩后，贝塔表现出强烈的食欲。"
    assert strip_leading_duplicate_chapter_title(body, "第234章 看不见的手") == body.split("\n\n", 1)[1]
    assert strip_leading_duplicate_chapter_title("第234章 看不见的手\n\n正文", "第234章 看不见的手") == "正文"
    assert strip_leading_duplicate_chapter_title(body, "第234章 另一只手") == body
    assert strip_leading_duplicate_chapter_title("正文\n\n第477章 看不见的手", "第234章 看不见的手") == "正文\n\n第477章 看不见的手"


def test_generated_chapter_rule_never_returns_encoded_url_on_failure():
    from app.core.legado_source import generate_legado_source

    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required to execute generated reader rules")

    harness = r"""
const fs = require('fs');
const input = JSON.parse(fs.readFileSync(0, 'utf8'));
const calls = [];
const java = {
  hexDecodeToString: value => Buffer.from(value, 'hex').toString('utf8'),
  ajax: value => {
    calls.push(['direct', value]);
    if (input.directError) throw new Error(input.directError);
    return input.directResponse;
  }
};
function legadoHubRewriteApiUrl(value) { return value; }
function legadoHubAjax(value) {
  calls.push(['helper', value]);
  if (input.helperError) throw new Error(input.helperError);
  return input.response;
}
let result = Buffer.from(input.url).toString('hex');
try {
  eval(input.rule.slice(4));
  process.stdout.write(JSON.stringify({ ok: true, result, calls }));
} catch (error) {
  process.stdout.write(JSON.stringify({ ok: false, error: String(error), calls }));
}
"""
    url = "https://books.example.test/api/legado/chapter/encoded-id"
    sources = (
        generate_legado_source("https://books.example.test")[0],
        generate_legado_source("https://books.example.test", reader="max")[0],
    )
    for reader, source in enumerate(sources):
        rule = source["ruleContent"]["content"]

        def run_rule(
            *, response: str = "", helper_error: str = "",
            direct_response: str = "", direct_error: str = "",
        ) -> dict:
            completed = subprocess.run(
                [node, "-e", harness],
                input=json.dumps({
                    "rule": rule, "url": url, "response": response,
                    "helperError": helper_error, "directResponse": direct_response,
                    "directError": direct_error,
                }),
                text=True,
                encoding="utf-8",
                capture_output=True,
                check=True,
            )
            return json.loads(completed.stdout)

        success = run_rule(response=json.dumps({"content": "第一段\n\n第二段"}))
        assert success["ok"] is True
        assert success["result"] == "第一段<br><br>第二段"
        chapter_url = url + ("?reviewBubbles=1" if reader else "")
        assert success["calls"] == [["helper", chapter_url]]

        fallback = run_rule(
            helper_error="java.ajax is not a function",
            direct_response=json.dumps({"content": "回退正文"}),
        )
        assert fallback["ok"] is True
        assert fallback["result"] == "回退正文"
        assert fallback["calls"] == [["helper", chapter_url], ["direct", chapter_url]]

        for response, expected in (
            ("not-json", "章节接口返回无效 JSON"),
            (json.dumps({"detail": "Unauthorized"}), "章节接口未返回正文: Unauthorized"),
            (json.dumps({"content": ""}), "章节正文为空"),
        ):
            failed = run_rule(response=response)
            assert failed["ok"] is False
            assert expected in failed["error"]
            assert "68747470" not in failed["error"]

        failed_requests = run_rule(helper_error="temporary timeout", direct_error="still offline")
        assert failed_requests["ok"] is False
        assert "章节请求失败" in failed_requests["error"]
        assert "68747470" not in failed_requests["error"]


def test_legado_max_bubbles_preserve_paragraphs_and_click_targets():
    import base64
    import re
    from xml.etree import ElementTree

    from app.services.legado_max_bubbles import decorate_legado_max_content

    view_url = "https://books.example.test/api/legado/chapter/source:chapter/reviews/view"
    reviews = {
        "hotParagraphReviews": [
            {"paragraphId": 11, "matchedParagraphIndex": 0, "matchedParagraphCount": 1,
             "matchedText": "第一段", "commentCount": 7},
            {"paragraphId": 25, "matchedParagraphIndex": 1, "matchedParagraphCount": 2,
             "matchedText": "第二段\n\n第三段", "commentCount": 3},
            {"paragraphId": 99, "matchedParagraphIndex": 8,
             "matchedText": "未匹配", "commentCount": 9},
        ],
        "chapterEndHot": [
            {"id": "first", "userName": "甲", "content": "<b>第一条</b>评论"},
            {"id": "second", "userName": "乙", "content": "第二条评论"},
        ],
        "chapterEnd": [
            {"id": "second", "userName": "乙", "content": "第二条评论"},
            {"id": "third", "userName": "丙", "content": "第三条评论"},
        ],
        "summary": {"chapterEndCount": 4},
    }
    body = "# 标题\n<p><b>第一段</b></p><p>第二段</p><p>第三段</p>"
    result = decorate_legado_max_content(body, reviews, view_url=view_url)
    assert "<b>第一段</b><img" in result
    assert "<p>第二段</p><p>第三段<img" in result
    assert "paragraphId=11" in result
    assert "paragraphId=25" in result
    assert "paragraphId=99" not in result
    assert "本章说 <img" not in result
    assert "data:image/svg+xml;base64," in result
    img_pattern = re.compile(r'<img[^>]*src="([^"]*(?:"[^>]+\})?)"[^>]*>')
    sources = img_pattern.findall(result)
    assert len(sources) == 3
    for src in sources[:2]:
        bubble_url, options = src.split(",{", 1)
        assert bubble_url.startswith("bubble://paragraph?num=")
        assert json.loads("{" + options)["style"] == "TEXT"
        assert "java.showBrowser" in json.loads("{" + options)["click"]
    card_url, card_options = sources[2].split(",{", 1)
    assert json.loads("{" + card_options)["style"] == "FULL"
    assert "?tab=chapter" in json.loads("{" + card_options)["click"]
    svg = base64.b64decode(card_url.removeprefix("data:image/svg+xml;base64,"))
    texts = [element.text for element in ElementTree.fromstring(svg).iter() if element.tag.endswith("text")]
    assert "本章说" in texts
    assert "4 条评论  >" in texts
    assert any("甲：第一条 评论" in text for text in texts)
    assert any("乙：第二条评论" in text for text in texts)
    assert any("丙：第三条评论" in text for text in texts)
    assert decorate_legado_max_content(body, {"hotParagraphReviews": []}, view_url=view_url) == body


def test_subscribe_candidate_response_does_not_expose_private_primary_urls(client, monkeypatch):
    import app.api.subscribe as subscribe_api

    created_payload = {
        "created": True,
        "payload": {"name": "测试书", "author": "作者", "sources": []},
        "book": {
            "aggregateBookId": "book-new-1",
            "coverUrl": "",
            "name": "测试书",
            "author": "作者",
            "intro": "简介",
            "bookStatus": "ongoing",
            "totalChaptersAtSubscribe": 10,
            "startChapterIndex": 1,
            "autoArchiveOnComplete": True,
            "primarySourceId": "src",
            "primarySourceName": "来源",
            "primaryBookId": "src:book",
            "primaryBookUrl": "https://private.example/book",
            "primaryTocUrl": "https://private.example/toc",
            "settingsJson": "{}",
        },
    }

    monkeypatch.setattr(
        subscribe_api.subscription_search_service,
        "find_card_group_for_user",
        lambda _job_id, _candidate_id, _user_id: {"candidateId": "cand-1", "items": [{}]},
    )
    async def fake_create_or_get_shared_book(*args, **kwargs):
        return created_payload

    monkeypatch.setattr(
        subscribe_api.library_books_service,
        "create_or_get_shared_book",
        fake_create_or_get_shared_book,
    )
    monkeypatch.setattr(
        subscribe_api.library_books_service,
        "find_existing_book",
        lambda _group: None,
    )
    monkeypatch.setattr(
        subscribe_api.user_subscriptions_service,
        "check_capacity",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        subscribe_api.user_subscriptions_service,
        "ensure",
        lambda *args, **kwargs: (
            {
                "userId": "admin",
                "aggregateBookId": "book-new-1",
                "status": "active",
                "startChapterIndex": 1,
                "autoArchiveOnComplete": True,
                "createdAt": "",
                "updatedAt": "",
            },
            True,
        ),
    )
    monkeypatch.setattr(
        subscribe_api.AggregateProcessor,
        "enqueue_book",
        lambda self, aggregate_book_id, payload, **kwargs: {"queued": True, "bookId": aggregate_book_id},
    )
    monkeypatch.setattr(
        subscribe_api.SharedBookScheduler,
        "enqueue_initial_subscription",
        lambda self, aggregate_book_id, **kwargs: {"queued": True, "bookId": aggregate_book_id},
    )
    monkeypatch.setattr(
        subscribe_api.SharedBookScheduler,
        "run_periodic_once",
        lambda self, **kwargs: None,
    )
    monkeypatch.setattr(
        subscribe_api.asyncio,
        "create_task",
        lambda coro: coro.close(),
    )

    response = client.post(
        "/api/subscribe/search/job-1/cards/cand-1/subscribe",
        json={"startChapterIndex": 1, "autoArchiveOnComplete": True},
    )

    assert response.status_code == 200
    data = response.json()
    assert "primaryBookUrl" not in data["book"]
    assert "primaryTocUrl" not in data["book"]
    assert "settingsJson" not in data["book"]
    config = data["subscriptionConfig"]
    assert "primaryBookUrl" not in config
    assert "primaryTocUrl" not in config
    assert "primarySourceId" not in config
    assert "primaryBookId" not in config
    assert "supplementSourceConfig" not in config
    assert data["provisioning"]["state"] in {"processing", "error"}
    assert data["provisioning"]["firstReadableChapter"] is None
    assert data["processingWakeRequested"] is True


def test_subscription_activation_wakes_unready_book_but_repeated_active_is_idempotent(
    client, tmp_path, monkeypatch
):
    import sqlite3

    import app.api.subscribe as subscribe_api

    book_id = "book-subscription-wake"
    db = tmp_path / "test.db"
    _insert_book(db, book_id, "订阅唤醒测试书", "作者")
    user = client.get("/api/auth/me").json()["user"]
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO users (user_id, username, password_hash, role)
            VALUES (?, ?, 'test-hash', 'admin')
            """,
            (user["userId"], user["username"]),
        )
        conn.commit()
    wake_calls: list[tuple[str, str]] = []

    def fake_schedule(book, *, payload, provisioning):
        wake_calls.append((book["aggregateBookId"], provisioning["state"]))
        return True

    monkeypatch.setattr(subscribe_api, "_schedule_subscription_wake", fake_schedule)

    created = client.put(
        f"/api/subscribe/books/{book_id}/subscription",
        json={"startChapterIndex": 1, "autoArchiveOnComplete": True},
    )
    repeated = client.put(
        f"/api/subscribe/books/{book_id}/subscription",
        json={"startChapterIndex": 1, "autoArchiveOnComplete": True},
    )
    paused = client.patch(
        f"/api/subscribe/books/{book_id}/subscription",
        json={"status": "paused"},
    )
    resumed = client.patch(
        f"/api/subscribe/books/{book_id}/subscription",
        json={"status": "active"},
    )

    assert created.status_code == 200
    assert created.json()["processingWakeRequested"] is True
    assert created.json()["provisioning"]["state"] == "processing"
    assert repeated.status_code == 200
    assert repeated.json()["processingWakeRequested"] is False
    assert paused.status_code == 200
    assert paused.json()["processingWakeRequested"] is False
    assert resumed.status_code == 200
    assert resumed.json()["processingWakeRequested"] is True
    assert wake_calls == [
        (book_id, "processing"),
        (book_id, "processing"),
    ]


def test_reader_access_requires_own_subscription(client, tmp_path):
    import sqlite3

    from fastapi.testclient import TestClient

    import app.api.subscribe as subscribe_api
    from app.services.user_auth import auth_service

    db = tmp_path / "test.db"
    book_id = "book-owner-scope"
    _insert_book(db, book_id, "归属测试书", "作者")

    username = "reader-owner-scope"
    user = auth_service.get_user_by_username(username)
    if not user:
        user = auth_service.create_user(username, "reader-password", role="user")
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO users (user_id, username, password_hash, role)
            VALUES (?, ?, 'test-hash', 'user')
            """,
            (user["userId"], username),
        )
        conn.commit()
    reader = TestClient(app)
    login = reader.post(
        "/api/auth/access/redeem",
        json={
            "accessCode": auth_service.build_access_code(username, "reader-password")
        },
    )
    assert login.status_code == 200

    assert reader.get(f"/api/subscribe/books/{book_id}").status_code == 404
    assert reader.get("/api/subscribe/library").status_code == 403

    subscribe_api.user_subscriptions_service.ensure(user["userId"], book_id)
    response = reader.get(f"/api/subscribe/books/{book_id}")
    assert response.status_code == 200
    assert response.json()["subscription"]["userId"] == user["userId"]
    mine = reader.get("/api/subscribe/library/mine")
    assert mine.status_code == 200
    mine_book = mine.json()["items"][0]
    assert mine_book["aggregateBookId"] == book_id
    assert "primaryBookUrl" not in mine_book
    assert "primaryTocUrl" not in mine_book
    assert "settingsJson" not in mine_book
    assert "addedByUserId" not in mine_book
    assert reader.get(f"/api/console/library-books/{book_id}/summary").status_code == 403
    assert reader.get(f"/api/console/library-books/{book_id}/chapters").status_code == 403
    assert reader.get(f"/api/console/library-books/{book_id}/logs").status_code == 403


def test_subscription_routes_keep_each_reader_on_their_own_relationship(client, tmp_path):
    import app.api.subscribe as subscribe_api

    db = tmp_path / "test.db"
    storage = SharedBookStorage(root=tmp_path / "library")
    book_id = "book-owner-complete"
    _insert_book(db, book_id, "完整归属测试书", "作者")
    _write_shared_files(storage, book_id, "完整归属测试书", "作者")

    reader_a, user_a = _reader_client(db, "reader-owner-complete-a")
    reader_b, user_b = _reader_client(db, "reader-owner-complete-b")
    subscribe_api.user_subscriptions_service.ensure(
        user_a["userId"],
        book_id,
        start_chapter_index=2,
        auto_archive_on_complete=False,
    )

    search = reader_a.post("/api/subscribe/search", json={"keyword": "归属测试", "page": 1})
    assert search.status_code == 200
    job_id = search.json()["jobId"]
    assert job_id
    assert reader_b.get(f"/api/subscribe/search/{job_id}").status_code == 404

    chapters = reader_a.get(f"/api/subscribe/books/{book_id}/chapters")
    assert chapters.status_code == 200
    chapter_id = chapters.json()["items"][0]["readChapterId"]
    chapter_body = reader_a.get(f"/api/subscribe/chapters/{chapter_id}")
    assert chapter_body.status_code == 200
    assert "完整免费正文" in chapter_body.json()["content"]
    assert chapter_body.json()["extra"]["contentAccess"] == "full"

    assert reader_b.get(f"/api/subscribe/books/{book_id}").status_code == 404
    assert reader_b.get(f"/api/subscribe/books/{book_id}/chapters").status_code == 404
    assert reader_b.get(f"/api/subscribe/books/{book_id}/subscription").status_code == 404
    assert reader_b.patch(
        f"/api/subscribe/books/{book_id}/subscription",
        json={"startChapterIndex": 7},
    ).status_code == 404
    assert reader_b.get(f"/api/subscribe/chapters/{chapter_id}").status_code == 404

    own_subscription = reader_b.put(
        f"/api/subscribe/books/{book_id}/subscription",
        json={"startChapterIndex": 7, "autoArchiveOnComplete": True},
    )
    assert own_subscription.status_code == 200
    assert own_subscription.json()["subscription"]["userId"] == user_b["userId"]
    assert own_subscription.json()["subscription"]["startChapterIndex"] == 7
    assert reader_b.get(f"/api/subscribe/books/{book_id}").status_code == 200
    assert reader_b.get(f"/api/subscribe/books/{book_id}/chapters").status_code == 200
    assert reader_b.get(f"/api/subscribe/chapters/{chapter_id}").status_code == 200

    subscription_a = reader_a.get(f"/api/subscribe/books/{book_id}/subscription").json()["subscription"]
    assert subscription_a["userId"] == user_a["userId"]
    assert subscription_a["startChapterIndex"] == 2
    assert subscription_a["autoArchiveOnComplete"] is False


def test_subscription_api_responses_exclude_private_fields(client, tmp_path):
    import app.api.subscribe as subscribe_api

    db = tmp_path / "test.db"
    storage = SharedBookStorage(root=tmp_path / "library")
    book_id = "book-private-response-scan"
    _insert_book(db, book_id, "安全响应测试书", "作者")
    _write_shared_files(storage, book_id, "安全响应测试书", "作者")
    reader, user = _reader_client(db, "reader-private-response-scan")
    subscribe_api.user_subscriptions_service.ensure(user["userId"], book_id)

    responses = [
        reader.get("/api/subscribe/library/mine"),
        reader.get(f"/api/subscribe/books/{book_id}"),
        reader.get(f"/api/subscribe/books/{book_id}/chapters"),
        reader.get(f"/api/subscribe/books/{book_id}/subscription"),
        reader.put(
            f"/api/subscribe/books/{book_id}/subscription",
            json={"startChapterIndex": 1, "autoArchiveOnComplete": True},
        ),
        reader.patch(
            f"/api/subscribe/books/{book_id}/subscription",
            json={"startChapterIndex": 2},
        ),
        reader.post("/api/subscribe/search", json={"keyword": "安全响应", "page": 1}),
    ]
    chapter_id = responses[2].json()["items"][0]["readChapterId"]
    responses.append(reader.get(f"/api/subscribe/chapters/{chapter_id}"))

    for response in responses:
        assert response.status_code == 200, response.text
        _assert_no_private_response_fields(response.json())


def test_repeated_subscription_patch_is_a_noop_and_does_not_consume_rate_limit(
    client, tmp_path, monkeypatch
):
    import sqlite3
    from types import SimpleNamespace

    import app.api.subscribe as subscribe_api
    from app.core.app_config import AppConfig

    db = tmp_path / "test.db"
    book_id = "book-repeat-patch"
    _insert_book(db, book_id, "重复 PATCH 测试书", "作者")
    user_id = client.get("/api/auth/me").json()["user"]["userId"]
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO users (user_id, username, password_hash, role)
            VALUES (?, 'repeat-patch-admin', 'test-hash', 'admin')
            """,
            (user_id,),
        )
        conn.commit()
    subscribe_api.user_subscriptions_service.ensure(user_id, book_id)
    limits = SimpleNamespace(
        max_active_per_user=100,
        max_new_shared_books_per_day=10,
        max_global_provisioning_books=20,
        rate_limit_window_seconds=60,
        search_rate_limit_per_window=30,
        create_rate_limit_per_window=10,
        update_rate_limit_per_window=1,
    )
    monkeypatch.setattr(
        AppConfig,
        "get",
        classmethod(lambda _cls: SimpleNamespace(subscription=limits)),
    )
    subscribe_api.subscription_rate_limiter.reset()

    changed = client.patch(
        f"/api/subscribe/books/{book_id}/subscription",
        json={"startChapterIndex": 3, "autoArchiveOnComplete": False},
    )
    assert changed.status_code == 200
    with sqlite3.connect(db) as conn:
        counts_after_change = {
            "operations": conn.execute(
                "SELECT COUNT(*) FROM aggregate_operation_logs WHERE aggregate_book_id = ?",
                (book_id,),
            ).fetchone()[0],
            "audits": conn.execute(
                "SELECT COUNT(*) FROM audit_events WHERE target_id = ?",
                (f"{user_id}:{book_id}",),
            ).fetchone()[0],
        }

    repeated = client.patch(
        f"/api/subscribe/books/{book_id}/subscription",
        json={"startChapterIndex": 3, "autoArchiveOnComplete": False},
    )
    assert repeated.status_code == 200
    assert repeated.json() == changed.json()
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM aggregate_operation_logs WHERE aggregate_book_id = ?",
            (book_id,),
        ).fetchone()[0] == counts_after_change["operations"]
        assert conn.execute(
            "SELECT COUNT(*) FROM audit_events WHERE target_id = ?",
            (f"{user_id}:{book_id}",),
        ).fetchone()[0] == counts_after_change["audits"]


def test_subscription_inputs_reject_invalid_types_with_422(client, tmp_path, monkeypatch):
    import sqlite3

    import app.api.subscribe as subscribe_api

    db = tmp_path / "test.db"
    book_id = "book-invalid-subscription-input"
    _insert_book(db, book_id, "非法订阅输入测试书", "作者")
    user_id = client.get("/api/auth/me").json()["user"]["userId"]
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO users (user_id, username, password_hash, role)
            VALUES (?, 'invalid-input-admin', 'test-hash', 'admin')
            """,
            (user_id,),
        )
        conn.commit()

    monkeypatch.setattr(
        subscribe_api.subscription_search_service,
        "find_card_group_for_user",
        lambda _job_id, _candidate_id, _user_id: {"candidateId": "candidate-invalid"},
    )
    candidate = client.post(
        "/api/subscribe/search/job-invalid/cards/candidate-invalid/subscribe",
        json={"startChapterIndex": "abc"},
    )
    assert candidate.status_code == 422
    assert candidate.json()["detail"] == "startChapterIndex 必须是大于等于 1 的整数"

    put = client.put(
        f"/api/subscribe/books/{book_id}/subscription",
        json={"startChapterIndex": 1.5},
    )
    assert put.status_code == 422
    assert put.json()["detail"] == "startChapterIndex 必须是大于等于 1 的整数"

    subscribe_api.user_subscriptions_service.ensure(user_id, book_id)
    patch = client.patch(
        f"/api/subscribe/books/{book_id}/subscription",
        json={"autoArchiveOnComplete": "false"},
    )
    assert patch.status_code == 422
    assert patch.json()["detail"] == "autoArchiveOnComplete 必须是布尔值"


def test_concurrent_subscription_api_requests_respect_user_quota(client, tmp_path, monkeypatch):
    import sqlite3
    from types import SimpleNamespace

    import app.api.subscribe as subscribe_api
    from app.core.app_config import AppConfig

    db = tmp_path / "test.db"
    first_book_id = "book-api-quota-a"
    second_book_id = "book-api-quota-b"
    _insert_book(db, first_book_id, "并发配额书 A", "作者")
    _insert_book(db, second_book_id, "并发配额书 B", "作者")
    user_id = client.get("/api/auth/me").json()["user"]["userId"]
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO users (user_id, username, password_hash, role)
            VALUES (?, 'api-quota-admin', 'test-hash', 'admin')
            """,
            (user_id,),
        )
        conn.commit()
    limits = SimpleNamespace(
        max_active_per_user=1,
        max_new_shared_books_per_day=10,
        max_global_provisioning_books=20,
        rate_limit_window_seconds=60,
        search_rate_limit_per_window=30,
        create_rate_limit_per_window=10,
        update_rate_limit_per_window=60,
    )
    second_client = TestClient(app)
    assert second_client.post(
        "/api/auth/login", json={"username": "admin", "password": "admin123"}
    ).status_code == 200
    monkeypatch.setattr(
        AppConfig,
        "get",
        classmethod(lambda _cls: SimpleNamespace(subscription=limits)),
    )
    subscribe_api.subscription_rate_limiter.reset()

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                request_client.put,
                f"/api/subscribe/books/{book_id}/subscription",
                json={"startChapterIndex": 1, "autoArchiveOnComplete": True},
            )
            for request_client, book_id in (
                (client, first_book_id),
                (second_client, second_book_id),
            )
        ]
        responses = [future.result(timeout=10) for future in futures]

    assert sorted(response.status_code for response in responses) == [200, 429]
    limited = next(response for response in responses if response.status_code == 429)
    assert limited.json()["detail"]["code"] == "subscription_limit_reached"
    with sqlite3.connect(db) as conn:
        active_count = conn.execute(
            """
            SELECT COUNT(*) FROM user_book_subscriptions
            WHERE user_id = ? AND status IN ('active', 'paused')
            """,
            (user_id,),
        ).fetchone()[0]
    assert active_count == 1


def test_subscription_update_rate_limit_returns_structured_429(client, tmp_path, monkeypatch):
    import sqlite3
    from types import SimpleNamespace

    import app.api.subscribe as subscribe_api
    from app.core.app_config import AppConfig

    book_id = "book-rate-limit"
    _insert_book(tmp_path / "test.db", book_id, "限流测试书", "作者")
    user_id = client.get("/api/auth/me").json()["user"]["userId"]
    with sqlite3.connect(tmp_path / "test.db") as conn:
        conn.execute(
            """
            INSERT INTO users (user_id, username, password_hash, role, disabled, created_at, updated_at)
            VALUES (?, 'rate-limit-admin', 'test-hash', 'admin', 0, datetime('now'), datetime('now'))
            """,
            (user_id,),
        )
        conn.commit()
    subscribe_api.user_subscriptions_service.ensure(user_id, book_id)
    limits = SimpleNamespace(
        rate_limit_window_seconds=60,
        search_rate_limit_per_window=10,
        create_rate_limit_per_window=10,
        update_rate_limit_per_window=1,
    )
    monkeypatch.setattr(
        AppConfig,
        "get",
        classmethod(lambda _cls: SimpleNamespace(subscription=limits)),
    )
    subscribe_api.subscription_rate_limiter.reset()
    try:
        assert client.patch(
            f"/api/subscribe/books/{book_id}/subscription",
            json={"unknown": True},
        ).status_code == 422
        assert client.patch(
            "/api/subscribe/books/missing-book/subscription",
            json={"startChapterIndex": 2},
        ).status_code == 404
        assert client.patch(
            f"/api/subscribe/books/{book_id}/subscription",
            json={"startChapterIndex": 2},
        ).status_code == 200
        limited = client.patch(
            f"/api/subscribe/books/{book_id}/subscription",
            json={"startChapterIndex": 3},
        )
        assert limited.status_code == 429
        assert limited.json()["detail"] == {
            "code": "subscription_update_rate_limited",
            "message": "订阅设置更新操作过于频繁，请稍后重试",
            "retryable": True,
            "retryAfterSeconds": 60,
        }
    finally:
        subscribe_api.subscription_rate_limiter.reset()


def test_legado_max_toc_inserts_volume_headers_without_changing_x_layout():
    from app.api.legado import _public_toc_response

    result = {
        "chapters": [
            {
                "index": 1,
                "title": "第一章",
                "chapterUrl": "https://example.test/chapter/1",
                "extra": {"volumeName": "第一卷"},
            },
            {
                "index": 2,
                "title": "第二章",
                "chapterUrl": "https://example.test/chapter/2",
                "extra": {"volumeName": "第一卷"},
            },
            {
                "index": 3,
                "title": "第三章",
                "chapterUrl": "https://example.test/chapter/3",
                "extra": {"volumeName": "第二卷"},
            },
        ]
    }

    x_toc = _public_toc_response(
        result,
        source_id="fixture",
        book_id="fixture:book",
        base_api="http://testserver",
        reader="x",
    )
    max_toc = _public_toc_response(
        result,
        source_id="fixture",
        book_id="fixture:book",
        base_api="http://testserver",
        reader="max",
    )

    assert len(x_toc["chapters"]) == 3
    assert not any(item.get("isVolume") for item in x_toc["chapters"])
    assert [item["title"] for item in max_toc["chapters"]] == [
        "第一卷",
        "第一章",
        "第二章",
        "第二卷",
        "第三章",
    ]
    volume_nodes = [item for item in max_toc["chapters"] if item.get("isVolume")]
    assert len(volume_nodes) == 2
    assert all(item["chapterUrl"] == "" for item in volume_nodes)


def test_aggregate_toc_persists_volume_names_for_processed_chapters(tmp_path):
    import sqlite3

    from app.services.aggregate_processor import AggregateProcessor
    from app.storage.db import initialize_database

    db_path = tmp_path / "aggregate.db"
    initialize_database(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO aggregate_book_tasks (
                aggregate_book_id, name, author, primary_book_id,
                primary_source_id, aggregate_payload_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("volume-book", "多卷测试", "作者", "fixture:book", "fixture", "{}"),
        )
        conn.commit()

    processor = AggregateProcessor(db_path=db_path)
    chapters = [
        {
            "chapterId": "fixture:1",
            "title": "第一章",
            "chapterUrl": "https://example.test/1",
            "extra": {"volumeName": "第一卷"},
        },
        {
            "chapterId": "fixture:2",
            "title": "第二章",
            "chapterUrl": "https://example.test/2",
            "extra": {"volumeName": "第一卷"},
        },
    ]
    result = processor.register_toc(
        "volume-book",
        {"primaryBookId": "fixture:book"},
        chapters,
    )
    assert result["newChapters"] == 2

    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT volume_name FROM aggregate_chapter_tasks WHERE chapter_index = 1"
        ).fetchone() == ("第一卷",)
        conn.execute(
            "UPDATE aggregate_chapter_tasks SET status = 'processed' WHERE chapter_index = 1"
        )
        conn.commit()

    chapters[0]["extra"]["volumeName"] = "序章"
    processor.register_toc("volume-book", {"primaryBookId": "fixture:book"}, chapters)
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT volume_name FROM aggregate_chapter_tasks WHERE chapter_index = 1"
        ).fetchone() == ("序章",)
