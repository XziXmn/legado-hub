"""Tests for realtime search job API."""

import pytest
import asyncio
import sqlite3
import json
from types import SimpleNamespace
from fastapi.testclient import TestClient
from app.main import app
from app.storage.db import initialize_database
from app.services.search_jobs import SearchJobService
from app.source_plugins.errors import BrowserRequired
from app.source_plugins.models import LoadedPlugin, PluginMetadata
from app.source_plugins.scheduler import PluginScheduler
import app.source_plugins.scheduler as scheduler_module
from app.core.app_config import AppConfig
import app.core.app_config as app_config_module

client = TestClient(app)

@pytest.fixture(autouse=True)
def setup_db(tmp_path, monkeypatch):
    app_config_path = tmp_path / "app_config.json"
    # Reset the global AppConfig singleton so tests in this file are not
    # affected by config changes made by other test modules.
    monkeypatch.setattr(app_config_module, "APP_CONFIG_PATH", app_config_path)
    AppConfig.reset()

    db = tmp_path / "test.db"
    monkeypatch.setattr("app.config.DB_PATH", db)
    monkeypatch.setattr("app.services.search_coordinator.DB_PATH", db)
    monkeypatch.setattr("app.services.cache.DB_PATH", db)
    initialize_database(db)

    login = client.post(
        "/api/auth/login",
        json={"username": "admin", "password": "admin123"},
    )
    assert login.status_code == 200

    # Isolate tests from the global console service singleton so that
    # background sessions created in one test are not reused by the next.
    from app.api.console import _search_service
    from app.services.library_books import library_books_service
    from app.source_plugins.scheduler import PluginScheduler

    search_service = SearchJobService()
    search_service._coordinator.scheduler = PluginScheduler()
    monkeypatch.setattr("app.api.console._search_service", search_service)
    monkeypatch.setattr(library_books_service, "db_path", db)
    yield
    AppConfig.reset()


def test_realtime_app_config_uses_per_test_path(tmp_path):
    assert AppConfig.get().path == tmp_path / "app_config.json"


class FakeSchedulerBase:
    """Minimal scheduler double that satisfies SearchCoordinator's runtime contract."""

    def _search_priority_plugins(self, plugins):
        return sorted(
            plugins,
            key=lambda p: (
                0 if p.metadata.is_official_source() else 1,
                getattr(p.metadata, "priority", 50) or 50,
                p.metadata.name or "",
                p.metadata.id,
            ),
        )

    async def search_one(self, source_id, keyword, page):
        plugin = self._plugins.get(source_id)
        if plugin is None:
            return {"items": [], "error": None, "latencyMs": 0, "proxyUsed": False}
        ctx = self._make_ctx(source_id)
        try:
            raw_items = await plugin.source.search(ctx, keyword, page)
            items = []
            for item in raw_items or []:
                if isinstance(item, dict):
                    item.setdefault("sourceId", plugin.metadata.id)
                    item.setdefault("sourceName", plugin.metadata.name)
                    items.append(item)
            return {"items": items, "error": None, "latencyMs": 0, "proxyUsed": False}
        except Exception as exc:
            code = getattr(exc, "code", "PLUGIN_RUNTIME_ERROR")
            url = getattr(exc, "url", "") or ""
            extra = {}
            if code in {"BROWSER_REQUIRED", "CLOUDFLARE_REQUIRED"}:
                extra["bypassRequired"] = True
            error = {
                "sourceId": plugin.metadata.id,
                "stage": "search",
                "code": code,
                "message": str(exc),
                "url": url,
                "extra": extra,
            }
            return {"items": [], "error": error, "latencyMs": 0, "proxyUsed": False}
        finally:
            await ctx._fetcher.close()


def _plugin_from_metadata(metadata_dict, source):
    return type("Plugin", (), {
        "metadata": PluginMetadata.from_dict(metadata_dict),
        "source": source,
        "capabilities": ["search"],
    })()


def test_create_search_job():
    response = client.post("/api/console/search-jobs", json={"keyword": "凡人修仙传", "page": 1, "limit": 3})
    assert response.status_code == 200
    data = response.json()
    assert "jobId" in data
    assert data["status"] == "running"
    assert data["sourceCount"] == 3
    assert data["completedCount"] == 0
    assert data["events"][0]["type"] == "queued"


def test_create_search_job_exposes_immediate_progress_event():
    response = client.post("/api/console/search-jobs", json={"keyword": "凡人修仙传", "page": 1, "limit": 2})
    job_id = response.json()["jobId"]

    events_response = client.get(f"/api/console/search-jobs/{job_id}/events")

    assert events_response.status_code == 200
    events = events_response.json()["events"]
    assert events
    assert events[0]["type"] == "queued"
    assert events[0]["sourceCount"] == 2


def test_get_search_job():
    create = client.post("/api/console/search-jobs", json={"keyword": "test", "page": 1, "limit": 2})
    job_id = create.json()["jobId"]
    response = client.get(f"/api/console/search-jobs/{job_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["jobId"] == job_id


def test_get_missing_search_job_returns_404():
    response = client.get("/api/console/search-jobs/missing")

    assert response.status_code == 404
    assert response.json()["detail"] == "任务不存在"


def test_cancel_search_job():
    create = client.post("/api/console/search-jobs", json={"keyword": "test", "page": 1, "limit": 2})
    job_id = create.json()["jobId"]
    response = client.post(f"/api/console/search-jobs/{job_id}/cancel")
    assert response.status_code == 200
    data = response.json()
    assert data["cancelled"] is True


def test_cancel_completed_search_job_is_noop():
    service = SearchJobService()
    job = service.create_job("test", page=1, limit=0)
    job.status = "completed"

    assert service.cancel_job(job.job_id) is False
    assert job.status == "completed"


@pytest.mark.asyncio
async def test_search_job_exposes_first_source_result_before_all_sources_finish(monkeypatch):
    service = SearchJobService()
    slow_started = asyncio.Event()
    slow_can_finish = asyncio.Event()

    class FastSource:
        async def search(self, ctx, keyword, page):
            return [{"name": keyword, "author": "作者甲", "bookUrl": "https://fast.example/book/1"}]

    class SlowSource:
        async def search(self, ctx, keyword, page):
            slow_started.set()
            await slow_can_finish.wait()
            return [{"name": f"{keyword} 慢源", "author": "作者乙", "bookUrl": "https://slow.example/book/1"}]

    class FakeContext:
        class Fetcher:
            async def close(self):
                return None

        _fetcher = Fetcher()

    class FakeScheduler(FakeSchedulerBase):
        def __init__(self):
            self.plugins = [
                _plugin_from_metadata({
                    "contractVersion": "1.0",
                    "id": "fast_source",
                    "name": "Fast Source",
                    "version": "0.1.0",
                    "type": "source",
                    "domains": ["fast.example"],
                    "baseUrls": ["https://fast.example"],
                    "capabilities": ["search"],
                    "auth": {"mode": "none"},
                    "content": {"access": "free"},
                    "tags": [],
                }, FastSource()),
                _plugin_from_metadata({
                    "contractVersion": "1.0",
                    "id": "slow_source",
                    "name": "Slow Source",
                    "version": "0.1.0",
                    "type": "source",
                    "domains": ["slow.example"],
                    "baseUrls": ["https://slow.example"],
                    "capabilities": ["search"],
                    "auth": {"mode": "none"},
                    "content": {"access": "free"},
                    "tags": [],
                }, SlowSource()),
            ]
            self._plugins = {p.metadata.id: p for p in self.plugins}

        def _enabled_plugins(self):
            return self.plugins

        def _make_ctx(self, source_id):
            return FakeContext()

        def search_timeout_for_plugin(self, plugin):
            return 30

    service._coordinator.scheduler = FakeScheduler()
    job = service._coordinator._create_job("剑宗外门", page=1, source_ids=None, limit=2, search_mode="source")
    service._coordinator._jobs[job.job_id] = job

    runner = asyncio.create_task(service.run_job(job.job_id))
    await asyncio.wait_for(slow_started.wait(), timeout=1)

    for _ in range(20):
        if job.candidate_groups:
            break
        await asyncio.sleep(0.05)

    assert job.status == "running"
    assert job.candidate_groups
    assert job.candidate_groups[0]["items"][0]["sourceId"] == "fast_source"
    assert "done" not in [event["type"] for event in job.events]

    slow_can_finish.set()
    await runner


@pytest.mark.asyncio
async def test_search_job_honors_max_concurrency(monkeypatch):
    cfg = AppConfig.get()
    monkeypatch.setitem(cfg._data.setdefault("search", {}), "globalSourceConcurrency", 2)

    service = SearchJobService()
    active_count = 0
    max_active_count = 0

    class CountingSource:
        async def search(self, ctx, keyword, page):
            nonlocal active_count, max_active_count
            active_count += 1
            max_active_count = max(max_active_count, active_count)
            await asyncio.sleep(0.02)
            active_count -= 1
            return [{"name": keyword, "author": "作者", "bookUrl": f"https://{ctx.source_id}.example/book/1"}]

    class FakeContext:
        def __init__(self, source_id):
            self.source_id = source_id
            self._fetcher = self.Fetcher()

        class Fetcher:
            async def close(self):
                return None

    def make_plugin(index):
        source_id = f"source_{index}"
        return _plugin_from_metadata({
            "contractVersion": "1.0",
            "id": source_id,
            "name": f"Source {index}",
            "version": "0.1.0",
            "type": "source",
            "domains": [f"{source_id}.example"],
            "baseUrls": [f"https://{source_id}.example"],
            "capabilities": ["search"],
            "auth": {"mode": "none"},
            "content": {"access": "free"},
            "tags": [],
        }, CountingSource())

    class FakeScheduler(FakeSchedulerBase):
        plugins = [make_plugin(index) for index in range(5)]

        def __init__(self):
            self._plugins = {p.metadata.id: p for p in self.plugins}

        def _enabled_plugins(self):
            return self.plugins

        def _make_ctx(self, source_id):
            return FakeContext(source_id)

        def search_timeout_for_plugin(self, plugin):
            return 30

    service._coordinator.scheduler = FakeScheduler()
    job = service._coordinator._create_job("并发样例", page=1, source_ids=None, limit=5, search_mode="source")
    service._coordinator._jobs[job.job_id] = job

    await service.run_job(job.job_id)

    assert job.status == "completed"
    assert job.success_count == 5
    assert max_active_count == 2


@pytest.mark.asyncio
async def test_cancel_search_job_stops_pending_sources(monkeypatch):
    service = SearchJobService()
    source_started = asyncio.Event()

    class BlockingSource:
        async def search(self, ctx, keyword, page):
            source_started.set()
            await asyncio.sleep(30)
            return []

    class FakeContext:
        class Fetcher:
            async def close(self):
                return None

        _fetcher = Fetcher()

    class FakeScheduler(FakeSchedulerBase):
        plugin = _plugin_from_metadata({
            "contractVersion": "1.0",
            "id": "blocking_source",
            "name": "Blocking Source",
            "version": "0.1.0",
            "type": "source",
            "domains": ["blocking.example"],
            "baseUrls": ["https://blocking.example"],
            "capabilities": ["search"],
            "auth": {"mode": "none"},
            "content": {"access": "free"},
            "tags": [],
        }, BlockingSource())

        def __init__(self):
            self._plugins = {self.plugin.metadata.id: self.plugin}

        def _enabled_plugins(self):
            return [self.plugin]

        def _make_ctx(self, source_id):
            return FakeContext()

        def search_timeout_for_plugin(self, plugin):
            return 30

    cfg = AppConfig.get()
    monkeypatch.setitem(cfg._data.setdefault("search", {}), "firstResultTimeoutSeconds", 0.3)

    service._coordinator.scheduler = FakeScheduler()
    job = service._coordinator._create_job("剑宗外门", page=1, source_ids=None, limit=1, search_mode="source")
    service._coordinator._jobs[job.job_id] = job

    runner = asyncio.create_task(service.run_job(job.job_id))
    await asyncio.wait_for(source_started.wait(), timeout=1)

    assert service.cancel_job(job.job_id) is True
    # The coordinator shields the actual network task so it may keep running in
    # the background, but the job runner itself should return promptly.
    await asyncio.wait_for(runner, timeout=2)

    assert job.status == "cancelled"
    assert "cancelled" in [event["type"] for event in job.events]


def test_search_stream():
    response = client.get("/api/console/search/stream?keyword=test&limit=2")
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/event-stream; charset=utf-8"


def test_search_job_candidates_endpoint():
    create = client.post("/api/console/search-jobs", json={"keyword": "test", "page": 1, "limit": 2})
    job_id = create.json()["jobId"]
    response = client.get(f"/api/console/search-jobs/{job_id}/candidates")
    assert response.status_code == 200
    data = response.json()
    assert data["jobId"] == job_id
    assert "items" in data


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "live_result",
    [
        {"items": [], "error": None, "latencyMs": 3, "proxyUsed": False},
        {
            "items": [],
            "error": {"sourceId": "fixture_cached", "code": "PLUGIN_RUNTIME_ERROR", "message": "network error"},
            "latencyMs": 3,
            "proxyUsed": False,
        },
    ],
)
async def test_search_one_uses_cache_for_fast_empty_or_error(monkeypatch, live_result):
    service = SearchJobService()
    coordinator = service._coordinator

    class FakeScheduler:
        _plugins = {}

        async def search_one(self, source_id, keyword, page):
            return dict(live_result)

    coordinator.scheduler = FakeScheduler()
    monkeypatch.setattr(
        coordinator,
        "_query_source_cache",
        lambda keyword, source_id: [{"name": "剑宗外门", "bookUrl": "https://cached.example/book/1"}],
    )

    result = await coordinator._search_one(
        {"sourceId": "fixture_cached", "bookSourceName": "缓存源"},
        "剑宗外门",
        1,
        timeout=1,
    )

    assert result["_cache_fallback"] is True
    assert result["items"][0]["freshness"] == "cached"
    assert result["items"][0]["sourceId"] == "fixture_cached"


@pytest.mark.asyncio
async def test_search_job_marks_browser_required_as_bypass_error(monkeypatch):
    service = SearchJobService()

    class FakeSource:
        async def search(self, ctx, keyword, page):
            raise BrowserRequired("browser verification required", url="https://example.com/search")

    class FakeContext:
        class Fetcher:
            async def close(self):
                return None

        _fetcher = Fetcher()

    class FakeScheduler(FakeSchedulerBase):
        def __init__(self):
            self.plugin = _plugin_from_metadata({
                "contractVersion": "1.0",
                "id": "fixture_browser",
                "name": "Fixture Browser",
                "version": "0.1.0",
                "type": "source",
                "domains": ["example.com"],
                "baseUrls": ["https://example.com"],
                "capabilities": ["search"],
                "auth": {"mode": "none"},
                "content": {"access": "free"},
                "browser": {"mode": "required"},
                "tags": [],
            }, FakeSource())
            self._plugins = {self.plugin.metadata.id: self.plugin}

        def _enabled_plugins(self):
            return [self.plugin]

        def _make_ctx(self, source_id):
            return FakeContext()

        def search_timeout_for_plugin(self, plugin):
            return 30

    service._coordinator.scheduler = FakeScheduler()
    job = service._coordinator._create_job("剑宗外门", page=1, source_ids=None, limit=1, search_mode="source")
    service._coordinator._jobs[job.job_id] = job

    await service.run_job(job.job_id)

    event_types = [event["type"] for event in job.events]
    assert "source_complete" in event_types
    assert "source_timeout" not in event_types
    error_event = next(
        (event for event in job.events if event["type"] == "source_complete" and event.get("error")),
        None,
    )
    assert error_event is not None
    assert error_event["error"]["code"] == "BROWSER_REQUIRED"
    assert error_event["error"]["extra"]["bypassRequired"] is True


@pytest.mark.asyncio
async def test_search_job_does_not_preflight_cloudflare_browser_source(monkeypatch):
    service = SearchJobService()

    class FakeSource:
        async def search(self, ctx, keyword, page):
            raise BrowserRequired("browser bypass required", url="https://example.com/rank")

    class FakeContext:
        class Fetcher:
            async def close(self):
                return None

        _fetcher = Fetcher()

    class FakeScheduler(FakeSchedulerBase):
        def __init__(self):
            self.plugin = _plugin_from_metadata({
                "contractVersion": "1.0",
                "id": "fixture_cf",
                "name": "Fixture CF",
                "version": "0.1.0",
                "type": "source",
                "domains": ["example.com"],
                "baseUrls": ["https://example.com"],
                "capabilities": ["search"],
                "auth": {"mode": "none"},
                "content": {"access": "free"},
                "browser": {
                    "mode": "required",
                    "reason": "cloudflare_verification",
                },
                "tags": [],
            }, FakeSource())
            self._plugins = {self.plugin.metadata.id: self.plugin}

        def _enabled_plugins(self):
            return [self.plugin]

        def _make_ctx(self, source_id):
            return FakeContext()

        def search_timeout_for_plugin(self, plugin):
            return 30

    service._coordinator.scheduler = FakeScheduler()
    job = service._coordinator._create_job("剑宗外门", page=1, source_ids=None, limit=1, search_mode="source")
    service._coordinator._jobs[job.job_id] = job

    await service.run_job(job.job_id)

    event_types = [event["type"] for event in job.events]
    assert "source_complete" in event_types
    assert job.completed_count == 1
    error_event = next(
        (event for event in job.events if event["type"] == "source_complete" and event.get("error")),
        None,
    )
    assert error_event is not None
    assert error_event["error"]["extra"]["bypassRequired"] is True


@pytest.mark.asyncio
async def test_search_job_skips_cloudflare_preflight_when_search_provider_fallback_declared(monkeypatch):
    service = SearchJobService()

    class FakeSource:
        async def search(self, ctx, keyword, page):
            return [{
                "sourceId": "fixture_cf_fallback",
                "name": keyword,
                "author": "作者",
                "bookUrl": "https://www.69shuba.com/book/1.htm",
            }]

    class FakeContext:
        class Fetcher:
            async def close(self):
                return None

        _fetcher = Fetcher()

    class FakeScheduler(FakeSchedulerBase):
        def __init__(self):
            self.plugin = _plugin_from_metadata({
                "contractVersion": "1.0",
                "id": "fixture_cf_fallback",
                "name": "Fixture CF Fallback",
                "version": "0.1.0",
                "type": "source",
                "domains": ["69shuba.com"],
                "baseUrls": ["https://www.69shuba.com"],
                "capabilities": ["search"],
                "auth": {"mode": "none"},
                "content": {"access": "free"},
                "browser": {
                    "mode": "required",
                    "reason": "cloudflare_verification",
                },
                "accessStrategy": {"search": "search_provider"},
                "tags": [],
            }, FakeSource())
            self._plugins = {self.plugin.metadata.id: self.plugin}

        def _enabled_plugins(self):
            return [self.plugin]

        def _make_ctx(self, source_id):
            return FakeContext()

        def search_timeout_for_plugin(self, plugin):
            return 30

    service._coordinator.scheduler = FakeScheduler()
    job = service._coordinator._create_job("剑宗外门", page=1, source_ids=None, limit=1, search_mode="source")
    service._coordinator._jobs[job.job_id] = job

    await service.run_job(job.job_id)

    event_types = [event["type"] for event in job.events]
    assert "source_verification_required" not in event_types
    assert job.completed_count == 1
    assert job.success_count == 1
    assert job.candidate_groups[0]["name"] == "剑宗外门"


@pytest.mark.asyncio
async def test_search_job_records_candidate_books_for_bookshelf(monkeypatch):
    import app.config as config

    service = SearchJobService()

    class FakeSource:
        async def search(self, ctx, keyword, page):
            return [{
                "sourceId": "fixture_bookshelf",
                "sourceName": "Fixture Bookshelf",
                "name": keyword,
                "author": "作者甲",
                "bookUrl": "https://bookshelf.example/book/1",
                "lastChapter": "第一章 起点",
            }]

    class FakeContext:
        class Fetcher:
            async def close(self):
                return None

        _fetcher = Fetcher()

    class FakeScheduler(FakeSchedulerBase):
        plugin = _plugin_from_metadata({
            "contractVersion": "1.0",
            "id": "fixture_bookshelf",
            "name": "Fixture Bookshelf",
            "version": "0.1.0",
            "type": "source",
            "domains": ["bookshelf.example"],
            "baseUrls": ["https://bookshelf.example"],
            "capabilities": ["search"],
            "auth": {"mode": "none"},
            "content": {"access": "free"},
            "tags": [],
        }, FakeSource())

        def __init__(self):
            self._plugins = {self.plugin.metadata.id: self.plugin}

        def _enabled_plugins(self):
            return [self.plugin]

        def _make_ctx(self, source_id):
            return FakeContext()

        def search_timeout_for_plugin(self, plugin):
            return 30

    service._coordinator.scheduler = FakeScheduler()
    job = service._coordinator._create_job("书架样例", page=1, source_ids=None, limit=1, search_mode="source")
    service._coordinator._jobs[job.job_id] = job

    await service.run_job(job.job_id)

    assert job.status == "completed"
    book_id = job.result["items"][0]["bookId"]
    with sqlite3.connect(config.DB_PATH) as conn:
        row = conn.execute(
            "SELECT book_id, name, author, selected_source_id, last_chapter FROM book_records WHERE book_id = ?",
            (book_id,),
        ).fetchone()

    assert row == (book_id, "书架样例", "作者甲", "fixture_bookshelf", "第一章 起点")


@pytest.mark.asyncio
async def test_search_snapshot_includes_raw_items_and_candidate_groups(monkeypatch):
    service = SearchJobService()

    class SourceA:
        async def search(self, ctx, keyword, page):
            return [{
                "name": keyword,
                "author": "作者甲",
                "bookUrl": "https://a.example/book/1",
                "lastChapter": "第一章",
            }]

    class SourceB:
        async def search(self, ctx, keyword, page):
            return [{
                "name": keyword,
                "author": "作者甲",
                "bookUrl": "https://b.example/book/1",
                "lastChapter": "第二章",
            }]

    class FakeContext:
        class Fetcher:
            async def close(self):
                return None

        _fetcher = Fetcher()

    def plugin(source_id, name, source):
        return _plugin_from_metadata({
            "contractVersion": "1.0",
            "id": source_id,
            "name": name,
            "version": "0.1.0",
            "type": "source",
            "domains": [f"{source_id}.example"],
            "baseUrls": [f"https://{source_id}.example"],
            "capabilities": ["search"],
            "auth": {"mode": "none"},
            "content": {"access": "free"},
            "tags": [],
        }, source)

    class FakeScheduler(FakeSchedulerBase):
        plugins = [
            plugin("source_a", "Source A", SourceA()),
            plugin("source_b", "Source B", SourceB()),
        ]

        def __init__(self):
            self._plugins = {p.metadata.id: p for p in self.plugins}

        def _enabled_plugins(self):
            return self.plugins

        def _make_ctx(self, source_id):
            return FakeContext()

        def search_timeout_for_plugin(self, plugin):
            return 30

    service._coordinator.scheduler = FakeScheduler()
    job = service._coordinator._create_job("聚合样例", page=1, source_ids=None, limit=2, search_mode="source")
    service._coordinator._jobs[job.job_id] = job

    await service.run_job(job.job_id)

    result_source_ids = [item["sourceId"] for item in job.result["items"]]
    assert result_source_ids.count("source_a") == 1
    assert result_source_ids.count("source_b") == 1
    assert "legadohub_ai_aggregate" not in result_source_ids

    snapshot = service._coordinator.snapshot(job)
    source_ids = [item["sourceId"] for item in snapshot["items"]]
    assert source_ids.count("source_a") == 1
    assert source_ids.count("source_b") == 1
    assert snapshot["candidateGroups"]
    assert snapshot["candidateGroups"][0]["name"] == "聚合样例"


@pytest.mark.skip(
    reason=(
        "当前 snapshot() 只在命中共享书库书籍时注入 AI 聚合源，"
        "不再根据 contentWorkflow.returnOnlyAggregateSource 自动生成虚拟聚合源；"
        "admin_settings 表也已移除，无法通过原方式配置该行为。"
    )
)
@pytest.mark.asyncio
async def test_search_snapshot_can_return_only_virtual_aggregate_source(monkeypatch):
    import app.config as config

    service = SearchJobService()

    class FakeSource:
        async def search(self, ctx, keyword, page):
            return [{
                "name": keyword,
                "author": "作者甲",
                "bookUrl": "https://a.example/book/1",
            }]

    class FakeContext:
        class Fetcher:
            async def close(self):
                return None

        _fetcher = Fetcher()

    class FakeScheduler(FakeSchedulerBase):
        plugin = _plugin_from_metadata({
            "contractVersion": "1.0",
            "id": "source_a",
            "name": "Source A",
            "version": "0.1.0",
            "type": "source",
            "domains": ["a.example"],
            "baseUrls": ["https://a.example"],
            "capabilities": ["search"],
            "auth": {"mode": "none"},
            "content": {"access": "free"},
            "tags": [],
        }, FakeSource())

        def __init__(self):
            self._plugins = {self.plugin.metadata.id: self.plugin}

        def _enabled_plugins(self):
            return [self.plugin]

        def _make_ctx(self, source_id):
            return FakeContext()

        def search_timeout_for_plugin(self, plugin):
            return 30

    with sqlite3.connect(config.DB_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO admin_settings (key, value_json) VALUES (?, ?)",
            (
                "contentWorkflow",
                json.dumps({"returnOnlyAggregateSource": True}, ensure_ascii=False),
            ),
        )
        conn.commit()

    service._coordinator.scheduler = FakeScheduler()
    job = service.create_job("聚合样例", page=1, limit=1)

    await service.run_job(job.job_id)

    snapshot = service._coordinator.snapshot(job)
    assert [item["sourceId"] for item in snapshot["items"]] == ["legadohub_ai_aggregate"]


@pytest.mark.skip(
    reason=(
        "SearchCoordinator 当前只持久化搜索任务摘要和 search_results，"
        "不再持久化事件流；事件仅保留在内存中，因此从 DB 恢复的作业无法保留原事件。"
    )
)
@pytest.mark.asyncio
async def test_search_job_persists_completed_result_and_events(monkeypatch):
    service = SearchJobService()

    class FakeSource:
        async def search(self, ctx, keyword, page):
            return [{
                "name": keyword,
                "author": "作者甲",
                "bookUrl": "https://persist.example/book/1",
            }]

    class FakeContext:
        class Fetcher:
            async def close(self):
                return None

        _fetcher = Fetcher()

    class FakeScheduler(FakeSchedulerBase):
        plugin = _plugin_from_metadata({
            "contractVersion": "1.0",
            "id": "persist_source",
            "name": "Persist Source",
            "version": "0.1.0",
            "type": "source",
            "domains": ["persist.example"],
            "baseUrls": ["https://persist.example"],
            "capabilities": ["search"],
            "auth": {"mode": "none"},
            "content": {"access": "free"},
            "tags": [],
        }, FakeSource())

        def __init__(self):
            self._plugins = {self.plugin.metadata.id: self.plugin}

        def _enabled_plugins(self):
            return [self.plugin]

        def _make_ctx(self, source_id):
            return FakeContext()

        def search_timeout_for_plugin(self, plugin):
            return 30

    service._coordinator.scheduler = FakeScheduler()
    job = service._coordinator._create_job("持久化样例", page=1, source_ids=None, limit=1, search_mode="source")
    service._coordinator._jobs[job.job_id] = job
    job.events.append({"type": "queued", "keyword": job.keyword, "page": job.page})
    service.persist_job(job)

    await service.run_job(job.job_id)

    restored_service = SearchJobService()
    restored_job = restored_service.get_job(job.job_id)

    assert restored_job is not None
    assert restored_job.status == "completed"
    assert restored_job.result["items"][0]["name"] == "持久化样例"
    assert [event["type"] for event in restored_job.events][0] == "queued"
    assert "done" in [event["type"] for event in restored_job.events]
    assert restored_service.get_events(job.job_id)
    assert restored_service.list_jobs(limit=5)[0]["jobId"] == job.job_id


def test_verify_search_job_candidate_returns_reviews(monkeypatch):
    from app.api.console import _live_acceptance_service, _search_service

    async def mock_verify(candidate, keyword="", chapter_index=0, include_reviews=True):
        return {
            "pluginId": candidate.get("sourceId", ""),
            "keyword": keyword,
            "status": "passed",
            "passed": True,
            "search": {"count": 0, "firstName": "", "groups": []},
            "selectedCandidate": {
                "candidateId": "abc123",
                "sourceId": candidate.get("sourceId", ""),
                "sourceName": "起点中文网",
                "name": "凡人修仙传",
                "author": "忘语",
                "bookUrl": candidate.get("bookUrl", ""),
                "lastChapter": "",
            },
            "detail": {"name": "凡人修仙传", "author": "忘语", "coverUrl": "", "intro": "", "kind": "", "lastChapter": "", "wordCount": "", "wordCountText": "", "bookUrl": "", "tocUrl": "", "passed": True},
            "toc": {"chapterCount": 1, "count": 1, "firstTitle": "第一章", "items": [{"title": "第一章", "chapterUrl": "https://m.qidian.com/chapter/1/1/"}], "passed": True},
            "chapter": {"title": "第一章", "chapterUrl": "https://m.qidian.com/chapter/1/1/", "contentLength": 900, "preview": "正文" * 300, "content": "正文" * 300, "passed": True},
            "reviews": {
                "paragraphs": {"0": [{"id": "r1", "content": "段评样例", "userName": "读者甲", "likeNum": 3, "replyCount": 0, "reviewTime": "2026-06-11", "paragraphId": 0}]},
                "chapterEnd": [{"id": "r2", "content": "章末评论样例", "userName": "读者乙", "likeNum": 5, "replyCount": 1, "reviewTime": "2026-06-11"}],
                "summary": {"totalReviews": 2, "totalParagraphs": 1, "chapterEndCount": 1, "authMode": "public"},
                "debug": {"mock": True},
                "passed": True,
            },
            "diagnostics": [],
            "timings": {"elapsedMs": 123},
            "explore": {"groupCount": 0, "itemCount": 0, "groups": [], "selected": {"sourceId": "", "sourceName": "", "name": "", "author": "", "bookUrl": "", "groupId": "", "groupTitle": ""}, "detailPassed": False, "tocCount": 0, "chapterTitle": "", "contentLength": 0, "passed": None},
        }

    monkeypatch.setattr(_live_acceptance_service, "verify_candidate", mock_verify)

    job = _search_service.create_job("凡人修仙传", page=1, limit=1)
    _search_service.cancel_job(job.job_id)
    job.status = "completed"
    job.candidate_groups = [{
        "candidateId": "abc123",
        "name": "凡人修仙传",
        "author": "忘语",
        "items": [{
            "candidateId": "abc123",
            "sourceId": "qidian_com",
            "sourceName": "起点中文网",
            "name": "凡人修仙传",
            "author": "忘语",
            "bookUrl": "https://m.qidian.com/book/1/",
            "score": 1200,
        }],
    }]
    _search_service.persist_job(job)

    response = client.post(f"/api/console/search-jobs/{job.job_id}/candidates/abc123/verify", json={"chapterIndex": 0})
    assert response.status_code == 200
    data = response.json()
    assert data["jobId"] == job.job_id
    assert data["candidateId"] == "abc123"
    result = data["result"]
    assert result["reviews"]["paragraphs"]["0"][0]["content"] == "段评样例"
    assert result["reviews"]["chapterEnd"][0]["content"] == "章末评论样例"
    assert result["reviews"]["summary"]["totalReviews"] == 2
    assert result["reviews"]["passed"] is True


def test_verify_search_job_candidate_can_skip_reviews(monkeypatch):
    from app.api.console import _live_acceptance_service, _search_service

    captured: dict = {}

    async def mock_verify(candidate, keyword="", chapter_index=0, include_reviews=True):
        captured["include_reviews"] = include_reviews
        return {"reviews": {"paragraphs": {}, "chapterEnd": [], "summary": {}}}

    monkeypatch.setattr(_live_acceptance_service, "verify_candidate", mock_verify)

    job = _search_service.create_job("凡人修仙传", page=1, limit=1)
    _search_service.cancel_job(job.job_id)
    job.status = "completed"
    job.candidate_groups = [{
        "candidateId": "abc123",
        "name": "凡人修仙传",
        "author": "忘语",
        "items": [{
            "candidateId": "abc123",
            "sourceId": "qidian_com",
            "sourceName": "起点中文网",
            "name": "凡人修仙传",
            "author": "忘语",
            "bookUrl": "https://m.qidian.com/book/1/",
            "score": 1200,
        }],
    }]
    _search_service.persist_job(job)

    response = client.post(
        f"/api/console/search-jobs/{job.job_id}/candidates/abc123/verify",
        json={"chapterIndex": 0, "includeReviews": False},
    )
    assert response.status_code == 200
    assert captured.get("include_reviews") is False


def test_fetch_search_job_candidate_reviews_endpoint(monkeypatch):
    from app.api.console import _live_acceptance_service, _search_service

    async def mock_fetch_reviews(candidate, chapter_index=0, timeout=None):
        return {
            "ok": True,
            "pluginId": candidate.get("sourceId", ""),
            "candidateId": "abc123",
            "chapterIndex": chapter_index,
            "reviews": {
                "paragraphs": {},
                "chapterEnd": [{"id": "r3", "content": "异步章评", "userName": "读者丙"}],
                "summary": {"totalReviews": 1, "chapterEndCount": 1},
            },
        }

    monkeypatch.setattr(_live_acceptance_service, "fetch_reviews", mock_fetch_reviews)

    job = _search_service.create_job("凡人修仙传", page=1, limit=1)
    _search_service.cancel_job(job.job_id)
    job.status = "completed"
    job.candidate_groups = [{
        "candidateId": "abc123",
        "name": "凡人修仙传",
        "author": "忘语",
        "items": [{
            "candidateId": "abc123",
            "sourceId": "qidian_com",
            "sourceName": "起点中文网",
            "name": "凡人修仙传",
            "author": "忘语",
            "bookUrl": "https://m.qidian.com/book/1/",
            "score": 1200,
        }],
    }]
    _search_service.persist_job(job)

    response = client.post(
        f"/api/console/search-jobs/{job.job_id}/candidates/abc123/reviews",
        json={"chapterIndex": 2, "timeout": 60},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["jobId"] == job.job_id
    assert data["candidateId"] == "abc123"
    assert data["result"]["reviews"]["chapterEnd"][0]["content"] == "异步章评"


@pytest.mark.asyncio
async def test_scheduler_enforces_declared_plugin_rate_limit():
    metadata = PluginMetadata.from_dict({
        "contractVersion": "1.0",
        "id": "rate_limited_source",
        "name": "限流测试源",
        "version": "1.0.0",
        "type": "source",
        "domains": ["example.test"],
        "baseUrls": ["https://example.test"],
        "capabilities": ["chapter"],
        "auth": {"mode": "none"},
        "content": {"access": "free"},
        "tags": [],
        "rateLimit": {"perHostConcurrency": 2, "minIntervalMs": 20},
    })
    plugin = LoadedPlugin(metadata=metadata, module=None, source=object(), capabilities=["chapter"])

    class Loader:
        def load_all(self):
            return {metadata.id: plugin}

    scheduler = PluginScheduler(loader=Loader(), config={"browser_source_concurrency": 1})
    active = 0
    max_active = 0
    started: list[float] = []
    lock = asyncio.Lock()

    async def operation():
        nonlocal active, max_active
        async with lock:
            active += 1
            max_active = max(max_active, active)
            started.append(asyncio.get_running_loop().time())
        await asyncio.sleep(0.06)
        async with lock:
            active -= 1

    await asyncio.gather(*[
        scheduler._call_plugin(plugin, operation, timeout=1.0)
        for _ in range(4)
    ])

    assert max_active == 2
    assert min(b - a for a, b in zip(started, started[1:])) >= 0.015


@pytest.mark.asyncio
async def test_scheduler_rate_limiter_rechecks_after_early_wakeup(monkeypatch):
    now = 0.0
    sleeps = []
    starts = []

    def monotonic():
        return now

    async def early_sleep(seconds):
        nonlocal now
        sleeps.append(seconds)
        now += seconds / 2 if len(sleeps) == 1 else seconds

    monkeypatch.setattr(scheduler_module, "time", SimpleNamespace(monotonic=monotonic))
    monkeypatch.setattr(scheduler_module, "asyncio", SimpleNamespace(
        Semaphore=asyncio.Semaphore,
        Lock=asyncio.Lock,
        sleep=early_sleep,
    ))
    limiter = scheduler_module._PluginRateLimiter(concurrency=2, min_interval_ms=20)

    async def operation():
        starts.append(now)

    await limiter.run(operation)
    await limiter.run(operation)

    assert len(sleeps) == 2
    assert starts[1] - starts[0] >= 0.02
