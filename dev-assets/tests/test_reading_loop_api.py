"""Reading-compatible API end-to-end fixture loop."""

import sqlite3
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from app.main import app
from app.source_plugins.loader import PluginLoader
from app.source_plugins.smoke import FixtureFetcher


@pytest.mark.parametrize("debug,failed,message", [
    ({}, False, "暂无更多回复"),
    ({"error": "web_full_replies_unavailable", "supported": False}, False, "暂不支持加载完整回复"),
    ({"error": "upstream failure"}, True, "回复加载失败"),
])
def test_reply_detail_empty_and_failure_states(debug, failed, message):
    from lxml import html
    from app.services.reading_reviews import render_chapter_reviews_html

    rendered = render_chapter_reviews_html(
        chapter_title="测试", reviews={}, review_view_url="/reviews/view",
        reply_detail={"replies": [], "debug": debug},
    )
    container = html.fromstring(rendered).get_element_by_id("reply-detail-comments")
    assert message in container.text_content()
    assert (container.get("data-reply-error") == "true") is failed


def _write_reading_plugin(tmp_path: Path) -> tuple[Path, dict[str, str]]:
    plugin_id = "fixture_reading"
    plugin_dir = tmp_path / plugin_id
    fixture_dir = plugin_dir / "smoke" / "fixtures"
    fixture_dir.mkdir(parents=True)
    metadata = {
        "contractVersion": "1.0",
        "id": plugin_id,
        "name": "Fixture Reading",
        "version": "0.1.0",
        "type": "source",
        "domains": ["example.com"],
        "baseUrls": ["https://example.com"],
        "capabilities": ["search", "detail", "toc", "chapter", "chapter_reviews"],
        "auth": {"mode": "none"},
        "content": {"access": "free"},
        "tags": ["fixture"],
    }
    (plugin_dir / "metadata.yaml").write_text(yaml.safe_dump(metadata, allow_unicode=True), encoding="utf-8")
    (plugin_dir / "source.py").write_text(
        '''
class Source:
    id = "fixture_reading"
    name = "Fixture Reading"
    contract_version = "1.0"

    async def search(self, ctx, keyword: str, page: int):
        html = await ctx.access.http.fetch_text("https://example.com/search")
        return [{
            "sourceId": self.id,
            "name": ctx.text(html, ".name"),
            "author": ctx.text(html, ".author"),
            "bookUrl": ctx.urljoin("https://example.com", ctx.attr(html, ".name", "href")),
            "lastChapter": ctx.text(html, ".latest"),
        }]

    async def detail(self, ctx, book_url: str):
        html = await ctx.access.http.fetch_text(book_url)
        return {
            "sourceId": self.id,
            "name": ctx.text(html, "h1"),
            "author": ctx.text(html, ".author"),
            "bookUrl": book_url,
            "tocUrl": book_url,
            "lastChapter": ctx.text(html, ".latest"),
        }

    async def toc(self, ctx, toc_url: str):
        html = await ctx.access.http.fetch_text(toc_url)
        return [
            {"sourceId": self.id, "index": index, "title": a.text_content().strip(), "chapterUrl": ctx.urljoin(toc_url, a.get("href", ""))}
            for index, a in enumerate(ctx.select(html, ".chapters a"), start=1)
        ]

    async def chapter(self, ctx, chapter_url: str):
        html = await ctx.access.http.fetch_text(chapter_url)
        return {"sourceId": self.id, "title": ctx.text(html, "h1"), "chapterUrl": chapter_url, "content": ctx.html(html, "#content")}

    async def chapter_reviews(self, ctx, chapter_url: str):
        return {
            "paragraphs": {
                "1": [{
                    "id": "review-1",
                    "content": "第一段评论",
                    "userName": "读者甲",
                    "likeNum": 3,
                    "replyCount": 0,
                    "reviewTime": "刚刚",
                    "paragraphId": 1,
                    "isTop": False,
                }]
            },
            "chapterEnd": [{
                "id": "review-end",
                "content": "章末评论",
                "userName": "读者乙",
                "likeNum": 1,
                "replyCount": 0,
                "reviewTime": "刚刚",
                "paragraphId": -1,
                "isTop": False,
            }],
            "summary": {
                "totalParagraphs": 1,
                "totalReviews": 2,
                "paragraphsWithReviews": [1],
                "chapterEndCount": 1,
                "authMode": "public",
            },
        }
''',
        encoding="utf-8",
    )
    search_html = (
        '<html><body><a class="name" href="/book/1/">凡人修仙传</a>'
        '<span class="author">忘语</span><span class="latest">第一章 初入修仙</span></body></html>'
    )
    detail_html = (
        '<html><body><h1>凡人修仙传</h1><span class="author">忘语</span>'
        '<span class="latest">第一章 初入修仙</span>'
        '<div class="chapters"><a href="/book/1/1.html">第一章 初入修仙</a></div></body></html>'
    )
    chapter_html = (
        '<html><body><h1>第一章 初入修仙</h1>'
        '<div id="content">这是 Reading API 端到端 fixture 正文，长度超过二十个字符。</div></body></html>'
    )
    return plugin_dir, {
        "https://example.com/search": search_html,
        "https://example.com/book/1/": detail_html,
        "https://example.com/book/1/1.html": chapter_html,
    }


@pytest.fixture
def fixture_client(monkeypatch, tmp_path):
    _, responses = _write_reading_plugin(tmp_path)
    from app.source_plugins.scheduler import PluginScheduler
    import app.api.legado as legado_api
    import app.api.subscribe as subscribe_api

    original_init = PluginScheduler.__init__

    def patched_init(self, loader=None, config=None):
        loader = loader or PluginLoader(plugins_dir=tmp_path)
        original_init(self, loader=loader, config=config or {})

    monkeypatch.setattr(PluginScheduler, "__init__", patched_init)
    monkeypatch.setattr(PluginScheduler, "_make_fetcher", lambda self: FixtureFetcher(responses))
    import app.source_plugins.scheduler as scheduler_module
    monkeypatch.setattr(scheduler_module, "_scheduler_instance", None)

    from app.storage.db import initialize_database

    cache_db = tmp_path / "reading-cache.db"
    initialize_database(cache_db)
    monkeypatch.setattr("app.config.DB_PATH", cache_db)
    monkeypatch.setattr("app.services.cache.DB_PATH", cache_db)
    monkeypatch.setattr("app.services.search_coordinator.DB_PATH", cache_db)

    from app.services.library_books import LibraryBooksService
    from app.services.shared_book_storage import SharedBookStorage

    library_service = LibraryBooksService(
        db_path=cache_db,
        shared_book_storage=SharedBookStorage(root=tmp_path / "library"),
    )
    monkeypatch.setattr(subscribe_api, "library_books_service", library_service)
    monkeypatch.setattr(legado_api, "library_books_service", library_service)
    monkeypatch.setattr("app.services.search_coordinator.library_books_service", library_service)
    monkeypatch.setattr(subscribe_api, "_legado_search_service", None)
    monkeypatch.setattr(subscribe_api, "_legado_search_service_init_token", None)
    monkeypatch.setattr(subscribe_api, "_legado_search_owners", {})
    client = TestClient(app)
    login = client.post("/api/auth/login", json={"username": "admin", "password": "admin123"})
    assert login.status_code == 200
    return client


def test_reading_search_and_direct_third_party_loop(fixture_client, tmp_path):
    search = fixture_client.get(
        "/api/subscribe/legado/search",
        params={"keyword": "凡人修仙传", "waitMs": 5000},
    )

    assert search.status_code == 200
    payload = search.json()
    item = next(item for item in payload["items"] if item["sourceId"] == "fixture_reading")
    assert item["sourceName"] == "Fixture Reading"
    assert item["readingLastChapter"].startswith("Fixture Reading · ")
    assert "rawBookUrl" not in item
    assert "debug" not in item

    detail = fixture_client.get(item["bookUrl"].replace("http://testserver", ""))
    assert detail.status_code == 200
    detail_data = detail.json()["data"]
    assert detail_data["name"] == "凡人修仙传"
    assert "rawBookUrl" not in detail_data
    assert "rawTocUrl" not in detail_data
    assert "debug" not in detail.json()

    toc = fixture_client.get(detail_data["tocUrl"].replace("http://testserver", ""))
    assert toc.status_code == 200
    chapters = toc.json()["chapters"]
    assert len(chapters) == 1
    assert "rawChapterUrl" not in chapters[0]

    chapter = fixture_client.get(chapters[0]["chapterUrl"].replace("http://testserver", ""))
    assert chapter.status_code == 200
    assert "Reading API 端到端 fixture 正文" in chapter.json()["content"]
    assert "rawChapterUrl" not in chapter.json()
    assert "debug" not in chapter.json()

    reviews = fixture_client.get(
        chapters[0]["chapterUrl"].replace("http://testserver", "") + "/reviews"
    )
    assert reviews.status_code == 200
    assert reviews.json()["summary"]["totalReviews"] == 2

    with sqlite3.connect(tmp_path / "reading-cache.db") as conn:
        assert conn.execute("SELECT COUNT(*) FROM user_book_subscriptions").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM aggregate_book_tasks").fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM book_records WHERE book_id = ?",
            (item["bookId"],),
        ).fetchone()[0] == 1


def test_reading_rejects_official_and_unknown_plugin_ids_before_catalog(
    fixture_client, monkeypatch
):
    from app.services.catalog import Catalog
    from app.source_plugins.id_codec import encode_book_id, encode_chapter_id

    catalog = Catalog()
    plugin = catalog.scheduler._plugins["fixture_reading"]
    monkeypatch.setattr(plugin.metadata, "tags", ["official"])
    book_id = encode_book_id("fixture_reading", "https://example.com/book/1/")
    chapter_id = encode_chapter_id("fixture_reading", "https://example.com/book/1/1.html")
    unknown_book_id = encode_book_id("missing_plugin", "https://example.com/book/1/")
    unknown_chapter_id = encode_chapter_id("missing_plugin", "https://example.com/book/1/1.html")

    async def must_not_call(*_args, **_kwargs):
        pytest.fail("official or unknown plugin ids must not reach Catalog")

    monkeypatch.setattr(Catalog, "book_detail", must_not_call)
    monkeypatch.setattr(Catalog, "toc", must_not_call)
    monkeypatch.setattr(Catalog, "chapter", must_not_call)
    monkeypatch.setattr(Catalog, "chapter_reviews", must_not_call)

    assert fixture_client.get(f"/api/legado/book/{book_id}").status_code == 404
    assert fixture_client.get(f"/api/legado/book/{book_id}/toc").status_code == 404
    assert fixture_client.get(f"/api/legado/chapter/{chapter_id}").status_code == 404
    assert fixture_client.get(f"/api/legado/chapter/{chapter_id}/reviews").status_code == 404
    assert fixture_client.get(f"/api/legado/chapter/{chapter_id}/reviews/view").status_code == 404
    assert fixture_client.get(f"/api/legado/book/{unknown_book_id}").status_code == 404
    assert fixture_client.get(f"/api/legado/chapter/{unknown_chapter_id}").status_code == 404


def test_reading_rejects_third_party_urls_outside_declared_domains(
    fixture_client, monkeypatch
):
    from app.services.catalog import Catalog
    from app.source_plugins.id_codec import encode_book_id, encode_chapter_id

    async def must_not_call(*_args, **_kwargs):
        pytest.fail("off-domain ids must not reach Catalog")

    monkeypatch.setattr(Catalog, "book_detail", must_not_call)
    monkeypatch.setattr(Catalog, "chapter", must_not_call)
    book_id = encode_book_id("fixture_reading", "http://127.0.0.1:8766/api/console/plugins")
    chapter_id = encode_chapter_id("fixture_reading", "http://169.254.169.254/latest/meta-data")

    assert fixture_client.get(f"/api/legado/book/{book_id}").status_code == 404
    assert fixture_client.get(f"/api/legado/chapter/{chapter_id}").status_code == 404


def test_review_view_embedding_and_focused_paragraph_layout(fixture_client, monkeypatch):
    import app.api.legado as legado_api
    from app.services.aggregate_virtual_source import VIRTUAL_SOURCE_ID, make_aggregate_chapter_url
    from app.source_plugins.id_codec import encode_chapter_id

    chapter_id = encode_chapter_id(
        VIRTUAL_SOURCE_ID,
        make_aggregate_chapter_url("book-1", "chapter-1", index=1),
    )
    monkeypatch.setattr(
        legado_api.library_books_service,
        "legado_chapter",
        lambda _chapter_id: {"title": "第一章"},
    )

    async def empty_reviews(_chapter_id: str, **_kwargs) -> dict:
        return {
            "authorReviews": [],
            "chapterEnd": [],
            "hotParagraphReviews": [],
            "summary": {"totalReviews": 0, "chapterEndCount": 0},
        }

    async def empty_paragraph_reviews(
        _self,
        _chapter_id: str,
        _paragraph_id: int,
        *,
        page: int = 1,
        page_size: int = 10,
    ) -> dict:
        return {
            "comments": [],
            "totalCount": 0,
            "page": page,
            "pageSize": page_size,
            "hasMore": False,
        }

    monkeypatch.setattr(legado_api, "_chapter_reviews", empty_reviews)
    monkeypatch.setattr(legado_api.Catalog, "paragraph_reviews", empty_paragraph_reviews)

    api_response = fixture_client.get(f"/api/legado/chapter/{chapter_id}/reviews")
    view_response = fixture_client.get(f"/api/legado/chapter/{chapter_id}/reviews/view")
    focused_response = fixture_client.get(
        f"/api/legado/chapter/{chapter_id}/reviews/view?tab=paragraph&paragraphId=1"
    )

    assert api_response.status_code == 200
    assert api_response.headers["x-frame-options"] == "DENY"
    assert "frame-ancestors 'none'" in api_response.headers["content-security-policy"]
    assert view_response.status_code == 200
    assert view_response.headers["x-frame-options"] == "SAMEORIGIN"
    assert "frame-ancestors 'self'" in view_response.headers["content-security-policy"]
    assert 'data-tab="author"' not in view_response.text
    assert 'data-tab="chapter"' in view_response.text
    assert ".review-tabs{\n  display:flex;" in view_response.text
    assert "grid-template-columns:repeat(2,1fr)" not in view_response.text
    assert focused_response.status_code == 200
    assert 'data-tab="author"' not in focused_response.text
    assert 'data-tab="chapter"' not in focused_response.text
    assert 'data-panel="author"' not in focused_response.text
    assert 'data-panel="chapter"' not in focused_response.text
    assert 'data-panel="paragraph"' in focused_response.text
    assert fixture_client.get(
        f"/api/legado/chapter/{chapter_id}/reviews/view?tab=author"
    ).status_code == 422


def test_review_view_adopts_bearer_session_into_view_cookie(fixture_client, monkeypatch):
    """The sheet hands the viewing WebView its own session cookie.

    Legado Max opens the sheet with the book-source Login Header on the first
    request only, so the page's own pagination and reply fetches arrive without a
    credential and used to fail with 401 ("加载失败，点击重试").
    """
    import app.api.legado as legado_api
    from app.main import app
    from app.services.aggregate_virtual_source import VIRTUAL_SOURCE_ID, make_aggregate_chapter_url
    from app.source_plugins.id_codec import encode_chapter_id

    chapter_id = encode_chapter_id(
        VIRTUAL_SOURCE_ID,
        make_aggregate_chapter_url("book-1", "chapter-1", index=1),
    )
    monkeypatch.setattr(
        legado_api.library_books_service,
        "legado_chapter",
        lambda _chapter_id: {"title": "第一章"},
    )

    async def empty_reviews(_chapter_id: str, **_kwargs) -> dict:
        return {
            "authorReviews": [],
            "chapterEnd": [],
            "hotParagraphReviews": [],
            "summary": {"totalReviews": 0, "chapterEndCount": 0},
        }

    async def empty_paragraph_reviews(
        _self,
        _chapter_id: str,
        _paragraph_id: int,
        *,
        page: int = 1,
        page_size: int = 10,
    ) -> dict:
        return {
            "comments": [],
            "totalCount": 0,
            "page": page,
            "pageSize": page_size,
            "hasMore": False,
        }

    monkeypatch.setattr(legado_api, "_chapter_reviews", empty_reviews)
    monkeypatch.setattr(legado_api.Catalog, "paragraph_reviews", empty_paragraph_reviews)

    session_id = fixture_client.cookies.get("legadohub_session")
    assert session_id

    header_signed = TestClient(app)
    response = header_signed.get(
        f"/api/legado/chapter/{chapter_id}/reviews/view",
        headers={"Authorization": f"Bearer {session_id}"},
    )

    assert response.status_code == 200
    assert response.cookies.get("legadohub_session") == session_id
    assert "httponly" in response.headers["set-cookie"].lower()

    # The WebView keeps that cookie, so later same-origin fetches authenticate
    # without the header.
    assert header_signed.cookies.get("legadohub_session") == session_id
    paginated = header_signed.get(
        f"/api/legado/chapter/{chapter_id}/reviews/view",
        params={"tab": "paragraph", "paragraphId": 1, "page": 2, "pageSize": 10},
    )
    assert paginated.status_code == 200


def test_reading_disabled_plugin_chapter_does_not_use_cached_content(fixture_client, monkeypatch):
    from app.services.catalog import Catalog
    from app.source_plugins.id_codec import encode_chapter_id

    chapter_id = encode_chapter_id("fixture_reading", "https://example.com/book/1/1.html")
    catalog = Catalog()
    catalog.cache.set_chapter(
        chapter_id,
        "fixture_reading",
        "https://example.com/book/1/1.html",
        {
            "implemented": True,
            "chapterId": chapter_id,
            "title": "缓存章节",
            "content": "缓存正文",
            "debug": {},
        },
    )
    plugin = catalog.scheduler._plugins["fixture_reading"]
    monkeypatch.setattr(plugin.metadata, "enabled", False)

    chapter_res = fixture_client.get(f"/api/legado/chapter/{chapter_id}")

    assert chapter_res.status_code == 404
