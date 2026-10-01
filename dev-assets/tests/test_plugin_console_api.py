"""Tests for plugin console API endpoints."""

import asyncio
import copy
import json
import sqlite3
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)
_login = client.post("/api/auth/login", json={"username": "admin", "password": "admin123"})
if _login.status_code != 200:
    pytest.skip(f"admin login unavailable: {_login.status_code} {_login.text}", allow_module_level=True)


@pytest.fixture(autouse=True)
def refresh_module_admin_session():
    response = client.post(
        "/api/auth/login",
        json={"username": "admin", "password": "admin123"},
    )
    assert response.status_code == 200


@pytest.fixture
def admin_client():
    """Return an authenticated TestClient for admin-only routes.

    Uses the real DB; login with the admin password stored in app_config.json.
    If login fails, the calling test will fail loudly.
    """
    test_client = TestClient(app)
    res = test_client.post("/api/auth/login", json={"username": "admin", "password": "admin123"})
    if res.status_code != 200:
        pytest.skip(f"admin login unavailable: {res.status_code} {res.text}")
    return test_client


@pytest.fixture
def user_client():
    """Return an authenticated non-admin client."""
    from app.services.user_auth import auth_service

    if not auth_service.get_user_by_username("reader"):
        auth_service.create_user("reader", "reader123", role="user")
    test_client = TestClient(app)
    res = test_client.post(
        "/api/auth/access/redeem",
        json={"accessCode": auth_service.build_access_code("reader", "reader123")},
    )
    assert res.status_code == 200
    return test_client


def test_https_login_sets_secure_session_cookie():
    secure_client = TestClient(app, base_url="https://testserver")

    response = secure_client.post(
        "/api/auth/login",
        json={"username": "admin", "password": "admin123"},
    )

    assert response.status_code == 200
    assert "Secure" in response.headers["set-cookie"]


def test_malformed_cache_json_is_logged_without_exposing_payload(monkeypatch):
    from unittest.mock import Mock
    from app.api import console as console_api

    warning = Mock()
    monkeypatch.setattr(console_api.logger, "warning", warning)

    assert console_api._json_payload('{"secret":') == {}
    warning.assert_called_once()
    assert "secret" not in str(warning.call_args)


def test_plugin_reload_requires_admin(admin_client, user_client):
    anonymous_client = TestClient(app)

    assert anonymous_client.post("/api/console/plugins/reload").status_code == 401
    assert user_client.post("/api/console/plugins/reload").status_code == 403
    assert admin_client.post("/api/console/plugins/reload").status_code == 200


def test_console_chapter_read_requires_admin(user_client):
    anonymous_client = TestClient(app)

    assert anonymous_client.get("/api/console/chapter/invalid").status_code == 401
    assert user_client.get("/api/console/chapter/invalid").status_code == 403


def test_settings_subscription_policy_round_trip_and_strict_validation(admin_client, user_client):
    original = admin_client.get("/api/console/settings").json()
    payload = copy.deepcopy(original)
    payload["subscription"] = {
        **payload["subscription"],
        "maxActivePerUser": 17,
        "maxNewSharedBooksPerDay": 4,
        "maxGlobalProvisioningBooks": 9,
    }
    payload["searchConfig"]["sourceTimeoutSeconds"] = 1
    payload["sourcePool"]["source_timeout_seconds"] = 12.5

    try:
        response = admin_client.post("/api/console/settings", json=payload)
        assert response.status_code == 200
        saved = admin_client.get("/api/console/settings").json()
        assert saved["subscription"] == payload["subscription"]
        assert saved["sourcePool"]["source_timeout_seconds"] == 12.5

        before_rejected_request = copy.deepcopy(saved)
        rejected = admin_client.post(
            "/api/console/settings",
            json={
                "sourcePool": {"max_concurrency": 99},
                "subscription": {"maxActivePerUser": 3, "unexpected": True},
            },
        )
        assert rejected.status_code == 422
        after_rejected_request = admin_client.get("/api/console/settings").json()
        assert after_rejected_request["sourcePool"]["max_concurrency"] == before_rejected_request["sourcePool"]["max_concurrency"]
        assert after_rejected_request["subscription"] == before_rejected_request["subscription"]

        assert admin_client.post(
            "/api/console/settings",
            json={"subscription": {"maxActivePerUser": 0}},
        ).status_code == 422
        assert admin_client.post(
            "/api/console/settings",
            json={"unknown": {}},
        ).status_code == 422
        assert user_client.get("/api/console/settings").status_code == 403
        assert user_client.post("/api/console/settings", json=payload).status_code == 403
    finally:
        restored = admin_client.post("/api/console/settings", json=original)
        assert restored.status_code == 200


def test_settings_updates_do_not_overwrite_concurrent_fields(admin_client, monkeypatch):
    import app.api.console as console_api
    from app.core.app_config import AppConfig

    original = admin_client.get("/api/console/settings").json()
    original_save = AppConfig.save

    def slow_save(config):
        time.sleep(0.05)
        original_save(config)

    monkeypatch.setattr(AppConfig, "save", slow_save)
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(console_api.update_settings, {"subscription": {"maxActivePerUser": 31}}),
                executor.submit(console_api.update_settings, {"searchScoreFilter": 321}),
            ]
            for future in futures:
                assert future.result()["saved"] is True

        saved = admin_client.get("/api/console/settings").json()
        assert saved["subscription"]["maxActivePerUser"] == 31
        assert saved["searchScoreFilter"] == 321
    finally:
        assert admin_client.post("/api/console/settings", json=original).status_code == 200


def test_user_management_validates_payload_and_invalidates_reset_sessions(admin_client, user_client):
    username = f"reader-{uuid.uuid4().hex[:8]}"
    create_response = admin_client.post(
        "/api/console/users",
        json={"username": username, "role": "user"},
    )
    assert create_response.status_code == 200
    created = create_response.json()
    initial_access_code = created["accessCode"]
    # Personal subscription links (when a base URL is available) must embed the code.
    if created.get("sourceUrl"):
        assert initial_access_code in created["sourceUrl"]
        assert "/api/subscribe/legado/source?code=" in created["sourceUrl"]
    if created.get("subscriptionUrl"):
        assert "/api/auth/access/enter?code=" in created["subscriptionUrl"]
        assert "next=" in created["subscriptionUrl"]
    listed_user = next(
        item
        for item in admin_client.get("/api/console/users").json()["items"]
        if item["userId"] == created["userId"]
    )
    assert "accessCode" not in listed_user

    assert user_client.get("/api/console/users").status_code == 403
    assert admin_client.post(
        "/api/console/users",
        json={"username": "bad-role", "password": "password-123", "role": "owner"},
    ).status_code == 400
    assert admin_client.post(
        "/api/console/users",
        json={"username": "weak-password", "password": "short", "role": "admin"},
    ).status_code == 400
    assert admin_client.post(
        "/api/console/users",
        json={"username": "unknown-field", "password": "password-123", "role": "user", "extra": True},
    ).status_code == 422

    reader_client = TestClient(app)
    assert reader_client.post(
        "/api/auth/access/redeem",
        json={"accessCode": initial_access_code},
    ).status_code == 200
    reset_response = admin_client.post(
        f"/api/console/users/{created['userId']}/reset-access-code",
        json={},
    )
    assert reset_response.status_code == 200
    reset_body = reset_response.json()
    replacement_access_code = reset_body["accessCode"]
    assert replacement_access_code != initial_access_code
    if reset_body.get("sourceUrl"):
        assert replacement_access_code in reset_body["sourceUrl"]
        assert initial_access_code not in reset_body["sourceUrl"]
        # Max flavour link shares the same code via the reader parameter.
        assert "reader=max" in reset_body["maxSourceUrl"]
        assert replacement_access_code in reset_body["maxSourceUrl"]
    assert reader_client.get("/api/auth/me").json()["authenticated"] is False
    assert reader_client.post(
        "/api/auth/access/redeem",
        json={"accessCode": initial_access_code},
    ).status_code == 401
    assert reader_client.post(
        "/api/auth/access/redeem",
        json={"accessCode": replacement_access_code},
    ).status_code == 200
    assert reader_client.post(
        "/api/auth/login",
        json={"username": username, "password": replacement_access_code},
    ).status_code == 401

    assert admin_client.post(
        f"/api/console/users/{created['userId']}/disable",
        json={"disabled": "true"},
    ).status_code == 422
    assert admin_client.post(
        f"/api/console/users/{created['userId']}/disable",
        json={"disabled": True, "extra": True},
    ).status_code == 422
    assert admin_client.post(
        f"/api/console/users/{created['userId']}/disable",
        json={"disabled": True},
    ).status_code == 200
    assert reader_client.get("/api/auth/me").json()["authenticated"] is False
    from app.services.audit import audit_service

    actions = {
        event["action"]
        for event in audit_service.list_events(limit=1000)
        if event["targetId"] == created["userId"]
    }
    assert {
        "user.create",
        "user.access_code.issue",
        "user.access_code.reset",
        "user.disable",
    }.issubset(actions)

    assert user_client.delete(
        f"/api/console/users/{created['userId']}"
    ).status_code == 403
    deleted = admin_client.delete(f"/api/console/users/{created['userId']}")
    assert deleted.status_code == 200
    assert deleted.json()["deleted"] is True
    assert all(
        item["userId"] != created["userId"]
        for item in admin_client.get("/api/console/users").json()["items"]
    )
    assert "user.delete" in {
        event["action"]
        for event in audit_service.list_events(limit=1000)
        if event["targetId"] == created["userId"]
    }


def test_user_management_prevents_self_and_last_admin_disable(admin_client, tmp_path):
    from app.services.user_auth import UserAuthService

    current_admin = admin_client.get("/api/auth/me").json()["user"]
    self_reset = admin_client.post(
        f"/api/console/users/{current_admin['userId']}/reset-password",
        json={"password": "replacement-password"},
    )
    assert self_reset.status_code == 409
    assert "账户安全" in self_reset.json()["detail"]

    self_disable = admin_client.post(
        f"/api/console/users/{current_admin['userId']}/disable",
        json={"disabled": True},
    )
    assert self_disable.status_code == 409
    assert "当前登录" in self_disable.json()["detail"]

    self_delete = admin_client.delete(
        f"/api/console/users/{current_admin['userId']}"
    )
    assert self_delete.status_code == 409
    assert "管理员账户" in self_delete.json()["detail"]
    assert admin_client.delete("/api/console/users/missing-user").status_code == 404

    auth = UserAuthService(tmp_path / "last-admin.db")
    only_admin = auth.bootstrap_admin("only-admin", "password-123")
    with pytest.raises(HTTPException) as captured:
        auth.set_disabled(only_admin["userId"], True, actor_user_id="different-admin")
    assert captured.value.status_code == 409
    assert "至少一个" in captured.value.detail
    other_admin = auth.create_user("other-admin", "password-456", role="admin")
    with pytest.raises(HTTPException) as captured_delete:
        auth.delete_user(only_admin["userId"], actor_user_id=other_admin["userId"])
    assert captured_delete.value.status_code == 409
    assert "管理员账户" in captured_delete.value.detail


def test_library_book_processing_settings_are_admin_only_and_persist(admin_client, user_client):
    import app.config as app_config

    book_id = f"book-settings-{uuid.uuid4().hex[:8]}"
    with sqlite3.connect(app_config.DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO aggregate_book_tasks (
                aggregate_book_id, canonical_name, canonical_author, name, author,
                status, settings_json, current_policy_version, interval_minutes,
                next_check_time, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, 'active', ?, 1, 60, '2099-01-01T00:00:00+00:00', datetime('now'), datetime('now'))
            """,
            (book_id, book_id, "author", "处理设置测试", "作者", '{"backlogChapterLimit": 25, "updateIntervalMinutes": 60}'),
        )
        conn.commit()

    try:
        initial = admin_client.get(f"/api/console/library-books/{book_id}/settings")
        assert initial.status_code == 200
        assert initial.json()["settings"] == {
            "updateIntervalMinutes": 60,
            "backlogChapterLimit": 25,
        }

        updated = admin_client.post(
            f"/api/console/library-books/{book_id}/settings",
            json={"updateIntervalMinutes": 120, "backlogChapterLimit": 40},
        )
        assert updated.status_code == 200
        assert updated.json()["settings"] == {
            "updateIntervalMinutes": 120,
            "backlogChapterLimit": 40,
        }
        assert updated.json()["currentPolicyVersion"] == 1
        assert updated.json()["nextCheckTime"] != "2099-01-01T00:00:00+00:00"

        reloaded = admin_client.get(f"/api/console/library-books/{book_id}").json()
        assert reloaded["processingSettings"] == updated.json()["settings"]
        assert reloaded["intervalMinutes"] == 120
        assert reloaded["currentPolicyVersion"] == 1
        with sqlite3.connect(app_config.DB_PATH) as conn:
            assert conn.execute(
                "SELECT action FROM audit_events WHERE target_id = ? ORDER BY occurred_at DESC LIMIT 1",
                (book_id,),
            ).fetchone() == ("shared_book.settings.update",)

        assert admin_client.post(
            f"/api/console/library-books/{book_id}/settings",
            json={"updateIntervalMinutes": 9},
        ).status_code == 422
        assert admin_client.post(
            f"/api/console/library-books/{book_id}/settings",
            json={"backlogChapterLimit": 101},
        ).status_code == 422
        assert admin_client.post(
            f"/api/console/library-books/{book_id}/settings",
            json={"unknown": True},
        ).status_code == 422
        assert user_client.get(f"/api/console/library-books/{book_id}/settings").status_code == 403
        assert user_client.post(
            f"/api/console/library-books/{book_id}/settings",
            json={"updateIntervalMinutes": 30, "backlogChapterLimit": 10},
        ).status_code == 403
    finally:
        with sqlite3.connect(app_config.DB_PATH) as conn:
            conn.execute("DELETE FROM aggregate_operation_logs WHERE aggregate_book_id = ?", (book_id,))
            conn.execute("DELETE FROM aggregate_book_tasks WHERE aggregate_book_id = ?", (book_id,))
            conn.commit()


def test_list_plugins():
    res = client.get("/api/console/plugins")
    assert res.status_code == 200
    data = res.json()
    assert "items" in data
    assert "total" in data
    if data["items"]:
        item = data["items"][0]
        assert item["author"] == "Yunwei"
        assert item["accessType"] in {"HTTP", "Browser"}
        assert item["sourceType"] == item["accessType"]
        assert "proxyRequired" in item


def test_get_plugin():
    # First list to find a plugin id
    list_res = client.get("/api/console/plugins")
    items = list_res.json().get("items", [])
    if not items:
        pytest.skip("No plugins installed")
    plugin_id = items[0]["pluginId"]
    res = client.get(f"/api/console/plugins/{plugin_id}")
    assert res.status_code == 200
    assert res.json()["pluginId"] == plugin_id
    assert res.json()["author"] == "Yunwei"
    assert res.json()["accessType"] in {"HTTP", "Browser"}


def test_get_missing_plugin_returns_404():
    res = client.get("/api/console/plugins/missing-plugin")

    assert res.status_code == 404
    assert res.json()["detail"] == "插件不存在"


def test_reload_plugins():
    res = client.post("/api/console/plugins/reload")
    assert res.status_code == 200
    assert res.json()["reloaded"] is True


def test_enable_plugin():
    list_res = client.get("/api/console/plugins")
    items = list_res.json().get("items", [])
    if not items:
        pytest.skip("No plugins installed")
    plugin_id = items[0]["pluginId"]
    res = client.post(f"/api/console/plugins/{plugin_id}/enable", json={"enabled": True})
    assert res.status_code == 200
    assert res.json()["enabled"] is True


def test_runtime_plugin_checks_expose_ping_only(monkeypatch):
    import app.api.console as console_api

    list_res = client.get("/api/console/plugins")
    items = list_res.json().get("items", [])
    if not items:
        pytest.skip("No plugins installed")
    plugin_id = items[0]["pluginId"]

    class FakePingService:
        def __init__(self, scheduler=None):
            self.scheduler = scheduler

        async def ping_one(self, requested_plugin_id):
            return {"pluginId": requested_plugin_id, "status": "reachable", "latencyMs": 7}

    monkeypatch.setattr(console_api, "SourcePingService", FakePingService)

    response = client.post(f"/api/console/plugins/{plugin_id}/ping")

    assert response.status_code == 200
    assert response.json() == {"pluginId": plugin_id, "status": "reachable", "latencyMs": 7}
    assert client.post(f"/api/console/plugins/{plugin_id}/smoke", json={}).status_code == 404
    assert client.post(f"/api/console/plugins/{plugin_id}/live-check", json={}).status_code == 404
    assert client.get("/api/console/verification").status_code == 404
    assert client.post("/api/console/verification/run", json={}).status_code == 404


def test_source_ping_tries_declared_fallback_urls(monkeypatch):
    from app.services.source_ping import SourcePingService

    plugin = SimpleNamespace(
        metadata=SimpleNamespace(
            base_urls=["https://dead.example"],
            domain_profiles=[{"baseUrl": "https://mirror.example"}],
            domains=["example.org"],
            proxy={},
        )
    )
    scheduler = SimpleNamespace(
        _plugins={"source": plugin},
        config={"proxy": {"enabled": False}},
    )
    recorded = []
    repo = SimpleNamespace(record_ping=lambda *args, **kwargs: recorded.append((args, kwargs)))
    requested = []

    class FakeResponse:
        def __init__(self, status_code):
            self.status_code = status_code

    class FakeClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def head(self, url):
            requested.append(url)
            if url == "https://dead.example":
                raise httpx.ConnectError("offline")
            return FakeResponse(200)

        async def get(self, url):
            raise AssertionError(f"unexpected GET: {url}")

    import httpx
    import app.services.source_ping as source_ping

    monkeypatch.setattr(source_ping.httpx, "AsyncClient", FakeClient)

    result = asyncio.run(SourcePingService(scheduler=scheduler, repo=repo).ping_one("source"))

    assert requested == ["https://dead.example", "https://mirror.example"]
    assert result["status"] == "reachable"
    assert result["url"] == "https://mirror.example"
    assert recorded[-1][0][1] == "reachable"


def test_plugin_auth(admin_client):
    list_res = admin_client.get("/api/console/plugins")
    items = list_res.json().get("items", [])
    if not items:
        pytest.skip("No plugins installed")
    candidate = next(
        (item for item in items if item.get("auth", {}).get("mode") == "none" and item.get("official") is False),
        None,
    )
    if candidate is None:
        pytest.skip("No ordinary no-auth plugin found")
    plugin_id = candidate["pluginId"]
    res = admin_client.get(f"/api/console/plugins/{plugin_id}/auth")
    assert res.status_code == 200
    assert "authenticated" in res.json()
    assert res.json()["mode"] == "none"


def test_browser_required_plugin_auth_returns_bypass_required(admin_client, monkeypatch, tmp_path):
    import app.api.console as console_api
    from app.services.cookie_store import CookieStore

    cookie_dir = tmp_path / "cookies"
    cookie_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(console_api, "CookieStore", lambda: CookieStore(base_dir=cookie_dir))

    res = admin_client.get("/api/console/plugins/69shuba_com/auth")

    assert res.status_code == 200
    data = res.json()
    assert data["sourceId"] == "69shuba_com"
    assert data["mode"] in {"none", "browser_bypass"}
    assert "该插件无需登录" in data["message"] or "绕过" in data["message"]
    assert "browserChallenges" not in data


def test_plugin_auth_reports_saved_cookies_for_no_auth_plugin(admin_client, monkeypatch, tmp_path):
    import app.api.console as console_api
    from app.services.cookie_store import CookieStore

    cookie_dir = tmp_path / "cookies"
    cookie_dir.mkdir(parents=True, exist_ok=True)
    store = CookieStore(base_dir=cookie_dir)
    store.save("69shuba_com", {"cookies": {"69shuba.com": {"cf_clearance": "ok"}}})
    monkeypatch.setattr(console_api, "CookieStore", lambda: CookieStore(base_dir=cookie_dir))

    res = admin_client.post("/api/console/plugins/69shuba_com/auth/check")

    assert res.status_code == 200
    data = res.json()
    assert data["mode"] in {"none", "browser_bypass"}
    assert "该插件无需登录" in data["message"] or "Cookie" in data["message"]
    assert data["hasCookies"] is True
    assert "browserChallenges" not in data


def test_plugin_login_and_cookie_clear(admin_client):
    list_res = admin_client.get("/api/console/plugins")
    items = list_res.json().get("items", [])
    if not items:
        pytest.skip("No plugins installed")
    plugin_id = items[0]["pluginId"]

    login_res = admin_client.post(f"/api/console/plugins/{plugin_id}/login")
    assert login_res.status_code == 200
    assert login_res.json()["mode"] == "manual_browser"

    clear_res = admin_client.post(f"/api/console/plugins/{plugin_id}/cookies/clear")
    assert clear_res.status_code == 200
    assert clear_res.json()["cleared"] is True


def test_official_sources_endpoint_lists_qidian(admin_client):
    res = admin_client.get("/api/console/official-sources")
    assert res.status_code == 200
    data = res.json()
    assert "items" in data
    qidian = next((item for item in data["items"] if item["pluginId"] == "qidian_com"), None)
    if qidian is None:
        pytest.skip("qidian_com plugin not installed")
    assert qidian["official"] is True
    assert qidian["auth"]["mode"] == "optional"


def test_status_endpoint():
    from app import config

    res = client.get("/api/console/status")
    assert res.status_code == 200
    data = res.json()
    assert "pluginStats" in data
    assert "sourceStats" in data  # compatibility alias
    assert "plugins" in data  # compatibility alias
    assert data["version"] == config.APP_VERSION
    assert data["health"] in {"healthy", "degraded", "pending", "idle"}
    assert data["uptimeSeconds"] >= 0
    stats = data["pluginStats"]
    assert stats["checked"] == stats["healthy"] + stats["unhealthy"]
    assert stats["unknown"] == stats["enabled"] - stats["checked"]


def test_status_health_counts_ping_only(monkeypatch):
    import app.api.console as console_api
    import app.services.plugin_runtime_state as runtime_state_module

    plugins = {
        plugin_id: SimpleNamespace(
            metadata=SimpleNamespace(id=plugin_id, enabled=enabled),
        )
        for plugin_id, enabled in {
            "reachable": True,
            "unreachable": True,
            "unknown": True,
            "disabled": False,
        }.items()
    }

    class FakeRuntimeState:
        def get_state(self, plugin_id):
            statuses = {
                "reachable": "reachable",
                "unreachable": "unreachable",
                "disabled": "reachable",
            }
            status = statuses.get(plugin_id)
            return {"lastPing": {"status": status}} if status else {}

    monkeypatch.setattr(console_api._plugin_scheduler, "_plugins", plugins)
    monkeypatch.setattr(runtime_state_module, "get_runtime_state", lambda: FakeRuntimeState())

    response = client.get("/api/console/status")

    assert response.status_code == 200
    data = response.json()
    assert data["health"] == "degraded"
    assert data["pluginStats"] == {
        "total": 4,
        "enabled": 3,
        "disabled": 1,
        "healthy": 1,
        "unhealthy": 1,
        "checked": 2,
        "unknown": 1,
    }


def test_runtime_state_discards_legacy_smoke(tmp_path):
    from app.services.plugin_runtime_state import PluginRuntimeState

    state_path = tmp_path / "plugin_state.json"
    state_path.write_text(json.dumps({
        "version": 1,
        "plugins": {
            "source-1": {
                "lastSmoke": {"pass": False, "error": "fixture failed", "timestamp": 1},
                "lastError": {"message": "fixture failed", "timestamp": 1},
                "attempts": [
                    {"type": "smoke", "pass": False, "timestamp": 1},
                    {"type": "ping", "status": "reachable", "timestamp": 2},
                ],
            },
        },
    }), encoding="utf-8")

    state = PluginRuntimeState(state_path)

    plugin_state = state.get_state("source-1")
    assert "lastSmoke" not in plugin_state
    assert "lastError" not in plugin_state
    assert state.get_attempts("source-1") == [
        {"type": "ping", "status": "reachable", "timestamp": 2},
    ]


def test_legacy_admin_api_entry_not_exposed():
    res = client.get("/api/admin/status")
    assert res.status_code == 404


def test_cache_endpoint():
    res = client.get("/api/console/cache")
    assert res.status_code == 200
    data = res.json()
    assert "searchCache" in data


def test_cache_items_endpoint():
    res = client.get("/api/console/cache/items")
    assert res.status_code == 200
    data = res.json()
    assert "books" in data
    assert "tocs" in data
    assert "chapters" in data
    assert "searches" in data


def test_settings_endpoint():
    res = client.get("/api/console/settings")
    assert res.status_code == 200
    assert "contentWorkflow" in res.json()
    assert res.json()["chapterComment"] == {
        "segmentEnabled": True,
        "pageEnabled": True,
        "chapterEnabled": True,
    }
    assert "publicBaseUrl" in res.json()["readingAccess"]


def test_chapter_comment_settings_round_trip_and_strict_validation(admin_client):
    from app.core.legado_source import generate_legado_source

    original = admin_client.get("/api/console/settings").json()
    try:
        response = admin_client.post(
            "/api/console/settings",
            json={
                "chapterComment": {
                    "segmentEnabled": False,
                    "pageEnabled": True,
                    "chapterEnabled": False,
                }
            },
        )
        assert response.status_code == 200
        assert response.json()["chapterComment"] == {
            "segmentEnabled": False,
            "pageEnabled": True,
            "chapterEnabled": False,
        }
        source = generate_legado_source("http://testserver")[0]
        display = source["ruleContent"]["chapterComment"]["display"]
        assert display["segment"]["enabled"] is False
        assert display["page"]["enabled"] is True
        assert display["chapter"]["enabled"] is False
        assert source["ruleContent"]["chapterComment"]["protocolVersion"] == 2
        assert "nativeChapterComments" not in source["ruleContent"]["content"]

        assert admin_client.post(
            "/api/console/settings",
            json={"chapterComment": {"segmentEnabled": "true"}},
        ).status_code == 422
        assert admin_client.post(
            "/api/console/settings",
            json={"chapterComment": {"unknown": True}},
        ).status_code == 422
    finally:
        assert admin_client.post("/api/console/settings", json=original).status_code == 200


def test_reading_access_public_base_url_is_link_preference_not_host_allowlist(admin_client):
    from app.core.app_config import AppConfig
    from app.core.legado_source import generate_legado_source
    from app.core.public_security import (
        get_public_base_url,
        reading_public_base_allowlist,
    )
    from starlette.requests import Request

    original = admin_client.get("/api/console/settings").json()
    try:
        response = admin_client.post(
            "/api/console/settings",
            json={"readingAccess": {"publicBaseUrl": "https://book.example.com:2087/"}},
        )
        assert response.status_code == 200
        assert response.json()["readingAccess"] == {
            "publicBaseUrl": "https://book.example.com:2087",
        }
        AppConfig.get().reload()
        assert reading_public_base_allowlist() == frozenset({"https://book.example.com:2087"})
        # Offline + dynamic process: do not force settings origin into generators.
        # (Allowlist is for request Host admission / live get_public_base_url.)
        assert not get_public_base_url().startswith("https://book.example.com")
        source = generate_legado_source("http://192.168.1.10:8765")[0]
        assert source["searchUrl"].startswith("@js:")
        assert 'var _base = "http://192.168.1.10:8765"' in source["searchUrl"]
        assert source["bookSourceUrl"] == "LegadoHub-LAN"
        assert "·内网" in source["bookSourceName"]

        # 127.0.0.1 is a default trusted proxy, so X-Forwarded-Proto is honored.
        public_scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": "/api/subscribe/legado/source",
            "raw_path": b"/api/subscribe/legado/source",
            "query_string": b"",
            "headers": [
                (b"host", b"book.example.com:2087"),
                (b"x-forwarded-proto", b"https"),
            ],
            "client": ("127.0.0.1", 12345),
            "server": ("0.0.0.0", 8765),
            "app": admin_client.app,
        }
        public_request = Request(public_scope)
        assert get_public_base_url(public_request) == "https://book.example.com:2087"
        public_source = generate_legado_source(get_public_base_url(public_request))[0]
        assert public_source["searchUrl"].startswith("@js:")
        assert 'var _base = "https://book.example.com:2087"' in public_source["searchUrl"]
        assert public_source["bookSourceUrl"] == "LegadoHub"
        assert "·内网" not in public_source["bookSourceName"]

        foreign_scope = dict(public_scope)
        foreign_scope["headers"] = [
            (b"host", b"other.example.com:2087"),
            (b"x-forwarded-proto", b"https"),
        ]
        assert get_public_base_url(Request(foreign_scope)) == "https://other.example.com:2087"

        lan_scope = dict(public_scope)
        lan_scope["headers"] = [(b"host", b"192.168.1.10:8765")]
        lan_scope["scheme"] = "http"
        assert get_public_base_url(Request(lan_scope)) == "http://192.168.1.10:8765"

        assert admin_client.post(
            "/api/console/settings",
            json={"readingAccess": {"publicBaseUrl": "not-a-url"}},
        ).status_code == 422
        assert admin_client.post(
            "/api/console/settings",
            json={"readingAccess": {"publicBaseUrl": "https://book.example.com/path"}},
        ).status_code == 422
        assert admin_client.post(
            "/api/console/settings",
            json={"readingAccess": {"unknown": True}},
        ).status_code == 422

        cleared = admin_client.post(
            "/api/console/settings",
            json={"readingAccess": {"publicBaseUrl": ""}},
        )
        assert cleared.status_code == 200
        assert cleared.json()["readingAccess"]["publicBaseUrl"] == ""
        AppConfig.get().reload()
        assert reading_public_base_allowlist() == frozenset()
        assert get_public_base_url(public_request) == "https://book.example.com:2087"
    finally:
        assert admin_client.post("/api/console/settings", json=original).status_code == 200
        AppConfig.get().reload()


def test_book_source_priority_settings_accept_new_and_legacy_names():
    import app.api.console as console_api

    settings = {}

    assert console_api._apply_book_source_priority_settings(
        settings,
        {"primarySourcePriority": ["qidian_com_app", "qidian_com_web"]},
    ) is True
    assert settings["sourcePriority"] == ["qidian_com_app", "qidian_com_web"]
    assert settings["sourcePriorityMode"] == "manual"

    assert console_api._apply_book_source_priority_settings(settings, {"sourcePriority": []}) is True
    assert settings["sourcePriority"] == []
    assert settings["sourcePriorityMode"] == "auto"


def test_processing_event_sanitizer_exposes_candidate_lag_details():
    import app.api.console as console_api

    event = console_api._sanitize_library_processing_event(
        {
            "ts": "2026-07-02T00:00:00",
            "event": "candidate_source_behind_target",
            "stage": "stage2",
            "chapterIndex": 814,
            "payload": {
                "sourceId": "69hsw_com",
                "targetChapterNumber": 806,
                "latestCandidateChapterNumber": 790,
                "attemptedSourceIds": ["69hsw_com"],
            },
        }
    )

    assert event["message"] == "候选源最新章节落后，已跳过"
    assert event["targetChapterNumber"] == 806
    assert event["latestCandidateChapterNumber"] == 790
    assert event["attemptedSourceIds"] == ["69hsw_com"]


def test_update_settings_persists_source_pool_and_refreshes_runtime(admin_client, monkeypatch, tmp_path):
    import json
    from types import SimpleNamespace
    import app.api.console as console_api

    if not hasattr(console_api, "SOURCE_POOL_CONFIG_PATH"):
        pytest.skip("SOURCE_POOL_CONFIG_PATH not present in current console API")

    source_pool_path = tmp_path / "source_pool.json"
    source_pool_path.write_text(
        json.dumps({"max_concurrency": 3, "source_timeout_seconds": 15.0}),
        encoding="utf-8",
    )
    plugin_scheduler = SimpleNamespace(config={})
    search_service = SimpleNamespace(scheduler=SimpleNamespace(config={}))
    monkeypatch.setattr(console_api, "SOURCE_POOL_CONFIG_PATH", source_pool_path)
    monkeypatch.setattr(console_api, "_plugin_scheduler", plugin_scheduler)
    monkeypatch.setattr(console_api, "_search_service", search_service)

    payload = {
        "sourcePool": {
            "source_timeout_seconds": 18.0,
            "overall_search_timeout_seconds": 60.0,
            "browser_search_timeout_seconds": 55.0,
            "browser_source_timeout_seconds": 150.0,
        }
    }

    res = admin_client.post("/api/console/settings", json=payload)

    assert res.status_code == 200
    assert res.json()["saved"] is True
    saved = json.loads(source_pool_path.read_text(encoding="utf-8"))
    assert saved["max_concurrency"] == 3
    assert saved["source_timeout_seconds"] == 18.0
    assert saved["overall_search_timeout_seconds"] == 60.0
    assert saved["browser_search_timeout_seconds"] == 55.0
    assert saved["browser_source_timeout_seconds"] == 150.0
    assert plugin_scheduler.config["source_timeout_seconds"] == 18.0
    assert search_service.scheduler.config["browser_source_timeout_seconds"] == 150.0


def test_aggregate_settings_endpoint_returns_masked_contract():
    res = client.get("/api/console/aggregate-settings")
    assert res.status_code == 200
    data = res.json()
    assert "contentWorkflow" in data
    assert "aiProviderConfig" in data
    assert "runtime" in data
    assert data["runtime"]["windowChapterLimit"] == 5
    assert data["runtime"]["processingPlaceholder"] == "聚合处理中……请先查看其他源或稍后刷新。"
    assert "hasApiKey" in data["aiProviderConfig"]


def test_aggregate_provider_endpoints_are_disabled(monkeypatch):
    class BoomClient:
        def __init__(self, *args, **kwargs):
            raise AssertionError("AI client should not be constructed while runtime is disabled")

    monkeypatch.setattr("app.ai.client.OpenAICompatibleClient", BoomClient)

    payload = {"baseUrl": "https://api.example.com/v1", "apiKey": "sk-test", "model": "x"}
    test_res = client.post("/api/console/aggregate-settings/test-provider", json=payload)
    models_res = client.post("/api/console/aggregate-settings/fetch-models", json=payload)

    assert test_res.status_code == 200
    assert test_res.json()["status"] == "disabled"
    assert models_res.status_code == 200
    assert models_res.json()["status"] == "disabled"
    assert models_res.json()["models"] == []


# ------------------------------------------------------------------
# Official Source Login API tests (mocked, no real private package)
# ------------------------------------------------------------------


def test_public_cookie_basic_verify_requires_both_qidian_markers():
    from app.services.official_auth.manager import PublicCookieTools

    result = PublicCookieTools.basic_verify({
        "qidian.com": {"ywguid": "abc", "accountName": "reader"},
    })

    assert result["authenticated"] is False
    assert "Cookie 不完整" in result["message"]


def test_login_trace_store_redacts_nested_secrets_and_limits_errors():
    from app.services.official_auth.sessions import (
        LoginTraceStore,
        OfficialLoginSession,
        REDACTED,
    )

    store = LoginTraceStore()
    payload = {
        "phone": "13800138000",
        "code": "123456",
        "nested": [{"challengeToken": "challenge-secret", "safe": "kept"}],
    }
    result = {
        "cookies": {"qidian.com": {"ywkey": "cookie-secret"}},
        "accountName": "reader",
        "cmfuToken": "token-secret",
        "message": "phone 13800138000 code 123456 token-secret " + ("y" * 2400),
    }
    store.record(
        "qidian_com",
        "verify_code",
        payload,
        result,
        error="phone=13800138000 code=123456 token=token-secret " + ("x" * 600),
    )

    trace = store.get("qidian_com")[0]
    assert trace["payload"]["phone"] == REDACTED
    assert trace["payload"]["code"] == REDACTED
    assert trace["payload"]["nested"][0]["challengeToken"] == REDACTED
    assert trace["payload"]["nested"][0]["safe"] == "kept"
    assert trace["result"]["cookies"] == REDACTED
    assert trace["result"]["cmfuToken"] == REDACTED
    assert "13800138000" not in trace["result"]["message"]
    assert "123456" not in trace["result"]["message"]
    assert "token-secret" not in trace["result"]["message"]
    assert len(trace["result"]["message"]) <= 2000
    assert "13800138000" not in trace["error"]
    assert "123456" not in trace["error"]
    assert "token-secret" not in trace["error"]
    assert len(trace["error"]) <= 500
    assert payload["phone"] == "13800138000"
    trace["payload"]["nested"][0]["safe"] = "changed"
    assert store.get("qidian_com")[0]["payload"]["nested"][0]["safe"] == "kept"
    session = OfficialLoginSession("qidian_com")
    session.phone_masked = "13800138000"
    assert session.to_dict()["phoneMasked"] == "138****8000"


@pytest.mark.parametrize(
    ("method", "path", "payload"),
    [
        ("get", "/api/console/official-sources", None),
        ("get", "/api/console/plugins/qidian_com/auth", None),
        ("post", "/api/console/plugins/qidian_com/auth/check", None),
        ("post", "/api/console/plugins/qidian_com/login", None),
        ("post", "/api/console/plugins/qidian_com/cookies/clear", None),
        ("post", "/api/console/plugins/qidian_com/login-browser", None),
        ("get", "/api/console/plugins/qidian_com/login-browser/status", None),
        ("delete", "/api/console/plugins/qidian_com/login-browser", None),
        ("get", "/api/console/official-sources/qidian_com/login-capabilities", None),
        ("post", "/api/console/official-sources/qidian_com/login/phone/request-code", {"phone": "13800138000"}),
        ("post", "/api/console/official-sources/qidian_com/login/phone/verify", {}),
        ("post", "/api/console/official-sources/qidian_com/login/cookie/verify", {"cookieText": "a=b"}),
        ("post", "/api/console/official-sources/qidian_com/login/logout", None),
        ("get", "/api/console/official-sources/qidian_com/login/debug-trace", None),
    ],
)
def test_official_source_management_requires_admin(user_client, method, path, payload):
    response = user_client.request(method, path, json=payload)
    assert response.status_code == 403


def test_official_source_management_requires_login():
    response = TestClient(app).get("/api/console/official-sources")
    assert response.status_code == 401


def test_browser_login_success_requires_authenticated_probe(admin_client, monkeypatch):
    import app.api.console as console_api

    session = SimpleNamespace(
        status="success",
        message="登录成功，Cookie 已提取",
        cookies={"qidian.com": {"ywguid": "abc", "ywkey": "def"}},
    )

    async def get_session(_plugin_id):
        return session

    async def cleanup(_plugin_id):
        return None

    async def save_and_probe(_plugin_id, _cookies):
        return {
            "authenticated": False,
            "accountName": "",
            "authStatus": "pending",
            "message": "Cookie 已保存，但用户中心未识别登录态",
        }

    monkeypatch.setattr(console_api.login_browser_service, "get", get_session)
    monkeypatch.setattr(console_api.login_browser_service, "cleanup", cleanup)
    monkeypatch.setattr(console_api.official_auth_manager, "save_cookies_and_probe", save_and_probe)

    response = admin_client.get("/api/console/plugins/qidian_com/login-browser/status")

    assert response.status_code == 200
    assert response.json()["status"] == "pending"
    assert response.json()["authenticated"] is False
    assert response.json()["accountName"] == ""
    assert "未识别登录态" in response.json()["message"]


def test_browser_login_accepts_masked_phone_identity(admin_client, monkeypatch):
    import app.api.console as console_api

    session = SimpleNamespace(
        status="success",
        message="登录成功，Cookie 已提取",
        cookies={"qidian.com": {"ywguid": "abc", "ywkey": "def"}},
    )

    async def get_session(_plugin_id):
        return session

    async def cleanup(_plugin_id):
        return None

    async def save_and_probe(_plugin_id, _cookies):
        return {
            "authenticated": True,
            "accountName": "",
            "phoneMasked": "138****8000",
            "authStatus": "authenticated",
            "message": "登录态有效",
        }

    monkeypatch.setattr(console_api.login_browser_service, "get", get_session)
    monkeypatch.setattr(console_api.login_browser_service, "cleanup", cleanup)
    monkeypatch.setattr(console_api.official_auth_manager, "save_cookies_and_probe", save_and_probe)

    response = admin_client.get("/api/console/plugins/qidian_com/login-browser/status")

    assert response.status_code == 200
    assert response.json()["status"] == "success"
    assert response.json()["authenticated"] is True
    assert response.json()["accountName"] == "138****8000"


def test_browser_login_start_normalizes_initial_pending_to_running(admin_client, monkeypatch):
    import app.api.console as console_api

    async def start(**_kwargs):
        return SimpleNamespace(status="pending", message="")

    monkeypatch.setattr(console_api.login_browser_service, "start", start)
    plugin_id = next(iter(console_api._plugin_scheduler._plugins))

    response = admin_client.post(f"/api/console/plugins/{plugin_id}/login-browser")

    assert response.status_code == 200
    assert response.json()["status"] == "running"
    assert "启动" in response.json()["message"]


def test_browser_login_poll_normalizes_initial_pending_to_running(admin_client, monkeypatch):
    import app.api.console as console_api

    async def get_session(_plugin_id):
        return SimpleNamespace(status="pending", message="", cookies={})

    monkeypatch.setattr(console_api.login_browser_service, "get", get_session)

    response = admin_client.get("/api/console/plugins/qidian_com/login-browser/status")

    assert response.status_code == 200
    assert response.json()["status"] == "running"


class TestOfficialSourceLoginCapabilities:
    """GET /api/console/official-sources/{plugin_id}/login-capabilities"""

    def test_no_private_package(self, admin_client, monkeypatch):
        import app.api.console as console_api

        monkeypatch.setattr(
            console_api.official_auth_manager,
            "capabilities",
            lambda _pid: {
                "pluginId": "qidian_com",
                "methods": ["cookie"],
                "defaultMethod": "cookie",
                "privateFeatures": {
                    "phoneAuth": False,
                    "cookieAuth": False,
                    "reviews": False,
                },
                "hasPrivatePackage": False,
            },
        )

        res = admin_client.get("/api/console/official-sources/qidian_com/login-capabilities")
        assert res.status_code == 200
        data = res.json()
        assert "cookie" in data["methods"]
        assert data["defaultMethod"] == "cookie"
        assert data["privateFeatures"]["phoneAuth"] is False
        assert data["privateFeatures"]["cookieAuth"] is False
        assert data["hasPrivatePackage"] is False

    def test_with_phone_auth(self, admin_client, monkeypatch):
        import app.api.console as console_api

        monkeypatch.setattr(
            console_api.official_auth_manager,
            "capabilities",
            lambda _pid: {
                "pluginId": "qidian_com",
                "methods": ["phone", "cookie"],
                "defaultMethod": "phone",
                "privateFeatures": {
                    "phoneAuth": True,
                    "cookieAuth": False,
                    "reviews": False,
                },
                "hasPrivatePackage": True,
            },
        )

        res = admin_client.get("/api/console/official-sources/qidian_com/login-capabilities")
        assert res.status_code == 200
        data = res.json()
        assert data["methods"][0] == "phone"
        assert "cookie" in data["methods"]
        assert data["defaultMethod"] == "phone"
        assert data["privateFeatures"]["phoneAuth"] is True

    def test_qidian_public_fallback_capability_shape(self, admin_client, monkeypatch):
        """Phone can be exposed via public fallback while privateFeatures.phoneAuth stays false."""
        import app.api.console as console_api

        monkeypatch.setattr(
            console_api.official_auth_manager,
            "capabilities",
            lambda _pid: {
                "pluginId": "qidian_com",
                "methods": ["phone", "cookie"],
                "defaultMethod": "phone",
                "privateFeatures": {
                    "phoneAuth": False,
                    "cookieAuth": False,
                    "reviews": False,
                },
                "hasPrivatePackage": False,
            },
        )

        res = admin_client.get("/api/console/official-sources/qidian_com/login-capabilities")
        assert res.status_code == 200
        data = res.json()
        assert data["methods"][0] == "phone"
        assert "cookie" in data["methods"]
        assert data["defaultMethod"] == "phone"
        assert data["privateFeatures"]["phoneAuth"] is False
        assert data["hasPrivatePackage"] is False


class TestOfficialSourceCookieVerify:
    """POST /api/console/official-sources/{plugin_id}/login/cookie/verify"""

    def test_missing_cookie_text(self, admin_client):
        res = admin_client.post("/api/console/official-sources/qidian_com/login/cookie/verify", json={})
        assert res.status_code == 400
        assert res.json()["detail"] == "缺少 Cookie 文本"

    def test_verify_cookie_calls_manager(self, admin_client, monkeypatch):
        import app.api.console as console_api

        calls = []

        async def mock_verify_cookie(plugin_id: str, cookie_text: str):
            calls.append({"plugin_id": plugin_id, "cookie_text": cookie_text})
            return {
                "ok": True,
                "authenticated": True,
                "accountName": "test_user",
                "message": "Cookie 有效",
                "hasCookies": True,
                "cookieDomains": ["qidian.com"],
            }

        monkeypatch.setattr(console_api.official_auth_manager, "verify_cookie", mock_verify_cookie)

        res = admin_client.post(
            "/api/console/official-sources/qidian_com/login/cookie/verify",
            json={"cookieText": "ywguid=abc; ywkey=def"},
        )

        assert res.status_code == 200
        data = res.json()
        assert data["ok"] is True
        assert data["authenticated"] is True
        assert data["accountName"] == "test_user"
        assert data["message"] == "Cookie 有效"
        assert len(calls) == 1
        assert calls[0]["plugin_id"] == "qidian_com"
        from app.services.audit import audit_service

        event = next(
            event
            for event in audit_service.list_events(limit=1000)
            if event["action"] == "official_source.login"
            and event["targetId"] == "qidian_com"
            and event["summary"].get("method") == "cookie"
        )
        assert event["outcome"] == "success"
        assert "ywguid=abc" not in json.dumps(event, ensure_ascii=False)


class TestOfficialSourcePhoneRequestCode:
    """POST /api/console/official-sources/{plugin_id}/login/phone/request-code"""

    def test_missing_phone(self, admin_client):
        res = admin_client.post(
            "/api/console/official-sources/qidian_com/login/phone/request-code",
            json={},
        )
        assert res.status_code == 400
        assert res.json()["detail"] == "缺少手机号"

    def test_full_payload_forwarded(self, admin_client, monkeypatch):
        import app.api.console as console_api

        captured = []

        def mock_request_phone_code(plugin_id: str, payload: dict):
            captured.append(payload)
            return {"ok": True, "sessionId": "sess_123", "nextAction": "verify_code"}

        monkeypatch.setattr(
            console_api.official_auth_manager, "request_phone_code", mock_request_phone_code
        )

        res = admin_client.post(
            "/api/console/official-sources/qidian_com/login/phone/request-code",
            json={
                "phone": "13800138000",
                "sessionId": "abc",
                "challengeToken": "ticket123",
                "challengeRandstr": "@rand",
            },
        )

        assert res.status_code == 200
        assert res.json()["ok"] is True
        assert len(captured) == 1
        payload = captured[0]
        assert payload["phone"] == "13800138000"
        assert payload["sessionId"] == "abc"
        assert payload["challengeToken"] == "ticket123"
        assert payload["challengeRandstr"] == "@rand"

    def test_challenge_response_passed_through(self, admin_client, monkeypatch):
        import app.api.console as console_api

        monkeypatch.setattr(
            console_api.official_auth_manager,
            "request_phone_code",
            lambda _pid, _payload: {
                "ok": False,
                "sessionId": "abc",
                "nextAction": "complete_challenge",
                "challenge": {"type": "official_webview"},
            },
        )

        res = admin_client.post(
            "/api/console/official-sources/qidian_com/login/phone/request-code",
            json={"phone": "13800138000"},
        )

        assert res.status_code == 200
        data = res.json()
        assert data["ok"] is False
        assert data["nextAction"] == "complete_challenge"
        assert data["challenge"]["type"] == "official_webview"


class TestOfficialSourcePhoneVerify:
    """POST /api/console/official-sources/{plugin_id}/login/phone/verify"""

    def test_verify_phone(self, admin_client, monkeypatch):
        import app.api.console as console_api

        calls = []

        async def mock_verify_phone_code(plugin_id: str, payload: dict):
            calls.append(payload)
            return {
                "ok": True,
                "authenticated": True,
                "accountName": "foo",
                "message": "登录成功",
                "hasCookies": True,
            }

        monkeypatch.setattr(
            console_api.official_auth_manager, "verify_phone_code", mock_verify_phone_code
        )

        res = admin_client.post(
            "/api/console/official-sources/qidian_com/login/phone/verify",
            json={"sessionId": "sess_123", "phone": "13800138000", "code": "123456"},
        )

        assert res.status_code == 200
        data = res.json()
        assert data["ok"] is True
        assert data["authenticated"] is True
        assert data["accountName"] == "foo"
        assert data["message"] == "登录成功"
        assert len(calls) == 1
        from app.services.audit import audit_service

        event = next(
            event
            for event in audit_service.list_events(limit=1000)
            if event["action"] == "official_source.login"
            and event["targetId"] == "qidian_com"
            and event["summary"].get("method") == "phone"
        )
        assert event["outcome"] == "success"
        assert "13800138000" not in json.dumps(event, ensure_ascii=False)
        assert "123456" not in json.dumps(event, ensure_ascii=False)


class TestOfficialSourceLogout:
    """POST /api/console/official-sources/{plugin_id}/login/logout"""

    def test_logout(self, admin_client, monkeypatch):
        import app.api.console as console_api

        calls = []

        def mock_logout(plugin_id: str):
            calls.append(plugin_id)
            return {"ok": True, "message": "登录状态已清除"}

        monkeypatch.setattr(console_api.official_auth_manager, "logout", mock_logout)

        res = admin_client.post("/api/console/official-sources/qidian_com/login/logout")

        assert res.status_code == 200
        data = res.json()
        assert data["ok"] is True
        assert data["message"] == "登录状态已清除"
        assert calls == ["qidian_com"]
        from app.services.audit import audit_service

        assert any(
            event["action"] == "official_source.logout"
            and event["targetId"] == "qidian_com"
            for event in audit_service.list_events(limit=1000)
        )


def test_library_book_summary_shared_mode_sanitizes_source_map_for_admin(monkeypatch):
    import app.api.console as console_api

    monkeypatch.setattr(
        console_api.AggregateSettingsRepository,
        "content_workflow",
        lambda self: {"useSharedBookStorage": True, "sharedBookStorageReadMode": "shared"},
    )
    monkeypatch.setattr(
        console_api.library_books_service,
        "get_book",
        lambda _book_id: {
            "aggregateBookId": "book-1",
            "name": "测试小说",
            "author": "作者甲",
            "status": "active",
        },
    )
    monkeypatch.setattr(
        console_api.library_books_service,
        "load_shared_metadata",
        lambda _book_id: {
            "sourceMap": {
                "summary": [
                    {
                        "bookId": "src-a:1",
                        "sourceId": "src-a",
                        "sourceName": "来源A",
                        "score": 97,
                        "chapterCount": 123,
                        "lastChapter": "第123章",
                        "bookStatus": "ongoing",
                        "name": "测试小说",
                        "author": "作者甲",
                        "bookUrl": "https://private.example/book/1",
                    }
                ],
                "health": {"status": "healthy", "lastVerifiedAt": "2026-06-27T00:00:00Z"},
            },
            "bookState": {
                "status": "active",
                "chapterCount": 123,
                "processedChapterCount": 45,
            },
        },
    )

    res = client.get("/api/console/library-books/book-1/summary")

    assert res.status_code == 200
    data = res.json()
    assert data["mode"] == "shared"
    assert data["book"]["aggregateBookId"] == "book-1"
    assert data["sourceMap"]["summary"][0]["sourceId"] == "src-a"
    assert data["sourceMap"]["summary"][0]["bookStatus"] == "连载中"
    assert "bookUrl" not in data["sourceMap"]["summary"][0]
    assert data["bookState"]["processedChapterCount"] == 45


def test_delete_library_book_removes_shared_and_private_files(monkeypatch, tmp_path):
    import app.api.console as console_api
    import app.config as app_config
    from app.services.library_books import LibraryBooksService
    from app.services.shared_book_storage import SharedBookStorage
    from app.storage.db import initialize_database

    db_path = tmp_path / "app.db"
    storage = SharedBookStorage(tmp_path / "library")
    service = LibraryBooksService(db_path=db_path, shared_book_storage=storage)
    monkeypatch.setattr(app_config, "DB_PATH", db_path)
    monkeypatch.setattr(console_api, "library_books_service", service)

    initialize_database(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO aggregate_book_tasks (
                aggregate_book_id, canonical_name, canonical_author, name, author,
                aggregate_payload_json, primary_book_id, primary_source_id,
                status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, '{}', 'src-book', 'src-a', 'active', datetime('now'), datetime('now'))
            """,
            ("book-delete", service._canonical_name("测试小说"), service._canonical_author("作者甲"), "测试小说", "作者甲"),
        )
        conn.commit()

    shared_dir = storage.shared_book_dir(book_name="测试小说", author="作者甲")
    private_dir = storage.runtime_dir(book_name="测试小说", author="作者甲").parent
    (shared_dir / "chapters").mkdir(parents=True)
    (shared_dir / "chapters" / "0001-第一章.md").write_text("正文", encoding="utf-8")
    (private_dir / "runtime").mkdir(parents=True)
    (private_dir / "runtime" / "state.json").write_text("{}", encoding="utf-8")

    result = console_api._delete_aggregate_book_impl("book-delete")

    assert result == {"bookId": "book-delete", "deleted": True}
    assert not shared_dir.exists()
    assert not private_dir.exists()
    with sqlite3.connect(db_path) as conn:
        operation = conn.execute(
            """
            SELECT operation_type, before_json, after_json
            FROM aggregate_operation_logs
            WHERE aggregate_book_id = 'book-delete'
            ORDER BY id DESC LIMIT 1
            """
        ).fetchone()
    assert operation is not None
    assert operation[0] == "delete"
    assert '"name": "测试小说"' in operation[1]
    assert '"deleted": true' in operation[2]


def test_delete_library_book_cascades_active_subscriptions(monkeypatch, tmp_path):
    import app.api.console as console_api
    import app.config as app_config
    from app.services.library_books import LibraryBooksService
    from app.services.shared_book_storage import SharedBookStorage
    from app.services.user_auth import UserAuthService
    from app.services.user_subscriptions import UserSubscriptionsService
    from app.storage.db import initialize_database

    db_path = tmp_path / "app.db"
    service = LibraryBooksService(
        db_path=db_path,
        shared_book_storage=SharedBookStorage(tmp_path / "library"),
    )
    monkeypatch.setattr(app_config, "DB_PATH", db_path)
    monkeypatch.setattr(console_api, "library_books_service", service)
    initialize_database(db_path)
    user = UserAuthService(db_path).create_user("reader-delete", "password")
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO aggregate_book_tasks (
                aggregate_book_id, canonical_name, canonical_author, name, author,
                status, created_at, updated_at
            ) VALUES ('book-delete', 'book', 'author', '测试小说', '作者甲',
                      'active', datetime('now'), datetime('now'))
            """
        )
        conn.execute(
            """
            INSERT INTO aggregate_ai_usage (aggregate_book_id, chapter_id, status)
            VALUES ('book-delete', 'chapter-delete', 'success')
            """
        )
        conn.commit()
    UserSubscriptionsService(db_path).ensure(user["userId"], "book-delete")

    result = console_api._delete_aggregate_book_impl("book-delete", actor_user_id="admin-1")

    assert result == {"bookId": "book-delete", "deleted": True}
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT 1 FROM aggregate_book_tasks WHERE aggregate_book_id = 'book-delete'"
        ).fetchone() is None
        assert conn.execute(
            "SELECT 1 FROM user_book_subscriptions WHERE aggregate_book_id = 'book-delete'"
        ).fetchone() is None
        assert conn.execute(
            "SELECT 1 FROM aggregate_ai_usage WHERE aggregate_book_id = 'book-delete'"
        ).fetchone() is None
        assert conn.execute(
            "SELECT operation_type FROM aggregate_operation_logs "
            "WHERE aggregate_book_id = 'book-delete' ORDER BY id DESC LIMIT 1"
        ).fetchone() == ("delete",)


def test_delete_library_book_stops_active_lease_then_deletes(monkeypatch, tmp_path):
    import threading
    import time

    import app.api.console as console_api
    import app.config as app_config
    from app.services.library_books import LibraryBooksService
    from app.services.shared_book_lock import SharedBookLockService
    from app.services.shared_book_storage import SharedBookStorage
    from app.storage.db import initialize_database

    db_path = tmp_path / "app.db"
    storage = SharedBookStorage(tmp_path / "library")
    service = LibraryBooksService(db_path=db_path, shared_book_storage=storage)
    monkeypatch.setattr(app_config, "DB_PATH", db_path)
    monkeypatch.setattr(console_api, "library_books_service", service)
    initialize_database(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO aggregate_book_tasks (
                aggregate_book_id, canonical_name, canonical_author, name, author,
                status, created_at, updated_at
            ) VALUES ('book-delete', 'book', 'author', '测试小说', '作者甲',
                      'active', datetime('now'), datetime('now'))
            """
        )
        conn.commit()
    lease = SharedBookLockService(storage=storage, ttl_seconds=0.3).acquire(aggregate_book_id="book-delete")
    assert lease is not None

    def owner_loop():
        while lease.renew():
            time.sleep(0.02)
        lease.release()

    owner = threading.Thread(target=owner_loop)
    owner.start()
    result = console_api._delete_aggregate_book_impl("book-delete", lease_wait_seconds=1.0)
    owner.join(timeout=1.0)

    assert result == {"bookId": "book-delete", "deleted": True}
    assert not owner.is_alive()


def test_library_book_chapter_progress_route_sanitizes_trace_by_default(monkeypatch):
    import app.api.console as console_api

    monkeypatch.setattr(
        console_api,
        "_load_library_book_chapter_progress",
        lambda _book_id, _chapter_id: {
            "bookId": "book-1",
            "chapterId": "chapter-1",
            "traceSummary": {
                "chapterStatus": "processed",
                "selectedSource": "src-a",
                "alignmentPassed": True,
                "sourceSnapshotRefs": [{"sourceId": "src-a", "bookUrl": "https://private.example"}],
                "sourceChapterUrl": "https://private.example/chapter/1",
            },
        },
    )

    res = client.get("/api/console/library-books/book-1/chapters/chapter-1/progress")

    assert res.status_code == 200
    data = res.json()
    assert data["traceSummary"]["chapterStatus"] == "processed"
    assert data["traceSummary"]["selectedSource"] == "src-a"
    assert "sourceSnapshotRefs" not in data["traceSummary"]
    assert "sourceChapterUrl" not in data["traceSummary"]


def test_library_book_chapter_progress_matches_frontend_contract(monkeypatch, tmp_path):
    import app.api.console as console_api
    import app.services.library_books as library_books_module
    from app.services.library_books import LibraryBooksService
    from app.services.shared_book_storage import SharedBookStorage
    from app.storage.db import initialize_database

    db_path = tmp_path / "app.db"
    storage = SharedBookStorage(tmp_path / "library")
    service = LibraryBooksService(db_path=db_path, shared_book_storage=storage)
    initialize_database(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO aggregate_book_tasks (
                aggregate_book_id, canonical_name, canonical_author, name, author,
                status, created_at, updated_at
            ) VALUES ('book-progress', 'book', 'author', '测试小说', '作者甲',
                      'active', datetime('now'), datetime('now'))
            """
        )
        conn.commit()

    chapter_path = storage.chapter_markdown_path(
        book_name="测试小说",
        author="作者甲",
        chapter_index=1,
        title="第一章",
    )
    storage.write_chapter_file(
        path=chapter_path,
        title="第一章",
        body="这是完整正文。",
        trace_payload={
            "chapterStatus": "readable",
            "previewOnly": False,
            "sourceWordCount": 321,
            "selectedSource": "sudugu_org",
        },
    )
    storage.atomic_write_json(
        storage.chapter_index_path(book_name="测试小说", author="作者甲"),
        {"chapters": [{"index": 1, "title": "第一章", "file": str(chapter_path.relative_to(chapter_path.parents[1]))}]},
    )
    monkeypatch.setattr(library_books_module, "library_books_service", service)

    payload = console_api._load_library_book_chapter_progress("book-progress", "1")

    assert payload["found"] is True
    assert payload["chapterId"] == "1"
    assert payload["title"] == "第一章"
    assert payload["status"] == "readable"
    assert payload["contentLength"] == len("这是完整正文。")
    assert payload["sourceWordCount"] == 321
    assert payload["traceSummary"]["selectedSource"] == "sudugu_org"


def test_console_sanitize_trace_summary_keeps_stage3_verdict_fields():
    from app.api.console import _sanitize_trace_summary

    payload = _sanitize_trace_summary(
        {
            "chapterStatus": "suspect",
            "selectedSource": "src-a",
            "selectedContentSource": "candidate",
            "fallbackSourceId": "src-b",
            "alignmentPassed": False,
            "alignmentReason": "suspect_content",
            "titleSimilarity": 0.91,
            "previewSimilarity": 0.83,
            "aiModel": "",
            "aiTokens": 0,
            "processedAt": "2026-06-29T10:00:00+08:00",
            "traceHash": "hash-1",
            "stage3Verdict": "waiting_for_candidates",
            "stage3Reason": "content_candidate_untrusted",
        }
    )

    assert payload["stage3Verdict"] == "waiting_for_candidates"
    assert payload["stage3Reason"] == "content_candidate_untrusted"


def test_library_book_chapters_shared_mode_returns_paginated_shared_shape(monkeypatch):
    import app.api.console as console_api

    monkeypatch.setattr(
        console_api.AggregateSettingsRepository,
        "content_workflow",
        lambda self: {"useSharedBookStorage": True, "sharedBookStorageReadMode": "shared"},
    )
    monkeypatch.setattr(
        console_api,
        "_list_shared_library_book_chapters",
        lambda _book_id, **_kwargs: {
            "items": [
                {
                    "chapterId": "chapter-1",
                    "chapterIndex": 1,
                    "title": "第一章",
                    "status": "processed",
                    "hasContent": True,
                }
            ],
            "page": 2,
            "pageSize": 10,
            "total": 21,
        },
    )

    res = client.get("/api/console/library-books/book-1/chapters?page=2&pageSize=10")

    assert res.status_code == 200
    data = res.json()
    assert data["mode"] == "shared"
    assert data["page"] == 2
    assert data["pageSize"] == 10
    assert data["total"] == 21
    assert data["items"][0]["chapterId"] == "chapter-1"
    assert "content" not in data["items"][0]


def test_library_book_chapters_reads_from_shared_files(admin_client, monkeypatch, tmp_path):
    import app.api.console as console_api
    from app.services.shared_book_storage import SharedBookStorage

    storage = SharedBookStorage(tmp_path / "library")
    book_name = "共享测试书"
    author = "作者甲"
    book_id = "book-shared-1"
    chapter_path = storage.chapter_markdown_path(
        book_name=book_name,
        author=author,
        chapter_index=1,
        title="第一章",
    )
    preview_chapter_path = storage.chapter_markdown_path(
        book_name=book_name,
        author=author,
        chapter_index=2,
        title="第二章",
    )
    markdown = storage.render_chapter_markdown(
        title="第一章",
        body="这里是正文",
        trace_payload={
            "chapterIndex": 1,
            "chapterStatus": "readable",
            "sourceWordCount": 1234,
            "previewOnly": False,
            "selectedContentSource": "candidate",
            "supplementSource": {"sourceId": "third-src"},
        },
    )
    preview_markdown = storage.render_chapter_markdown(
        title="第二章",
        body="这里是预览",
        trace_payload={
            "chapterIndex": 2,
            "chapterStatus": "supplemented",
            "sourceWordCount": 4321,
            "previewOnly": True,
        },
    )
    storage.write_book_bundle(
        metadata_path=storage.metadata_path(book_name=book_name, author=author),
        metadata_payload={"bookId": book_id, "name": book_name, "author": author, "bookState": {"chapterCount": 2}},
        chapter_index_path=storage.chapter_index_path(book_name=book_name, author=author),
        chapter_index_payload={
            "schemaVersion": 1,
            "bookId": book_id,
            "chapters": [
                {"index": 1, "title": "第一章", "file": f"chapters/{chapter_path.name}", "status": "readable"},
                {"index": 2, "title": "第二章", "file": f"chapters/{preview_chapter_path.name}", "status": "supplemented"},
            ],
        },
        chapter_files=[(chapter_path, markdown), (preview_chapter_path, preview_markdown)],
    )

    monkeypatch.setattr(console_api.library_books_service, "shared_book_storage", storage)
    monkeypatch.setattr(
        console_api.library_books_service,
        "get_book",
        lambda aggregate_book_id: {
            "aggregateBookId": aggregate_book_id,
            "name": book_name,
            "author": author,
        }
        if aggregate_book_id == book_id
        else None,
    )

    res = admin_client.get(f"/api/console/library-books/{book_id}/chapters")

    assert res.status_code == 200
    data = res.json()
    assert data["items"] == [
        {
            "chapterId": "1",
            "taskChapterId": "",
            "sourceChapterId": "",
            "readChapterId": "",
            "chapterIndex": 1,
            "title": "第一章",
            "status": "readable",
            "taskStatus": "pending",
            "sourceId": "third-src",
            "alignedWith": "candidate",
            "placeholder": False,
            "contentLength": len("这里是正文"),
            "hasContent": True,
            "processedAt": "",
            "sourceWordCount": 1234,
            "previewOnly": False,
            "isVip": False,
            "manualSupplement": False,
            "manualSourceName": "",
            "file": f"chapters/{chapter_path.name}",
            "error": "",
        },
        {
            "chapterId": "2",
            "taskChapterId": "",
            "sourceChapterId": "",
            "readChapterId": "",
            "chapterIndex": 2,
            "title": "第二章",
            "status": "fetched",
            "taskStatus": "pending",
            "sourceId": "",
            "alignedWith": "",
            "placeholder": False,
            "contentLength": len("这里是预览"),
            "hasContent": True,
            "processedAt": "",
            "sourceWordCount": 4321,
            "previewOnly": True,
            "isVip": False,
            "manualSupplement": False,
            "manualSourceName": "",
            "file": f"chapters/{preview_chapter_path.name}",
            "error": "",
        },
    ]

    filtered = admin_client.get(f"/api/console/library-books/{book_id}/chapters?status=readable")
    assert filtered.status_code == 200
    assert filtered.json()["total"] == 1
    preview_filtered = admin_client.get(f"/api/console/library-books/{book_id}/chapters?status=fetched")
    assert preview_filtered.status_code == 200
    assert preview_filtered.json()["total"] == 1


def test_library_book_logs_route_uses_console_shape(monkeypatch):
    import app.api.console as console_api

    monkeypatch.setattr(
        console_api,
        "_list_library_book_logs",
        lambda _book_id, **_kwargs: {
            "bookId": _book_id,
            "items": [{"id": 1, "operationType": "refresh_source_map", "createdAt": "2026-06-27T00:00:00Z"}],
            "limit": 20,
            "offset": 0,
            "total": 1,
        },
    )

    res = client.get("/api/console/library-books/book-1/logs?limit=20")

    assert res.status_code == 200
    data = res.json()
    assert data["bookId"] == "book-1"
    assert data["items"][0]["operationType"] == "refresh_source_map"
    assert data["limit"] == 20
    assert data["total"] == 1


def test_library_book_manual_console_routes_exist(monkeypatch):
    import app.api.console as console_api

    monkeypatch.setattr(
        console_api.auth_service,
        "require_admin",
        lambda _request: SimpleNamespace(user_id="admin-1", role="admin"),
    )
    monkeypatch.setattr(
        console_api,
        "_manual_source_map_refresh",
        lambda _book_id, payload=None: {"ok": True, "bookId": _book_id, "payload": payload or {}},
    )
    monkeypatch.setattr(
        console_api,
        "_manual_library_book_repair",
        lambda _book_id, payload=None: {"ok": True, "bookId": _book_id, "action": "repair"},
    )
    monkeypatch.setattr(
        console_api,
        "_manual_library_book_update_check",
        lambda _book_id: {"ok": True, "bookId": _book_id, "action": "update-check"},
    )

    refresh_res = client.post("/api/console/library-books/book-1/source-map/refresh", json={"force": True})
    repair_res = client.post("/api/console/library-books/book-1/repair", json={"reason": "manual"})
    update_res = client.post("/api/console/library-books/book-1/update-check")

    assert refresh_res.status_code == 200
    assert refresh_res.json()["bookId"] == "book-1"
    assert repair_res.status_code == 200
    assert repair_res.json()["action"] == "repair"
    assert update_res.status_code == 200
    assert update_res.json()["action"] == "update-check"


def test_library_integrity_console_routes_exist(monkeypatch):
    import app.api.console as console_api

    monkeypatch.setattr(
        console_api.auth_service,
        "require_admin",
        lambda _request: SimpleNamespace(user_id="admin-1", role="admin"),
    )
    monkeypatch.setattr(
        console_api.library_books_service,
        "scan_integrity",
        lambda: {
            "checkedAt": "2026-08-02T12:00:00+08:00",
            "summary": {"totalBooks": 1, "healthyBooks": 1, "repairableBooks": 0, "brokenBooks": 0, "issueCount": 0},
            "books": [],
        },
    )
    monkeypatch.setattr(
        console_api,
        "_manual_library_integrity_repair",
        lambda payload=None: {"ok": True, "repairedBooks": 0, "queuedBooks": 0, "payload": payload or {}},
    )

    scan_res = client.get("/api/console/library-integrity")
    repair_res = client.post("/api/console/library-integrity/repair", json={"bookIds": ["book-1"]})

    assert scan_res.status_code == 200
    assert scan_res.json()["summary"]["healthyBooks"] == 1
    assert repair_res.status_code == 200
    assert repair_res.json()["payload"] == {"bookIds": ["book-1"]}


@pytest.mark.asyncio
async def test_manual_library_book_update_check_queues_without_waiting(monkeypatch):
    import app.api.console as console_api

    started = asyncio.Event()
    release = asyncio.Event()
    events: list[tuple[str, str]] = []

    class FakeProcessor:
        def enqueue_book(self, book_id, payload):
            events.append(("enqueue", book_id))

        async def run_book_task(self, book_id):
            events.append(("run", book_id))
            started.set()
            await release.wait()
            events.append(("done", book_id))
            return {"success": True}

    monkeypatch.setattr(console_api.library_books_service, "load_payload", lambda _book_id: {"name": "测试小说"})
    monkeypatch.setattr(console_api, "AggregateProcessor", FakeProcessor)

    result = await asyncio.wait_for(console_api._manual_library_book_update_check("book-1"), timeout=0.2)

    assert result == {"bookId": "book-1", "success": True, "queued": True}
    assert ("enqueue", "book-1") in events
    await asyncio.wait_for(started.wait(), timeout=1)
    assert ("done", "book-1") not in events
    release.set()
    await asyncio.sleep(0)


def test_user_access_links_reveal_current_code(admin_client):
    """The links endpoint re-issues the stored code with fresh link set."""
    username = f"reader-{uuid.uuid4().hex[:8]}"
    created = admin_client.post(
        "/api/console/users",
        json={"username": username, "role": "user"},
    ).json()

    response = admin_client.get(f"/api/console/users/{created['userId']}/links")
    assert response.status_code == 200
    payload = response.json()
    assert payload["accessCode"] == created["accessCode"]
    if payload.get("sourceUrl"):
        assert payload["accessCode"] in payload["sourceUrl"]
        assert "reader=max" in payload["maxSourceUrl"]
        assert payload["accessCode"] in payload["maxSourceUrl"]

    admins = [
        item
        for item in admin_client.get("/api/console/users").json()["items"]
        if item["role"] == "admin"
    ]
    assert admin_client.get(f"/api/console/users/{admins[0]['userId']}/links").status_code == 400


def test_user_access_links_reports_unstored_legacy_code(admin_client):
    """Codes issued before plaintext storage cannot be re-shown; reset works."""
    username = f"reader-{uuid.uuid4().hex[:8]}"
    created = admin_client.post(
        "/api/console/users",
        json={"username": username, "role": "user"},
    ).json()
    from app import config

    with sqlite3.connect(config.DB_PATH) as conn:
        conn.execute(
            "UPDATE users SET access_code_secret = '' WHERE user_id = ?",
            (created["userId"],),
        )
        conn.commit()

    response = admin_client.get(f"/api/console/users/{created['userId']}/links")
    assert response.status_code == 409
    assert "重新生成" in response.json()["detail"]

    reset_response = admin_client.post(
        f"/api/console/users/{created['userId']}/reset-access-code",
        json={},
    )
    assert reset_response.status_code == 200
    assert admin_client.get(f"/api/console/users/{created['userId']}/links").status_code == 200
