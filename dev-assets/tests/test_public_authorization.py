from __future__ import annotations

import base64
import hashlib
import json
import sqlite3
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app import config
from app.core.app_config import AppConfig, AppConfigLoadError
from app.main import app
from app.services.user_auth import (
    AuthRateLimitError,
    AuthRateLimiter,
    UserAuthService,
    auth_rate_limiter,
    auth_service,
)


def _create_access_user() -> dict:
    username = f"access-{uuid.uuid4().hex[:10]}"
    return auth_service.create_access_user(username)


def test_access_code_redeems_to_hashed_bearer_session() -> None:
    created = _create_access_user()
    client = TestClient(app)

    response = client.post(
        "/api/auth/access/redeem",
        json={"accessCode": created["accessCode"]},
    )

    assert response.status_code == 200
    payload = response.json()
    token = payload["token"]
    assert payload["user"]["username"] == created["username"]
    assert payload["expiresAt"]
    assert response.headers["cache-control"] == "no-store"

    with sqlite3.connect(config.DB_PATH) as conn:
        stored = conn.execute(
            "SELECT session_id FROM user_sessions WHERE user_id = ?",
            (created["userId"],),
        ).fetchone()[0]
    assert stored == hashlib.sha256(token.encode("utf-8")).hexdigest()
    assert stored != token

    bearer = TestClient(app)
    me = bearer.get(
        "/api/auth/access/me",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert me.status_code == 200
    assert me.json()["user"]["username"] == created["username"]


def test_invalid_bearer_does_not_fall_back_to_valid_cookie() -> None:
    created = _create_access_user()
    client = TestClient(app)
    assert client.post(
        "/api/auth/access/redeem",
        json={"accessCode": created["accessCode"]},
    ).status_code == 200
    assert client.get("/api/auth/access/me").status_code == 200

    rejected = client.get(
        "/api/auth/access/me",
        headers={"Authorization": "Bearer invalid-session-token"},
    )

    assert rejected.status_code == 401


def test_access_code_rejects_admin_and_user_password_login() -> None:
    created = _create_access_user()
    _, encoded_username, secret = created["accessCode"].split(".", 2)
    username = base64.urlsafe_b64decode(encoded_username + "=" * (-len(encoded_username) % 4)).decode()
    client = TestClient(app)

    assert client.post(
        "/api/auth/login",
        json={"username": username, "password": secret},
    ).status_code == 401
    admin_code = auth_service.build_access_code("admin", "admin123")
    assert client.post(
        "/api/auth/access/redeem",
        json={"accessCode": admin_code},
    ).status_code == 401


def test_access_code_reset_revokes_old_code_and_sessions() -> None:
    created = _create_access_user()
    client = TestClient(app)
    first = client.post(
        "/api/auth/access/redeem",
        json={"accessCode": created["accessCode"]},
    )
    assert first.status_code == 200
    old_token = first.json()["token"]

    reset = auth_service.reset_access_code(created["userId"])

    assert client.post(
        "/api/auth/access/redeem",
        json={"accessCode": created["accessCode"]},
    ).status_code == 401
    assert TestClient(app).get(
        "/api/auth/access/me",
        headers={"Authorization": f"Bearer {old_token}"},
    ).status_code == 401
    assert client.post(
        "/api/auth/access/redeem",
        json={"accessCode": reset["accessCode"]},
    ).status_code == 200


def test_session_limit_is_enforced_atomically(monkeypatch) -> None:
    created = _create_access_user()
    user = auth_service.get_user(created["userId"])
    assert user is not None
    timestamps = iter(
        [
            "2026-01-01T00:00:01+00:00",
            "2026-01-01T00:00:02+00:00",
            "2026-01-01T00:00:03+00:00",
            "2026-01-01T00:00:04+00:00",
        ]
    )
    monkeypatch.setattr(auth_service, "_now", lambda: next(timestamps))

    tokens = [auth_service.create_session(user) for _ in range(4)]

    with sqlite3.connect(config.DB_PATH) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM user_sessions WHERE user_id = ?",
            (created["userId"],),
        ).fetchone()[0]
    assert count == 3
    assert auth_service._get_session(tokens[0]) is None
    assert all(auth_service._get_session(token) is not None for token in tokens[1:])


def test_concurrent_session_creation_cannot_exceed_limit(tmp_path) -> None:
    service = UserAuthService(tmp_path / "sessions.db")
    created = service.create_user("reader", "reader-password", role="user")
    user = service.get_user(created["userId"])
    assert user is not None

    with ThreadPoolExecutor(max_workers=4) as executor:
        tokens = list(executor.map(lambda _: service.create_session(user), range(4)))

    with sqlite3.connect(service.db_path) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM user_sessions WHERE user_id = ?",
            (created["userId"],),
        ).fetchone()[0]
    assert count == 3
    assert sum(service._get_session(token) is not None for token in tokens) == 3


def test_auth_failure_limiter_uses_window_and_retry_after() -> None:
    now = [100.0]
    limiter = AuthRateLimiter(limit=5, window_seconds=600, clock=lambda: now[0])

    for _ in range(5):
        limiter.check("ip:test", "user:test")
        limiter.record_failure("ip:test", "user:test")

    with pytest.raises(AuthRateLimitError) as captured:
        limiter.check("ip:test", "user:test")
    assert captured.value.retry_after_seconds == 600

    now[0] += 600
    limiter.check("ip:test", "user:test")


def test_auth_failure_limiter_bounds_identifier_storage() -> None:
    limiter = AuthRateLimiter(limit=5, window_seconds=600, max_keys=8, clock=lambda: 100.0)

    for index in range(100):
        limiter.record_failure("ip:shared", f"user:{index}")

    assert len(limiter._events) <= 8
    assert all(len(events) <= limiter.limit for events in limiter._events.values())
    with pytest.raises(AuthRateLimitError):
        limiter.check("ip:shared")


def test_admin_password_change_revokes_every_existing_session(tmp_path) -> None:
    service = UserAuthService(tmp_path / "password-change.db")
    created = service.create_user("admin", "old-password", role="admin")
    user = service.get_user(created["userId"])
    assert user is not None
    sessions = [service.create_session(user) for _ in range(2)]

    result = service.change_password(user, "old-password", "new-password")

    assert result["passwordReset"] is True
    assert service.authenticate("admin", "new-password").is_admin
    assert all(service._get_session(session) is None for session in sessions)


@pytest.mark.parametrize("payload", ["{broken", "[]", "null"])
def test_existing_invalid_app_config_fails_closed_without_overwrite(tmp_path, payload) -> None:
    path = tmp_path / "app_config.json"
    path.write_text(payload, encoding="utf-8")

    with pytest.raises(AppConfigLoadError):
        AppConfig(path)

    assert path.read_text(encoding="utf-8") == payload


def test_access_redeem_returns_429_after_repeated_failures() -> None:
    client = TestClient(app)

    for _ in range(5):
        assert client.post(
            "/api/auth/access/redeem",
            json={"accessCode": "LH1.invalid.invalid-secret"},
        ).status_code == 401

    limited = client.post(
        "/api/auth/access/redeem",
        json={"accessCode": "LH1.invalid.invalid-secret"},
    )
    assert limited.status_code == 429
    assert int(limited.headers["retry-after"]) > 0
    assert limited.json()["detail"]["code"] == "auth_rate_limited"
    auth_rate_limiter.reset()


def test_disabled_admin_login_is_indistinguishable_from_invalid_credentials() -> None:
    auth_rate_limiter.reset()
    username = f"disabled-admin-{uuid.uuid4().hex[:10]}"
    password = "disabled-admin-password"
    created = auth_service.create_user(username, password, "admin")
    auth_service.set_disabled(created["userId"], True)
    client = TestClient(app)

    disabled = client.post(
        "/api/auth/login",
        json={"username": username, "password": password},
    )
    invalid = client.post(
        "/api/auth/login",
        json={"username": f"missing-{uuid.uuid4().hex}", "password": password},
    )

    assert disabled.status_code == invalid.status_code == 401
    assert disabled.json() == invalid.json() == {"detail": "用户名或密码错误"}
    auth_rate_limiter.reset()


def test_admin_password_config_migrates_to_hash_and_revokes_sessions(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "auth.db"
    config_path = tmp_path / "app_config.json"
    legacy_password = "legacy-admin-password"
    runtime_config = AppConfig(config_path)
    runtime_config.set(
        "auth.adminPasswordBase64",
        base64.b64encode(legacy_password.encode("utf-8")).decode("ascii"),
    )
    runtime_config.save()
    monkeypatch.setattr(AppConfig, "_instance", runtime_config)

    service = UserAuthService(db_path)
    admin = service.bootstrap_admin("admin", "different-password")
    admin_user = service.get_user(admin["userId"])
    assert admin_user is not None
    raw_session = service.create_session(admin_user)

    assert service.migrate_admin_password_config() is True

    assert service.authenticate("admin", legacy_password).is_admin
    with pytest.raises(HTTPException):
        service.authenticate("admin", "different-password")
    assert service._get_session(raw_session) is None
    saved = json.loads(config_path.read_text(encoding="utf-8"))
    assert "adminPasswordBase64" not in saved.get("auth", {})


def test_admin_password_config_migration_marks_database_before_config_cleanup(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "auth.db"
    config_path = tmp_path / "app_config.json"
    legacy_password = "legacy-admin-password"
    runtime_config = AppConfig(config_path)
    runtime_config.set(
        "auth.adminPasswordBase64",
        base64.b64encode(legacy_password.encode("utf-8")).decode("ascii"),
    )
    runtime_config.save()
    monkeypatch.setattr(AppConfig, "_instance", runtime_config)

    service = UserAuthService(db_path)
    admin = service.bootstrap_admin("admin", "different-password")
    original_save = runtime_config.save
    monkeypatch.setattr(runtime_config, "save", lambda: (_ for _ in ()).throw(OSError("disk full")))

    with pytest.raises(OSError, match="disk full"):
        service.migrate_admin_password_config()

    with sqlite3.connect(db_path) as conn:
        marker = conn.execute(
            "SELECT value FROM schema_meta WHERE key = ?",
            ("auth_admin_password_config_migrated",),
        ).fetchone()
    assert marker == ("1",)
    residual = json.loads(config_path.read_text(encoding="utf-8"))
    assert residual["auth"]["adminPasswordBase64"]
    assert service.authenticate("admin", legacy_password).is_admin
    with pytest.raises(HTTPException):
        service.authenticate("admin", "different-password")

    # A later password change remains authoritative. Retrying cleanup must not
    # reapply the inert legacy value left by the failed file save.
    service.reset_password(admin["userId"], "changed-after-failure")
    monkeypatch.setattr(runtime_config, "save", original_save)
    assert service.migrate_admin_password_config() is True
    assert service.authenticate("admin", "changed-after-failure").is_admin
    with pytest.raises(HTTPException):
        service.authenticate("admin", legacy_password)
    saved = json.loads(config_path.read_text(encoding="utf-8"))
    assert "adminPasswordBase64" not in saved.get("auth", {})


def test_first_start_environment_password_wins_over_legacy_config(tmp_path, monkeypatch) -> None:
    config_path = tmp_path / "app_config.json"
    runtime_config = AppConfig(config_path)
    runtime_config.set(
        "auth.adminPasswordBase64",
        base64.b64encode(b"stale-config-password").decode("ascii"),
    )
    runtime_config.save()
    monkeypatch.setattr(AppConfig, "_instance", runtime_config)
    monkeypatch.setenv("LEGADOHUB_ADMIN_PASSWORD", "environment-password")

    service = UserAuthService(tmp_path / "auth.db")
    generated_passwords: list[str] = []
    assert (
        service.ensure_default_admin(
            on_generated_password=generated_passwords.append,
        )
        is True
    )
    assert generated_passwords == []
    assert service.authenticate("admin", "environment-password").is_admin
    with pytest.raises(HTTPException):
        service.authenticate("admin", "stale-config-password")
    saved = json.loads(config_path.read_text(encoding="utf-8"))
    assert "adminPasswordBase64" not in saved.get("auth", {})


def test_first_start_generates_admin_password_once(tmp_path, monkeypatch) -> None:
    runtime_config = AppConfig(tmp_path / "app_config.json")
    monkeypatch.setattr(AppConfig, "_instance", runtime_config)
    monkeypatch.delenv("LEGADOHUB_ADMIN_PASSWORD", raising=False)
    monkeypatch.delenv("LEGADOHUB_ADMIN_PASSWORD_FILE", raising=False)

    service = UserAuthService(tmp_path / "auth.db")
    generated_passwords: list[str] = []

    assert (
        service.ensure_default_admin(
            on_generated_password=generated_passwords.append,
        )
        is True
    )
    assert len(generated_passwords) == 1
    generated_password = generated_passwords[0]
    assert len(generated_password) >= 40
    assert service.authenticate("admin", generated_password).is_admin

    repeated_passwords: list[str] = []
    assert (
        service.ensure_default_admin(
            on_generated_password=repeated_passwords.append,
        )
        is False
    )
    assert repeated_passwords == []


def test_first_start_password_file_wins_over_legacy_config(tmp_path, monkeypatch) -> None:
    runtime_config = AppConfig(tmp_path / "app_config.json")
    runtime_config.set(
        "auth.adminPasswordBase64",
        base64.b64encode(b"legacy-password-must-not-win").decode("ascii"),
    )
    runtime_config.save()
    monkeypatch.setattr(AppConfig, "_instance", runtime_config)
    monkeypatch.delenv("LEGADOHUB_ADMIN_PASSWORD", raising=False)
    secret_file = tmp_path / "admin-password.txt"
    secret_file.write_text("configured-file-password", encoding="utf-8")
    monkeypatch.setenv("LEGADOHUB_ADMIN_PASSWORD_FILE", str(secret_file))

    service = UserAuthService(tmp_path / "auth.db")
    generated_passwords: list[str] = []
    assert service.ensure_default_admin(on_generated_password=generated_passwords.append) is True
    assert generated_passwords == []
    assert service.authenticate("admin", "configured-file-password").is_admin
    with pytest.raises(HTTPException):
        service.authenticate("admin", "legacy-password-must-not-win")


def test_bootstrap_http_endpoint_is_not_exposed(monkeypatch) -> None:
    client = TestClient(app)
    monkeypatch.setattr(
        auth_service,
        "hash_password",
        lambda _password: (_ for _ in ()).throw(AssertionError("must not hash")),
    )

    rejected = client.post(
        "/api/auth/bootstrap",
        json={"username": "bootstrap-disabled", "password": "unused-password"},
    )
    assert rejected.status_code == 404


def test_startup_rejects_database_without_enabled_admin(tmp_path) -> None:
    service = UserAuthService(tmp_path / "no-admin.db")
    service.create_user("reader", "reader-password", role="user")

    with pytest.raises(RuntimeError, match="no enabled administrator"):
        service.ensure_default_admin()


def test_access_payload_rejects_unknown_fields() -> None:
    response = TestClient(app).post(
        "/api/auth/access/redeem",
        json={"accessCode": "invalid", "redirect": "https://evil.invalid"},
    )
    assert response.status_code == 422


def test_build_access_subscription_links_embed_code_and_enter_path() -> None:
    code = "LH1.dGVzdA.secret-value"
    links = UserAuthService.build_access_subscription_links(
        code,
        public_base="https://books.example.com",
        lan_base="http://192.168.1.10:8765",
    )
    assert links["sourceUrl"] == (
        "https://books.example.com/api/subscribe/legado/source?code=LH1.dGVzdA.secret-value"
    )
    assert links["subscriptionUrl"].startswith(
        "https://books.example.com/api/auth/access/enter?code="
    )
    assert "next=%2Fconsole%2Fsubscription" in links["subscriptionUrl"]
    assert links["lanSourceUrl"].startswith("http://192.168.1.10:8765/api/subscribe/legado/source?code=")
    assert "/api/auth/access/enter?" in links["lanSubscriptionUrl"]
    assert UserAuthService.build_access_subscription_links("") == {}


def test_access_enter_sets_session_cookie_and_redirects() -> None:
    created = _create_access_user()
    client = TestClient(app)

    response = client.get(
        "/api/auth/access/enter",
        params={"code": created["accessCode"], "next": "/console/subscription"},
        follow_redirects=False,
    )

    assert response.status_code == 302
    assert response.headers["location"] == "/console/subscription"
    assert "legadohub_session=" in response.headers.get("set-cookie", "")
    assert response.headers["cache-control"] == "no-store"

    me = client.get("/api/auth/me")
    assert me.status_code == 200
    assert me.json()["authenticated"] is True
    assert me.json()["user"]["username"] == created["username"]


def test_access_enter_invalid_code_redirects_to_login() -> None:
    client = TestClient(app, follow_redirects=False)
    response = client.get(
        "/api/auth/access/enter",
        params={"code": "LH1.invalid.invalid", "next": "/console/subscription"},
    )
    assert response.status_code == 302
    location = response.headers["location"]
    assert location.startswith("/login?")
    assert "error=invalid_code" in location
    assert "next=" in location


def test_access_enter_blocks_open_redirect() -> None:
    created = _create_access_user()
    client = TestClient(app, follow_redirects=False)
    response = client.get(
        "/api/auth/access/enter",
        params={"code": created["accessCode"], "next": "https://evil.invalid/phish"},
    )
    assert response.status_code == 302
    assert response.headers["location"] == "/console/subscription"


def test_personal_legado_source_embeds_bound_access_code() -> None:
    created = _create_access_user()
    client = TestClient(app)

    response = client.get(
        "/api/subscribe/legado/source",
        params={"code": created["accessCode"]},
    )
    assert response.status_code == 200
    sources = response.json()
    assert len(sources) == 2
    assert [source["bookSourceUrl"] for source in sources] == ["LegadoHub-LAN", "LegadoHub-LAN-Max"]
    source = sources[0]
    code_literal = json.dumps(created["accessCode"], ensure_ascii=False)
    for source in sources:
        assert f"var LEGADOHUB_ACCESS_CODE = {code_literal}" in source["loginUrl"]
        assert f"var LEGADOHUB_ACCESS_CODE = {code_literal}" in source["jsLib"]
    assert "legadoHubOpenSubscriptions" in source["loginUi"]
    assert "legadoHubOpenLibrary" in source["loginUi"]
    assert "专属" in source["bookSourceComment"] or "自动鉴权" in source["bookSourceComment"]

    rejected = client.get(
        "/api/subscribe/legado/source",
        params={"code": "LH1.invalid.invalid"},
    )
    assert rejected.status_code == 401

    anonymous = client.get("/api/subscribe/legado/source")
    assert anonymous.status_code == 401
    assert "专属书源" in str(anonymous.json().get("detail", ""))
