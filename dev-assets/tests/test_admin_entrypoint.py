"""Public and management listener isolation regressions."""

from __future__ import annotations

import uuid
import socket
import threading
import time

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from app.core.public_security import (
    load_admin_security_config,
    load_public_security_config,
)
from app.main import EntryPoint, create_app
from app.server import PortDispatchApp
from app.services.user_auth import auth_service


PUBLIC_ORIGIN = "http://public.test:8765"
ADMIN_ORIGIN = "http://admin.test:8766"


@pytest.fixture
def isolated_apps(monkeypatch, tmp_path):
    import app.main as main_module

    frontend_dist = tmp_path / "dist"
    (frontend_dist / "assets").mkdir(parents=True)
    (frontend_dist / "index.html").write_text("<html><body>LegadoHub</body></html>", encoding="utf-8")
    (frontend_dist / "assets" / "app.js").write_text("window.LEGADOHUB = true", encoding="utf-8")
    (frontend_dist / "favicon.svg").write_text("<svg></svg>", encoding="utf-8")
    (frontend_dist / "icons.svg").write_text("<svg></svg>", encoding="utf-8")
    monkeypatch.setattr(main_module, "FRONTEND_DIST", frontend_dist)

    monkeypatch.delenv("LEGADOHUB_PUBLIC_BASE_URL", raising=False)
    monkeypatch.setenv("LEGADOHUB_ALLOWED_HOSTS", "public.test")
    monkeypatch.setenv("LEGADOHUB_ALLOWED_ORIGINS", PUBLIC_ORIGIN)
    monkeypatch.setenv("LEGADOHUB_TRUSTED_PROXIES", "127.0.0.1/32")
    monkeypatch.setenv("LEGADOHUB_ADMIN_BASE_URL", ADMIN_ORIGIN)
    monkeypatch.setenv("LEGADOHUB_ADMIN_ALLOWED_HOSTS", "admin.test")
    monkeypatch.setenv("LEGADOHUB_ADMIN_ALLOWED_ORIGINS", ADMIN_ORIGIN)
    monkeypatch.setenv("LEGADOHUB_ADMIN_TRUSTED_PROXIES", "127.0.0.1/32")

    public_app = create_app(
        load_public_security_config(),
        entrypoint=EntryPoint.PUBLIC,
        manage_runtime=False,
    )
    admin_app = create_app(
        load_admin_security_config(),
        entrypoint=EntryPoint.ADMIN,
        manage_runtime=False,
    )
    return public_app, admin_app


def test_public_listener_exposes_only_reader_and_user_routes(isolated_apps) -> None:
    public_app, _ = isolated_apps
    client = TestClient(public_app, base_url=PUBLIC_ORIGIN)

    assert client.get("/api/auth/entrypoint").json() == {"entrypoint": "public"}
    assert client.get("/health").status_code == 200
    assert client.get("/api/subscribe/legado/source").status_code == 401

    created = auth_service.create_access_user(f"port-reader-{uuid.uuid4().hex[:10]}")
    assert client.get(
        "/api/subscribe/legado/source",
        params={"code": created["accessCode"]},
    ).status_code == 200
    redeemed = client.post(
        "/api/auth/access/redeem",
        json={"accessCode": created["accessCode"]},
    )
    assert redeemed.status_code == 200
    headers = {"Authorization": f"Bearer {redeemed.json()['token']}"}
    assert client.get(
        "/api/subscribe/legado/search?keyword=",
        headers=headers,
    ).status_code == 200

    blocked_paths = (
        "/api/auth/login",
        "/api/auth/bootstrap",
        "/api/auth/change-password",
        "/api/info",
        "/api/console/status",
        "/api/console/library-books/book/logs/stream",
        "/api/subscribe/library",
        "/api/subscribe/books/book/source-map/refresh",
        "/docs",
        "/redoc",
        "/openapi.json",
        "/console/plugins",
        "/console/settings",
    )
    for path in blocked_paths:
        response = client.get(path, headers=headers)
        assert response.status_code == 404, path

    for encoded_path in (
        "/api%2Fconsole/status",
        "/api/console%2Fstatus",
        "/api/console/%2e%2e/console/status",
    ):
        assert client.get(encoded_path, headers=headers).status_code != 200

    for path, payload in (
        ("/api/auth/login", {"username": "admin", "password": "admin123"}),
        ("/api/auth/bootstrap", {"username": "admin", "password": "admin123"}),
        ("/api/auth/change-password", {"currentPassword": "old", "newPassword": "password-123"}),
        ("/api/console/plugins/ping", {}),
    ):
        assert client.post(path, headers=headers, json=payload).status_code == 404
    options = client.options(
        "/api/auth/login",
        headers={"Origin": "https://evil.invalid", "Access-Control-Request-Method": "POST"},
    )
    assert options.status_code != 200
    assert "access-control-allow-origin" not in options.headers
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/api/console/status"):
            pass

    assert client.get("/login").status_code == 200
    root = client.get("/", follow_redirects=False)
    assert root.status_code == 200
    assert "LegadoHub" in root.text
    assert client.get("/console").status_code == 200
    assert client.get("/console/subscription").status_code == 200
    assert client.get("/console/library/book-a").status_code == 200
    assert client.get("/assets/app.js").status_code == 200


def test_admin_listener_exposes_management_but_not_access_code_redemption(isolated_apps) -> None:
    _, admin_app = isolated_apps
    client = TestClient(admin_app, base_url=ADMIN_ORIGIN)

    assert client.get("/api/auth/entrypoint").json() == {"entrypoint": "admin"}
    assert client.post(
        "/api/auth/access/redeem",
        json={"accessCode": "LH1.invalid.invalid"},
    ).status_code == 404
    login = client.post(
        "/api/auth/login",
        json={"username": "admin", "password": "admin123"},
    )
    assert login.status_code == 200
    assert client.get("/api/info").status_code == 200
    assert client.get("/api/console/status").status_code == 200
    assert client.get("/openapi.json").status_code == 200
    assert client.get("/docs").status_code == 200
    assert client.get("/redoc").status_code == 200
    root = client.get("/", follow_redirects=False)
    assert root.status_code == 200
    assert "LegadoHub" in root.text
    assert client.get("/console/plugins").status_code == 200
    assert client.get("/assets/app.js").status_code == 200

    route_paths = {getattr(route, "path", "") for route in admin_app.routes}
    assert "/api/console/library-books/{book_id}/logs/stream" in route_paths


def test_admin_listener_enforces_its_own_host_and_cookie_origin(isolated_apps) -> None:
    _, admin_app = isolated_apps
    client = TestClient(admin_app, base_url=ADMIN_ORIGIN)
    assert client.get("/health", headers={"Host": "evil.invalid"}).status_code == 400
    login = client.post(
        "/api/auth/login",
        json={"username": "admin", "password": "admin123"},
    )
    assert login.status_code == 200
    assert client.post("/api/auth/logout").status_code == 403
    assert client.post(
        "/api/auth/logout",
        headers={"Origin": "http://evil.invalid"},
    ).status_code == 403
    assert client.post(
        "/api/auth/logout",
        headers={"Origin": ADMIN_ORIGIN},
    ).status_code == 200


def test_admin_listener_does_not_accept_a_reader_identity(isolated_apps) -> None:
    _, admin_app = isolated_apps
    created = auth_service.create_access_user(f"admin-port-reader-{uuid.uuid4().hex[:10]}")
    token = auth_service.create_session(auth_service.authenticate_access_code(created["accessCode"]))
    client = TestClient(admin_app, base_url=ADMIN_ORIGIN)

    response = client.get(
        "/api/auth/me",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    assert response.json() == {"authenticated": False, "user": None}
    assert client.get(
        "/api/console/status",
        headers={"Authorization": f"Bearer {token}"},
    ).status_code == 403


def test_port_dispatch_uses_the_local_socket_port(isolated_apps) -> None:
    public_app, admin_app = isolated_apps
    dispatcher = PortDispatchApp(
        public_port=8765,
        admin_port=8766,
        public_app=public_app,
        admin_app=admin_app,
    )

    public_client = TestClient(dispatcher, base_url=PUBLIC_ORIGIN)
    admin_client = TestClient(dispatcher, base_url=ADMIN_ORIGIN)
    unknown_client = TestClient(dispatcher, base_url="http://public.test:9999")
    assert public_client.get("/api/auth/entrypoint").json()["entrypoint"] == "public"
    assert admin_client.get("/api/auth/entrypoint").json()["entrypoint"] == "admin"
    assert unknown_client.get("/health").status_code == 404


def test_uvicorn_dispatches_two_real_listening_sockets(isolated_apps) -> None:
    public_app, admin_app = isolated_apps
    listeners: list[socket.socket] = []
    for _ in range(2):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(128)
        listeners.append(listener)
    public_port = listeners[0].getsockname()[1]
    admin_port = listeners[1].getsockname()[1]
    dispatcher = PortDispatchApp(
        public_port=public_port,
        admin_port=admin_port,
        public_app=public_app,
        admin_app=admin_app,
    )
    server = uvicorn.Server(
        uvicorn.Config(
            dispatcher,
            host="127.0.0.1",
            port=public_port,
            lifespan="on",
            log_level="error",
            proxy_headers=False,
        )
    )
    thread = threading.Thread(target=server.run, kwargs={"sockets": listeners}, daemon=True)
    thread.start()
    deadline = time.monotonic() + 5
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.01)
    try:
        assert server.started
        public_response = httpx.get(
            f"http://127.0.0.1:{public_port}/api/auth/entrypoint",
            headers={"Host": "public.test"},
        )
        admin_response = httpx.get(
            f"http://127.0.0.1:{admin_port}/api/auth/entrypoint",
            headers={"Host": "admin.test"},
        )
        assert public_response.json() == {"entrypoint": "public"}
        assert admin_response.json() == {"entrypoint": "admin"}
        public_admin_response = httpx.get(
            f"http://127.0.0.1:{public_port}/api/console/status",
            headers={"Host": "public.test"},
        )
        admin_openapi_response = httpx.get(
            f"http://127.0.0.1:{admin_port}/openapi.json",
            headers={"Host": "admin.test"},
        )
        assert public_admin_response.status_code == 404
        assert admin_openapi_response.status_code == 200
    finally:
        server.should_exit = True
        thread.join(timeout=5)
        for listener in listeners:
            listener.close()
    assert not thread.is_alive()


def test_lan_admin_origin_may_use_http(monkeypatch) -> None:
    monkeypatch.setenv("LEGADOHUB_ADMIN_BASE_URL", "http://192.0.2.30:8766")
    monkeypatch.setenv("LEGADOHUB_ADMIN_ALLOWED_HOSTS", "192.0.2.30")
    monkeypatch.setenv("LEGADOHUB_ADMIN_ALLOWED_ORIGINS", "http://192.0.2.30:8766")
    security = load_admin_security_config()
    assert security.public_base_url == "http://192.0.2.30:8766"
    assert security.require_https is False


@pytest.mark.asyncio
async def test_dispatch_lifespan_is_owned_only_by_public_app() -> None:
    calls: list[str] = []

    async def public_app(scope, receive, send):
        calls.append(f"public:{scope['type']}")

    async def admin_app(scope, receive, send):
        calls.append(f"admin:{scope['type']}")

    dispatcher = PortDispatchApp(
        public_port=8765,
        admin_port=8766,
        public_app=public_app,
        admin_app=admin_app,
    )
    await dispatcher(
        {"type": "lifespan", "asgi": {"version": "3.0", "spec_version": "2.0"}},
        lambda: None,
        lambda _message: None,
    )
    assert calls == ["public:lifespan"]


# ---- Admin surface origin/proxy ergonomics and reader-target redirects ----


def _dynamic_admin_app(monkeypatch) -> "TestClient":
    monkeypatch.delenv("LEGADOHUB_PUBLIC_BASE_URL", raising=False)
    monkeypatch.delenv("LEGADOHUB_ADMIN_BASE_URL", raising=False)
    monkeypatch.delenv("LEGADOHUB_ADMIN_ALLOWED_HOSTS", raising=False)
    monkeypatch.delenv("LEGADOHUB_ADMIN_ALLOWED_ORIGINS", raising=False)
    monkeypatch.delenv("LEGADOHUB_ADMIN_TRUSTED_PROXIES", raising=False)
    monkeypatch.delenv("LEGADOHUB_READER_EXTERNAL_ORIGIN", raising=False)
    admin_app = create_app(
        load_admin_security_config(),
        entrypoint=EntryPoint.ADMIN,
        manage_runtime=False,
    )
    return TestClient(admin_app, base_url="http://nas.lan:8766")


def test_admin_login_tolerates_proxied_same_host_origin(monkeypatch) -> None:
    client = _dynamic_admin_app(monkeypatch)
    # TLS-terminating proxy without forwarded proto: Origin scheme differs from
    # the backend view, but the host matches — this is one deployment, not CSRF.
    tolerated = client.post(
        "/api/auth/login",
        json={"username": "admin", "password": "definitely-wrong"},
        headers={"Origin": "https://nas.lan"},
    )
    assert tolerated.status_code == 401
    assert tolerated.json()["detail"] == "用户名或密码错误"

    rejected = client.post(
        "/api/auth/login",
        json={"username": "admin", "password": "definitely-wrong"},
        headers={"Origin": "https://other.lan"},
    )
    assert rejected.status_code == 403
    detail = rejected.json()["detail"]
    assert detail["code"] == "admin_origin_rejected"
    assert "来源校验未通过" in detail["message"]
    assert "https://other.lan" in detail["observedOrigin"]
    assert "http://nas.lan:8766" in detail["expectedOrigin"]


def test_admin_explicit_origins_keep_strict_scheme_check(monkeypatch) -> None:
    _dynamic_admin_app(monkeypatch)
    monkeypatch.setenv("LEGADOHUB_ADMIN_ALLOWED_ORIGINS", "http://nas.lan:8766")
    admin_app = create_app(
        load_admin_security_config(),
        entrypoint=EntryPoint.ADMIN,
        manage_runtime=False,
    )
    client = TestClient(admin_app, base_url="http://nas.lan:8766")
    rejected = client.post(
        "/api/auth/login",
        json={"username": "admin", "password": "definitely-wrong"},
        headers={"Origin": "https://nas.lan"},
    )
    assert rejected.status_code == 403


def test_admin_enter_redirect_targets_external_reader_origin(monkeypatch) -> None:
    client = _dynamic_admin_app(monkeypatch)
    monkeypatch.setenv("LEGADOHUB_READER_EXTERNAL_ORIGIN", "http://192.168.31.5:4390")
    response = client.get(
        "/api/auth/access/enter",
        params={"code": "LH1.code", "next": "/console/subscription"},
        follow_redirects=False,
    )
    assert response.status_code == 307
    assert response.headers["location"] == (
        "http://192.168.31.5:4390/api/auth/access/enter"
        "?code=LH1.code&next=%2Fconsole%2Fsubscription"
    )


def test_admin_enter_redirect_without_mapping_keeps_legacy_reader_port(monkeypatch) -> None:
    client = _dynamic_admin_app(monkeypatch)
    response = client.get(
        "/api/auth/access/enter",
        params={"code": "LH1.code", "next": "/console/subscription"},
        follow_redirects=False,
    )
    assert response.status_code == 307
    assert response.headers["location"].startswith("http://nas.lan:8765/api/auth/access/enter")
