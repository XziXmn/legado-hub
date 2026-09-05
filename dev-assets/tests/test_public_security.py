"""Reader entrypoint request and credential boundary regressions."""

from __future__ import annotations

import os
import stat
import uuid
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from starlette.datastructures import Headers

from app.core.public_security import load_admin_security_config, load_public_security_config
from app.main import EntryPoint, create_app
from app.services.cookie_store import CookieStore
from app.services.user_auth import auth_service


READER_ORIGIN = "https://books.example.test"
LAN_READER_ORIGIN = "http://192.168.31.161:8765"
LAN_ADMIN_ORIGIN = "http://192.168.31.161:8766"


def _reader_config(monkeypatch):
    monkeypatch.delenv("LEGADOHUB_PUBLIC_BASE_URL", raising=False)
    monkeypatch.setenv("LEGADOHUB_REQUIRE_HTTPS", "1")
    monkeypatch.setenv("LEGADOHUB_ALLOWED_HOSTS", "books.example.test")
    monkeypatch.setenv("LEGADOHUB_ALLOWED_ORIGINS", READER_ORIGIN)
    monkeypatch.setenv("LEGADOHUB_TRUSTED_PROXIES", "127.0.0.1/32")
    return load_public_security_config()


def _clear_network_config(monkeypatch) -> None:
    for name in (
        "LEGADOHUB_PUBLIC_BASE_URL",
        "LEGADOHUB_REQUIRE_HTTPS",
        "LEGADOHUB_ALLOWED_HOSTS",
        "LEGADOHUB_ALLOWED_ORIGINS",
        "LEGADOHUB_ADMIN_BASE_URL",
        "LEGADOHUB_ADMIN_ALLOWED_HOSTS",
        "LEGADOHUB_ADMIN_ALLOWED_ORIGINS",
    ):
        monkeypatch.delenv(name, raising=False)


def _personal_source_params() -> dict[str, str]:
    created = auth_service.create_access_user(f"src-{uuid.uuid4().hex[:10]}")
    return {"code": created["accessCode"]}


def test_dynamic_host_accepts_lan_proxy_and_ipv6_but_rejects_public_ipv4_peer(
    monkeypatch,
    tmp_path,
) -> None:
    from app.core.app_config import AppConfig
    from app.core.public_security import (
        effective_public_base_url,
        settings_public_base_url,
    )

    _clear_network_config(monkeypatch)
    config_path = tmp_path / "app_config.json"
    config_path.write_text("{}", encoding="utf-8")
    AppConfig.reset()
    monkeypatch.setattr(AppConfig, "get", classmethod(lambda cls: AppConfig(config_path)))

    reader_security = load_public_security_config()
    assert reader_security.dynamic_base_url is True
    assert reader_security.enforce_reading_public_allowlist is False
    reader_app = create_app(reader_security, manage_runtime=False)
    proxy_origin = "http://books.example.test:8765"

    # Public Hosts are open at the app layer (WAF/firewall is operator-owned).
    lan_proxy = TestClient(
        reader_app,
        base_url=proxy_origin,
        client=("192.168.31.161", 50000),
    )
    source_params = _personal_source_params()
    assert lan_proxy.get("/api/subscribe/legado/source").status_code == 401
    assert lan_proxy.get("/api/subscribe/legado/source", params=source_params).status_code == 200
    search_url = lan_proxy.get("/api/subscribe/legado/source", params=source_params).json()[0]["searchUrl"]
    assert proxy_origin in search_url

    public_peer_on_public_host = TestClient(
        reader_app,
        base_url=proxy_origin,
        client=("203.0.113.20", 50000),
    )
    assert public_peer_on_public_host.get("/health").status_code == 200
    assert public_peer_on_public_host.get(
        "/api/subscribe/legado/source",
        params=source_params,
    ).status_code == 200

    other_port = TestClient(
        reader_app,
        base_url="http://books.example.test:2087",
        client=("203.0.113.20", 50000),
    )
    assert other_port.get(
        "/api/subscribe/legado/source",
        params=source_params,
    ).status_code == 200

    # Optional settings only affect preferred issued-link base, not Host gating.
    cfg = AppConfig(config_path)
    public_book_origin = "https://book.example.com:2087"
    cfg.set("readingAccess.publicBaseUrl", public_book_origin)
    cfg.save()
    AppConfig.reset()
    monkeypatch.setattr(AppConfig, "get", classmethod(lambda cls: AppConfig(config_path)))

    assert settings_public_base_url() == public_book_origin
    assert effective_public_base_url() == public_book_origin

    # Spoofing a LAN Host from a public peer remains denied.
    public_ipv4_peer = TestClient(
        reader_app,
        base_url=LAN_READER_ORIGIN,
        client=("203.0.113.20", 50000),
    )
    assert public_ipv4_peer.get("/health").status_code == 400

    ipv6_proxy = TestClient(
        reader_app,
        base_url=proxy_origin,
        client=("2001:db8::20", 50000),
    )
    assert ipv6_proxy.get("/health").status_code == 200

    admin_security = load_admin_security_config()
    assert admin_security.dynamic_base_url is True
    admin_app = create_app(
        admin_security,
        entrypoint=EntryPoint.ADMIN,
        manage_runtime=False,
    )
    admin_origin = "http://admin.example.test:8766"
    assert TestClient(
        admin_app,
        base_url=admin_origin,
        client=("192.168.31.161", 50000),
    ).post(
        "/api/missing",
        headers={"Origin": admin_origin},
    ).status_code == 404


def test_settings_public_book_source_url_overrides_environment_fallback(monkeypatch, tmp_path) -> None:
    """公网书源地址 uses the deployment value until settings override it."""
    from app.core.app_config import AppConfig
    from app.core.public_security import effective_public_base_url, get_public_base_url

    _clear_network_config(monkeypatch)
    config_path = tmp_path / "app_config.json"
    config_path.write_text("{}", encoding="utf-8")
    AppConfig.reset()
    monkeypatch.setattr(AppConfig, "get", classmethod(lambda cls: AppConfig(config_path)))

    environment_origin = "https://books.environment.example:2087"
    monkeypatch.setenv("LEGADOHUB_PUBLIC_BASE_URL", environment_origin)
    assert effective_public_base_url() == environment_origin
    security = load_public_security_config()
    assert security.dynamic_base_url is True
    assert security.enforce_reading_public_allowlist is False

    cfg = AppConfig(config_path)
    settings_origin = "https://books.settings.example:2087"
    cfg.set("readingAccess.publicBaseUrl", settings_origin)
    cfg.save()
    AppConfig.reset()
    monkeypatch.setattr(AppConfig, "get", classmethod(lambda cls: AppConfig(config_path)))

    assert effective_public_base_url() == settings_origin
    # Request/offline base resolution remains independent from issued-link defaults.
    assert get_public_base_url() != settings_origin


def test_default_network_accepts_ipv6_and_rejects_public_ipv4(monkeypatch) -> None:
    _clear_network_config(monkeypatch)
    public_app = create_app(load_public_security_config())

    lan_client = TestClient(public_app, base_url=LAN_READER_ORIGIN)
    source_params = _personal_source_params()
    assert lan_client.get("/api/subscribe/legado/source").status_code == 401
    manifest = lan_client.get("/api/subscribe/legado/source", params=source_params)
    assert manifest.status_code == 200
    assert LAN_READER_ORIGIN in manifest.json()[0]["searchUrl"]

    # Public Hosts are open (auth still required for data APIs).
    public_client = TestClient(public_app, base_url="http://203.0.113.10:8765")
    assert public_client.get("/health").status_code == 200

    arbitrary_name_client = TestClient(public_app, base_url="http://evil.example:8765")
    assert arbitrary_name_client.get("/health").status_code == 200
    # Invalid Host syntax is still rejected.
    assert arbitrary_name_client.get(
        "/health",
        headers={"Host": "192.168.31.161:99999"},
    ).status_code == 400
    assert arbitrary_name_client.get(
        "/health",
        headers={"Host": "[fe80::1%25eth0]:8765"},
    ).status_code == 400

    local_name_client = TestClient(public_app, base_url="http://reader.home.arpa:8765")
    assert local_name_client.get("/health").status_code == 200

    source_params = _personal_source_params()
    ipv6_client = TestClient(public_app)
    ipv6_manifest = ipv6_client.get(
        "/api/subscribe/legado/source",
        params=source_params,
        headers={"Host": "[fd00::20]:8765"},
    )
    assert ipv6_manifest.status_code == 200
    assert "http://[fd00::20]:8765" in ipv6_manifest.json()[0]["searchUrl"]

    public_ipv6_manifest = ipv6_client.get(
        "/api/subscribe/legado/source",
        params=source_params,
        headers={"Host": "[2001:4860:4860::8888]:8765"},
    )
    assert public_ipv6_manifest.status_code == 200
    assert "http://[2001:4860:4860::8888]:8765" in public_ipv6_manifest.json()[0]["searchUrl"]

    mapped_lan_manifest = ipv6_client.get(
        "/api/subscribe/legado/source",
        params=source_params,
        headers={"Host": "[::ffff:192.168.31.161]:8765"},
    )
    assert mapped_lan_manifest.status_code == 200
    assert "http://192.168.31.161:8765" in mapped_lan_manifest.json()[0]["searchUrl"]

    # Unspecified / multicast Host literals remain invalid.
    for rejected_host in (
        "[::]:8765",
        "[ff02::1]:8765",
    ):
        assert ipv6_client.get(
            "/health",
            headers={"Host": rejected_host},
        ).status_code == 400
    # Public IPv4-mapped IPv6 is treated as a normal public Host (allowed).
    assert ipv6_client.get(
        "/health",
        headers={"Host": "[::ffff:203.0.113.10]:8765"},
    ).status_code == 200


def test_default_lan_admin_accepts_same_origin_only(monkeypatch) -> None:
    _clear_network_config(monkeypatch)
    admin_app = create_app(
        load_admin_security_config(),
        entrypoint=EntryPoint.ADMIN,
        manage_runtime=False,
    )
    client = TestClient(admin_app, base_url=LAN_ADMIN_ORIGIN)

    assert client.post("/api/missing", headers={"Origin": LAN_ADMIN_ORIGIN}).status_code == 404
    assert client.post(
        "/api/missing",
        headers={"Origin": "http://192.168.31.99:8766"},
    ).status_code == 403


def test_reader_config_derives_https_and_rejects_wildcard_hosts(monkeypatch) -> None:
    security = _reader_config(monkeypatch)
    assert security.require_https is True
    assert security.enforce_origin is True

    monkeypatch.setenv("LEGADOHUB_ALLOWED_HOSTS", "*")
    with pytest.raises(RuntimeError, match="without wildcards"):
        load_public_security_config()


def test_reader_app_rejects_host_and_forwarded_spoof_and_uses_fixed_urls(monkeypatch) -> None:
    public_app = create_app(_reader_config(monkeypatch))
    secure_client = TestClient(public_app, base_url=READER_ORIGIN)

    assert secure_client.get("/health").status_code == 200
    # Fixed-mode TrustedHost still rejects unknown Host when ALLOWED_HOSTS is set.
    assert secure_client.get("/health", headers={"Host": "evil.invalid"}).status_code == 400
    source_params = _personal_source_params()
    manifest = secure_client.get(
        "/api/subscribe/legado/source",
        params=source_params,
        headers={"X-Forwarded-Host": "evil.invalid"},
    )
    assert manifest.status_code == 200
    source = manifest.json()[0]
    assert READER_ORIGIN in source["searchUrl"]
    assert "evil.invalid" not in str(source)
    assert manifest.headers["cache-control"] == "no-store"
    assert manifest.headers["x-content-type-options"] == "nosniff"
    assert "max-age=31536000" in manifest.headers["strict-transport-security"]

    insecure_client = TestClient(public_app, base_url="http://books.example.test")
    rejected = insecure_client.get(
        "/api/subscribe/legado/source",
        params=source_params,
        headers={"X-Forwarded-Proto": "https"},
    )
    assert rejected.status_code == 400


def test_https_reader_cookie_writes_require_origin_but_bearer_does_not(monkeypatch) -> None:
    public_app = create_app(_reader_config(monkeypatch))
    browser = TestClient(public_app, base_url=READER_ORIGIN)
    login = browser.post(
        "/api/auth/login",
        headers={"Origin": READER_ORIGIN},
        json={"username": "admin", "password": "admin123"},
    )
    assert login.status_code == 200
    cookie = login.headers["set-cookie"]
    assert "Secure" in cookie
    assert "HttpOnly" in cookie
    assert "SameSite=lax" in cookie

    assert browser.post("/api/auth/logout").status_code == 403
    assert browser.post(
        "/api/auth/logout",
        headers={"Origin": "https://evil.invalid"},
    ).status_code == 403
    assert browser.post(
        "/api/auth/logout",
        headers={"Origin": READER_ORIGIN},
    ).status_code == 200

    created = auth_service.create_access_user(f"public-{uuid.uuid4().hex[:10]}")
    redemption_client = TestClient(public_app, base_url=READER_ORIGIN)
    redeemed = redemption_client.post(
        "/api/auth/access/redeem",
        json={"accessCode": created["accessCode"]},
    )
    assert redeemed.status_code == 200
    bearer = TestClient(public_app, base_url=READER_ORIGIN)
    assert bearer.post(
        "/api/auth/access/logout",
        headers={"Authorization": f"Bearer {redeemed.json()['token']}"},
    ).status_code == 200


def test_trusted_proxy_client_ip_ignores_spoofed_leftmost_values(monkeypatch) -> None:
    _clear_network_config(monkeypatch)
    monkeypatch.setenv("LEGADOHUB_TRUSTED_PROXIES", "10.0.0.0/24")
    security = load_public_security_config()
    assert security.require_https is False
    request = SimpleNamespace(
        client=SimpleNamespace(host="10.0.0.2"),
        headers=Headers({"X-Forwarded-For": "203.0.113.99, 198.51.100.25"}),
    )
    assert security.client_ip(request) == "198.51.100.25"

    oversized_chain = SimpleNamespace(
        client=SimpleNamespace(host="10.0.0.2"),
        headers=Headers({"X-Forwarded-For": ", ".join(["198.51.100.1"] * 17)}),
    )
    assert security.client_ip(oversized_chain) == "10.0.0.2"

    ambiguous_proto = SimpleNamespace(
        client=SimpleNamespace(host="10.0.0.2"),
        url=SimpleNamespace(scheme="http"),
        headers=Headers({"X-Forwarded-Proto": "https,http"}),
    )
    assert security.request_is_https(ambiguous_proto) is False


def test_cookie_store_rejects_path_traversal_and_uses_private_modes(tmp_path) -> None:
    store = CookieStore(tmp_path / "cookies")
    with pytest.raises(ValueError, match="Invalid plugin id"):
        store.save("../../outside", {"secret": "no"})

    store.save("safe_plugin", {"cookie": "value"})
    assert store.load("safe_plugin") == {"cookie": "value"}
    if os.name != "nt":
        assert stat.S_IMODE(store.base_dir.stat().st_mode) == 0o700
        assert stat.S_IMODE(store.path_for("safe_plugin").stat().st_mode) == 0o600


# ---- Reader entrypoint origins under Docker port mappings ----


def _request_for_app(
    app,
    *,
    host: str,
    scheme: str = "http",
    client=("192.168.31.9", 50000),
    extra_headers=(),
):
    from starlette.requests import Request

    scope = {
        "type": "http",
        "app": app,
        "scheme": scheme,
        "http_version": "1.1",
        "server": ("192.168.31.5", 8766),
        "client": tuple(client),
        "method": "GET",
        "path": "/",
        "query_string": b"",
        "headers": [(b"host", host.encode()), *extra_headers],
    }
    return Request(scope)


def test_reader_origin_rewrite_rewrites_only_admin_port_without_config(monkeypatch) -> None:
    from app.core.public_security import ensure_reader_entrypoint_origin

    _clear_network_config(monkeypatch)
    assert (
        ensure_reader_entrypoint_origin("http://192.168.31.5:8766")
        == "http://192.168.31.5:8765"
    )
    # Non-admin origins pass through untouched.
    assert (
        ensure_reader_entrypoint_origin("http://192.168.31.5:4390")
        == "http://192.168.31.5:4390"
    )
    assert (
        ensure_reader_entrypoint_origin("https://books.example.test")
        == "https://books.example.test"
    )


def test_reader_origin_rewrite_honors_reader_external_origin_env(monkeypatch) -> None:
    from app.core.public_security import ensure_reader_entrypoint_origin

    _clear_network_config(monkeypatch)
    monkeypatch.setenv("LEGADOHUB_READER_EXTERNAL_ORIGIN", "http://192.168.31.5:4390")
    assert (
        ensure_reader_entrypoint_origin("http://192.168.31.5:8766")
        == "http://192.168.31.5:4390"
    )
    # Bare port form applies to the base origin's host.
    monkeypatch.setenv("LEGADOHUB_READER_EXTERNAL_ORIGIN", "4390")
    assert (
        ensure_reader_entrypoint_origin("http://192.168.31.5:8766")
        == "http://192.168.31.5:4390"
    )
    # Invalid values fall back to the legacy same-host reader port.
    monkeypatch.setenv("LEGADOHUB_READER_EXTERNAL_ORIGIN", "not-a-port")
    assert (
        ensure_reader_entrypoint_origin("http://192.168.31.5:8766")
        == "http://192.168.31.5:8765"
    )


def test_reader_origin_rewrite_uses_matching_public_base_url(monkeypatch) -> None:
    from app.core.public_security import ensure_reader_entrypoint_origin

    _clear_network_config(monkeypatch)
    monkeypatch.setenv("LEGADOHUB_PUBLIC_BASE_URL", "http://192.168.31.5:4390")
    assert (
        ensure_reader_entrypoint_origin("http://192.168.31.5:8766")
        == "http://192.168.31.5:4390"
    )
    # A public-domain 公网地址 must not leak into a LAN origin's rewrite.
    monkeypatch.setenv("LEGADOHUB_PUBLIC_BASE_URL", "https://books.example.test")
    assert (
        ensure_reader_entrypoint_origin("http://192.168.31.5:8766")
        == "http://192.168.31.5:8765"
    )


def test_admin_console_request_origin_rewrites_to_external_reader(monkeypatch) -> None:
    from app.core.public_security import load_admin_security_config, reading_base_url

    monkeypatch.setenv("LEGADOHUB_READER_EXTERNAL_ORIGIN", "http://192.168.31.5:4390")
    admin_app = create_app(
        load_admin_security_config(),
        entrypoint=EntryPoint.ADMIN,
        manage_runtime=False,
    )
    request = _request_for_app(admin_app, host="192.168.31.5:4391")
    assert reading_base_url(request) == "http://192.168.31.5:4390"

    # Without the declaration the legacy internal-port fallback applies.
    monkeypatch.delenv("LEGADOHUB_READER_EXTERNAL_ORIGIN", raising=False)
    request = _request_for_app(admin_app, host="192.168.31.5:4391")
    assert reading_base_url(request) == "http://192.168.31.5:8765"


def test_reader_surface_request_origin_is_used_verbatim(monkeypatch) -> None:
    from app.core.public_security import load_public_security_config, reading_base_url

    _clear_network_config(monkeypatch)
    reader_app = create_app(
        load_public_security_config(),
        entrypoint=EntryPoint.PUBLIC,
        manage_runtime=False,
    )
    request = _request_for_app(
        reader_app,
        host="192.168.31.5:4390",
        client=("192.168.31.9", 50000),
    )
    assert reading_base_url(request) == "http://192.168.31.5:4390"


def test_dynamic_admin_config_trusts_private_proxies_by_default(monkeypatch) -> None:
    from starlette.datastructures import Headers

    _clear_network_config(monkeypatch)
    security = load_admin_security_config()
    assert security.admin_surface is True
    assert security.origin_allow_same_host is True
    assert security.is_trusted_proxy("127.0.0.1") is True
    assert security.is_trusted_proxy("172.17.0.5") is True
    assert security.is_trusted_proxy("192.168.31.1") is True

    proxied = SimpleNamespace(
        client=SimpleNamespace(host="172.17.0.5"),
        url=SimpleNamespace(scheme="http"),
        headers=Headers({"X-Forwarded-Proto": "https"}),
    )
    assert security.request_is_https(proxied) is True
    # Public surface keeps the loopback-only default.
    reader_security = load_public_security_config()
    assert reader_security.is_trusted_proxy("172.17.0.5") is False
