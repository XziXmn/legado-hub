from __future__ import annotations

import httpx
import pytest

from app.source_plugins.fetcher import Fetcher


class _Cookies:
    def clear(self) -> None:
        pass


class _Client:
    def __init__(self) -> None:
        self.cookies = _Cookies()
        self.headers: dict[str, str] = {}

    async def request(self, method: str, url: str, **kwargs) -> httpx.Response:
        self.headers = dict(kwargs.get("headers") or {})
        return httpx.Response(200, text="ok", request=httpx.Request(method, url))


@pytest.mark.asyncio
async def test_explicit_cookie_header_is_not_overwritten_by_cookie_store(monkeypatch):
    fetcher = Fetcher(cookies={"if.qidian.com": {"cmfuToken": "stored-token"}})
    client = _Client()

    async def client_instance(proxy_url=None):
        return client

    monkeypatch.setattr(fetcher, "_client_instance", client_instance)
    await fetcher._fetch_raw(
        "https://druidv6.if.qidian.com/argus/api/v1/test",
        "GET",
        {},
        None,
        None,
        {"Cookie": "QDInfo=signed-profile; qid=device-id"},
        None,
    )

    assert client.headers["Cookie"] == "QDInfo=signed-profile; qid=device-id"


@pytest.mark.asyncio
async def test_cookie_store_header_is_used_when_request_has_no_cookie(monkeypatch):
    fetcher = Fetcher(cookies={"example.com": {"session": "stored"}})
    client = _Client()

    async def client_instance(proxy_url=None):
        return client

    monkeypatch.setattr(fetcher, "_client_instance", client_instance)
    await fetcher._fetch_raw(
        "https://example.com/data",
        "GET",
        {},
        None,
        None,
        {},
        None,
    )

    assert client.headers["Cookie"] == "session=stored"


# ---- http2 availability regression (primp 2.0 dropped transitive h2) ----


def test_build_async_client_follows_h2_availability(monkeypatch):
    from app.source_plugins import fetcher as fetcher_module

    captured: dict = {}
    real_client = fetcher_module.httpx.AsyncClient

    def spy_client(**kwargs):
        captured.update(kwargs)
        return real_client(**kwargs)

    monkeypatch.setattr(fetcher_module.httpx, "AsyncClient", spy_client)

    monkeypatch.setattr(fetcher_module, "_HTTP2_AVAILABLE", False)
    client = fetcher_module._build_async_client("", 5.0)
    assert captured.get("http2") is False

    monkeypatch.setattr(fetcher_module, "_HTTP2_AVAILABLE", True)
    client = fetcher_module._build_async_client("", 5.0)
    assert captured.get("http2") is True


def test_h2_flag_matches_local_availability() -> None:
    try:
        import h2  # noqa: F401

        expected = True
    except ImportError:
        expected = False
    from app.source_plugins import fetcher as fetcher_module

    assert fetcher_module._HTTP2_AVAILABLE is expected
