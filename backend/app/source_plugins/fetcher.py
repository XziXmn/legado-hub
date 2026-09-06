"""Controlled HTTP fetch wrapper for plugin runtime.

Routes through httpx with timeout, proxy, cookie, and trace controls.
"""

from __future__ import annotations

import asyncio
import logging
import re
import threading
from typing import Any
from types import SimpleNamespace

import httpx

from app.config import get_default_user_agent
from app.source_plugins.errors import (
    FetchNetworkError,
    FetchHttp4xx,
    FetchHttp5xx,
    RateLimited,
    CloudflareRequired,
    BrowserRequired,
)
from app.source_plugins.challenges import looks_like_browser_challenge, looks_like_cloudflare_challenge


_SHARED_CLIENTS: dict[tuple[int, int, str], httpx.AsyncClient] = {}

logger = logging.getLogger(__name__)

# httpx raises ImportError at client construction when http2=True without the
# optional h2 extra (requirements declare httpx[http2,...], but a dependency
# drift once dropped the transitive h2 and bricked every plugin fetch). Detect
# availability once and degrade to HTTP/1.1 instead of failing all requests.
try:
    import h2  # noqa: F401

    _HTTP2_AVAILABLE = True
except ImportError:
    _HTTP2_AVAILABLE = False
    logger.warning(
        "h2 package is not installed; plugin fetches fall back to HTTP/1.1. "
        "Install httpx with the http2 extra (pip install 'httpx[http2]')."
    )


def _shared_client_key(proxy_url: str) -> tuple[int, int, str]:
    """Key shared HTTP clients by event loop, thread, and proxy URL.

    AsyncClient instances are not guaranteed to be reusable across threads or
    event loops, so we scope the pool to the current async loop and thread.
    This still gives us connection reuse for the common request path handled
    by the API worker, while avoiding cross-loop transport issues in tests or
    background jobs.
    """
    try:
        loop_id = id(asyncio.get_running_loop())
    except RuntimeError:
        loop_id = 0
    return (threading.get_ident(), loop_id, proxy_url or "")


def _build_async_client(proxy_url: str, timeout: float) -> httpx.AsyncClient:
    mounts: dict[str, httpx.AsyncHTTPTransport] | None = None
    if proxy_url:
        mounts = {
            "all://": httpx.AsyncHTTPTransport(proxy=proxy_url),
        }
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout),
        mounts=mounts,
        follow_redirects=True,
        http2=_HTTP2_AVAILABLE,
        limits=httpx.Limits(
            max_connections=100,
            max_keepalive_connections=20,
            keepalive_expiry=30.0,
        ),
    )


def _clear_client_cookies(client: httpx.AsyncClient) -> None:
    """Keep shared clients as connection pools, not shared cookie jars."""
    try:
        client.cookies.clear()
    except Exception:
        pass


class Fetcher:
    def __init__(
        self,
        user_agent: str = "",
        timeout: float = 8.0,
        proxy_url: str = "",
        cookies: dict[str, dict[str, str]] | None = None,
        proxy_mode: str = "auto",
        proxy_config: dict | None = None,
    ):
        self.user_agent = user_agent or get_default_user_agent()
        self.timeout = timeout
        self.proxy_url = proxy_url
        self.proxy_mode = proxy_mode
        self.proxy_config = proxy_config or {}
        self._cookies = cookies or {}
        self._client: httpx.AsyncClient | None = None
        self._owns_client = False
        self._traces: list[dict] = []

    async def _client_instance(self, proxy_url: str | None = None) -> httpx.AsyncClient:
        actual_proxy_url = self.proxy_url if proxy_url is None else proxy_url
        key = _shared_client_key(actual_proxy_url)
        client = _SHARED_CLIENTS.get(key)
        if client is None:
            client = _build_async_client(actual_proxy_url, self.timeout)
            _SHARED_CLIENTS[key] = client
        if proxy_url is None or actual_proxy_url == self.proxy_url:
            self._client = client
            self._owns_client = False
        return client

    def _get_cookie_header(self, url: str) -> str:
        from urllib.parse import urlparse
        domain = urlparse(url).netloc
        jar: dict[str, str] = {}
        for cookie_domain, cookies in self._cookies.items():
            if self._domain_matches(cookie_domain, domain):
                jar.update(cookies)
        return "; ".join(f"{k}={v}" for k, v in jar.items())

    async def fetch_text(
        self,
        url: str,
        *,
        method: str = "GET",
        params: dict | None = None,
        data: dict | None = None,
        json: dict | None = None,
        headers: dict | None = None,
        timeout: float | None = None,
        impersonate: str | None = None,
        proxy: bool = True,
    ) -> str:
        text, _ = await self._fetch(url, method, params, data, json, headers, timeout, impersonate, proxy)
        return text

    async def fetch_json(
        self,
        url: str,
        *,
        method: str = "GET",
        params: dict | None = None,
        data: dict | None = None,
        json: dict | None = None,
        headers: dict | None = None,
        timeout: float | None = None,
        impersonate: str | None = None,
        proxy: bool = True,
    ) -> Any:
        text, _ = await self._fetch(url, method, params, data, json, headers, timeout, impersonate, proxy)
        import json as _json
        return _json.loads(text)

    async def fetch_bytes(
        self,
        url: str,
        *,
        method: str = "GET",
        params: dict | None = None,
        data: dict | None = None,
        json: dict | None = None,
        headers: dict | None = None,
        timeout: float | None = None,
        impersonate: str | None = None,
        proxy: bool = True,
    ) -> bytes:
        _, resp = await self._fetch(url, method, params, data, json, headers, timeout, impersonate, proxy)
        return resp.content

    async def fetch_many(
        self,
        urls: list[str],
        *,
        limit: int | None = None,
    ) -> list[str]:
        sem = asyncio.Semaphore(limit if limit is not None else 6)

        async def _one(url: str) -> str:
            async with sem:
                return await self.fetch_text(url)

        return await asyncio.gather(*[_one(u) for u in urls])

    async def _fetch(
        self,
        url: str,
        method: str,
        params: dict | None,
        data: dict | None,
        json: dict | None,
        headers: dict | None,
        timeout: float | None,
        impersonate: str | None,
        proxy: bool,
    ) -> tuple[str, httpx.Response]:
        from app.core.proxy import ProxyConfig, decide_proxy_mode, should_retry_with_proxy

        proxy_cfg = ProxyConfig.from_dict(self.proxy_config)
        try_direct, try_proxy = decide_proxy_mode(self.proxy_mode, proxy_cfg)

        # Caller override wins over plugin proxy.mode
        if proxy is False:
            try_direct = True
            try_proxy = False
        elif proxy is True and self.proxy_mode == "always":
            try_direct = False
            try_proxy = True

        last_error: Exception | None = None

        if try_direct:
            try:
                return await self._fetch_raw(url, method, params, data, json, headers, timeout, impersonate, proxy=False)
            except Exception as exc:
                last_error = exc
                if not (try_proxy and should_retry_with_proxy(exc, proxy_cfg)):
                    raise

        if try_proxy:
            try:
                return await self._fetch_raw(url, method, params, data, json, headers, timeout, impersonate, proxy=True)
            except Exception as exc:
                if last_error is not None:
                    raise last_error from exc
                raise

        if last_error is not None:
            raise last_error
        raise FetchNetworkError("proxy disabled for this request")

    async def _fetch_raw(
        self,
        url: str,
        method: str,
        params: dict | None,
        data: dict | None,
        json: dict | None,
        headers: dict | None,
        timeout: float | None,
        impersonate: str | None = None,
        proxy: bool = True,
    ) -> tuple[str, Any]:
        if impersonate:
            return await self._fetch_raw_impersonate(url, method, params, data, json, headers, timeout, impersonate, proxy)
        if proxy is False and self.proxy_url:
            client = await self._client_instance(proxy_url="")
        else:
            client = await self._client_instance()
        req_headers = dict(headers) if headers else {}
        req_headers.setdefault("User-Agent", self.user_agent)
        cookie_hdr = self._get_cookie_header(url)
        if cookie_hdr:
            req_headers.setdefault("Cookie", cookie_hdr)
        _clear_client_cookies(client)
        try:
            resp = await client.request(
                method,
                url,
                params=params,
                data=data,
                json=json,
                headers=req_headers,
                timeout=timeout if timeout is not None else self.timeout,
            )
        except httpx.NetworkError as exc:
            raise FetchNetworkError(str(exc)) from exc
        except httpx.TimeoutException as exc:
            from app.source_plugins.errors import PluginTimeout
            raise PluginTimeout(str(exc)) from exc
        finally:
            _clear_client_cookies(client)

        if resp.status_code == 429:
            raise RateLimited(f"HTTP {resp.status_code}")
        if 400 <= resp.status_code < 500:
            body_sample = resp.text[:1000]
            if looks_like_cloudflare_challenge(body_sample):
                raise CloudflareRequired(
                    "Cloudflare verification required",
                    url=str(resp.url),
                    status_code=resp.status_code,
                    body_sample=body_sample,
                )
            if looks_like_browser_challenge(body_sample):
                raise BrowserRequired(
                    "Browser verification required",
                    url=str(resp.url),
                    status_code=resp.status_code,
                    body_sample=body_sample,
                )
            raise FetchHttp4xx(f"HTTP {resp.status_code}")
        if 500 <= resp.status_code < 600:
            raise FetchHttp5xx(f"HTTP {resp.status_code}")

        # Update cookies from response
        self._update_cookies(resp)

        # httpx defaults to UTF-8 when a legacy Chinese site omits charset.
        # Prefer the page declaration, then fall back to GB18030/GBK.
        text = self._decode_response_text(resp)
        if looks_like_cloudflare_challenge(text):
            raise CloudflareRequired(
                "Cloudflare verification required",
                url=str(resp.url),
                status_code=resp.status_code,
                body_sample=text[:1000],
            )
        self._traces.append({
            "url": str(resp.url),
            "status": resp.status_code,
            "method": method,
            "proxy_used": proxy is not False and bool(self.proxy_url),
        })
        return text, resp

    async def _fetch_raw_impersonate(
        self,
        url: str,
        method: str,
        params: dict | None,
        data: dict | None,
        json: dict | None,
        headers: dict | None,
        timeout: float | None,
        impersonate: str,
        proxy: bool = True,
    ) -> tuple[str, Any]:
        try:
            from curl_cffi.requests import AsyncSession
        except Exception as exc:
            raise FetchNetworkError("curl_cffi is required for impersonated fetch") from exc

        req_headers = dict(headers) if headers else {}
        req_headers.setdefault("User-Agent", self.user_agent)
        cookie_hdr = self._get_cookie_header(url)
        if cookie_hdr:
            req_headers.setdefault("Cookie", cookie_hdr)
        proxies = None
        if proxy is not False and self.proxy_url:
            proxies = {"http": self.proxy_url, "https": self.proxy_url}
        try:
            async with AsyncSession(impersonate=impersonate, proxies=proxies, timeout=timeout if timeout is not None else self.timeout) as session:
                resp = await session.request(
                    method,
                    url,
                    params=params,
                    data=data,
                    json=json,
                    headers=req_headers,
                    allow_redirects=True,
                )
        except Exception as exc:
            message = str(exc).lower()
            if "timeout" in message:
                from app.source_plugins.errors import PluginTimeout
                raise PluginTimeout(str(exc)) from exc
            raise FetchNetworkError(str(exc)) from exc

        if resp.status_code == 429:
            raise RateLimited(f"HTTP {resp.status_code}")
        if 400 <= resp.status_code < 500:
            body_sample = resp.text[:1000]
            if looks_like_cloudflare_challenge(body_sample):
                raise CloudflareRequired(
                    "Cloudflare verification required",
                    url=str(resp.url),
                    status_code=resp.status_code,
                    body_sample=body_sample,
                )
            if looks_like_browser_challenge(body_sample):
                raise BrowserRequired(
                    "Browser verification required",
                    url=str(resp.url),
                    status_code=resp.status_code,
                    body_sample=body_sample,
                )
            raise FetchHttp4xx(f"HTTP {resp.status_code}")
        if 500 <= resp.status_code < 600:
            raise FetchHttp5xx(f"HTTP {resp.status_code}")

        self._update_cookies(resp)
        text = self._decode_response_text(resp)
        if looks_like_cloudflare_challenge(text):
            raise CloudflareRequired(
                "Cloudflare verification required",
                url=str(resp.url),
                status_code=resp.status_code,
                body_sample=text[:1000],
            )
        wrapped = SimpleNamespace(
            status_code=resp.status_code,
            url=str(resp.url),
            text=text,
            content=resp.content,
            headers=SimpleNamespace(get_list=lambda _name: []),
        )
        self._traces.append({
            "url": str(resp.url),
            "status": resp.status_code,
            "method": method,
            "impersonate": impersonate,
            "proxy_used": proxy is not False and bool(self.proxy_url),
        })
        return text, wrapped

    def _decode_response_text(self, resp: Any) -> str:
        content = getattr(resp, "content", b"") or b""
        if not isinstance(content, bytes):
            return str(getattr(resp, "text", "") or "")
        charset = self._charset_from_html_meta(content[:4096])
        if not charset:
            content_type = ""
            headers = getattr(resp, "headers", {}) or {}
            try:
                content_type = str(headers.get("content-type", "") or headers.get("Content-Type", ""))
            except Exception:
                content_type = ""
            match = re.search(r"charset=([A-Za-z0-9._-]+)", content_type, re.I)
            charset = match.group(1) if match else ""
        # ISO-8859-1 is a common server-side default rather than a real page
        # declaration. It always decodes and would hide the GBK fallback.
        if charset.lower() in {"ascii", "iso-8859-1", "latin-1", "latin1"}:
            charset = ""
        for encoding in [charset, "utf-8", "gb18030", "gbk"]:
            if not encoding:
                continue
            try:
                return content.decode(encoding)
            except (LookupError, UnicodeDecodeError):
                continue
        return content.decode("utf-8", errors="replace")

    def _charset_from_html_meta(self, sample: bytes) -> str:
        text = sample.decode("ascii", errors="ignore")
        match = re.search(r"<meta[^>]+charset=[\"']?\s*([A-Za-z0-9._-]+)", text, re.I)
        if match:
            return match.group(1).lower()
        match = re.search(r"content=[\"'][^\"']*charset=([A-Za-z0-9._-]+)", text, re.I)
        return match.group(1).lower() if match else ""

    def _update_cookies(self, resp: httpx.Response) -> None:
        from urllib.parse import urlparse
        domain = urlparse(str(resp.url)).netloc
        set_cookie = resp.headers.get_list("set-cookie")
        for raw in set_cookie:
            if "=" in raw:
                key, val = raw.split("=", 1)
                val = val.split(";")[0]
                cookie_domain = self._cookie_domain_from_header(raw) or domain
                self._cookies.setdefault(self._normalize_cookie_domain(cookie_domain), {})[key.strip()] = val.strip()

    def get_traces(self) -> list[dict]:
        return list(self._traces)

    async def close(self) -> None:
        if self._client is not None and self._owns_client:
            await self._client.aclose()
        self._client = None

    def cookies_for_domain(self, domain: str) -> dict[str, str]:
        return dict(self._cookies.get(domain, {}))

    def cookie_snapshot(self) -> dict[str, dict[str, str]]:
        return {domain: dict(jar) for domain, jar in self._cookies.items()}

    def set_cookie(self, domain: str, name: str, value: str) -> None:
        self._cookies.setdefault(self._normalize_cookie_domain(domain), {})[name] = value

    def clear_cookies(self, domain: str | None = None) -> None:
        if domain is None:
            self._cookies.clear()
        else:
            self._cookies.pop(self._normalize_cookie_domain(domain), None)

    def _domain_matches(self, cookie_domain: str, request_domain: str) -> bool:
        cookie_domain = self._normalize_cookie_domain(cookie_domain)
        request_domain = self._normalize_cookie_domain(request_domain)
        return request_domain == cookie_domain or request_domain.endswith(f".{cookie_domain}")

    def _normalize_cookie_domain(self, domain: str) -> str:
        return str(domain or "").split(":", 1)[0].strip().lstrip(".").lower()

    def _cookie_domain_from_header(self, raw: str) -> str:
        for part in raw.split(";")[1:]:
            item = part.strip()
            if item.lower().startswith("domain="):
                return item.split("=", 1)[1].strip()
        return ""
