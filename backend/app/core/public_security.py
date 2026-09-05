"""Reader/admin request, URL, proxy, and filesystem security boundaries."""

from __future__ import annotations

import ipaddress
import logging
import os
import re
import secrets
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import urlsplit

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.middleware.trustedhost import TrustedHostMiddleware

from app import config

UNSAFE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
SESSION_COOKIE_NAME = "legadohub_session"
_DNS_LABEL_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
_MAX_FORWARDED_CHAIN_LENGTH = 16
_MAX_FORWARDED_HEADER_LENGTH = 1024
_LAN_NETWORKS = tuple(
    ipaddress.ip_network(value)
    for value in (
        "10.0.0.0/8",
        "127.0.0.0/8",
        "169.254.0.0/16",
        "172.16.0.0/12",
        "192.168.0.0/16",
        "::1/128",
        "fc00::/7",
        "fe80::/10",
    )
)
_LAN_DNS_SUFFIXES = (".home", ".home.arpa", ".lan", ".local")
logger = logging.getLogger(__name__)
_SECURITY_LOG_INTERVAL_SECONDS = 60
_security_log_lock = threading.Lock()
_security_last_log: dict[str, float] = {}


def _csv(name: str) -> list[str]:
    return [item.strip() for item in os.getenv(name, "").split(",") if item.strip()]


def _normalize_host(host: str) -> str:
    normalized = host.strip().lower().rstrip(".")
    try:
        address = ipaddress.ip_address(normalized)
        if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
            address = address.ipv4_mapped
        return str(address)
    except ValueError:
        return normalized


def _origin(value: str, *, label: str) -> tuple[str, str]:
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise RuntimeError(f"{label} must be an absolute HTTP(S) origin.")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise RuntimeError(f"{label} must not contain credentials, query, or fragment.")
    if parsed.path not in {"", "/"}:
        raise RuntimeError(f"{label} must not contain a path.")
    host = _normalize_host(parsed.hostname)
    if not _valid_host(host):
        raise RuntimeError(f"{label} contains an invalid host name.")
    default_port = 443 if parsed.scheme == "https" else 80
    try:
        port = parsed.port
    except ValueError as exc:
        raise RuntimeError(f"{label} contains an invalid port.") from exc
    port_suffix = f":{port}" if port and port != default_port else ""
    rendered_host = f"[{host}]" if ":" in host else host
    return f"{parsed.scheme}://{rendered_host}{port_suffix}", host


def _valid_host(host: str) -> bool:
    if "%" in host:
        return False
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        pass
    if len(host) > 253 or not host.isascii():
        return False
    labels = host.rstrip(".").split(".")
    return bool(labels) and all(_DNS_LABEL_PATTERN.fullmatch(label) for label in labels)


def _networks(values: Iterable[str]) -> tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]:
    networks: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
    for value in values:
        try:
            networks.append(ipaddress.ip_network(value, strict=False))
        except ValueError as exc:
            raise RuntimeError(f"Invalid trusted proxy network: {value}") from exc
    return tuple(networks)


@dataclass(frozen=True)
class PublicSecurityConfig:
    public_base_url: str
    allowed_hosts: tuple[str, ...]
    allowed_origins: frozenset[str]
    trusted_proxies: tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]
    dynamic_base_url: bool = False
    require_https: bool = False
    enforce_origin: bool = False
    # Reader entry only: public Host must match settings allowlist.
    enforce_reading_public_allowlist: bool = False
    # Admin listener: dynamic mode tolerates same-host origins that differ in
    # scheme/port (TLS-terminating reverse proxies without forwarded headers).
    admin_surface: bool = False
    origin_allow_same_host: bool = False

    def is_trusted_proxy(self, host: str) -> bool:
        try:
            address = ipaddress.ip_address(str(host or "").strip())
        except ValueError:
            return False
        return any(address in network for network in self.trusted_proxies)

    def request_is_https(self, request: Request) -> bool:
        if request.url.scheme == "https":
            return True
        client_host = request.client.host if request.client else ""
        if not self.is_trusted_proxy(client_host):
            return False
        forwarded_values = request.headers.getlist("x-forwarded-proto")
        if len(forwarded_values) != 1 or "," in forwarded_values[0]:
            return False
        return forwarded_values[0].strip().lower() == "https"

    def client_ip(self, request: Request) -> str:
        immediate = request.client.host if request.client and request.client.host else "unknown"
        if not self.is_trusted_proxy(immediate):
            return immediate
        forwarded_values = request.headers.getlist("x-forwarded-for")
        if len(forwarded_values) != 1:
            return immediate
        if len(forwarded_values[0]) > _MAX_FORWARDED_HEADER_LENGTH:
            return immediate
        values = forwarded_values[0].split(",")
        if not values or len(values) > _MAX_FORWARDED_CHAIN_LENGTH:
            return immediate
        chain: list[str] = []
        for value in values:
            candidate = value.strip()
            if not candidate:
                return immediate
            try:
                chain.append(str(ipaddress.ip_address(candidate)))
            except ValueError:
                return immediate
        for candidate in reversed(chain):
            if not self.is_trusted_proxy(candidate):
                return candidate
        return immediate


def load_public_security_config() -> PublicSecurityConfig:
    """Reader security: Host is taken from the request.

    Optional ``LEGADOHUB_ALLOWED_HOSTS`` / ``_ORIGINS`` still enable TrustedHost
    and cookie Origin checks for locked-down deploys. Optional
    ``LEGADOHUB_REQUIRE_HTTPS=1`` forces HTTPS. Public book-source link base is
    selected by ``readingAccess.publicBaseUrl`` first, then by the optional
    ``LEGADOHUB_PUBLIC_BASE_URL`` deploy-time fallback.
    """
    base_url, _base_host = _origin(
        f"http://{config.HOST}:{config.PORT}",
        label="default reader base",
    )

    hosts = _csv("LEGADOHUB_ALLOWED_HOSTS")
    normalized_hosts: list[str] = []
    for host in hosts:
        normalized = _normalize_host(host)
        if not normalized or "*" in normalized or not _valid_host(normalized):
            raise RuntimeError("LEGADOHUB_ALLOWED_HOSTS must contain exact host names without wildcards.")
        normalized_hosts.append(normalized)

    origin_values = _csv("LEGADOHUB_ALLOWED_ORIGINS")
    origins = frozenset(
        _origin(value.rstrip("/"), label="LEGADOHUB_ALLOWED_ORIGINS")[0]
        for value in origin_values
    )

    require_https = os.getenv("LEGADOHUB_REQUIRE_HTTPS", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    if not require_https and origins:
        require_https = all(item.startswith("https://") for item in origins)

    proxy_values = _csv("LEGADOHUB_TRUSTED_PROXIES") or ["127.0.0.1/32", "::1/128"]
    trusted_proxies = _networks(proxy_values)
    return PublicSecurityConfig(
        public_base_url=base_url,
        allowed_hosts=tuple(dict.fromkeys(normalized_hosts)),
        allowed_origins=origins,
        trusted_proxies=trusted_proxies,
        dynamic_base_url=True,
        require_https=require_https,
        enforce_origin=require_https or bool(origins),
        enforce_reading_public_allowlist=False,
    )


def load_admin_security_config() -> PublicSecurityConfig:
    """Load the isolated management listener's host, origin, and proxy policy."""
    configured_base = os.getenv("LEGADOHUB_ADMIN_BASE_URL", "").strip().rstrip("/")
    dynamic_base_url = not configured_base
    base_url, base_host = _origin(
        configured_base or f"http://127.0.0.1:{config.ADMIN_PORT}",
        label="LEGADOHUB_ADMIN_BASE_URL",
    )
    require_https = base_url.startswith("https://")

    hosts = _csv("LEGADOHUB_ADMIN_ALLOWED_HOSTS")
    if not hosts and not dynamic_base_url:
        hosts = [base_host, "127.0.0.1", "localhost", "testserver"]
    normalized_hosts: list[str] = []
    for host in hosts:
        normalized = _normalize_host(host)
        if not normalized or "*" in normalized or not _valid_host(normalized):
            raise RuntimeError(
                "LEGADOHUB_ADMIN_ALLOWED_HOSTS must contain exact host names without wildcards."
            )
        normalized_hosts.append(normalized)
    if not dynamic_base_url and base_host not in normalized_hosts:
        raise RuntimeError(
            "LEGADOHUB_ADMIN_ALLOWED_HOSTS must include the admin base URL host."
        )

    origin_values = _csv("LEGADOHUB_ADMIN_ALLOWED_ORIGINS")
    if not origin_values and not dynamic_base_url:
        origin_values = [base_url]
    origins = frozenset(
        _origin(value.rstrip("/"), label="LEGADOHUB_ADMIN_ALLOWED_ORIGINS")[0]
        for value in origin_values
    )
    if not dynamic_base_url and base_url not in origins:
        raise RuntimeError(
            "LEGADOHUB_ADMIN_ALLOWED_ORIGINS must include LEGADOHUB_ADMIN_BASE_URL."
        )

    # The admin listener is a LAN/trusted-operator surface. Home-lab reverse
    # proxies (Synology/NPM/1Panel…) terminate TLS from a Docker bridge or LAN
    # address, so private networks are trusted for forwarded headers by default;
    # an explicit LEGADOHUB_ADMIN_TRUSTED_PROXIES still wins.
    proxy_values = _csv("LEGADOHUB_ADMIN_TRUSTED_PROXIES") or [
        "127.0.0.1/32",
        "::1/128",
        "10.0.0.0/8",
        "172.16.0.0/12",
        "192.168.0.0/16",
        "fc00::/7",
        "fe80::/10",
    ]
    return PublicSecurityConfig(
        public_base_url=base_url,
        allowed_hosts=tuple(dict.fromkeys(normalized_hosts)),
        allowed_origins=origins,
        trusted_proxies=_networks(proxy_values),
        dynamic_base_url=dynamic_base_url,
        require_https=require_https,
        enforce_origin=True,
        enforce_reading_public_allowlist=False,
        admin_surface=True,
        # Dynamic mode without an explicit origin allowlist: browsers behind a
        # TLS-terminating proxy send an Origin whose host matches the Host but
        # whose scheme/port differ (https://nas vs http://nas:8766). Host, not
        # scheme, is what same-site enforcement hinges on here.
        origin_allow_same_host=dynamic_base_url and not origins,
    )


def _is_default_allowed_host(host: str) -> bool:
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return _valid_host(host) and (
            host in {"localhost", "testserver"} or host.endswith(_LAN_DNS_SUFFIXES)
        )
    if isinstance(address, ipaddress.IPv6Address):
        if address.ipv4_mapped is None:
            return not address.is_unspecified and not address.is_multicast
        address = address.ipv4_mapped
    return any(
        address.version == network.version and address in network
        for network in _LAN_NETWORKS
    )


def is_lan_reading_base(base_url: str) -> bool:
    """True when a reading base URL host is private/local (LAN dual-source identity).

    Used to choose ``LegadoHub-LAN`` vs ``LegadoHub`` for bookSourceUrl/name so
    public and intranet imports can coexist in Reading. Broader than
    ``_is_default_allowed_host`` for bare public IPv6: only private/loopback/
    link-local ranges and known LAN DNS suffixes count as 内网.
    """
    raw = str(base_url or "").strip()
    if not raw:
        return False
    try:
        origin = normalize_public_base_url(raw)
    except RuntimeError:
        return False
    host = (urlsplit(origin).hostname or "").strip().lower()
    if not host:
        return False
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return host in {"localhost", "testserver"} or host.endswith(_LAN_DNS_SUFFIXES)
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        address = address.ipv4_mapped
    if address.is_loopback or address.is_private or address.is_link_local:
        return True
    return any(
        address.version == network.version and address in network
        for network in _LAN_NETWORKS
    )


def reading_network_lane(base_url: str) -> str:
    """Stable lane id for dual-source isolation: ``lan`` or ``public``."""
    return "lan" if is_lan_reading_base(base_url) else "public"


def _dynamic_host_client_allowed(request: Request) -> bool | None:
    immediate = request.client.host if request.client and request.client.host else ""
    normalized = _normalize_host(immediate)
    try:
        ipaddress.ip_address(normalized)
    except ValueError:
        # ASGI tests and internal transports may identify clients by name.
        return None
    return _is_default_allowed_host(normalized)


def parse_public_base_urls(raw: str) -> list[str]:
    """Parse one or more public origins (comma / semicolon / whitespace separated).

    Each entry must be a full origin (``https://host`` or ``https://host:port``).
    Order is preserved; duplicates after normalization are dropped.
    """
    text = str(raw or "").strip()
    if not text:
        return []
    parts = re.split(r"[\s,;]+", text)
    out: list[str] = []
    seen: set[str] = set()
    for part in parts:
        item = str(part or "").strip().rstrip("/")
        if not item:
            continue
        try:
            normalized = normalize_public_base_url(item)
        except RuntimeError:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def settings_public_base_urls() -> list[str]:
    """``readingAccess.publicBaseUrl`` from settings (single origin → 0..1 items)."""
    try:
        from app.core.app_config import AppConfig

        raw = str(AppConfig.get().reading_access.public_base_url or "").strip()
    except Exception:
        return []
    return parse_public_base_urls(raw)


def settings_public_base_url() -> str:
    """Configured 公网书源地址 from settings UI, or empty."""
    urls = settings_public_base_urls()
    return urls[0] if urls else ""


def env_public_base_urls() -> list[str]:
    """Deploy-time fallback for the public book-source origin."""
    return parse_public_base_urls(os.getenv("LEGADOHUB_PUBLIC_BASE_URL", ""))


def effective_public_base_urls() -> list[str]:
    """Configured public origins: settings override the deploy-time fallback."""
    settings = settings_public_base_urls()
    return settings if settings else env_public_base_urls()


def effective_public_base_url() -> str:
    """Primary **公网书源地址** for issued links (settings > environment).

    Empty means issued-link UI falls back to request Host / LAN only.
    Does **not** gate HTTP Host acceptance.
    """
    urls = effective_public_base_urls()
    return urls[0] if urls else ""


def _request_origin(request: Request, security: PublicSecurityConfig) -> str:
    """Resolve request origin from Host / forwarded proto.

    Public Host access control is **not** enforced here — put perimeter rules
    on reverse proxy / WAF (e.g. 雷池). The configured public base URL only
    affects preferred issued book-source links, not whether a Host is served.

    LAN Host spoofing from a public peer is still rejected so generated
    book-source bases cannot be forced to private IPs over the Internet.
    """
    scheme = "https" if security.request_is_https(request) else request.url.scheme.lower()
    if scheme not in {"http", "https"}:
        raise RuntimeError("Request scheme is not HTTP(S).")
    origin, host = _origin(
        f"{scheme}://{request.headers.get('host', '').strip()}",
        label="Host",
    )
    # Reject unusable IP literals (not an access allowlist — just invalid Hosts).
    try:
        address = ipaddress.ip_address(host)
        if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
            address = address.ipv4_mapped
        if address.is_unspecified or address.is_multicast:
            raise RuntimeError("Host is not allowed")
    except ValueError:
        pass

    if _is_default_allowed_host(host):
        client_allowed = _dynamic_host_client_allowed(request)
        if client_allowed is False:
            raise RuntimeError(
                "LAN Host requires a local/private client address."
            )
        return origin

    # Public domain / public IP Host: accept any valid origin (auth still required
    # for APIs). Perimeter filtering is operator-owned (WAF / 雷池 / firewall).
    # Legacy allowlist flag kept for forks that re-enable it.
    if security.enforce_reading_public_allowlist:
        allowlist = reading_public_base_allowlist()
        if not allowlist or not public_origin_is_allowlisted(origin, allowlist):
            raise RuntimeError("Public Host is not allowlisted")
        return origin

    return origin


def reading_public_base_allowlist() -> frozenset[str]:
    """Public origins allowed for reading Host: settings UI > env bootstrap.

    Supports multiple origins. Prefer portless HTTPS/HTTP (443/80); non-default
    ports remain valid when explicitly listed. Empty means only LAN/local Hosts
    are accepted under dynamic mode.
    """
    return frozenset(effective_public_base_urls())


def public_origin_is_allowlisted(origin: str, allowlist: frozenset[str] | None = None) -> bool:
    """True if request origin is allowed for public reading Host checks.

    Matching rules (after normalization):
    1. Exact origin match (scheme + host + non-default port).
    2. Same scheme + host as any allowlisted origin — **port ignored**.
       So listing ``https://book.example.com`` also allows ``:2087`` reverse
       proxies and direct ``:8765`` on that hostname.
    """
    allowed = allowlist if allowlist is not None else reading_public_base_allowlist()
    if not allowed:
        return False
    try:
        request_origin = normalize_public_base_url(origin)
    except RuntimeError:
        return False
    if request_origin in allowed:
        return True
    try:
        req_parts = urlsplit(request_origin)
        req_host = _normalize_host(req_parts.hostname or "")
        req_scheme = (req_parts.scheme or "").lower()
    except Exception:
        return False
    if not req_host or req_scheme not in {"http", "https"}:
        return False
    for item in allowed:
        try:
            parts = urlsplit(item)
            host = _normalize_host(parts.hostname or "")
            scheme = (parts.scheme or "").lower()
        except Exception:
            continue
        if host and host == req_host and scheme == req_scheme:
            return True
    return False


def get_public_base_url(request: Request | None = None) -> str:
    """Resolve the reading base for this request (or offline default).

    With a request: use Host / trusted forwarded proto (dynamic).
    Offline: process default base (not the settings 公网书源地址 — that is only
    for issued subscription links via ``effective_public_base_url``).
    """
    security = (
        getattr(request.app.state, "public_security", None)
        if request is not None
        else None
    ) or load_public_security_config()
    if request is not None:
        return _request_origin(request, security)
    return security.public_base_url


def reading_base_url(request: Request | None = None) -> str:
    """Base URL for Reading book-source JSON and access/enter links.

    Always targets the **reader** entrypoint. Admin console is often on
    ``ADMIN_PORT`` (8766); baking that into LEGADOHUB_BASE makes
    ``/api/auth/access/enter`` 404 because access routes only register on the
    public listener.
    """
    return ensure_reader_entrypoint_origin(get_public_base_url(request), request=request)


def reader_external_origin_env() -> str:
    """``LEGADOHUB_READER_EXTERNAL_ORIGIN`` — the reader entrypoint as clients see it.

    Docker port mappings (``4390:8765``) make the external reader port differ
    from ``config.PORT``; the backend cannot discover it, so operators declare
    it. Accepts a full origin (``http://192.168.31.5:4390``) or a bare external
    port (``4390``, applied to the base's host).
    """
    return os.getenv("LEGADOHUB_READER_EXTERNAL_ORIGIN", "").strip().rstrip("/")


def _reader_external_target(base_host: str) -> str:
    """Configured externally reachable reader origin for ``base_host``, or ""."""
    raw = reader_external_origin_env()
    if raw:
        if raw.isdigit() and len(raw) <= 5:
            port = int(raw)
            if 1 <= port <= 65535:
                return f"http://{base_host}:{port}"
        else:
            try:
                return normalize_public_base_url(raw)
            except RuntimeError:
                pass
    # Settings/env 公网书源地址 entries only apply to their own host so a
    # public domain never leaks into a LAN origin's rewrite.
    for candidate in effective_public_base_urls():
        if _origin_host(candidate) == base_host:
            return candidate
    return ""


def _origin_host(value: str) -> str:
    try:
        return _normalize_host(urlsplit(value).hostname or "")
    except Exception:
        return ""


def _request_is_admin_surface(request: Request | None) -> bool:
    return bool(request is not None and getattr(request.app.state, "entrypoint", "") == "admin")


def ensure_reader_entrypoint_origin(base: str, *, request: Request | None = None) -> str:
    """Rewrite admin-entrypoint origins to the externally reachable reader origin.

    An origin on the admin surface — internal ``ADMIN_PORT``, or the origin of
    a request served by the admin listener — must not be baked into generated
    links: ``/api/auth/access/enter`` and the Reading APIs only register on the
    reader listener. Under Docker port mappings the external reader port is
    neither derivable from ``config.PORT`` nor equal to the admin port, so the
    reader target resolves as:

    1. ``LEGADOHUB_READER_EXTERNAL_ORIGIN`` (full origin or bare external port).
    2. A configured 公网书源地址 entry whose host matches the base host.
    3. The current request's own origin when it is served by the reader
       listener and shares the base host.
    4. Legacy fallback: same host + ``config.PORT`` (host port == container port).

    Origins that are not on the admin surface pass through unchanged.
    """
    from urllib.parse import urlsplit, urlunsplit

    from app.config import ADMIN_PORT, PORT as PUBLIC_PORT

    raw = str(base or "").strip().rstrip("/")
    if not raw:
        return ""
    try:
        normalized = normalize_public_base_url(raw)
    except Exception:
        normalized = raw
    parts = urlsplit(normalized if "://" in normalized else f"http://{normalized}")
    host = parts.hostname or ""
    if not host:
        return normalized
    port = parts.port
    scheme = (parts.scheme or "http").lower()
    on_admin_surface = port == ADMIN_PORT
    if not on_admin_surface and _request_is_admin_surface(request):
        try:
            request_origin = get_public_base_url(request) if request is not None else ""
        except Exception:
            request_origin = ""
        on_admin_surface = _origin_host(request_origin) == _normalize_host(host)
    if not on_admin_surface:
        return normalized.rstrip("/")

    # Admin surface → externally reachable reader origin.
    configured = _reader_external_target(_normalize_host(host))
    if configured:
        return configured.rstrip("/")
    if request is not None and not _request_is_admin_surface(request):
        try:
            request_origin = get_public_base_url(request)
        except Exception:
            request_origin = ""
        if request_origin and _origin_host(request_origin) == _normalize_host(host):
            return request_origin.rstrip("/")
    # Legacy fallback: same host on the internal reader port.
    if (scheme == "http" and PUBLIC_PORT == 80) or (scheme == "https" and PUBLIC_PORT == 443):
        netloc = host
    else:
        netloc = f"{host}:{PUBLIC_PORT}"
    userinfo = ""
    if parts.username:
        userinfo = parts.username
        if parts.password:
            userinfo += f":{parts.password}"
        userinfo += "@"
    return urlunsplit((scheme, f"{userinfo}{netloc}", "", "", "")).rstrip("/")


def normalize_public_base_url(value: str) -> str:
    return _origin(str(value or "").strip().rstrip("/"), label="public base URL")[0]


def request_uses_https(request: Request) -> bool:
    security = getattr(request.app.state, "public_security", None) or load_public_security_config()
    return bool(security.require_https or security.request_is_https(request))


def request_client_ip(request: Request) -> str:
    security = getattr(request.app.state, "public_security", None) or load_public_security_config()
    return security.client_ip(request)


def _apply_response_headers(response, security: PublicSecurityConfig, *, api_response: bool) -> None:
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    response.headers.setdefault("Cross-Origin-Resource-Policy", "same-origin")
    response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
    response.headers.setdefault("Content-Security-Policy", "frame-ancestors 'none'; base-uri 'self'; object-src 'none'")
    if api_response:
        response.headers["Cache-Control"] = "no-store"
    if security.require_https:
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")


def _security_rejection(
    *,
    security: PublicSecurityConfig,
    request_id: str,
    event: str,
    status_code: int,
    detail: str,
    api_response: bool,
    content: dict | None = None,
    log_context: str = "",
) -> JSONResponse:
    now = time.monotonic()
    should_log = False
    with _security_log_lock:
        last_logged = _security_last_log.get(event, 0.0)
        if now - last_logged >= _SECURITY_LOG_INTERVAL_SECONDS:
            _security_last_log[event] = now
            should_log = True
    if should_log:
        logger.warning(
            "Security request rejected: event=%s request_id=%s%s",
            event,
            request_id,
            f" {log_context}" if log_context else "",
        )
    response = JSONResponse(
        status_code=status_code,
        content=content if content is not None else {"detail": detail},
    )
    response.headers["X-Request-ID"] = request_id
    _apply_response_headers(response, security, api_response=api_response)
    return response


def _admin_origin_hint(origin: str, request_origin: str) -> dict:
    """Structured, actionable detail for admin-surface origin rejections.

    The admin login previously failed with a bare English "Origin is not
    allowed", which users read as a wrong password. Name the mismatch and the
    exact fixes so operators can self-serve.
    """
    observed = origin or "(missing)"
    return {
        "code": "admin_origin_rejected",
        "observedOrigin": observed,
        "expectedOrigin": request_origin,
        "message": (
            f"来源校验未通过：浏览器提交的 Origin「{observed}」与后端识别的访问地址"
            f"「{request_origin}」不一致。若经反向代理访问，请透传 Host 与协议："
            "proxy_set_header Host $http_host; proxy_set_header X-Forwarded-Proto $scheme;"
            "（信任的代理网段默认含内网地址段），或在环境变量 "
            "LEGADOHUB_ADMIN_ALLOWED_ORIGINS 中加入浏览器地址栏的完整访问地址后重启。"
        ),
    }


def _origin_same_host(origin: str, request_origin: str) -> bool:
    if not origin or not request_origin:
        return False
    return bool(_origin_host(origin)) and _origin_host(origin) == _origin_host(request_origin)


def install_public_security(app: FastAPI, security: PublicSecurityConfig) -> None:
    app.state.public_security = security
    if security.allowed_hosts:
        app.add_middleware(TrustedHostMiddleware, allowed_hosts=list(security.allowed_hosts))

    @app.middleware("http")
    async def public_security_boundary(request: Request, call_next):
        request_id = secrets.token_hex(16)
        request.state.request_id = request_id
        api_response = request.url.path.startswith("/api/")
        request_origin = ""
        if security.dynamic_base_url:
            try:
                request_origin = _request_origin(request, security)
            except RuntimeError:
                host_hint = (
                    {
                        "code": "admin_host_rejected",
                        "message": (
                            "Host 校验未通过：请求的 Host 与客户端地址不匹配"
                            "（内网 Host 仅接受来自本地/内网的客户端）。"
                            "若经反向代理访问，请透传 Host（proxy_set_header Host $http_host）"
                            "并把代理网段加入 LEGADOHUB_ADMIN_TRUSTED_PROXIES；"
                            "固定域名访问请设置 LEGADOHUB_ADMIN_BASE_URL。"
                        ),
                    }
                    if security.admin_surface
                    else None
                )
                return _security_rejection(
                    security=security,
                    request_id=request_id,
                    event="host_rejected",
                    status_code=400,
                    detail="Host is not allowed",
                    api_response=api_response,
                    content={"detail": host_hint} if host_hint else None,
                )
        if security.require_https and request.url.path != "/health" and not security.request_is_https(request):
            return _security_rejection(
                security=security,
                request_id=request_id,
                event="https_required",
                status_code=400,
                detail="HTTPS is required",
                api_response=api_response,
            )

        if security.enforce_origin and request.method.upper() in UNSAFE_METHODS:
            origin = request.headers.get("origin", "").strip().rstrip("/")
            if origin:
                try:
                    normalized_origin = _origin(origin, label="Origin")[0]
                except RuntimeError:
                    normalized_origin = ""
                origin_allowed = (
                    normalized_origin in security.allowed_origins
                    or normalized_origin == request_origin
                    or (
                        security.origin_allow_same_host
                        and _origin_same_host(normalized_origin, request_origin)
                    )
                )
                if not origin_allowed:
                    admin_hint = (
                        _admin_origin_hint(normalized_origin, request_origin)
                        if security.admin_surface
                        else None
                    )
                    return _security_rejection(
                        security=security,
                        request_id=request_id,
                        event="origin_rejected",
                        status_code=403,
                        detail="Origin is not allowed",
                        api_response=api_response,
                        content={"detail": admin_hint} if admin_hint else None,
                        log_context=(
                            f"origin={normalized_origin!r} expected={request_origin!r}"
                            if security.admin_surface
                            else ""
                        ),
                    )
            elif SESSION_COOKIE_NAME in request.cookies and not request.headers.get("authorization"):
                admin_hint = (
                    {
                        "code": "admin_origin_missing",
                        "message": (
                            "缺少 Origin 请求头：携带会话 Cookie 的写请求必须带 Origin。"
                            "请勿在浏览器扩展/代理中剥离 Origin、Referer 请求头后重试。"
                        ),
                    }
                    if security.admin_surface
                    else None
                )
                return _security_rejection(
                    security=security,
                    request_id=request_id,
                    event="origin_missing",
                    status_code=403,
                    detail="Origin header is required",
                    api_response=api_response,
                    content={"detail": admin_hint} if admin_hint else None,
                )

        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        _apply_response_headers(response, security, api_response=api_response)
        return response


def prepare_runtime_permissions() -> None:
    if os.name == "nt":
        return
    os.umask(0o077)
    directories = [config.CONFIG_DIR, config.COOKIE_DIR, config.DATA_DIR, config.GENERATED_DIR, config.RUNTIME_DIR]
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        directory.chmod(0o700)
    protected_files: list[Path] = [config.APP_CONFIG_PATH, config.DB_PATH]
    if config.COOKIE_DIR.exists():
        protected_files.extend(path for path in config.COOKIE_DIR.glob("*.json") if path.is_file())
    for path in protected_files:
        if path.exists():
            path.chmod(0o600)
