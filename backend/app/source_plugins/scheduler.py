"""Plugin scheduler: execute plugins concurrently from LegadoHub core."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Awaitable, Callable

from app.core.app_config import AppConfig
from app.services.cookie_store import CookieStore
from app.source_plugins.loader import PluginLoader
from app.source_plugins.context import CookieJar, PluginContext
from app.source_plugins.fetcher import Fetcher
from app.source_plugins.models import (
    LoadedPlugin,
    SearchResult,
    BookDetail,
    ChapterItem,
    ChapterContent,
    PluginFailure,
)
from app.source_plugins.errors import (
    PluginExecutionError,
    PluginTimeout,
    ERROR_CODE_MAP,
    normalize_failure,
)
from app.source_plugins.id_codec import encode_book_id, encode_chapter_id

import threading


class _PluginRateLimiter:
    """Host-owned limiter for a plugin's declared operational envelope."""

    def __init__(self, *, concurrency: int, min_interval_ms: int):
        self._semaphore = asyncio.Semaphore(concurrency)
        self._interval_lock = asyncio.Lock()
        self._min_interval_seconds = min_interval_ms / 1000.0
        self._next_start_at = 0.0

    async def run(self, operation: Callable[[], Awaitable[Any]]) -> Any:
        async with self._semaphore:
            if self._min_interval_seconds:
                async with self._interval_lock:
                    while (wait_seconds := self._next_start_at - time.monotonic()) > 0:
                        await asyncio.sleep(wait_seconds)
                    self._next_start_at = time.monotonic() + self._min_interval_seconds
            return await operation()


class PluginScheduler:
    def __init__(
        self,
        loader: PluginLoader | None = None,
        config: dict | None = None,
    ):
        self.loader = loader or PluginLoader()
        self._plugins: dict[str, LoadedPlugin] = {}
        self.config = self._default_config() if config is None else config
        self._cookie_store = CookieStore()
        self._official_source_queue = asyncio.Semaphore(3)
        self._browser_source_queue = asyncio.Semaphore(
            self._positive_int(self.config.get("browser_source_concurrency"), 3)
        )
        self._plugin_rate_limiters: dict[str, _PluginRateLimiter] = {}
        self._load_plugins()

    def _default_config(self) -> dict:
        cfg = AppConfig.get()
        return {
            "proxy": {
                "enabled": cfg.proxy.enabled,
                "url": cfg.proxy.url,
                "allowAutoRetry": cfg.proxy.allow_auto_retry,
            },
            "max_concurrency": cfg.search.global_source_concurrency,
            "source_timeout_seconds": cfg.search.source_timeout_seconds,
            "source_hard_timeout_seconds": cfg.search.source_hard_timeout_seconds,
            "overall_search_timeout_seconds": cfg.search.overall_timeout_seconds,
            "source_batch_size": 20,
            "browser_source_concurrency": cfg.search.browser_source_concurrency,
            "browser_source_timeout_seconds": cfg.search.browser_source_timeout_seconds,
            "browser_search_timeout_seconds": cfg.search.browser_search_timeout_seconds,
            "default_user_agent": cfg.search.default_user_agent,
        }

    def _load_plugins(self) -> None:
        self._plugins = self.loader.load_all()
        # Apply runtime enabled overrides from app_config.json on top of metadata.
        try:
            from app.core.app_config import AppConfig

            cfg = AppConfig.get()
            for plugin_id, plugin in self._plugins.items():
                plugin.metadata.enabled = cfg.is_plugin_enabled(
                    plugin_id, default=plugin.metadata.enabled
                )
        except Exception:
            pass

    def reload(self) -> None:
        self._load_plugins()

    async def close(self) -> None:
        bridge = getattr(self, "_access_bridge", None)
        if bridge is not None:
            await bridge.close()
            self._access_bridge = None

    def refresh_config(self) -> None:
        """Rebuild runtime config from the canonical AppConfig file."""
        self.config = self._default_config()

    def _enabled_plugins(self) -> list[LoadedPlugin]:
        return [p for p in self._plugins.values() if p.metadata.enabled]

    def _search_priority_plugins(self, plugins: list[LoadedPlugin]) -> list[LoadedPlugin]:
        """Run fast/reliable sources first for better partial results.

        Official sources are still kept first when they are included. Ordering
        is based purely on runtime metadata (priority, name); no persistent
        health table is consulted.
        """
        def sort_key(plugin: LoadedPlugin) -> tuple:
            priority = getattr(plugin.metadata, "priority", 50) or 50
            return (
                0 if plugin.metadata.is_official_source() else 1,
                priority,
                plugin.metadata.name or "",
                plugin.metadata.id,
            )

        return sorted(plugins, key=sort_key)

    def _official_explore_plugins(self) -> list[LoadedPlugin]:
        return [
            p
            for p in self._enabled_plugins()
            if "explore" in p.capabilities and p.metadata.is_official_source()
        ]

    def _resolve_proxy_url(self, plugin: LoadedPlugin | None) -> str:
        """Return the proxy URL for a plugin under the tightened policy.

        Policy:
        - Direct by default.
        - ``never`` => direct.
        - ``always`` => proxy.
        - ``auto`` => direct first, then proxy for configured retryable failures.
        """
        proxy_cfg = self.config.get("proxy", {})
        if not proxy_cfg.get("enabled"):
            return ""
        proxy_meta = plugin.metadata.proxy if plugin else {}
        proxy_mode = proxy_meta.get("mode", "auto")
        if proxy_mode == "never":
            return ""
        if proxy_mode == "always":
            return proxy_cfg.get("url", "")
        if proxy_mode == "auto" and proxy_cfg.get("allowAutoRetry"):
            return proxy_cfg.get("url", "")
        return ""

    def _make_fetcher(self, plugin: LoadedPlugin | None = None) -> Fetcher:
        proxy_cfg = self.config.get("proxy", {})
        proxy_url = self._resolve_proxy_url(plugin)
        proxy_mode = (plugin.metadata.proxy or {}).get("mode", "auto") if plugin else "auto"
        return Fetcher(
            user_agent=self.config.get("default_user_agent", ""),
            timeout=self.config.get("source_timeout_seconds", 20.0),
            proxy_url=proxy_url,
            proxy_mode=proxy_mode,
            proxy_config=proxy_cfg,
        )

    def _make_ctx(self, plugin_id: str) -> PluginContext:
        from app.services.access_bridge.client import AccessBridgeClient

        if getattr(self, "_access_bridge", None) is None:
            self._access_bridge = AccessBridgeClient()
        plugin = self._plugins.get(plugin_id)
        proxy_url = self._resolve_proxy_url(plugin)
        proxy_mode = (plugin.metadata.proxy or {}).get("mode", "auto") if plugin else "auto"
        cookie_allowed = bool(plugin and plugin.metadata.declares_cookies)
        ctx = PluginContext(
            fetcher=self._make_fetcher_with_cookies(plugin_id),
            plugin_id=plugin_id,
            cookie_store=self._cookie_store,
            access_bridge=self._access_bridge,
            proxy_mode=proxy_mode,
            proxy_url=proxy_url,
            cookie_allowed=cookie_allowed,
        )
        if plugin and plugin.metadata.uses_search_provider("search"):
            ctx.allow_search_provider = True
        return ctx

    def _make_fetcher_with_cookies(self, plugin_id: str) -> Fetcher:
        try:
            fetcher = self._make_fetcher(self._plugins.get(plugin_id))
        except TypeError:
            fetcher = self._make_fetcher()

        # Load persisted cookies from the host-managed store into the fetcher.
        plugin = self._plugins.get(plugin_id)
        cookie_allowed = bool(plugin and plugin.metadata.declares_cookies)
        CookieJar(fetcher, plugin_id, self._cookie_store, allowed=cookie_allowed).load_into_fetcher()
        return fetcher

    def _positive_int(self, value: Any, default: int) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return default
        return parsed if parsed > 0 else default

    def _rate_limiter_for(self, plugin: LoadedPlugin) -> _PluginRateLimiter | None:
        raw_value = getattr(plugin.metadata, "rate_limit", {})
        raw = raw_value if isinstance(raw_value, dict) else {}
        concurrency = self._positive_int(raw.get("perHostConcurrency"), 0)
        try:
            min_interval_ms = max(0, int(raw.get("minIntervalMs", 0) or 0))
        except (TypeError, ValueError):
            min_interval_ms = 0
        if not concurrency:
            return None
        limiters = getattr(self, "_plugin_rate_limiters", None)
        if limiters is None:
            limiters = self._plugin_rate_limiters = {}
        plugin_id = str(getattr(plugin.metadata, "id", "") or "")
        if not plugin_id:
            return None
        limiter = limiters.get(plugin_id)
        if limiter is None:
            # ponytail: per-plugin cap is conservative for plugins with fallback domains;
            # split by request host only if lifecycle operations expose that host centrally.
            limiter = _PluginRateLimiter(
                concurrency=concurrency,
                min_interval_ms=min_interval_ms,
            )
            limiters[plugin_id] = limiter
        return limiter

    def timeout_for_plugin(self, plugin: LoadedPlugin | None = None) -> float:
        if plugin and (plugin.metadata.browser or {}).get("mode") in {"required", "optional"}:
            return float(self.config.get("browser_source_timeout_seconds", 120.0))
        return float(self.config.get("source_timeout_seconds", 20.0))

    def search_timeout_for_plugin(self, plugin: LoadedPlugin | None = None) -> float:
        if plugin and plugin.metadata.uses_search_provider("search"):
            return float(self.config.get("browser_search_timeout_seconds", 60.0))
        if plugin and (plugin.metadata.browser or {}).get("mode") in {"required", "optional"}:
            return float(self.config.get("browser_search_timeout_seconds", 60.0))
        return float(self.config.get("source_timeout_seconds", 20.0))

    def toc_timeout_for_plugin(self, plugin: LoadedPlugin | None = None) -> float:
        """Allow multi-page catalogs to use the configured hard source limit."""
        return max(
            self.timeout_for_plugin(plugin),
            float(self.config.get("source_hard_timeout_seconds", 25.0)),
        )

    async def _call_plugin(
        self,
        plugin: LoadedPlugin,
        operation: Callable[[], Awaitable[Any]],
        *,
        timeout: float | None,
    ) -> Any:
        """Queue official and browser traffic before applying the operation timeout."""
        async def run() -> Any:
            if timeout is None:
                return await operation()
            return await asyncio.wait_for(operation(), timeout=timeout)

        limiter = self._rate_limiter_for(plugin)

        async def run_limited() -> Any:
            if limiter is None:
                return await run()
            return await limiter.run(run)

        if plugin.metadata.is_official_source():
            async with self._official_source_queue:
                return await run_limited()
        browser_mode = (getattr(plugin.metadata, "browser", {}) or {}).get("mode")
        if browser_mode in {"required", "optional"}:
            async with self._browser_source_queue:
                return await run_limited()
        return await run_limited()

    async def search_one(self, plugin_id: str, keyword: str, page: int = 1) -> dict:
        """Search a single plugin and return normalized items/errors.

        This is the per-source primitive used by the host SearchCoordinator.
        """
        plugin = self._plugins.get(plugin_id)
        if not plugin or "search" not in plugin.capabilities:
            return {"items": [], "error": None, "latencyMs": 0, "proxyUsed": False}
        ctx = self._make_ctx(plugin_id)
        t0 = time.perf_counter()
        try:
            raw_items = await self._call_plugin(
                plugin,
                lambda: plugin.source.search(ctx, keyword, page),
                timeout=self.search_timeout_for_plugin(plugin),
            )
            latency_ms = int((time.perf_counter() - t0) * 1000)
            items = []
            for item in raw_items or []:
                if isinstance(item, dict):
                    item.setdefault("sourceId", plugin.metadata.id)
                    item.setdefault("sourceName", plugin.metadata.name)
                    items.append(item)
            self._trace_success(ctx, plugin.metadata.id, "search", latency_ms)
            return {"items": items, "error": None, "latencyMs": latency_ms, "proxyUsed": bool(ctx.proxy_url)}
        except asyncio.TimeoutError:
            latency_ms = int((time.perf_counter() - t0) * 1000)
            extra: dict[str, Any] = {}
            code = "BROWSER_REQUIRED" if (plugin.metadata.browser or {}).get("mode") in {"required", "optional"} else "PLUGIN_TIMEOUT"
            message = "timeout; browser bypass required" if code == "BROWSER_REQUIRED" else "timeout"
            if code == "BROWSER_REQUIRED":
                extra["bypassRequired"] = True
            err = normalize_failure(
                source_id=plugin.metadata.id,
                stage="search",
                code=code,
                message=message,
                url="",
                extra=extra,
            )
            self._trace_failure(ctx, plugin.metadata.id, "search", code, message)
            return {"items": [], "error": err, "latencyMs": latency_ms, "proxyUsed": bool(ctx.proxy_url)}
        except Exception as exc:
            latency_ms = int((time.perf_counter() - t0) * 1000)
            err = self._failure_for_exception(plugin, "search", exc)
            self._trace_failure(ctx, plugin.metadata.id, "search", err.get("code", "PLUGIN_RUNTIME_ERROR"), err.get("message", str(exc)))
            return {"items": [], "error": err, "latencyMs": latency_ms, "proxyUsed": bool(ctx.proxy_url)}
        finally:
            await ctx._fetcher.close()

    async def search(self, keyword: str, page: int = 1) -> dict:
        all_enabled = self._enabled_plugins()
        plugins = self._search_priority_plugins(all_enabled)
        skipped_unreachable = 0

        max_concurrency = self._positive_int(self.config.get("max_concurrency"), 3)
        overall_timeout = self.config.get("overall_search_timeout_seconds", 60.0)
        source_batch_size = self._positive_int(self.config.get("source_batch_size"), 20)

        if not plugins:
            return {
                "implemented": True,
                "keyword": keyword,
                "page": page,
                "items": [],
                "debug": {
                    "sourceCount": len(all_enabled),
                    "reachableCount": len(plugins),
                    "skippedUnreachable": skipped_unreachable,
                    "attemptedCount": 0,
                    "successCount": 0,
                    "errorCount": 0,
                    "disabledCount": 0,
                    "timeoutCount": 0,
                    "elapsedMs": 0,
                    "errors": [],
                    "partialSuccess": False,
                },
            }

        all_items: list[dict] = []
        errors: list[dict] = []
        start_time = time.perf_counter()
        success_count = 0
        attempted_count = 0
        timeout_count = 0

        batches = [plugins[i : i + source_batch_size] for i in range(0, len(plugins), source_batch_size)]
        semaphore = asyncio.Semaphore(max_concurrency)

        async def _search_one(plugin: LoadedPlugin) -> tuple[list[dict], dict | None]:
            if "search" not in plugin.capabilities:
                return [], None
            ctx = self._make_ctx(plugin.metadata.id)
            source_timeout = self.search_timeout_for_plugin(plugin)
            t0 = time.perf_counter()
            try:
                raw_items = await self._call_plugin(
                    plugin,
                    lambda: plugin.source.search(ctx, keyword, page),
                    timeout=source_timeout,
                )
                latency_ms = int((time.perf_counter() - t0) * 1000)
                items = []
                for item in raw_items or []:
                    if isinstance(item, dict):
                        item.setdefault("sourceId", plugin.metadata.id)
                        item.setdefault("sourceName", plugin.metadata.name)
                        items.append(item)
                self._trace_success(ctx, plugin.metadata.id, "search", latency_ms)
                return items, None
            except asyncio.TimeoutError:
                latency_ms = int((time.perf_counter() - t0) * 1000)
                extra: dict[str, Any] = {}
                code = "PLUGIN_TIMEOUT"
                message = "timeout"
                if (plugin.metadata.browser or {}).get("mode") in {"required", "optional"}:
                    code = "BROWSER_REQUIRED"
                    message = "timeout; browser bypass required"
                    extra["bypassRequired"] = True
                err = {
                    **normalize_failure(
                        source_id=plugin.metadata.id,
                        stage="search",
                        code=code,
                        message=message,
                        url="",
                        extra=extra,
                    )
                }
                self._trace_failure(ctx, plugin.metadata.id, "search", code, message)
                return [], err
            except Exception as exc:
                latency_ms = int((time.perf_counter() - t0) * 1000)
                err = self._failure_for_exception(plugin, "search", exc)
                self._trace_failure(ctx, plugin.metadata.id, "search", err.get("code", "PLUGIN_RUNTIME_ERROR"), str(exc))
                return [], err
            finally:
                await ctx._fetcher.close()

        for batch in batches:
            if (time.perf_counter() - start_time) >= overall_timeout:
                errors.append({"sourceId": "", "code": "PLUGIN_TIMEOUT", "stage": "search", "message": "overall timeout"})
                break

            attempted_count += len(batch)
            pending_plugins = list(batch)
            pending_tasks: set[asyncio.Task] = set()

            def start_next_plugins() -> None:
                while pending_plugins and len(pending_tasks) < max_concurrency:
                    pending_tasks.add(asyncio.create_task(_search_one(pending_plugins.pop(0))))

            start_next_plugins()
            try:
                results = []
                while pending_tasks:
                    remaining_timeout = max(0.1, overall_timeout - (time.perf_counter() - start_time))
                    done, pending_tasks = await asyncio.wait(
                        pending_tasks,
                        return_when=asyncio.FIRST_COMPLETED,
                        timeout=remaining_timeout,
                    )
                    if not done:
                        raise asyncio.TimeoutError
                    for task in done:
                        try:
                            results.append(task.result())
                        except Exception as e:
                            results.append(([], {"sourceId": "", "code": "PLUGIN_RUNTIME_ERROR", "stage": "search", "message": str(e)}))
                    start_next_plugins()
            except asyncio.TimeoutError:
                for task in pending_tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*pending_tasks, return_exceptions=True)
                results.append(([], {"sourceId": "", "code": "PLUGIN_TIMEOUT", "stage": "search", "message": "overall timeout"}))

            for result in results:
                if isinstance(result, Exception):
                    errors.append({"sourceId": "", "code": "PLUGIN_RUNTIME_ERROR", "stage": "search", "message": str(result)})
                    continue
                items, err = result
                if isinstance(items, Exception):
                    errors.append({"sourceId": "", "code": "PLUGIN_RUNTIME_ERROR", "stage": "search", "message": str(items)})
                    continue
                if err:
                    errors.append(err)
                    if err.get("code") == "PLUGIN_TIMEOUT":
                        timeout_count += 1
                if items:
                    success_count += 1
                    for item in items:
                        self._score_search_item(item, keyword)
                    all_items.extend(items)
                    # Yield to sibling threads/requests during CPU-heavy aggregation.
                    await asyncio.sleep(0.01)

        items = self._source_result_items(all_items)

        elapsed_ms = int((time.perf_counter() - start_time) * 1000)
        partial_success = success_count > 0 and len(errors) > 0

        total_enabled = len(all_enabled) if 'all_enabled' in locals() else len(plugins)
        skipped = skipped_unreachable if 'skipped_unreachable' in locals() else 0
        return {
            "implemented": True,
            "keyword": keyword,
            "page": page,
            "items": items,
            "debug": {
                "sourceCount": total_enabled,
                "reachableCount": len(plugins),
                "skippedUnreachable": skipped,
                "batchSize": source_batch_size,
                "batchCount": len(batches),
                "attemptedCount": attempted_count,
                "successCount": success_count,
                "errorCount": len(errors),
                "disabledCount": 0,
                "timeoutCount": timeout_count,
                "elapsedMs": elapsed_ms,
                "errors": errors,
                "partialSuccess": partial_success,
            },
        }

    async def detail(self, source_id: str, book_url: str) -> dict:
        plugin = self._plugins.get(source_id)
        if not plugin or "detail" not in plugin.capabilities:
            return {"implemented": True, "data": None, "debug": {"error": f"plugin not found or no detail capability: {source_id}"}}
        ctx = self._make_ctx(source_id)
        try:
            raw = await self._call_plugin(
                plugin,
                lambda: plugin.source.detail(ctx, book_url),
                timeout=self.timeout_for_plugin(plugin),
            )
            if isinstance(raw, dict):
                raw.setdefault("sourceId", source_id)
            else:
                raw = {"sourceId": source_id}
            return {"implemented": True, "data": raw, "debug": {}}
        except Exception as exc:
            err = self._failure_for_exception(plugin, "detail", exc)
            return {"implemented": True, "data": None, "debug": {"error": err}}
        finally:
            await ctx._fetcher.close()

    async def toc(self, source_id: str, toc_url: str) -> dict:
        plugin = self._plugins.get(source_id)
        if not plugin or "toc" not in plugin.capabilities:
            return {"implemented": True, "bookId": "", "chapters": [], "debug": {"error": f"plugin not found or no toc capability: {source_id}"}}
        ctx = self._make_ctx(source_id)
        try:
            raw_items = await self._call_plugin(
                plugin,
                lambda: plugin.source.toc(ctx, toc_url),
                timeout=self.toc_timeout_for_plugin(plugin),
            )
            chapters = []
            for item in raw_items or []:
                if isinstance(item, dict):
                    item.setdefault("sourceId", source_id)
                    chapters.append(item)
            # Rewrite chapter URLs
            from app.config import HOST, PORT
            base_api = f"http://{HOST}:{PORT}"
            for ch in chapters:
                ch_url = ch.get("rawChapterUrl") or ch.get("chapterUrl", "")
                if ch_url:
                    ch["rawChapterUrl"] = ch_url
                    ch_id = ch.get("chapterId") or encode_chapter_id(source_id, ch_url)
                    ch["chapterId"] = ch_id
                    ch["chapterUrl"] = f"{base_api}/api/legado/chapter/{ch_id}"
            return {"implemented": True, "bookId": "", "chapters": chapters, "debug": {}}
        except Exception as exc:
            err = self._failure_for_exception(plugin, "toc", exc)
            return {"implemented": True, "bookId": "", "chapters": [], "debug": {"error": err}}
        finally:
            await ctx._fetcher.close()

    async def chapter(self, source_id: str, chapter_url: str) -> dict:
        plugin = self._plugins.get(source_id)
        if not plugin or "chapter" not in plugin.capabilities:
            return {"implemented": True, "chapterId": "", "title": "", "content": "", "debug": {"error": f"plugin not found or no chapter capability: {source_id}"}}
        ctx = self._make_ctx(source_id)
        try:
            raw = await self._call_plugin(
                plugin,
                lambda: plugin.source.chapter(ctx, chapter_url),
                timeout=self.timeout_for_plugin(plugin),
            )
            if isinstance(raw, dict):
                raw.setdefault("sourceId", source_id)
                debug = raw.get("debug", {}) if isinstance(raw.get("debug", {}), dict) else {}
                return {
                    "implemented": True,
                    "chapterId": raw.get("chapterId", ""),
                    "title": raw.get("title", ""),
                    "content": raw.get("content", ""),
                    "chapterUrl": raw.get("chapterUrl", ""),
                    "rawChapterUrl": raw.get("rawChapterUrl", "") or raw.get("chapterUrl", ""),
                    "authRequired": bool(raw.get("authRequired", False)),
                    "isPaid": bool(raw.get("isPaid", False)),
                    "extra": raw.get("extra", {}) if isinstance(raw.get("extra", {}), dict) else {},
                    "debug": debug,
                }
            return {"implemented": True, "chapterId": "", "title": "", "content": "", "debug": {}}
        except Exception as exc:
            err = self._failure_for_exception(plugin, "chapter", exc)
            return {"implemented": True, "chapterId": "", "title": "", "content": "", "debug": {"error": err}}
        finally:
            await ctx._fetcher.close()

    async def chapter_reviews(self, source_id: str, chapter_url: str) -> dict:
        plugin = self._plugins.get(source_id)
        if not plugin or "chapter_reviews" not in plugin.capabilities:
            return {
                "implemented": True,
                "paragraphs": {},
                "chapterEnd": [],
                "chapterEndHot": [],
                "authorReviews": [],
                "hotParagraphReviews": [],
                "summary": {},
                "debug": {"error": f"plugin not found or no chapter_reviews capability: {source_id}"},
            }
        ctx = self._make_ctx(source_id)
        try:
            raw = await self._call_plugin(
                plugin,
                lambda: plugin.source.chapter_reviews(ctx, chapter_url),
                timeout=self.timeout_for_plugin(plugin),
            )
            if not isinstance(raw, dict):
                raw = {}
            debug = raw.get("debug", {}) if isinstance(raw.get("debug", {}), dict) else {}
            return {
                "implemented": True,
                "paragraphs": raw.get("paragraphs", {}),
                "chapterEnd": raw.get("chapterEnd", []),
                "chapterEndHot": raw.get("chapterEndHot", []),
                "authorReviews": raw.get("authorReviews", []),
                "hotParagraphReviews": raw.get("hotParagraphReviews", []),
                "summary": raw.get("summary", {}),
                "debug": debug,
            }
        except Exception as exc:
            err = self._failure_for_exception(plugin, "chapter_reviews", exc)
            return {
                "implemented": True,
                "paragraphs": {},
                "chapterEnd": [],
                "chapterEndHot": [],
                "authorReviews": [],
                "hotParagraphReviews": [],
                "summary": {},
                "debug": {"error": err},
            }
        finally:
            await ctx._fetcher.close()

    async def _review_extension(
        self,
        source_id: str,
        method_name: str,
        chapter_url: str,
        *args: Any,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Call one optional method that belongs to the chapter_reviews plugin capability."""
        plugin = self._plugins.get(source_id)
        method = getattr(plugin.source, method_name, None) if plugin else None
        if not plugin or "chapter_reviews" not in plugin.capabilities or not callable(method):
            return {
                "comments": [],
                "totalCount": 0,
                "hasMore": False,
                "debug": {"error": f"plugin has no {method_name} method: {source_id}"},
            }
        ctx = self._make_ctx(source_id)
        try:
            raw = await self._call_plugin(
                plugin,
                lambda: method(ctx, chapter_url, *args, **kwargs),
                timeout=self.timeout_for_plugin(plugin),
            )
            return raw if isinstance(raw, dict) else {}
        except Exception as exc:
            return {
                "comments": [],
                "totalCount": 0,
                "hasMore": False,
                "debug": {"error": self._failure_for_exception(plugin, method_name, exc)},
            }
        finally:
            await ctx._fetcher.close()

    @staticmethod
    def _paged_review_result(raw: dict[str, Any], **extra: Any) -> dict[str, Any]:
        comments = raw.get("comments", []) if isinstance(raw.get("comments"), list) else []
        return {
            "implemented": True,
            **extra,
            "comments": comments,
            "hotComments": raw.get("hotComments", []),
            "normalComments": raw.get("normalComments", []),
            "totalCount": raw.get("totalCount", len(comments)),
            "page": raw.get("page", 1),
            "pageSize": raw.get("pageSize", 20),
            "hasMore": bool(raw.get("hasMore", False)),
            "nextPage": raw.get("nextPage"),
            "debug": raw.get("debug", {}) if isinstance(raw.get("debug"), dict) else {},
        }

    async def page_hot_reviews(
        self,
        source_id: str,
        chapter_url: str,
        paragraph_ids: list[int],
        *,
        page: int = 1,
        page_size: int = 20,
    ) -> dict:
        raw = await self._review_extension(
            source_id,
            "page_hot_reviews",
            chapter_url,
            paragraph_ids,
            page=page,
            page_size=page_size,
        )
        return self._paged_review_result(raw, paragraphIds=paragraph_ids)

    async def chapter_say(
        self,
        source_id: str,
        chapter_url: str,
        *,
        page: int = 1,
        page_size: int = 20,
    ) -> dict:
        raw = await self._review_extension(
            source_id,
            "chapter_say",
            chapter_url,
            page=page,
            page_size=page_size,
        )
        return self._paged_review_result(raw)

    async def paragraph_reviews(
        self,
        source_id: str,
        chapter_url: str,
        paragraph_id: int,
        *,
        page: int = 1,
        page_size: int = 20,
    ) -> dict:
        raw = await self._review_extension(
            source_id,
            "paragraph_say",
            chapter_url,
            paragraph_id,
            page=page,
            page_size=page_size,
        )
        return self._paged_review_result(raw, paragraphId=paragraph_id)

    async def review_replies(
        self,
        source_id: str,
        chapter_url: str,
        root_review_id: int,
        *,
        page: int = 1,
        page_size: int = 20,
        cursor_id: int = 0,
    ) -> dict:
        raw = await self._review_extension(
            source_id,
            "review_replies",
            chapter_url,
            root_review_id,
            page=page,
            page_size=page_size,
            cursor_id=cursor_id,
        )
        result = self._paged_review_result(raw, rootReviewId=str(root_review_id))
        result.update({
            "rootReview": raw.get("rootReview"),
            "replies": raw.get("replies", []),
            "nextCursorId": raw.get("nextCursorId"),
        })
        return result

    async def explore_groups(self, source_id: str | None = None) -> dict:
        unsupported_reason = ""
        plugins = self._official_explore_plugins()
        if source_id:
            plugin = self._plugins.get(source_id)
            if plugin and plugin.metadata.enabled and "explore" in plugin.capabilities and plugin.metadata.is_official_source():
                plugins = [plugin]
            else:
                plugins = []
                if plugin and "explore" in plugin.capabilities and not plugin.metadata.is_official_source():
                    unsupported_reason = "普通书源不提供排行榜/分类，聚合源排行榜后续仅使用正版书源。"
        groups: list[dict] = []
        errors: list[dict] = []
        start_time = time.perf_counter()
        for plugin in plugins:
            if not plugin or "explore" not in plugin.capabilities:
                continue
            ctx = self._make_ctx(plugin.metadata.id)
            timeout = self.timeout_for_plugin(plugin)
            try:
                raw_groups = await self._call_plugin(
                    plugin,
                    lambda: plugin.source.explore_groups(ctx),
                    timeout=timeout,
                )
                for group in raw_groups or []:
                    if not isinstance(group, dict):
                        continue
                    group.setdefault("sourceId", plugin.metadata.id)
                    group.setdefault("sourceName", plugin.metadata.name)
                    group.setdefault("kind", "other")
                    group.setdefault("pageable", True)
                    groups.append(group)
            except asyncio.TimeoutError:
                errors.append(normalize_failure(source_id=plugin.metadata.id, stage="explore_groups", code="PLUGIN_TIMEOUT", message="timeout"))
            except Exception as exc:
                errors.append(self._failure_for_exception(plugin, "explore_groups", exc))
            finally:
                await ctx._fetcher.close()
        return {
            "implemented": True,
            "sourceId": source_id or "",
            "groups": groups,
            "debug": {
                "sourceCount": len(plugins),
                "groupCount": len(groups),
                "errorCount": len(errors),
                "elapsedMs": int((time.perf_counter() - start_time) * 1000),
                "errors": errors,
                "unsupportedReason": unsupported_reason,
            },
        }

    async def explore(self, source_id: str, group_id: str | None = None, page: int = 1) -> dict:
        plugin = self._plugins.get(source_id)
        if plugin and "explore" in plugin.capabilities and not plugin.metadata.is_official_source():
            return {
                "implemented": True,
                "sourceId": source_id,
                "groupId": group_id or "",
                "page": page,
                "items": [],
                "debug": {
                    "error": {
                        "sourceId": source_id,
                        "stage": "explore",
                        "code": "EXPLORE_OFFICIAL_SOURCE_REQUIRED",
                        "message": "普通书源不提供排行榜/分类，聚合源排行榜后续仅使用正版书源。",
                    },
                    "errors": [],
                },
            }
        if not plugin or "explore" not in plugin.capabilities:
            return {
                "implemented": True,
                "sourceId": source_id,
                "groupId": group_id or "",
                "page": page,
                "items": [],
                "debug": {"error": f"plugin not found or no explore capability: {source_id}"},
            }
        ctx = self._make_ctx(source_id)
        start_time = time.perf_counter()
        try:
            raw_items = await self._call_plugin(
                plugin,
                lambda: plugin.source.explore(ctx, group_id, page),
                timeout=self.timeout_for_plugin(plugin),
            )
            items = []
            for index, item in enumerate(raw_items or [], start=1):
                if not isinstance(item, dict):
                    continue
                item.setdefault("sourceId", source_id)
                item.setdefault("sourceName", plugin.metadata.name)
                item.setdefault("groupId", group_id or "")
                item.setdefault("rank", index)
                items.append(item)
            return {
                "implemented": True,
                "sourceId": source_id,
                "groupId": group_id or "",
                "page": page,
                "items": items,
                "debug": {"elapsedMs": int((time.perf_counter() - start_time) * 1000), "errorCount": 0, "errors": []},
            }
        except asyncio.TimeoutError:
            err = normalize_failure(source_id=source_id, stage="explore", code="PLUGIN_TIMEOUT", message="timeout")
            return {"implemented": True, "sourceId": source_id, "groupId": group_id or "", "page": page, "items": [], "debug": {"error": err, "errors": [err]}}
        except Exception as exc:
            err = self._failure_for_exception(plugin, "explore", exc)
            return {"implemented": True, "sourceId": source_id, "groupId": group_id or "", "page": page, "items": [], "debug": {"error": err, "errors": [err]}}
        finally:
            await ctx._fetcher.close()

    def _score_search_item(self, item: dict, keyword: str) -> dict:
        score = 0
        name = item.get("name", "")
        kw = keyword.lower()
        name_lower = name.lower()
        # Title match
        if kw == name_lower:
            score += 200
        elif kw in name_lower:
            score += 100
        # Field completeness bonus
        if item.get("author"):
            score += 10
        if item.get("lastChapter"):
            score += 5
        if item.get("intro"):
            score += 3
        if item.get("coverUrl"):
            score += 3
        if item.get("kind"):
            score += 2
        if item.get("wordCount"):
            score += 2
        if item.get("updateTime"):
            score += 1
        item["score"] = score
        return item

    def _source_result_items(self, items: list[dict]) -> list[dict]:
        from app.config import HOST, PORT

        base_api = f"http://{HOST}:{PORT}"
        source_items = [dict(item) for item in items if isinstance(item, dict)]
        source_items.sort(
            key=lambda item: (
                -item.get("score", 0),
                item.get("name", ""),
                item.get("sourceName", "") or item.get("sourceId", ""),
            )
        )
        for item in source_items:
            source_id = item.get("sourceId", "")
            raw_book_url = item.get("rawBookUrl") or item.get("bookUrl", "")
            if raw_book_url and "/api/legado/book/" not in raw_book_url:
                book_id = encode_book_id(source_id, raw_book_url)
                item["bookId"] = book_id
                item["rawBookUrl"] = raw_book_url
                item["bookUrl"] = f"{base_api}/api/legado/book/{book_id}"
        return source_items

    def _trace_success(self, ctx: PluginContext, plugin_id: str, stage: str, latency_ms: int) -> None:
        ctx.trace(stage, message=f"success {latency_ms}ms")

    def _trace_failure(self, ctx: PluginContext, plugin_id: str, stage: str, code: str, message: str) -> None:
        ctx.trace(stage, message=f"failure {code}: {message}")

    def _failure_for_exception(self, plugin: LoadedPlugin, stage: str, exc: Exception) -> dict:
        code = getattr(exc, "code", "PLUGIN_RUNTIME_ERROR")
        url = getattr(exc, "url", "") or ""
        extra: dict[str, Any] = {}
        if code in {"CLOUDFLARE_REQUIRED", "BROWSER_REQUIRED"}:
            extra["bypassRequired"] = True
            extra["bypassStrategy"] = "skip_source_until_bypass_available"
        if getattr(exc, "status_code", None):
            extra["statusCode"] = getattr(exc, "status_code")
        return normalize_failure(
            source_id=plugin.metadata.id,
            stage=stage,
            code=code,
            message=str(exc),
            url=url,
            extra=extra,
        )


_scheduler_instance: PluginScheduler | None = None
_scheduler_lock = threading.Lock()


def get_plugin_scheduler(config: dict | None = None, reload: bool = False) -> "PluginScheduler":
    """Return the process-wide PluginScheduler singleton.

    Creating a PluginScheduler scans the plugins directory and imports every
    source plugin, which is expensive. Sharing one instance across requests
    removes that per-request overhead and keeps the enabled plugin pool in
    memory. Call with ``reload=True`` after plugin files have changed.
    """
    global _scheduler_instance
    with _scheduler_lock:
        if reload or _scheduler_instance is None:
            _scheduler_instance = PluginScheduler(config=config)
        elif config is not None:
            _scheduler_instance.config = config
        return _scheduler_instance


async def shutdown_plugin_scheduler() -> None:
    """Release runtime-owned plugin resources during application shutdown."""
    with _scheduler_lock:
        scheduler = _scheduler_instance
    if scheduler is not None:
        await scheduler.close()
