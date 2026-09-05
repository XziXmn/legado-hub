"""FastAPI app factory and route registration."""

import asyncio
import logging
from contextlib import asynccontextmanager
from enum import StrEnum

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi import Request
from fastapi.responses import FileResponse
from fastapi.responses import JSONResponse, RedirectResponse

from app import config
from app.api import health, legado, console, auth, subscribe
from app.services.shared_book_scheduler import SharedBookScheduler
from app.services.source_ping_scheduler import SourcePingScheduler
from app.storage.db import initialize_database
from app.services.reading_limits import ReadingLimitError
from app.core.public_security import (
    PublicSecurityConfig,
    install_public_security,
    load_public_security_config,
    prepare_runtime_permissions,
)

FRONTEND_DIST = config.FRONTEND_DIST_DIR
logger = logging.getLogger(__name__)


def _log_generated_admin_password(password: str) -> None:
    """Deliver the local first-start credential once through the process console."""
    logger.warning(
        "首次启动已创建管理员账号。用户名：admin；随机密码：%s。"
        "请立即登录并修改密码，此密码后续不会再次显示。",
        password,
    )


class EntryPoint(StrEnum):
    """Network surface exposed by one FastAPI application instance."""

    PUBLIC = "public"
    ADMIN = "admin"
    COMBINED = "combined"


async def _update_lexicon_on_startup() -> None:
    try:
        from app.services.lexicon_updater import LexiconUpdater

        await asyncio.to_thread(LexiconUpdater().check_and_update)
    except Exception:
        # Startup should never fail because the optional upstream lexicon is unavailable.
        logger.warning("Failed to update the optional lexicon on startup", exc_info=True)


async def _probe_saved_plugin_cookies(official_auth_manager) -> None:
    try:
        from app.services.cookie_store import CookieStore

        plugin_ids = CookieStore().list_plugin_ids()
    except Exception:
        logger.warning("Failed to enumerate saved plugin cookies", exc_info=True)
        return
    for plugin_id in plugin_ids:
        try:
            await official_auth_manager.probe_saved_cookie_file(plugin_id)
        except Exception:
            logger.warning("Failed to probe saved cookies for plugin %s", plugin_id, exc_info=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    prepare_runtime_permissions()
    initialize_database()
    from app.services.official_auth.manager import official_auth_manager
    from app.services.user_auth import auth_service

    if auth_service.ensure_default_admin(
        on_generated_password=_log_generated_admin_password,
    ):
        logger.info("LegadoHub administrator initialized")

    # Clean up jobs that were left running from a previous server process.  Their
    # workers/tasks are gone, so keeping them as "running" would make new requests
    # wait forever on an orphan job.
    try:
        from app.services.search_jobs import SearchJobService

        _job_service = SearchJobService()
        for _job in _job_service.list_jobs(limit=500):
            if _job["status"] in {"pending", "running"}:
                _job_service.cancel_job(_job["jobId"])
    except Exception:
        logger.warning("Failed to clean up orphan search jobs", exc_info=True)

    try:
        from app.services.subscription_search import subscription_search_service

        subscription_search_service.recover_interrupted_jobs()
    except Exception:
        logger.warning("Failed to recover interrupted subscription searches", exc_info=True)

    # Migrate legacy plugin-directory Cookie.json files to the host store once.
    try:
        from app.services.cookie_store import migrate_legacy_plugin_cookies

        migrate_legacy_plugin_cookies()
    except Exception:
        logger.warning("Failed to migrate legacy plugin cookies", exc_info=True)

    # On startup, probe any plugin that has a saved cookie in the host store.
    await _probe_saved_plugin_cookies(official_auth_manager)

    stop_event = asyncio.Event()
    shared_book_scheduler = SharedBookScheduler()
    aggregate_task = asyncio.create_task(shared_book_scheduler.run_forever(stop_event))
    ping_task = asyncio.create_task(SourcePingScheduler().run_forever(stop_event))
    lexicon_task = asyncio.create_task(_update_lexicon_on_startup())
    try:
        yield
    finally:
        stop_event.set()
        aggregate_task.cancel()
        ping_task.cancel()
        lexicon_task.cancel()
        try:
            await aggregate_task
        except asyncio.CancelledError:
            pass
        try:
            await ping_task
        except asyncio.CancelledError:
            pass
        try:
            await lexicon_task
        except asyncio.CancelledError:
            pass
        try:
            from app.source_plugins.scheduler import shutdown_plugin_scheduler

            await shutdown_plugin_scheduler()
        except Exception:
            logger.warning("Failed to close source access bridge", exc_info=True)


@asynccontextmanager
async def passive_lifespan(_app: FastAPI):
    """Keep secondary listeners from starting duplicate background workers."""
    yield


def create_app(
    security_config: PublicSecurityConfig | None = None,
    *,
    entrypoint: EntryPoint | str = EntryPoint.COMBINED,
    manage_runtime: bool = True,
) -> FastAPI:
    entrypoint = EntryPoint(entrypoint)
    security = security_config or load_public_security_config()
    public_entrypoint = entrypoint is EntryPoint.PUBLIC
    app = FastAPI(
        title=config.APP_NAME,
        version=config.APP_VERSION,
        lifespan=lifespan if manage_runtime else passive_lifespan,
        docs_url=None if public_entrypoint else "/docs",
        redoc_url=None if public_entrypoint else "/redoc",
        openapi_url=None if public_entrypoint else "/openapi.json",
    )
    app.state.entrypoint = entrypoint.value
    install_public_security(app, security)

    @app.exception_handler(ReadingLimitError)
    async def reading_limit_error_handler(_request, exc: ReadingLimitError):
        return JSONResponse(
            status_code=429,
            headers={"Retry-After": str(exc.retry_after_seconds)},
            content={
                "detail": {
                    "code": exc.code,
                    "message": str(exc),
                    "retryable": True,
                    "retryAfterSeconds": exc.retry_after_seconds,
                }
            },
        )
    if entrypoint is EntryPoint.PUBLIC:
        app.include_router(health.public_router)
        app.include_router(auth.public_router)
        app.include_router(subscribe.public_router)
        app.include_router(legado.router)
    elif entrypoint is EntryPoint.ADMIN:
        app.include_router(health.router)
        app.include_router(auth.admin_router)
        app.include_router(subscribe.router)
        app.include_router(legado.router)
        app.include_router(console.console_router)

        # Compat: old book sources baked admin port (8766) into LEGADOHUB_BASE.
        # Access redeem/enter only exist on the reader listener — bounce GET enter.
        @app.get("/api/auth/access/enter")
        async def admin_access_enter_redirect(request: Request):
            host = (request.headers.get("host") or "127.0.0.1").split(":")[0]
            scheme = "https" if request.url.scheme == "https" else "http"
            # Prefer forwarded proto when behind TLS terminator.
            xf_proto = (request.headers.get("x-forwarded-proto") or "").split(",")[0].strip()
            if xf_proto in {"http", "https"}:
                scheme = xf_proto
            qs = request.url.query
            from app.core.public_security import ensure_reader_entrypoint_origin

            reader_base = ensure_reader_entrypoint_origin(
                f"{scheme}://{host}:{config.ADMIN_PORT}", request=request
            )
            target = f"{reader_base}/api/auth/access/enter"
            if qs:
                target = f"{target}?{qs}"
            return RedirectResponse(url=target, status_code=307)
    else:
        app.include_router(health.router)
        app.include_router(legado.router)
        app.include_router(auth.router)
        app.include_router(subscribe.router)
        app.include_router(console.console_router)

    # Serve React console frontend.
    if FRONTEND_DIST.exists():
        app.mount("/console-static", StaticFiles(directory=str(FRONTEND_DIST), html=True), name="console-static")
        assets_dir = FRONTEND_DIST / "assets"
        if assets_dir.exists():
            app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="console-assets")

        @app.get("/")
        async def root_spa():
            return FileResponse(str(FRONTEND_DIST / "index.html"))

        @app.get("/console")
        async def console_spa():
            return FileResponse(str(FRONTEND_DIST / "index.html"))

        @app.get("/login")
        async def console_login_spa():
            return FileResponse(str(FRONTEND_DIST / "index.html"))

        if entrypoint is EntryPoint.PUBLIC:
            @app.get("/console/subscription")
            async def console_subscription_spa():
                return FileResponse(str(FRONTEND_DIST / "index.html"))

            @app.get("/console/library")
            async def console_library_spa():
                return FileResponse(str(FRONTEND_DIST / "index.html"))

            @app.get("/console/library/{book_id}")
            async def console_library_book_spa(book_id: str):
                return FileResponse(str(FRONTEND_DIST / "index.html"))
        else:
            @app.get("/console/{path:path}")
            async def console_spa_catchall(path: str):
                return FileResponse(str(FRONTEND_DIST / "index.html"))

        @app.get("/favicon.svg")
        async def console_favicon():
            return FileResponse(str(FRONTEND_DIST / "favicon.svg"))

        @app.get("/icons.svg")
        async def console_icons():
            return FileResponse(str(FRONTEND_DIST / "icons.svg"))

    return app


app = create_app()
