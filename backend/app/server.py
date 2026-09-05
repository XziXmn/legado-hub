"""Run isolated public and management entrypoints in one Uvicorn process."""

from __future__ import annotations

import argparse
import socket

import uvicorn
from fastapi.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from app import config
from app.core.public_security import (
    load_admin_security_config,
    load_public_security_config,
)
from app.main import EntryPoint, create_app


def _uvicorn_log_config() -> dict:
    """Send uvicorn access logs to stderr like every other log source.

    Synology Container Manager / Portainer web UIs fail to render json-file
    logs whose entries mix stdout and stderr stream types; keeping a single
    stream makes the container logs visible there. ``docker logs`` is
    unaffected either way.
    """
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "()": "uvicorn.logging.DefaultFormatter",
                "fmt": "%(levelprefix)s %(message)s",
                "use_colors": None,
            },
            "access": {
                "()": "uvicorn.logging.AccessFormatter",
                "fmt": '%(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s',
            },
        },
        "handlers": {
            "default": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "stream": "ext://sys.stderr",
            },
            "access": {
                "class": "logging.StreamHandler",
                "formatter": "access",
                "stream": "ext://sys.stderr",
            },
        },
        "loggers": {
            "uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False},
            "uvicorn.error": {"level": "INFO"},
            "uvicorn.access": {"handlers": ["access"], "level": "INFO", "propagate": False},
        },
    }


class PortDispatchApp:
    """Dispatch HTTP and WebSocket scopes by the local listening port."""

    def __init__(
        self,
        *,
        public_port: int,
        admin_port: int,
        public_app: ASGIApp,
        admin_app: ASGIApp,
    ) -> None:
        if public_port == admin_port:
            raise ValueError("Public and admin ports must be different.")
        self.public_port = public_port
        self.admin_port = admin_port
        self.public_app = public_app
        self.admin_app = admin_app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            # Only the public app owns database initialization and background workers.
            await self.public_app(scope, receive, send)
            return

        server = scope.get("server")
        local_port = server[1] if server and len(server) > 1 else None
        if local_port == self.public_port:
            await self.public_app(scope, receive, send)
            return
        if local_port == self.admin_port:
            await self.admin_app(scope, receive, send)
            return

        if scope["type"] == "websocket":
            await send({"type": "websocket.close", "code": 1008})
            return
        response = JSONResponse(status_code=404, content={"detail": "Unknown entrypoint"})
        await response(scope, receive, send)


def build_dispatch_app(*, public_port: int, admin_port: int) -> PortDispatchApp:
    """Build both route surfaces while assigning runtime ownership once."""
    public_app = create_app(
        load_public_security_config(),
        entrypoint=EntryPoint.PUBLIC,
        manage_runtime=True,
    )
    admin_app = create_app(
        load_admin_security_config(),
        entrypoint=EntryPoint.ADMIN,
        manage_runtime=False,
    )
    return PortDispatchApp(
        public_port=public_port,
        admin_port=admin_port,
        public_app=public_app,
        admin_app=admin_app,
    )


def _port(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("port must be an integer") from exc
    if not 1 <= parsed <= 65535:
        raise argparse.ArgumentTypeError("port must be between 1 and 65535")
    return parsed


def run(*, host: str, public_port: int, admin_port: int) -> None:
    """Bind both sockets and serve them with one ASGI lifespan."""
    application = build_dispatch_app(public_port=public_port, admin_port=admin_port)
    log_config = _uvicorn_log_config()
    public_config = uvicorn.Config(
        application,
        host=host,
        port=public_port,
        proxy_headers=False,
        server_header=False,
        log_config=log_config,
    )
    admin_config = uvicorn.Config(
        application,
        host=host,
        port=admin_port,
        proxy_headers=False,
        server_header=False,
        log_config=log_config,
    )
    sockets: list[socket.socket] = []
    try:
        sockets.append(public_config.bind_socket())
        sockets.append(admin_config.bind_socket())
        uvicorn.Server(public_config).run(sockets=sockets)
    finally:
        for listener in sockets:
            listener.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run LegadoHub public and admin entrypoints.")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--public-port", type=_port, default=config.PORT)
    parser.add_argument("--admin-port", type=_port, default=config.ADMIN_PORT)
    args = parser.parse_args()
    if args.public_port == args.admin_port:
        parser.error("--public-port and --admin-port must be different")
    run(host=args.host, public_port=args.public_port, admin_port=args.admin_port)


if __name__ == "__main__":
    main()
