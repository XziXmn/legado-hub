"""Minimal user auth and admin helpers for subscription/library pages.

Copyright (c) 2026 moo. All rights reserved.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import math
import os
import secrets
import sqlite3
import threading
import time
import uuid
from collections import OrderedDict, deque
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from fastapi import HTTPException, Request, Response, status

from app.config import DB_PATH, read_secret
from app.core.app_config import AppConfig
from app.services.audit import audit_service
from app.storage.db import initialize_database

SESSION_COOKIE_NAME = "legadohub_session"
READING_LOGIN_REQUIRED_MESSAGE = "当前未登陆，请登陆后使用。"
SESSION_TTL_DAYS = 30
PBKDF2_ITERATIONS = 240_000
USERNAME_MAX_LENGTH = 64
PASSWORD_MIN_LENGTH = 8
PASSWORD_MAX_LENGTH = 128
ALLOWED_ROLES = {"admin", "user"}
ACCESS_CODE_PREFIX = "LH1"
ACCESS_CODE_MAX_LENGTH = 256
MAX_SESSIONS_PER_USER = 3
AUTH_FAILURE_LIMIT = 5
AUTH_FAILURE_WINDOW_SECONDS = 10 * 60
AUTH_RATE_LIMIT_MAX_KEYS = 4096
ADMIN_PASSWORD_CONFIG_MIGRATION_KEY = "auth_admin_password_config_migrated"
GENERATED_ADMIN_PASSWORD_BYTES = 32
_DUMMY_PASSWORD_SALT = b"legadohub-auth!"


@dataclass
class AuthUser:
    user_id: str
    username: str
    role: str
    disabled: bool

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"


class AuthRateLimitError(RuntimeError):
    def __init__(self, retry_after_seconds: int):
        super().__init__("认证尝试过于频繁")
        self.retry_after_seconds = max(1, int(retry_after_seconds))


class AuthRateLimiter:
    """Small single-process failure limiter; the public edge provides the durable IP gate."""

    def __init__(
        self,
        *,
        limit: int = AUTH_FAILURE_LIMIT,
        window_seconds: int = AUTH_FAILURE_WINDOW_SECONDS,
        max_keys: int = AUTH_RATE_LIMIT_MAX_KEYS,
        clock=time.monotonic,
    ):
        self.limit = max(1, int(limit))
        self.window_seconds = max(1, int(window_seconds))
        self.max_keys = max(2, int(max_keys))
        self.clock = clock
        self._events: OrderedDict[str, deque[float]] = OrderedDict()
        self._lock = threading.Lock()

    def _active_events(self, key: str, now: float) -> deque[float] | None:
        events = self._events.get(key)
        if events is None:
            return None
        cutoff = now - self.window_seconds
        while events and events[0] <= cutoff:
            events.popleft()
        if not events:
            self._events.pop(key, None)
            return None
        self._events.move_to_end(key)
        return events

    def check(self, *keys: str) -> None:
        now = self.clock()
        with self._lock:
            for key in dict.fromkeys(str(value) for value in keys):
                events = self._active_events(key, now)
                if events is None:
                    continue
                if len(events) >= self.limit:
                    retry_after = self.window_seconds - (now - events[0])
                    raise AuthRateLimitError(math.ceil(retry_after))

    def record_failure(self, *keys: str) -> None:
        now = self.clock()
        with self._lock:
            for key in dict.fromkeys(str(value) for value in keys):
                events = self._active_events(key, now)
                if events is None:
                    events = deque()
                    self._events[key] = events
                events.append(now)
                while len(events) > self.limit:
                    events.popleft()
                self._events.move_to_end(key)
            while len(self._events) > self.max_keys:
                self._events.popitem(last=False)

    def reset(self) -> None:
        with self._lock:
            self._events.clear()


class UserAuthService:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self._bootstrap_lock = threading.Lock()

    @contextmanager
    def _conn(self):
        initialize_database(self.db_path)
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
        finally:
            conn.close()

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _future(self, days: int = SESSION_TTL_DAYS) -> str:
        return (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()

    def user_count(self) -> int:
        with self._conn() as conn:
            row = conn.execute("SELECT COUNT(*) FROM users").fetchone()
        return int(row[0] or 0)

    def enabled_admin_count(self) -> int:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM users WHERE role = 'admin' AND disabled = 0"
            ).fetchone()
        return int(row[0] or 0)

    def list_users(self) -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT user_id, username, role, disabled, created_at, updated_at
                FROM users
                ORDER BY created_at ASC, username ASC
                """
            ).fetchall()
        return [
            {
                "userId": row[0],
                "username": row[1],
                "role": row[2],
                "disabled": bool(row[3]),
                "createdAt": row[4],
                "updatedAt": row[5],
            }
            for row in rows
        ]

    def _validate_username(self, username: str) -> str:
        username = str(username or "").strip()
        if not username:
            raise HTTPException(status_code=400, detail="用户名不能为空")
        if len(username) > USERNAME_MAX_LENGTH:
            raise HTTPException(status_code=400, detail=f"用户名不能超过 {USERNAME_MAX_LENGTH} 个字符")
        if any(character.isspace() or ord(character) < 32 for character in username):
            raise HTTPException(status_code=400, detail="用户名不能包含空白或控制字符")
        return username

    def _validate_password(self, password: str) -> str:
        password = str(password or "")
        if len(password) < PASSWORD_MIN_LENGTH:
            raise HTTPException(status_code=400, detail=f"密码至少需要 {PASSWORD_MIN_LENGTH} 个字符")
        if len(password) > PASSWORD_MAX_LENGTH:
            raise HTTPException(status_code=400, detail=f"密码不能超过 {PASSWORD_MAX_LENGTH} 个字符")
        if any(ord(character) < 32 for character in password):
            raise HTTPException(status_code=400, detail="密码不能包含控制字符")
        return password

    def create_user(
        self,
        username: str,
        password: str,
        role: str = "user",
        *,
        require_empty: bool = False,
        actor_user_id: str = "",
        actor_role: str = "",
    ) -> dict[str, Any]:
        username = self._validate_username(username)
        password = self._validate_password(password)
        role = str(role or "").strip()
        if role not in ALLOWED_ROLES:
            raise HTTPException(status_code=400, detail="角色必须是 admin 或 user")
        if require_empty and self.user_count() > 0:
            raise HTTPException(status_code=409, detail="系统已存在用户，不能再次初始化管理员")
        user_id = uuid.uuid4().hex
        now = self._now()
        password_hash = self.hash_password(password)
        try:
            with self._conn() as conn:
                conn.execute("BEGIN IMMEDIATE")
                if require_empty:
                    existing = conn.execute("SELECT COUNT(*) FROM users").fetchone()
                    if int(existing[0] or 0) > 0:
                        raise HTTPException(status_code=409, detail="系统已存在用户，不能再次初始化管理员")
                conn.execute(
                    """
                    INSERT INTO users (user_id, username, password_hash, role, disabled, created_at, updated_at)
                    VALUES (?, ?, ?, ?, 0, ?, ?)
                    """,
                    (user_id, username, password_hash, role, now, now),
                )
                if actor_user_id:
                    audit_service.record(
                        action="user.create",
                        actor_user_id=actor_user_id,
                        actor_role=actor_role,
                        target_type="user",
                        target_id=user_id,
                        summary={"role": role, "disabled": False},
                        conn=conn,
                    )
                conn.commit()
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=409, detail="用户名已存在")
        return {"userId": user_id, "username": username, "role": role, "disabled": False}

    @staticmethod
    def build_access_code(username: str, secret: str) -> str:
        encoded_username = base64.urlsafe_b64encode(username.encode("utf-8")).decode("ascii").rstrip("=")
        return f"{ACCESS_CODE_PREFIX}.{encoded_username}.{secret}"

    @staticmethod
    def _parse_access_code(access_code: str) -> tuple[str, str] | None:
        value = str(access_code or "").strip()
        if not value or len(value) > ACCESS_CODE_MAX_LENGTH:
            return None
        parts = value.split(".")
        if len(parts) != 3 or parts[0] != ACCESS_CODE_PREFIX or not parts[1] or not parts[2]:
            return None
        try:
            padding = "=" * (-len(parts[1]) % 4)
            username_bytes = base64.b64decode(
                (parts[1] + padding).encode("ascii"),
                altchars=b"-_",
                validate=True,
            )
            username = username_bytes.decode("utf-8")
        except (UnicodeError, ValueError):
            return None
        if len(parts[2]) < PASSWORD_MIN_LENGTH or len(parts[2]) > PASSWORD_MAX_LENGTH:
            return None
        return username, parts[2]

    @classmethod
    def access_code_identifier(cls, access_code: str) -> str:
        parsed = cls._parse_access_code(access_code)
        return parsed[0] if parsed else "invalid"

    @staticmethod
    def build_access_subscription_links(
        access_code: str,
        *,
        public_base: str = "",
        lan_base: str = "",
    ) -> dict[str, str]:
        """Build personal source/subscription URLs that embed the access code.

        Source URLs bake the code into book-source JSON (Reading auto-login).
        Subscription URLs hit ``/api/auth/access/enter`` so the browser gets a
        session cookie without re-typing the code on the SPA login form.
        """
        from urllib.parse import quote

        code = str(access_code or "").strip()
        if not code:
            return {}
        q = quote(code, safe="")
        next_q = quote("/console/subscription", safe="")
        links: dict[str, str] = {}
        public = str(public_base or "").strip().rstrip("/")
        lan = str(lan_base or "").strip().rstrip("/")

        def _pair(base: str) -> tuple[str, str]:
            source = f"{base}/api/subscribe/legado/source?code={q}"
            subscription = f"{base}/api/auth/access/enter?code={q}&next={next_q}"
            return source, subscription

        def _max_source(base: str) -> str:
            return f"{base}/api/subscribe/legado/source?code={q}&reader=max"

        if public:
            source, subscription = _pair(public)
            links["publicSourceUrl"] = source
            links["publicSubscriptionUrl"] = subscription
            links["publicMaxSourceUrl"] = _max_source(public)
        if lan and lan != public:
            source, subscription = _pair(lan)
            links["lanSourceUrl"] = source
            links["lanSubscriptionUrl"] = subscription
            links["lanMaxSourceUrl"] = _max_source(lan)
        # Preferred single-copy targets for the admin UI.
        if public:
            links["sourceUrl"] = links["publicSourceUrl"]
            links["subscriptionUrl"] = links["publicSubscriptionUrl"]
            links["maxSourceUrl"] = links["publicMaxSourceUrl"]
        elif lan:
            links["sourceUrl"] = links["lanSourceUrl"]
            links["subscriptionUrl"] = links["lanSubscriptionUrl"]
            links["maxSourceUrl"] = links["lanMaxSourceUrl"]
        return links

    def create_access_user(
        self,
        username: str,
        *,
        actor_user_id: str = "",
        actor_role: str = "",
    ) -> dict[str, Any]:
        secret = secrets.token_urlsafe(32)
        result = self.create_user(
            username=username,
            password=secret,
            role="user",
            actor_user_id=actor_user_id,
            actor_role=actor_role,
        )
        self._store_access_code_secret(result["userId"], secret)
        if actor_user_id:
            with self._conn() as conn:
                audit_service.record(
                    action="user.access_code.issue",
                    actor_user_id=actor_user_id,
                    actor_role=actor_role,
                    target_type="user",
                    target_id=result["userId"],
                    conn=conn,
                )
                conn.commit()
        return {
            **result,
            "accessCode": self.build_access_code(result["username"], secret),
        }

    def _invalid_access_code(self) -> HTTPException:
        return HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "invalid_access_code",
                "message": "授权码无效",
                "retryable": False,
            },
        )

    def _burn_dummy_password_check(self, secret: str) -> None:
        hashlib.pbkdf2_hmac(
            "sha256",
            str(secret or "").encode("utf-8"),
            _DUMMY_PASSWORD_SALT,
            PBKDF2_ITERATIONS,
        )

    def authenticate_access_code(self, access_code: str) -> AuthUser:
        parsed = self._parse_access_code(access_code)
        if not parsed:
            self._burn_dummy_password_check("")
            raise self._invalid_access_code()
        username, secret = parsed
        row = self.get_user_by_username(username)
        if not row:
            self._burn_dummy_password_check(secret)
            raise self._invalid_access_code()
        password_valid = self.verify_password(secret, row["passwordHash"])
        if row["role"] != "user" or row["disabled"] or not password_valid:
            raise self._invalid_access_code()
        user = self.get_user(row["userId"])
        if not user:
            raise self._invalid_access_code()
        return user

    def reset_access_code(
        self,
        user_id: str,
        *,
        actor_user_id: str = "",
        actor_role: str = "",
    ) -> dict[str, Any]:
        user = self.get_user(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="用户不存在")
        if user.is_admin:
            raise HTTPException(status_code=400, detail="管理员必须重置密码")
        secret = secrets.token_urlsafe(32)
        self.reset_password(
            user_id,
            secret,
            actor_user_id=actor_user_id,
            actor_role=actor_role,
            audit_action="user.access_code.reset",
        )
        self._store_access_code_secret(user_id, secret)
        return {
            "userId": user_id,
            "accessCode": self.build_access_code(user.username, secret),
        }

    def _store_access_code_secret(self, user_id: str, secret: str) -> None:
        """Persist the reading-code secret so the admin can re-show the links.

        The login hash stays one-way; this column only mirrors the reading
        access code, which stays revocable at any time via reset.
        """
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE users SET access_code_secret = ?, updated_at = ? WHERE user_id = ?",
                (str(secret or ""), self._now(), user_id),
            )
            conn.commit()

    def access_code_for_user(
        self,
        user_id: str,
        *,
        actor_user_id: str = "",
        actor_role: str = "",
    ) -> str:
        """Return the user's current access code for link re-display.

        Codes issued before the secret was persisted cannot be recovered; the
        admin must regenerate (invalidating the old link) in that case.
        """
        user = self.get_user(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="用户不存在")
        if user.is_admin:
            raise HTTPException(status_code=400, detail="管理员账号没有书源链接")
        with self._conn() as conn:
            row = conn.execute(
                "SELECT username, access_code_secret FROM users WHERE user_id = ?",
                (user_id,),
            ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="用户不存在")
        secret = str(row[1] or "").strip()
        if not secret:
            raise HTTPException(
                status_code=409,
                detail="该用户的授权码创建于旧版本、未存储明文，无法直接查看；请重新生成后使用新链接",
            )
        if actor_user_id:
            with self._conn() as conn:
                audit_service.record(
                    action="user.access_code.reveal",
                    actor_user_id=actor_user_id,
                    actor_role=actor_role,
                    target_type="user",
                    target_id=user_id,
                    conn=conn,
                )
                conn.commit()
        return self.build_access_code(str(row[0]), secret)

    def bootstrap_admin(self, username: str, password: str | None = None) -> dict[str, Any]:
        with self._bootstrap_lock:
            if self.user_count() > 0:
                raise HTTPException(status_code=409, detail="系统已存在用户，不能再次初始化管理员")
            if password is None:
                password = read_secret("LEGADOHUB_ADMIN_PASSWORD") or self._admin_password_from_config()
            if not password:
                raise HTTPException(status_code=400, detail="管理员密码不能为空")
            return self.create_user(
                username=username,
                password=password,
                role="admin",
                require_empty=True,
            )

    def ensure_default_admin(
        self,
        username: str = "admin",
        *,
        on_generated_password: Callable[[str], None] | None = None,
    ) -> bool:
        """Create the first administrator, generating a local-only password when needed."""
        if self.user_count() > 0:
            self.migrate_admin_password_config(username=username)
            if self.enabled_admin_count() <= 0:
                raise RuntimeError("The database has users but no enabled administrator.")
            return False
        environment_password = read_secret("LEGADOHUB_ADMIN_PASSWORD")
        password = environment_password or self._admin_password_from_config()
        generated_password = False
        if not password:
            password = secrets.token_urlsafe(GENERATED_ADMIN_PASSWORD_BYTES)
            generated_password = True
        self.bootstrap_admin(username=username, password=password)
        if generated_password and on_generated_password is not None:
            on_generated_password(password)
        # The newly created database already contains the selected password.
        # Explicit environment configuration must not be overwritten by a stale
        # legacy app_config value during retirement.
        self.migrate_admin_password_config(username=username, apply_password=False)
        return True

    def migrate_admin_password_config(
        self,
        *,
        username: str = "admin",
        apply_password: bool = True,
    ) -> bool:
        config = AppConfig.get()
        legacy_value = config.auth.admin_password_base64.strip()
        if not legacy_value:
            return False
        password = config.auth.admin_password()
        if apply_password and password is None:
            raise RuntimeError("Legacy administrator password configuration is invalid.")
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            migrated = conn.execute(
                "SELECT value FROM schema_meta WHERE key = ?",
                (ADMIN_PASSWORD_CONFIG_MIGRATION_KEY,),
            ).fetchone()
            if not migrated:
                admin = conn.execute(
                    "SELECT user_id FROM users WHERE username = ? AND role = 'admin'",
                    (username,),
                ).fetchone()
                if apply_password and admin:
                    validated_password = self._validate_password(password or "")
                    conn.execute(
                        "UPDATE users SET password_hash = ?, updated_at = ? WHERE user_id = ?",
                        (self.hash_password(validated_password), self._now(), admin[0]),
                    )
                    conn.execute("DELETE FROM user_sessions WHERE user_id = ?", (admin[0],))
                conn.execute(
                    "INSERT OR REPLACE INTO schema_meta (key, value) VALUES (?, '1')",
                    (ADMIN_PASSWORD_CONFIG_MIGRATION_KEY,),
                )
            # The marker makes the database authoritative before the legacy
            # file value is retired. If the atomic config save below fails,
            # startup aborts and the next run only retries removing that
            # inert value; it must never reapply the password.
            conn.commit()
        config.unset("auth.adminPasswordBase64")
        try:
            config.save()
        except Exception:
            config.reload()
            raise
        return True

    def reset_password(
        self,
        user_id: str,
        password: str,
        *,
        invalidate_sessions: bool = True,
        actor_user_id: str = "",
        actor_role: str = "",
        audit_action: str = "user.password.reset",
    ) -> dict[str, Any]:
        password = self._validate_password(password)
        now = self._now()
        password_hash = self.hash_password(password)
        with self._conn() as conn:
            cursor = conn.execute(
                "UPDATE users SET password_hash = ?, updated_at = ? WHERE user_id = ?",
                (password_hash, now, user_id),
            )
            if cursor.rowcount > 0 and invalidate_sessions:
                conn.execute("DELETE FROM user_sessions WHERE user_id = ?", (user_id,))
            if cursor.rowcount > 0 and actor_user_id:
                audit_service.record(
                    action=audit_action,
                    actor_user_id=actor_user_id,
                    actor_role=actor_role,
                    target_type="user",
                    target_id=user_id,
                    conn=conn,
                )
            conn.commit()
        if cursor.rowcount <= 0:
            raise HTTPException(status_code=404, detail="用户不存在")

        return {"userId": user_id, "passwordReset": True}

    def change_password(self, user: AuthUser, current_password: str, new_password: str) -> dict[str, Any]:
        self.authenticate(user.username, current_password)
        return self.reset_password(
            user.user_id,
            new_password,
            invalidate_sessions=True,
            actor_user_id=user.user_id,
            actor_role=user.role,
            audit_action="user.password.change",
        )

    def set_disabled(
        self,
        user_id: str,
        disabled: bool,
        *,
        actor_user_id: str = "",
        actor_role: str = "",
    ) -> dict[str, Any]:
        now = self._now()
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            target = conn.execute(
                "SELECT role, disabled FROM users WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            if not target:
                raise HTTPException(status_code=404, detail="用户不存在")
            if disabled and actor_user_id and user_id == actor_user_id:
                raise HTTPException(status_code=409, detail="不能禁用当前登录的管理员")
            if disabled and target[0] == "admin" and not bool(target[1]):
                enabled_admins = conn.execute(
                    "SELECT COUNT(*) FROM users WHERE role = 'admin' AND disabled = 0"
                ).fetchone()
                if int(enabled_admins[0] or 0) <= 1:
                    raise HTTPException(status_code=409, detail="系统必须保留至少一个可用管理员")
            cursor = conn.execute(
                "UPDATE users SET disabled = ?, updated_at = ? WHERE user_id = ?",
                (1 if disabled else 0, now, user_id),
            )
            if disabled:
                conn.execute("DELETE FROM user_sessions WHERE user_id = ?", (user_id,))
            if actor_user_id:
                audit_service.record(
                    action="user.disable" if disabled else "user.enable",
                    actor_user_id=actor_user_id,
                    actor_role=actor_role,
                    target_type="user",
                    target_id=user_id,
                    summary={"role": target[0], "disabled": bool(disabled)},
                    conn=conn,
                )
            conn.commit()
        return {"userId": user_id, "disabled": bool(disabled)}

    def delete_user(
        self,
        user_id: str,
        *,
        actor_user_id: str,
    ) -> dict[str, Any]:
        """Delete an access user and their private runtime state.

        Shared books and historical audit/operation records are intentionally
        retained because they can still be used by other subscribers.
        """
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            actor = conn.execute(
                "SELECT role, disabled FROM users WHERE user_id = ?",
                (actor_user_id,),
            ).fetchone()
            if not actor or actor[0] != "admin" or bool(actor[1]):
                raise HTTPException(status_code=403, detail="需要可用的管理员账户")
            target = conn.execute(
                "SELECT role FROM users WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            if not target:
                raise HTTPException(status_code=404, detail="用户不存在")
            if target[0] != "user":
                raise HTTPException(status_code=409, detail="管理员账户不能删除")
            if actor_user_id and user_id == actor_user_id:
                raise HTTPException(status_code=409, detail="不能删除当前登录账户")

            revoked_sessions = conn.execute(
                "DELETE FROM user_sessions WHERE user_id = ?",
                (user_id,),
            ).rowcount
            deleted_subscriptions = conn.execute(
                "DELETE FROM user_book_subscriptions WHERE user_id = ?",
                (user_id,),
            ).rowcount
            deleted_search_jobs = conn.execute(
                "DELETE FROM subscription_search_jobs WHERE owner_user_id = ?",
                (user_id,),
            ).rowcount
            conn.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
            audit_service.record(
                action="user.delete",
                actor_user_id=actor_user_id,
                actor_role=actor[0],
                target_type="user",
                target_id=user_id,
                summary={
                    "role": target[0],
                    "deleted": True,
                    "affectedCount": (
                        max(0, revoked_sessions)
                        + max(0, deleted_subscriptions)
                        + max(0, deleted_search_jobs)
                    ),
                },
                conn=conn,
            )
            conn.commit()

        return {
            "userId": user_id,
            "deleted": True,
            "revokedSessions": max(0, revoked_sessions),
            "deletedSubscriptions": max(0, deleted_subscriptions),
            "deletedSearchJobs": max(0, deleted_search_jobs),
        }

    def get_user_by_username(self, username: str) -> dict[str, Any] | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT user_id, username, password_hash, role, disabled
                FROM users
                WHERE username = ?
                """,
                (username,),
            ).fetchone()
        if not row:
            return None
        return {
            "userId": row[0],
            "username": row[1],
            "passwordHash": row[2],
            "role": row[3],
            "disabled": bool(row[4]),
        }

    def get_user(self, user_id: str) -> AuthUser | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT user_id, username, role, disabled
                FROM users
                WHERE user_id = ?
                """,
                (user_id,),
            ).fetchone()
        if not row:
            return None
        return AuthUser(
            user_id=row[0],
            username=row[1],
            role=row[2],
            disabled=bool(row[3]),
        )

    def hash_password(self, password: str) -> str:
        salt = os.urandom(16)
        digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, PBKDF2_ITERATIONS)
        return "pbkdf2_sha256${}${}${}".format(
            PBKDF2_ITERATIONS,
            base64.urlsafe_b64encode(salt).decode("ascii"),
            base64.urlsafe_b64encode(digest).decode("ascii"),
        )

    def verify_password(self, password: str, stored: str) -> bool:
        try:
            scheme, iterations, salt_b64, digest_b64 = stored.split("$", 3)
            if scheme != "pbkdf2_sha256":
                return False
            salt = base64.urlsafe_b64decode(salt_b64.encode("ascii"))
            expected = base64.urlsafe_b64decode(digest_b64.encode("ascii"))
            actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, int(iterations))
            return hmac.compare_digest(expected, actual)
        except Exception:
            return False

    def _admin_password_from_config(self) -> str | None:
        """Return the admin password configured in app_config.json, if any."""
        return AppConfig.get().auth.admin_password()

    def authenticate(self, username: str, password: str) -> AuthUser:
        username = str(username or "").strip()
        supplied = str(password or "")
        row = self.get_user_by_username(username)
        if not row:
            self._burn_dummy_password_check(supplied)
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="用户名或密码错误")
        if not self.verify_password(supplied, row["passwordHash"]):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="用户名或密码错误")

        if row["disabled"]:
            # Keep every administrator login rejection indistinguishable at
            # the public boundary. Account state remains available to the
            # control plane and audit log, not to an unauthenticated caller.
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="用户名或密码错误")
        user = self.get_user(row["userId"])
        if not user:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="用户不存在")
        return user

    @staticmethod
    def _session_key(session_id: str) -> str:
        return hashlib.sha256(str(session_id or "").encode("utf-8")).hexdigest()

    def create_session(self, user: AuthUser) -> str:
        session_id = secrets.token_urlsafe(32)
        now = self._now()
        expires = self._future()
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DELETE FROM user_sessions WHERE expires_at <= ?", (now,))
            existing = conn.execute(
                """
                SELECT session_id
                FROM user_sessions
                WHERE user_id = ?
                ORDER BY created_at ASC, session_id ASC
                """,
                (user.user_id,),
            ).fetchall()
            evict_count = max(0, len(existing) - MAX_SESSIONS_PER_USER + 1)
            for row in existing[:evict_count]:
                conn.execute("DELETE FROM user_sessions WHERE session_id = ?", (row[0],))
            conn.execute(
                """
                INSERT INTO user_sessions (session_id, user_id, expires_at, created_at, last_seen_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (self._session_key(session_id), user.user_id, expires, now, now),
            )
            if evict_count:
                audit_service.record(
                    action="user.sessions.limit",
                    actor_user_id=user.user_id,
                    actor_role=user.role,
                    target_type="user",
                    target_id=user.user_id,
                    summary={"affectedCount": evict_count},
                    conn=conn,
                )
            conn.commit()
        return session_id

    def destroy_session(self, session_id: str) -> None:
        if not session_id:
            return
        with self._conn() as conn:
            conn.execute("DELETE FROM user_sessions WHERE session_id = ?", (self._session_key(session_id),))
            conn.commit()

    def revoke_user_sessions(
        self,
        user_id: str,
        *,
        actor_user_id: str = "",
        actor_role: str = "",
    ) -> dict[str, Any]:
        if not self.get_user(user_id):
            raise HTTPException(status_code=404, detail="用户不存在")
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute("DELETE FROM user_sessions WHERE user_id = ?", (user_id,))
            if actor_user_id:
                audit_service.record(
                    action="user.sessions.revoke",
                    actor_user_id=actor_user_id,
                    actor_role=actor_role,
                    target_type="user",
                    target_id=user_id,
                    summary={"affectedCount": max(0, cursor.rowcount)},
                    conn=conn,
                )
            conn.commit()
        return {"userId": user_id, "revokedSessions": max(0, cursor.rowcount)}

    def _get_session(self, session_id: str) -> dict[str, Any] | None:
        session_key = self._session_key(session_id)
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT session_id, user_id, expires_at
                FROM user_sessions
                WHERE session_id = ?
                """,
                (session_key,),
            ).fetchone()
        if not row:
            return None
        return {"sessionKey": row[0], "userId": row[1], "expiresAt": row[2]}

    def session_expires_at(self, session_id: str) -> str:
        session = self._get_session(session_id)
        return str(session["expiresAt"]) if session else ""

    def _is_expired(self, expires_at: str) -> bool:
        try:
            return datetime.fromisoformat(expires_at) <= datetime.now(timezone.utc)
        except Exception:
            return True

    def session_token_from_request(self, request: Request) -> str:
        authorization_values = request.headers.getlist("authorization")
        if authorization_values:
            if len(authorization_values) != 1:
                return ""
            authorization = authorization_values[0].strip()
            if "," in authorization:
                return ""
            parts = authorization.split()
            if len(parts) != 2 or parts[0].lower() != "bearer":
                return ""
            token = parts[1]
            if not 20 <= len(token) <= 128 or any(character.isspace() for character in token):
                return ""
            return token
        return request.cookies.get(SESSION_COOKIE_NAME, "")

    def current_user(self, request: Request, *, touch: bool = True) -> AuthUser | None:
        session_id = self.session_token_from_request(request)
        if not session_id:
            return None
        session = self._get_session(session_id)
        if not session:
            return None
        if self._is_expired(session["expiresAt"]):
            self.destroy_session(session_id)
            return None
        user = self.get_user(session["userId"])
        if not user or user.disabled:
            self.destroy_session(session_id)
            return None
        if touch:
            with self._conn() as conn:
                conn.execute(
                    "UPDATE user_sessions SET last_seen_at = ? WHERE session_id = ?",
                    (self._now(), session["sessionKey"]),
                )
                conn.commit()
        return user

    def require_user(self, request: Request, *, touch: bool = True) -> AuthUser:
        user = self.current_user(request, touch=touch)
        if not user:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="请先登录")
        return user

    def require_reading_user(self, request: Request, *, touch: bool = True) -> AuthUser:
        """Require a user through the public Reading data plane."""
        user = self.current_user(request, touch=touch)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=READING_LOGIN_REQUIRED_MESSAGE,
            )
        return user

    def require_admin(self, request: Request, *, touch: bool = True) -> AuthUser:
        user = self.require_user(request, touch=touch)
        if not user.is_admin:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="需要管理员权限")
        return user

    def set_session_cookie(self, response: Response, session_id: str, *, secure: bool = False) -> None:
        response.set_cookie(
            key=SESSION_COOKIE_NAME,
            value=session_id,
            httponly=True,
            samesite="lax",
            secure=secure,
            max_age=SESSION_TTL_DAYS * 24 * 60 * 60,
            path="/",
        )

    def clear_session_cookie(self, response: Response) -> None:
        response.delete_cookie(SESSION_COOKIE_NAME, path="/")


auth_service = UserAuthService()
auth_rate_limiter = AuthRateLimiter()
