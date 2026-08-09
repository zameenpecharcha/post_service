"""Helpers for multi-schema PostgreSQL (post / user / api_gateway)."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4

POST_SCHEMA = "post"
USER_SCHEMA = "user"
MEDIA_SCHEMA = "api_gateway"

POST_TYPES = frozenset({"TEXT", "IMAGE", "VIDEO", "PROPERTY", "POLL", "REVIEW"})
POST_VISIBILITIES = frozenset({"PUBLIC", "FOLLOWERS_ONLY", "PRIVATE"})
POST_STATUSES = frozenset({"DRAFT", "PUBLISHED", "ARCHIVED", "DELETED"})
COMMENT_STATUSES = frozenset({"ACTIVE", "DELETED", "HIDDEN"})
POST_REACTIONS = frozenset({"LIKE", "LOVE", "WOW", "HAHA", "SAD", "ANGRY"})
COMMENT_REACTIONS = frozenset({"LIKE", "LOVE", "WOW", "HAHA"})
SHARE_TYPES = frozenset({"REPOST", "SHARE"})
REPORT_REASON_CODES = frozenset({
    "SPAM", "FAKE_PROPERTY", "ABUSIVE_LANGUAGE", "MISLEADING_INFORMATION",
    "HARASSMENT", "INAPPROPRIATE_CONTENT", "SCAM", "COPYRIGHT", "OTHER",
})


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_uuid(value) -> Optional[UUID]:
    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    text = str(value).strip()
    if not text or text == "0":
        return None
    return UUID(text)


def uuid_str(value) -> str:
    if value is None:
        return ""
    if isinstance(value, UUID):
        return str(value)
    return str(value)


def generate_business_code(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex[:8].upper()}"


def _normalize(value: Optional[str], default: str, allowed: frozenset, legacy: Optional[dict] = None) -> str:
    if not value or not str(value).strip():
        return default
    key = str(value).strip().upper().replace(" ", "_").replace("-", "_")
    if legacy and key in legacy:
        key = legacy[key]
    if key in allowed:
        return key
    return default


def normalize_post_type(value: Optional[str]) -> str:
    return _normalize(value, "TEXT", POST_TYPES)


def normalize_visibility(value: Optional[str]) -> str:
    return _normalize(value, "PUBLIC", POST_VISIBILITIES)


def normalize_post_status(value: Optional[str]) -> str:
    return _normalize(
        value,
        "DRAFT",
        POST_STATUSES,
        legacy={"ACTIVE": "PUBLISHED"},
    )


def normalize_comment_status(value: Optional[str]) -> str:
    return _normalize(
        value,
        "ACTIVE",
        COMMENT_STATUSES,
        legacy={"DELETED": "DELETED", "HIDDEN": "HIDDEN"},
    )


def normalize_post_reaction(value: Optional[str]) -> str:
    return _normalize(value, "LIKE", POST_REACTIONS)


def normalize_comment_reaction(value: Optional[str]) -> str:
    return _normalize(value, "LIKE", COMMENT_REACTIONS)


def normalize_share_type(value: Optional[str]) -> str:
    return _normalize(value, "SHARE", SHARE_TYPES)


def normalize_reason_code(value: Optional[str]) -> Optional[str]:
    if not value or not str(value).strip():
        return None
    key = str(value).strip().upper().replace(" ", "_").replace("-", "_")
    if key in REPORT_REASON_CODES:
        return key
    return "OTHER"
