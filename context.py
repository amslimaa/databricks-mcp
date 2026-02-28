"""
Request context management for per-request credentials.
"""

from dataclasses import dataclass
from contextvars import ContextVar


@dataclass
class RequestCredentials:
    host: str
    token: str


_request_credentials: ContextVar[RequestCredentials | None] = ContextVar(
    "request_credentials", default=None
)


def set_request_credentials(host: str | None, token: str | None) -> None:
    """Store credentials for the current request."""
    if host and token:
        _request_credentials.set(RequestCredentials(host=host, token=token))


def get_request_credentials() -> RequestCredentials | None:
    """Get credentials for the current request."""
    return _request_credentials.get()


def clear_request_credentials() -> None:
    """Clear credentials after request completes."""
    _request_credentials.set(None)
