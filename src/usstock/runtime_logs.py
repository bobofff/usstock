"""Runtime file logging helpers."""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Mapping
from datetime import date, datetime
from functools import wraps
from pathlib import Path
from typing import Any, ParamSpec, TypeVar

from usstock.config.settings import PROJECT_ROOT


LOG_DIR = PROJECT_ROOT / "logs"
_LOG_LOCK = threading.Lock()
_P = ParamSpec("_P")
_R = TypeVar("_R")


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def _daily_log_path(now: datetime) -> Path:
    return LOG_DIR / f"{now.date().isoformat()}.log"


def log_sync_event(
    *,
    source: str,
    action: str,
    status: str,
    details: Mapping[str, Any] | None = None,
    result: Mapping[str, Any] | None = None,
    error: BaseException | None = None,
    now: datetime | None = None,
) -> Path | None:
    """Append one sync event to logs/YYYY-MM-DD.log."""

    logged_at = now or datetime.now()
    event: dict[str, Any] = {
        "timestamp": logged_at.isoformat(timespec="seconds"),
        "source": source,
        "action": action,
        "status": status,
    }
    if details:
        event["details"] = dict(details)
    if result:
        event["result"] = dict(result)
    if error is not None:
        event["error"] = {
            "type": error.__class__.__name__,
            "message": str(error),
        }

    line = json.dumps(event, ensure_ascii=False, sort_keys=True, default=_json_default)
    path = _daily_log_path(logged_at)
    try:
        with _LOG_LOCK:
            LOG_DIR.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as file:
                file.write(line + "\n")
    except OSError:
        return None
    return path


def log_sync_operation(
    *,
    source: str,
    action: str,
    details_builder: Callable[_P, Mapping[str, Any]] | None = None,
    result_builder: Callable[[_R], Mapping[str, Any]] | None = None,
) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    """Decorate a sync operation with success/fail file logging."""

    def decorator(func: Callable[_P, _R]) -> Callable[_P, _R]:
        @wraps(func)
        def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            details = details_builder(*args, **kwargs) if details_builder else {}
            try:
                value = func(*args, **kwargs)
            except Exception as exc:
                log_sync_event(
                    source=source,
                    action=action,
                    status="fail",
                    details=details,
                    error=exc,
                )
                raise

            log_sync_event(
                source=source,
                action=action,
                status="success",
                details=details,
                result=result_builder(value) if result_builder else {"value": value},
            )
            return value

        return wrapper

    return decorator
