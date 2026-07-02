"""Small JSON file cache for free public APIs."""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any

from usstock.config.settings import PROJECT_ROOT


DEFAULT_CACHE_DIR = PROJECT_ROOT / "data" / "polymarket_weather_cache"


class FileCache:
    """TTL-based JSON cache keyed by stable request metadata."""

    def __init__(self, cache_dir: Path = DEFAULT_CACHE_DIR) -> None:
        self.cache_dir = cache_dir

    def _path_for_key(self, key: Any) -> Path:
        payload = json.dumps(key, sort_keys=True, default=str, ensure_ascii=False)
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        return self.cache_dir / f"{digest}.json"

    def get(self, key: Any, *, max_age_seconds: int | None = None) -> Any | None:
        path = self._path_for_key(key)
        if not path.exists():
            return None
        if max_age_seconds is not None:
            age = time.time() - path.stat().st_mtime
            if age > max_age_seconds:
                return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

    def set(self, key: Any, value: Any) -> Path | None:
        path = self._path_for_key(key)
        try:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str),
                encoding="utf-8",
            )
        except OSError:
            return None
        return path
