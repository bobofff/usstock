"""Minimal JSON HTTP client using the standard library."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from typing import Any


class HttpClientError(RuntimeError):
    """Raised when an HTTP JSON request fails."""


class JsonHttpClient:
    def __init__(
        self,
        *,
        base_url: str = "",
        timeout_seconds: float = 30.0,
        user_agent: str = "usstock-polymarket-weather/0.1",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.user_agent = user_agent

    def get_json(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Any:
        url = self._build_url(path, params=params)
        request_headers = {"User-Agent": self.user_agent, "Accept": "application/json"}
        if headers:
            request_headers.update(headers)
        request = urllib.request.Request(url, headers=request_headers, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise HttpClientError(f"GET {url} failed with {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise HttpClientError(f"GET {url} failed: {exc.reason}") from exc
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise HttpClientError(f"GET {url} returned invalid JSON.") from exc

    def _build_url(self, path: str, *, params: Mapping[str, Any] | None) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            url = path
        elif self.base_url:
            url = f"{self.base_url}/{path.lstrip('/')}"
        else:
            url = path

        if not params:
            return url
        query_items: list[tuple[str, str]] = []
        for key, value in params.items():
            if value is None:
                continue
            if isinstance(value, (list, tuple, set)):
                encoded_value = ",".join(str(item) for item in value)
            else:
                encoded_value = str(value)
            query_items.append((key, encoded_value))
        query = urllib.parse.urlencode(query_items)
        separator = "&" if "?" in url else "?"
        return f"{url}{separator}{query}"
