"""Polymarket Gamma and CLOB market-data adapters."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any

from usstock.polymarket_weather.buckets import BucketParseError, parse_temperature_bucket
from usstock.polymarket_weather.cache import FileCache
from usstock.polymarket_weather.http import JsonHttpClient
from usstock.polymarket_weather.models import MarketBucket, TemperatureUnit


class MarketDataError(RuntimeError):
    """Raised when Polymarket data cannot be loaded or parsed."""


def _json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return [part.strip() for part in text.split(",") if part.strip()]
        if isinstance(parsed, list):
            return parsed
        return []
    return []


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class GammaMarketClient:
    """Read Polymarket market metadata and outcome prices from Gamma."""

    def __init__(
        self,
        *,
        gamma_base_url: str = "https://gamma-api.polymarket.com",
        clob_base_url: str = "https://clob.polymarket.com",
        http_client: JsonHttpClient | None = None,
        clob_http_client: JsonHttpClient | None = None,
        cache: FileCache | None = None,
        cache_max_age_seconds: int = 60,
    ) -> None:
        self.gamma_base_url = gamma_base_url.rstrip("/")
        self.clob_base_url = clob_base_url.rstrip("/")
        self.http = http_client or JsonHttpClient(base_url=self.gamma_base_url)
        self.clob_http = clob_http_client or JsonHttpClient(base_url=self.clob_base_url)
        self.cache = cache or FileCache()
        self.cache_max_age_seconds = cache_max_age_seconds

    def search_markets(
        self,
        *,
        query: str | None = None,
        slug: str | None = None,
        condition_id: str | None = None,
        limit: int = 50,
        active: bool = True,
        closed: bool = False,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "limit": limit,
            "active": str(active).lower(),
            "closed": str(closed).lower(),
        }
        if query:
            params["search"] = query
        if slug:
            params["slug"] = slug
        if condition_id:
            params["condition_ids"] = condition_id

        cache_key = {"provider": "polymarket-gamma", "path": "/markets", "params": params}
        data = self.cache.get(cache_key, max_age_seconds=self.cache_max_age_seconds)
        if data is None:
            data = self.http.get_json("/markets", params=params)
            self.cache.set(cache_key, data)
        if not isinstance(data, list):
            raise MarketDataError("Gamma /markets response is not a list.")
        return [item for item in data if isinstance(item, dict)]

    def find_market(
        self,
        *,
        query: str | None = None,
        slug: str | None = None,
        condition_id: str | None = None,
    ) -> dict[str, Any]:
        markets = self.search_markets(query=query, slug=slug, condition_id=condition_id)
        if slug:
            for market in markets:
                if str(market.get("slug") or "") == slug:
                    return market
        if condition_id:
            for market in markets:
                if str(market.get("conditionId") or market.get("condition_id") or "") == condition_id:
                    return market
        if not markets:
            raise MarketDataError("No matching Polymarket market found.")
        return markets[0]

    def get_market_buckets(
        self,
        *,
        query: str | None = None,
        slug: str | None = None,
        condition_id: str | None = None,
        default_unit: TemperatureUnit = "F",
        refresh_clob_midpoints: bool = False,
    ) -> tuple[MarketBucket, ...]:
        market = self.find_market(query=query, slug=slug, condition_id=condition_id)
        buckets = tuple(parse_market_buckets(market, default_unit=default_unit))
        if refresh_clob_midpoints:
            buckets = tuple(self._with_midpoints(buckets))
        return buckets

    def _with_midpoints(self, buckets: Iterable[MarketBucket]) -> Iterable[MarketBucket]:
        for bucket in buckets:
            if not bucket.token_id:
                yield bucket
                continue
            midpoint = self.get_midpoint(bucket.token_id)
            if midpoint is None:
                yield bucket
                continue
            yield MarketBucket(
                market_id=bucket.market_id,
                question=bucket.question,
                slug=bucket.slug,
                condition_id=bucket.condition_id,
                outcome=bucket.outcome,
                price=midpoint,
                bucket=bucket.bucket,
                token_id=bucket.token_id,
                raw_payload=bucket.raw_payload,
            )

    def get_midpoint(self, token_id: str) -> float | None:
        cache_key = {"provider": "polymarket-clob", "path": "/midpoint", "token_id": token_id}
        data = self.cache.get(cache_key, max_age_seconds=self.cache_max_age_seconds)
        if data is None:
            data = self.clob_http.get_json("/midpoint", params={"token_id": token_id})
            self.cache.set(cache_key, data)
        if isinstance(data, Mapping):
            return _safe_float(data.get("mid") or data.get("midpoint"))
        return None


def parse_market_buckets(
    market: Mapping[str, Any],
    *,
    default_unit: TemperatureUnit = "F",
) -> Iterable[MarketBucket]:
    outcomes = _json_list(market.get("outcomes") or market.get("tokens"))
    prices = _json_list(market.get("outcomePrices") or market.get("outcome_prices"))
    token_ids = _json_list(
        market.get("clobTokenIds")
        or market.get("clob_token_ids")
        or market.get("tokenIds")
    )
    market_id = str(market.get("id") or market.get("marketId") or market.get("market_id") or "")
    question = str(market.get("question") or market.get("title") or "")
    slug = str(market.get("slug")) if market.get("slug") else None
    condition_id = (
        str(market.get("conditionId"))
        if market.get("conditionId")
        else str(market.get("condition_id")) if market.get("condition_id") else None
    )

    for index, raw_outcome in enumerate(outcomes):
        if isinstance(raw_outcome, Mapping):
            outcome = str(raw_outcome.get("outcome") or raw_outcome.get("name") or "")
            token_id = (
                str(raw_outcome.get("token_id"))
                if raw_outcome.get("token_id")
                else str(raw_outcome.get("clobTokenId"))
                if raw_outcome.get("clobTokenId")
                else None
            )
            price = _safe_float(raw_outcome.get("price") or raw_outcome.get("lastPrice"))
        else:
            outcome = str(raw_outcome)
            token_id = str(token_ids[index]) if index < len(token_ids) and token_ids[index] else None
            price = _safe_float(prices[index]) if index < len(prices) else None

        if not outcome:
            continue
        if price is None:
            price = 0.0
        try:
            bucket = parse_temperature_bucket(outcome, default_unit=default_unit)
        except BucketParseError:
            continue
        yield MarketBucket(
            market_id=market_id,
            question=question,
            slug=slug,
            condition_id=condition_id,
            outcome=outcome,
            price=price,
            bucket=bucket,
            token_id=token_id,
            raw_payload=dict(market),
        )
