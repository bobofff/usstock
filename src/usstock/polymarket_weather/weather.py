"""Weather forecast providers."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from usstock.polymarket_weather.cache import FileCache
from usstock.polymarket_weather.http import JsonHttpClient
from usstock.polymarket_weather.models import (
    CityConfig,
    EnsembleForecast,
    ForecastPoint,
    TemperatureKind,
    TemperatureUnit,
)
from usstock.polymarket_weather.units import convert_temperature
from usstock.runtime_logs import log_sync_event


LOGGER = logging.getLogger(__name__)
OPEN_METEO_DAILY_FIELDS = {
    "high": "temperature_2m_max",
    "low": "temperature_2m_min",
}


class WeatherProviderError(RuntimeError):
    """Raised when a weather provider cannot return usable forecast data."""


def _unit_from_open_meteo(value: str | None) -> TemperatureUnit:
    text = (value or "").upper()
    if "F" in text:
        return "F"
    return "C"


class OpenMeteoForecastClient:
    """Open-Meteo daily forecast client.

    The client fetches one numerical model at a time. This keeps parsing stable
    across Open-Meteo model combinations and makes per-model caching explicit.
    """

    def __init__(
        self,
        *,
        base_url: str = "https://api.open-meteo.com",
        http_client: JsonHttpClient | None = None,
        cache: FileCache | None = None,
        cache_max_age_seconds: int = 900,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.http = http_client or JsonHttpClient(base_url=self.base_url)
        self.cache = cache or FileCache()
        self.cache_max_age_seconds = cache_max_age_seconds

    def fetch_point(
        self,
        city: CityConfig,
        *,
        target_date: date,
        kind: TemperatureKind,
        model: str,
    ) -> ForecastPoint:
        field = OPEN_METEO_DAILY_FIELDS[kind]
        params = {
            "latitude": city.latitude,
            "longitude": city.longitude,
            "daily": "temperature_2m_max,temperature_2m_min",
            "timezone": city.timezone,
            "start_date": target_date.isoformat(),
            "end_date": target_date.isoformat(),
            "models": model,
        }
        cache_key = {"provider": "open-meteo", "path": "/v1/forecast", "params": params}
        data = self.cache.get(cache_key, max_age_seconds=self.cache_max_age_seconds)
        if data is None:
            data = self.http.get_json("/v1/forecast", params=params)
            self.cache.set(cache_key, data)

        try:
            daily = data["daily"]
            times = daily["time"]
            index = times.index(target_date.isoformat())
            value = float(daily[field][index])
            source_unit = _unit_from_open_meteo((data.get("daily_units") or {}).get(field))
        except (KeyError, TypeError, ValueError, IndexError) as exc:
            raise WeatherProviderError(
                f"Open-Meteo response missing {field} for {city.city_id} {target_date}."
            ) from exc

        return ForecastPoint(
            city_id=city.city_id,
            target_date=target_date,
            kind=kind,
            value=convert_temperature(
                value,
                from_unit=source_unit,
                to_unit=city.settlement_unit,
            ),
            unit=city.settlement_unit,
            source_model=model,
            raw_payload={"provider": "open-meteo", "model": model},
        )

    def fetch_points(
        self,
        city: CityConfig,
        *,
        target_date: date,
        kind: TemperatureKind,
        models: tuple[str, ...],
    ) -> tuple[ForecastPoint, ...]:
        points: list[ForecastPoint] = []
        errors: list[str] = []
        for model in models:
            try:
                points.append(
                    self.fetch_point(
                        city,
                        target_date=target_date,
                        kind=kind,
                        model=model,
                    )
                )
            except Exception as exc:
                LOGGER.warning("Open-Meteo model failed: %s", model, exc_info=exc)
                errors.append(f"{model}: {exc}")
        if not points:
            raise WeatherProviderError("; ".join(errors) or "No Open-Meteo points fetched.")
        return tuple(points)


class NWSForecastClient:
    """National Weather Service forecast adapter for US locations."""

    def __init__(
        self,
        *,
        base_url: str = "https://api.weather.gov",
        http_client: JsonHttpClient | None = None,
        cache: FileCache | None = None,
        cache_max_age_seconds: int = 900,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.http = http_client or JsonHttpClient(base_url=self.base_url)
        self.cache = cache or FileCache()
        self.cache_max_age_seconds = cache_max_age_seconds

    def fetch_point(
        self,
        city: CityConfig,
        *,
        target_date: date,
        kind: TemperatureKind,
    ) -> ForecastPoint:
        point_key = {
            "provider": "nws",
            "path": "points",
            "lat": round(city.latitude, 4),
            "lon": round(city.longitude, 4),
        }
        points_data = self.cache.get(point_key, max_age_seconds=self.cache_max_age_seconds)
        if points_data is None:
            points_data = self.http.get_json(f"/points/{city.latitude:.4f},{city.longitude:.4f}")
            self.cache.set(point_key, points_data)

        try:
            forecast_url = points_data["properties"]["forecast"]
        except (KeyError, TypeError) as exc:
            raise WeatherProviderError("NWS points response does not include forecast URL.") from exc

        forecast_key = {"provider": "nws", "url": forecast_url}
        forecast_data = self.cache.get(forecast_key, max_age_seconds=self.cache_max_age_seconds)
        if forecast_data is None:
            forecast_data = self.http.get_json(forecast_url)
            self.cache.set(forecast_key, forecast_data)

        value = self._extract_temperature(forecast_data, target_date=target_date, kind=kind)
        return ForecastPoint(
            city_id=city.city_id,
            target_date=target_date,
            kind=kind,
            value=convert_temperature(value, from_unit="F", to_unit=city.settlement_unit),
            unit=city.settlement_unit,
            source_model="nws",
            raw_payload={"provider": "nws"},
        )

    @staticmethod
    def _extract_temperature(
        payload: dict[str, Any],
        *,
        target_date: date,
        kind: TemperatureKind,
    ) -> float:
        try:
            periods = payload["properties"]["periods"]
        except (KeyError, TypeError) as exc:
            raise WeatherProviderError("NWS forecast response missing periods.") from exc

        target_prefix = target_date.isoformat()
        matching: list[dict[str, Any]] = []
        for period in periods:
            if not isinstance(period, dict):
                continue
            start_time = str(period.get("startTime") or "")
            if not start_time.startswith(target_prefix):
                continue
            is_daytime = bool(period.get("isDaytime"))
            if kind == "high" and is_daytime:
                matching.append(period)
            elif kind == "low" and not is_daytime:
                matching.append(period)
        if not matching:
            raise WeatherProviderError(f"NWS forecast has no {kind} period for {target_date}.")
        return float(matching[0]["temperature"])


class WeatherEnsembleProvider:
    """Combined provider for Open-Meteo models plus optional NWS."""

    def __init__(
        self,
        *,
        open_meteo: OpenMeteoForecastClient | None = None,
        nws: NWSForecastClient | None = None,
    ) -> None:
        self.open_meteo = open_meteo or OpenMeteoForecastClient()
        self.nws = nws or NWSForecastClient()

    def fetch_ensemble(
        self,
        city: CityConfig,
        *,
        target_date: date,
        kind: TemperatureKind,
        models: tuple[str, ...] | None = None,
    ) -> EnsembleForecast:
        selected_models = models or city.weather_models
        open_meteo_models = tuple(
            model for model in selected_models if model.strip().lower() != "nws"
        )
        points: list[ForecastPoint] = []
        if open_meteo_models:
            points.extend(
                self.open_meteo.fetch_points(
                    city,
                    target_date=target_date,
                    kind=kind,
                    models=open_meteo_models,
                )
            )

        if any(model.strip().lower() == "nws" for model in selected_models):
            try:
                points.append(self.nws.fetch_point(city, target_date=target_date, kind=kind))
            except Exception as exc:
                LOGGER.warning("NWS forecast failed", exc_info=exc)
                log_sync_event(
                    source="polymarket_weather",
                    action="fetch_nws",
                    status="fail",
                    details={"city": city.city_id, "date": target_date.isoformat()},
                    error=exc,
                )

        if not points:
            raise WeatherProviderError("No forecast points available from configured providers.")
        log_sync_event(
            source="polymarket_weather",
            action="fetch_ensemble",
            status="success",
            details={
                "city": city.city_id,
                "date": target_date.isoformat(),
                "kind": kind,
            },
            result={"models": [point.source_model for point in points]},
        )
        return EnsembleForecast(
            city=city,
            target_date=target_date,
            kind=kind,
            points=tuple(points),
        )
