"""Temperature unit helpers."""

from __future__ import annotations

from usstock.polymarket_weather.models import TemperatureUnit


class UnitError(ValueError):
    """Raised when a temperature unit cannot be normalized."""


def normalize_unit(value: str | None, *, default: TemperatureUnit = "F") -> TemperatureUnit:
    text = (value or default).strip().upper()
    if text in {"C", "CELSIUS", "°C"}:
        return "C"
    if text in {"F", "FAHRENHEIT", "°F"}:
        return "F"
    raise UnitError(f"Unsupported temperature unit: {value}")


def c_to_f(value: float) -> float:
    return value * 9.0 / 5.0 + 32.0


def f_to_c(value: float) -> float:
    return (value - 32.0) * 5.0 / 9.0


def convert_temperature(
    value: float,
    *,
    from_unit: TemperatureUnit,
    to_unit: TemperatureUnit,
) -> float:
    if from_unit == to_unit:
        return value
    if from_unit == "C" and to_unit == "F":
        return c_to_f(value)
    if from_unit == "F" and to_unit == "C":
        return f_to_c(value)
    raise UnitError(f"Unsupported conversion: {from_unit} -> {to_unit}")
