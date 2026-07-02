"""Temperature bucket parsing and construction."""

from __future__ import annotations

import re
from collections.abc import Iterable

from usstock.polymarket_weather.models import TemperatureBucket, TemperatureUnit
from usstock.polymarket_weather.units import normalize_unit


class BucketParseError(ValueError):
    """Raised when a market outcome cannot be converted to a temperature bucket."""


_NUMBER_RE = re.compile(r"-?\d+(?:\.\d+)?")
_RANGE_RE = re.compile(
    r"(?P<first>-?\d+(?:\.\d+)?)\s*(?:-|to|through|and|到|至)\s*"
    r"(?P<second>-?\d+(?:\.\d+)?)",
    re.IGNORECASE,
)


def _detect_unit(text: str, default_unit: TemperatureUnit) -> TemperatureUnit:
    lowered = text.lower()
    if "°c" in lowered or " celsius" in lowered or lowered.endswith(" c"):
        return "C"
    if "°f" in lowered or " fahrenheit" in lowered or lowered.endswith(" f"):
        return "F"
    return normalize_unit(default_unit)


def _with_integer_pad(low: float, high: float) -> tuple[float, float]:
    if low.is_integer() and high.is_integer():
        return low - 0.5, high + 0.5
    return low, high


def parse_temperature_bucket(
    label: str,
    *,
    default_unit: TemperatureUnit = "F",
) -> TemperatureBucket:
    """Parse common Polymarket outcome labels into continuous intervals.

    Integer labels are expanded by half a degree so that a quoted bucket like
    "85 to 86" captures integer settlement values 85 and 86.
    """

    original = label.strip()
    if not original:
        raise BucketParseError("Empty bucket label.")

    unit = _detect_unit(original, default_unit)
    text = original.lower()
    text = text.replace("degrees", "").replace("degree", "")
    text = text.replace("°f", "").replace("°c", "").replace("°", "")
    text = text.replace("fahrenheit", "").replace("celsius", "")
    text = text.replace(",", " ")
    text = re.sub(r"\s+", " ", text).strip()

    range_match = _RANGE_RE.search(text)
    if range_match:
        first = float(range_match.group("first"))
        second = float(range_match.group("second"))
        low, high = sorted((first, second))
        lower, upper = _with_integer_pad(low, high)
        return TemperatureBucket(label=original, lower=lower, upper=upper, unit=unit)

    numbers = [float(value) for value in _NUMBER_RE.findall(text)]
    if not numbers:
        raise BucketParseError(f"Cannot parse temperature bucket: {label}")

    value = numbers[0]
    below_tokens = (
        "below",
        "under",
        "less than",
        "lower",
        "or lower",
        "or below",
        "<",
        "≤",
        "不高于",
        "低于",
        "以下",
    )
    above_tokens = (
        "above",
        "over",
        "greater than",
        "higher",
        "or higher",
        "or above",
        "+",
        ">",
        "≥",
        "不低于",
        "高于",
        "以上",
    )

    if any(token in text for token in below_tokens):
        upper = value + 0.5 if value.is_integer() else value
        return TemperatureBucket(label=original, lower=None, upper=upper, unit=unit)

    if any(token in text for token in above_tokens) or text.endswith("+"):
        lower = value - 0.5 if value.is_integer() else value
        return TemperatureBucket(label=original, lower=lower, upper=None, unit=unit)

    lower, upper = _with_integer_pad(value, value)
    return TemperatureBucket(label=original, lower=lower, upper=upper, unit=unit)


def build_regular_buckets(
    *,
    start: int,
    end: int,
    step: int = 1,
    unit: TemperatureUnit = "F",
    include_tails: bool = True,
) -> tuple[TemperatureBucket, ...]:
    if step <= 0:
        raise ValueError("step must be positive.")
    if end < start:
        raise ValueError("end must be >= start.")

    buckets: list[TemperatureBucket] = []
    if include_tails:
        buckets.append(
            TemperatureBucket(
                label=f"{start - 1} or below",
                lower=None,
                upper=start - 0.5,
                unit=unit,
            )
        )
    value = start
    while value <= end:
        high = min(value + step - 1, end)
        label = f"{value}" if value == high else f"{value} to {high}"
        lower, upper = _with_integer_pad(float(value), float(high))
        buckets.append(TemperatureBucket(label=label, lower=lower, upper=upper, unit=unit))
        value += step
    if include_tails:
        buckets.append(
            TemperatureBucket(
                label=f"{end + 1} or above",
                lower=end + 0.5,
                upper=None,
                unit=unit,
            )
        )
    return tuple(buckets)


def unique_buckets(buckets: Iterable[TemperatureBucket]) -> tuple[TemperatureBucket, ...]:
    seen: set[str] = set()
    result: list[TemperatureBucket] = []
    for bucket in buckets:
        if bucket.canonical_key in seen:
            continue
        seen.add(bucket.canonical_key)
        result.append(bucket)
    return tuple(result)
