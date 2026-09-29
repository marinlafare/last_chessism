"""Resolve the reporting timezone used by player-hero analytics.

Game timestamps remain UTC. Country fallbacks use the geographical middle of
the country's IANA zones. When a country has an even number of zones, reports
use the arithmetic mean of the two middle zones for each timestamp.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


ZONE_TAB_PATHS = (
    Path("/usr/share/zoneinfo/zone.tab"),
    Path("/usr/share/lib/zoneinfo/tab/zone_sun.tab"),
)


@dataclass(frozen=True)
class CountryZone:
    country: str
    timezone: str
    longitude: float
    comment: str = ""


@dataclass(frozen=True)
class ResolvedPlayerTimezone:
    zones: tuple[str, ...]
    source: str
    country: str | None = None
    estimated: bool = False

    @property
    def left_zone(self) -> str:
        return self.zones[0]

    @property
    def right_zone(self) -> str:
        return self.zones[-1]

    @property
    def is_utc_fallback(self) -> bool:
        return self.source == "utc_fallback"

    @property
    def label(self) -> str:
        if len(self.zones) == 1:
            return self.zones[0]
        return f"mean({self.zones[0]}, {self.zones[1]})"

    def sql_params(self) -> dict[str, str]:
        return {
            "timezone_left": self.left_zone,
            "timezone_right": self.right_zone,
        }

    def local_boundary_to_utc(self, local_datetime: datetime) -> datetime:
        """Translate a local wall-clock boundary into its mean UTC instant."""
        instants = [
            local_datetime.replace(tzinfo=ZoneInfo(zone)).astimezone(timezone.utc)
            for zone in self.zones
        ]
        if len(instants) == 1:
            return instants[0]
        return instants[0] + ((instants[1] - instants[0]) / 2)

    def local_datetime(self, utc_datetime: datetime | None) -> datetime | None:
        if utc_datetime is None:
            return None
        if utc_datetime.tzinfo is None:
            utc_datetime = utc_datetime.replace(tzinfo=timezone.utc)
        converted = [utc_datetime.astimezone(ZoneInfo(zone)) for zone in self.zones]
        if len(converted) == 1:
            return converted[0]
        left = converted[0].replace(tzinfo=None)
        right = converted[1].replace(tzinfo=None)
        return left + ((right - left) / 2)

    def payload(self) -> dict[str, Any]:
        if self.is_utc_fallback:
            message = (
                "The player's timezone could not be determined; results are "
                "grouped in UTC."
            )
            time_basis = "utc"
        elif self.estimated:
            message = (
                "Local time is estimated from the geographical center of the "
                "player's country timezones."
            )
            time_basis = "estimated_local"
        elif self.source == "request_override":
            message = "Results use the timezone requested by the caller."
            time_basis = "requested"
        else:
            message = None
            time_basis = "player_local"
        return {
            "timezone": self.label,
            "component_timezones": list(self.zones),
            "source": self.source,
            "time_basis": time_basis,
            "is_player_local": time_basis == "player_local",
            "is_estimated": self.estimated,
            "message": message,
        }


def player_local_timestamp_sql(alias: str = "gp") -> str:
    """PostgreSQL expression for a resolved single/mean local wall clock."""
    return f"""(
        timezone(:timezone_left, {alias}.played_at)
        + (
            timezone(:timezone_right, {alias}.played_at)
            - timezone(:timezone_left, {alias}.played_at)
        ) / 2
    )"""


def _parse_longitude(coordinates: str) -> float:
    split_at = max(coordinates.find("+", 1), coordinates.find("-", 1))
    if split_at <= 0:
        raise ValueError(f"Invalid zone.tab coordinates: {coordinates!r}")
    raw = coordinates[split_at:]
    sign = -1 if raw.startswith("-") else 1
    digits = raw[1:]
    if len(digits) not in (5, 7):
        raise ValueError(f"Invalid zone.tab longitude: {raw!r}")
    degrees = int(digits[:3])
    minutes = int(digits[3:5])
    seconds = int(digits[5:7]) if len(digits) == 7 else 0
    return sign * (degrees + minutes / 60 + seconds / 3600)


@lru_cache(maxsize=1)
def country_zones() -> dict[str, tuple[CountryZone, ...]]:
    path = next((candidate for candidate in ZONE_TAB_PATHS if candidate.exists()), None)
    if path is None:
        return {}
    grouped: dict[str, dict[str, CountryZone]] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if not raw_line or raw_line.startswith("#"):
            continue
        parts = raw_line.split("\t")
        if len(parts) < 3:
            continue
        country, coordinates, zone = parts[:3]
        comment = parts[3] if len(parts) > 3 else ""
        try:
            longitude = _parse_longitude(coordinates)
            ZoneInfo(zone)
        except (ValueError, ZoneInfoNotFoundError):
            continue
        for country_code in country.split(","):
            normalized_country = country_code.strip().upper()
            grouped.setdefault(normalized_country, {})[zone] = CountryZone(
                country=normalized_country,
                timezone=zone,
                longitude=longitude,
                comment=comment,
            )
    return {
        country: tuple(sorted(zones.values(), key=lambda item: (item.longitude, item.timezone)))
        for country, zones in grouped.items()
    }


def _valid_zone(value: str | None) -> str | None:
    zone = str(value or "").strip()
    if not zone:
        return None
    try:
        ZoneInfo(zone)
    except ZoneInfoNotFoundError:
        return None
    return zone


def _normalized_location(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _zone_from_location(location: str | None, zones: tuple[CountryZone, ...]) -> str | None:
    normalized = _normalized_location(location)
    if not normalized:
        return None
    explicit = _valid_zone(location)
    if explicit:
        return explicit
    matches = []
    for item in zones:
        city = _normalized_location(item.timezone.rsplit("/", 1)[-1].replace("_", " "))
        if len(city) >= 4 and (city in normalized or normalized == city):
            matches.append(item.timezone)
    return matches[0] if len(set(matches)) == 1 else None


def resolve_player_timezone(
    player: dict[str, Any],
    requested_timezone: str | None = None,
) -> ResolvedPlayerTimezone:
    if requested_timezone:
        requested = _valid_zone(requested_timezone)
        if requested is None:
            raise ValueError("timezone must be a valid IANA timezone name")
        return ResolvedPlayerTimezone((requested,), "request_override")

    stored = _valid_zone(player.get("timezone"))
    if stored:
        return ResolvedPlayerTimezone(
            (stored,),
            str(player.get("timezone_source") or "player"),
            country=str(player.get("country") or "").upper() or None,
        )

    country = str(player.get("country") or "").strip().upper()
    zones = country_zones().get(country, ())
    location_zone = _zone_from_location(player.get("location"), zones)
    if location_zone:
        return ResolvedPlayerTimezone((location_zone,), "location", country=country)

    if zones:
        middle = len(zones) // 2
        components = (zones[middle].timezone,) if len(zones) % 2 else (
            zones[middle - 1].timezone,
            zones[middle].timezone,
        )
        return ResolvedPlayerTimezone(
            components,
            "country_center",
            country=country,
            estimated=True,
        )

    return ResolvedPlayerTimezone(("UTC",), "utc_fallback", country=country or None)
