"""Hold the zone-exit alarm until the scooter is still outside 15 minutes later."""

from __future__ import annotations

import math

ZONE_EXIT_CONFIRM_MINUTES = 15


def zone_exit_action(
    age_minutes: int | None,
    outside: bool | None,
    *,
    confirm_minutes: int = ZONE_EXIT_CONFIRM_MINUTES,
) -> str:
    if age_minutes is None or age_minutes < confirm_minutes:
        return "wait"
    if outside is False:
        return "skip"
    return "send"


def decode_polyline(encoded: str) -> list[tuple[float, float]]:
    coords: list[tuple[float, float]] = []
    index = lat = lng = 0
    length = len(encoded)
    try:
        while index < length:
            lat, index = _read_delta(encoded, index, lat)
            lng, index = _read_delta(encoded, index, lng)
            coords.append((lat / 1e5, lng / 1e5))
    except IndexError:
        return []
    return coords


def _read_delta(encoded: str, index: int, value: int) -> tuple[int, int]:
    shift = result = 0
    while True:
        chunk = ord(encoded[index]) - 63
        index += 1
        result |= (chunk & 0x1F) << shift
        shift += 5
        if chunk < 0x20:
            break
    delta = ~(result >> 1) if result & 1 else result >> 1
    return value + delta, index


def point_in_polygon(lat: float, lng: float, polygon: list[tuple[float, float]]) -> bool:
    if len(polygon) < 3:
        return False
    inside = False
    j = len(polygon) - 1
    for i, (yi, xi) in enumerate(polygon):
        yj, xj = polygon[j]
        if ((yi > lat) != (yj > lat)) and (
            lng < (xj - xi) * (lat - yi) / ((yj - yi) or 1e-15) + xi
        ):
            inside = not inside
        j = i
    return inside


def still_outside_service(lat: object, lng: object, area_detail: object) -> bool | None:
    try:
        lat_f = float(lat)  # type: ignore[arg-type]
        lng_f = float(lng)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if math.isnan(lat_f) or math.isnan(lng_f):
        return None
    if abs(lat_f) < 1e-6 and abs(lng_f) < 1e-6:
        return None
    if not isinstance(area_detail, str) or not area_detail.strip():
        return None
    polygon = decode_polyline(area_detail.strip())
    if len(polygon) < 3:
        return None
    return not point_in_polygon(lat_f, lng_f, polygon)


def load_zone_exit_checks(engine_mysql, ids: list) -> dict[int, dict]:
    import pandas as pd

    clean = [int(raw) for raw in ids if raw is not None and not pd.isna(raw)]
    if not clean:
        return {}
    id_list = ",".join(str(item) for item in clean)
    df = pd.read_sql(
        f"""
        SELECT ta.id,
               TIMESTAMPDIFF(MINUTE, ta.created, NOW()) AS age_minutes,
               tb.g_lat,
               tb.g_lng,
               tc.area_detail
        FROM shamri.t_alert ta
        LEFT JOIN shamri.t_bike tb ON ta.bike_id = tb.id
        LEFT JOIN shamri.t_city tc ON tb.city_id = tc.id
        WHERE ta.id IN ({id_list})
        """,
        engine_mysql,
    )
    checks: dict[int, dict] = {}
    for _, row in df.iterrows():
        age = row["age_minutes"]
        checks[int(row["id"])] = {
            "age_minutes": None if pd.isna(age) else int(age),
            "outside": still_outside_service(row["g_lat"], row["g_lng"], row["area_detail"]),
            "lat": row["g_lat"],
            "lng": row["g_lng"],
        }
    return checks


def mark_alarm_6(engine, record_id: int, flag: str) -> None:
    import sqlalchemy as sa

    sql = sa.text("UPDATE damir.alarms_6 SET is_message_sent = :flag WHERE id = :id")
    with engine.connect() as connection:
        with connection.begin():
            connection.execute(sql, {"flag": flag, "id": int(record_id)})
