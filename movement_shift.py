"""Skip unauthorized-movement alarms while a relocation task's assignee is on shift."""

from __future__ import annotations

from datetime import datetime, time, timedelta

MOVING_TASK_TYPE = 2
CLOSED_TASK_MINUTES = 30


def _cell(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() == "nan":
        return ""
    return text


def _parse_hms(value: object) -> time | None:
    text = _cell(value)
    if not text or text in {"0", "00:00:70"}:
        return None
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue
    return None


def _worker_id(value: object) -> int | None:
    text = _cell(value)
    if not text or text == "0":
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _covers_clock(start: time, finish: time | None, clock: time) -> bool:
    if finish is None or finish < start:
        return clock >= start or (finish is not None and clock <= finish)
    return start <= clock <= finish


def on_shift_worker_ids(rows, now: datetime) -> set[int]:
    """Workers whose actual shift covers `now`. Empty finish means still on shift."""
    today = now.date().isoformat()
    yesterday = (now.date() - timedelta(days=1)).isoformat()
    clock = now.time()
    workers: set[int] = set()
    for row in rows:
        worker_id = _worker_id(row.get("Worker id"))
        start = _parse_hms(row.get("Actual start time"))
        if worker_id is None or start is None:
            continue
        finish = _parse_hms(row.get("Actual finish time"))
        row_date = _cell(row.get("Date"))
        if row_date == today and _covers_clock(start, finish, clock):
            workers.add(worker_id)
        elif (
            row_date == yesterday
            and finish is not None
            and finish < start
            and clock <= finish
        ):
            workers.add(worker_id)
    return workers


def suppress_for_assignees(assignee_ids, on_shift: set[int] | None) -> bool:
    """Unknown schedule does not suppress: the alarm is still sent."""
    if on_shift is None:
        return False
    for assignee_id in assignee_ids:
        worker_id = _worker_id(assignee_id)
        if worker_id is not None and worker_id in on_shift:
            return True
    return False


def moving_task_assignees(engine_mysql, numbers: list) -> dict[str, list[int]]:
    import pandas as pd
    import sqlalchemy as sa

    clean = [str(number) for number in numbers if _cell(number) and _cell(number) != "0"]
    if not clean:
        return {}
    stmt = sa.text(
        """
        SELECT tb.number AS number, tt.assignee_id AS assignee_id
        FROM t_task tt
        JOIN t_bike tb ON tt.bike_id = tb.id
        WHERE tt.type = :task_type
          AND tb.number IN :numbers
          AND tt.assignee_id IS NOT NULL
          AND tt.assignee_id <> 0
          AND (
                tt.status IN (0, 1)
                OR (
                    tt.status = 2
                    AND tt.resolved >= DATE_SUB(NOW(), INTERVAL :closed_minutes MINUTE)
                )
          )
        """
    ).bindparams(sa.bindparam("numbers", expanding=True))
    df = pd.read_sql(
        stmt,
        engine_mysql,
        params={
            "task_type": MOVING_TASK_TYPE,
            "numbers": clean,
            "closed_minutes": CLOSED_TASK_MINUTES,
        },
    )
    grouped: dict[str, list[int]] = {}
    for _, row in df.iterrows():
        worker_id = _worker_id(row["assignee_id"])
        if worker_id is None:
            continue
        grouped.setdefault(str(row["number"]), []).append(worker_id)
    return grouped


def mark_alarm_1(engine, record_id: int, flag: str) -> None:
    import sqlalchemy as sa

    sql = sa.text("UPDATE damir.alarms_1 SET is_message_sent = :flag WHERE id = :id")
    with engine.connect() as connection:
        with connection.begin():
            connection.execute(sql, {"flag": flag, "id": int(record_id)})
