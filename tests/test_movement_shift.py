from datetime import datetime
from zoneinfo import ZoneInfo

from movement_shift import on_shift_worker_ids, suppress_for_assignees

ATHENS = ZoneInfo("Europe/Athens")
NOW = datetime(2026, 9, 28, 21, 6, tzinfo=ATHENS)


def _row(worker_id, start, finish, date="2026-09-28"):
    return {
        "Date": date,
        "Worker id": worker_id,
        "Actual start time": start,
        "Actual finish time": finish,
    }


def test_on_shift_when_started_and_not_finished() -> None:
    rows = [_row(10, "17:42:41", ""), _row(11, "08:16:10", "15:16:14")]
    assert on_shift_worker_ids(rows, NOW) == {10}


def test_overnight_shift_still_covers_after_midnight() -> None:
    late = datetime(2026, 9, 29, 1, 0, tzinfo=ATHENS)
    rows = [_row(10, "22:00:00", "02:00:00", date="2026-09-28")]
    assert on_shift_worker_ids(rows, late) == {10}
    assert on_shift_worker_ids(rows, datetime(2026, 9, 29, 3, 0, tzinfo=ATHENS)) == set()


def test_on_shift_ignores_other_days_and_blank_start() -> None:
    rows = [_row(10, "17:42:41", "", date="2026-09-27"), _row(12, "", "")]
    assert on_shift_worker_ids(rows, NOW) == set()


def test_suppress_only_when_assignee_is_on_shift() -> None:
    assert suppress_for_assignees([10], {10}) is True
    assert suppress_for_assignees([10], {11}) is False
    assert suppress_for_assignees([0, None], {10}) is False
    assert suppress_for_assignees([10], None) is False
