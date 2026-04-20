from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class PeriodDefinition:
    key: str
    title: str
    start: date
    end: date
    compare_start: date
    compare_end: date


def _today(tz_name: str) -> date:
    return datetime.now(ZoneInfo(tz_name)).date()


def get_period(period_key: str, tz_name: str) -> PeriodDefinition:
    today = _today(tz_name)
    yesterday = today - timedelta(days=1)

    if period_key == "yesterday":
        start = yesterday
        end = yesterday
        compare_start = start - timedelta(days=1)
        compare_end = end - timedelta(days=1)
        return PeriodDefinition(
            key=period_key,
            title="Вчера",
            start=start,
            end=end,
            compare_start=compare_start,
            compare_end=compare_end,
        )

    if period_key == "last_7_days":
        end = yesterday
        start = end - timedelta(days=6)
        compare_end = start - timedelta(days=1)
        compare_start = compare_end - timedelta(days=6)
        return PeriodDefinition(
            key=period_key,
            title="Последние 7 дней",
            start=start,
            end=end,
            compare_start=compare_start,
            compare_end=compare_end,
        )

    if period_key == "previous_week":
        # previous full Monday-Sunday week
        current_monday = today - timedelta(days=today.weekday())
        end = current_monday - timedelta(days=1)
        start = end - timedelta(days=6)
        compare_end = start - timedelta(days=1)
        compare_start = compare_end - timedelta(days=6)
        return PeriodDefinition(
            key=period_key,
            title="Прошлая неделя",
            start=start,
            end=end,
            compare_start=compare_start,
            compare_end=compare_end,
        )

    raise ValueError(f"Неизвестный период: {period_key}")
