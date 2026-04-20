from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(slots=True)
class Period:
    key: str
    label: str
    current_start: date
    current_end: date
    past_start: date
    past_end: date


def build_period(key: str, today: date | None = None) -> Period:
    today = today or date.today()
    yesterday = today - timedelta(days=1)

    if key == "yesterday":
        current_start = yesterday
        current_end = yesterday
        past_start = yesterday - timedelta(days=1)
        past_end = past_start
        label = "Вчера"
    elif key == "last_7_days":
        current_end = yesterday
        current_start = current_end - timedelta(days=6)
        past_end = current_start - timedelta(days=1)
        past_start = past_end - timedelta(days=6)
        label = "Последние 7 дней"
    elif key == "last_week":
        current_week_monday = today - timedelta(days=today.weekday())
        current_start = current_week_monday - timedelta(days=7)
        current_end = current_start + timedelta(days=6)
        past_start = current_start - timedelta(days=7)
        past_end = past_start + timedelta(days=6)
        label = "Прошлая неделя"
    else:
        raise ValueError(f"Неизвестный период: {key}")

    return Period(
        key=key,
        label=label,
        current_start=current_start,
        current_end=current_end,
        past_start=past_start,
        past_end=past_end,
    )
