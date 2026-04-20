from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    bot_token: str
    wb_analytics_token: str
    wb_content_token: str
    timezone_name: str = "Europe/Moscow"

    @classmethod
    def from_env(cls) -> "Settings":
        bot_token = os.getenv("BOT_TOKEN", "").strip()
        wb_analytics_token = os.getenv("WB_ANALYTICS_TOKEN", "").strip()
        wb_content_token = os.getenv("WB_CONTENT_TOKEN", "").strip()
        timezone_name = os.getenv("TZ", "Europe/Moscow").strip() or "Europe/Moscow"

        missing = [
            name
            for name, value in [
                ("BOT_TOKEN", bot_token),
                ("WB_ANALYTICS_TOKEN", wb_analytics_token),
                ("WB_CONTENT_TOKEN", wb_content_token),
            ]
            if not value
        ]
        if missing:
            raise RuntimeError(
                "Не заданы обязательные переменные окружения: " + ", ".join(missing)
            )

        return cls(
            bot_token=bot_token,
            wb_analytics_token=wb_analytics_token,
            wb_content_token=wb_content_token,
            timezone_name=timezone_name,
        )
