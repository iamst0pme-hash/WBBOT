from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(slots=True)
class Settings:
    bot_token: str
    wb_api_key: str

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        bot_token = (os.getenv("BOT_TOKEN") or "").strip()
        wb_api_key = (os.getenv("WB_API_KEY") or "").strip()

        missing: list[str] = []
        if not bot_token:
            missing.append("BOT_TOKEN")
        if not wb_api_key:
            missing.append("WB_API_KEY")

        if missing:
            raise RuntimeError(
                "Не заданы обязательные переменные окружения: " + ", ".join(missing)
            )

        return cls(bot_token=bot_token, wb_api_key=wb_api_key)
