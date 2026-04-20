from __future__ import annotations

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Продажи", callback_data="sales")]
        ]
    )


def sales_period_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🌙 Вчера", callback_data="sales:period:yesterday")],
            [InlineKeyboardButton(text="🗓 Последние 7 дней", callback_data="sales:period:last_7_days")],
            [InlineKeyboardButton(text="📅 Прошлая неделя", callback_data="sales:period:previous_week")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:back")],
        ]
    )
