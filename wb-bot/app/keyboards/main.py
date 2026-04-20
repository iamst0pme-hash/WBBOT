from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

MAIN_MENU_KB = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="📈 Продажи", callback_data="sales_menu")],
    ]
)

SALES_PERIOD_KB = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="🌙 Вчера", callback_data="sales_export:yesterday")],
        [InlineKeyboardButton(text="📅 Последние 7 дней", callback_data="sales_export:last_7_days")],
        [InlineKeyboardButton(text="🗓 Прошлая неделя", callback_data="sales_export:last_week")],
    ]
)
