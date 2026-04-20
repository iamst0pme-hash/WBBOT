from __future__ import annotations

import asyncio
import logging
from datetime import date

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import BufferedInputFile, CallbackQuery, Message

from .config import Settings
from .keyboards import main_menu_keyboard, sales_period_keyboard
from .periods import PeriodDefinition, get_period
from .reporting import build_sales_workbook
from .wb_client import WBClient, WBApiError


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


def period_label(period: PeriodDefinition) -> str:
    return (
        f"{period.title} "
        f"({period.start.strftime('%d.%m.%Y')} — {period.end.strftime('%d.%m.%Y')})"
    )


def progress_text(period: PeriodDefinition) -> str:
    return (
        "⏳ Запрос в обработке...\n\n"
        f"Период: {period_label(period)}\n"
        f"Сравнение: {period.compare_start.strftime('%d.%m.%Y')} — {period.compare_end.strftime('%d.%m.%Y')}\n\n"
        "Собираю карточки, историю продаж и остатки WB.\n"
        "Если товаров много, выгрузка может занять время из-за лимитов Wildberries."
    )


async def main() -> None:
    settings = Settings.from_env()
    bot = Bot(token=settings.bot_token)
    dp = Dispatcher()
    wb_client = WBClient(
        analytics_token=settings.wb_analytics_token,
        content_token=settings.wb_content_token,
    )

    @dp.message(CommandStart())
    async def cmd_start(message: Message) -> None:
        await message.answer(
            "Привет. Это временный бот для выгрузки продаж в Excel через WB history.",
            reply_markup=main_menu_keyboard(),
        )

    @dp.callback_query(F.data == "menu:back")
    async def cb_back(callback: CallbackQuery) -> None:
        await callback.message.edit_text(
            "Главное меню",
            reply_markup=main_menu_keyboard(),
        )
        await callback.answer()

    @dp.callback_query(F.data == "sales")
    async def cb_sales(callback: CallbackQuery) -> None:
        await callback.message.edit_text(
            "Выбери период для выгрузки продаж.",
            reply_markup=sales_period_keyboard(),
        )
        await callback.answer()

    @dp.callback_query(F.data.startswith("sales:period:"))
    async def cb_sales_period(callback: CallbackQuery) -> None:
        period_key = callback.data.split(":")[-1]
        period = get_period(period_key, settings.timezone_name)

        await callback.answer("Запрос принят")
        status_message = callback.message
        await status_message.edit_text(progress_text(period))

        try:
            rows = await wb_client.fetch_sales_rows(
                start=period.start,
                end=period.end,
                compare_start=period.compare_start,
                compare_end=period.compare_end,
            )

            xlsx_path = build_sales_workbook(rows=rows, period_label=period_label(period))
            file_bytes = xlsx_path.read_bytes()
            filename = (
                f"sales_{period.key}_{period.start.isoformat()}_{period.end.isoformat()}.xlsx"
            )

            await callback.message.answer_document(
                document=BufferedInputFile(file=file_bytes, filename=filename),
                caption=(
                    "Готово. Это временная выгрузка для проверки структуры данных.\n"
                    "Следующим шагом этот Excel можно будет заменить на готовый отчёт."
                ),
            )
            await status_message.edit_text(
                "✅ Файл сформирован и отправлен.",
                reply_markup=sales_period_keyboard(),
            )
        except WBApiError as exc:
            logger.exception("WB API error while building sales export")
            await status_message.edit_text(
                "❌ Ошибка WB API:\n"
                f"{exc}"
            )
        except Exception as exc:
            logger.exception("Unexpected error while building sales export")
            await status_message.edit_text(
                "❌ Непредвиденная ошибка при формировании файла:\n"
                f"{exc}"
            )

    try:
        await dp.start_polling(bot)
    finally:
        await wb_client.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
