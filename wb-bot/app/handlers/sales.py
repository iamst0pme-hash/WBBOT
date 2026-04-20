from __future__ import annotations

from pathlib import Path
from tempfile import gettempdir

from aiogram import F, Router
from aiogram.types import CallbackQuery, FSInputFile, Message

from app.keyboards.main import MAIN_MENU_KB, SALES_PERIOD_KB
from app.services.periods import build_period
from app.services.wb_client import WBApiError, WBClient
from app.services.xlsx_export import save_sales_report


router = Router()


@router.message(F.text == "/start")
async def cmd_start(message: Message) -> None:
    await message.answer(
        "Привет. Сейчас доступна одна кнопка: Продажи.",
        reply_markup=MAIN_MENU_KB,
    )


@router.callback_query(F.data == "sales_menu")
async def sales_menu(callback: CallbackQuery) -> None:
    await callback.answer()
    await callback.message.answer(
        "Выбери период для выгрузки продаж в XLS.",
        reply_markup=SALES_PERIOD_KB,
    )


@router.callback_query(F.data.startswith("sales_export:"))
async def sales_export(callback: CallbackQuery, wb_client: WBClient) -> None:
    await callback.answer("⏳ Запрос в обработке...")
    period_key = callback.data.split(":", 1)[1]

    try:
        period = build_period(period_key)
    except ValueError as exc:
        await callback.message.answer(f"❌ {exc}")
        return

    status = await callback.message.answer(
        f"⏳ Собираю продажи за период «{period.label}» и формирую XLS..."
    )

    try:
        rows = await wb_client.export_sales_report(
            current_start=period.current_start,
            current_end=period.current_end,
            past_start=period.past_start,
            past_end=period.past_end,
        )
    except WBApiError as exc:
        await status.edit_text(f"❌ Ошибка WB API: {exc}")
        return
    except Exception as exc:
        await status.edit_text(f"❌ Непредвиденная ошибка: {exc}")
        return

    tmp_dir = Path(gettempdir())
    file_path = tmp_dir / f"sales_{period.key}_{period.current_start.isoformat()}_{period.current_end.isoformat()}.xlsx"
    payload = [
        {
            "vendor_article": row.vendor_article,
            "orders_qty": row.orders_qty,
            "orders_sum": row.orders_sum,
            "stock_qty": row.stock_qty,
            "orders_sum_dynamic": row.orders_sum_dynamic,
        }
        for row in rows
    ]
    save_sales_report(payload, file_path)

    caption = (
        f"Готово. XLS за период «{period.label}».\n"
        f"Текущий период: {period.current_start:%d.%m.%Y} — {period.current_end:%d.%m.%Y}\n"
        f"Сравнение: {period.past_start:%d.%m.%Y} — {period.past_end:%d.%m.%Y}"
    )
    await callback.message.answer_document(FSInputFile(file_path), caption=caption)
    await status.delete()
