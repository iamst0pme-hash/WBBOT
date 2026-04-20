from __future__ import annotations

from aiogram import F, Router
from aiogram.types import CallbackQuery, Message

from app.keyboards.main import MAIN_MENU_KB, SALES_PERIOD_KB
from app.services.periods import build_period
from app.services.wb_client import WBApiError, WBClient, SalesReport


router = Router()


def _fmt_num(value: float) -> str:
    rounded = round(float(value), 2)
    if abs(rounded - round(rounded)) < 1e-9:
        text = f"{int(round(rounded)):,}"
    else:
        text = f"{rounded:,.2f}"
    return text.replace(",", " ")


def _fmt_money(value: float) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{_fmt_num(value)} ₽"


def _fmt_percent(value: float) -> str:
    return f"{_fmt_num(value)}%"


def _build_sales_message(period, report: SalesReport) -> str:
    lines = [
        "<b>📊 Продажи</b>",
        f"Период: <b>{period.current_start:%d.%m.%Y} — {period.current_end:%d.%m.%Y}</b>",
        f"Сравнение: {period.past_start:%d.%m.%Y} — {period.past_end:%d.%m.%Y}",
        "",
        f"Заказы, шт: <b>{_fmt_num(report.total_orders_qty)}</b>",
        f"Заказы, сумма: <b>{_fmt_num(report.total_orders_sum)} ₽</b>",
        f"Остатки, шт: <b>{_fmt_num(report.total_stock_qty)}</b>",
        f"Рекламные расходы: <b>{_fmt_num(report.total_adv_sum)} ₽</b>",
        f"ДРР: <b>{_fmt_percent(report.total_drr)}</b>",
        f"Динамика заказов: <b>{_fmt_money(report.total_orders_sum_dynamic)}</b>",
    ]

    top_rows = report.rows[:5]
    if top_rows:
        lines.extend(["", "<b>Топ 5 артикулов</b>"])
        for idx, row in enumerate(top_rows, start=1):
            lines.extend(
                [
                    f"{idx}. <b>{row.vendor_article}</b>",
                    f"• Заказы: {_fmt_num(row.orders_qty)} шт",
                    f"• Сумма: {_fmt_num(row.orders_sum)} ₽",
                    f"• Остатки: {_fmt_num(row.stock_qty)} шт",
                    f"• Реклама: {_fmt_num(row.adv_sum)} ₽",
                    f"• ДРР: {_fmt_percent(row.drr)}",
                    f"• Динамика: {_fmt_money(row.orders_sum_dynamic)}",
                    "",
                ]
            )
        if lines[-1] == "":
            lines.pop()
    else:
        lines.extend(["", "Данных за выбранный период нет."])

    return "\n".join(lines)


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
        "Выбери период для отчёта по продажам.",
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
        f"⏳ Собираю продажи и рекламу за период «{period.label}»..."
    )

    try:
        report = await wb_client.get_sales_report(
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

    await status.edit_text(_build_sales_message(period, report))
