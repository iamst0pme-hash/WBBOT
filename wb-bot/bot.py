import asyncio
import json
import os
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import httpx
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

import ai_agent
import wb_api
from formatters import (
    format_abc,
    format_campaigns,
    format_funnel,
    format_income_weeks,
    format_ratings,
    format_stock,
    format_weekly_stats,
)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
WB_API_KEY = os.getenv("WB_API_KEY")
WB_ADS_KEY = os.getenv("WB_ADS_KEY") or WB_API_KEY
WB_FINANCE_KEY = os.getenv("WB_FINANCE_KEY") or WB_API_KEY
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

wb_api.init(WB_API_KEY, WB_ADS_KEY, WB_FINANCE_KEY)
if GROQ_API_KEY:
    ai_agent.init_groq(GROQ_API_KEY)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

USERS_FILE = "users.json"
MSK = ZoneInfo("Europe/Moscow")


def _load_users() -> set[int]:
    try:
        with open(USERS_FILE, encoding="utf-8") as f:
            return set(json.load(f))
    except (FileNotFoundError, ValueError):
        return set()


def _save_users():
    with open(USERS_FILE, "w", encoding="utf-8") as f:
        json.dump(list(user_chat_ids), f)


user_chat_ids: set[int] = _load_users()
ai_mode: set[int] = set()
_reply_states: dict[int, list] = {}


MENU_KB = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="💰 Продажи", callback_data="sales"),
            InlineKeyboardButton(text="📦 Склад", callback_data="stock"),
            InlineKeyboardButton(text="📢 Кампании", callback_data="campaigns"),
        ],
        [
            InlineKeyboardButton(text="💵 Приходы", callback_data="finance"),
            InlineKeyboardButton(text="🛒 Воронка", callback_data="funnel"),
            InlineKeyboardButton(text="⭐ Рейтинг", callback_data="ratings"),
        ],
        [
            InlineKeyboardButton(text="📊 ABC", callback_data="abc"),
            InlineKeyboardButton(text="💱 Курс валют", callback_data="rates"),
            InlineKeyboardButton(text="🤖 AI Директор", callback_data="ai"),
        ],
    ]
)

SALES_PERIOD_KB = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="📅 За прошлый месяц", callback_data="sales_period:last_month")],
        [InlineKeyboardButton(text="📆 Этот месяц", callback_data="sales_period:this_month")],
        [InlineKeyboardButton(text="🗓 Неделя", callback_data="sales_period:last_week")],
        [InlineKeyboardButton(text="🌙 Вчера", callback_data="sales_period:yesterday")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="sales_back")],
    ]
)


async def refresh_kb(call: CallbackQuery):
    try:
        await call.message.edit_reply_markup(reply_markup=MENU_KB)
    except Exception:
        pass


def register_chat(chat_id: int, clear_ai: bool = True):
    user_chat_ids.add(chat_id)
    _save_users()
    if clear_ai:
        ai_mode.discard(chat_id)


def _msk_today() -> date:
    return datetime.now(MSK).date()


def _date_str(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def _label_dmy(d: date) -> str:
    return d.strftime("%d.%m.%Y")


def _period_label(start: date, end: date) -> str:
    if start == end:
        return _label_dmy(start)
    return f"{_label_dmy(start)} — {_label_dmy(end)}"


def _sales_periods(kind: str) -> dict:
    today = _msk_today()
    yesterday = today - timedelta(days=1)

    if kind == "yesterday":
        current_start = yesterday
        current_end = yesterday
        previous_start = yesterday - timedelta(days=1)
        previous_end = previous_start
        return {
            "title": "Продажи за вчера",
            "current_name": "Вчера",
            "previous_name": "Позавчера",
            "current_start": current_start,
            "current_end": current_end,
            "previous_start": previous_start,
            "previous_end": previous_end,
        }

    if kind == "last_week":
        this_week_monday = today - timedelta(days=today.weekday())
        current_start = this_week_monday - timedelta(days=7)
        current_end = current_start + timedelta(days=6)
        previous_start = current_start - timedelta(days=7)
        previous_end = previous_start + timedelta(days=6)
        return {
            "title": "Продажи за прошлую неделю",
            "current_name": "Прошлая неделя",
            "previous_name": "Неделей ранее",
            "current_start": current_start,
            "current_end": current_end,
            "previous_start": previous_start,
            "previous_end": previous_end,
        }

    if kind == "this_month":
        current_start = today.replace(day=1)
        current_end = yesterday
        if current_end < current_start:
            raise ValueError("Недостаточно данных: сегодня первый день месяца.")
        prev_month_last_day = current_start - timedelta(days=1)
        prev_month_start = prev_month_last_day.replace(day=1)
        days_count = (current_end - current_start).days + 1
        previous_start = prev_month_start
        previous_end = previous_start + timedelta(days=days_count - 1)
        if previous_end > prev_month_last_day:
            previous_end = prev_month_last_day
        return {
            "title": "Продажи за текущий месяц",
            "current_name": "Этот месяц",
            "previous_name": "Аналогичный период прошлого месяца",
            "current_start": current_start,
            "current_end": current_end,
            "previous_start": previous_start,
            "previous_end": previous_end,
        }

    if kind == "last_month":
        first_day_this_month = today.replace(day=1)
        last_day_prev_month = first_day_this_month - timedelta(days=1)
        current_start = last_day_prev_month.replace(day=1)
        current_end = last_day_prev_month
        prev_prev_month_last_day = current_start - timedelta(days=1)
        previous_start = prev_prev_month_last_day.replace(day=1)
        previous_end = prev_prev_month_last_day
        return {
            "title": "Продажи за прошлый месяц",
            "current_name": "Прошлый месяц",
            "previous_name": "Месяцем ранее",
            "current_start": current_start,
            "current_end": current_end,
            "previous_start": previous_start,
            "previous_end": previous_end,
        }

    raise ValueError("Неизвестный период продаж.")


def _to_float(value) -> float:
    if value is None or value == "":
        return 0.0
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        value = value.replace(" ", "").replace(",", ".")
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


def _to_int(value) -> int:
    return int(round(_to_float(value)))


def _pick_first(row: dict, keys: list[str], default=None):
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return default


def _extract_sale_date(row: dict) -> str:
    raw = _pick_first(
        row,
        [
            "date",
            "saleDate",
            "lastChangeDate",
            "rrdDate",
            "statisticsDate",
            "day",
        ],
        "",
    )
    if not raw:
        return "—"
    return str(raw)[:10]


def _aggregate_sales(rows: list[dict]) -> dict:
    totals = {
        "orders": 0,
        "sales_qty": 0,
        "revenue": 0.0,
        "returns_qty": 0,
        "returns_sum": 0.0,
        "items": {},
    }

    for row in rows or []:
        orders = _to_int(_pick_first(row, ["ordersCount", "orders", "ordersSum"], 0))
        sales_qty = _to_int(_pick_first(row, ["saleCount", "salesCount", "quantity", "forPayCount"], 0))
        returns_qty = _to_int(_pick_first(row, ["returnCount", "returnsCount", "refundCount"], 0))

        revenue = _to_float(
            _pick_first(
                row,
                ["forPay", "forPaySum", "saleSum", "retailAmount", "finishedPrice", "priceWithDisc"],
                0,
            )
        )
        returns_sum = _to_float(_pick_first(row, ["returnSum", "refundAmount", "returnsSum"], 0))

        totals["orders"] += orders
        totals["sales_qty"] += sales_qty
        totals["revenue"] += revenue
        totals["returns_qty"] += returns_qty
        totals["returns_sum"] += returns_sum

        nm_id = str(_pick_first(row, ["nmId", "nmID", "subjectID", "supplierArticle", "barcode"], "—"))
        name = str(
            _pick_first(
                row,
                ["subjectName", "brandName", "vendorCode", "supplierArticle", "techSize", "nmId"],
                "Товар",
            )
        )
        item = totals["items"].setdefault(
            nm_id,
            {
                "name": name,
                "revenue": 0.0,
                "qty": 0,
                "orders": 0,
            },
        )
        item["name"] = name or item["name"]
        item["revenue"] += revenue
        item["qty"] += sales_qty
        item["orders"] += orders

    top_items = sorted(
        totals["items"].values(),
        key=lambda x: (x["revenue"], x["qty"], x["orders"]),
        reverse=True,
    )[:5]

    totals["top_items"] = top_items
    return totals


def _fmt_money(value: float) -> str:
    return f"{value:,.0f} ₽".replace(",", " ")


def _fmt_delta(current: float, previous: float, is_money: bool = False) -> str:
    diff = current - previous
    if previous == 0:
        pct = "0%" if current == 0 else "—"
    else:
        pct = f"{(diff / previous) * 100:+.1f}%"

    if is_money:
        diff_text = _fmt_money(diff)
        if diff > 0:
            diff_text = f"+{diff_text}"
    else:
        diff_text = f"{diff:+.0f}"

    return f"{diff_text} ({pct})"


def _build_sales_text(period_cfg: dict, current_rows: list[dict], previous_rows: list[dict]) -> str:
    current = _aggregate_sales(current_rows)
    previous = _aggregate_sales(previous_rows)

    current_period = _period_label(period_cfg["current_start"], period_cfg["current_end"])
    previous_period = _period_label(period_cfg["previous_start"], period_cfg["previous_end"])

    lines = [
        f"📈 *{period_cfg['title']}*",
        "",
        f"*{period_cfg['current_name']}*: {current_period}",
        f"*{period_cfg['previous_name']}*: {previous_period}",
        "",
        "📊 *Текущий период*",
        f"• Выручка: *{_fmt_money(current['revenue'])}*",
        f"• Заказы: *{current['orders']}*",
        f"• Продано шт.: *{current['sales_qty']}*",
        f"• Возвраты: *{current['returns_qty']} шт. / {_fmt_money(current['returns_sum'])}*",
        "",
        "↔️ *Сравнение с прошлым периодом*",
        f"• Выручка: {_fmt_delta(current['revenue'], previous['revenue'], is_money=True)}",
        f"• Заказы: {_fmt_delta(current['orders'], previous['orders'])}",
        f"• Продано шт.: {_fmt_delta(current['sales_qty'], previous['sales_qty'])}",
        f"• Возвраты, шт.: {_fmt_delta(current['returns_qty'], previous['returns_qty'])}",
        "",
        "🕘 *Прошлый период*",
        f"• Выручка: *{_fmt_money(previous['revenue'])}*",
        f"• Заказы: *{previous['orders']}*",
        f"• Продано шт.: *{previous['sales_qty']}*",
        f"• Возвраты: *{previous['returns_qty']} шт. / {_fmt_money(previous['returns_sum'])}*",
    ]

    if current["top_items"]:
        lines.extend(["", "🏆 *Топ-5 товаров текущего периода*"])
        for i, item in enumerate(current["top_items"], start=1):
            lines.append(f"{i}. *{item['name']}* — {_fmt_money(item['revenue'])}, {item['qty']} шт.")

    if current_rows or previous_rows:
        lines.extend(
            [
                "",
                "🧾 *Техническая справка*",
                f"• Строк в текущем периоде: {len(current_rows)}",
                f"• Строк в прошлом периоде: {len(previous_rows)}",
            ]
        )
        if current_rows:
            lines.append(f"• Первая дата в текущей выборке: {_extract_sale_date(current_rows[0])}")
        if previous_rows:
            lines.append(f"• Первая дата в прошлой выборке: {_extract_sale_date(previous_rows[0])}")

    return "\n".join(lines)


@dp.message(CommandStart())
async def cmd_start(message: Message):
    register_chat(message.chat.id)
    await message.answer("Привет! Выбери отчет.", reply_markup=MENU_KB)


@dp.message(F.text == "/all")
async def cmd_all(message: Message):
    if not user_chat_ids:
        await message.answer("Нет пользователей в базе.")
        return

    count = 0
    for chat_id in user_chat_ids:
        try:
            await bot.send_message(
                chat_id,
                "📢 *Тестовая рассылка*\n\nЕсли ты видишь это сообщение — уведомления работают!",
                parse_mode="Markdown",
            )
            count += 1
        except Exception:
            pass

    await message.answer(f"Отправлено {count} из {len(user_chat_ids)} пользователей.")


# ─── ПРОДАЖИ ─────────────────────────────────────────────────────────────────


@dp.callback_query(F.data == "sales")
async def cb_sales(call: CallbackQuery):
    await call.answer()
    await refresh_kb(call)
    register_chat(call.message.chat.id)
    await call.message.answer("Выбери период для отчёта по продажам:", reply_markup=SALES_PERIOD_KB)


@dp.callback_query(F.data == "sales_back")
async def cb_sales_back(call: CallbackQuery):
    await call.answer()
    try:
        await call.message.edit_text("Главное меню:", reply_markup=MENU_KB)
    except Exception:
        await call.message.answer("Главное меню:", reply_markup=MENU_KB)


@dp.callback_query(F.data.startswith("sales_period:"))
async def cb_sales_period(call: CallbackQuery):
    await call.answer()
    register_chat(call.message.chat.id)

    period_key = call.data.split(":", 1)[1]
    try:
        period_cfg = _sales_periods(period_key)
    except Exception as e:
        await call.message.answer(f"❌ Ошибка периода: {e}", reply_markup=MENU_KB)
        return

    msg = await call.message.answer("⏳ Загружаю данные по продажам...")
    try:
        async with httpx.AsyncClient(timeout=90) as client:
            nm_ids = await wb_api.get_cards(client)

            current_rows, previous_rows = await asyncio.gather(
                wb_api.get_sales_history(
                    client,
                    nm_ids,
                    _date_str(period_cfg["current_start"]),
                    _date_str(period_cfg["current_end"]),
                ),
                wb_api.get_sales_history(
                    client,
                    nm_ids,
                    _date_str(period_cfg["previous_start"]),
                    _date_str(period_cfg["previous_end"]),
                ),
            )

        text = _build_sales_text(period_cfg, current_rows or [], previous_rows or [])
        await msg.delete()
        await call.message.answer(text, parse_mode="Markdown", reply_markup=MENU_KB)
    except Exception as e:
        await msg.delete()
        await call.message.answer(f"❌ Ошибка: {e}", reply_markup=MENU_KB)


# ─── СКЛАД ────────────────────────────────────────────────────────────────────


@dp.callback_query(F.data == "stock")
async def cb_stock(call: CallbackQuery):
    await call.answer()
    await refresh_kb(call)
    register_chat(call.message.chat.id)
    msg = await call.message.answer("⏳ Формирую отчёт по складу (~30 сек)...")
    try:
        async with httpx.AsyncClient(timeout=180) as client:
            items = await wb_api.get_stock_report(client)
        text = format_stock(items)
        await msg.delete()
        await call.message.answer(text, reply_markup=MENU_KB)
    except Exception as e:
        await msg.delete()
        await call.message.answer(f"❌ Ошибка: {e}", reply_markup=MENU_KB)


# ─── КАМПАНИИ ────────────────────────────────────────────────────────────────


@dp.callback_query(F.data == "campaigns")
async def cb_campaigns(call: CallbackQuery):
    await call.answer()
    await refresh_kb(call)
    register_chat(call.message.chat.id)
    msg = await call.message.answer("⏳ Загружаю кампании...")
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            campaigns = await wb_api.get_active_campaigns(client)
        text = format_campaigns(campaigns)
        await msg.delete()
        await call.message.answer(text, parse_mode="Markdown", reply_markup=MENU_KB)
    except Exception as e:
        await msg.delete()
        await call.message.answer(f"❌ Ошибка: {e}", reply_markup=MENU_KB)


# ─── ФИНАНСЫ ─────────────────────────────────────────────────────────────────


@dp.callback_query(F.data == "finance")
async def cb_finance(call: CallbackQuery):
    await call.answer()
    await refresh_kb(call)
    register_chat(call.message.chat.id)
    msg = await call.message.answer("⏳ Загружаю приходы за 4 недели...")
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            reports = await wb_api.get_weekly_payments(client)
        text = format_income_weeks(reports)
        await msg.delete()
        await call.message.answer(text, parse_mode="Markdown", reply_markup=MENU_KB)
    except Exception as e:
        await msg.delete()
        await call.message.answer(f"❌ Ошибка: {e}", reply_markup=MENU_KB)


# ─── ВОРОНКА ─────────────────────────────────────────────────────────────────


@dp.callback_query(F.data == "funnel")
async def cb_funnel(call: CallbackQuery):
    await call.answer()
    await refresh_kb(call)
    register_chat(call.message.chat.id)
    msg = await call.message.answer("⏳ Собираю недельный отчёт...")
    try:
        async with httpx.AsyncClient(timeout=90) as client:
            nm_ids = await wb_api.get_cards(client)
            weekly, funnel, campaigns = await asyncio.gather(
                wb_api.get_weekly_article_stats(client, nm_ids),
                wb_api.get_funnel(client, nm_ids),
                wb_api.get_active_campaigns(client),
                return_exceptions=True,
            )

        weekly = weekly if isinstance(weekly, dict) else {"current": {}, "previous": {}}
        funnel = funnel if isinstance(funnel, list) else []
        campaigns = campaigns if isinstance(campaigns, list) else []
        text = format_weekly_stats(weekly, funnel, campaigns)

        await msg.delete()
        await call.message.answer(text, parse_mode="Markdown", reply_markup=MENU_KB)
    except Exception as e:
        await msg.delete()
        await call.message.answer(f"❌ Ошибка: {e}", reply_markup=MENU_KB)


# ─── РЕЙТИНГ ─────────────────────────────────────────────────────────────────


@dp.callback_query(F.data == "ratings")
async def cb_ratings(call: CallbackQuery):
    await call.answer()
    await refresh_kb(call)
    chat_id = call.message.chat.id
    register_chat(chat_id)
    msg = await call.message.answer("⏳ Загружаю отзывы...")
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            data = await wb_api.get_ratings(client)

        text = format_ratings(data)
        await msg.delete()
        await call.message.answer(text, parse_mode="Markdown", reply_markup=MENU_KB)

        feedbacks = data.get("feedbacks") or []
        if feedbacks:
            _reply_states[chat_id] = [dict(f) for f in feedbacks[:5]]
            for i, f in enumerate(feedbacks[:5]):
                stars = "⭐" * int(f.get("productValuation") or 0)
                product = f.get("subjectName") or f.get("productName") or "—"
                review = (f.get("text") or "").strip()[:150]
                kb = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="✍️ Ответить", callback_data=f"reply_{i}")]
                    ]
                )
                await call.message.answer(
                    f"{stars} *{product}*\n_{review}_",
                    parse_mode="Markdown",
                    reply_markup=kb,
                )
    except Exception as e:
        await msg.delete()
        await call.message.answer(f"❌ Ошибка: {e}", reply_markup=MENU_KB)


# ─── ОТВЕТЫ НА ОТЗЫВЫ ────────────────────────────────────────────────────────


@dp.callback_query(F.data.startswith("reply_"))
async def cb_reply(call: CallbackQuery):
    await call.answer()
    chat_id = call.message.chat.id
    try:
        idx = int(call.data.split("_")[1])
    except (IndexError, ValueError):
        return

    feedbacks = _reply_states.get(chat_id, [])
    if idx >= len(feedbacks):
        await call.message.answer("⚠️ Состояние устарело.\nНажми ⭐ Рейтинг заново.")
        return

    f = feedbacks[idx]
    msg = await call.message.answer("🤖 Генерирую ответ...")
    try:
        reply_text = await ai_agent.generate_feedback_reply(
            product=f.get("subjectName") or f.get("productName") or "товар",
            rating=int(f.get("productValuation") or 5),
            review=f.get("text") or "",
            article=f.get("_vendorCode") or "",
        )
        feedbacks[idx]["_draft"] = reply_text

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Отправить", callback_data=f"send_reply_{idx}"),
                    InlineKeyboardButton(text="🔄 Другой вариант", callback_data=f"reply_{idx}"),
                    InlineKeyboardButton(text="⏭ Пропустить", callback_data=f"skip_reply_{idx}"),
                ]
            ]
        )

        await msg.edit_text(
            f"📝 *Черновик ответа:*\n\n{reply_text}",
            parse_mode="Markdown",
            reply_markup=kb,
        )
    except Exception as e:
        await msg.edit_text(f"❌ Ошибка генерации: {e}")


@dp.callback_query(F.data.startswith("send_reply_"))
async def cb_send_reply(call: CallbackQuery):
    await call.answer()
    chat_id = call.message.chat.id
    try:
        idx = int(call.data.split("_")[2])
    except (IndexError, ValueError):
        return

    feedbacks = _reply_states.get(chat_id, [])
    if idx >= len(feedbacks):
        await call.message.edit_text("⚠️ Состояние устарело.")
        return

    f = feedbacks[idx]
    draft = f.get("_draft", "")
    feedback_id = f.get("id") or f.get("feedbackId") or ""
    if not feedback_id:
        await call.message.edit_text("❌ Не найден ID отзыва.")
        return

    try:
        async with httpx.AsyncClient() as client:
            await wb_api.reply_to_feedback(client, feedback_id, draft)
        await call.message.edit_text("✅ *Ответ отправлен!*", parse_mode="Markdown")
    except Exception as e:
        await call.message.edit_text(f"❌ Ошибка отправки:\n`{e}`", parse_mode="Markdown")


@dp.callback_query(F.data.startswith("skip_reply_"))
async def cb_skip_reply(call: CallbackQuery):
    await call.answer("Пропущено")
    await call.message.edit_reply_markup(reply_markup=None)


# ─── КУРС ВАЛЮТ ──────────────────────────────────────────────────────────────


@dp.callback_query(F.data == "rates")
async def cb_rates(call: CallbackQuery):
    await call.answer()
    await refresh_kb(call)
    register_chat(call.message.chat.id)
    msg = await call.message.answer("⏳ Загружаю курс валют...")
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            rates = await wb_api.get_exchange_rates(client)

        date_value = rates.get("date", "—")
        usd = rates.get("USD", "—")
        cny = rates.get("CNY", "—")
        text = (
            f"💱 *Курс валют ЦБ РФ на {date_value}*\n\n"
            f"• Доллар (USD): *{usd} ₽*\n"
            f"• Юань (CNY): *{cny} ₽*"
        )

        await msg.delete()
        await call.message.answer(text, parse_mode="Markdown", reply_markup=MENU_KB)
    except Exception as e:
        await msg.delete()
        await call.message.answer(f"❌ Ошибка: {e}", reply_markup=MENU_KB)


# ─── ABC ─────────────────────────────────────────────────────────────────────


@dp.callback_query(F.data == "abc")
async def cb_abc(call: CallbackQuery):
    await call.answer()
    await refresh_kb(call)
    register_chat(call.message.chat.id)
    msg = await call.message.answer("⏳ Считаю ABC-анализ за 30 дней...")
    try:
        async with httpx.AsyncClient(timeout=90) as client:
            nm_ids = await wb_api.get_cards(client)
            products = await wb_api.get_abc(client, nm_ids)

        text = format_abc(products)
        await msg.delete()
        await call.message.answer(text, parse_mode="Markdown", reply_markup=MENU_KB)
    except Exception as e:
        await msg.delete()
        await call.message.answer(f"❌ Ошибка: {e}", reply_markup=MENU_KB)


# ─── AI ДИРЕКТОР ─────────────────────────────────────────────────────────────


@dp.callback_query(F.data == "ai")
async def cb_ai(call: CallbackQuery):
    await call.answer()
    await refresh_kb(call)
    chat_id = call.message.chat.id
    register_chat(chat_id, clear_ai=False)
    ai_mode.add(chat_id)
    msg = await call.message.answer("🤖 Собираю данные магазина для анализа...")
    try:
        async with httpx.AsyncClient(timeout=90) as client:
            summary = await wb_api.get_ai_summary(client)
        ai_agent.set_context(chat_id, summary)

        await msg.edit_text("🤖 Анализирую данные...")
        answer = await ai_agent.ask(chat_id, "")
        await msg.delete()
        await call.message.answer(
            f"🧠 *AI Директор*\n\n{answer}\n\n_Задай любой вопрос текстом. Для выхода нажми другую кнопку._",
            parse_mode="Markdown",
            reply_markup=MENU_KB,
        )
    except Exception as e:
        await msg.delete()
        ai_mode.discard(chat_id)
        await call.message.answer(f"❌ Ошибка: {e}", reply_markup=MENU_KB)


@dp.message(F.text)
async def handle_text(message: Message):
    chat_id = message.chat.id
    register_chat(chat_id, clear_ai=False)
    msg = await message.answer("🤔 Думаю...")
    try:
        if not ai_agent.has_context(chat_id):
            await msg.edit_text("📥 Загружаю данные магазина...")
            async with httpx.AsyncClient(timeout=90) as client:
                summary = await wb_api.get_ai_summary(client)
            ai_agent.set_context(chat_id, summary)

        answer = await ai_agent.ask(chat_id, message.text)
        await msg.delete()
        await message.answer(
            f"💬 {answer}\n\n_Продолжай задавать вопросы или нажми кнопку меню._",
            parse_mode="Markdown",
            reply_markup=MENU_KB,
        )
    except Exception as e:
        await msg.delete()
        await message.answer(f"❌ Ошибка AI: {e}", reply_markup=MENU_KB)


# ─── ФОНОВАЯ ПРОВЕРКА БЮДЖЕТОВ ───────────────────────────────────────────────


async def check_budgets_loop():
    await asyncio.sleep(60)
    while True:
        try:
            if user_chat_ids:
                async with httpx.AsyncClient(timeout=60) as client:
                    campaigns = await wb_api.get_active_campaigns(client)

                low = [c for c in campaigns if c.get("balance", 0) < 100]
                if low:
                    lines = ["⚠️ *НИЗКИЙ БАЛАНС КАМПАНИИ*", ""]
                    for c in low:
                        lines.append(f"• {c['name']} — остаток *{c['balance']} ₽*")
                    lines.append("\nПополни рекламный кабинет WB!")
                    text = "\n".join(lines)

                    for chat_id in user_chat_ids:
                        try:
                            await bot.send_message(chat_id, text, parse_mode="Markdown")
                        except Exception:
                            pass
        except Exception as e:
            print(f"[budget check error] {e}")

        await asyncio.sleep(30 * 60)


async def main():
    asyncio.create_task(check_budgets_loop())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
