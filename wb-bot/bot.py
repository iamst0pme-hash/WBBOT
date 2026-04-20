import asyncio
import json
import os
import httpx
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import CommandStart

import wb_api
import ai_agent
from formatters import (
    format_sales, format_stock, format_campaigns,
    format_income_weeks, format_funnel, format_weekly_stats,
    format_ratings, format_abc,
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

def _load_users() -> set[int]:
    try:
        with open(USERS_FILE) as f:
            return set(json.load(f))
    except (FileNotFoundError, ValueError):
        return set()

def _save_users():
    with open(USERS_FILE, "w") as f:
        json.dump(list(user_chat_ids), f)

user_chat_ids: set[int] = _load_users()
ai_mode: set[int] = set()
_reply_states: dict[int, list] = {}  # chat_id → [feedback_dict, ...]

MENU_KB = InlineKeyboardMarkup(inline_keyboard=[
    [
        InlineKeyboardButton(text="📊 Продажи", callback_data="sales"),
        InlineKeyboardButton(text="📦 Склад", callback_data="stock"),
        InlineKeyboardButton(text="📢 Кампании", callback_data="campaigns"),
    ],
    [
        InlineKeyboardButton(text="💵 Приходы", callback_data="finance"),
        InlineKeyboardButton(text="📈 Воронка", callback_data="funnel"),
        InlineKeyboardButton(text="⭐ Рейтинг", callback_data="ratings"),
    ],
    [
        InlineKeyboardButton(text="🏆 ABC", callback_data="abc"),
        InlineKeyboardButton(text="💱 Курс валют", callback_data="rates"),
        InlineKeyboardButton(text="🤖 AI Директор", callback_data="ai"),
    ],
])

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
            await bot.send_message(chat_id, "📢 *Тестовая рассылка*\n\nЕсли ты видишь это сообщение — уведомления работают!", parse_mode="Markdown")
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
    msg = await call.message.answer("⏳ Загружаю данные по продажам...")
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            nm_ids = await wb_api.get_cards(client)
            rows = await wb_api.get_sales_history(client, nm_ids, wb_api.msk_date(2), wb_api.msk_date(1))
        text = format_sales(rows)
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
        weekly    = weekly    if isinstance(weekly, dict)  else {"current": {}, "previous": {}}
        funnel    = funnel    if isinstance(funnel, list)   else []
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

        # Отправляем каждый непрочитанный отзыв с кнопкой «Ответить»
        feedbacks = data.get("feedbacks") or []
        if feedbacks:
            _reply_states[chat_id] = [dict(f) for f in feedbacks[:5]]
            for i, f in enumerate(feedbacks[:5]):
                stars   = "⭐" * int(f.get("productValuation") or 0)
                product = f.get("subjectName") or f.get("productName") or "—"
                review  = (f.get("text") or "").strip()[:150]
                kb = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="✍️ Ответить", callback_data=f"reply_{i}")
                ]])
                await call.message.answer(
                    f"{stars} *{product}*\n_{review}_",
                    parse_mode="Markdown", reply_markup=kb,
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
        await call.message.answer("⚠️ Состояние устарело. Нажми ⭐ Рейтинг заново.")
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
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Отправить",       callback_data=f"send_reply_{idx}"),
            InlineKeyboardButton(text="🔄 Другой вариант",  callback_data=f"reply_{idx}"),
            InlineKeyboardButton(text="⏭ Пропустить",      callback_data=f"skip_reply_{idx}"),
        ]])
        await msg.edit_text(
            f"💬 *Черновик ответа:*\n\n{reply_text}",
            parse_mode="Markdown", reply_markup=kb,
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
        date = rates.get("date", "—")
        usd = rates.get("USD", "—")
        cny = rates.get("CNY", "—")
        text = (
            f"💱 *Курс валют ЦБ РФ на {date}*\n\n"
            f"🇺🇸 Доллар (USD): *{usd} ₽*\n"
            f"🇨🇳 Юань (CNY): *{cny} ₽*"
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
            f"🤖 *AI Директор*\n\n{answer}\n\n_Задай любой вопрос текстом. Для выхода нажми другую кнопку._",
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
    msg = await message.answer("🤖 Думаю...")
    try:
        # Если контекст магазина ещё не загружен — загружаем автоматически
        if not ai_agent.has_context(chat_id):
            await msg.edit_text("🤖 Загружаю данные магазина...")
            async with httpx.AsyncClient(timeout=90) as client:
                summary = await wb_api.get_ai_summary(client)
            ai_agent.set_context(chat_id, summary)

        answer = await ai_agent.ask(chat_id, message.text)
        await msg.delete()
        await message.answer(
            f"🤖 {answer}\n\n_Продолжай задавать вопросы или нажми кнопку меню._",
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
                        lines.append(f"🔴 {c['name']} — остаток *{c['balance']} ₽*")
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
