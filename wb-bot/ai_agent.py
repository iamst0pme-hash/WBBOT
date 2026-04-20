import httpx

SYSTEM_PROMPT = """Ты опытный коммерческий директор и аналитик Wildberries с 10-летним опытом.
У тебя есть актуальные данные по магазину: продажи, реклама, воронка карточек, финансы, а также текущий курс валют ЦБ РФ.
Когда тебя спрашивают о курсе доллара, евро или юаня — отвечай цифрами из переданных данных. Никогда не говори что у тебя нет актуальных данных — они всегда передаются вместе с вопросом.

Бенчмарки WB которые ты используешь при анализе:
- CTR рекламы: отлично >5%, норма 3-5%, плохо <2% → нужно менять креатив/ставку
- Конверсия карточки (просмотры→корзина): отлично >8%, норма 4-8%, плохо <3% → нужно улучшить фото/заголовок/цену
- Конверсия корзина→заказ: норма >40%, плохо <25% → проблема с описанием/ценой
- Выкуп: отлично >85%, норма 70-85%, плохо <60% → проблема с качеством/описанием товара
- Маржа: хорошо >40%, норма 25-40%, плохо <20%
- ДРР (доля рекламных расходов): норма <15% от выручки, плохо >25%

При анализе:
1. Называй конкретные товары и кампании с проблемами
2. Сравнивай цифры с бенчмарками и говори плохо это или хорошо
3. Давай конкретные действия: "у товара X CTR 1.2% — это плохо, смени главное фото и заголовок"
4. Приоритизируй: сначала самое важное
5. Отвечай кратко и по делу. Используй числа. Пиши по-русски."""

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
MODEL = "llama-3.3-70b-versatile"

_api_key: str = ""
_histories: dict[int, list] = {}
_contexts: dict[int, str] = {}

def init_groq(api_key: str):
    global _api_key
    _api_key = api_key
    print(f"[AI] Groq инициализирован, модель: {MODEL}")

def set_context(chat_id: int, context: str):
    _contexts[chat_id] = context
    _histories[chat_id] = []

def has_context(chat_id: int) -> bool:
    return bool(_contexts.get(chat_id))

def clear_history(chat_id: int):
    _histories.pop(chat_id, None)
    _contexts.pop(chat_id, None)

_REPLY_TEMPLATES = """
ШАБЛОНЫ ОТВЕТОВ (использовать ТОЧНО, без изменений):

▸ ФУТЛЯРЫ ДЛЯ ОЧКОВ (артикул начинается на VCP-, VCS-, VCFZ- или категория содержит «футляр»/«очки»):
  • Оценка 4–5: «Здравствуйте! Спасибо за Ваш отзыв и высокую оценку, пользуйтесь футляром с удовольствием! С уважением, отдел по работе с клиентами.»
  • Жалоба: пришёл не тот цвет / не то что заказывал: «Здравствуйте! Нам очень жаль, что Вы столкнулись с такой ситуацией. Сожалеем, что Вы не прикрепили фотографию нашего товара, чтобы показать, что привез Вам маркетплейс, фото штрихкода с оборота упаковки для подтверждения рекламации. Это помогло бы нам разобраться в ситуации.»
  • Жалоба: тонкий / не защищает / мягкий: «Здравствуйте! Мы сожалеем, что Вам не понравился наш товар. Хотим обратить внимание, что мягкая конструкция футляра помогает защитить ваши очки от царапин и легкого физического воздействия. С уважением, отдел по работе с клиентами.»

▸ ЧАСЫ (артикул начинается на WCH-, VCHRD- или категория содержит «часы»/«watch»):
  • Оценка 4–5: «Здравствуйте! Спасибо за Ваш отзыв и высокую оценку, пользуйтесь часами с удовольствием! Мы всегда стараемся, чтобы наши покупатели оставались довольны своим приобретением и сервисом, поэтому очень ценим вашу обратную связь. С уважением, отдел по работе с клиентами.»
  • Оценка 1–3: «Здравствуйте! Очень жаль, что Вам не понравился наш товар. В любом случае спасибо за обратную связь. Всего вам доброго и приятных покупок!»
"""

async def generate_feedback_reply(product: str, rating: int, review: str, article: str = "") -> str:
    """Одиночный запрос к Groq — выбор шаблона или генерация ответа без сохранения в историю."""
    if not _api_key:
        return "❌ GROQ_API_KEY не настроен."
    prompt = (
        f"Ты — менеджер WB-магазина. Твоя задача: найти подходящий шаблон из списка ниже и вернуть его ДОСЛОВНО.\n"
        f"Если ни один шаблон не подходит — напиши вежливый ответ 2–3 предложения по-русски.\n"
        f"Верни ТОЛЬКО текст ответа, без пояснений.\n\n"
        f"{_REPLY_TEMPLATES}\n"
        f"Артикул: {article}\nТовар/Категория: {product}\nОценка: {rating}/5\nОтзыв: «{review}»"
    )
    messages = [
        {"role": "system", "content": "Ты менеджер по работе с клиентами интернет-магазина на Wildberries."},
        {"role": "user",   "content": prompt},
    ]
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            GROQ_URL,
            headers={"Authorization": f"Bearer {_api_key}", "Content-Type": "application/json"},
            json={"model": MODEL, "messages": messages, "max_tokens": 300, "temperature": 0.3},
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()

async def ask(chat_id: int, question: str) -> str:
    if not _api_key:
        return "❌ GROQ_API_KEY не настроен. Добавь его в переменные Railway."

    history = _histories.get(chat_id, [])
    context = _contexts.get(chat_id, "")

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]

    if context:
        messages.append({
            "role": "user",
            "content": f"Вот актуальные данные магазина:\n\n{context}"
        })
        messages.append({
            "role": "assistant",
            "content": "Данные получил, готов анализировать."
        })

    messages.extend(history)

    if question:
        messages.append({"role": "user", "content": question})
    else:
        messages.append({"role": "user", "content": "Проанализируй данные и дай главные рекомендации."})

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            GROQ_URL,
            headers={"Authorization": f"Bearer {_api_key}", "Content-Type": "application/json"},
            json={"model": MODEL, "messages": messages, "max_tokens": 1024, "temperature": 0.7},
        )
        resp.raise_for_status()
        text = resp.json()["choices"][0]["message"]["content"]

    history.append({"role": "user", "content": question or "Анализ данных"})
    history.append({"role": "assistant", "content": text})
    _histories[chat_id] = history[-20:]

    return text
