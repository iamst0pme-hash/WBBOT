# WB Sales Bot

Минимальный Telegram-бот для выгрузки продаж в XLS.

## Переменные окружения

Нужны только две переменные:

- `BOT_TOKEN` — токен Telegram-бота
- `WB_API_KEY` — ключ WB API

## Railway

В Railway → Variables добавь:

- `BOT_TOKEN`
- `WB_API_KEY`

## Локальный запуск

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
python -m app.main
```

## Что делает бот

Кнопка **Продажи** → выбор периода → сбор данных из WB API → формирование XLS со столбцами:

- Артикул Продавца
- Заказы, шт
- Заказы, сумма
- Остатки, шт
- Динамика заказов в рублях
