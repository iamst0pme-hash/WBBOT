import asyncio
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
import httpx

MSK = timezone(timedelta(hours=3))


def _is_retryable_status(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code < 600


def _retry_delay(response: httpx.Response | None, attempt: int) -> float:
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(float(retry_after), 1.0)
            except ValueError:
                pass
    # 2, 4, 8, 16, 20 seconds
    return min(2 ** attempt, 20)


async def _request_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    attempts: int = 5,
    sleep_before: float = 0.0,
    **kwargs,
) -> httpx.Response:
    last_error = None

    for attempt in range(1, attempts + 1):
        if sleep_before > 0:
            await asyncio.sleep(sleep_before)
        try:
            response = await client.request(method, url, **kwargs)
            if not _is_retryable_status(response.status_code):
                response.raise_for_status()
                return response

            last_error = httpx.HTTPStatusError(
                f"Retryable status {response.status_code} for url '{url}'",
                request=response.request,
                response=response,
            )
            if attempt == attempts:
                response.raise_for_status()
            await asyncio.sleep(_retry_delay(response, attempt))
        except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.RemoteProtocolError, httpx.NetworkError) as e:
            last_error = e
            if attempt == attempts:
                raise
            await asyncio.sleep(min(2 ** attempt, 20))

    if last_error:
        raise last_error
    raise RuntimeError("Request failed without a specific error")


MSK = timezone(timedelta(hours=3))
def msk_date(days_ago: int) -> str:
    return (datetime.now(MSK) - timedelta(days=days_ago)).strftime("%Y-%m-%d")

def msk_label(days_ago: int) -> str:
    return (datetime.now(MSK) - timedelta(days=days_ago)).strftime("%d.%m")

HEADERS = {}
ADS_HEADERS = {}
FINANCE_HEADERS = {}

def init(api_key: str, ads_key: str = None, finance_key: str = None):
    HEADERS["Authorization"] = api_key
    ADS_HEADERS["Authorization"] = ads_key or api_key
    FINANCE_HEADERS["Authorization"] = finance_key or api_key

# ─── КАРТОЧКИ ────────────────────────────────────────────────────────────────

async def get_cards(client: httpx.AsyncClient) -> list[int]:
    resp = await client.post(
        "https://content-api.wildberries.ru/content/v2/get/cards/list",
        json={"settings": {"sort": {"ascending": False}, "cursor": {"limit": 100}, "filter": {"withPhoto": -1}}},
        headers=HEADERS,
    )
    resp.raise_for_status()
    data = resp.json()
    cards = data.get("cards") or data.get("data", {}).get("cards") or []
    return list({int(c.get("nmID") or c.get("nmId") or 0) for c in cards if c.get("nmID") or c.get("nmId")})

async def get_card_map(client: httpx.AsyncClient) -> dict:
    """Возвращает {nmId: {vendorCode, rating, feedbacksCount}}"""
    resp = await client.post(
        "https://content-api.wildberries.ru/content/v2/get/cards/list",
        json={"settings": {"sort": {"ascending": False}, "cursor": {"limit": 100}, "filter": {"withPhoto": -1}}},
        headers=HEADERS,
    )
    resp.raise_for_status()
    data = resp.json()
    cards = data.get("cards") or data.get("data", {}).get("cards") or []
    return {
        int(c.get("nmID") or c.get("nmId") or 0): {
            "vendorCode":     c.get("vendorCode") or "",
            "rating":         float(c.get("rating") or 0),
            "feedbacksCount": int(c.get("feedbacksCount") or 0),
        }
        for c in cards if c.get("nmID") or c.get("nmId")
    }

# ─── ПРОДАЖИ ─────────────────────────────────────────────────────────────────

async def get_sales_history(client: httpx.AsyncClient, nm_ids: list[int], date_start: str, date_end: str) -> list:
    results = []
    if not nm_ids:
        return results

    # Делаем меньше запросов и даём WB больше времени между чанками,
    # иначе seller-analytics-api часто отвечает 429 даже на коротком периоде.
    chunk_size = 100

    for i in range(0, len(nm_ids), chunk_size):
        chunk = nm_ids[i:i + chunk_size]
        resp = await _request_with_retry(
            client,
            "POST",
            "https://seller-analytics-api.wildberries.ru/api/analytics/v3/sales-funnel/products/history",
            json={
                "selectedPeriod": {"start": date_start, "end": date_end},
                "nmIds": chunk,
                "brandNames": [], "subjectIds": [], "tagIds": [],
                "skipDeletedNm": False,
                "orderBy": {"field": "ordersSumRub", "mode": "desc"},
                # limit должен покрывать весь переданный чанк, иначе часть nmIds может не вернуться
                "limit": max(len(chunk), 100),
                "offset": 0,
            },
            headers=HEADERS,
            attempts=6,
            sleep_before=1.5 if i > 0 else 0.0,
        )
        body = resp.json()
        rows = body if isinstance(body, list) else body.get("data") or []
        results.extend(rows)

    return results

# ─── СКЛАД ────────────────────────────────────────────────────────────────────

async def get_stock_report(client: httpx.AsyncClient) -> list:
    resp = await client.get(
        "https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains",
        params={"groupBySa": "true"},
        headers=HEADERS,
    )
    resp.raise_for_status()
    task_id = resp.json()["data"]["taskId"]

    for _ in range(12):
        await asyncio.sleep(5)
        status_resp = await client.get(
            f"https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains/tasks/{task_id}/status",
            headers=HEADERS,
        )
        status_resp.raise_for_status()
        body = status_resp.json()
        status = (body.get("data") or {}).get("status") or body.get("status") or ""
        if status in ("done", "complete", "completed", "finish", "finished"):
            break

    dl_resp = await client.get(
        f"https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains/tasks/{task_id}/download",
        headers=HEADERS,
    )
    dl_resp.raise_for_status()
    data = dl_resp.json()
    return data if isinstance(data, list) else data.get("data") or []

# ─── КАМПАНИИ ────────────────────────────────────────────────────────────────

async def get_active_campaigns(client: httpx.AsyncClient) -> list:
    r = await client.get(
        "https://advert-api.wildberries.ru/adv/v1/promotion/count",
        headers=ADS_HEADERS,
    )
    r.raise_for_status()
    data = r.json()

    groups = data.get("adverts") or []
    campaign_ids = []
    for group in groups:
        if group.get("status") == 9:
            for adv in (group.get("advert_list") or []):
                adv_id = adv.get("advertId") or adv.get("id")
                if adv_id:
                    campaign_ids.append(int(adv_id))

    if not campaign_ids:
        return []

    info_resp = await client.post(
        "https://advert-api.wildberries.ru/adv/v2/adverts",
        json={"ids": campaign_ids},
        headers=ADS_HEADERS,
    )
    info_list = []
    if info_resp.status_code == 200:
        raw = info_resp.json()
        info_list = raw if isinstance(raw, list) else (raw.get("data") or [])

    date_from = msk_date(6)
    date_to = msk_date(0)
    ids_str = ",".join(str(i) for i in campaign_ids)
    stats_resp = await client.get(
        f"https://advert-api.wildberries.ru/adv/v3/fullstats?ids={ids_str}&beginDate={date_from}&endDate={date_to}",
        headers=ADS_HEADERS,
    )
    stats_list = []
    if stats_resp.status_code == 200:
        raw = stats_resp.json()
        stats_list = raw if isinstance(raw, list) else (raw.get("data") or [])

    budgets = {}
    for camp_id in campaign_ids:
        try:
            b_resp = await client.get(
                "https://advert-api.wildberries.ru/adv/v1/budget",
                params={"id": camp_id},
                headers=ADS_HEADERS,
            )
            if b_resp.status_code == 200:
                budgets[camp_id] = float(b_resp.json().get("total") or 0)
        except Exception:
            budgets[camp_id] = 0

    info_by_id = {}
    for row in info_list:
        rid = int(row.get("id") or row.get("advertId") or 0)
        if rid:
            info_by_id[rid] = row

    stats_by_id = {}
    for row in stats_list:
        rid = int(row.get("advertId") or row.get("id") or 0)
        if not rid:
            continue
        views = clicks = orders = 0
        spend = 0.0
        views = int(row.get("views") or 0)
        clicks = int(row.get("clicks") or 0)
        orders = int(row.get("orders") or 0)
        spend = float(row.get("sum") or row.get("spend") or 0)
        if not (views or clicks or orders or spend):
            for d in (row.get("days") or []):
                views += int(d.get("views") or 0)
                clicks += int(d.get("clicks") or 0)
                orders += int(d.get("orders") or 0)
                spend += float(d.get("sum") or d.get("spend") or 0)
        stats_by_id[rid] = {"views": views, "clicks": clicks, "orders": orders, "spend": spend}

    campaigns = []
    for camp_id in campaign_ids:
        info = info_by_id.get(camp_id) or {}
        stat = stats_by_id.get(camp_id) or {"views": 0, "clicks": 0, "orders": 0, "spend": 0.0}
        name = (info.get("name") or info.get("advert_name") or
                (info.get("settings") or {}).get("name") or f"Кампания {camp_id}")
        campaigns.append({
            "id": camp_id,
            "name": name,
            "balance": round(budgets.get(camp_id, 0)),
            "views": stat["views"],
            "clicks": stat["clicks"],
            "orders": stat["orders"],
            "spend": round(stat["spend"]),
        })

    campaigns = [c for c in campaigns if c["views"] > 0 or c["orders"] > 0 or c["spend"] > 0]
    campaigns.sort(key=lambda x: -x["spend"])
    return campaigns

# ─── ФИНАНСЫ ─────────────────────────────────────────────────────────────────

async def get_finance_report(client: httpx.AsyncClient) -> list:
    date_from = msk_date(28)
    date_to = msk_date(1)
    all_rows = []
    rrdid = 0

    while True:
        resp = await client.get(
            "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod",
            params={"dateFrom": date_from, "dateTo": date_to, "rrdid": rrdid, "limit": 100000},
            headers=HEADERS,
            timeout=120,
        )
        resp.raise_for_status()
        if not resp.content or resp.text.strip() in ("", "null", "[]"):
            break
        try:
            data = resp.json()
        except Exception:
            break
        rows = data if isinstance(data, list) else (data.get("data") or [])
        if not rows:
            break
        all_rows.extend(rows)
        last_rrdid = int(rows[-1].get("rrdid") or rows[-1].get("rrd_id") or 0)
        if last_rrdid <= rrdid:
            break
        rrdid = last_rrdid

    return all_rows

# ─── НЕДЕЛЬНАЯ СТАТИСТИКА ПО АРТИКУЛАМ ───────────────────────────────────────

async def get_weekly_article_stats(client: httpx.AsyncClient, nm_ids: list[int]) -> dict:
    def _num(d: dict, *keys) -> float:
        for k in keys:
            v = d.get(k)
            if v is not None:
                try:
                    return float(v)
                except Exception:
                    pass
        return 0.0

    def _aggregate(rows: list) -> dict:
        out = {}
        for row in rows:
            product = row.get("product") or {}
            vendor = product.get("vendorCode") or str(product.get("nmId") or product.get("nmID") or "?")
            for day in (row.get("history") or []):
                cnt = int(_num(day, "ordersCount", "orderCount", "orders"))
                s = _num(day, "ordersSumRub", "ordersSumRUB", "ordersSum", "orderSum", "sumRub")
                if cnt == 0 and s == 0:
                    continue
                if vendor not in out:
                    out[vendor] = {"count": 0, "sum": 0.0}
                out[vendor]["count"] += cnt
                out[vendor]["sum"] += s
        return out

    cur_rows = []
    prev_rows = []
    try:
        cur_rows = await get_sales_history(client, nm_ids, msk_date(7), msk_date(1))
    except Exception as e:
        print(f"[DEBUG] weekly current: {e}")
    try:
        # Этот эндпоинт ограничен ~7 днями — prev может вернуть 400, тогда дельта = "—"
        prev_rows = await get_sales_history(client, nm_ids, msk_date(14), msk_date(8))
    except Exception as e:
        print(f"[DEBUG] weekly previous (ожидаемо, API ~7 дней): {e}")

    return {"current": _aggregate(cur_rows), "previous": _aggregate(prev_rows)}

# ─── ВОРОНКА ─────────────────────────────────────────────────────────────────

async def get_funnel(client: httpx.AsyncClient, nm_ids: list[int]) -> list:
    date_from = msk_date(7)
    date_to = msk_date(1)
    # Пробуем разные варианты эндпоинта и параметров
    attempts = [
        ("https://seller-analytics-api.wildberries.ru/api/analytics/v2/nm-report/grouped",
         {"brandNames": [], "objectIDs": [], "tagIDs": [], "nmIDs": [],
          "timezone": "Europe/Moscow", "period": {"begin": date_from, "end": date_to}, "page": 1}),
        ("https://seller-analytics-api.wildberries.ru/api/analytics/v1/nm-report/grouped",
         {"brandNames": [], "objectIDs": [], "tagIDs": [], "nmIDs": [],
          "timezone": "Europe/Moscow", "period": {"begin": date_from, "end": date_to}, "page": 1}),
        ("https://seller-analytics-api.wildberries.ru/api/analytics/v2/nm-report/detail",
         {"brandNames": [], "objectIDs": [], "tagIDs": [], "nmIDs": [],
          "timezone": "Europe/Moscow", "period": {"begin": date_from, "end": date_to}, "page": 1}),
    ]
    for url, body in attempts:
        try:
            resp = await client.post(url, json=body, headers=HEADERS)
            print(f"[DEBUG] funnel {url.split('/')[-1]} → {resp.status_code}: {resp.text[:100]}")
            if resp.status_code == 200:
                data = resp.json()
                cards = (data.get("data") or {}).get("cards") or data.get("cards") or []
                if cards:
                    return cards
        except Exception as e:
            print(f"[DEBUG] funnel error: {e}")
    return []

# ─── ОТВЕТ НА ОТЗЫВ ──────────────────────────────────────────────────────────

async def reply_to_feedback(client: httpx.AsyncClient, feedback_id: str, text: str) -> bool:
    resp = await client.post(
        "https://feedbacks-api.wildberries.ru/api/v1/feedbacks/answer",
        json={"id": feedback_id, "text": text},
        headers=HEADERS,
        timeout=15,
    )
    print(f"[reply_to_feedback] id={feedback_id} status={resp.status_code} body={resp.text[:300]}")
    if resp.status_code not in (200, 204):
        raise RuntimeError(f"WB {resp.status_code}: {resp.text[:200]}")
    return True

# ─── РЕЙТИНГ И ОТЗЫВЫ ────────────────────────────────────────────────────────

async def get_ratings(client: httpx.AsyncClient) -> dict:
    result = {"unanswered": 0, "feedbacks": [], "cards": []}

    card_map = {}
    try:
        card_map = await get_card_map(client)
    except Exception:
        pass

    # Загружаем все отзывы: и с ответом и без — для расчёта рейтинга
    all_feedbacks = []
    unanswered_feedbacks = []
    for answered in ["false", "true"]:
        try:
            resp = await client.get(
                "https://feedbacks-api.wildberries.ru/api/v1/feedbacks",
                params={"isAnswered": answered, "take": 100, "skip": 0, "order": "dateDesc"},
                headers=HEADERS,
            )
            if resp.status_code == 200:
                data = resp.json()
                fbs = (data.get("data") or {}).get("feedbacks") or data.get("feedbacks") or []
                all_feedbacks.extend(fbs)
                if answered == "false":
                    unanswered_feedbacks = fbs
        except Exception as e:
            print(f"[DEBUG] feedbacks {answered}: {e}")

    result["feedbacks"] = unanswered_feedbacks
    result["unanswered"] = len(unanswered_feedbacks)

    try:
        count_resp = await client.get(
            "https://feedbacks-api.wildberries.ru/api/v1/feedbacks/count-unanswered",
            headers=HEADERS,
        )
        if count_resp.status_code == 200:
            cnt = count_resp.json()
            total = cnt.get("countUnanswered") or cnt.get("count") or 0
            if total > result["unanswered"]:
                result["unanswered"] = total
    except Exception:
        pass

    # Добавляем vendorCode к каждому отзыву для шаблонов ответов
    for f in all_feedbacks:
        nm_id = int(f.get("nmId") or f.get("nmID") or 0)
        f["_vendorCode"] = (card_map.get(nm_id) or {}).get("vendorCode") or ""

    # Считаем рейтинг по артикулам из загруженных отзывов
    by_art: dict = {}
    for f in all_feedbacks:
        nm_id  = int(f.get("nmId") or f.get("nmID") or 0)
        vendor = (card_map.get(nm_id) or {}).get("vendorCode") or f.get("subjectName") or "—"
        val    = f.get("productValuation")
        if not val:
            continue
        if vendor not in by_art:
            by_art[vendor] = []
        by_art[vendor].append(int(val))

    cards = []
    for vendor, vals in by_art.items():
        avg = round(sum(vals) / len(vals), 1)
        cards.append({"vendorCode": vendor, "rating": avg, "feedbacksCount": len(vals)})

    # Сортировка: сначала с низким рейтингом
    result["cards"] = sorted(cards, key=lambda x: x["rating"])
    return result

# ─── ABC-АНАЛИЗ ───────────────────────────────────────────────────────────────

async def get_abc(client: httpx.AsyncClient, nm_ids: list[int]) -> list:
    # API ограничен ~7 днями для этого эндпоинта
    date_from = msk_date(7)
    date_to = msk_date(1)
    rows = await get_sales_history(client, nm_ids, date_from, date_to)

    product_map = {}
    for row in rows:
        product = row.get("product") or {}
        history = row.get("history") or []
        vendor = product.get("vendorCode") or str(product.get("nmId") or "?")
        total_sum = sum(float(d.get("ordersSumRub") or 0) for d in history)
        total_count = sum(int(d.get("ordersCount") or 0) for d in history)
        if vendor not in product_map:
            product_map[vendor] = {"vendorCode": vendor, "sum": 0, "count": 0}
        product_map[vendor]["sum"] += total_sum
        product_map[vendor]["count"] += total_count

    products = sorted(product_map.values(), key=lambda x: -x["sum"])
    total = sum(p["sum"] for p in products)
    if total == 0:
        return []

    cumulative = 0
    for p in products:
        cumulative += p["sum"]
        share = cumulative / total * 100
        p["cumulative_pct"] = share
        if share <= 80:
            p["class"] = "A"
        elif share <= 95:
            p["class"] = "B"
        else:
            p["class"] = "C"

    return products

# ─── ЕЖЕНЕДЕЛЬНЫЕ ВЫПЛАТЫ ────────────────────────────────────────────────────

async def get_weekly_payments(client: httpx.AsyncClient) -> list:
    token = FINANCE_HEADERS.get("Authorization", "")
    # Пробуем Finance API с обоими вариантами заголовка
    for auth_headers in [{"HeaderApiKey": token}, {"Authorization": token}]:
        try:
            resp = await client.post(
                "https://finance-api.wildberries.ru/api/finance/v1/sales-reports/list",
                json={"dateFrom": msk_date(35), "dateTo": msk_date(0), "limit": 1000, "offset": 0, "period": "weekly"},
                headers=auth_headers,
                timeout=30,
            )
            if resp.status_code == 200:
                data = resp.json()
                rows = data if isinstance(data, list) else (data.get("data") or data.get("reports") or data.get("items") or [])
                if rows:
                    return rows
        except Exception:
            pass
    # Fallback: 4 отдельных запроса по неделям через reportDetailByPeriod
    return await _get_weekly_payments_fallback(client)

async def _get_weekly_payments_fallback(client: httpx.AsyncClient) -> list:
    # Один запрос за 35 дней — не бьём rate limit и захватываем даты формирования отчётов
    all_rows = []
    rrdid = 0
    while True:
        resp = await client.get(
            "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod",
            params={"dateFrom": msk_date(35), "dateTo": msk_date(0), "rrdid": rrdid, "limit": 100000},
            headers=HEADERS,
            timeout=120,
        )
        resp.raise_for_status()
        if not resp.content or resp.text.strip() in ("", "null", "[]"):
            break
        data = resp.json()
        rows = data if isinstance(data, list) else (data.get("data") or [])
        if not rows:
            break
        all_rows.extend(rows)
        last_rrdid = int(rows[-1].get("rrdid") or rows[-1].get("rrd_id") or 0)
        if last_rrdid <= rrdid:
            break
        rrdid = last_rrdid

    # Группируем по realizationreport_id
    reports = {}
    for r in all_rows:
        rid = r.get("realizationreport_id") or 0
        if not rid:
            continue
        raw = str(r.get("rr_dt") or "")[:10]
        if not raw:
            continue
        try:
            d = datetime.fromisoformat(raw).date()
        except Exception:
            continue
        if rid not in reports:
            reports[rid] = {"total": 0.0, "max_date": d}
        reports[rid]["total"] += (
            float(r.get("ppvz_for_pay") or 0)
            - float(r.get("delivery_rub") or 0)
            - float(r.get("storage_fee") or 0)
            - float(r.get("acceptance") or 0)
            - float(r.get("penalty") or 0)
            + float(r.get("ppvz_reward") or 0)
        )
        if d > reports[rid]["max_date"]:
            reports[rid]["max_date"] = d

    # Определяем неделю по максимальной дате в группе:
    # если max_date = понедельник (дата формирования) → неделя = max_date - 7 дней
    # иначе → неделя = начало недели max_date
    current_monday = datetime.now(MSK).date() - timedelta(days=datetime.now(MSK).weekday())
    weeks = {}
    for data in reports.values():
        mx = data["max_date"]
        settlement_monday = (mx - timedelta(days=7)) if mx.weekday() == 0 else (mx - timedelta(days=mx.weekday()))
        if settlement_monday >= current_monday:
            continue  # текущая незакрытая неделя
        weeks[settlement_monday] = weeks.get(settlement_monday, 0.0) + data["total"]

    sorted_weeks = sorted(weeks.items())[-4:]
    return [
        {"dateFrom": str(mon), "dateTo": str(mon + timedelta(days=6)), "forPaySum": amt}
        for mon, amt in sorted_weeks
    ]

# ─── КУРС ВАЛЮТ (ЦБ РФ) ─────────────────────────────────────────────────────

async def get_exchange_rates(client: httpx.AsyncClient) -> dict:
    """Курс валют с официального XML-API ЦБ РФ."""
    try:
        resp = await client.get(
            "https://www.cbr.ru/scripts/XML_daily.asp",
            timeout=10,
        )
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        date = root.get("Date", "")[:10]
        rates: dict = {"date": date}
        for v in root.findall("Valute"):
            code    = v.findtext("CharCode") or ""
            nominal = int(v.findtext("Nominal") or 1)
            value   = float((v.findtext("Value") or "0").replace(",", "."))
            if code in ("USD", "EUR", "CNY"):
                rates[code] = round(value / nominal, 2)
        return rates
    except Exception as e:
        print(f"[DEBUG] exchange_rates: {e}")
        return {}

# ─── AI СВОДКА ───────────────────────────────────────────────────────────────

async def get_ai_summary(client: httpx.AsyncClient) -> str:
    lines = []
    nm_ids = []

    # Карточки
    try:
        nm_ids = await get_cards(client)
    except Exception:
        pass

    # Продажи за 2 дня + топ товаров
    try:
        rows = await get_sales_history(client, nm_ids, msk_date(2), msk_date(1))
        d1, d2 = msk_date(1), msk_date(2)
        day1_sum = day2_sum = day1_cnt = day2_cnt = 0
        top_d1: dict = {}
        for row in rows:
            product = row.get("product") or {}
            vendor = product.get("vendorCode") or str(product.get("nmId") or "?")
            for day in (row.get("history") or []):
                date = str(day.get("date") or "")
                s = float(day.get("ordersSumRub") or 0)
                c = int(day.get("ordersCount") or 0)
                if date == d1:
                    day1_sum += s; day1_cnt += c
                    if vendor not in top_d1:
                        top_d1[vendor] = {"count": 0, "sum": 0}
                    top_d1[vendor]["count"] += c
                    top_d1[vendor]["sum"] += s
                elif date == d2:
                    day2_sum += s; day2_cnt += c
        diff = day1_sum - day2_sum
        lines.append(f"ПРОДАЖИ:")
        lines.append(f"  Вчера ({msk_label(1)}): {int(day1_sum)} ₽, {day1_cnt} заказов")
        lines.append(f"  Позавчера ({msk_label(2)}): {int(day2_sum)} ₽, {day2_cnt} заказов")
        lines.append(f"  Динамика: {'+' if diff >= 0 else ''}{int(diff)} ₽")
        top5 = sorted(top_d1.items(), key=lambda x: -x[1]["count"])[:5]
        if top5:
            lines.append(f"  ТОП товаров вчера:")
            for v, d in top5:
                lines.append(f"    {v}: {d['count']} заказов, {int(d['sum'])} ₽")
    except Exception as e:
        lines.append(f"ПРОДАЖИ: ошибка ({e})")

    # Воронка карточек (просмотры → корзина → заказы → выкупы)
    try:
        funnel = await get_funnel(client, nm_ids)
        if funnel:
            lines.append(f"\nВОРОНКА КАРТОЧЕК (7 дней):")
            funnel_sorted = sorted(funnel, key=lambda x: -int(x.get("openCardCount") or 0))
            for item in funnel_sorted[:10]:
                vendor = item.get("vendorCode") or str(item.get("nmID") or "?")
                opens = int(item.get("openCardCount") or 0)
                cart = int(item.get("addToCartCount") or 0)
                orders = int(item.get("ordersCount") or 0)
                buyouts = int(item.get("buyoutsCount") or 0)
                if opens == 0:
                    continue
                cart_pct = round(cart / opens * 100, 1)
                order_pct = round(orders / cart * 100, 1) if cart else 0
                buyout_pct = round(buyouts / orders * 100, 1) if orders else 0
                lines.append(
                    f"  {vendor}: просм {opens} → корзина {cart} ({cart_pct}%)"
                    f" → заказы {orders} ({order_pct}%) → выкуп {buyouts} ({buyout_pct}%)"
                )
    except Exception as e:
        lines.append(f"\nВОРОНКА: ошибка ({e})")

    # Реклама с CTR
    try:
        campaigns = await get_active_campaigns(client)
        lines.append(f"\nРЕКЛАМА (активных кампаний: {len(campaigns)}):")
        total_ad_spend = sum(c["spend"] for c in campaigns)
        for c in campaigns:
            ctr = round(c["clicks"] / c["views"] * 100, 2) if c["views"] > 0 else 0
            drr = round(c["spend"] / c["orders"] if c["orders"] > 0 else 0)
            bal_warn = " ⚠️ НИЗКИЙ БАЛАНС" if c["balance"] < 100 else ""
            lines.append(
                f"  {c['name']}: показы {c['views']}, CTR {ctr}%,"
                f" заказы {c['orders']}, затраты {c['spend']} ₽, баланс {c['balance']} ₽{bal_warn}"
            )
        lines.append(f"  Итого затраты на рекламу: {total_ad_spend} ₽")
    except Exception as e:
        lines.append(f"\nРЕКЛАМА: ошибка ({e})")

    # Финансы с маржой
    try:
        fin_rows = await get_finance_report(client)
        sales = commission = logistics = storage = to_pay = 0
        for r in fin_rows:
            to_pay     += float(r.get("ppvz_for_pay") or 0)
            commission += float(r.get("ppvz_vw") or 0)       # < 0 из API
            logistics  += float(r.get("delivery_rub") or 0)  # > 0 из API (расход)
            storage    += float(r.get("storage_fee") or 0)   # > 0 из API (расход)
            doc = str(r.get("doc_type_name") or "")
            if "продажа" in doc.lower():
                sales += float(r.get("retail_price_withdisc_rub") or 0)
        margin = round(to_pay / sales * 100, 1) if sales > 0 else 0
        lines.append(f"\nФИНАНСЫ (7 дней):")
        lines.append(f"  Выручка (продажи): {int(sales)} ₽")
        lines.append(f"  Комиссия WB: -{int(abs(commission))} ₽")
        lines.append(f"  Логистика: -{int(logistics)} ₽")
        lines.append(f"  Хранение: -{int(storage)} ₽")
        lines.append(f"  К получению: {int(to_pay)} ₽")
        lines.append(f"  Маржа: {margin}%")
    except Exception as e:
        lines.append(f"\nФИНАНСЫ: ошибка ({e})")

    # Курс валют
    try:
        rates = await get_exchange_rates(client)
        if rates:
            lines.append(f"\nКУРС ВАЛЮТ (ЦБ РФ на {rates['date']}):")
            lines.append(f"  USD: {rates['USD']} ₽")
            lines.append(f"  EUR: {rates['EUR']} ₽")
            lines.append(f"  CNY: {rates['CNY']} ₽")
    except Exception:
        pass

    return "\n".join(lines)
