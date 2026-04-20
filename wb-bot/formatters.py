from wb_api import msk_date, msk_label
from datetime import datetime, timedelta

def fmt(v) -> str:
    return f"{int(v):,}".replace(",", "\u202f")

def fmtf(v, dec=1) -> str:
    return f"{float(v):.{dec}f}"

# ─── ПРОДАЖИ ─────────────────────────────────────────────────────────────────

def format_sales(rows: list) -> str:
    day1_date = msk_date(1)
    day2_date = msk_date(2)
    day1_label = msk_label(1)
    day2_label = msk_label(2)

    stats = {
        day1_date: {"title": "Вчера", "label": day1_label, "sum": 0, "count": 0, "products": {}},
        day2_date: {"title": "Позавчера", "label": day2_label, "sum": 0, "count": 0, "products": {}},
    }

    for row in rows:
        product = row.get("product") or {}
        history = row.get("history") or []
        for day in history:
            date = str(day.get("date") or "")
            if date not in stats:
                continue
            def _num(d, *keys):
                for k in keys:
                    v = d.get(k)
                    if v is not None:
                        return float(v)
                return 0.0
            orders_sum = _num(day, "ordersSumRub", "ordersSumRUB", "ordersSum", "orderSum", "sumRub")
            orders_count = int(_num(day, "ordersCount", "orderCount", "orders"))
            stats[date]["sum"] += orders_sum
            stats[date]["count"] += orders_count
            key = str(product.get("nmId") or product.get("nmID") or product.get("vendorCode") or "?")
            pm = stats[date]["products"]
            if key not in pm:
                pm[key] = {"vendorCode": product.get("vendorCode") or "", "count": 0, "sum": 0}
            pm[key]["count"] += orders_count
            pm[key]["sum"] += orders_sum

    lines = []
    for date in [day1_date, day2_date]:
        s = stats[date]
        lines.append(f"💰 *{s['title']} ({s['label']})*")
        lines.append(f"Сумма заказов: {fmt(s['sum'])} ₽")
        lines.append("")
        lines.append(f"ТОП-5 {s['title']} ({s['label']}):")
        top5 = sorted(s["products"].values(), key=lambda x: (-x["count"], -x["sum"]))[:5]
        if top5:
            for i, p in enumerate(top5, 1):
                lines.append(f"{i}️⃣ {p['vendorCode'] or '—'} — {p['count']} шт ({fmt(p['sum'])} ₽)")
        else:
            lines.append("— Нет заказов")
        lines.append("")

    d1_sum = stats[day1_date]["sum"]
    d2_sum = stats[day2_date]["sum"]
    diff = d1_sum - d2_sum
    prefix = "+" if diff > 0 else ""
    lines.append(f"📊 *Разница:* {prefix}{fmt(diff)} ₽")

    header = f"📊 *WB ЗАКАЗЫ: Вчера ({day1_label}) / Позавчера ({day2_label})*\n"
    return header + "\n" + "\n".join(lines)

# ─── СКЛАД ────────────────────────────────────────────────────────────────────

def format_stock(items: list) -> str:
    THRESHOLD = 50
    processed = []
    for item in items:
        art = item.get("vendorCode") or "Без артикула"
        warehouses = item.get("warehouses") or []
        physical = [w for w in warehouses
                    if w.get("warehouseName") != "Всего находится на складах"
                    and "В пути" not in str(w.get("warehouseName") or "")]
        transit = [w for w in warehouses
                   if "В пути" in str(w.get("warehouseName") or "")]
        qty = sum(int(w.get("quantity") or 0) for w in physical)
        in_way = sum(int(w.get("quantity") or 0) for w in transit)
        if qty > 0 or in_way > 0:
            processed.append({"art": art, "qty": qty, "in_way": in_way})

    processed.sort(key=lambda x: x["qty"])
    lines = ["📦 ОСТАТКИ НА СКЛАДЕ", ""]
    for x in processed:
        icon = "🔴" if x["qty"] < THRESHOLD else "✅"
        lines.append(f"{icon} {x['art']}: {x['qty']} шт (в пути: {x['in_way']})")
    if not processed:
        lines.append("Данные не найдены.")
    return "\n".join(lines)

# ─── КАМПАНИИ ────────────────────────────────────────────────────────────────

def format_campaigns(campaigns: list) -> str:
    if not campaigns:
        return "📢 Активных кампаний со статистикой нет."
    lines = ["📢 *КАМПАНИИ WB*", ""]
    low_balance = []
    for c in campaigns:
        ctr = round(c["clicks"] / c["views"] * 100, 2) if c["views"] > 0 else 0
        bal_icon = "🔴" if c["balance"] < 100 else "💰"
        lines.append(f"*{c['name']}*")
        lines.append(f"{bal_icon} Баланс: {fmt(c['balance'])} ₽")
        lines.append(f"💸 Затраты: {fmt(c['spend'])} ₽")
        lines.append(f"👁 Показы: {fmt(c['views'])} | CTR: {ctr}%")
        lines.append(f"🛒 Заказы: {c['orders']}")
        lines.append("")
        if c["balance"] < 100:
            low_balance.append(c["name"])
    if low_balance:
        lines.append("⚠️ *Пополни баланс:*")
        for name in low_balance:
            lines.append(f"— {name}")
    return "\n".join(lines)

# ─── ПРИХОДЫ ПО НЕДЕЛЯМ ──────────────────────────────────────────────────────

def format_income_weeks(reports: list) -> str:
    # Данные из нового Finance API: forPaySum = Итого к оплате per report
    # Группируем по dateFrom (период), суммируем Основной + По выкупам
    weeks = {}  # dateFrom[:10] -> {total, d_to}
    for r in reports:
        d_from = str(r.get("dateFrom") or r.get("date_from") or "")[:10]
        d_to   = str(r.get("dateTo")   or r.get("date_to")   or "")[:10]
        if not d_from:
            continue
        amount = float(r.get("forPaySum") or r.get("for_pay_sum") or 0)
        if d_from not in weeks:
            weeks[d_from] = {"total": 0.0, "d_to": d_to}
        weeks[d_from]["total"] += amount

    sorted_weeks = sorted(weeks.items())[-4:]
    total = sum(v["total"] for _, v in sorted_weeks)
    lines = ["💵 *ПРИХОДЫ WB — 4 недели*", ""]
    for i, (d_from, v) in enumerate(sorted_weeks, 1):
        try:
            df = datetime.fromisoformat(d_from).strftime("%d.%m")
            dt = datetime.fromisoformat(v["d_to"]).strftime("%d.%m")
        except Exception:
            df, dt = d_from[:5], v["d_to"][:5]
        lines.append(f"📅 *Неделя {i}:* {df} — {dt}")
        lines.append(f"   ✅ К получению: *{fmt(v['total'])} ₽*")
        lines.append("")
    lines.append(f"💰 *Итого за месяц: {fmt(total)} ₽*")
    return "\n".join(lines)

# ─── ФИНАНСЫ ─────────────────────────────────────────────────────────────────

def format_finance(rows: list) -> str:
    sales_sum = commission = logistics = storage = penalties = to_pay = correction = acceptance = 0

    for r in rows:
        to_pay      += float(r.get("ppvz_for_pay") or 0)
        commission  += float(r.get("ppvz_vw") or 0)       # приходит < 0 из API
        logistics   += float(r.get("delivery_rub") or 0)  # приходит > 0 из API (расход)
        storage     += float(r.get("storage_fee") or 0)   # приходит > 0 из API (расход)
        penalties   += float(r.get("penalty") or 0)
        correction  += float(r.get("ppvz_reward") or 0)
        acceptance  += float(r.get("acceptance") or 0)    # приходит > 0 из API (расход)
        doc = str(r.get("doc_type_name") or r.get("supplier_oper_name") or "")
        if "продажа" in doc.lower():
            sales_sum += float(r.get("retail_price_withdisc_rub") or 0)

    # комиссия уже отрицательная → abs; остальные расходы положительные → as-is
    expenses = abs(commission) + logistics + storage + abs(penalties) + abs(acceptance)
    label = msk_label(7)
    today = msk_label(1)

    corr_str = f"+{fmt(correction)}" if correction >= 0 else f"-{fmt(abs(correction))}"

    lines = [
        f"💰 *ФИНАНСЫ WB* ({label} — {today})",
        "",
        f"📈 Приход: {fmt(sales_sum)} ₽",
        f"📉 Расход: -{fmt(expenses)} ₽",
        "",
        "Детализация:",
        f"💵 Продажи: {fmt(sales_sum)} ₽",
        f"🏦 Комиссия WB: -{fmt(abs(commission))} ₽",
        f"🚚 Логистика: -{fmt(logistics)} ₽",
        f"🏪 Хранение: -{fmt(storage)} ₽",
        f"🔄 Корректировка: {corr_str} ₽",
        f"⚠️ Штрафы: -{fmt(abs(penalties))} ₽",
        f"📥 Операции при приёмке: -{fmt(abs(acceptance))} ₽",
        "",
        f"✅ *Итого к получению: {fmt(to_pay)} ₽*",
    ]
    return "\n".join(lines)

# ─── ВОЗВРАТЫ ────────────────────────────────────────────────────────────────

def format_returns(rows: list) -> str:
    returns_by_art = {}
    for r in rows:
        doc = str(r.get("doc_type_name") or r.get("supplier_oper_name") or "")
        if "возврат" not in doc.lower():
            continue
        art = r.get("sa_name") or r.get("supplierArticle") or "—"
        retail = float(r.get("retail_price_withdisc_rub") or 0)
        qty = int(r.get("quantity") or 1)
        if art not in returns_by_art:
            returns_by_art[art] = {"count": 0, "sum": 0}
        returns_by_art[art]["count"] += qty
        returns_by_art[art]["sum"] += abs(retail)

    if not returns_by_art:
        return "↩️ *ВОЗВРАТЫ*\n\nВозвратов за 7 дней нет."

    items = sorted(returns_by_art.items(), key=lambda x: -x[1]["count"])
    label = msk_label(7)
    today = msk_label(1)
    total_count = sum(v["count"] for v in returns_by_art.values())
    total_sum = sum(v["sum"] for v in returns_by_art.values())

    lines = [
        f"↩️ *ВОЗВРАТЫ WB* ({label} — {today})",
        f"Всего: {total_count} шт на {fmt(total_sum)} ₽",
        "",
    ]
    for art, data in items[:10]:
        lines.append(f"🔴 {art}: {data['count']} шт ({fmt(data['sum'])} ₽)")

    return "\n".join(lines)

# ─── НЕДЕЛЬНЫЙ ДАШБОРД ───────────────────────────────────────────────────────

def _delta(cur: float, prev: float) -> str:
    if prev == 0:
        return "—"
    pct = (cur - prev) / prev * 100
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.0f}%"

def _trend(cur: float, prev: float) -> str:
    if prev == 0:
        return ""
    return " 🟢" if cur >= prev else " 🔴"

def format_weekly_stats(weekly: dict, funnel: list, campaigns: list) -> str:
    cur = weekly.get("current") or {}
    prev = weekly.get("previous") or {}

    label_start = msk_label(7)
    label_end   = msk_label(1)

    total_cur_cnt = sum(v["count"] for v in cur.values())
    total_cur_sum = sum(v["sum"]   for v in cur.values())
    total_prev_cnt = sum(v["count"] for v in prev.values())
    total_prev_sum = sum(v["sum"]   for v in prev.values())
    avg_check = round(total_cur_sum / total_cur_cnt) if total_cur_cnt else 0

    def _growth(cur, prev):
        if not prev:
            return ""
        pct = (cur - prev) / prev * 100
        icon = "🟢" if pct >= 0 else "🔴"
        sign = "+" if pct >= 0 else ""
        return f" {icon} {sign}{pct:.0f}%"

    lines = [f"📊 *НЕДЕЛЯ {label_start} — {label_end}*", ""]

    # ── Итого ──
    lines.append(
        f"🛍 *{fmt(total_cur_cnt)} заказов · {fmt(total_cur_sum)} ₽*"
        + _growth(total_cur_cnt, total_prev_cnt)
    )
    if avg_check:
        lines.append(f"   Средний чек: {fmt(avg_check)} ₽")
    lines.append("")

    # ── По артикулам ──
    active_arts = [(v, d) for v, d in cur.items() if d["count"] > 0 or d["sum"] > 0]
    if active_arts:
        lines.append("*Артикулы:*")
        sorted_arts = sorted(active_arts, key=lambda x: -x[1]["sum"])[:12]
        for i, (vendor, d) in enumerate(sorted_arts, 1):
            p = prev.get(vendor, {"count": 0, "sum": 0.0})
            growth = _growth(d["sum"], p["sum"])
            lines.append(f"{i}. *{vendor}* — {d['count']} шт · {fmt(d['sum'])} ₽{growth}")
        lines.append("")
    elif not total_cur_cnt:
        lines.append("Заказов за неделю не найдено")
        lines.append("")

    # ── Воронка ──
    if funnel:
        total_opens   = sum(int(i.get("openCardCount")  or 0) for i in funnel)
        total_cart    = sum(int(i.get("addToCartCount") or 0) for i in funnel)
        total_orders  = sum(int(i.get("ordersCount")    or 0) for i in funnel)
        total_buyouts = sum(int(i.get("buyoutsCount")   or 0) for i in funnel)
        if total_opens:
            p_cart  = round(total_cart    / total_opens  * 100, 1)
            p_ord   = round(total_orders  / total_cart   * 100, 1) if total_cart   else 0
            p_buy   = round(total_buyouts / total_orders * 100, 1) if total_orders else 0
            lines.append("*Воронка (7 дней):*")
            lines.append(f"👁 {fmt(total_opens)}  →  🛒 {fmt(total_cart)} ({p_cart}%)")
            lines.append(f"📦 {fmt(total_orders)} ({p_ord}%)  →  ✅ {fmt(total_buyouts)} ({p_buy}%)")
            lines.append("")

    # ── ДРР ──
    if campaigns and total_cur_sum > 0:
        total_spend = sum(c.get("spend") or 0 for c in campaigns)
        if total_spend > 0:
            drr = round(total_spend / total_cur_sum * 100, 1)
            drr_icon = "🟢" if drr < 15 else ("🟡" if drr < 25 else "🔴")
            lines.append(f"📢 *ДРР {drr}%* {drr_icon}   реклама {fmt(total_spend)} ₽")

    return "\n".join(lines)

# ─── ВОРОНКА ─────────────────────────────────────────────────────────────────

def format_funnel(history: list) -> str:
    if not history:
        return "📈 *ВОРОНКА*\n\nДанные не найдены."

    label = msk_label(7)
    today = msk_label(1)
    lines = [f"📈 *ВОРОНКА ПРОДАЖ* ({label} — {today})", ""]

    total_opens = total_cart = total_orders = total_buyouts = 0
    rows = []
    for item in history:
        opens = int(item.get("openCardCount") or 0)
        cart = int(item.get("addToCartCount") or 0)
        orders = int(item.get("ordersCount") or 0)
        buyouts = int(item.get("buyoutsCount") or 0)
        vendor = item.get("vendorCode") or str(item.get("nmID") or "?")
        total_opens += opens
        total_cart += cart
        total_orders += orders
        total_buyouts += buyouts
        if opens > 0:
            rows.append({"vendor": vendor, "opens": opens, "cart": cart, "orders": orders, "buyouts": buyouts})

    rows.sort(key=lambda x: -x["orders"])

    # Итого
    ctr_cart = round(total_cart / total_opens * 100, 1) if total_opens else 0
    ctr_order = round(total_orders / total_cart * 100, 1) if total_cart else 0
    ctr_buyout = round(total_buyouts / total_orders * 100, 1) if total_orders else 0

    lines.append(f"👁 Просмотры: {fmt(total_opens)}")
    lines.append(f"🛒 В корзину: {fmt(total_cart)} ({ctr_cart}%)")
    lines.append(f"📦 Заказы: {fmt(total_orders)} ({ctr_order}% из корзины)")
    lines.append(f"✅ Выкупы: {fmt(total_buyouts)} ({ctr_buyout}% из заказов)")
    lines.append("")
    lines.append("*По товарам:*")

    for r in rows[:8]:
        c2o = round(r["orders"] / r["cart"] * 100, 1) if r["cart"] else 0
        lines.append(f"• {r['vendor']}: {r['opens']}→{r['cart']}→{r['orders']}→{r['buyouts']} | C→O: {c2o}%")

    return "\n".join(lines)

# ─── РЕЙТИНГ ─────────────────────────────────────────────────────────────────

def format_ratings(data: dict) -> str:
    unanswered = data.get("unanswered", 0)
    feedbacks  = data.get("feedbacks") or []
    cards      = data.get("cards") or []

    lines = ["⭐ *ОТЗЫВЫ WB*", ""]

    # ── Рейтинг карточек ──
    if cards:
        lines.append("*Рейтинг карточек:*")
        for c in cards:
            r = c["rating"]
            cnt = c["feedbacksCount"]
            warn = " ⚠️" if r > 0 and r < 4.0 else ""
            stars = f"{r:.1f} ⭐" if r > 0 else "нет оценок"
            lines.append(f"• *{c['vendorCode']}* — {stars} ({cnt} отз.){warn}")
        lines.append("")

    # ── Без ответа ──
    lines.append(f"💬 *Без ответа: {unanswered}*")

    if feedbacks:
        lines.append("")
        lines.append("*Последние без ответа:*")
        for f in feedbacks[:5]:
            rating  = f.get("productValuation") or "?"
            text    = (f.get("text") or "").strip()[:80]
            product = f.get("subjectName") or f.get("productName") or "—"
            stars   = "⭐" * int(rating) if isinstance(rating, int) else ""
            lines.append(f"{stars} *{product}*")
            if text:
                lines.append(f"_{text}_")
            lines.append("")
    else:
        lines.append("\nОтзывов без ответа нет!")

    return "\n".join(lines)

# ─── ABC-АНАЛИЗ ───────────────────────────────────────────────────────────────

def format_abc(products: list) -> str:
    if not products:
        return "🏆 *ABC-АНАЛИЗ*\n\nДанных нет."

    label = msk_label(30)
    today = msk_label(1)
    lines = [f"🏆 *ABC-АНАЛИЗ* ({label} — {today})", ""]

    by_class = {"A": [], "B": [], "C": []}
    for p in products:
        by_class[p["class"]].append(p)

    total = sum(p["sum"] for p in products)

    for cls, emoji, desc in [("A", "🟢", "80% выручки — фокус"), ("B", "🟡", "15% — поддержка"), ("C", "🔴", "5% — пересмотр")]:
        items = by_class[cls]
        if not items:
            continue
        cls_sum = sum(p["sum"] for p in items)
        pct = round(cls_sum / total * 100, 1) if total else 0
        lines.append(f"{emoji} *Класс {cls}* — {len(items)} товаров, {pct}% выручки ({desc})")
        for p in items[:5]:
            lines.append(f"  • {p['vendorCode']}: {fmt(p['sum'])} ₽ ({p['count']} шт)")
        if len(items) > 5:
            lines.append(f"  ...ещё {len(items)-5}")
        lines.append("")

    return "\n".join(lines)
