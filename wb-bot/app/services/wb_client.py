from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import date
from typing import Any

import httpx


class WBApiError(RuntimeError):
    pass


@dataclass(slots=True)
class ProductCard:
    nm_id: int
    vendor_article: str


@dataclass(slots=True)
class SalesArticleRow:
    nm_id: int
    vendor_article: str
    orders_qty: float
    orders_sum: float
    stock_qty: float
    adv_sum: float
    drr: float
    orders_sum_dynamic: float


@dataclass(slots=True)
class SalesReport:
    rows: list[SalesArticleRow]
    total_orders_qty: float
    total_orders_sum: float
    total_stock_qty: float
    total_adv_sum: float
    total_drr: float
    total_orders_sum_dynamic: float


class WBClient:
    CONTENT_URL = "https://content-api.wildberries.ru/content/v2/get/cards/list"
    FUNNEL_URL = "https://seller-analytics-api.wildberries.ru/api/analytics/v3/sales-funnel/products"
    ADV_COUNTS_URL = "https://advert-api.wildberries.ru/adv/v1/promotion/count"
    ADV_DETAILS_URL = "https://advert-api.wildberries.ru/api/advert/v2/adverts"
    ADV_EXPENSES_URL = "https://advert-api.wildberries.ru/adv/v1/upd"

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key.strip()
        self._headers = {"Authorization": self.api_key}
        self._timeout = httpx.Timeout(60.0, connect=20.0)
        self._analytics_min_interval = 22.0
        self._adv_min_interval = 1.1
        self._last_analytics_request_monotonic = 0.0
        self._last_adv_request_monotonic = 0.0

    async def get_sales_report(
        self,
        current_start: date,
        current_end: date,
        past_start: date,
        past_end: date,
    ) -> SalesReport:
        cards = await self.get_all_cards()
        article_by_nm = {card.nm_id: card.vendor_article for card in cards}

        current_items = await self.get_funnel_report(
            start=current_start.isoformat(),
            end=current_end.isoformat(),
            past_start=past_start.isoformat(),
            past_end=past_end.isoformat(),
        )
        article_metrics = self._build_article_metrics(current_items, article_by_nm)

        campaign_details = await self.get_adv_campaign_details()
        adv_rows = await self.get_adv_expenses(current_start, current_end)
        adv_totals_by_advert = self._build_adv_totals_by_advert(adv_rows)
        self._apply_adv_to_articles(article_metrics, campaign_details, adv_totals_by_advert)

        rows: list[SalesArticleRow] = []
        total_orders_qty = 0.0
        total_orders_sum = 0.0
        total_stock_qty = 0.0
        total_adv_sum = 0.0
        total_orders_sum_dynamic = 0.0

        for bucket in article_metrics.values():
            orders_sum = bucket["orders_sum"]
            adv_sum = bucket["adv_sum"]
            row = SalesArticleRow(
                nm_id=int(bucket["nm_id"]),
                vendor_article=str(bucket["vendor_article"]),
                orders_qty=float(bucket["orders_qty"]),
                orders_sum=float(orders_sum),
                stock_qty=float(bucket["stock_qty"]),
                adv_sum=float(adv_sum),
                drr=(float(adv_sum) / float(orders_sum) * 100.0) if orders_sum else 0.0,
                orders_sum_dynamic=float(bucket["orders_sum_dynamic"]),
            )
            rows.append(row)
            total_orders_qty += row.orders_qty
            total_orders_sum += row.orders_sum
            total_stock_qty += row.stock_qty
            total_adv_sum += row.adv_sum
            total_orders_sum_dynamic += row.orders_sum_dynamic

        rows.sort(key=lambda x: (x.orders_sum, x.orders_qty, -x.adv_sum, x.vendor_article), reverse=True)
        total_drr = (total_adv_sum / total_orders_sum * 100.0) if total_orders_sum else 0.0
        return SalesReport(
            rows=rows,
            total_orders_qty=total_orders_qty,
            total_orders_sum=total_orders_sum,
            total_stock_qty=total_stock_qty,
            total_adv_sum=total_adv_sum,
            total_drr=total_drr,
            total_orders_sum_dynamic=total_orders_sum_dynamic,
        )

    async def get_all_cards(self) -> list[ProductCard]:
        cards: list[ProductCard] = []
        cursor: dict[str, Any] = {"limit": 100}
        while True:
            payload = {
                "settings": {
                    "sort": {"ascending": True},
                    "cursor": cursor,
                    "filter": {"withPhoto": -1},
                }
            }
            raw = await self._post_json(self.CONTENT_URL, payload)
            payload_cards = self._extract_cards(raw)
            for item in payload_cards:
                if not isinstance(item, dict):
                    continue
                nm_id = self._to_int(item.get("nmID") or item.get("nmId"))
                if nm_id is None:
                    continue
                vendor_article = self._extract_vendor_article(item)
                cards.append(ProductCard(nm_id=nm_id, vendor_article=vendor_article or str(nm_id)))

            cursor_resp = self._extract_cursor(raw)
            total = self._to_int(cursor_resp.get("total")) or len(payload_cards)
            limit = self._to_int(cursor.get("limit")) or 100
            if total < limit or not payload_cards:
                break

            updated_at = cursor_resp.get("updatedAt")
            nm_id_cursor = cursor_resp.get("nmID") or cursor_resp.get("nmId")
            if not updated_at or nm_id_cursor is None:
                break
            cursor = {
                "limit": limit,
                "updatedAt": updated_at,
                "nmID": nm_id_cursor,
            }
            await asyncio.sleep(0.65)

        uniq: dict[int, ProductCard] = {}
        for card in cards:
            uniq[card.nm_id] = card
        return list(uniq.values())

    async def get_funnel_report(
        self,
        start: str,
        end: str,
        past_start: str,
        past_end: str,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        limit = 1000
        offset = 0
        while True:
            payload = {
                "selectedPeriod": {"start": start, "end": end},
                "pastPeriod": {"start": past_start, "end": past_end},
                "nmIds": [],
                "brandNames": [],
                "subjectIds": [],
                "tagIds": [],
                "skipDeletedNm": False,
                "orderBy": {"field": "ordersSumRub", "mode": "desc"},
                "limit": limit,
                "offset": offset,
            }
            raw = await self._post_json(self.FUNNEL_URL, payload, retries=4, base_delay=22.0, request_kind="analytics")
            items = self._extract_items(raw)
            if not items:
                break
            rows.extend(items)
            if len(items) < limit:
                break
            offset += limit
        return rows

    async def get_adv_campaign_details(self) -> list[dict[str, Any]]:
        counts_raw = await self._get_json(self.ADV_COUNTS_URL, retries=3, base_delay=2.0, request_kind="adv")
        advert_ids = self._extract_advert_ids_from_counts(counts_raw)
        if not advert_ids:
            return []

        details: list[dict[str, Any]] = []
        for idx in range(0, len(advert_ids), 50):
            batch = advert_ids[idx : idx + 50]
            params = {"id": ",".join(str(x) for x in batch)}
            raw = await self._get_json(self.ADV_DETAILS_URL, params=params, retries=3, base_delay=2.0, request_kind="adv")
            adverts = raw.get("adverts") if isinstance(raw, dict) else None
            if isinstance(adverts, list):
                details.extend([x for x in adverts if isinstance(x, dict)])
            elif isinstance(raw, list):
                details.extend([x for x in raw if isinstance(x, dict)])
        return details

    async def get_adv_expenses(self, start_dt: date, end_dt: date) -> list[dict[str, Any]]:
        all_rows: list[dict[str, Any]] = []
        cursor = start_dt
        while cursor <= end_dt:
            part_end = min(end_dt, cursor.fromordinal(cursor.toordinal() + 30))
            params = {
                "from": cursor.strftime("%Y-%m-%d"),
                "to": part_end.strftime("%Y-%m-%d"),
            }
            raw = await self._get_json(self.ADV_EXPENSES_URL, params=params, retries=3, base_delay=2.0, request_kind="adv")
            if isinstance(raw, list):
                all_rows.extend([x for x in raw if isinstance(x, dict)])
            elif isinstance(raw, dict):
                for key in ("data", "adverts", "items"):
                    val = raw.get(key)
                    if isinstance(val, list):
                        all_rows.extend([x for x in val if isinstance(x, dict)])
                        break
            cursor = part_end.fromordinal(part_end.toordinal() + 1)
        return all_rows

    def _build_article_metrics(self, items: list[dict[str, Any]], article_by_nm: dict[int, str]) -> dict[str, dict[str, Any]]:
        metrics: dict[str, dict[str, Any]] = {}
        stock_nm_seen: dict[str, set[int]] = {}

        for item in items:
            if not isinstance(item, dict):
                continue
            product = item.get("product") if isinstance(item.get("product"), dict) else {}
            nm_id = self._to_int(item.get("nmId") or item.get("nmID") or product.get("nmId") or product.get("nmID"))
            if nm_id is None:
                continue

            statistic = item.get("statistic") if isinstance(item.get("statistic"), dict) else {}
            selected = self._pick_mapping(
                statistic.get("selected"),
                item.get("selectedPeriod"),
                item.get("selected"),
            )
            past = self._pick_mapping(
                statistic.get("past"),
                item.get("pastPeriod"),
                item.get("past"),
            )

            vendor_article = self._clean_text(
                product.get("vendorCode")
                or product.get("supplierArticle")
                or item.get("vendorCode")
                or item.get("supplierArticle")
                or item.get("article")
                or item.get("saName")
                or article_by_nm.get(nm_id, "")
            ) or article_by_nm.get(nm_id, str(nm_id))

            article_key = vendor_article.upper()
            bucket = metrics.get(article_key)
            if bucket is None:
                bucket = {
                    "nm_id": nm_id,
                    "vendor_article": vendor_article,
                    "orders_qty": 0.0,
                    "orders_sum": 0.0,
                    "stock_qty": 0.0,
                    "adv_sum": 0.0,
                    "orders_sum_dynamic": 0.0,
                    "nm_ids": set(),
                }
                metrics[article_key] = bucket
                stock_nm_seen[article_key] = set()

            orders_qty = self._to_number(
                selected.get("orderCount")
                or selected.get("ordersCount")
                or item.get("orderCount")
                or item.get("ordersCount")
                or 0
            )
            orders_sum = self._to_number(
                selected.get("orderSum")
                or selected.get("ordersSumRub")
                or selected.get("sum")
                or item.get("orderSum")
                or item.get("ordersSumRub")
                or item.get("sum")
                or 0
            )
            past_sum = self._to_number(
                past.get("orderSum")
                or past.get("ordersSumRub")
                or past.get("sum")
                or 0
            )
            stocks = product.get("stocks") if isinstance(product.get("stocks"), dict) else {}
            stock_qty = self._to_number(stocks.get("wb") or item.get("stocksWb") or 0)

            bucket["orders_qty"] += orders_qty
            bucket["orders_sum"] += orders_sum
            bucket["orders_sum_dynamic"] += orders_sum - past_sum
            bucket["nm_ids"].add(nm_id)
            if nm_id not in stock_nm_seen[article_key]:
                bucket["stock_qty"] += stock_qty
                stock_nm_seen[article_key].add(nm_id)

        return metrics

    def _build_adv_totals_by_advert(self, adv_rows: list[dict[str, Any]]) -> dict[int, float]:
        totals: dict[int, float] = {}
        seen: set[tuple[int, str, float]] = set()
        for row in adv_rows:
            if not isinstance(row, dict):
                continue
            advert_id = self._to_int(row.get("advertId") or row.get("id") or row.get("advert_id"))
            if advert_id is None:
                continue
            amount = self._to_number(row.get("updSum") or row.get("sum") or row.get("spent") or row.get("payment") or 0)
            if amount <= 0:
                continue
            uniq = (advert_id, self._clean_text(row.get("updTime") or row.get("date") or row.get("dt")), round(amount, 2))
            if uniq in seen:
                continue
            seen.add(uniq)
            totals[advert_id] = totals.get(advert_id, 0.0) + amount
        return totals

    def _apply_adv_to_articles(
        self,
        article_metrics: dict[str, dict[str, Any]],
        campaign_details: list[dict[str, Any]],
        adv_totals_by_advert: dict[int, float],
    ) -> None:
        nm_to_article_key: dict[int, str] = {}
        for article_key, bucket in article_metrics.items():
            for nm_id in bucket.get("nm_ids", set()):
                nm_to_article_key[int(nm_id)] = article_key

        for campaign in campaign_details:
            if not isinstance(campaign, dict):
                continue
            advert_id = self._to_int(campaign.get("advertId") or campaign.get("id") or campaign.get("advert_id"))
            if advert_id is None:
                continue
            amount = adv_totals_by_advert.get(advert_id, 0.0)
            if amount <= 0:
                continue

            nm_ids = sorted({nm for nm in self._extract_nm_ids_from_campaign(campaign) if nm in nm_to_article_key})
            if not nm_ids:
                continue

            article_keys = sorted({nm_to_article_key[nm] for nm in nm_ids})
            if not article_keys:
                continue

            weights = []
            for article_key in article_keys:
                orders_sum = float(article_metrics[article_key].get("orders_sum", 0.0) or 0.0)
                weights.append(max(orders_sum, 0.0))
            total_weight = sum(weights)

            if total_weight > 0:
                for article_key, weight in zip(article_keys, weights):
                    article_metrics[article_key]["adv_sum"] += amount * (weight / total_weight)
            else:
                part = amount / len(article_keys)
                for article_key in article_keys:
                    article_metrics[article_key]["adv_sum"] += part

    async def _post_json(
        self,
        url: str,
        payload: dict[str, Any],
        retries: int = 2,
        base_delay: float = 2.0,
        request_kind: str = "default",
    ) -> Any:
        await self._respect_rate_limit(request_kind)
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            for attempt in range(retries + 1):
                response = await client.post(url, headers=self._headers, json=payload)
                if response.status_code < 400:
                    return self._decode_json(response)
                if response.status_code == 429 and attempt < retries:
                    retry_after = self._retry_after_seconds(response, base_delay * (attempt + 1))
                    await asyncio.sleep(retry_after)
                    continue
                raise WBApiError(self._format_error(response))
        raise WBApiError("Не удалось выполнить запрос WB API")

    async def _get_json(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        retries: int = 2,
        base_delay: float = 2.0,
        request_kind: str = "default",
    ) -> Any:
        await self._respect_rate_limit(request_kind)
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            for attempt in range(retries + 1):
                response = await client.get(url, headers=self._headers, params=params)
                if response.status_code < 400:
                    return self._decode_json(response)
                if response.status_code == 429 and attempt < retries:
                    retry_after = self._retry_after_seconds(response, base_delay * (attempt + 1))
                    await asyncio.sleep(retry_after)
                    continue
                raise WBApiError(self._format_error(response))
        raise WBApiError("Не удалось выполнить запрос WB API")

    async def _respect_rate_limit(self, request_kind: str) -> None:
        now = time.monotonic()
        if request_kind == "analytics":
            elapsed = now - self._last_analytics_request_monotonic
            if elapsed < self._analytics_min_interval:
                await asyncio.sleep(self._analytics_min_interval - elapsed)
            self._last_analytics_request_monotonic = time.monotonic()
            return
        if request_kind == "adv":
            elapsed = now - self._last_adv_request_monotonic
            if elapsed < self._adv_min_interval:
                await asyncio.sleep(self._adv_min_interval - elapsed)
            self._last_adv_request_monotonic = time.monotonic()

    def _extract_advert_ids_from_counts(self, data: Any) -> list[int]:
        ids: set[int] = set()
        if isinstance(data, dict):
            adverts = data.get("adverts")
            if isinstance(adverts, list):
                for group in adverts:
                    if not isinstance(group, dict):
                        continue
                    advert_list = group.get("advert_list")
                    if not isinstance(advert_list, list):
                        continue
                    for item in advert_list:
                        if not isinstance(item, dict):
                            continue
                        advert_id = self._to_int(item.get("advertId"))
                        if advert_id is not None:
                            ids.add(advert_id)
        return sorted(ids)

    def _extract_nm_ids_from_campaign(self, data: Any) -> list[int]:
        found: set[int] = set()

        def walk(obj: Any, parent_key: str = "") -> None:
            if isinstance(obj, dict):
                for key, value in obj.items():
                    lk = str(key).lower()
                    if lk in {"nm", "nmid", "nmid", "nomenclature", "nomenclatureid"}:
                        if isinstance(value, list):
                            for item in value:
                                parsed = self._to_int(item)
                                if parsed is not None:
                                    found.add(parsed)
                        else:
                            parsed = self._to_int(value)
                            if parsed is not None:
                                found.add(parsed)
                    elif lk in {"nms", "nomenclatures", "nmids", "nm_settings", "nmsettings", "goods"}:
                        walk(value, lk)
                    else:
                        walk(value, lk)
            elif isinstance(obj, list):
                for item in obj:
                    walk(item, parent_key)
            else:
                if parent_key in {"nm", "nmid", "nmid", "nomenclature", "nomenclatureid"}:
                    parsed = self._to_int(obj)
                    if parsed is not None:
                        found.add(parsed)

        walk(data)
        return sorted(found)

    def _extract_cards(self, raw: Any) -> list[dict[str, Any]]:
        if isinstance(raw, dict):
            cards = raw.get("cards")
            if isinstance(cards, list):
                return [x for x in cards if isinstance(x, dict)]
            data = raw.get("data")
            if isinstance(data, dict) and isinstance(data.get("cards"), list):
                return [x for x in data["cards"] if isinstance(x, dict)]
            if isinstance(data, list):
                return [x for x in data if isinstance(x, dict)]
        elif isinstance(raw, list):
            return [x for x in raw if isinstance(x, dict)]
        return []

    def _extract_cursor(self, raw: Any) -> dict[str, Any]:
        if isinstance(raw, dict):
            cursor = raw.get("cursor")
            if isinstance(cursor, dict):
                return cursor
            data = raw.get("data")
            if isinstance(data, dict) and isinstance(data.get("cursor"), dict):
                return data["cursor"]
        return {}

    def _extract_items(self, raw: Any) -> list[dict[str, Any]]:
        if isinstance(raw, dict):
            for key in ("data", "items", "products", "cards"):
                val = raw.get(key)
                if isinstance(val, list):
                    return [x for x in val if isinstance(x, dict)]
                if isinstance(val, dict):
                    for sub_key in ("items", "data", "cards", "products"):
                        sub_val = val.get(sub_key)
                        if isinstance(sub_val, list):
                            return [x for x in sub_val if isinstance(x, dict)]
        elif isinstance(raw, list):
            return [x for x in raw if isinstance(x, dict)]
        return []

    def _extract_vendor_article(self, item: dict[str, Any]) -> str:
        vendor_article = self._clean_text(
            item.get("vendorCode") or item.get("supplierArticle") or item.get("article") or ""
        )
        if vendor_article:
            return vendor_article

        sizes = item.get("sizes")
        if isinstance(sizes, list):
            for size in sizes:
                if not isinstance(size, dict):
                    continue
                candidate = self._clean_text(
                    size.get("vendorCode") or size.get("supplierArticle") or size.get("article") or ""
                )
                if candidate:
                    return candidate
        return ""

    def _pick_mapping(self, *values: Any) -> dict[str, Any]:
        for value in values:
            if isinstance(value, dict):
                return value
        return {}

    def _decode_json(self, response: httpx.Response) -> Any:
        try:
            return response.json()
        except Exception as exc:
            text = response.text[:500].strip()
            raise WBApiError(f"WB API вернул не-JSON ответ: {text}") from exc

    def _format_error(self, response: httpx.Response) -> str:
        text = response.text.strip()
        snippet = text[:700] if text else ""
        return f"Ошибка запроса WB API: {response.status_code} {response.reason_phrase}: {response.url}\n{snippet}"

    def _retry_after_seconds(self, response: httpx.Response, default: float) -> float:
        raw = response.headers.get("Retry-After")
        try:
            if raw is not None:
                return max(float(raw), default)
        except Exception:
            pass
        return default

    def _to_number(self, value: Any) -> float:
        if value is None:
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        try:
            text = str(value).replace("\xa0", " ").replace(" ", "").replace(",", ".")
            return float(text)
        except Exception:
            return 0.0

    def _to_int(self, value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(str(value).strip())
        except Exception:
            return None

    def _clean_text(self, value: Any) -> str:
        text = str(value or "").strip()
        return " ".join(text.split())
