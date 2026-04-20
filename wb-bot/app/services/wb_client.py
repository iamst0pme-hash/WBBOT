from __future__ import annotations

import asyncio
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
class SalesRow:
    nm_id: int
    vendor_article: str
    orders_qty: float
    orders_sum: float
    stock_qty: float
    orders_sum_dynamic: float


class WBClient:
    CONTENT_URL = "https://content-api.wildberries.ru/content/v2/get/cards/list"
    FUNNEL_URL = "https://seller-analytics-api.wildberries.ru/api/analytics/v3/sales-funnel/products"

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key.strip()
        self._headers = {"Authorization": self.api_key}
        self._timeout = httpx.Timeout(60.0, connect=20.0)
        self._analytics_min_interval = 22.0
        self._last_analytics_request_monotonic = 0.0

    async def export_sales_report(
        self,
        current_start: date,
        current_end: date,
        past_start: date,
        past_end: date,
    ) -> list[SalesRow]:
        cards = await self.get_all_cards()
        article_by_nm = {card.nm_id: card.vendor_article for card in cards}

        funnel_rows = await self.get_funnel_rows(
            start=current_start.isoformat(),
            end=current_end.isoformat(),
            past_start=past_start.isoformat(),
            past_end=past_end.isoformat(),
        )

        aggregated: dict[str, SalesRow] = {}

        for item in funnel_rows:
            if not isinstance(item, dict):
                continue

            product = item.get("product") if isinstance(item.get("product"), dict) else {}
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

            nm_id = self._to_int(
                item.get("nmId")
                or item.get("nmID")
                or product.get("nmId")
                or product.get("nmID")
            )
            if nm_id is None:
                continue

            vendor_article = self._clean_text(
                product.get("vendorCode")
                or product.get("supplierArticle")
                or item.get("vendorCode")
                or item.get("supplierArticle")
                or item.get("article")
                or item.get("saName")
                or article_by_nm.get(nm_id, "")
            )
            if not vendor_article:
                vendor_article = str(nm_id)

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
            stock_qty = self._to_number(
                (product.get("stocks") or {}).get("wb") if isinstance(product.get("stocks"), dict) else None
            )
            if not stock_qty:
                stock_qty = self._to_number(item.get("stocksWb") or item.get("stock") or 0)

            key = vendor_article.casefold()
            existing = aggregated.get(key)
            if existing is None:
                aggregated[key] = SalesRow(
                    nm_id=nm_id,
                    vendor_article=vendor_article,
                    orders_qty=orders_qty,
                    orders_sum=orders_sum,
                    stock_qty=stock_qty,
                    orders_sum_dynamic=orders_sum - past_sum,
                )
            else:
                existing.orders_qty += orders_qty
                existing.orders_sum += orders_sum
                existing.stock_qty += stock_qty
                existing.orders_sum_dynamic += orders_sum - past_sum

        rows = list(aggregated.values())
        rows.sort(key=lambda x: (x.orders_sum, x.orders_qty, x.vendor_article.casefold()), reverse=True)
        return rows

    async def get_all_cards(self) -> list[ProductCard]:
        cards: list[ProductCard] = []
        cursor: dict[str, Any] = {"limit": 100}

        while True:
            payload = {
                "settings": {
                    "cursor": cursor,
                    "filter": {"withPhoto": -1},
                }
            }
            raw = await self._post_json(self.CONTENT_URL, payload, retries=3, base_delay=2.0)
            payload_cards = self._extract_cards(raw)
            for item in payload_cards:
                nm_id = self._to_int(item.get("nmID") or item.get("nmId"))
                if nm_id is None:
                    continue
                vendor_article = self._extract_vendor_article(item)
                cards.append(ProductCard(nm_id=nm_id, vendor_article=vendor_article or str(nm_id)))

            if not payload_cards:
                break

            cursor_resp = self._extract_cursor(raw)
            total = self._to_int(cursor_resp.get("total")) or len(payload_cards)
            limit = self._to_int(cursor.get("limit")) or 100
            if total < limit:
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

    async def get_funnel_rows(
        self,
        *,
        start: str,
        end: str,
        past_start: str,
        past_end: str,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
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
                "skipDeletedNm": True,
                "orderBy": {"field": "openCard", "mode": "desc"},
                "limit": limit,
                "offset": offset,
            }
            raw = await self._post_json(self.FUNNEL_URL, payload, retries=4, base_delay=22.0)
            part = self._extract_items(raw)
            items.extend(part)
            if len(part) < limit:
                break
            offset += limit
            await asyncio.sleep(22.0)
        return items

    async def _wait_for_analytics_slot(self, url: str) -> None:
        if "seller-analytics-api.wildberries.ru" not in url:
            return
        loop = asyncio.get_running_loop()
        now = loop.time()
        elapsed = now - self._last_analytics_request_monotonic
        if self._last_analytics_request_monotonic and elapsed < self._analytics_min_interval:
            await asyncio.sleep(self._analytics_min_interval - elapsed)
        self._last_analytics_request_monotonic = loop.time()

    async def _post_json(
        self,
        url: str,
        payload: dict[str, Any],
        *,
        retries: int = 3,
        base_delay: float = 2.0,
    ) -> Any:
        last_error: Exception | None = None
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            for attempt in range(1, retries + 1):
                try:
                    await self._wait_for_analytics_slot(url)
                    response = await client.post(url, headers=self._headers, json=payload)
                    if response.status_code == 429:
                        retry_after = response.headers.get("Retry-After")
                        delay = float(retry_after) if retry_after else max(base_delay * attempt, self._analytics_min_interval)
                        await asyncio.sleep(delay)
                        self._last_analytics_request_monotonic = 0.0
                        last_error = WBApiError(f"429 Too Many Requests: {url}")
                        continue
                    if response.status_code >= 400:
                        try:
                            details = response.text[:1000]
                        except Exception:
                            details = ""
                        raise WBApiError(f"{response.status_code} {response.reason_phrase}: {url}. {details}".strip())
                    return response.json()
                except (httpx.HTTPError, ValueError, WBApiError) as exc:
                    last_error = exc
                    if attempt >= retries:
                        break
                    await asyncio.sleep(base_delay * attempt)

        raise WBApiError(f"Ошибка запроса WB API: {last_error}")

    @staticmethod
    def _extract_cards(raw: Any) -> list[dict[str, Any]]:
        if isinstance(raw, list):
            return [x for x in raw if isinstance(x, dict)]
        if not isinstance(raw, dict):
            return []
        direct = raw.get("cards")
        if isinstance(direct, list):
            return [x for x in direct if isinstance(x, dict)]
        data = raw.get("data")
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            cards = data.get("cards")
            if isinstance(cards, list):
                return [x for x in cards if isinstance(x, dict)]
        return []

    @staticmethod
    def _extract_cursor(raw: Any) -> dict[str, Any]:
        if not isinstance(raw, dict):
            return {}
        cursor = raw.get("cursor")
        if isinstance(cursor, dict):
            return cursor
        data = raw.get("data")
        if isinstance(data, dict):
            cursor = data.get("cursor")
            if isinstance(cursor, dict):
                return cursor
        return {}

    @staticmethod
    def _extract_items(raw: Any) -> list[dict[str, Any]]:
        if isinstance(raw, list):
            return [x for x in raw if isinstance(x, dict)]
        if not isinstance(raw, dict):
            return []
        for key in ("data", "items", "cards", "products", "result"):
            value = raw.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
            if isinstance(value, dict):
                for key2 in ("items", "cards", "products", "result"):
                    nested = value.get(key2)
                    if isinstance(nested, list):
                        return [x for x in nested if isinstance(x, dict)]
        return []

    @staticmethod
    def _pick_mapping(*values: Any) -> dict[str, Any]:
        for value in values:
            if isinstance(value, dict):
                return value
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        return item
        return {}

    @staticmethod
    def _extract_vendor_article(item: dict[str, Any]) -> str:
        vendor_article = item.get("vendorCode") or item.get("supplierArticle") or item.get("article") or ""
        if vendor_article:
            return str(vendor_article).strip()
        sizes = item.get("sizes") or []
        if isinstance(sizes, list):
            for size in sizes:
                if not isinstance(size, dict):
                    continue
                skus = size.get("skus") or []
                if isinstance(skus, list):
                    for sku in skus:
                        if sku:
                            return str(sku).strip()
        return str(item.get("nmID") or item.get("nmId") or "").strip()

    @staticmethod
    def _clean_text(value: Any) -> str:
        return str(value or "").strip()

    @staticmethod
    def _to_number(value: Any) -> float:
        if value in (None, ""):
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).replace("\xa0", "").replace(" ", "").replace(",", ".")
        try:
            return float(text)
        except ValueError:
            return 0.0

    @staticmethod
    def _to_int(value: Any) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
