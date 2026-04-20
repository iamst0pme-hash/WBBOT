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
        self._timeout = httpx.Timeout(120.0, connect=30.0)

    async def export_sales_report(
        self,
        current_start: date,
        current_end: date,
        past_start: date,
        past_end: date,
    ) -> list[SalesRow]:
        cards = await self.get_all_cards()
        current_items = await self.fetch_funnel_all(
            start=current_start.isoformat(),
            end=current_end.isoformat(),
        )
        past_items = await self.fetch_funnel_all(
            start=past_start.isoformat(),
            end=past_end.isoformat(),
        )

        article_by_nm: dict[int, str] = {card.nm_id: card.vendor_article for card in cards}
        current_by_article = self._aggregate_funnel_rows(current_items, article_by_nm)
        past_by_article = self._aggregate_funnel_rows(past_items, article_by_nm)

        all_articles = sorted(set(current_by_article) | set(past_by_article))
        rows: list[SalesRow] = []
        for article_key in all_articles:
            current = current_by_article.get(article_key, {})
            past = past_by_article.get(article_key, {})
            vendor_article = str(current.get("vendor_article") or past.get("vendor_article") or article_key).strip()
            nm_id = self._to_int(current.get("nm_id") or past.get("nm_id")) or 0
            orders_sum = self._to_number(current.get("orders_sum"))
            past_sum = self._to_number(past.get("orders_sum"))
            rows.append(
                SalesRow(
                    nm_id=nm_id,
                    vendor_article=vendor_article,
                    orders_qty=self._to_number(current.get("orders_qty")),
                    orders_sum=orders_sum,
                    stock_qty=self._to_number(current.get("stock_qty")),
                    orders_sum_dynamic=orders_sum - past_sum,
                )
            )

        rows.sort(key=lambda x: (x.orders_sum, x.orders_qty, x.vendor_article.casefold()), reverse=True)
        return rows

    async def get_all_cards(self) -> list[ProductCard]:
        cards: list[ProductCard] = []
        cursor_nm: int | None = None
        cursor_updated: str | None = None
        page = 0

        while True:
            page += 1
            payload: dict[str, Any] = {
                "settings": {
                    "cursor": {"limit": 100},
                    "filter": {"withPhoto": -1},
                }
            }
            if cursor_nm:
                payload["settings"]["cursor"]["nmID"] = cursor_nm
            if cursor_updated:
                payload["settings"]["cursor"]["updatedAt"] = cursor_updated

            raw = await self._post_json(self.CONTENT_URL, payload, retries=4, base_delay=2.0)
            page_cards = self._extract_cards(raw)
            if not page_cards:
                break

            for item in page_cards:
                nm_id = self._nm_id_from_any(item)
                if not nm_id:
                    continue
                article = self._article_from_card(item) or str(nm_id)
                cards.append(ProductCard(nm_id=nm_id, vendor_article=article))

            cursor = self._extract_cursor(raw)
            next_nm = self._to_int(cursor.get("nmID") or cursor.get("nmId"))
            next_updated = str(cursor.get("updatedAt") or "").strip() or None
            if not next_nm or next_nm == cursor_nm:
                break

            cursor_nm = next_nm
            cursor_updated = next_updated
            await asyncio.sleep(0.65)

        uniq: dict[int, ProductCard] = {}
        for card in cards:
            uniq[card.nm_id] = card
        return list(uniq.values())

    async def fetch_funnel_all(self, start: str, end: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        limit = 1000
        offset = 0
        page = 0

        while True:
            page += 1
            payload = {
                "selectedPeriod": {"start": start, "end": end},
                "timezone": "Europe/Moscow",
                "orderBy": {"field": "ordersSumRub", "mode": "desc"},
                "page": {"limit": limit, "offset": offset},
            }
            raw = await self._post_json(self.FUNNEL_URL, payload, retries=4, base_delay=5.0)
            part = self._extract_items(raw)
            items.extend(part)
            if len(part) < limit:
                break
            offset += limit
            await asyncio.sleep(0.65)

        return items

    def _aggregate_funnel_rows(
        self,
        items: list[dict[str, Any]],
        article_by_nm: dict[int, str],
    ) -> dict[str, dict[str, Any]]:
        rows_by_article: dict[str, dict[str, Any]] = {}
        stock_nm_seen_by_article: dict[str, set[int]] = {}

        for item in items:
            if not isinstance(item, dict):
                continue

            product = item.get("product") if isinstance(item.get("product"), dict) else {}
            statistic = item.get("statistic") if isinstance(item.get("statistic"), dict) else {}
            selected = self._pick_mapping(
                statistic.get("selected"),
                item.get("selected"),
                item.get("selectedPeriod"),
            )

            nm_id = self._nm_id_from_any(product) or self._nm_id_from_any(item)
            raw_article = self._clean_text(
                product.get("vendorCode")
                or product.get("supplierArticle")
                or item.get("vendorCode")
                or item.get("supplierArticle")
                or item.get("article")
                or item.get("saName")
            )
            preferred_article = article_by_nm.get(nm_id or 0, "")
            article = self._choose_better_article(preferred_article, raw_article)
            if not article:
                if nm_id:
                    article = article_by_nm.get(nm_id, str(nm_id))
                else:
                    continue

            article_key = self._canonical_article(article)
            bucket = rows_by_article.get(article_key)
            if bucket is None:
                bucket = {
                    "vendor_article": article,
                    "nm_id": nm_id or 0,
                    "orders_qty": 0.0,
                    "orders_sum": 0.0,
                    "stock_qty": 0.0,
                }
                rows_by_article[article_key] = bucket

            if nm_id and not bucket.get("nm_id"):
                bucket["nm_id"] = nm_id

            qty = self._to_number(
                selected.get("orderCount")
                or selected.get("ordersCount")
                or item.get("orderCount")
                or item.get("ordersCount")
                or 0
            )
            amount = self._to_number(
                selected.get("orderSum")
                or selected.get("ordersSumRub")
                or selected.get("sum")
                or item.get("orderSum")
                or item.get("ordersSumRub")
                or item.get("sum")
                or 0
            )
            stocks = product.get("stocks") if isinstance(product.get("stocks"), dict) else {}
            stock = self._to_number(stocks.get("wb") or item.get("stocksWb") or 0)

            bucket["orders_qty"] += qty
            bucket["orders_sum"] += amount

            if nm_id:
                seen = stock_nm_seen_by_article.setdefault(article_key, set())
                if nm_id not in seen:
                    bucket["stock_qty"] += stock
                    seen.add(nm_id)
            else:
                bucket["stock_qty"] += stock

        return rows_by_article

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
                    response = await client.post(url, headers=self._headers, json=payload)
                    if response.status_code == 429:
                        retry_after = response.headers.get("Retry-After")
                        delay = float(retry_after) if retry_after else base_delay * attempt
                        await asyncio.sleep(delay)
                        last_error = WBApiError(f"429 Too Many Requests: {url}")
                        continue
                    response.raise_for_status()
                    return response.json()
                except (httpx.HTTPError, ValueError) as exc:
                    last_error = exc
                    if attempt >= retries:
                        break
                    await asyncio.sleep(base_delay * attempt)

        raise WBApiError(f"Ошибка запроса WB API: {last_error}")

    @staticmethod
    def _extract_cards(raw: Any) -> list[dict[str, Any]]:
        if isinstance(raw, dict):
            cards = raw.get("cards")
            if isinstance(cards, list):
                return [x for x in cards if isinstance(x, dict)]
            data = raw.get("data")
            if isinstance(data, dict):
                nested_cards = data.get("cards")
                if isinstance(nested_cards, list):
                    return [x for x in nested_cards if isinstance(x, dict)]
        if isinstance(raw, list):
            return [x for x in raw if isinstance(x, dict)]
        return []

    @staticmethod
    def _extract_cursor(raw: Any) -> dict[str, Any]:
        if isinstance(raw, dict):
            cursor = raw.get("cursor")
            if isinstance(cursor, dict):
                return cursor
            data = raw.get("data")
            if isinstance(data, dict):
                nested = data.get("cursor")
                if isinstance(nested, dict):
                    return nested
        return {}

    @staticmethod
    def _extract_items(raw: Any) -> list[dict[str, Any]]:
        if isinstance(raw, list):
            return [x for x in raw if isinstance(x, dict)]
        if not isinstance(raw, dict):
            return []
        for key in ("data", "items", "products", "result"):
            value = raw.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
            if isinstance(value, dict):
                for nested_key in ("items", "products", "data"):
                    nested = value.get(nested_key)
                    if isinstance(nested, list):
                        return [x for x in nested if isinstance(x, dict)]
        return []

    @staticmethod
    def _pick_mapping(*values: Any) -> dict[str, Any]:
        for value in values:
            if isinstance(value, dict):
                return value
        return {}

    @staticmethod
    def _nm_id_from_any(obj: Any) -> int | None:
        if not isinstance(obj, dict):
            return None
        value = obj.get("nmID") or obj.get("nmId")
        try:
            return int(value) if value not in (None, "") else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _extract_candidate_articles(obj: Any) -> list[str]:
        found: list[str] = []

        def add(value: Any) -> None:
            if isinstance(value, list):
                for item in value:
                    add(item)
                return
            txt = WBClient._clean_text(value)
            if txt and txt not in found:
                found.append(txt)

        if not isinstance(obj, dict):
            return found

        for key in ("vendorCode", "supplierArticle", "article", "saName"):
            add(obj.get(key))

        sizes = obj.get("sizes")
        if isinstance(sizes, list):
            for size in sizes:
                if not isinstance(size, dict):
                    continue
                for key in ("vendorCode", "supplierArticle", "article"):
                    add(size.get(key))

        return found

    @staticmethod
    def _article_looks_like_barcode(article: str) -> bool:
        txt = WBClient._clean_text(article)
        if not txt:
            return False
        digits_only = "".join(ch for ch in txt if ch.isdigit())
        if not digits_only:
            return False
        if digits_only == txt and len(digits_only) >= 8:
            return True
        return len(digits_only) >= 12 and len(digits_only) >= max(8, len(txt) - 2)

    @classmethod
    def _choose_better_article(cls, primary: str, secondary: str) -> str:
        a = cls._clean_text(primary)
        b = cls._clean_text(secondary)
        if a and not cls._article_looks_like_barcode(a):
            return a
        if b and not cls._article_looks_like_barcode(b):
            return b
        return a or b

    @classmethod
    def _article_from_card(cls, card: dict[str, Any]) -> str:
        best = ""
        for art in cls._extract_candidate_articles(card):
            best = cls._choose_better_article(best, art)
        return best

    @staticmethod
    def _canonical_article(article: str) -> str:
        return WBClient._clean_text(article).upper()

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
