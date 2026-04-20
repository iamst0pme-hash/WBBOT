from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

import httpx

from .reporting import SalesRow, to_decimal


CONTENT_API = "https://content-api.wildberries.ru/content/v2/get/cards/list"
HISTORY_API = "https://seller-analytics-api.wildberries.ru/api/analytics/v3/sales-funnel/products/history"
STOCKS_API = "https://seller-analytics-api.wildberries.ru/api/v2/stocks-report/products/products"


class WBApiError(RuntimeError):
    pass


@dataclass(frozen=True)
class ProductCard:
    nm_id: int
    vendor_code: str


class TTLCache:
    def __init__(self) -> None:
        self._store: dict[str, tuple[float, Any]] = {}

    def get(self, key: str) -> Any | None:
        item = self._store.get(key)
        if not item:
            return None
        expires_at, value = item
        if time.time() >= expires_at:
            self._store.pop(key, None)
            return None
        return value

    def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        self._store[key] = (time.time() + ttl_seconds, value)


class RequestGate:
    def __init__(self, min_interval_seconds: float) -> None:
        self._min_interval_seconds = min_interval_seconds
        self._lock = asyncio.Lock()
        self._last_call = 0.0

    async def wait_turn(self) -> None:
        async with self._lock:
            now = time.monotonic()
            wait_for = self._min_interval_seconds - (now - self._last_call)
            if wait_for > 0:
                await asyncio.sleep(wait_for)
            self._last_call = time.monotonic()


class WBClient:
    def __init__(self, analytics_token: str, content_token: str) -> None:
        self.analytics_token = analytics_token
        self.content_token = content_token
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=30.0))
        self._cards_cache = TTLCache()
        self._history_cache = TTLCache()
        self._stocks_cache = TTLCache()
        self._history_gate = RequestGate(min_interval_seconds=20.5)
        self._stocks_gate = RequestGate(min_interval_seconds=20.5)
        self._content_gate = RequestGate(min_interval_seconds=0.7)

    async def close(self) -> None:
        await self._client.aclose()

    async def list_product_cards(self) -> list[ProductCard]:
        cache_key = "all_cards"
        cached = self._cards_cache.get(cache_key)
        if cached is not None:
            return cached

        cards: list[ProductCard] = []
        cursor: dict[str, Any] = {"limit": 100}
        seen_nm_ids: set[int] = set()

        while True:
            await self._content_gate.wait_turn()
            response = await self._client.post(
                CONTENT_API,
                headers={"Authorization": self.content_token},
                params={"locale": "ru"},
                json={
                    "settings": {
                        "sort": {"ascending": True},
                        "filter": {"withPhoto": -1},
                        "cursor": cursor,
                    }
                },
            )
            self._raise_for_status(response)

            payload = response.json()
            data = payload.get("cards") or payload.get("data") or []
            response_cursor = payload.get("cursor") or {}
            total = int(response_cursor.get("total") or len(data))

            for item in data:
                nm_id = item.get("nmID") or item.get("nmId")
                vendor_code = item.get("vendorCode") or item.get("supplierArticle") or ""
                if not nm_id:
                    continue
                nm_id_int = int(nm_id)
                if nm_id_int in seen_nm_ids:
                    continue
                seen_nm_ids.add(nm_id_int)
                cards.append(ProductCard(nm_id=nm_id_int, vendor_code=str(vendor_code).strip()))

            if not data or total < int(cursor.get("limit", 100)):
                break

            updated_at = response_cursor.get("updatedAt")
            nm_id_cursor = response_cursor.get("nmID") or response_cursor.get("nmId")
            if not updated_at or not nm_id_cursor:
                break

            cursor = {
                "limit": 100,
                "updatedAt": updated_at,
                "nmID": nm_id_cursor,
            }

        self._cards_cache.set(cache_key, cards, ttl_seconds=1800)
        return cards

    async def fetch_sales_rows(self, start: date, end: date, compare_start: date, compare_end: date) -> list[SalesRow]:
        cards = await self.list_product_cards()
        if not cards:
            return []

        nm_ids = [card.nm_id for card in cards]
        article_by_nm = {card.nm_id: card.vendor_code or str(card.nm_id) for card in cards}

        current_items = await self._history_for_period(nm_ids, start, end)
        compare_items = await self._history_for_period(nm_ids, compare_start, compare_end)
        stocks = await self._fetch_current_stocks(nm_ids, current_date=end)

        current_agg = self._aggregate_history(current_items)
        compare_agg = self._aggregate_history(compare_items)

        all_nm_ids = set(article_by_nm) | set(current_agg) | set(compare_agg) | set(stocks)
        rows: list[SalesRow] = []
        for nm_id in sorted(all_nm_ids):
            article = article_by_nm.get(nm_id, str(nm_id))
            current_metrics = current_agg.get(nm_id, {"qty": Decimal("0"), "amount": Decimal("0")})
            compare_metrics = compare_agg.get(nm_id, {"qty": Decimal("0"), "amount": Decimal("0")})
            rows.append(
                SalesRow(
                    article=article,
                    orders_qty=current_metrics["qty"],
                    orders_amount=current_metrics["amount"],
                    stock_qty=stocks.get(nm_id, Decimal("0")),
                    dynamics_amount=current_metrics["amount"] - compare_metrics["amount"],
                )
            )

        rows.sort(key=lambda row: (row.orders_amount, row.orders_qty, row.article), reverse=True)
        return rows

    async def _history_for_period(self, nm_ids: list[int], start: date, end: date) -> list[dict[str, Any]]:
        all_items: list[dict[str, Any]] = []
        for index in range(0, len(nm_ids), 20):
            chunk = nm_ids[index:index + 20]
            cache_key = f"history:{start.isoformat()}:{end.isoformat()}:{','.join(map(str, chunk))}"
            cached = self._history_cache.get(cache_key)
            if cached is not None:
                all_items.extend(cached)
                continue

            payload = {
                "selectedPeriod": {
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                },
                "nmIds": chunk,
                "skipDeletedNm": False,
                "aggregationLevel": "day",
            }

            data = await self._post_with_retry(
                url=HISTORY_API,
                headers={"Authorization": self.analytics_token},
                json_payload=payload,
                gate=self._history_gate,
                max_attempts=4,
            )

            if not isinstance(data, list):
                raise WBApiError("WB history вернул неожиданный формат ответа")

            self._history_cache.set(cache_key, data, ttl_seconds=300)
            all_items.extend(data)

        return all_items

    async def _fetch_current_stocks(self, nm_ids: list[int], current_date: date) -> dict[int, Decimal]:
        cache_key = f"stocks:{current_date.isoformat()}:{','.join(map(str, nm_ids))}"
        cached = self._stocks_cache.get(cache_key)
        if cached is not None:
            return cached

        stocks: dict[int, Decimal] = {}
        for index in range(0, len(nm_ids), 1000):
            chunk = nm_ids[index:index + 1000]
            offset = 0
            while True:
                payload = {
                    "nmIDs": chunk,
                    "currentPeriod": {
                        "start": current_date.isoformat(),
                        "end": current_date.isoformat(),
                    },
                    "stockType": "wb",
                    "skipDeletedNm": False,
                    "orderBy": {
                        "field": "ordersSum",
                        "mode": "desc",
                    },
                    "availabilityFilters": [],
                    "limit": 1000,
                    "offset": offset,
                }

                data = await self._post_with_retry(
                    url=STOCKS_API,
                    headers={"Authorization": self.analytics_token},
                    json_payload=payload,
                    gate=self._stocks_gate,
                    max_attempts=4,
                )
                items = (((data or {}).get("data") or {}).get("items")) or []
                if not isinstance(items, list):
                    raise WBApiError("WB stocks вернул неожиданный формат ответа")

                for item in items:
                    nm_id = item.get("nmID") or item.get("nmId")
                    metrics = item.get("metrics") or {}
                    if not nm_id:
                        continue
                    stocks[int(nm_id)] = to_decimal(metrics.get("stockCount") or 0)

                if len(items) < 1000:
                    break
                offset += 1000

        self._stocks_cache.set(cache_key, stocks, ttl_seconds=300)
        return stocks

    def _aggregate_history(self, items: list[dict[str, Any]]) -> dict[int, dict[str, Decimal]]:
        totals: dict[int, dict[str, Decimal]] = {}
        for item in items:
            product = item.get("product") or {}
            nm_id_raw = product.get("nmId") or product.get("nmID")
            if not nm_id_raw:
                continue
            nm_id = int(nm_id_raw)
            bucket = totals.setdefault(
                nm_id,
                {"qty": Decimal("0"), "amount": Decimal("0")},
            )
            history = item.get("history") or []
            for day_row in history:
                bucket["qty"] += to_decimal(day_row.get("orderCount") or day_row.get("ordersCount") or 0)
                bucket["amount"] += to_decimal(day_row.get("orderSum") or day_row.get("ordersSumRub") or day_row.get("sum") or 0)
        return totals

    async def _post_with_retry(
        self,
        url: str,
        headers: dict[str, str],
        json_payload: dict[str, Any],
        gate: RequestGate,
        max_attempts: int,
    ) -> Any:
        last_error: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            await gate.wait_turn()
            try:
                response = await self._client.post(url, headers=headers, json=json_payload)
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    if retry_after and retry_after.isdigit():
                        await asyncio.sleep(int(retry_after))
                    else:
                        await asyncio.sleep(20 * attempt)
                    continue
                self._raise_for_status(response)
                return response.json()
            except (httpx.HTTPError, WBApiError) as exc:
                last_error = exc
                if attempt >= max_attempts:
                    break
                await asyncio.sleep(min(5 * attempt, 20))

        raise WBApiError(f"Не удалось получить ответ WB API: {last_error}")

    @staticmethod
    def _raise_for_status(response: httpx.Response) -> None:
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            body = exc.response.text[:1000]
            raise WBApiError(
                f"{exc.response.status_code} {exc.request.url} :: {body}"
            ) from exc
