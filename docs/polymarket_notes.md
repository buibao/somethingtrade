# Polymarket API Notes

Phase 2 uses only public market-data endpoints. No API keys, wallet private keys, signatures, or derived credentials are needed for discovery or the public market websocket.

Official references:
- Polymarket market-data overview: https://docs.polymarket.com/market-data/overview
- Polymarket quickstart market fetch example: https://docs.polymarket.com/quickstart
- Polymarket CLOB market websocket channel: https://docs.polymarket.com/market-data/websocket/market-channel

## Discovery

- Public market discovery uses Gamma: `https://gamma-api.polymarket.com/markets`.
- The request filters with `active=true`, `closed=false`, and a local `limit`.
- The bot filters locally for BTC/ETH short-duration up/down style markets because naming conventions can vary.
- Token IDs are read from `clobTokenIds` and zipped with the actual Gamma `outcomes` array.
- Directional mapping is not inferred from token order. The bot parses actual outcomes and only accepts markets where UP/DOWN direction can be mapped confidently.
- Cached fields are public metadata only:
  - `condition_id`
  - `market_id`
  - `market_slug`
  - `question`
  - `end_time`
  - `up_token_id`
  - `down_token_id`
  - `yes_token_id`
  - `no_token_id`
  - `token_outcomes`
  - `tick_size`
  - `min_order_size`

## Market WebSocket

- Public endpoint: `wss://ws-subscriptions-clob.polymarket.com/ws/market`.
- Subscription is by token/asset IDs:

```json
{
  "assets_ids": ["<token_id_1>", "<token_id_2>"],
  "type": "market",
  "custom_feature_enabled": true
}
```

- `custom_feature_enabled: true` enables extra public market events including `best_bid_ask`.
- Supported normalized messages:
  - `book` -> `PolymarketQuote`
  - `price_change` -> `PolymarketQuote`
  - `best_bid_ask` -> `PolymarketQuote`
  - `last_trade_price` / `trade` -> `MarketTick`

## Local Assumptions

- `best_bid_size` and `best_ask_size` are tracked separately. A hypothetical BUY uses `best_ask_size`; a hypothetical SELL would use `best_bid_size`.
- `available_liquidity_at_best` is only a backward-compatible computed field.
- Polymarket timestamps are parsed as seconds, milliseconds, microseconds, or nanoseconds based on digit length; current docs show millisecond timestamps.
- Quote staleness is enforced in `MarketState` using `POLYMARKET_MAX_QUOTE_AGE_MS`.
- This phase deliberately does not use authenticated CLOB trading endpoints.
