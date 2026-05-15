import argparse
import asyncio
from collections.abc import Sequence

from app.config.settings import get_settings
from app.execution.paper_executor import PaperExecutor
from app.marketdata.binance_ws import BinanceWSClient
from app.marketdata.polymarket_discovery import (
    PolymarketDiscoveryClient,
    PolymarketMarketMetadata,
    flatten_token_ids,
)
from app.marketdata.polymarket_ws import PolymarketWSClient
from app.state.market_state import MarketState


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m app.main")
    subparsers = parser.add_subparsers(dest="command")

    monitor = subparsers.add_parser(
        "binance-monitor",
        help="Print compact Binance realtime state once per second.",
    )
    monitor.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated Binance symbols, for example BTCUSDT,ETHUSDT.",
    )
    monitor.add_argument(
        "--url",
        default=None,
        help="Binance websocket base URL.",
    )

    poly_monitor = subparsers.add_parser(
        "polymarket-monitor",
        help="Discover short-duration crypto markets and print live CLOB quotes.",
    )
    poly_monitor.add_argument(
        "--gamma-url",
        default=None,
        help="Polymarket Gamma API base URL.",
    )
    poly_monitor.add_argument(
        "--ws-url",
        default=None,
        help="Polymarket market websocket URL.",
    )
    poly_monitor.add_argument(
        "--cache-path",
        default=None,
        help="Market metadata cache path.",
    )
    poly_monitor.add_argument(
        "--max-quote-age-ms",
        type=float,
        default=None,
        help="Reject Polymarket quotes older than this many milliseconds.",
    )

    return parser.parse_args(argv)


async def run(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    if args.command == "binance-monitor":
        await run_binance_monitor(args)
        return
    if args.command == "polymarket-monitor":
        await run_polymarket_monitor(args)
        return

    settings = get_settings()
    if settings.mode != "paper":
        raise RuntimeError("Only MODE=paper is supported in this skeleton.")

    _executor = PaperExecutor()


async def run_binance_monitor(args: argparse.Namespace) -> None:
    settings = get_settings()
    symbols = _parse_symbols(args.symbols) if args.symbols else settings.binance_symbols
    state = MarketState()
    client = BinanceWSClient(url=args.url or settings.binance_ws_url, symbols=symbols)

    ingest_task = asyncio.create_task(_ingest_binance(client, state))
    try:
        while True:
            await asyncio.sleep(1.0)
            lines = state.compact_lines()
            if lines:
                print(" | ".join(lines), flush=True)
            else:
                print(f"waiting for Binance data symbols={','.join(symbols)}", flush=True)
    finally:
        ingest_task.cancel()
        try:
            await ingest_task
        except asyncio.CancelledError:
            pass


async def run_polymarket_monitor(args: argparse.Namespace) -> None:
    settings = get_settings()
    discovery = PolymarketDiscoveryClient(
        gamma_url=args.gamma_url or settings.polymarket_gamma_url,
        cache_path=args.cache_path or settings.polymarket_market_cache_path,
    )
    markets = await discovery.discover(write_cache=True)
    if not markets:
        cached = discovery.read_cache()
        markets = tuple(cached.markets)
        if markets:
            print("using cached Polymarket markets", flush=True)
    if not markets:
        print("no active BTC/ETH 5m/15m Polymarket markets discovered", flush=True)
        return

    _print_polymarket_markets(markets)

    state = MarketState(
        max_polymarket_quote_age_ms=args.max_quote_age_ms
        or settings.polymarket_max_quote_age_ms
    )
    client = PolymarketWSClient(
        url=args.ws_url or settings.polymarket_ws_url,
        markets=markets,
        token_ids=flatten_token_ids(markets),
    )
    ingest_task = asyncio.create_task(_ingest_polymarket(client, state))
    try:
        while True:
            await asyncio.sleep(1.0)
            lines = state.polymarket_compact_lines()
            if lines:
                print(" | ".join(lines), flush=True)
            else:
                print("waiting for Polymarket quote updates", flush=True)
    finally:
        ingest_task.cancel()
        try:
            await ingest_task
        except asyncio.CancelledError:
            pass


async def _ingest_binance(client: BinanceWSClient, state: MarketState) -> None:
    async for event in client.stream():
        state.apply(event)


async def _ingest_polymarket(client: PolymarketWSClient, state: MarketState) -> None:
    async for event in client.stream():
        state.apply(event)


def _parse_symbols(value: str) -> tuple[str, ...]:
    symbols = tuple(symbol.strip().upper() for symbol in value.split(",") if symbol.strip())
    if not symbols:
        raise ValueError("At least one Binance symbol is required.")
    return symbols


def _print_polymarket_markets(markets: tuple[PolymarketMarketMetadata, ...]) -> None:
    print(f"active Polymarket markets: {len(markets)}", flush=True)
    for market in markets:
        print(
            " ".join(
                [
                    market.market_slug,
                    f"asset={market.base_asset or '-'}",
                    f"duration={market.duration_minutes or '-'}m",
                    f"YES={market.yes_token_id[:10]}",
                    f"NO={market.no_token_id[:10]}",
                    f"tick={market.tick_size:g}",
                    f"min={market.min_order_size:g}",
                ]
            ),
            flush=True,
        )


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
