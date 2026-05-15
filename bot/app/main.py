import argparse
import asyncio
from collections.abc import Sequence

import aiohttp

from app.config.settings import get_settings
from app.execution.paper_executor import PaperExecutor
from app.logging.event_logger import AsyncJsonlEventLogger
from app.marketdata.binance_ws import BinanceWSClient
from app.marketdata.polymarket_discovery import (
    PolymarketDiscoveryClient,
    PolymarketMarketCache,
    PolymarketMarketMetadata,
    flatten_token_ids,
)
from app.marketdata.polymarket_ws import PolymarketWSClient
from app.state.market_state import MarketState
from app.strategy.gap_detector import GapDetector, GapMonitorStats


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

    gap_monitor = subparsers.add_parser(
        "gap-monitor",
        help="Measure Binance-led Polymarket repricing gaps and log JSONL events.",
    )
    gap_monitor.add_argument("--gamma-url", default=None, help="Polymarket Gamma API base URL.")
    gap_monitor.add_argument("--poly-ws-url", default=None, help="Polymarket market websocket URL.")
    gap_monitor.add_argument("--binance-url", default=None, help="Binance websocket base URL.")
    gap_monitor.add_argument("--cache-path", default=None, help="Market metadata cache path.")
    gap_monitor.add_argument("--log-dir", default=None, help="Directory for gap_events_YYYYMMDD.jsonl.")
    gap_monitor.add_argument(
        "--min-move-pct",
        type=float,
        default=None,
        help="Minimum Binance move in percent before tracking a gap.",
    )
    gap_monitor.add_argument(
        "--reprice-threshold",
        type=float,
        default=None,
        help="Minimum Polymarket probability move to count as repricing.",
    )
    gap_monitor.add_argument(
        "--min-exit-edge",
        type=float,
        default=None,
        help="Minimum positive bid-over-entry edge for executable repricing.",
    )
    gap_monitor.add_argument(
        "--max-entry-spread",
        type=float,
        default=None,
        help="Maximum Polymarket spread for a fillable stale quote.",
    )
    gap_monitor.add_argument(
        "--max-entry-price-move",
        type=float,
        default=None,
        help="Maximum stale ask price move before the entry window is closed.",
    )
    gap_monitor.add_argument(
        "--max-pending-ms",
        type=float,
        default=None,
        help="Maximum observation lifetime before closing an unresolved gap.",
    )
    gap_monitor.add_argument(
        "--binance-stale-ms",
        type=float,
        default=None,
        help="Strict Binance staleness threshold for candidate detection.",
    )
    gap_monitor.add_argument(
        "--polymarket-stale-ms",
        type=float,
        default=None,
        help="Strict Polymarket quote staleness threshold for candidate detection.",
    )
    gap_monitor.add_argument(
        "--measurement-stale-ms",
        type=float,
        default=None,
        help="Wider feed stale threshold for monitor stats.",
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
    if args.command == "gap-monitor":
        await run_gap_monitor(args)
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
    markets = await _discover_polymarket_markets(discovery)
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


async def run_gap_monitor(args: argparse.Namespace) -> None:
    settings = get_settings()
    discovery = PolymarketDiscoveryClient(
        gamma_url=args.gamma_url or settings.polymarket_gamma_url,
        cache_path=args.cache_path or settings.polymarket_market_cache_path,
    )
    markets = await _discover_polymarket_markets(discovery)
    if not markets:
        print("no active BTC/ETH 5m/15m Polymarket markets discovered", flush=True)
        return

    state = MarketState(max_polymarket_quote_age_ms=settings.polymarket_max_quote_age_ms)
    detector = GapDetector(
        markets=markets,
        min_move_pct=_arg_or_setting(args.min_move_pct, settings.gap_min_move_pct),
        reprice_threshold=_arg_or_setting(
            args.reprice_threshold,
            settings.gap_reprice_threshold,
        ),
        min_exit_edge=_arg_or_setting(args.min_exit_edge, settings.gap_min_exit_edge),
        max_entry_spread=_arg_or_setting(
            args.max_entry_spread,
            settings.gap_max_entry_spread,
        ),
        max_entry_price_move=_arg_or_setting(
            args.max_entry_price_move,
            settings.gap_max_entry_price_move,
        ),
        max_pending_gap_ms=_arg_or_setting(args.max_pending_ms, settings.gap_max_pending_ms),
        binance_stale_ms=_arg_or_setting(args.binance_stale_ms, settings.gap_binance_stale_ms),
        polymarket_stale_ms=_arg_or_setting(
            args.polymarket_stale_ms,
            settings.gap_polymarket_stale_ms,
        ),
        measurement_stale_ms=_arg_or_setting(
            args.measurement_stale_ms,
            settings.gap_measurement_stale_ms,
        ),
    )
    symbols = _symbols_for_markets(markets) or settings.binance_symbols
    binance = BinanceWSClient(
        url=args.binance_url or settings.binance_ws_url,
        symbols=symbols,
    )
    polymarket = PolymarketWSClient(
        url=args.poly_ws_url or settings.polymarket_ws_url,
        markets=markets,
        token_ids=flatten_token_ids(markets),
    )
    logger = AsyncJsonlEventLogger(log_dir=args.log_dir or settings.gap_log_dir)
    logger.start()

    tasks = [
        asyncio.create_task(_ingest_gap_binance(binance, state, detector, logger)),
        asyncio.create_task(_ingest_gap_polymarket(polymarket, state, detector, logger)),
    ]
    try:
        while True:
            await asyncio.sleep(1.0)
            print(_format_gap_stats(detector.stats(state)), flush=True)
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        await logger.close()


async def _ingest_binance(client: BinanceWSClient, state: MarketState) -> None:
    async for event in client.stream():
        state.apply(event)


async def _ingest_polymarket(client: PolymarketWSClient, state: MarketState) -> None:
    async for event in client.stream():
        state.apply(event)


async def _ingest_gap_binance(
    client: BinanceWSClient,
    state: MarketState,
    detector: GapDetector,
    logger: AsyncJsonlEventLogger,
) -> None:
    async for event in client.stream():
        updated = state.apply(event)
        if updated is not None:
            for gap in detector.on_market_event(updated, state):
                await logger.log(gap)


async def _ingest_gap_polymarket(
    client: PolymarketWSClient,
    state: MarketState,
    detector: GapDetector,
    logger: AsyncJsonlEventLogger,
) -> None:
    async for event in client.stream():
        updated = state.apply(event)
        if updated is not None:
            for gap in detector.on_market_event(updated, state):
                await logger.log(gap)


def _parse_symbols(value: str) -> tuple[str, ...]:
    symbols = tuple(symbol.strip().upper() for symbol in value.split(",") if symbol.strip())
    if not symbols:
        raise ValueError("At least one Binance symbol is required.")
    return symbols


async def _discover_polymarket_markets(
    discovery: PolymarketDiscoveryClient,
) -> tuple[PolymarketMarketMetadata, ...]:
    try:
        markets = await discovery.discover(write_cache=True)
    except (aiohttp.ClientError, TimeoutError, OSError, ValueError) as exc:
        print(
            "live Polymarket discovery failed "
            f"({type(exc).__name__}: {exc}); trying cached market metadata",
            flush=True,
        )
        markets = ()

    if markets:
        return markets

    cached = _read_cached_polymarket_markets(discovery)
    if cached.markets:
        print(f"using cached Polymarket markets ({len(cached.markets)})", flush=True)
    return tuple(cached.markets)


def _read_cached_polymarket_markets(
    discovery: PolymarketDiscoveryClient,
) -> PolymarketMarketCache:
    try:
        return discovery.read_cache()
    except (OSError, ValueError) as exc:
        print(
            "unable to read cached Polymarket market metadata "
            f"({type(exc).__name__}: {exc})",
            flush=True,
        )
        return PolymarketMarketCache()


def _print_polymarket_markets(markets: tuple[PolymarketMarketMetadata, ...]) -> None:
    print(f"active Polymarket markets: {len(markets)}", flush=True)
    for market in markets:
        print(
            " ".join(
                [
                    market.market_slug,
                    f"asset={market.base_asset or '-'}",
                    f"duration={market.duration_minutes or '-'}m",
                    f"UP={_short_token(market.up_token_id)}",
                    f"DOWN={_short_token(market.down_token_id)}",
                    f"tick={market.tick_size:g}",
                    f"min={market.min_order_size:g}",
                ]
            ),
            flush=True,
        )


def _symbols_for_markets(markets: tuple[PolymarketMarketMetadata, ...]) -> tuple[str, ...]:
    symbols: set[str] = set()
    for market in markets:
        if market.base_asset == "BTC":
            symbols.add("BTCUSDT")
        elif market.base_asset == "ETH":
            symbols.add("ETHUSDT")
    return tuple(sorted(symbols))


def _short_token(token_id: str | None) -> str:
    return "-" if token_id is None else token_id[:10]


def _arg_or_setting(value: float | None, default: float) -> float:
    return default if value is None else value


def _format_gap_stats(stats: GapMonitorStats) -> str:
    rejects = ",".join(
        f"{reason}:{count}" for reason, count in sorted(stats.reject_count_by_reason.items())
    )
    reject_stages = ",".join(
        f"{stage}:{count}" for stage, count in sorted(stats.reject_count_by_stage.items())
    )
    return " ".join(
        [
            f"detected={stats.detected_count}",
            f"completed={stats.completed_count}",
            f"fillable={stats.fillable_at_detection_count}",
            f"non_fillable={stats.non_fillable_at_detection_count}",
            f"median_mid={_fmt_ms(stats.median_mid_repricing_delay_ms)}",
            f"p95_mid={_fmt_ms(stats.p95_mid_repricing_delay_ms)}",
            f"median_exec={_fmt_ms(stats.median_executable_repricing_delay_ms)}",
            f"p95_exec={_fmt_ms(stats.p95_executable_repricing_delay_ms)}",
            f"median_window={_fmt_ms(stats.median_tradable_window_ms)}",
            f"p95_window={_fmt_ms(stats.p95_tradable_window_ms)}",
            f"avg_edge={_fmt_edge(stats.average_estimated_edge)}",
            f"rejects={rejects or '-'}",
            f"reject_stages={reject_stages or '-'}",
            f"stale_feeds={stats.stale_feed_count}",
        ]
    )


def _fmt_ms(value: float | None) -> str:
    return "-" if value is None else f"{value:.2f}ms"


def _fmt_edge(value: float | None) -> str:
    return "-" if value is None else f"{value:.5f}"


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
