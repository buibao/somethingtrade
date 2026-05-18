import argparse
import asyncio
from collections import Counter
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import Any

import aiohttp
import orjson

from app.backtest.dataset_quality import (
    default_report_path,
)
from app.backtest.dataset_quality_phase4 import (
    build_phase4_dataset_quality_report,
    should_fail_for_readiness,
    write_phase4_csv_outputs,
    write_phase4_dataset_quality_report,
    write_phase4_markdown_report,
)
from app.config.settings import get_settings
from app.core.clock import utc_now_ns
from app.core.events import (
    DepthUpdate,
    MarketLifecycleEvent,
    MarketTick,
    OrderBookTop,
    PolymarketQuote,
    TradableGapObservation,
)
from app.execution.paper_executor import PaperExecutor
from app.logging.event_logger import AsyncJsonlEventLogger
from app.marketdata.binance_ws import BinanceWSClient
from app.marketdata.orderbook_phase41 import (
    OrderbookPhase41Paths,
    run_orderbook_phase41_capture,
)
from app.marketdata.polymarket_discovery import (
    DEFAULT_DISCOVERY_DEBUG_JSONL_PATH,
    DEFAULT_MARKET_CACHE_TTL_MS,
    PolymarketDiscoveryClient,
    PolymarketMarketCache,
    PolymarketMarketMetadata,
    annotate_runtime_market_roles,
    classify_market_window,
    flatten_token_ids,
    is_runtime_tradable_market,
    select_runtime_markets,
)
from app.marketdata.market_universe import (
    MarketUniverseDiff,
    MarketUniverseSnapshot,
    RuntimeMarketUniverseManager,
    build_market_universe_snapshot,
    select_runtime_market_universe,
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

    orderbook_capture = subparsers.add_parser(
        "orderbook-quality-capture",
        help="Run Phase 4.1 Binance orderbook quality capture and write audit reports.",
    )
    orderbook_capture.add_argument(
        "--symbol",
        default="BTCUSDT",
        help="Binance symbol for Phase 4.1 capture.",
    )
    orderbook_capture.add_argument(
        "--duration-sec",
        type=float,
        default=900.0,
        help="Runtime capture duration in seconds.",
    )
    orderbook_capture.add_argument(
        "--depth-n",
        type=int,
        default=20,
        help="Top book depth to copy into clean samples.",
    )
    orderbook_capture.add_argument(
        "--ws-url",
        default="wss://stream.binance.com:9443/ws",
        help="Binance websocket base URL.",
    )
    orderbook_capture.add_argument(
        "--rest-url",
        default="https://api.binance.com",
        help="Binance REST base URL for depth snapshots.",
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
    gap_monitor.add_argument(
        "--pre-entry-log-cooldown-ms",
        type=float,
        default=None,
        help="Cooldown for duplicate pre-entry reject JSONL rows.",
    )
    gap_monitor.add_argument(
        "--require-book-ready",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Gate new candidates until both UP/DOWN local books have snapshots.",
    )
    gap_monitor.add_argument(
        "--book-warmup-max-ms",
        type=float,
        default=None,
        help="Maximum book warmup gate duration before recording normal rejects.",
    )
    gap_monitor.add_argument(
        "--best-validation-mode",
        choices=("strict", "tolerant", "diagnostic"),
        default=None,
        help=(
            "Polymarket reported-best validation mode: strict=conservative audit; "
            "tolerant=research default allowing mismatches within N ticks; "
            "diagnostic=debug-only mismatch recording."
        ),
    )
    gap_monitor.add_argument(
        "--best-validation-tolerance-ticks",
        type=int,
        default=None,
        help="Tolerance in ticks for reported-best validation in tolerant mode.",
    )
    gap_monitor.add_argument(
        "--mismatch-sample-per-token-per-min",
        type=int,
        default=None,
        help="Maximum Polymarket orderbook mismatch samples per token per minute.",
    )
    gap_monitor.add_argument(
        "--runtime-summary-interval-ms",
        type=int,
        default=60_000,
        help="Print compact per-symbol/base-asset runtime diagnostics at this interval.",
    )
    gap_monitor.add_argument(
        "--market-refresh-interval-ms",
        type=int,
        default=60_000,
        help="Rediscover rolling BTC/ETH 5m/15m Polymarket markets at this interval.",
    )
    gap_monitor.add_argument(
        "--market-refresh-force-when-no-signal",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Force an early market rediscovery when Binance moves continue but no signal markets are active.",
    )
    gap_monitor.add_argument(
        "--market-refresh-lookahead-windows",
        type=int,
        default=3,
        help="Number of future rolling windows to keep in the runtime market universe per asset/duration.",
    )
    gap_monitor.add_argument(
        "--market-cache-ttl-ms",
        type=int,
        default=DEFAULT_MARKET_CACHE_TTL_MS,
        help="Maximum age in milliseconds for using cached Polymarket markets at runtime.",
    )
    gap_monitor.add_argument(
        "--wait-for-markets",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Wait and retry at startup until runtime-tradable rolling markets are discovered.",
    )
    gap_monitor.add_argument(
        "--market-discovery-retry-ms",
        type=int,
        default=30_000,
        help="Startup retry interval in milliseconds when no runtime Polymarket markets are available.",
    )
    gap_monitor.add_argument(
        "--market-discovery-startup-timeout-ms",
        type=int,
        default=300_000,
        help="Maximum startup wait in milliseconds before exiting with no active markets.",
    )
    gap_monitor.add_argument(
        "--discovery-debug-jsonl",
        default=str(DEFAULT_DISCOVERY_DEBUG_JSONL_PATH),
        help="UTF-8 JSONL path for Polymarket discovery attempt diagnostics.",
    )
    gap_monitor.add_argument(
        "--runtime-summary-jsonl",
        default=None,
        help="Optional UTF-8 JSONL path for periodic runtime diagnostics.",
    )

    rolling_debug = subparsers.add_parser(
        "polymarket-rolling-discovery-debug",
        help="Debug direct rolling BTC/ETH 5m/15m Polymarket slug discovery.",
    )
    rolling_debug.add_argument("--gamma-url", default=None, help="Polymarket Gamma API base URL.")
    rolling_debug.add_argument("--cache-path", default=None, help="Market metadata cache path.")
    rolling_debug.add_argument(
        "--now-ts",
        type=int,
        default=None,
        help="Unix seconds to use for deterministic rolling slug generation.",
    )
    rolling_debug.add_argument(
        "--output",
        default="data/debug/polymarket_rolling_discovery.json",
        help="Path for raw and parsed rolling discovery debug JSON.",
    )
    rolling_debug.add_argument(
        "--market-cache-ttl-ms",
        type=int,
        default=DEFAULT_MARKET_CACHE_TTL_MS,
        help="Maximum age in milliseconds for considering cached markets usable at runtime.",
    )
    rolling_debug.add_argument(
        "--discovery-debug-jsonl",
        default=str(DEFAULT_DISCOVERY_DEBUG_JSONL_PATH),
        help="UTF-8 JSONL path for this discovery debug attempt.",
    )

    quality_report = subparsers.add_parser(
        "dataset-quality-report",
        help="Summarize Phase 3 gap measurement JSONL quality without pandas.",
    )
    quality_report.add_argument("--input", required=True, help="Input gap_events JSONL file.")
    quality_report.add_argument("--output", default=None, help="Output report JSON path.")
    quality_report.add_argument(
        "--markdown-output",
        default=None,
        help="Output Phase 4 markdown report path.",
    )
    quality_report.add_argument(
        "--csv-dir",
        default=None,
        help="Directory for Phase 4 CSV report files.",
    )
    quality_report.add_argument(
        "--min-quality-tier",
        choices=("A", "B", "C", "D"),
        default=None,
        help="Include rows at this data-quality tier or better.",
    )
    quality_report.add_argument(
        "--primary-min-tier",
        choices=("A", "B"),
        default="B",
        help="Highest tier allowed in primary empirical analysis.",
    )
    quality_report.add_argument(
        "--include-diagnostic",
        action="store_true",
        help="Allow diagnostic validation rows in primary empirical buckets when tier-qualified.",
    )
    quality_report.add_argument(
        "--print-top",
        type=int,
        default=20,
        help="Maximum grouped rows to include for high-cardinality sections.",
    )
    quality_report.add_argument(
        "--fail-on-readiness",
        choices=("NOT_READY", "NEEDS_MORE_DATA", "NEEDS_MORE_CLEANING"),
        default=None,
        help="Exit nonzero when readiness is at or below this conservative threshold.",
    )
    quality_report.add_argument(
        "--no-markdown",
        action="store_true",
        help="Do not write a markdown report.",
    )
    quality_report.add_argument(
        "--no-csv",
        action="store_true",
        help="Do not write CSV report files.",
    )
    quality_report.add_argument(
        "--mismatch-samples",
        default=None,
        help="Optional Polymarket orderbook mismatch sample JSONL path.",
    )
    quality_report.add_argument(
        "--runtime-summary-jsonl",
        default=None,
        help="Optional runtime summary JSONL path for gap-event coverage diagnostics.",
    )

    return parser.parse_args(argv)


async def run(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    if args.command == "binance-monitor":
        await run_binance_monitor(args)
        return
    if args.command == "orderbook-quality-capture":
        summary = await run_orderbook_phase41_capture(
            symbol=args.symbol,
            duration_sec=args.duration_sec,
            depth_n=args.depth_n,
            ws_url=args.ws_url,
            rest_url=args.rest_url,
            paths=OrderbookPhase41Paths(),
        )
        print(orjson.dumps(summary, option=orjson.OPT_INDENT_2).decode("utf-8"), flush=True)
        return
    if args.command == "polymarket-monitor":
        await run_polymarket_monitor(args)
        return
    if args.command == "gap-monitor":
        await run_gap_monitor(args)
        return
    if args.command == "polymarket-rolling-discovery-debug":
        await run_polymarket_rolling_discovery_debug(args)
        return
    if args.command == "dataset-quality-report":
        run_dataset_quality_report(args)
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
    markets = await _discover_polymarket_markets_for_startup(
        discovery,
        lookahead_windows=args.market_refresh_lookahead_windows,
        market_cache_ttl_ms=args.market_cache_ttl_ms,
        discovery_debug_jsonl=args.discovery_debug_jsonl,
        wait_for_markets=args.wait_for_markets,
        retry_ms=args.market_discovery_retry_ms,
        startup_timeout_ms=args.market_discovery_startup_timeout_ms,
    )
    if not markets:
        print(
            "No runtime-tradable BTC/ETH 5m/15m rolling markets discovered. Run:\n"
            " python -m app.main polymarket-rolling-discovery-debug",
            flush=True,
        )
        print("no active BTC/ETH 5m/15m Polymarket markets discovered", flush=True)
        return

    universe_manager = RuntimeMarketUniverseManager(
        discovery,
        markets,
        refresh_interval_ms=args.market_refresh_interval_ms,
        lookahead_windows=args.market_refresh_lookahead_windows,
        market_cache_ttl_ms=args.market_cache_ttl_ms,
        discovery_debug_jsonl=args.discovery_debug_jsonl,
    )
    markets = universe_manager.markets
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
        pre_entry_log_cooldown_ms=_arg_or_setting(
            args.pre_entry_log_cooldown_ms,
            settings.gap_pre_entry_log_cooldown_ms,
        ),
        require_book_ready=(
            settings.gap_require_book_ready
            if args.require_book_ready is None
            else args.require_book_ready
        ),
        book_warmup_max_ms=_arg_or_setting(
            args.book_warmup_max_ms,
            settings.gap_book_warmup_max_ms,
        ),
        validation_mode=(args.best_validation_mode or settings.polymarket_best_validation_mode),
        validation_tolerance_ticks=(
            args.best_validation_tolerance_ticks
            if args.best_validation_tolerance_ticks is not None
            else settings.polymarket_best_validation_tolerance_ticks
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
        best_validation_mode=(
            args.best_validation_mode or settings.polymarket_best_validation_mode
        ),
        best_validation_tolerance_ticks=(
            args.best_validation_tolerance_ticks
            if args.best_validation_tolerance_ticks is not None
            else settings.polymarket_best_validation_tolerance_ticks
        ),
        mismatch_sample_per_token_per_min=(
            args.mismatch_sample_per_token_per_min
            if args.mismatch_sample_per_token_per_min is not None
            else settings.polymarket_mismatch_sample_per_token_per_min
        ),
    )
    logger = AsyncJsonlEventLogger(log_dir=args.log_dir or settings.gap_log_dir)
    logger.start()
    runtime_summary = GapRuntimeSummary(universe_manager.snapshot())
    runtime_jsonl = RuntimeSummaryJsonlWriter(args.runtime_summary_jsonl)
    runtime_summary_interval_s = max(1, args.runtime_summary_interval_ms) / 1000.0
    market_refresh_interval_s = max(1, args.market_refresh_interval_ms) / 1000.0
    loop = asyncio.get_running_loop()
    next_runtime_summary_at = loop.time() + runtime_summary_interval_s
    next_forced_refresh_allowed_at = loop.time()
    last_forced_refresh_move_total = 0
    _write_book_readiness_debug(polymarket)
    print(_format_book_readiness_summary(polymarket.book_readiness_snapshot()), flush=True)

    tasks = [
        asyncio.create_task(_ingest_gap_binance(binance, state, detector, logger, runtime_summary)),
        asyncio.create_task(
            _ingest_gap_polymarket(polymarket, state, detector, logger, runtime_summary)
        ),
    ]
    try:
        while True:
            await asyncio.sleep(1.0)
            readiness = polymarket.book_readiness_snapshot()
            _write_book_readiness_debug(polymarket, payload=readiness)
            stats = detector.stats(state)
            move_total = sum(stats.binance_moves_detected_by_symbol.values())
            force_no_signal_refresh = should_force_market_refresh(
                enabled=args.market_refresh_force_when_no_signal,
                signal_enabled_markets=stats.signal_enabled_markets,
                binance_move_total=move_total,
                last_forced_refresh_move_total=last_forced_refresh_move_total,
                now_s=loop.time(),
                next_forced_refresh_allowed_at_s=next_forced_refresh_allowed_at,
            )
            force_lifecycle_refresh = runtime_summary.consume_lifecycle_refresh_request()
            if universe_manager.refresh_due() or force_no_signal_refresh or force_lifecycle_refresh:
                forced = force_no_signal_refresh or force_lifecycle_refresh
                refresh_reason = (
                    "forced_no_signal"
                    if force_no_signal_refresh
                    else "lifecycle_new_market"
                    if force_lifecycle_refresh
                    else "scheduled"
                )
                diff = await universe_manager.refresh(
                    forced=forced,
                    refresh_reason=refresh_reason,
                )
                if forced:
                    next_forced_refresh_allowed_at = loop.time() + market_refresh_interval_s
                    last_forced_refresh_move_total = move_total
                await _apply_market_universe_refresh(
                    detector=detector,
                    polymarket=polymarket,
                    logger=logger,
                    runtime_summary=runtime_summary,
                    snapshot=universe_manager.snapshot(),
                    diff=diff,
                )
                if diff.changed or diff.error or forced:
                    print(_format_market_universe_diff(diff), flush=True)
                readiness = polymarket.book_readiness_snapshot()
                stats = detector.stats(state)
            print(_format_gap_stats(stats), flush=True)
            print(_format_book_readiness_summary(readiness), flush=True)
            if loop.time() >= next_runtime_summary_at:
                summary_payload = runtime_summary.snapshot_payload(
                    stats,
                    readiness,
                    ws_diagnostics=polymarket.subscription_diagnostics(),
                    final=False,
                )
                print(runtime_summary.format_payload(summary_payload), flush=True)
                for warning in summary_payload["no_event_warnings"]:
                    print(f"runtime_warning reason={warning}", flush=True)
                runtime_jsonl.write(summary_payload)
                next_runtime_summary_at = loop.time() + runtime_summary_interval_s
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        try:
            final_readiness = polymarket.book_readiness_snapshot()
        except Exception:
            final_readiness = {}
        final_payload = runtime_summary.snapshot_payload(
            detector.stats(state),
            final_readiness,
            ws_diagnostics=polymarket.subscription_diagnostics(),
            final=True,
        )
        print(runtime_summary.format_payload(final_payload), flush=True)
        runtime_jsonl.write(final_payload)
        await logger.close()


async def run_polymarket_rolling_discovery_debug(args: argparse.Namespace) -> None:
    settings = get_settings()
    discovery = PolymarketDiscoveryClient(
        gamma_url=args.gamma_url or settings.polymarket_gamma_url,
        cache_path=args.cache_path or settings.polymarket_market_cache_path,
    )
    debug = await discovery.debug_rolling_discovery(
        now_ts=args.now_ts,
        market_cache_ttl_ms=args.market_cache_ttl_ms,
        discovery_debug_jsonl=args.discovery_debug_jsonl,
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(
        orjson.dumps(
            debug,
            option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE,
        )
    )

    print("generated slugs:", flush=True)
    for slug in debug["generated_slugs"]:
        print(f"  {slug}", flush=True)

    print("rolling discovery results:", flush=True)
    attempt = debug.get("attempt") or {}
    strategy_results = debug.get("strategy_results") or {}
    direct_summary = strategy_results.get("direct_slug") or {}
    active_summary = strategy_results.get("active_events") or {}
    cache_summary = strategy_results.get("cache") or {}
    print(
        " ".join(
            [
                "discovery summary:",
                f"direct_found_count={direct_summary.get('found_count', 0)}",
                f"direct_runtime_count={direct_summary.get('runtime_tradable_count', 0)}",
                f"active_events_found_runtime_count={active_summary.get('runtime_tradable_count', 0)}",
                f"cache_runtime_count={cache_summary.get('runtime_count', 0)}",
                f"cache_rejected={cache_summary.get('rejected', False)}",
                f"cache_rejected_reason={cache_summary.get('rejected_reason') or '-'}",
                f"fallback_used={attempt.get('fallback_used', False)}",
                f"failure_reason={attempt.get('failure_reason') or '-'}",
            ]
        ),
        flush=True,
    )
    if attempt.get("failure_reason") == "direct_slug_found_but_all_closed":
        print("direct_slug_found_but_all_closed", flush=True)
    print(
        f"active_events_found_runtime_count={active_summary.get('runtime_tradable_count', 0)}",
        flush=True,
    )
    print(f"cache_runtime_count={cache_summary.get('runtime_count', 0)}", flush=True)
    for result in debug["results"]:
        endpoint = result.get("endpoint_used") or "-"
        status = "FOUND" if result.get("parsed_markets") else "missing/rejected"
        print(f"  {result['slug']} status={status} endpoint={endpoint}", flush=True)
        rejects = result.get("reject_reasons") or []
        if rejects:
            print(f"    reject_reasons={','.join(str(reason) for reason in rejects)}", flush=True)
        for market in result.get("parsed_markets") or []:
            token_outcomes = market.get("token_outcomes") or {}
            print(
                "    "
                + " ".join(
                    [
                        f"slug={market.get('market_slug')}",
                        f"market_id={market.get('market_id')}",
                        f"asset={market.get('base_asset')}",
                        f"duration={market.get('duration_minutes')}m",
                        f"eventStartTime={market.get('event_start_time')}",
                        f"endDate={market.get('end_time')}",
                        f"active={market.get('active')}",
                        f"closed={market.get('closed')}",
                        f"acceptingOrders={market.get('accepting_orders')}",
                        f"enableOrderBook={market.get('enable_order_book')}",
                        f"classification={market.get('classification')}",
                        f"selected_for_runtime={market.get('selected_for_runtime')}",
                        f"signal_enabled={market.get('signal_enabled')}",
                        f"runtime_selection_reason={market.get('runtime_selection_reason')}",
                        f"discovery_source={market.get('discovery_source')}",
                        f"UP={market.get('up_token_id')}",
                        f"DOWN={market.get('down_token_id')}",
                        f"outcomes={token_outcomes}",
                    ]
                ),
                flush=True,
            )

    active_events = debug.get("active_events") or {}
    print("active events fallback results:", flush=True)
    print(
        "  "
        + " ".join(
            [
                f"attempted={active_events.get('attempted', False)}",
                f"event_count={active_events.get('event_count', 0)}",
                f"candidate_count={active_events.get('candidate_count', 0)}",
                f"parsed_count={active_events.get('parsed_count', 0)}",
                f"runtime_count={active_events.get('runtime_tradable_count', 0)}",
            ]
        ),
        flush=True,
    )
    for rejected in active_events.get("rejected_candidates") or []:
        print(f"    rejected={rejected}", flush=True)

    print("cache validation result:", flush=True)
    print(f"  {debug.get('cache_validation') or {}}", flush=True)

    print("final selected runtime markets:", flush=True)
    for slug in debug.get("selected_market_slugs") or []:
        print(f"  {slug}", flush=True)

    print(f"debug output written to {output_path}", flush=True)


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
    runtime_summary: "GapRuntimeSummary",
) -> None:
    async for event in client.stream():
        runtime_summary.record_binance_event(event)
        updated = state.apply(event)
        if updated is not None:
            for gap in detector.on_market_event(updated, state):
                await logger.log(gap)
                runtime_summary.record_gap_event_written(gap)


async def _ingest_gap_polymarket(
    client: PolymarketWSClient,
    state: MarketState,
    detector: GapDetector,
    logger: AsyncJsonlEventLogger,
    runtime_summary: "GapRuntimeSummary",
) -> None:
    async for event in client.stream():
        runtime_summary.record_polymarket_event(event)
        updated = state.apply(event)
        if updated is not None:
            for gap in detector.on_market_event(updated, state):
                await logger.log(gap)
                runtime_summary.record_gap_event_written(gap)


def run_dataset_quality_report(args: argparse.Namespace) -> None:
    output_path = Path(args.output) if args.output else default_report_path()
    markdown_output_path = (
        None
        if args.no_markdown
        else Path(args.markdown_output)
        if args.markdown_output
        else output_path.with_suffix(".md")
    )
    csv_dir = (
        None
        if args.no_csv
        else Path(args.csv_dir)
        if args.csv_dir
        else output_path.with_name(f"{output_path.stem}_csv")
    )
    try:
        report = build_phase4_dataset_quality_report(
            args.input,
            output_path=output_path,
            markdown_output_path=markdown_output_path,
            csv_dir=csv_dir,
            min_quality_tier=args.min_quality_tier,
            primary_min_tier=args.primary_min_tier,
            include_diagnostic=args.include_diagnostic,
            print_top=args.print_top,
            mismatch_samples_path=args.mismatch_samples,
            runtime_summary_jsonl_path=args.runtime_summary_jsonl,
        )
    except FileNotFoundError as exc:
        raise SystemExit(str(exc)) from exc

    write_phase4_dataset_quality_report(report, output_path)
    if markdown_output_path is not None:
        write_phase4_markdown_report(report, markdown_output_path)
    if csv_dir is not None:
        write_phase4_csv_outputs(report, csv_dir)

    readiness = report["readiness_assessment"]
    print(
        " ".join(
            [
                "dataset_quality_report",
                f"input={args.input}",
                f"output={output_path}",
                f"markdown={markdown_output_path or '-'}",
                f"csv_dir={csv_dir or '-'}",
                f"rows={report['dataset_health']['included_rows']}/{report['dataset_health']['total_rows']}",
                f"primary={report['dataset_health']['primary_rows']}",
                f"success={report['dataset_health']['success_count']}",
                f"readiness={readiness['classification']}",
                f"warnings={','.join(report['warnings']) or '-'}",
            ]
        ),
        flush=True,
    )
    if should_fail_for_readiness(readiness["classification"], args.fail_on_readiness):
        raise SystemExit(f"dataset readiness failed: {readiness['classification']}")


class GapRuntimeSummary:
    """Compact, observability-only gap monitor counters."""

    def __init__(
        self,
        markets: Sequence[PolymarketMarketMetadata] | MarketUniverseSnapshot,
    ) -> None:
        snapshot = (
            markets
            if isinstance(markets, MarketUniverseSnapshot)
            else build_market_universe_snapshot(
                tuple(markets),
                now_ts=utc_now_ns() // 1_000_000_000,
            )
        )
        self.markets = tuple(snapshot.markets)
        self.market_universe = snapshot
        self.binance_events_seen_by_symbol: Counter[str] = Counter()
        self.gap_events_written_by_symbol: Counter[str] = Counter()
        self._market_by_id = {market.market_id: market for market in self.markets}
        self._last_gap_event_total = 0
        self._last_gap_event_change_ts_ns = utc_now_ns()
        self._last_move_total = 0
        self._last_move_change_ts_ns: int | None = None
        self._last_summary_ts_ns = utc_now_ns()
        self._lifecycle_refresh_requested = False
        self._last_diff = snapshot.last_diff
        self._subscription_divergence_first_seen_ns: int | None = None

    def record_binance_event(self, event: object) -> None:
        if isinstance(event, (MarketTick, OrderBookTop, DepthUpdate)):
            self.binance_events_seen_by_symbol[event.symbol] += 1

    def record_polymarket_event(self, event: object) -> None:
        if isinstance(event, MarketLifecycleEvent) and event.lifecycle_type == "new_market":
            self._lifecycle_refresh_requested = True

    def record_gap_event_written(self, observation: TradableGapObservation) -> None:
        self.gap_events_written_by_symbol[observation.symbol] += 1

    def consume_lifecycle_refresh_request(self) -> bool:
        requested = self._lifecycle_refresh_requested
        self._lifecycle_refresh_requested = False
        return requested

    def update_market_universe(
        self,
        snapshot: MarketUniverseSnapshot,
        diff: MarketUniverseDiff,
    ) -> None:
        self.market_universe = snapshot
        self.markets = tuple(snapshot.markets)
        self._market_by_id = {market.market_id: market for market in self.markets}
        self._last_diff = diff

    def format(
        self,
        stats: GapMonitorStats,
        readiness: dict[str, Any],
        *,
        final: bool = False,
    ) -> str:
        return self.format_payload(
            self.snapshot_payload(stats, readiness, ws_diagnostics={}, final=final)
        )

    def snapshot_payload(
        self,
        stats: GapMonitorStats,
        readiness: dict[str, Any],
        *,
        ws_diagnostics: dict[str, Any],
        final: bool = False,
    ) -> dict[str, Any]:
        now_ns = utc_now_ns()
        now_ts = now_ns // 1_000_000_000
        market_counters = self._market_counters(readiness)
        move_total = sum(int(value) for value in stats.binance_moves_detected_by_symbol.values())
        gap_total = sum(int(value) for value in self.gap_events_written_by_symbol.values())
        if move_total > self._last_move_total:
            self._last_move_change_ts_ns = now_ns
        if gap_total > self._last_gap_event_total:
            self._last_gap_event_change_ts_ns = now_ns
        self._last_move_total = move_total
        self._last_gap_event_total = gap_total

        universe_snapshot = build_market_universe_snapshot(
            self.markets,
            now_ts=now_ts,
            last_market_discovery_ts=self.market_universe.last_market_discovery_ts,
            next_market_discovery_ts=self.market_universe.next_market_discovery_ts,
            market_refresh_count=self.market_universe.market_refresh_count,
            forced_market_refresh_count=self.market_universe.forced_market_refresh_count,
            market_refresh_error_count=self.market_universe.market_refresh_error_count,
            discovery_failure_reason=self.market_universe.discovery_failure_reason,
            last_discovery_attempt_summary=self.market_universe.last_discovery_attempt_summary,
            last_successful_discovery_ts=self.market_universe.last_successful_discovery_ts,
            last_successful_current_signal_slugs=(
                self.market_universe.last_successful_current_signal_slugs
            ),
            last_diff=self._last_diff,
        )
        self.market_universe = universe_snapshot
        last_attempt = universe_snapshot.last_discovery_attempt_summary
        no_event_duration_ms = (now_ns - self._last_gap_event_change_ts_ns) / 1_000_000.0
        subscription_matches = _subscription_matches(ws_diagnostics)
        if subscription_matches is False:
            if self._subscription_divergence_first_seen_ns is None:
                self._subscription_divergence_first_seen_ns = now_ns
        else:
            self._subscription_divergence_first_seen_ns = None
        payload: dict[str, Any] = {
            "event_type": "runtime_summary",
            "final": final,
            "generated_ts_ns": now_ns,
            "binance_events_seen_by_symbol": dict(sorted(self.binance_events_seen_by_symbol.items())),
            "binance_moves_detected_by_symbol": dict(
                sorted(stats.binance_moves_detected_by_symbol.items())
            ),
            "candidates_created_by_symbol": dict(sorted(stats.candidates_created_by_symbol.items())),
            "gap_events_written_by_symbol": dict(
                sorted(self.gap_events_written_by_symbol.items())
            ),
            "pre_entry_rejects_by_symbol": dict(sorted(stats.pre_entry_rejects_by_symbol.items())),
            "window_rejects_by_symbol": dict(sorted(stats.window_rejects_by_symbol.items())),
            "timeout_rejects_by_symbol": dict(sorted(stats.timeout_rejects_by_symbol.items())),
            "suppressed_candidates_by_symbol": dict(
                sorted(stats.suppressed_candidates_by_symbol.items())
            ),
            "non_fillable_by_symbol": dict(sorted(stats.non_fillable_by_symbol.items())),
            "top_reject_reasons_by_symbol": stats.top_reject_reasons_by_symbol,
            "book_ready_tokens_by_base_asset": dict(sorted(market_counters["book_ready"].items())),
            "book_not_ready_tokens_by_base_asset": dict(
                sorted(
                    (
                        base,
                        max(
                            0,
                            market_counters["book_total"].get(base, 0)
                            - market_counters["book_ready"].get(base, 0),
                        ),
                    )
                    for base in market_counters["book_total"]
                )
            ),
            "signal_book_not_ready_tokens_by_base_asset": dict(
                sorted(
                    (
                        base,
                        max(
                            0,
                            market_counters["signal_book_total"].get(base, 0)
                            - market_counters["signal_book_ready"].get(base, 0),
                        ),
                    )
                    for base in market_counters["signal_book_total"]
                )
            ),
            "current_signal_markets_by_base_asset": _counter_from_markets(
                universe_snapshot.current_signal_markets
            ),
            "next_warmup_markets_by_base_asset": _counter_from_markets(
                universe_snapshot.next_warmup_markets
            ),
            "future_tracked_markets_by_base_asset": _counter_from_markets(
                universe_snapshot.future_tracked_markets
            ),
            "expired_selected_markets_by_base_asset": _counter_from_markets(
                universe_snapshot.expired_selected_markets
            ),
            "closed_removed_markets_by_base_asset": _counter_from_markets(
                universe_snapshot.closed_removed_markets
            ),
            "signal_enabled_markets_by_base_asset": _counter_from_markets(
                universe_snapshot.current_signal_markets
            ),
            "active_ws_token_subscription_count": int(
                ws_diagnostics.get(
                    "active_ws_token_subscription_count",
                    len(universe_snapshot.token_ids),
                )
            ),
            "subscription_status": ws_diagnostics.get("subscription_status"),
            "active_subscription_established": ws_diagnostics.get(
                "active_subscription_established"
            ),
            "runtime_token_count": int(
                ws_diagnostics.get("runtime_token_count", len(universe_snapshot.token_ids))
            ),
            "subscription_token_set_matches_runtime_universe": subscription_matches,
            "missing_subscription_token_count": len(
                ws_diagnostics.get("missing_active_tokens", ()) or ()
            ),
            "extra_subscription_token_count": len(
                ws_diagnostics.get("extra_active_tokens", ()) or ()
            ),
            "missing_subscription_tokens_sample": list(
                (ws_diagnostics.get("missing_active_tokens", ()) or ())[:5]
            ),
            "extra_subscription_tokens_sample": list(
                (ws_diagnostics.get("extra_active_tokens", ()) or ())[:5]
            ),
            "subscription_transition_active": bool(
                ws_diagnostics.get("subscription_transition_active", False)
            ),
            "subscription_update_count": int(
                ws_diagnostics.get("subscription_update_count", 0)
            ),
            "ws_reconnect_count": int(
                ws_diagnostics.get(
                    "websocket_reconnect_count",
                    ws_diagnostics.get("ws_reconnect_count", 0),
                )
            ),
            "tracked_market_count": universe_snapshot.tracked_market_count,
            "last_market_discovery_ts": universe_snapshot.last_market_discovery_ts,
            "next_market_discovery_ts": universe_snapshot.next_market_discovery_ts,
            "discovery_failure_reason": universe_snapshot.discovery_failure_reason,
            "last_discovery_attempt_summary": last_attempt,
            "last_successful_discovery_ts": universe_snapshot.last_successful_discovery_ts,
            "last_successful_current_signal_slugs": list(
                universe_snapshot.last_successful_current_signal_slugs
            ),
            "direct_runtime_count": int(last_attempt.get("direct_runtime_count") or 0),
            "active_events_runtime_count": int(
                last_attempt.get("active_events_runtime_count")
                or last_attempt.get("active_events_found_runtime_count")
                or 0
            ),
            "cache_runtime_count": int(last_attempt.get("cache_runtime_count") or 0),
            "no_event_duration_ms": no_event_duration_ms,
            "new_markets_added_count": len(self._last_diff.added_markets),
            "expired_markets_removed_count": len(self._last_diff.expired_markets),
            "current_market_slugs_by_base_asset": _slugs_by_base(
                universe_snapshot.current_signal_markets
            ),
            "next_market_slugs_by_base_asset": _slugs_by_base(
                universe_snapshot.next_warmup_markets
            ),
            "market_refresh_count": universe_snapshot.market_refresh_count,
            "forced_market_refresh_count": universe_snapshot.forced_market_refresh_count,
            "market_refresh_error_count": universe_snapshot.market_refresh_error_count,
            "validation_errors_by_type": _validation_errors_by_type(readiness),
            "websocket_reconnect_count": int(
                ws_diagnostics.get("websocket_reconnect_count", 0)
            ),
            "pending_observation_count": stats.pending_observation_count,
            "pending_max_age_ms": stats.pending_max_age_ms,
            "candidate_duplicate_suppressed_count": (
                stats.candidate_duplicate_suppressed_count
            ),
            "candidates_per_symbol_direction_per_minute": (
                stats.candidates_per_symbol_direction_per_minute
            ),
            "candidates_per_market_window": stats.candidates_per_market_window,
            "market_lifecycle_diff": {
                "added_markets": self._last_diff.added_markets,
                "removed_markets": self._last_diff.removed_markets,
                "expired_markets": self._last_diff.expired_markets,
                "closed_markets": self._last_diff.closed_markets,
                "new_token_ids": self._last_diff.new_token_ids,
                "removed_token_ids": self._last_diff.removed_token_ids,
            },
            "subscription_diagnostics": ws_diagnostics,
        }
        payload["runtime_status"] = _runtime_status(payload, stats, ws_diagnostics)
        payload["no_event_warnings"] = self._no_event_warnings(
            payload,
            stats,
            ws_diagnostics,
            now_ns=now_ns,
        )
        payload["no_event_warning"] = (
            "no_signal_enabled_markets_while_binance_moves_continue"
            if "no_signal_enabled_markets_while_binance_moves_continue"
            in payload["no_event_warnings"]
            else (payload["no_event_warnings"][0] if payload["no_event_warnings"] else None)
        )
        self._last_summary_ts_ns = now_ns
        return payload

    def format_payload(self, payload: dict[str, Any]) -> str:
        return " ".join(
            [
                "runtime_summary",
                f"final={str(payload['final']).lower()}",
                (
                    "binance_events_seen_by_symbol="
                    f"{_format_counter(payload['binance_events_seen_by_symbol'])}"
                ),
                (
                    "binance_moves_detected_by_symbol="
                    f"{_format_counter(payload['binance_moves_detected_by_symbol'])}"
                ),
                (
                    "runtime_markets_selected_by_base_asset="
                    f"{_format_counter(_counter_from_markets(self.market_universe.markets))}"
                ),
                (
                    "current_signal_markets_by_base_asset="
                    f"{_format_counter(payload['current_signal_markets_by_base_asset'])}"
                ),
                (
                    "signal_enabled_markets_by_base_asset="
                    f"{_format_counter(payload['signal_enabled_markets_by_base_asset'])}"
                ),
                (
                    "next_warmup_markets_by_base_asset="
                    f"{_format_counter(payload['next_warmup_markets_by_base_asset'])}"
                ),
                (
                    "warmup_only_markets_by_base_asset="
                    f"{_format_counter(payload['next_warmup_markets_by_base_asset'])}"
                ),
                (
                    "future_tracked_markets_by_base_asset="
                    f"{_format_counter(payload['future_tracked_markets_by_base_asset'])}"
                ),
                (
                    "expired_selected_markets_by_base_asset="
                    f"{_format_counter(payload['expired_selected_markets_by_base_asset'])}"
                ),
                (
                    "active_ws_token_subscription_count="
                    f"{payload['active_ws_token_subscription_count']}"
                ),
                f"runtime_token_count={payload['runtime_token_count']}",
                (
                    "subscription_token_set_matches_runtime_universe="
                    f"{payload['subscription_token_set_matches_runtime_universe']}"
                ),
                f"missing_subscription_token_count={payload['missing_subscription_token_count']}",
                f"extra_subscription_token_count={payload['extra_subscription_token_count']}",
                f"subscription_transition_active={str(payload['subscription_transition_active']).lower()}",
                f"subscription_update_count={payload['subscription_update_count']}",
                f"ws_reconnect_count={payload['ws_reconnect_count']}",
                (
                    "tracked_market_count="
                    f"{payload['tracked_market_count']}"
                ),
                (
                    "current_market_slugs_by_base_asset="
                    f"{_format_slug_map(payload['current_market_slugs_by_base_asset'])}"
                ),
                (
                    "next_market_slugs_by_base_asset="
                    f"{_format_slug_map(payload['next_market_slugs_by_base_asset'])}"
                ),
                (
                    "book_ready_tokens_by_base_asset="
                    f"{_format_ready_tokens(Counter(payload['book_ready_tokens_by_base_asset']), _book_total_from_payload(payload))}"
                ),
                (
                    "candidates_created_by_symbol="
                    f"{_format_counter(payload['candidates_created_by_symbol'])}"
                ),
                (
                    "gap_events_written_by_symbol="
                    f"{_format_counter(payload['gap_events_written_by_symbol'])}"
                ),
                (
                    "pre_entry_rejects_by_symbol="
                    f"{_format_counter(payload['pre_entry_rejects_by_symbol'])}"
                ),
                (
                    "window_rejects_by_symbol="
                    f"{_format_counter(payload['window_rejects_by_symbol'])}"
                ),
                (
                    "timeout_rejects_by_symbol="
                    f"{_format_counter(payload['timeout_rejects_by_symbol'])}"
                ),
                (
                    "top_reject_reasons_by_symbol="
                    f"{_format_nested_counter(payload['top_reject_reasons_by_symbol'])}"
                ),
                f"market_refresh_count={payload['market_refresh_count']}",
                f"forced_market_refresh_count={payload['forced_market_refresh_count']}",
                f"market_refresh_error_count={payload['market_refresh_error_count']}",
                f"discovery_failure_reason={payload['discovery_failure_reason'] or '-'}",
                f"direct_runtime_count={payload['direct_runtime_count']}",
                f"active_events_runtime_count={payload['active_events_runtime_count']}",
                f"cache_runtime_count={payload['cache_runtime_count']}",
                f"runtime_status={payload['runtime_status']}",
                f"no_event_warnings={','.join(payload['no_event_warnings']) or '-'}",
            ]
        )

    def _market_counters(self, readiness: dict[str, Any]) -> dict[str, Counter[str]]:
        runtime_selected: Counter[str] = Counter()
        signal_enabled: Counter[str] = Counter()
        warmup_only: Counter[str] = Counter()
        book_ready: Counter[str] = Counter()
        book_total: Counter[str] = Counter()
        signal_book_ready: Counter[str] = Counter()
        signal_book_total: Counter[str] = Counter()

        for market in self.markets:
            base_asset = _base_asset(market)
            if market.selected_for_runtime:
                runtime_selected[base_asset] += 1

        readiness_rows = _readiness_market_rows(readiness)
        if not readiness_rows:
            now_ts = utc_now_ns() // 1_000_000_000
            for market in self.markets:
                base_asset = _base_asset(market)
                classification = classify_market_window(market, now_ts=now_ts)
                signal_market = (
                    is_runtime_tradable_market(market, now_ts=now_ts)
                    and classification == "current_signal"
                )
                if signal_market:
                    signal_enabled[base_asset] += 1
                elif market.selected_for_runtime and classification == "next_warmup":
                    warmup_only[base_asset] += 1
                token_count = int(market.up_token_id is not None) + int(
                    market.down_token_id is not None
                )
                book_total[base_asset] += token_count
                if signal_market:
                    signal_book_total[base_asset] += token_count
            return {
                "runtime_selected": runtime_selected,
                "signal_enabled": signal_enabled,
                "warmup_only": warmup_only,
                "book_ready": book_ready,
                "book_total": book_total,
                "signal_book_ready": signal_book_ready,
                "signal_book_total": signal_book_total,
            }

        for row in readiness_rows:
            market_id = row.get("market_id")
            market = self._market_by_id.get(str(market_id)) if market_id is not None else None
            if market is None:
                continue
            base_asset = _base_asset(market)
            classification = classify_market_window(
                market,
                now_ts=utc_now_ns() // 1_000_000_000,
            )
            signal_market = (
                row.get("signal_enabled_at_now") is True
                and classification == "current_signal"
            )
            if signal_market:
                signal_enabled[base_asset] += 1
            elif market.selected_for_runtime and classification == "next_warmup":
                warmup_only[base_asset] += 1
            for token_field, complete_field in (
                ("up_token_id", "up_token_book_complete"),
                ("down_token_id", "down_token_book_complete"),
            ):
                if row.get(token_field) is not None:
                    book_total[base_asset] += 1
                    if signal_market:
                        signal_book_total[base_asset] += 1
                    if row.get(complete_field) is True:
                        book_ready[base_asset] += 1
                        if signal_market:
                            signal_book_ready[base_asset] += 1

        return {
            "runtime_selected": runtime_selected,
            "signal_enabled": signal_enabled,
            "warmup_only": warmup_only,
            "book_ready": book_ready,
            "book_total": book_total,
            "signal_book_ready": signal_book_ready,
            "signal_book_total": signal_book_total,
        }

    def _no_event_warnings(
        self,
        payload: dict[str, Any],
        stats: GapMonitorStats,
        ws_diagnostics: dict[str, Any],
        *,
        now_ns: int,
    ) -> list[str]:
        warnings: list[str] = []
        gap_quiet_ms = (now_ns - self._last_gap_event_change_ts_ns) / 1_000_000.0
        moves_seen = sum(payload["binance_moves_detected_by_symbol"].values())
        binance_events_seen = sum(payload["binance_events_seen_by_symbol"].values())
        if binance_events_seen > 0 and moves_seen == 0:
            warnings.append("no_binance_moves_detected")
        if gap_quiet_ms < 600_000 or moves_seen <= 0:
            return warnings
        if not payload["signal_enabled_markets_by_base_asset"]:
            warnings.append("no_signal_enabled_markets_while_binance_moves_continue")
        signal_not_ready_total = sum(
            payload["signal_book_not_ready_tokens_by_base_asset"].values()
        )
        if payload["signal_enabled_markets_by_base_asset"] and signal_not_ready_total > 0:
            warnings.append("books_not_ready_while_binance_moves_continue")
        if (
            sum(payload["candidates_created_by_symbol"].values()) > sum(payload["gap_events_written_by_symbol"].values())
            and (
                sum(payload["pre_entry_rejects_by_symbol"].values())
                + sum(payload["window_rejects_by_symbol"].values())
                + sum(payload["timeout_rejects_by_symbol"].values())
                + sum(payload["suppressed_candidates_by_symbol"].values())
            )
            > 0
        ):
            warnings.append("candidates_rejected_or_suppressed_while_binance_moves_continue")
        runtime_tokens = int(ws_diagnostics.get("runtime_token_count", len(self.market_universe.token_ids)))
        active_tokens = int(
            ws_diagnostics.get("active_ws_token_subscription_count", runtime_tokens)
        )
        if (
            runtime_tokens != active_tokens
            and payload["subscription_token_set_matches_runtime_universe"] is not None
            and not payload["subscription_transition_active"]
        ):
            warnings.append("market_subscriptions_stale")
        if payload["market_refresh_error_count"] > 0:
            warnings.append("market_refresh_failing")
        if (
            payload["subscription_token_set_matches_runtime_universe"] is False
            and not payload["subscription_transition_active"]
            and self._subscription_divergence_first_seen_ns is not None
            and self._subscription_divergence_first_seen_ns <= self._last_summary_ts_ns
        ):
            warnings.append("websocket_subscription_out_of_sync")
        if (
            stats.pending_observation_count > 0
            and stats.pending_max_age_ms is not None
            and stats.pending_max_age_ms > stats.max_pending_gap_ms
        ):
            warnings.append("pending_observations_stuck")
        return sorted(set(warnings))


def _readiness_market_rows(readiness: dict[str, Any]) -> list[dict[str, Any]]:
    rows = readiness.get("markets")
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _base_asset(market: PolymarketMarketMetadata) -> str:
    return market.base_asset or "unknown"


class RuntimeSummaryJsonlWriter:
    def __init__(self, path: str | None) -> None:
        self.path = None if path is None else Path(path)

    def write(self, payload: dict[str, Any]) -> None:
        if self.path is None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("ab") as handle:
            handle.write(orjson.dumps(payload, option=orjson.OPT_APPEND_NEWLINE))


async def _apply_market_universe_refresh(
    *,
    detector: GapDetector,
    polymarket: PolymarketWSClient,
    logger: AsyncJsonlEventLogger,
    runtime_summary: GapRuntimeSummary,
    snapshot: MarketUniverseSnapshot,
    diff: MarketUniverseDiff,
) -> None:
    detector.update_markets(snapshot.markets)
    for observation in detector.drain_market_update_observations():
        await logger.log(observation)
        runtime_summary.record_gap_event_written(observation)
    polymarket.update_markets(snapshot.markets, token_ids=snapshot.token_ids)
    runtime_summary.update_market_universe(snapshot, diff)


def _format_market_universe_diff(diff: MarketUniverseDiff) -> str:
    return " ".join(
        [
            "market_universe_refresh",
            f"forced={str(diff.forced).lower()}",
            f"added_markets={_format_market_payloads(diff.added_markets)}",
            f"removed_markets={_format_market_payloads(diff.removed_markets)}",
            f"expired_markets={_format_market_payloads(diff.expired_markets)}",
            f"closed_markets={_format_market_payloads(diff.closed_markets)}",
            f"new_token_ids={len(diff.new_token_ids)}",
            f"removed_token_ids={len(diff.removed_token_ids)}",
            f"error={diff.error or '-'}",
        ]
    )


def _format_market_payloads(markets: Sequence[dict[str, Any]]) -> str:
    if not markets:
        return "-"
    return ",".join(str(market.get("market_slug") or market.get("market_id")) for market in markets)


def _counter_from_markets(markets: Sequence[PolymarketMarketMetadata]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for market in markets:
        counter[_base_asset(market)] += 1
    return dict(sorted(counter.items()))


def _slugs_by_base(markets: Sequence[PolymarketMarketMetadata]) -> dict[str, list[str]]:
    slugs: dict[str, list[str]] = {}
    for market in markets:
        slugs.setdefault(_base_asset(market), []).append(market.market_slug)
    return {base: sorted(values) for base, values in sorted(slugs.items())}


def _validation_errors_by_type(readiness: dict[str, Any]) -> dict[str, int]:
    summary = readiness.get("summary")
    if not isinstance(summary, dict):
        return {}
    errors = summary.get("top_validation_errors")
    if isinstance(errors, dict):
        return {
            str(reason): int(count)
            for reason, count in sorted(errors.items())
            if isinstance(count, int)
        }
    return {}


def _runtime_status(
    payload: dict[str, Any],
    stats: GapMonitorStats,
    ws_diagnostics: dict[str, Any],
) -> str:
    if payload.get("discovery_failure_reason") and not payload["signal_enabled_markets_by_base_asset"]:
        return "discovery_degraded"
    if payload["market_refresh_error_count"] > 0 and not payload["signal_enabled_markets_by_base_asset"]:
        return "market_refresh_failing"
    if not payload["signal_enabled_markets_by_base_asset"]:
        if payload["next_warmup_markets_by_base_asset"]:
            return "only_warming_next_markets"
        if payload["expired_selected_markets_by_base_asset"]:
            return "stale_expired"
        return "missing_signal_markets"
    if ws_diagnostics.get("subscription_out_of_sync") is True:
        return "blocked_by_subscriptions"
    if sum(payload.get("signal_book_not_ready_tokens_by_base_asset", {}).values()) > 0:
        return "waiting_for_book_readiness"
    if (
        sum(payload["candidates_created_by_symbol"].values())
        > sum(payload["gap_events_written_by_symbol"].values())
        and (
            sum(payload["pre_entry_rejects_by_symbol"].values())
            + sum(payload["window_rejects_by_symbol"].values())
            + sum(payload["timeout_rejects_by_symbol"].values())
            + sum(payload["suppressed_candidates_by_symbol"].values())
        )
        > 0
    ):
        return "blocked_by_rejects_or_suppression"
    return "actively_measuring_current_markets"


def should_force_market_refresh(
    *,
    enabled: bool,
    signal_enabled_markets: int,
    binance_move_total: int,
    last_forced_refresh_move_total: int,
    now_s: float,
    next_forced_refresh_allowed_at_s: float,
) -> bool:
    return (
        enabled
        and signal_enabled_markets == 0
        and binance_move_total > last_forced_refresh_move_total
        and now_s >= next_forced_refresh_allowed_at_s
    )


def _subscription_matches(ws_diagnostics: dict[str, Any]) -> bool | None:
    if not ws_diagnostics:
        return None
    if ws_diagnostics.get("subscription_status") == "pending":
        return None
    if "subscription_out_of_sync" in ws_diagnostics:
        out_of_sync = ws_diagnostics.get("subscription_out_of_sync")
        if isinstance(out_of_sync, bool):
            return out_of_sync is False
        return None
    runtime_count = ws_diagnostics.get("runtime_token_count")
    active_count = ws_diagnostics.get("active_ws_token_subscription_count")
    if isinstance(runtime_count, int) and isinstance(active_count, int):
        return runtime_count == active_count
    return None


def _book_total_from_payload(payload: dict[str, Any]) -> Counter[str]:
    ready = Counter(payload["book_ready_tokens_by_base_asset"])
    not_ready = Counter(payload["book_not_ready_tokens_by_base_asset"])
    total = Counter()
    for base in set(ready) | set(not_ready):
        total[base] = ready.get(base, 0) + not_ready.get(base, 0)
    return total


def _format_slug_map(slugs: dict[str, list[str]]) -> str:
    if not slugs:
        return "-"
    return ";".join(
        f"{base}[{','.join(values) or '-'}]" for base, values in sorted(slugs.items())
    )


def _format_counter(counter: Counter[str] | dict[str, int]) -> str:
    if not counter:
        return "-"
    return ",".join(f"{key}:{value}" for key, value in sorted(counter.items()))


def _format_ready_tokens(ready: Counter[str], total: Counter[str]) -> str:
    if not ready and not total:
        return "-"
    base_assets = sorted(set(ready) | set(total))
    return ",".join(f"{base}:{ready.get(base, 0)}/{total.get(base, 0)}" for base in base_assets)


def _format_nested_counter(counter: dict[str, dict[str, int]]) -> str:
    if not counter:
        return "-"
    chunks: list[str] = []
    for symbol, reasons in sorted(counter.items()):
        if not reasons:
            chunks.append(f"{symbol}[-]")
            continue
        reason_text = "|".join(
            f"{reason}:{count}"
            for reason, count in sorted(reasons.items(), key=lambda item: (-item[1], item[0]))[:3]
        )
        chunks.append(f"{symbol}[{reason_text}]")
    return ";".join(chunks)


def _parse_symbols(value: str) -> tuple[str, ...]:
    symbols = tuple(symbol.strip().upper() for symbol in value.split(",") if symbol.strip())
    if not symbols:
        raise ValueError("At least one Binance symbol is required.")
    return symbols


async def _discover_polymarket_markets(
    discovery: PolymarketDiscoveryClient,
    *,
    lookahead_windows: int = 1,
    market_cache_ttl_ms: int = DEFAULT_MARKET_CACHE_TTL_MS,
    discovery_debug_jsonl: str | None = None,
) -> tuple[PolymarketMarketMetadata, ...]:
    markets, _attempt = await _discover_polymarket_markets_once(
        discovery,
        lookahead_windows=lookahead_windows,
        market_cache_ttl_ms=market_cache_ttl_ms,
        discovery_debug_jsonl=discovery_debug_jsonl,
    )
    return markets


async def _discover_polymarket_markets_for_startup(
    discovery: PolymarketDiscoveryClient,
    *,
    lookahead_windows: int = 1,
    market_cache_ttl_ms: int = DEFAULT_MARKET_CACHE_TTL_MS,
    discovery_debug_jsonl: str | None = None,
    wait_for_markets: bool = True,
    retry_ms: int = 30_000,
    startup_timeout_ms: int = 300_000,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    monotonic: Callable[[], float] | None = None,
) -> tuple[PolymarketMarketMetadata, ...]:
    loop = asyncio.get_running_loop()
    clock = monotonic or loop.time
    started_at = clock()
    deadline = started_at + max(0, startup_timeout_ms) / 1000.0
    attempts: list[dict[str, Any]] = []

    while True:
        markets, attempt = await _discover_polymarket_markets_once(
            discovery,
            lookahead_windows=lookahead_windows,
            market_cache_ttl_ms=market_cache_ttl_ms,
            discovery_debug_jsonl=discovery_debug_jsonl,
        )
        attempts.append(attempt)
        if markets:
            if len(attempts) > 1:
                print(
                    f"market_discovery_recovered attempts={len(attempts)} "
                    f"selected={len(markets)}",
                    flush=True,
                )
            return markets

        _print_discovery_failure_summary(attempt, attempt_index=len(attempts))
        if not wait_for_markets:
            return ()

        now = clock()
        if now >= deadline:
            _print_startup_timeout_summary(attempts)
            raise SystemExit("no_active_markets_after_startup_timeout")

        delay_s = max(0.001, retry_ms / 1000.0)
        remaining_s = max(0.0, deadline - now)
        await sleep(min(delay_s, remaining_s))


async def _discover_polymarket_markets_once(
    discovery: PolymarketDiscoveryClient,
    *,
    lookahead_windows: int = 1,
    market_cache_ttl_ms: int = DEFAULT_MARKET_CACHE_TTL_MS,
    discovery_debug_jsonl: str | None = None,
) -> tuple[tuple[PolymarketMarketMetadata, ...], dict[str, Any]]:
    now_ts = utc_now_ns() // 1_000_000_000
    try:
        if discovery.enable_direct_slug_lookup:
            rolling_result = await discovery.discover_rolling_markets_robust(
                write_cache=True,
                now_ts=now_ts,
                lookahead_windows=lookahead_windows,
                market_cache_ttl_ms=market_cache_ttl_ms,
                discovery_debug_jsonl=discovery_debug_jsonl,
                refresh_reason="startup",
            )
        else:
            legacy_discovered = await discovery.discover(
                write_cache=True,
                now_ts=now_ts,
                rolling_lookahead_windows=lookahead_windows,
                market_cache_ttl_ms=market_cache_ttl_ms,
                discovery_debug_jsonl=discovery_debug_jsonl,
                refresh_reason="startup",
            )
            runtime_markets = (
                select_runtime_market_universe(
                    legacy_discovered,
                    now_ts=now_ts,
                    lookahead_windows=lookahead_windows,
                )
                if lookahead_windows > 1
                else select_runtime_markets(legacy_discovered, now_ts=now_ts)
            )
            attempt = {
                "event_type": "polymarket_discovery_attempt",
                "refresh_reason": "startup",
                "runtime_tradable_count": len(runtime_markets),
                "current_signal_count": sum(1 for market in runtime_markets if market.signal_enabled),
                "next_warmup_count": sum(
                    1
                    for market in runtime_markets
                    if market.runtime_selection_reason == "next_warmup"
                ),
                "selected_market_slugs": [market.market_slug for market in runtime_markets],
                "current_signal_slugs": [
                    market.market_slug for market in runtime_markets if market.signal_enabled
                ],
                "next_warmup_slugs": [
                    market.market_slug
                    for market in runtime_markets
                    if market.runtime_selection_reason == "next_warmup"
                ],
                "fallback_used": False,
                "failure_reason": None if runtime_markets else "no_runtime_tradable_markets",
            }
            return runtime_markets, attempt
    except (aiohttp.ClientError, TimeoutError, OSError, ValueError) as exc:
        print(
            "live Polymarket discovery failed "
            f"({type(exc).__name__}: {exc}); trying validated cached market metadata",
            flush=True,
        )
        cache_validation = discovery.validate_cache_for_runtime(
            now_ts=now_ts,
            ttl_ms=market_cache_ttl_ms,
            lookahead_windows=lookahead_windows,
        )
        attempt = {
            "event_type": "polymarket_discovery_attempt",
            "refresh_reason": "startup",
            "timestamp": None,
            "now_utc": None,
            "strategy_results": {"cache": cache_validation.to_summary()},
            "runtime_tradable_count": len(cache_validation.runtime_markets),
            "current_signal_count": sum(
                1 for market in cache_validation.runtime_markets if market.signal_enabled
            ),
            "next_warmup_count": sum(
                1
                for market in cache_validation.runtime_markets
                if market.runtime_selection_reason == "next_warmup"
            ),
            "cache_used": cache_validation.valid,
            "cache_rejected": cache_validation.rejected,
            "cache_rejected_reason": cache_validation.rejected_reason,
            "selected_market_slugs": [
                market.market_slug for market in cache_validation.runtime_markets
            ],
            "current_signal_slugs": [
                market.market_slug
                for market in cache_validation.runtime_markets
                if market.signal_enabled
            ],
            "next_warmup_slugs": [
                market.market_slug
                for market in cache_validation.runtime_markets
                if market.runtime_selection_reason == "next_warmup"
            ],
            "fallback_used": True,
            "failure_reason": None if cache_validation.valid else "live_discovery_failed",
        }
        if discovery_debug_jsonl is not None:
            from app.marketdata.polymarket_discovery import write_discovery_attempt_jsonl

            write_discovery_attempt_jsonl(discovery_debug_jsonl, attempt)
        if cache_validation.valid:
            print(f"using cached Polymarket markets ({len(cache_validation.markets)})", flush=True)
            _print_runtime_selection_diagnostics(cache_validation.markets)
            return cache_validation.runtime_markets, attempt
        return (), attempt

    discovered = rolling_result.markets

    if discovered:
        _print_runtime_selection_diagnostics(discovered)
        if rolling_result.runtime_markets:
            return rolling_result.runtime_markets, rolling_result.attempt
        if lookahead_windows > 1:
            runtime_markets = select_runtime_market_universe(
                discovered,
                now_ts=now_ts,
                lookahead_windows=lookahead_windows,
            )
        else:
            runtime_markets = select_runtime_markets(discovered, now_ts=now_ts)
        if runtime_markets:
            return runtime_markets, rolling_result.attempt
        print(
            f"discovered {len(discovered)} rolling markets, but none are runtime-tradable/current-or-warmup",
            flush=True,
        )

    if rolling_result.cache_used and rolling_result.runtime_markets:
        print(f"using cached Polymarket markets ({len(discovered)})", flush=True)
        return rolling_result.runtime_markets, rolling_result.attempt
    return (), rolling_result.attempt


def _print_discovery_failure_summary(
    attempt: dict[str, Any],
    *,
    attempt_index: int,
) -> None:
    strategy_results = attempt.get("strategy_results") or {}
    direct = strategy_results.get("direct_slug") or {}
    active = strategy_results.get("active_events") or {}
    cache = strategy_results.get("cache") or {}
    print(
        " ".join(
            [
                "market_discovery_no_runtime_markets",
                f"attempt={attempt_index}",
                f"failure_reason={attempt.get('failure_reason') or '-'}",
                f"direct_found_count={direct.get('found_count', attempt.get('direct_found_count', 0))}",
                f"direct_runtime_count={direct.get('runtime_tradable_count', 0)}",
                f"active_events_found_runtime_count={active.get('runtime_tradable_count', 0)}",
                f"cache_runtime_count={cache.get('runtime_count', 0)}",
                f"cache_rejected={cache.get('rejected', attempt.get('cache_rejected', False))}",
                f"cache_rejected_reason={cache.get('rejected_reason') or attempt.get('cache_rejected_reason') or '-'}",
            ]
        ),
        flush=True,
    )
    for diagnostic in attempt.get("diagnostics") or []:
        print(f"discovery_diagnostic={diagnostic}", flush=True)


def _print_startup_timeout_summary(attempts: Sequence[dict[str, Any]]) -> None:
    last = attempts[-1] if attempts else {}
    print(
        " ".join(
            [
                "no_active_markets_after_startup_timeout",
                f"attempts={len(attempts)}",
                f"last_failure_reason={last.get('failure_reason') or '-'}",
                f"last_runtime_tradable_count={last.get('runtime_tradable_count', 0)}",
                f"last_current_signal_count={last.get('current_signal_count', 0)}",
                f"last_next_warmup_count={last.get('next_warmup_count', 0)}",
            ]
        ),
        flush=True,
    )


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
                    f"classification={market.classification or '-'}",
                    f"signal={market.signal_enabled}",
                ]
            ),
            flush=True,
        )


def _print_runtime_selection_diagnostics(
    markets: Sequence[PolymarketMarketMetadata],
) -> None:
    now_ts = utc_now_ns() // 1_000_000_000
    selected_count = sum(1 for market in markets if market.selected_for_runtime)
    signal_count = sum(1 for market in markets if market.signal_enabled)
    warmup_count = sum(
        1
        for market in markets
        if market.selected_for_runtime and not market.signal_enabled
        and classify_market_window(market, now_ts=now_ts) == "next_warmup"
    )
    future_count = sum(
        1
        for market in markets
        if market.selected_for_runtime
        and not market.signal_enabled
        and classify_market_window(market, now_ts=now_ts) == "future_tracked"
    )
    skipped_by_classification: dict[str, int] = {}
    for market in markets:
        if market.selected_for_runtime:
            continue
        classification = market.classification or "unknown"
        skipped_by_classification[classification] = (
            skipped_by_classification.get(classification, 0) + 1
        )
    skipped = ",".join(
        f"{classification}:{count}"
        for classification, count in sorted(skipped_by_classification.items())
    )
    print(
        " ".join(
            [
                "Polymarket runtime selection:",
                f"total={len(markets)}",
                f"selected_for_runtime={selected_count}",
                f"signal_enabled={signal_count}",
                f"warmup_only={warmup_count}",
                f"future_tracked={future_count}",
                f"skipped_by_classification={skipped or '-'}",
            ]
        ),
        flush=True,
    )


def _write_book_readiness_debug(
    client: PolymarketWSClient,
    *,
    payload: dict[str, object] | None = None,
) -> None:
    output_path = Path("data/debug/polymarket_book_readiness.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    debug_payload = payload if payload is not None else client.book_readiness_snapshot()
    output_path.write_bytes(
        orjson.dumps(
            debug_payload,
            option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE,
        )
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
            f"pre_entry_written={stats.pre_entry_observations_written}",
            f"pre_entry_suppressed={stats.pre_entry_observations_suppressed}",
            f"book_warmup_suppressed={stats.book_warmup_suppressed}",
            f"warmup_quotes={stats.warmup_quotes_received}",
            f"signal_markets={stats.signal_enabled_markets}",
            f"warmup_markets={stats.warmup_only_markets}",
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


def _format_book_readiness_summary(payload: dict[str, object]) -> str:
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        return "book_readiness=unavailable"
    top_errors = summary.get("top_validation_errors")
    if isinstance(top_errors, dict):
        top_error_text = ",".join(
            f"{reason}:{count}" for reason, count in sorted(top_errors.items())
        )
    else:
        top_error_text = "-"
    avg_ms = summary.get("average_time_to_first_complete_quote_ms")
    avg_text = "-" if not isinstance(avg_ms, int | float) else f"{avg_ms:.2f}ms"
    return " ".join(
        [
            "book_readiness",
            f"validation_mode={summary.get('validation_mode', '-')}",
            f"tolerance_ticks={summary.get('validation_tolerance_ticks', '-')}",
            f"selected={summary.get('selected_runtime_markets', 0)}",
            f"signal={summary.get('signal_enabled_markets', 0)}",
            f"warmup_only={summary.get('warmup_only_markets', 0)}",
            f"complete={summary.get('complete_markets', 0)}",
            f"incomplete={summary.get('incomplete_markets', 0)}",
            f"avg_first_complete={avg_text}",
            f"top_validation_errors={top_error_text or '-'}",
            f"mismatch_samples={summary.get('sampled_mismatch_file_path', '-')}",
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
