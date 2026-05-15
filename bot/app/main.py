import argparse
import asyncio
from collections.abc import Sequence
from pathlib import Path

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
from app.execution.paper_executor import PaperExecutor
from app.logging.event_logger import AsyncJsonlEventLogger
from app.marketdata.binance_ws import BinanceWSClient
from app.marketdata.polymarket_discovery import (
    PolymarketDiscoveryClient,
    PolymarketMarketCache,
    PolymarketMarketMetadata,
    annotate_runtime_market_roles,
    flatten_token_ids,
    select_runtime_markets,
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
    markets = await _discover_polymarket_markets(discovery)
    if not markets:
        print(
            "No rolling BTC/ETH 5m/15m markets found from direct slugs. Run:\n"
            " python -m app.main polymarket-rolling-discovery-debug",
            flush=True,
        )
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
    _write_book_readiness_debug(polymarket)
    print(_format_book_readiness_summary(polymarket.book_readiness_snapshot()), flush=True)

    tasks = [
        asyncio.create_task(_ingest_gap_binance(binance, state, detector, logger)),
        asyncio.create_task(_ingest_gap_polymarket(polymarket, state, detector, logger)),
    ]
    try:
        while True:
            await asyncio.sleep(1.0)
            readiness = polymarket.book_readiness_snapshot()
            _write_book_readiness_debug(polymarket, payload=readiness)
            print(_format_gap_stats(detector.stats(state)), flush=True)
            print(_format_book_readiness_summary(readiness), flush=True)
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        await logger.close()


async def run_polymarket_rolling_discovery_debug(args: argparse.Namespace) -> None:
    settings = get_settings()
    discovery = PolymarketDiscoveryClient(
        gamma_url=args.gamma_url or settings.polymarket_gamma_url,
        cache_path=args.cache_path or settings.polymarket_market_cache_path,
    )
    debug = await discovery.debug_rolling_discovery(now_ts=args.now_ts)

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
                        f"UP={market.get('up_token_id')}",
                        f"DOWN={market.get('down_token_id')}",
                        f"outcomes={token_outcomes}",
                    ]
                ),
                flush=True,
            )

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


def _parse_symbols(value: str) -> tuple[str, ...]:
    symbols = tuple(symbol.strip().upper() for symbol in value.split(",") if symbol.strip())
    if not symbols:
        raise ValueError("At least one Binance symbol is required.")
    return symbols


async def _discover_polymarket_markets(
    discovery: PolymarketDiscoveryClient,
) -> tuple[PolymarketMarketMetadata, ...]:
    now_ts = utc_now_ns() // 1_000_000_000
    try:
        discovered = await discovery.discover(write_cache=True, now_ts=now_ts)
    except (aiohttp.ClientError, TimeoutError, OSError, ValueError) as exc:
        print(
            "live Polymarket discovery failed "
            f"({type(exc).__name__}: {exc}); trying cached market metadata",
            flush=True,
        )
        discovered = ()

    if discovered:
        _print_runtime_selection_diagnostics(discovered)
        runtime_markets = select_runtime_markets(discovered, now_ts=now_ts)
        if runtime_markets:
            return runtime_markets
        print(
            f"discovered {len(discovered)} rolling markets, but none are runtime-tradable/current-next",
            flush=True,
        )

    cached = _read_cached_polymarket_markets(discovery)
    if cached.markets:
        cached_markets = annotate_runtime_market_roles(cached.markets, now_ts=now_ts)
        print(f"using cached Polymarket markets ({len(cached_markets)})", flush=True)
        _print_runtime_selection_diagnostics(cached_markets)
        return select_runtime_markets(cached_markets, now_ts=now_ts)
    return ()


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
    selected_count = sum(1 for market in markets if market.selected_for_runtime)
    signal_count = sum(1 for market in markets if market.signal_enabled)
    warmup_count = sum(
        1
        for market in markets
        if market.selected_for_runtime and not market.signal_enabled
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
