import pytest

from app.core.tick_math import (
    diff_to_ticks,
    is_price_on_tick,
    price_to_ticks,
    round_price_to_tick,
)


def test_price_to_ticks() -> None:
    assert price_to_ticks(0.52, 0.01) == pytest.approx(52.0)


def test_diff_to_ticks() -> None:
    assert diff_to_ticks(0.02, 0.01) == pytest.approx(2.0)


def test_is_price_on_tick_true() -> None:
    assert is_price_on_tick(0.52, 0.01) is True


def test_is_price_on_tick_false() -> None:
    assert is_price_on_tick(0.523, 0.01) is False


def test_round_price_to_tick_buy_and_sell() -> None:
    assert round_price_to_tick(0.523, 0.01, side="buy") == pytest.approx(0.52)
    assert round_price_to_tick(0.523, 0.01, side="sell") == pytest.approx(0.53)
    assert round_price_to_tick(0.523, 0.01) == pytest.approx(0.52)


def test_helpers_handle_small_tick_sizes() -> None:
    assert price_to_ticks(0.523, 0.001) == pytest.approx(523.0)
    assert diff_to_ticks(0.0002, 0.0001) == pytest.approx(2.0)
    assert round_price_to_tick(0.52345, 0.0001, side="buy") == pytest.approx(0.5234)
    assert round_price_to_tick(0.52345, 0.0001, side="sell") == pytest.approx(0.5235)


def test_is_price_on_tick_handles_floating_point_noise() -> None:
    assert is_price_on_tick(0.1 + 0.2, 0.01, tolerance=1e-9) is True


def test_helpers_reject_non_positive_tick_size() -> None:
    with pytest.raises(ValueError):
        price_to_ticks(0.52, 0.0)
