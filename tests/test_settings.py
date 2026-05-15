from app.config.settings import Settings
from app.main import parse_args


def test_default_validation_mode_is_tolerant() -> None:
    settings = Settings.model_validate({})

    assert settings.polymarket_best_validation_mode == "tolerant"


def test_default_validation_tolerance_is_one_tick() -> None:
    settings = Settings.model_validate({})

    assert settings.polymarket_best_validation_tolerance_ticks == 1


def test_gap_monitor_cli_can_override_validation_mode_to_strict() -> None:
    args = parse_args(["gap-monitor", "--best-validation-mode", "strict"])

    assert args.best_validation_mode == "strict"


def test_gap_monitor_cli_can_override_validation_mode_to_diagnostic() -> None:
    args = parse_args(["gap-monitor", "--best-validation-mode", "diagnostic"])

    assert args.best_validation_mode == "diagnostic"


def test_gap_monitor_cli_can_override_validation_tolerance_ticks() -> None:
    args = parse_args(["gap-monitor", "--best-validation-tolerance-ticks", "2"])

    assert args.best_validation_tolerance_ticks == 2
