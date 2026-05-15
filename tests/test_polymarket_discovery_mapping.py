from app.marketdata.polymarket_discovery import parse_market_metadata


def _payload(
    *,
    outcomes: list[str],
    token_ids: list[str] | None = None,
    question: str = "Bitcoin Up or Down - 15 minute",
    slug: str = "bitcoin-up-or-down-15m",
) -> dict[str, object]:
    return {
        "conditionId": "0xcondition",
        "market": "0xmarket",
        "slug": slug,
        "question": question,
        "endDateIso": "2026-05-15T12:15:00Z",
        "clobTokenIds": token_ids if token_ids is not None else ["token-a", "token-b"],
        "outcomes": outcomes,
        "order_price_min_tick_size": "0.01",
        "minimum_order_size": "5",
    }


def test_outcomes_up_down_map_directly() -> None:
    metadata = parse_market_metadata(_payload(outcomes=["Up", "Down"]))

    assert metadata is not None
    assert metadata.up_token_id == "token-a"
    assert metadata.down_token_id == "token-b"
    assert metadata.token_for_direction("UP") == "token-a"
    assert metadata.token_for_direction("DOWN") == "token-b"
    assert metadata.token_outcomes == {"token-a": "Up", "token-b": "Down"}


def test_reversed_outcomes_down_up_do_not_break_mapping() -> None:
    metadata = parse_market_metadata(_payload(outcomes=["Down", "Up"]))

    assert metadata is not None
    assert metadata.up_token_id == "token-b"
    assert metadata.down_token_id == "token-a"
    assert metadata.token_for_direction("UP") == "token-b"
    assert metadata.token_for_direction("DOWN") == "token-a"


def test_yes_no_outcomes_use_question_direction() -> None:
    metadata = parse_market_metadata(
        _payload(
            outcomes=["Yes", "No"],
            question="Will Bitcoin be higher in the next 15 minutes?",
            slug="will-bitcoin-be-higher-15m",
        )
    )

    assert metadata is not None
    assert metadata.yes_token_id == "token-a"
    assert metadata.no_token_id == "token-b"
    assert metadata.up_token_id == "token-a"
    assert metadata.down_token_id == "token-b"


def test_yes_no_outcomes_reject_ambiguous_question() -> None:
    reasons: list[str] = []

    metadata = parse_market_metadata(
        _payload(outcomes=["Yes", "No"]),
        reject_logger=reasons.append,
    )

    assert metadata is None
    assert reasons == ["yes_no_direction_ambiguous"]


def test_malformed_outcomes_rejected() -> None:
    reasons: list[str] = []

    metadata = parse_market_metadata(
        _payload(outcomes=["Up"]),
        reject_logger=reasons.append,
    )

    assert metadata is None
    assert reasons == ["malformed_outcomes"]


def test_missing_clob_token_ids_rejected() -> None:
    reasons: list[str] = []

    metadata = parse_market_metadata(
        _payload(outcomes=["Up", "Down"], token_ids=[]),
        reject_logger=reasons.append,
    )

    assert metadata is None
    assert reasons == ["missing_clob_token_ids"]
