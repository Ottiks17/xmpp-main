import pytest

from app.validation import validate_jid, validate_message


def test_validate_message_ok():
    assert validate_message("  hello  ") == "hello"


def test_validate_message_too_long():
    with pytest.raises(ValueError):
        validate_message("x" * 300)


def test_validate_jid_with_domain():
    assert validate_jid("user", "arsenal") == "user@arsenal"


def test_validate_jid_full():
    assert validate_jid("user@arsenal", "arsenal") == "user@arsenal"

