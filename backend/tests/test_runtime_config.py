import pytest

from app.runtime_config import seconds_from_env


def test_seconds_from_env_uses_default_and_override(monkeypatch):
    monkeypatch.delenv("KCL_TEST_SECONDS", raising=False)
    assert seconds_from_env("KCL_TEST_SECONDS", 60, 10, 3600) == 60
    monkeypatch.setenv("KCL_TEST_SECONDS", "45.5")
    assert seconds_from_env("KCL_TEST_SECONDS", 60, 10, 3600) == 45.5


@pytest.mark.parametrize("value", ["", "0", "NaN", "inf", "3601", "bad"])
def test_seconds_from_env_rejects_invalid_values(monkeypatch, value):
    monkeypatch.setenv("KCL_TEST_SECONDS", value)
    with pytest.raises(ValueError, match="KCL_TEST_SECONDS"):
        seconds_from_env("KCL_TEST_SECONDS", 60, 10, 3600)
