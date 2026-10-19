"""Validated process settings shared by local and container deployments."""

from __future__ import annotations

import math
import os


def seconds_from_env(name: str, default: float, minimum: float, maximum: float) -> float:
    raw = os.environ.get(name, str(default))
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} 必须是数字（当前值：{raw!r}）") from exc
    if not math.isfinite(value) or not minimum <= value <= maximum:
        raise ValueError(f"{name} 必须在 {minimum:g}–{maximum:g} 秒之间（当前值：{raw!r}）")
    return value
