from __future__ import annotations
from dataclasses import dataclass, field


@dataclass
class FaultEngine:
    """Phase 5 stub: fault matrix, default OFF."""
    enabled: bool = False
    faults: dict = field(default_factory=dict)

    def set(self, name: str, on: bool) -> None:
        self.faults[name] = on
        self.enabled = any(self.faults.values())

    def clear(self) -> None:
        self.faults.clear()
        self.enabled = False
