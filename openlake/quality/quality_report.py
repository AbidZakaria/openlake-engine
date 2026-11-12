"""Quality report data model (one per dataset per processing run)."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone


@dataclass
class RuleResult:
    rule: str
    rule_type: str
    dimension: str
    severity: str
    column: str | None
    failed: int
    checked: int

    @property
    def passed(self) -> bool:
        return self.failed == 0

    @property
    def pass_rate(self) -> float:
        return 1.0 if self.checked == 0 else round(1 - self.failed / self.checked, 6)


@dataclass
class QualityReport:
    run_id: str
    dataset: str
    layer: str
    batch_ids: list[str]
    total_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    duplicate_records: int = 0
    null_counts: dict[str, int] = field(default_factory=dict)
    rules: list[RuleResult] = field(default_factory=list)
    executed_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @property
    def failed_rules(self) -> list[str]:
        return [r.rule for r in self.rules if not r.passed]

    @property
    def score(self) -> float:
        """Share of records that passed every blocking (error-severity) rule."""
        return 1.0 if self.total_records == 0 else round(self.valid_records / self.total_records, 6)

    @property
    def status(self) -> str:
        if any(not r.passed and r.severity == "error" for r in self.rules):
            return "FAILED_RECORDS_QUARANTINED"
        if any(not r.passed for r in self.rules):
            return "PASSED_WITH_WARNINGS"
        return "PASSED"

    def to_row(self) -> dict:
        return {
            "run_id": self.run_id,
            "dataset": self.dataset,
            "layer": self.layer,
            "batch_ids": ",".join(sorted(self.batch_ids)),
            "total_records": self.total_records,
            "valid_records": self.valid_records,
            "invalid_records": self.invalid_records,
            "duplicate_records": self.duplicate_records,
            "quality_score": float(self.score),
            "status": self.status,
            "failed_rules": ",".join(self.failed_rules),
            "null_counts": json.dumps(self.null_counts, sort_keys=True),
            "executed_at": datetime.fromisoformat(self.executed_at),
        }

    def rule_rows(self) -> list[dict]:
        return [
            {
                "run_id": self.run_id,
                "dataset": self.dataset,
                "rule": r.rule,
                "rule_type": r.rule_type,
                "dimension": r.dimension,
                "severity": r.severity,
                "column_name": r.column,
                "checked": r.checked,
                "failed": r.failed,
                "pass_rate": float(r.pass_rate),
                "executed_at": datetime.fromisoformat(self.executed_at),
            }
            for r in self.rules
        ]

    def to_dict(self) -> dict:
        data = asdict(self)
        data.update(status=self.status, score=self.score, failed_rules=self.failed_rules)
        return data
