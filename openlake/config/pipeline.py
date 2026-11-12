"""Typed view over ``config/pipeline_config.yaml``.

The YAML is the data contract of the platform. This module validates it once at
load time so that a typo in a rule fails fast instead of half-way through a
Spark job.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

SUPPORTED_TYPES = {"string", "int", "bigint", "double", "decimal", "boolean", "date", "timestamp"}
RULE_TYPES = {"not_null", "unique", "range", "accepted_values", "regex", "not_future", "ordered", "foreign_key"}
SEVERITIES = {"error", "warn"}


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    type: str
    required: bool = False


@dataclass(frozen=True)
class RuleSpec:
    name: str
    type: str
    severity: str
    column: str | None = None
    columns: tuple[str, ...] = ()
    values: tuple[Any, ...] = ()
    min: float | None = None
    max: float | None = None
    pattern: str | None = None
    ref_table: str | None = None
    ref_column: str | None = None

    @property
    def is_row_level(self) -> bool:
        """Row-level rules can quarantine individual records; others are dataset-level."""
        return self.type not in {"unique", "foreign_key"}


@dataclass(frozen=True)
class EntitySpec:
    name: str
    source: str
    file_pattern: str
    keys: tuple[str, ...]
    columns: tuple[ColumnSpec, ...]
    rules: tuple[RuleSpec, ...] = ()
    rename: dict[str, str] = field(default_factory=dict)
    standardize: dict[str, str] = field(default_factory=dict)
    partition_by: tuple[str, ...] = ()
    evolution_hints: dict[str, str] = field(default_factory=dict)

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def required_columns(self) -> list[str]:
        return [c.name for c in self.columns if c.required]

    def column(self, name: str) -> ColumnSpec | None:
        return next((c for c in self.columns if c.name == name), None)

    def source_name(self, column: str) -> str:
        """Name of a contract column in the raw file (inverse of ``rename``)."""
        inverse = {v: k for k, v in self.rename.items()}
        return inverse.get(column, column)


@dataclass(frozen=True)
class EvolutionPolicy:
    additive: str = "accept"
    missing_optional: str = "accept"
    missing_required: str = "reject"


@dataclass(frozen=True)
class MaintenancePolicy:
    retain_last: int = 10
    max_age_days: int = 7
    orphan_older_than_hours: int = 72
    min_input_files: int = 5
    target_file_size_mb: int = 128


@dataclass(frozen=True)
class PipelineConfig:
    catalog: str
    namespaces: dict[str, str]
    table_properties: dict[str, str]
    entities: dict[str, EntitySpec]
    entity_order: tuple[str, ...]
    value_sets: dict[str, tuple[str, ...]]
    evolution: EvolutionPolicy
    maintenance: MaintenancePolicy
    gold_partitions: dict[str, tuple[str, ...]]
    raw_prefixes: dict[str, str]

    def table(self, layer: str, name: str) -> str:
        return f"{self.catalog}.{self.namespaces[layer]}.{name}"

    def entity_for_file(self, filename: str) -> EntitySpec | None:
        """Map a raw file name to an entity; the longest matching pattern wins."""
        lowered = filename.lower()
        matches = [e for e in self.entities.values() if e.file_pattern.lower() in lowered]
        if not matches:
            return None
        return max(matches, key=lambda e: len(e.file_pattern))

    def ordered_entities(self) -> list[EntitySpec]:
        ordered = [self.entities[n] for n in self.entity_order if n in self.entities]
        rest = [e for n, e in self.entities.items() if n not in self.entity_order]
        return ordered + rest


def _parse_rule(raw: dict[str, Any], value_sets: dict[str, tuple[str, ...]], entity: str) -> RuleSpec:
    rtype = raw.get("type")
    if rtype not in RULE_TYPES:
        raise ConfigError(f"{entity}: unknown rule type {rtype!r}")
    severity = raw.get("severity", "error")
    if severity not in SEVERITIES:
        raise ConfigError(f"{entity}: rule {raw.get('name')} has invalid severity {severity!r}")
    values = raw.get("values", ())
    if isinstance(values, str) and values.startswith("@"):
        set_name = values[1:]
        if set_name not in value_sets:
            raise ConfigError(f"{entity}: unknown value set {values}")
        values = value_sets[set_name]
    rule = RuleSpec(
        name=raw.get("name") or f"{entity}_{rtype}_{raw.get('column')}",
        type=rtype,
        severity=severity,
        column=raw.get("column"),
        columns=tuple(raw.get("columns", ())),
        values=tuple(values or ()),
        min=raw.get("min"),
        max=raw.get("max"),
        pattern=raw.get("pattern"),
        ref_table=raw.get("ref_table"),
        ref_column=raw.get("ref_column"),
    )
    if rtype == "ordered" and len(rule.columns) != 2:
        raise ConfigError(f"{entity}: rule {rule.name} needs exactly two columns")
    if rtype != "ordered" and not rule.column:
        raise ConfigError(f"{entity}: rule {rule.name} needs a column")
    if rtype == "accepted_values" and not rule.values:
        raise ConfigError(f"{entity}: rule {rule.name} needs values")
    if rtype == "foreign_key" and not (rule.ref_table and rule.ref_column):
        raise ConfigError(f"{entity}: rule {rule.name} needs ref_table and ref_column")
    return rule


def parse_config(raw: dict[str, Any]) -> PipelineConfig:
    value_sets = {k: tuple(v) for k, v in (raw.get("value_sets") or {}).items()}
    entities: dict[str, EntitySpec] = {}
    raw_prefixes: dict[str, str] = {}
    for source_name, source in (raw.get("sources") or {}).items():
        raw_prefixes[source_name] = source.get("raw_prefix", f"raw/{source_name}")
        for name, ent in (source.get("entities") or {}).items():
            columns = []
            for col, spec in (ent.get("columns") or {}).items():
                ctype = spec.get("type", "string")
                if ctype not in SUPPORTED_TYPES:
                    raise ConfigError(f"{name}.{col}: unsupported type {ctype!r}")
                columns.append(ColumnSpec(col, ctype, bool(spec.get("required", False))))
            if not columns:
                raise ConfigError(f"entity {name} has no columns")
            keys = tuple(ent.get("keys") or ())
            if not keys:
                raise ConfigError(f"entity {name} needs at least one key column")
            col_names = {c.name for c in columns}
            missing_keys = [k for k in keys if k not in col_names]
            if missing_keys:
                raise ConfigError(f"entity {name}: key columns {missing_keys} not in contract")
            for hint_col, hint_type in (ent.get("evolution_hints") or {}).items():
                if hint_type not in SUPPORTED_TYPES:
                    raise ConfigError(f"{name}.{hint_col}: unsupported evolution hint type {hint_type!r}")
            rules = tuple(_parse_rule(r, value_sets, name) for r in (ent.get("rules") or ()))
            entities[name] = EntitySpec(
                name=name,
                source=source_name,
                file_pattern=ent.get("file_pattern", name),
                keys=keys,
                columns=tuple(columns),
                rules=rules,
                rename=dict(ent.get("rename") or {}),
                standardize=dict(ent.get("standardize") or {}),
                partition_by=tuple(ent.get("partition_by") or ()),
                evolution_hints=dict(ent.get("evolution_hints") or {}),
            )
    for ent in entities.values():
        for rule in ent.rules:
            if rule.type == "foreign_key" and rule.ref_table not in entities:
                raise ConfigError(f"{ent.name}: rule {rule.name} references unknown entity {rule.ref_table}")

    evo = raw.get("schema_evolution") or {}
    maint = raw.get("maintenance") or {}
    retention = maint.get("snapshot_retention") or {}
    gold = raw.get("gold") or {}
    return PipelineConfig(
        catalog=raw.get("catalog", "lake"),
        namespaces={
            "bronze": "bronze", "silver": "silver", "gold": "gold",
            "quarantine": "quarantine", "ops": "ops", "lab": "lab",
            **(raw.get("namespaces") or {}),
        },
        table_properties={str(k): str(v) for k, v in (raw.get("table_properties") or {}).items()},
        entities=entities,
        entity_order=tuple(raw.get("entity_order") or entities.keys()),
        value_sets=value_sets,
        evolution=EvolutionPolicy(
            additive=evo.get("additive", "accept"),
            missing_optional=evo.get("missing_optional", "accept"),
            missing_required=evo.get("missing_required", "reject"),
        ),
        maintenance=MaintenancePolicy(
            retain_last=int(retention.get("retain_last", 10)),
            max_age_days=int(retention.get("max_age_days", 7)),
            orphan_older_than_hours=int((maint.get("orphan_files") or {}).get("older_than_hours", 72)),
            min_input_files=int((maint.get("compaction") or {}).get("min_input_files", 5)),
            target_file_size_mb=int((maint.get("compaction") or {}).get("target_file_size_mb", 128)),
        ),
        gold_partitions={k: tuple(v) for k, v in (gold.get("partition_by") or {}).items()},
        raw_prefixes=raw_prefixes,
    )


def load_config(path: str | Path | None = None) -> PipelineConfig:
    if path is None:
        from openlake.config.settings import get_settings

        path = get_settings().config_path
    with open(path, encoding="utf-8") as fh:
        return parse_config(yaml.safe_load(fh))
