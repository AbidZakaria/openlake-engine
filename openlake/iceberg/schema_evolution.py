"""Schema-evolution policy.

When a raw file arrives, its column list is compared with the entity's data
contract. Every difference is classified and the configured policy decides
what happens:

==================  ==========================================  ==============
change              example                                     default action
==================  ==========================================  ==============
``ADD_COLUMN``      upstream adds ``delivery_priority``          accept → Iceberg ``ADD COLUMN``
``MISSING_OPTIONAL``  optional contract column absent           accept → NULLs
``MISSING_REQUIRED``  ``seller_state`` renamed to ``state``      reject → rows quarantined
==================  ==========================================  ==============

Accepted additive changes become an Iceberg schema change — a metadata-only
operation: existing data files are not rewritten, old rows simply read the new
column as NULL. Iceberg tracks columns by id, not by name or position, which is
what makes renames and reorders safe as well.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from openlake.config.pipeline import EntitySpec, EvolutionPolicy

ADD_COLUMN = "ADD_COLUMN"
MISSING_OPTIONAL = "MISSING_OPTIONAL"
MISSING_REQUIRED = "MISSING_REQUIRED"

ACCEPTED = "ACCEPTED"
REJECTED = "REJECTED"

# Metadata columns added by the platform itself — never part of a contract diff.
PLATFORM_COLUMNS = {"_batch_id", "_source_file", "_ingested_at", "_source_format", "_source_columns",
                    "_record_hash", "_corrupt_record"}


@dataclass(frozen=True)
class SchemaChange:
    change_type: str
    column: str
    action: str
    detail: str = ""
    new_type: str | None = None


@dataclass
class SchemaDecision:
    entity: str
    source_columns: list[str]
    changes: list[SchemaChange] = field(default_factory=list)

    @property
    def accepted(self) -> bool:
        return all(c.action == ACCEPTED for c in self.changes)

    @property
    def columns_to_add(self) -> list[SchemaChange]:
        return [c for c in self.changes if c.change_type == ADD_COLUMN and c.action == ACCEPTED]

    @property
    def rejected(self) -> list[SchemaChange]:
        return [c for c in self.changes if c.action == REJECTED]


def contract_columns(entity: EntitySpec, evolved: list[str] | None = None) -> list[str]:
    """Contract columns plus columns accepted by earlier evolutions."""
    cols = list(entity.column_names)
    for c in evolved or []:
        if c not in cols:
            cols.append(c)
    return cols


def classify(entity: EntitySpec, source_columns: list[str], policy: EvolutionPolicy,
             known_columns: list[str] | None = None) -> SchemaDecision:
    """Compare the columns of an incoming file with the contract.

    ``source_columns`` are raw names (before ``rename``). ``known_columns`` are
    columns the Silver table already has (contract + previously evolved).
    """
    known = set(known_columns or entity.column_names)
    renamed = [entity.rename.get(c, c) for c in source_columns if c not in PLATFORM_COLUMNS]
    present = set(renamed)
    decision = SchemaDecision(entity.name, list(source_columns))

    for col in renamed:
        if col not in known and col not in entity.column_names:
            action = ACCEPTED if policy.additive == "accept" else REJECTED
            new_type = entity.evolution_hints.get(col, "string")
            decision.changes.append(SchemaChange(ADD_COLUMN, col, action,
                                                 f"new upstream column, type {new_type}", new_type))
    for spec in entity.columns:
        if spec.name in present:
            continue
        if spec.required:
            action = ACCEPTED if policy.missing_required == "accept" else REJECTED
            decision.changes.append(SchemaChange(MISSING_REQUIRED, spec.name, action,
                                                 "required contract column absent from source"))
        else:
            action = ACCEPTED if policy.missing_optional == "accept" else REJECTED
            decision.changes.append(SchemaChange(MISSING_OPTIONAL, spec.name, action,
                                                 "optional contract column absent; filled with NULL"))
    if not decision.accepted:
        # A file is accepted or rejected as a whole: an additive column that
        # arrives together with a breaking change is not applied either.
        decision.changes = [
            SchemaChange(c.change_type, c.column, REJECTED, c.detail + " (file rejected by another change)", c.new_type)
            if c.action == ACCEPTED and c.change_type == ADD_COLUMN else c
            for c in decision.changes
        ]
    return decision


def spark_type(contract_type: str) -> str:
    return {"string": "string", "int": "int", "bigint": "bigint", "double": "double",
            "decimal": "decimal(18,2)", "boolean": "boolean", "date": "date",
            "timestamp": "timestamp"}[contract_type]


def add_columns_sql(table: str, changes: list[SchemaChange]) -> list[str]:
    return [f"ALTER TABLE {table} ADD COLUMN `{c.column}` {spark_type(c.new_type or 'string')} "
            f"COMMENT 'added by schema evolution'" for c in changes]
