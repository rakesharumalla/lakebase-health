from dataclasses import dataclass


def pii_masking_enabled(value) -> bool:
    """Fail closed: masking is off only for the exact string "false" (trimmed, any case)."""
    return not (isinstance(value, str) and value.strip().lower() == "false")

RAW_TABLES = (
    "active_session_history", "wait_event_counters", "plan_history", "pg_stat_statements_counters",
    "ddl_history", "postgres_logs", "compute_counters", "compute_gauges", "database_counters", "database_gauges",
)


@dataclass(frozen=True)
class EngineConfig:
    catalog: str
    schema: str = "lakebase_health"
    source_schema: str = "system.lakebase"
    backfill_days: int = 7
    retention_days: int = 90
    overlap_minutes: int = 10
    engine_enabled: bool = True
    resolve_after_clean_runs: int = 2
    pii_masking: bool = True  # False: drop the mask_text column masks so every reader sees query text
    service_roles: tuple = ()  # engine/fixer service principal roles whose queries the query checks ignore
    ingest_interval_minutes: int = 60  # the ingest schedule; analysis-only runs call a source stale after 2x this

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"

    def raw(self, source: str) -> str:
        return self.table(f"raw_{source}")
