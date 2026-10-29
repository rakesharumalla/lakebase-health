import time
from dataclasses import dataclass
from datetime import timedelta

from .config import RAW_TABLES
from .ddl import MASKED_COLUMNS, set_masks


@dataclass
class IngestResult:
    source: str
    status: str
    rows_added: int | None
    seconds: float
    error: str | None = None


def _watermark(sql, cfg, source):
    rows = sql.query(f"SELECT max_ts FROM {cfg.table('ingest_watermarks')} WHERE source_table = :s", {"s": source})
    return rows[0]["max_ts"] if rows and rows[0].get("max_ts") is not None else None


def _comparable_columns(sql, target):
    # VARIANT columns (e.g. `rescue`) are not allowed in set operations or joins, so dedup compares the rest
    cols = []
    for r in sql.query(f"DESCRIBE TABLE {target}"):
        name = r.get("col_name") or ""
        if not name or name.startswith("#"):
            break
        if (r.get("data_type") or "").lower() != "variant":
            cols.append(name)
    return cols


def ingest_all(sql, cfg, run_id, now, sources=RAW_TABLES) -> list:
    results = []
    for src in sources:  # one table at a time: parallel statements made system-table compile ~10x slower
        t0 = time.monotonic()
        target, source = cfg.raw(src), f"{cfg.source_schema}.{src}"
        # Query text never sits unmasked: with masking on, the table is created empty, masked, then filled, and the
        # masks are set again before every insert. A SET MASK failure fails this table (fail closed).
        masked = cfg.pii_masking and f"raw_{src}" in MASKED_COLUMNS
        backfill = f"SELECT * FROM {source} WHERE ts >= current_timestamp() - INTERVAL {cfg.backfill_days} DAYS"
        try:
            exists = sql.table_exists(target)
            wm = _watermark(sql, cfg, src) if exists else None
            if not exists and not masked:
                sql.execute(f"CREATE TABLE IF NOT EXISTS {target} CLUSTER BY (project_id, ts) AS {backfill}")
                added = None
            else:
                if not exists:
                    sql.execute(f"CREATE TABLE IF NOT EXISTS {target} CLUSTER BY (project_id, ts) AS "
                                f"SELECT * FROM {source} WHERE false")
                if masked:
                    try:
                        set_masks(sql, cfg, f"raw_{src}")
                    except Exception as e:
                        raise RuntimeError(f"masks not set, nothing ingested: {type(e).__name__}: {e}") from e
                if exists and wm is None:
                    wm = sql.query(f"SELECT max(ts) AS max_ts, count(*) AS n FROM {target}")[0]["max_ts"]
                if wm is None:  # new, or left empty by a run whose masks failed
                    out = sql.query(f"INSERT INTO {target} {backfill}")
                else:
                    lo = wm - timedelta(minutes=cfg.overlap_minutes)
                    cols = _comparable_columns(sql, target)
                    on = " AND ".join(f"s.{c} <=> t.{c}" for c in cols)
                    out = sql.query(f"INSERT INTO {target} SELECT s.* "
                                    f"FROM (SELECT * FROM {source} WHERE ts > :lo) s LEFT ANTI JOIN "
                                    f"(SELECT {', '.join(cols)} FROM {target} WHERE ts > :lo) t ON {on}", {"lo": lo})
                added = out[0].get("num_inserted_rows") if out else None
            sql.execute(f"DELETE FROM {target} WHERE ts < current_timestamp() - INTERVAL {cfg.retention_days} DAYS")
            new_max = sql.query(f"SELECT max(ts) AS max_ts, count(*) AS n FROM {target}")[0]["max_ts"]
            secs = time.monotonic() - t0
            sql.merge_rows(cfg.table("ingest_watermarks"),
                           [{"source_table": src, "max_ts": new_max, "last_run_id": run_id,
                             "rows_added": added, "seconds": secs}], ["source_table"])
            results.append(IngestResult(src, "ok", added, secs))
        except Exception as e:  # isolate per table; watermark not advanced
            results.append(IngestResult(src, "error", None, time.monotonic() - t0, f"{type(e).__name__}: {e}"[:1000]))
    return results
