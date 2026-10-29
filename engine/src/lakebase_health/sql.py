from dataclasses import dataclass
from typing import Any, Callable, Mapping, Protocol


@dataclass(frozen=True)
class Guard:
    """A MERGE matched-row condition: `sql` over t (target) and s (source); `check(t, s)` is the same rule for an
    in-memory Sql."""
    sql: str
    check: Callable[[dict, dict], bool]


class Sql(Protocol):
    def query(self, stmt: str, args: Mapping[str, Any] | None = None) -> list[dict]: ...
    def execute(self, stmt: str, args: Mapping[str, Any] | None = None) -> None: ...
    def table_exists(self, name: str) -> bool: ...
    def insert_rows(self, table: str, rows: list[dict]) -> None: ...
    def merge_rows(self, table: str, rows: list[dict], keys: list[str], only_if: Guard | None = None) -> None: ...


class SparkSql:
    """Sql over a SparkSession. Rows are written using the target table's own schema."""

    def __init__(self, spark):
        self.spark = spark

    def query(self, stmt, args=None):
        return [r.asDict(recursive=True) for r in self.spark.sql(stmt, args=dict(args) if args else None).collect()]

    def execute(self, stmt, args=None):
        self.spark.sql(stmt, args=dict(args) if args else None).collect()

    def table_exists(self, name):
        return self.spark.catalog.tableExists(name)

    def _df(self, table, rows):
        schema = self.spark.table(table).schema
        return self.spark.createDataFrame([tuple(r.get(f.name) for f in schema.fields) for r in rows], schema)

    def insert_rows(self, table, rows):
        if rows:
            self._df(table, rows).write.mode("append").saveAsTable(table)

    def merge_rows(self, table, rows, keys, only_if=None):
        if not rows:
            return
        view = "_lakebase_health_merge_src"
        self._df(table, rows).createOrReplaceTempView(view)
        on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        matched = f"WHEN MATCHED AND ({only_if.sql})" if only_if else "WHEN MATCHED"
        self.spark.sql(f"MERGE INTO {table} t USING {view} s ON {on} "
                       f"{matched} THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *").collect()
