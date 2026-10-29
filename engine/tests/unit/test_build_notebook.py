"""The generated notebook holds every check, each with the SQL the engine runs (scripts/build_notebook.py)."""
import ast
import importlib.util
import re
from pathlib import Path

import pytest

from lakebase_health.checkspec import load_checks, render_sql
from lakebase_health.config import EngineConfig
from lakebase_health.exclusions import load_exclusions
from lakebase_health.executors.api import API_CHECKS
from lakebase_health.executors.delta import DeltaExecutor

REPO = Path(__file__).resolve().parents[3]
CHECKS = REPO / "engine" / "src" / "lakebase_health" / "checks"
NOTEBOOK = REPO / "notebooks" / "lakebase_health_check.py"
SEP = "\n# COMMAND ----------\n"


def _generator():
    spec = importlib.util.spec_from_file_location("build_notebook", REPO / "scripts" / "build_notebook.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Sql:
    def query(self, sql):
        self.sql = sql
        return []


def expected_sql(spec, roles):
    """What the engine executes: DeltaExecutor for telemetry (catalog/schema left as placeholders), render_sql for
    Postgres, with the exclusions the engine job builds (exclusions.yaml + --service-roles)."""
    excl = load_exclusions(CHECKS / "exclusions.yaml").with_query_users(roles)
    if spec.source == "delta":
        sql = _Sql()
        DeltaExecutor(sql, EngineConfig(catalog="{catalog}", schema="{schema}"), excl).run(spec)
        return sql.sql
    return render_sql(spec, excl)


def notebook_checks(text):
    """check id -> (cell source, SQL literal or None) for every check cell."""
    assert text.startswith("# Databricks notebook source\n")
    out = {}
    for cell in text.split(SEP):
        m = re.match(r"\s*# check: (\S+)\n", cell)
        if not m:
            continue
        assert m.group(1) not in out, f"{m.group(1)} appears twice"
        sql = None
        for node in ast.walk(ast.parse(cell)):
            if isinstance(node, ast.Assign) and node.targets[0].id == "SQL":
                sql = ast.literal_eval(node.value)
        out[m.group(1)] = (cell, sql)
    return out


def _check_notebook(text, roles):
    specs = load_checks(CHECKS, frozenset(API_CHECKS))
    cells = notebook_checks(text)
    assert set(cells) == {s.id for s in specs}
    for s in specs:
        cell, sql = cells[s.id]
        if s.source == "api":
            assert sql is None and f'run_api("{s.id}"' in cell and f'function="{s.function}"' in cell
            continue
        assert sql is not None, s.id
        assert sql.removeprefix("\n") == expected_sql(s, roles), s.id
        assert (f'run_delta(SQL, "{s.id}"' if s.source == "delta" else f'run_pg(SQL, "{s.id}"') in cell
        if s.source == "postgres":
            assert f"per_database={s.per_database}" in cell, s.id


@pytest.mark.parametrize("roles", [(), ("11111111-2222-3333-4444-555555555555", "sp-fixer")])
def test_generated_notebook_matches_engine(roles):
    text, counts = _generator().build(roles)
    _check_notebook(text, roles)
    specs = load_checks(CHECKS, frozenset(API_CHECKS))
    assert counts == {src: sum(s.source == src for s in specs) for src in ("delta", "postgres", "api")}


def test_query_scoped_sql_filters_platform_and_service_roles():
    text, _ = _generator().build(("sp-engine",))
    _, sql = notebook_checks(text)["top_db_time_query"]
    assert "LEFT ANTI JOIN" in sql and "'sp-engine'" in sql and "system.lakebase" not in sql
    assert "{catalog}.{schema}.raw_pg_stat_statements_counters" in sql


def test_committed_notebook_is_current():
    """The committed notebook was generated from the current YAMLs, with the service roles it records."""
    text = NOTEBOOK.read_text()
    roles = ast.literal_eval(re.search(r"^SERVICE_ROLES = (.*)$", text, re.M).group(1))
    _check_notebook(text, roles)
