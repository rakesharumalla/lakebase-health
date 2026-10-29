from pathlib import Path

import pytest

from lakebase_health.checkspec import CheckSpecError, load_checks, parse_check, render_sql, render_template
from lakebase_health.exclusions import Exclusions, load_exclusions

PKG = Path(__file__).resolve().parents[2] / "src" / "lakebase_health" / "checks"

BASE = {
    "id": "x", "title": "X", "category": "c", "source": "postgres", "explain": "e",
    "sql": "SELECT 1 AS measure, 'a' AS object FROM pg_class WHERE relnamespace NOT IN {excluded_schemas}",
    "thresholds": {"warning": 0},
}


def test_parse_minimal_check():
    spec = parse_check(dict(BASE), stem="x")
    assert spec.direction == "above" and spec.schedule == "every_run" and spec.fix is None


@pytest.mark.parametrize("patch,msg", [
    ({"source": "nope"}, "source"),
    ({"thresholds": {}}, "thresholds"),
    ({"thresholds": {"warning": 10, "critical": 5}}, "order"),
    ({"sql": "SELECT {excluded_bogus}"}, "unknown token"),
    ({"guards": ["stats_window ~ 3"]}, "guard"),
    ({"fix": {"kind": "ddl", "template": "X", "risk": "safe", "reversible": True}}, "rollback"),
])
def test_invalid_checks_are_rejected(patch, msg):
    with pytest.raises(CheckSpecError, match=msg):
        parse_check({**BASE, **patch}, stem="x")


def test_id_must_match_filename():
    with pytest.raises(CheckSpecError, match="filename"):
        parse_check(dict(BASE), stem="y")


def test_api_check_needs_known_function():
    data = {k: v for k, v in BASE.items() if k != "sql"} | {"source": "api", "function": "f"}
    with pytest.raises(CheckSpecError, match="function"):
        parse_check(data, stem="x", api_functions=frozenset({"g"}))
    assert parse_check(data, stem="x", api_functions=frozenset({"f"})).function == "f"


def test_below_direction_order():
    spec = parse_check({**BASE, "direction": "below", "thresholds": {"warning": 99, "critical": 95}}, stem="x")
    assert spec.thresholds == {"warning": 99.0, "critical": 95.0}


def test_render_sql_replaces_tokens():
    excl = Exclusions(slots=("s1",), roles=("r1",), users=("u1",), schemas=("pg_catalog", "sage"),
                      role_regex="^(pg_)", settings_regex="^neon_")
    out = render_sql(parse_check(dict(BASE), stem="x"), excl)
    assert "('pg_catalog', 'sage')" in out and "{" not in out


def test_render_template():
    assert render_template("DROP INDEX {i};", {"i": "a.b"}) == "DROP INDEX a.b;"
    assert render_template("DROP INDEX {i};", {"i": None}) is None
    assert render_template(None, {}) is None


def test_fix_actionable_rules():
    fix = {"kind": "ddl", "template": "A {x}", "rollback": "B", "risk": "safe", "reversible": True}
    assert parse_check({**BASE, "fix": fix}, stem="x").fix.actionable
    manual = {"kind": "manual", "template": "do x", "risk": "high", "reversible": False}
    assert not parse_check({**BASE, "fix": manual}, stem="x").fix.actionable


def test_packaged_exclusions_file_loads():
    excl = load_exclusions(PKG / "exclusions.yaml")
    assert "wal_retention_slot" in excl.slots and "sage" in excl.schemas and "cloud_admin" in excl.users


def test_load_checks_rejects_duplicate_ids(tmp_path):
    for d in ("a", "b"):
        (tmp_path / d).mkdir()
        (tmp_path / d / "x.yaml").write_text(
            "id: x\ntitle: X\ncategory: c\nsource: delta\nexplain: e\nsql: SELECT 1\nthresholds: {warning: 0}\n")
    with pytest.raises(CheckSpecError, match="duplicate"):
        load_checks(tmp_path, frozenset())
