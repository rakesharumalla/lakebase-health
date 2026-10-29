"""Every shipped check names one of the 12 report areas, and the app's copy of the mapping is current."""
import importlib.util
from pathlib import Path

import pytest

from lakebase_health.checkspec import AREAS, CheckSpecError, load_checks, parse_check
from lakebase_health.executors.api import API_CHECKS
from lakebase_health.jobs.run import CHECKS_DIR
from lakebase_health.runner import META_SPECS

REPO = Path(__file__).resolve().parents[3]
BASE = {"id": "x", "title": "X", "category": "c", "source": "delta", "explain": "e", "sql": "SELECT 1",
        "thresholds": {"warning": 0}}


def test_there_are_twelve_areas():
    assert len(AREAS) == 12 and len(set(AREAS)) == 12


def test_area_is_parsed_and_validated():
    assert parse_check({**BASE, "area": "indexes"}, stem="x").area == "indexes"
    assert parse_check(dict(BASE), stem="x").area is None
    with pytest.raises(CheckSpecError, match="area must be one of"):
        parse_check({**BASE, "area": "everything"}, stem="x")


def test_every_shipped_check_has_an_area():
    missing = [s.id for s in load_checks(CHECKS_DIR, frozenset(API_CHECKS)) if s.area not in AREAS]
    assert missing == []


def test_project_level_checks_score_in_resilience_and_security():
    specs = {s.id: s for s in load_checks(CHECKS_DIR, frozenset(API_CHECKS))}
    assert {specs[i].area for i in ("primary_without_ha", "backup_restore_window", "default_branch_unprotected")} \
        == {"resilience"}
    assert specs["password_roles"].area == "config_security"


def test_meta_checks_have_no_area():
    assert all(s.area is None for s in META_SPECS.values())


def test_app_check_areas_file_is_current():
    spec = importlib.util.spec_from_file_location("gen_check_areas", REPO / "scripts" / "gen_check_areas.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert (REPO / "app" / "server" / "report" / "checks.json").read_text() == mod.build(), \
        "run python scripts/gen_check_areas.py"
