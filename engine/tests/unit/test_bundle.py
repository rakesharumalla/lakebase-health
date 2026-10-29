"""Bundle wiring (databricks.yml, resources/): one ingest interval drives the engine's stale-source threshold and
the app's stale banner (B7)."""
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[3]


def _load(name):
    return yaml.safe_load((REPO / name).read_text())


def test_ingest_interval_is_one_variable_for_both_jobs_and_the_app():
    variables = _load("databricks.yml")["variables"]
    assert variables["ingest_interval_minutes"]["default"] == "60"
    for f, key in (("resources/engine_job.yml", "lakebase_health_engine"),
                   ("resources/analysis_job.yml", "lakebase_health_analysis")):
        params = _load(f)["resources"]["jobs"][key]["tasks"][0]["python_wheel_task"]["parameters"]
        i = params.index("--ingest-interval-minutes")
        assert params[i + 1] == "${var.ingest_interval_minutes}", f
    env = {e["name"]: e for e in _load("resources/app.yml")["resources"]["apps"]["lakebase_health_app"]["config"]["env"]}
    assert env["SCHEDULE_MINUTES"]["value"] == "${var.ingest_interval_minutes}"
