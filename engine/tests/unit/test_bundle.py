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


def _wheel_task(f, key):
    return _load(f)["resources"]["jobs"][key]["tasks"][0]["python_wheel_task"]


def test_llm_variables_reach_the_fixer_and_the_app_and_the_test_switch_never_does():
    variables = _load("databricks.yml")["variables"]
    assert variables["llm_endpoint"]["default"] == "databricks-claude-opus-5-5"
    assert variables["llm_enabled"]["default"] == "true"
    params = _wheel_task("resources/fixer_job.yml", "lakebase_health_fixer")["parameters"]
    assert params[params.index("--llm-endpoint") + 1] == "${var.llm_endpoint}"
    assert params[params.index("--llm-enabled") + 1] == "${var.llm_enabled}"
    for f in ("databricks.yml", "resources/fixer_job.yml", "resources/engine_job.yml", "resources/analysis_job.yml",
              "resources/app.yml"):
        assert "llm-test-response" not in (REPO / f).read_text() and "llm_test_response" not in (REPO / f).read_text()
    env = {e["name"]: e for e in _load("resources/app.yml")["resources"]["apps"]["lakebase_health_app"]["config"]["env"]}
    assert env["LLM_ENABLED"]["value"] == "${var.llm_enabled}" and env["LLM_ENDPOINT"]["value"] == "${var.llm_endpoint}"


def test_every_job_installs_sqlglot_like_the_wheel_requires():
    pyproject = (REPO / "engine" / "pyproject.toml").read_text()
    assert '"sqlglot>=30.24,<31"' in pyproject and '"fixer/draft/prompts/*.md"' in pyproject
    for f, key in (("resources/engine_job.yml", "lakebase_health_engine"),
                   ("resources/analysis_job.yml", "lakebase_health_analysis"),
                   ("resources/fixer_job.yml", "lakebase_health_fixer")):
        deps = _load(f)["resources"]["jobs"][key]["environments"][0]["spec"]["dependencies"]
        assert "sqlglot>=30.24,<31" in deps, f
