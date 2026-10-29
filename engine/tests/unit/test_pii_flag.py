import pytest

from lakebase_health.config import pii_masking_enabled


@pytest.mark.parametrize("value", ["false", "False", " FALSE ", "false\n"])
def test_only_false_turns_masking_off(value):
    assert pii_masking_enabled(value) is False


@pytest.mark.parametrize("value", [None, "", "true", "True", "0", "no", "off", "ture", "yes", "falsey", "f"])
def test_anything_else_keeps_masking_on(value):
    assert pii_masking_enabled(value) is True


@pytest.mark.parametrize("module", ["lakebase_health.jobs.run", "lakebase_health.jobs.fix"])
def test_entry_points_use_the_strict_parse(module):
    import importlib
    import inspect
    src = inspect.getsource(importlib.import_module(module))
    assert "pii_masking_enabled(a.pii_masking)" in src
    assert 'pii_masking.lower() == "true"' not in src
