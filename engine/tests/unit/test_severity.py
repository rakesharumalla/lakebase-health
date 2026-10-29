import math

from lakebase_health.severity import classify, guards_pass


def test_classify_above_is_strict():
    t = {"warning": 50, "critical": 75}
    assert classify(50, t) is None
    assert classify(74.55, t) == "warning"
    assert classify(76, t) == "critical"


def test_classify_below():
    t = {"warning": 99, "critical": 95}
    assert classify(100, t, "below") is None
    assert classify(97, t, "below") == "warning"
    assert classify(90, t, "below") == "critical"


def test_classify_info_only():
    assert classify(1, {"info": 0}) == "info"


def test_classify_none_and_nan():
    assert classify(None, {"warning": 0}) is None
    assert classify(math.nan, {"warning": 0}) is None
    assert classify("12.5", {"warning": 10}) == "warning"


def test_guards():
    assert guards_pass(("stats_window_hours >= 24",), {"stats_window_hours": 30})
    assert not guards_pass(("stats_window_hours >= 24",), {"stats_window_hours": 2})
    assert not guards_pass(("stats_window_hours >= 24",), {})
    assert guards_pass((), {})
