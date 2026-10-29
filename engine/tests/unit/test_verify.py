import pytest

from lakebase_health.fixer.verify import judge, measure

Q = lambda ms, idx=(), write=False, nodes=(): {"ms": ms, "nodes": list(nodes), "indexes": list(idx), "is_write": write}
CHANGED = ["Hash Join"]  # a plan-node set different from the before side's


def test_create_index_passes_with_speedup_and_bounded_write_cost():
    before = {"1": Q(100), "2": Q(10), "3": Q(5, write=True)}
    after = {"1": Q(20, ["hc_test.ix"]), "2": Q(10.5), "3": Q(6, write=True)}
    r = judge("create_index", "hc_test.ix", before, after)
    assert r["passed"], r


def test_create_index_fails_when_unused_or_a_query_regresses_or_writes_get_expensive():
    assert not judge("create_index", "hc_test.ix", {"1": Q(100)}, {"1": Q(95)})["passed"]  # not used
    assert not judge("create_index", "hc_test.ix", {"1": Q(100), "2": Q(10)},
                     {"1": Q(20, ["hc_test.ix"]), "2": Q(12, nodes=CHANGED)})["passed"]
    assert not judge("create_index", "hc_test.ix", {"1": Q(100), "3": Q(5, write=True)},
                     {"1": Q(20, ["hc_test.ix"]), "3": Q(7, write=True)})["passed"]


def test_drop_index_rules():
    assert judge("drop_index", "hc_test.dup", {"1": Q(10, ["hc_test.other"])}, {"1": Q(10.4)})["passed"]
    assert not judge("drop_index", "hc_test.dup", {"1": Q(10, ["hc_test.dup"])}, {"1": Q(10)})["passed"]
    assert not judge("drop_index", "hc_test.dup", {"1": Q(10)}, {"1": Q(12, nodes=CHANGED)})["passed"]


def test_analyze_always_passes_and_no_queries_is_reported():
    assert judge("analyze", None, {}, {})["passed"]
    r = judge("create_index", "hc_test.ix", {}, {})
    assert not r["passed"] and any(x["rule"] == "affected_queries" for x in r["rules"])


class _Cur:
    def __init__(self, row=None):
        self.row = row

    def fetchone(self):
        return self.row


class _Conn:
    def __init__(self, fail_on=None):
        self.log, self.fail_on = [], fail_on

    def execute(self, stmt):
        self.log.append(stmt)
        if self.fail_on and self.fail_on in stmt:
            raise RuntimeError("boom")
        if stmt.startswith("EXPLAIN (ANALYZE"):
            return _Cur([[{"Execution Time": 2.0, "Plan": {"Node Type": "Nested Loop", "Plans": [
                {"Node Type": "Index Scan", "Index Name": "ix"}]}}]])
        return _Cur()


def test_measure_runs_each_sample_in_rolled_back_txn_with_literal_values():
    conn = _Conn()
    from psycopg import sql as pgsql
    orig = pgsql.Literal.as_string
    pgsql.Literal.as_string = lambda self, c: "'" + str(self._obj).replace("'", "''") + "'"
    try:
        r = measure(conn, "select * from t where a = $1", [["it's"], ["x"]], repeats=2)
    finally:
        pgsql.Literal.as_string = orig
    assert r == {"ms": 2.0, "nodes": ["Index Scan", "Nested Loop"], "indexes": ["ix"]}
    assert conn.log.count("BEGIN") == 2 and conn.log.count("ROLLBACK") == 2 and conn.log.count("DEALLOCATE ALL") == 2
    assert any("EXECUTE lbh_q('it''s')" in s for s in conn.log)
    assert conn.log[-2:] == ["ROLLBACK", "DEALLOCATE ALL"]


def test_measure_returns_error_type_and_still_cleans_up():
    conn = _Conn(fail_on="PREPARE")
    r = measure(conn, "select 1", [[]])
    assert r == {"ms": None, "nodes": [], "indexes": [], "error": "RuntimeError"}
    assert conn.log[-2:] == ["ROLLBACK", "DEALLOCATE ALL"]


def test_drop_index_fails_when_a_write_regresses():
    r = judge("drop_index", "hc_test.dup", {"3": Q(10, write=True)}, {"3": Q(12, write=True, nodes=CHANGED)})
    assert not r["passed"]


def test_judge_fails_on_query_error():
    err = {"ms": None, "nodes": [], "indexes": [], "error": "UndefinedTable", "is_write": False}
    r = judge("create_index", "hc_test.ix", {"1": Q(100), "2": Q(10)}, {"1": Q(20, ["hc_test.ix"]), "2": err})
    assert not r["passed"]
    assert {"rule": "query_error", "passed": False, "detail": "query 2 failed: UndefinedTable"} in r["rules"]


# --- ruling (round 10): verification noise ---

def _rule(r, name):
    return next(x for x in r["rules"] if x["rule"] == name)


def test_unchanged_plan_slowdown_is_a_noise_band_not_a_regression():
    """Live criterion 1: big scans that never touch the new index ran 10-30 % slower on the cold branch."""
    before = {"1": Q(71.6), "2": Q(27742.8, nodes=["Seq Scan"]), "3": Q(183.0, nodes=["Seq Scan"])}
    after = {"1": Q(0.05, ["hc_test.ix"], nodes=["Index Scan"]), "2": Q(31640.6, nodes=["Seq Scan"]),
             "3": Q(308.1, nodes=["Seq Scan"])}
    r = judge("create_index", "hc_test.ix", before, after)
    rule = _rule(r, "no_read_regression")
    assert rule["passed"] and r["passed"], r
    assert "noise band" in rule["detail"] and "27742.8" in rule["detail"] and "1.68x" in rule["detail"]


def test_sub_millisecond_swings_never_regress():
    """Live criterion 4: 0.0125 -> 0.033 ms is a 2.64x ratio but 0.02 ms."""
    r = judge("drop_index", "hc_test.dup", {"1": Q(0.0125, nodes=["Index Scan"])}, {"1": Q(0.033, nodes=CHANGED)})
    assert _rule(r, "no_read_regression")["passed"] and r["passed"]


def test_a_changed_plan_or_one_using_the_new_index_regresses_past_both_limits():
    changed = judge("drop_index", "hc_test.dup", {"1": Q(10, nodes=["Index Scan"])}, {"1": Q(11.5, nodes=CHANGED)})
    assert not _rule(changed, "no_read_regression")["passed"]
    small = judge("drop_index", "hc_test.dup", {"1": Q(5, nodes=["Index Scan"])}, {"1": Q(5.9, nodes=CHANGED)})
    assert _rule(small, "no_read_regression")["passed"]  # 1.18x but only +0.9 ms
    uses = judge("create_index", "hc_test.ix", {"1": Q(100), "2": Q(10, ["hc_test.ix"])},
                 {"1": Q(20, ["hc_test.ix"]), "2": Q(12, ["hc_test.ix"])})
    assert not _rule(uses, "no_read_regression")["passed"]  # same plan, but it uses the changed index
    edge = judge("drop_index", "hc_test.dup", {"1": Q(100, nodes=["A"])}, {"1": Q(110, nodes=CHANGED)})
    assert _rule(edge, "no_read_regression")["passed"]  # exactly 1.10x is not > 1.10


def test_write_probe_runs_three_alternating_rounds_and_takes_the_median():
    from lakebase_health.fixer.verify import measure_writes
    calls, times = [], {"w1": iter([5.0, 9.0, 6.0]), "w2": iter([1.0, 1.2, 1.1])}

    def fake(conn, q, params, repeats=3):
        calls.append((q, repeats))
        return {"ms": next(times[q]), "nodes": ["Insert"], "indexes": []}
    out = measure_writes(fake, None, [{"queryid": "a", "query": "w1", "params": [[]]},
                                       {"queryid": "b", "query": "w2", "params": [[]]}])
    assert [q for q, _ in calls] == ["w1", "w2", "w1", "w2", "w1", "w2"] and {r for _, r in calls} == {1}
    assert out["a"]["ms"] == 6.0 and out["a"]["samples_ms"] == [5.0, 9.0, 6.0] and out["b"]["ms"] == 1.1
    err = measure_writes(lambda *a, **k: {"ms": None, "nodes": [], "indexes": [], "error": "X"}, None,
                         [{"queryid": "a", "query": "w", "params": [[]]}])
    assert err["a"]["error"] == "X" and err["a"]["ms"] is None


# --- ruling (round 11): which writes the write rule judges ---

def W(ms, query, samples=None):
    return {"ms": ms, "nodes": [], "indexes": [], "is_write": True, "query": query, "samples_ms": samples or [ms] * 3}


READS_OK = ({"r": Q(100)}, {"r": Q(1, ["hc_test.ix"])})


def _judge_writes(before_w, after_w, cols=("customer_id",)):
    b, a = dict(READS_OK[0]), dict(READS_OK[1])
    b.update(before_w)
    a.update(after_w)
    return judge("create_index", "hc_test.ix", b, a, index_columns=list(cols))


def test_write_rule_judges_inserts_and_updates_of_indexed_columns_only():
    """Live criterion 1: DELETE 2.10x (table churn: DELETE maintains no secondary index), UPDATE status 1.21x (the new
    index is on customer_id), bulk INSERT 1.15x, single-row INSERT 0.04 ms 1.51x."""
    dele = "DELETE FROM orders WHERE id % 10 = 0"
    upd = "UPDATE orders SET status = 'refunded' WHERE id % 3 = 0"
    ins = "INSERT INTO orders (customer_id, status, amount, note, created) SELECT 1 + (g % 200000) FROM generate_series(1, 20000) g"
    one = "INSERT INTO hc_test.orders (customer_id, status, amount) VALUES ($1, 'new', 9.99)"
    r = _judge_writes({"d": W(182.408, dele), "u": W(8419.6, upd), "i": W(27412.4, ins), "o": W(0.0385, one)},
                      {"d": W(382.269, dele), "u": W(10155.6, upd), "i": W(31449.3, ins), "o": W(0.058, one)})
    rule = _rule(r, "write_cost")
    assert rule["passed"] and r["passed"], r
    d = rule["detail"]
    assert "judged" in d and "i 27412.4->31449.3 ms (1.15x)" in d
    assert "not judged" in d and "DELETE" in d and "does not change an indexed column" in d and "under 1 ms" in d


def test_write_rule_fails_on_a_judged_write_over_the_limit():
    ins = "INSERT INTO orders (customer_id) SELECT g FROM generate_series(1, 20000) g"
    upd = "UPDATE orders SET customer_id = customer_id + 1 WHERE id < 100"
    r = _judge_writes({"i": W(100.0, ins), "u": W(50.0, upd)}, {"i": W(120.0, ins), "u": W(70.0, upd)})
    rule = _rule(r, "write_cost")
    assert not rule["passed"] and "u 50->70 ms (1.40x)" in rule["detail"]


def test_update_set_columns():
    from lakebase_health.fixer.verify import update_set_columns
    assert update_set_columns("UPDATE orders SET status = 'x', amount = amount + 1 WHERE id = 1") == {"status", "amount"}
    assert update_set_columns('update hc_test.orders o set "Customer_Id" = 3, (a, b) = (1, 2) from t where x') == \
        {"Customer_Id", "a", "b"}
    assert update_set_columns("DELETE FROM t") == set()


def test_no_judged_writes_passes_and_says_why():
    dele = "DELETE FROM orders WHERE id % 10 = 0"
    rule = _rule(_judge_writes({"d": W(10.0, dele)}, {"d": W(30.0, dele)}), "write_cost")
    assert rule["passed"] and "no judged write" in rule["detail"]
