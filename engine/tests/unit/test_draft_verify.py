from types import SimpleNamespace

from lakebase_health.fixer.draft.verify import checksum, judge, mentioned, plan_shape

B = {"nodes": ["Aggregate", "Index Scan", "SubPlan"]}
A = {"nodes": ["Hash Join", "HashAggregate", "Index Scan"]}


def s(i, before_ms, after_ms, original=(10, "c1"), rewrite=(10, "c1")):
    return {"set": i, "original": {"rows": original[0], "checksum": original[1]},
            "rewrite": {"rows": rewrite[0], "checksum": rewrite[1]}, "original_ms": before_ms, "rewrite_ms": after_ms}


def rule(r, name):
    return next(x for x in r["rules"] if x["rule"] == name)


def test_verified_when_equivalent_faster_and_the_plan_changed_as_described():
    r = judge([s(1, 500, 2), s(2, 480, 2.5), s(3, 0.5, 1.2)], True, B, A,
              "The SubPlan becomes a Hash Join over a HashAggregate")
    assert r["passed"], r
    assert rule(r, "equivalence")["detail"] == "3 of 3 parameter sets: same rows in order"
    assert rule(r, "speed")["detail"].startswith("median 480 -> 2 ms, 240.00x faster")
    assert r["plan"]["removed"] == ["Aggregate", "SubPlan"] and r["plan"]["added"] == ["Hash Join", "HashAggregate"]
    assert rule(r, "plan")["warning"] is False


def test_equivalence_is_order_sensitive_only_for_ordered_queries():
    r = judge([s(1, 10, 5, rewrite=(9, "x")), s(2, 10, 5, rewrite=(10, "x"))], True, B, A, "Hash Join")
    assert not r["passed"]
    assert rule(r, "equivalence")["detail"] == "set 1: 10 rows vs 9; set 2: same row count, different rows or order"
    unordered = judge([s(1, 10, 5)], False, B, A, "Hash Join")
    assert rule(unordered, "equivalence")["detail"] == "1 of 1 parameter sets: same rows in any order"


def test_a_failing_query_fails_equivalence():
    bad = s(1, 10, None)
    bad["rewrite"] = {"rows": None, "checksum": None, "error": "UndefinedColumn"}
    r = judge([bad], True, B, A, "Hash Join")
    assert rule(r, "equivalence")["detail"] == "set 1: rewrite failed (UndefinedColumn)"
    assert not rule(r, "speed")["passed"]


def test_speed_needs_1_2x_on_the_median_and_no_set_slower_past_the_noise_floor():
    assert not rule(judge([s(1, 10, 8.4)], True, B, A, ""), "speed")["passed"]  # 1.19x
    assert rule(judge([s(1, 10, 8.3)], True, B, A, ""), "speed")["passed"]
    slow = judge([s(1, 100, 10), s(2, 100, 10), s(3, 10, 12)], True, B, A, "")
    assert not rule(slow, "speed")["passed"] and "slower: set 3 10 -> 12 ms" in rule(slow, "speed")["detail"]
    noise = judge([s(1, 100, 10), s(2, 100, 10), s(3, 0.5, 1.2)], True, B, A, "")  # 2.4x but +0.7 ms
    assert rule(noise, "speed")["passed"]
    edge = judge([s(1, 100, 10), s(2, 100, 10), s(3, 20, 22)], True, B, A, "")  # exactly 1.10x is not > 1.10
    assert rule(edge, "speed")["passed"]


def test_an_unexpected_plan_change_is_a_warning_not_a_failure():
    r = judge([s(1, 10, 5)], True, B, B, "Hash Join")
    p = rule(r, "plan")
    assert r["passed"] and p["passed"] and p["warning"]
    assert p["detail"] == ("removed nothing; added nothing; expected Hash Join "
                           "(warning: the expected change is not in the new plan)")


def test_node_names_are_read_longest_first():
    assert mentioned("Hash Join instead of a Nested Loop, plus a Hash node") == ["Hash", "Hash Join", "Nested Loop"]
    assert mentioned("HashAggregate replaces the SubPlan") == ["HashAggregate", "SubPlan"]
    assert mentioned(None) == []


class Conn:
    def __init__(self, row=None, plan=None, fail_on=None):
        self.log, self.row, self.plan, self.fail_on = [], row, plan, fail_on

    def execute(self, stmt):
        self.log.append(stmt)
        if self.fail_on and self.fail_on in stmt:
            raise RuntimeError("relation does not exist: 4111")
        if stmt.startswith("EXPLAIN"):
            return SimpleNamespace(fetchone=lambda: [[self.plan]])
        return SimpleNamespace(fetchone=lambda: self.row)


def literal(monkeypatch):
    from psycopg import sql as pgsql
    monkeypatch.setattr(pgsql.Literal, "as_string", lambda self, c: "'" + str(self._obj) + "'")


def test_checksum_runs_read_only_with_the_sampled_values_and_cleans_up(monkeypatch):
    literal(monkeypatch)
    conn = Conn(row=(1803, "ab12"))
    assert checksum(conn, "SELECT a FROM t WHERE x = $1 ORDER BY a", ["7"], True) == {"rows": 1803, "checksum": "ab12"}
    assert conn.log[0] == "BEGIN READ ONLY" and conn.log[-2:] == ["ROLLBACK", "DEALLOCATE ALL"]
    assert "string_agg(h, ',' ORDER BY n)" in conn.log[1] and "row_number() OVER ()" in conn.log[1]
    assert conn.log[2] == "EXECUTE lbh_eq('7')"
    unordered = Conn(row=(3, "123"))
    checksum(unordered, "SELECT a FROM t", [], False)
    assert "sum(('x' || substr(md5(t::text), 1, 15))::bit(60)::bigint)" in unordered.log[1]
    assert unordered.log[2] == "EXECUTE lbh_eq"


def test_checksum_errors_are_recorded_by_type_only(monkeypatch):
    literal(monkeypatch)
    conn = Conn(fail_on="EXECUTE")
    assert checksum(conn, "SELECT 1", [], True) == {"rows": None, "checksum": None, "error": "RuntimeError"}
    assert conn.log[-2:] == ["ROLLBACK", "DEALLOCATE ALL"]


def test_plan_shape_labels_nodes_and_reads_the_query_identifier(monkeypatch):
    literal(monkeypatch)
    plan = {"Query Identifier": -4711, "Plan": {"Node Type": "Index Scan", "Plans": [
        {"Node Type": "Aggregate", "Strategy": "Plain", "Parent Relationship": "SubPlan"},
        {"Node Type": "Aggregate", "Strategy": "Hashed", "Parent Relationship": "InitPlan"}]}}
    conn = Conn(plan=plan)
    assert plan_shape(conn, "SELECT 1", []) == {"nodes": ["Aggregate", "HashAggregate", "Index Scan", "InitPlan",
                                                          "SubPlan"], "queryid": "-4711"}
    assert conn.log[2] == "EXPLAIN (VERBOSE, FORMAT JSON) EXECUTE lbh_pl" and conn.log[0] == "BEGIN READ ONLY"


def test_sets_where_the_original_returns_no_rows_do_not_establish_equivalence():
    """Final review I-3: 0 = 0 on every set read as "same rows", and two empty runs then passed the speed rule."""
    r = judge([s(1, 500, 2, (0, "e"), (0, "e")), s(2, 480, 2, (0, "e"), (0, "e"))], True, B, A, "Hash Join")
    assert not r["passed"]
    eq = rule(r, "equivalence")
    assert not eq["passed"] and eq["detail"].startswith("no_informative_samples")
    one = judge([s(1, 500, 2, (0, "e"), (0, "e")), s(2, 480, 2)], True, B, A, "Hash Join")
    assert one["passed"] and rule(one, "equivalence")["detail"] == "2 of 2 parameter sets: same rows in order"


def test_checksum_can_run_under_a_statement_timeout(monkeypatch):
    literal(monkeypatch)
    conn = Conn(row=(1, "a"))
    checksum(conn, "SELECT a FROM t", [], False, timeout_ms=5000)
    assert conn.log[:2] == ["BEGIN READ ONLY", "SET LOCAL statement_timeout = '5000ms'"]
