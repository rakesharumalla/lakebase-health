import pytest

from lakebase_health.fixer.draft.sqltext import (SqlTextError, denormalize, has_comment, literals, normalize, params,
                                                  strip_literals, text_key)

Q = "SELECT * FROM hc_test.orders o WHERE o.status = 'paid' AND o.id BETWEEN $1 AND $2 AND o.amount > 10.5 LIMIT 10"


def test_parameters_and_literals_are_told_apart():
    assert params(Q) == {1, 2} and params("SELECT $10, $2") == {10, 2}
    assert literals(Q) == ["'paid'", "10.5", "10"]
    assert literals("SELECT x FROM t WHERE a = $1") == []


def test_normalize_turns_every_literal_into_a_new_parameter_and_back():
    text, values = normalize(Q)
    assert text == ("SELECT * FROM hc_test.orders o WHERE o.status = $3 AND o.id BETWEEN $1 AND $2 "
                    "AND o.amount > $4 LIMIT $5")
    assert values == {3: "'paid'", 4: "10.5", 5: "10"}
    assert literals(text) == [] and denormalize(text, values) == Q


def test_every_literal_form_is_normalized():
    text, values = normalize("SELECT E'a\\'b', $$q$$, x'1F', B'101', interval '1 day', -5, 1e3")
    assert text == "SELECT $1, $2, $3, $4, interval $5, -$6, $7"
    assert len(values) == 7


def test_denormalize_only_replaces_literal_placeholders():
    assert denormalize("SELECT $1 WHERE s = $3 OR s = $3", {3: "'x'"}) == "SELECT $1 WHERE s = 'x' OR s = 'x'"


def test_strip_literals_for_plans_and_catalog_text():
    assert strip_literals("((status = 'paid'::text) AND (id >= $1) AND (amount > 10.5))") == \
        "((status = ?::text) AND (id >= $1) AND (amount > ?))"
    assert strip_literals("CREATE INDEX x ON s.t USING btree (a) WHERE (s = 'x'::text)") == \
        "CREATE INDEX x ON s.t USING btree (a) WHERE (s = ?::text)"
    assert strip_literals("'unterminated") == "?"


def test_comments_are_found_outside_strings_only():
    assert has_comment("SELECT 1 /*+ SeqScan(t) */") and has_comment("SELECT 1 -- x\n")
    assert not has_comment("SELECT '/* not a comment */'")


def test_text_key_ignores_literal_values_whitespace_and_case():
    assert text_key("SELECT a FROM t WHERE x = 5") == text_key("select  a from t\nwhere x = $1")
    assert text_key("SELECT a FROM t WHERE x = 5") != text_key("SELECT b FROM t WHERE x = 5")


def test_untokenizable_text_raises():
    with pytest.raises(SqlTextError):
        params("SELECT 'unterminated")


def test_a_tokenizer_error_names_no_sql():
    """Final review I-2: sqlglot's TokenError message quotes the text around the error, literals included."""
    with pytest.raises(SqlTextError) as e:
        normalize("SELECT a FROM t WHERE ssn = '123-45-6789' AND x = $$unterminated")
    assert str(e.value) == "cannot tokenize (TokenError)"


def test_space_params_lets_the_tokenizer_read_adjacent_placeholders():
    """pg_stat_statements keeps the application's spacing: `IN ($1,$2)` and `$1||$2` break sqlglot's tokenizer,
    which reads `$1,$` as a dollar-quote tag."""
    from lakebase_health.fixer.draft.sqltext import space_params
    q = "SELECT a FROM t WHERE b IN ($1,$2) AND c = $3||$4 AND d = f($5)"
    spaced = space_params(q)
    assert spaced == "SELECT a FROM t WHERE b IN ($1 ,$2 ) AND c = $3 ||$4 AND d = f($5 )"
    assert params(spaced) == {1, 2, 3, 4, 5}
    with pytest.raises(SqlTextError):
        params(q)
    # strings, quoted identifiers, dollar quotes and comments are left alone; already-spaced text is unchanged
    s = "SELECT '$1,$2', \"$1,x\", $tag$ $1,$2 $tag$, $1 -- $2,$3\nFROM t"
    assert space_params(s) == s
    assert space_params("SELECT $1 FROM t WHERE a = $2") == "SELECT $1 FROM t WHERE a = $2" and space_params("SELECT $10") == "SELECT $10"
