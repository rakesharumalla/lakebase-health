from lakebase_health.fixer.states import can_request


def test_legal_requests():
    assert can_request("verify", "proposed", True)
    assert can_request("verify", "verify_failed", True)
    assert can_request("apply", "verified", True)
    assert can_request("apply", "proposed", False)            # not branch-verifiable
    assert not can_request("apply", "proposed", True)         # must verify first
    assert can_request("apply", "apply_failed", True)
    assert can_request("rollback", "regressed", True) and can_request("rollback", "applied", True)
    assert can_request("reject", "verified", True)
    for bad in [("verify", "applied"), ("apply", "applying"), ("rollback", "proposed"), ("reject", "applied")]:
        assert not can_request(bad[0], bad[1], True), bad


def test_privilege_block_accepts_a_retry_of_the_blocked_request_only():
    from lakebase_health.fixer.states import can_request
    assert can_request("verify", "blocked", True, retry="verify")
    assert can_request("apply", "blocked", True, retry="apply")
    assert not can_request("apply", "blocked", True, retry="verify")  # never skips verification
    assert not can_request("verify", "blocked", True, retry=None)  # allowlist/other blocks stay terminal
    assert not can_request("rollback", "blocked", True, retry="rollback")


def test_block_retry_tells_privilege_blocks_from_allowlist_blocks():
    from lakebase_health.fixer.states import block_retry
    assert block_retry("verify", '{"blocked_reason": "privileges"}', None) == "verify"
    assert block_retry("apply", '{"blocked_reason": "not_allowed"}', "Not allowed") is None
    # rows written before the reason was recorded
    assert block_retry("verify", '{"approval_id": "x"}',
                       "fixer not enrolled or missing privileges on this project: 42501") == "verify"
    assert block_retry("verify", None, "DROP TABLE is not allowed") is None
    assert block_retry(None, None, None) is None
