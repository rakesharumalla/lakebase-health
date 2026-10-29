import json

REQUEST_FROM = {
    "verify": {"proposed", "verify_failed"},
    "apply": {"verified", "apply_failed"},
    "rollback": {"regressed", "applied", "confirmed", "rollback_failed"},
    "reject": {"proposed", "verified", "verify_failed", "apply_failed"},
}
START = {"verify": "verifying", "apply": "applying", "rollback": "rolling_back", "reject": "rejected"}
TERMINAL = {"rejected", "expired", "rolled_back", "confirmed", "blocked"}


# Why a run ended blocked (action_runs.report blocked_reason). Only a privilege/enrolment block can be retried.
PRIVILEGES, NOT_ALLOWED, NO_DATABASE = "privileges", "not_allowed", "no_database"
RETRYABLE_AFTER_BLOCK = {"verify", "apply"}
_LEGACY_PRIVILEGE_ERROR = "fixer not enrolled or missing privileges"


def block_retry(kind, report, error):
    """The request kind a blocked action may retry (the one that was blocked), or None if the block is final.
    `kind`, `report` (JSON text) and `error` come from the action's latest blocked run."""
    try:
        reason = (json.loads(report) if report else {}).get("blocked_reason")
    except (ValueError, AttributeError):
        reason = None
    if reason is None and (error or "").startswith(_LEGACY_PRIVILEGE_ERROR):
        reason = PRIVILEGES  # runs recorded before blocked_reason existed
    return kind if reason == PRIVILEGES and kind in RETRYABLE_AFTER_BLOCK else None


def can_request(request: str, state: str, verify_on_branch: bool, retry: str | None = None) -> bool:
    if state == "blocked":
        return retry in RETRYABLE_AFTER_BLOCK and request == retry
    if request == "apply" and state == "proposed" and not verify_on_branch:
        return True
    return state in REQUEST_FROM.get(request, set())
