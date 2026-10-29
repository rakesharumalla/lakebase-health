"""Draft states, the checks whose findings can be rewritten, and the attempt token an approval covers."""
import hashlib
import json
import re
from datetime import timedelta

REWRITE_CHECKS = ("top_db_time_query", "query_regression", "plan_flip", "storage_wait", "disk_spill",
                  "sort_limit_waste", "row_misestimate")
RUNNING = ("drafting", "verifying")
FINAL = ("draft_failed", "verified", "verify_failed", "adopted", "not_adopted")
ADOPTION_WINDOW = timedelta(days=7)  # a verified rewrite not seen in the workload by then is not_adopted
_QUERYID = re.compile(r"-?\d{1,20}")


def draft_token(fingerprint: str, attempt: int) -> str:
    """What a draft approval's sql_hash holds: the attempt it asks for (app/server/lib/drafts.ts draftToken)."""
    return hashlib.sha256(f"draft:{fingerprint}:{attempt}".encode()).hexdigest()


def finding_queryid(check_id, evidence, obj) -> str | None:
    """The query a rewrite is drafted for: evidence.queryid (row_misestimate: its top query), else the object."""
    if check_id not in REWRITE_CHECKS:
        return None
    try:
        ev = json.loads(evidence) if isinstance(evidence, str) else (evidence or {})
    except ValueError:
        ev = {}
    ev = ev if isinstance(ev, dict) else {}
    if check_id == "row_misestimate":
        top = ev.get("top_queries") if isinstance(ev.get("top_queries"), list) else []
        q = top[0].get("queryid") if top and isinstance(top[0], dict) else None
    else:
        q = ev.get("queryid", str(obj or "").split("@")[0])
    q = "" if q is None else str(q)
    return q if _QUERYID.fullmatch(q) else None
