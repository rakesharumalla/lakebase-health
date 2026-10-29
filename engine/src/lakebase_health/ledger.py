import hashlib
import json
import uuid
from datetime import timedelta


def _digest(prev_hash, event_id, action_id, event_type, actor, payload, ts) -> str:
    raw = "\x1f".join([prev_hash or "", event_id, action_id, event_type, actor, payload, ts.isoformat()])
    return hashlib.sha256(raw.encode()).hexdigest()


def chain(last_by_action: dict, events: list, now, actor: str = "engine") -> list:
    """Each event links to the previous event of the same action (engine and fixer both write)."""
    last = dict(last_by_action)
    rows = []
    for i, e in enumerate(events):
        ts = now + timedelta(microseconds=i)
        payload = json.dumps(e["payload"], sort_keys=True, default=str)
        event_id = uuid.uuid4().hex
        prev = last.get(e["action_id"])
        h = _digest(prev, event_id, e["action_id"], e["event_type"], actor, payload, ts)
        rows.append({"event_id": event_id, "action_id": e["action_id"], "event_type": e["event_type"], "actor": actor,
                     "payload": payload, "ts": ts, "prev_hash": prev, "hash": h})
        last[e["action_id"]] = h
    return rows


def verify(rows: list) -> bool:
    by_action: dict = {}
    for r in rows:
        by_action.setdefault(r["action_id"], []).append(r)
    for chain_rows in by_action.values():
        prev = chain_rows[0]["prev_hash"]
        for r in chain_rows:
            if r["prev_hash"] != prev:
                return False
            if _digest(r["prev_hash"], r["event_id"], r["action_id"], r["event_type"], r["actor"], r["payload"], r["ts"]) != r["hash"]:
                return False
            prev = r["hash"]
    return True


def last_hashes(sql, cfg, action_ids) -> dict:
    ids = ",".join(f"'{a}'" for a in action_ids if a and all(c in "0123456789abcdef" for c in a))
    if not ids:
        return {}
    rows = sql.query(f"SELECT action_id, max_by(hash, ts) AS hash FROM {cfg.table('ledger')} WHERE action_id IN ({ids}) GROUP BY action_id")
    return {r["action_id"]: r["hash"] for r in rows}
