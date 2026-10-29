import hashlib


def fingerprint(check_id: str, project_id: str | None, endpoint_id: str | None, obj: str | None) -> str:
    raw = "\x1f".join([check_id, project_id or "", endpoint_id or "", obj or ""])
    return hashlib.sha256(raw.encode()).hexdigest()[:32]
