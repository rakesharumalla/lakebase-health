import math
import operator

from .checkspec import GUARD_RE

SEVERITY_RANK = {"info": 0, "warning": 1, "critical": 2}
_OPS = {">=": operator.ge, "<=": operator.le, ">": operator.gt, "<": operator.lt, "==": operator.eq}


def classify(measure, thresholds: dict, direction: str = "above") -> str | None:
    if measure is None:
        return None
    try:
        m = float(measure)
    except (TypeError, ValueError):
        return None
    if math.isnan(m):
        return None
    for sev in ("critical", "warning", "info"):
        t = thresholds.get(sev)
        if t is None:
            continue
        if (direction == "above" and m > t) or (direction == "below" and m < t):
            return sev
    return None


def guards_pass(guards: tuple, ctx: dict) -> bool:
    for g in guards:
        name, op, value = GUARD_RE.match(g).groups()
        if ctx.get(name) is None or not _OPS[op](float(ctx[name]), float(value)):
            return False
    return True
