#!/usr/bin/env python3
"""Generate app/server/report/checks.json (check id -> area, source, title, thresholds) from the check YAMLs.

The app's report builder scores areas from this file; the engine's findings carry no area (the spec keeps it on
the check). Usage (needs PyYAML, for example the engine venv):
    python scripts/gen_check_areas.py           # rewrite the file
    python scripts/gen_check_areas.py --check   # exit 1 if it is out of date
"""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "engine" / "src"))

from lakebase_health.checkspec import load_checks  # noqa: E402
from lakebase_health.executors.api import API_CHECKS  # noqa: E402
from lakebase_health.jobs.run import CHECKS_DIR  # noqa: E402

OUT = ROOT / "app" / "server" / "report" / "checks.json"


def build() -> str:
    specs = sorted(load_checks(CHECKS_DIR, frozenset(API_CHECKS)), key=lambda s: s.id)
    data = {s.id: {"area": s.area, "source": s.source, "title": s.title, "thresholds": s.thresholds,
                   "direction": s.direction} for s in specs}
    return json.dumps(data, indent=2, sort_keys=True) + "\n"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check", action="store_true")
    a = ap.parse_args()
    text = build()
    if a.check:
        if not OUT.exists() or OUT.read_text() != text:
            print(f"{OUT} is out of date: run python scripts/gen_check_areas.py", file=sys.stderr)
            sys.exit(1)
        return
    OUT.write_text(text)
    print(f"wrote {OUT}: {len(json.loads(text))} checks")


if __name__ == "__main__":
    main()
