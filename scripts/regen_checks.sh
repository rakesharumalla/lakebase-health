#!/bin/bash
# Regenerate every file derived from the check YAMLs (run from the repo root with the engine venv's python).
set -euo pipefail
PY=${PY:-engine/.venv/bin/python}
NB=notebooks/lakebase_health_check.py
ROLES=$($PY -c "import ast,re,sys; t=open('$NB').read(); print(','.join(ast.literal_eval(re.search(r'^SERVICE_ROLES = (.*)$', t, re.M).group(1))))")
WHEEL=$($PY -c "import ast,re; t=open('$NB').read(); print(ast.literal_eval(re.search(r'^ENGINE_WHEEL = (.*)$', t, re.M).group(1)) or '')")
$PY scripts/build_notebook.py --service-roles "$ROLES" --engine-wheel "$WHEEL"
$PY scripts/gen_checks_doc.py
$PY scripts/gen_check_areas.py
