#!/usr/bin/env bash
# Run psql against a Lakebase endpoint using a freshly generated OAuth token.
# Usage: PROFILE=my-profile EP=projects/<p>/branches/<b>/endpoints/<e> scripts/pg.sh [psql args...]
set -euo pipefail
: "${PROFILE:?set PROFILE}" "${EP:?set EP}"
DB="${DB:-databricks_postgres}"
HOST=$(databricks postgres get-endpoint "$EP" --profile "$PROFILE" -o json \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['status']['hosts']['host'])")
TOKEN=$(databricks postgres generate-database-credential "$EP" --profile "$PROFILE" -o json \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['token'])")
USER_NAME=$(databricks current-user me --profile "$PROFILE" -o json \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['userName'])")
PGPASSWORD="$TOKEN" exec psql "host=$HOST user=$USER_NAME dbname=$DB sslmode=require" -X -v ON_ERROR_STOP=0 "$@"
