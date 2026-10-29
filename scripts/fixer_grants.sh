#!/usr/bin/env bash
# Run from the repo root as the catalog owner, after deploy.
set -euo pipefail
: "${PROFILE:?}" "${CATALOG:?}" "${ENGINE_SP:?}" "${FIXER_SP:?}" "${APP_SP:?}" "${WAREHOUSE:?}"
uuid='^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
[[ "$ENGINE_SP" =~ $uuid ]] || { echo "ENGINE_SP is not a UUID" >&2; exit 2; }
[[ "$FIXER_SP" =~ $uuid ]] || { echo "FIXER_SP is not a UUID" >&2; exit 2; }
[[ "$APP_SP" =~ $uuid ]] || { echo "APP_SP is not a UUID" >&2; exit 2; }
[[ "$CATALOG" =~ ^[A-Za-z0-9_]+$ ]] || { echo "CATALOG must match [A-Za-z0-9_]+" >&2; exit 2; }
[[ "$WAREHOUSE" =~ ^[A-Za-z0-9]+$ ]] || { echo "WAREHOUSE must be alphanumeric" >&2; exit 2; }

q() {
  local body resp state sid
  body=$(python3 -c 'import json,sys;print(json.dumps({"warehouse_id":sys.argv[1],"statement":sys.argv[2],"wait_timeout":"50s"}))' "$WAREHOUSE" "$1")
  resp=$(databricks api post /api/2.0/sql/statements --profile "$PROFILE" --json "$body")
  for _ in $(seq 1 30); do
    state=$(printf '%s' "$resp" | python3 -c 'import json,sys;print(json.load(sys.stdin)["status"]["state"])')
    case "$state" in
      SUCCEEDED) return 0 ;;
      PENDING|RUNNING)
        sid=$(printf '%s' "$resp" | python3 -c 'import json,sys;print(json.load(sys.stdin)["statement_id"])')
        sleep 5
        resp=$(databricks api get "/api/2.0/sql/statements/$sid" --profile "$PROFILE") ;;
      *) break ;;
    esac
  done
  echo "FAILED: $1" >&2
  printf '%s' "$resp" | python3 -c 'import json,sys;print(json.load(sys.stdin).get("status",{}).get("error") or "state did not reach SUCCEEDED")' >&2
  exit 1
}

q "GRANT USE CATALOG ON CATALOG $CATALOG TO \`$FIXER_SP\`"
q "GRANT USE SCHEMA, SELECT ON SCHEMA $CATALOG.lakebase_health TO \`$FIXER_SP\`"
for t in proposed_actions action_runs ledger shadow_log; do
  q "GRANT MODIFY ON TABLE $CATALOG.lakebase_health.$t TO \`$FIXER_SP\`"
done
q "GRANT MODIFY ON TABLE $CATALOG.lakebase_health.approvals TO \`$APP_SP\`"
# The bundle target to grant for (the CLI's own default is dev). The fixer job is the one this target deployed:
# its ID from `bundle summary`, never a name match, which could pick another target's job in the same workspace.
TARGET="${DATABRICKS_BUNDLE_TARGET:-dev}"
VARS=(--var "catalog=$CATALOG" --var "engine_sp=$ENGINE_SP" --var "fixer_sp=$FIXER_SP" --var "warehouse_id=$WAREHOUSE")
JOB=$(databricks bundle summary -t "$TARGET" -o json --profile "$PROFILE" "${VARS[@]}" \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["resources"]["jobs"]["lakebase_health_fixer"]["id"])')
[[ "$JOB" =~ ^[0-9]+$ ]] || { echo "FAILED: no fixer job ID in bundle summary -t $TARGET (deploy first)" >&2; exit 1; }
databricks permissions update jobs "$JOB" --profile "$PROFILE" \
  --json "{\"access_control_list\":[{\"service_principal_name\":\"$APP_SP\",\"permission_level\":\"CAN_MANAGE_RUN\"}]}" >/dev/null
# The jobs run as the engine and fixer SPs, which must read the deployed wheel in the bundle root folder.
# Granted on the folder (not via bundle permissions, which would rewrite the app and job ACLs).
ROOT=$(databricks bundle validate -t "$TARGET" -o json --profile "$PROFILE" "${VARS[@]}" \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["workspace"]["root_path"])')
DIR_ID=$(databricks workspace get-status "$ROOT" --profile "$PROFILE" -o json | python3 -c 'import json,sys;print(json.load(sys.stdin)["object_id"])')
[[ "$DIR_ID" =~ ^[0-9]+$ ]] || { echo "FAILED: no object_id for $ROOT" >&2; exit 1; }
databricks permissions update directories "$DIR_ID" --profile "$PROFILE" \
  --json "{\"access_control_list\":[{\"service_principal_name\":\"$ENGINE_SP\",\"permission_level\":\"CAN_READ\"},{\"service_principal_name\":\"$FIXER_SP\",\"permission_level\":\"CAN_READ\"}]}" >/dev/null \
  || { echo "FAILED: folder grant on $ROOT" >&2; exit 1; }
echo "grants done (target $TARGET, job $JOB, folder $ROOT)"
