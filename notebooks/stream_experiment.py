# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming-ingest experiment: `system.lakebase.compute_gauges` → Delta
# MAGIC Measures whether a Structured Streaming read avoids the ~4 min file-listing cost that a batch
# MAGIC `INSERT … WHERE ts > max(ts)` pays on every incremental run. Runs 3 `availableNow` batches a few minutes apart
# MAGIC and records wall time plus Spark's per-phase durations. Writes to a separate test table.

# COMMAND ----------

dbutils.widgets.text("source", "system.lakebase.compute_gauges")
dbutils.widgets.text("target", "my_catalog.lakebase_health.raw_compute_gauges_stream")
dbutils.widgets.text("checkpoint_volume", "my_catalog.lakebase_health.checkpoints")
dbutils.widgets.text("start_minutes_ago", "30")
dbutils.widgets.text("batches", "3")
dbutils.widgets.text("pause_s", "300")

import json
import time
from datetime import datetime, timedelta, timezone

SOURCE = dbutils.widgets.get("source")
TARGET = dbutils.widgets.get("target")
VOL = dbutils.widgets.get("checkpoint_volume")
cat, sch, vol = VOL.split(".")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {VOL}")
CKPT = f"/Volumes/{cat}/{sch}/{vol}/{TARGET.split('.')[-1]}"
dbutils.fs.rm(CKPT, True)                      # fresh experiment every run
spark.sql(f"DROP TABLE IF EXISTS {TARGET}")
START = (datetime.now(timezone.utc) - timedelta(minutes=int(dbutils.widgets.get("start_minutes_ago")))).strftime("%Y-%m-%d %H:%M:%S")
print("source", SOURCE, "| target", TARGET, "| checkpoint", CKPT, "| startingTimestamp", START)

# COMMAND ----------

results = []
for i in range(int(dbutils.widgets.get("batches"))):
    t = time.time()
    q = (spark.readStream
         .option("startingTimestamp", START)   # only used on the first batch (no checkpoint yet)
         .table(SOURCE)
         .writeStream
         .option("checkpointLocation", CKPT)
         .trigger(availableNow=True)
         .toTable(TARGET))
    q.awaitTermination()
    wall = time.time() - t
    progress = q.recentProgress
    rows = sum(p.get("numInputRows", 0) for p in progress)
    durations = {}
    for p in progress:
        for k, v in (p.get("durationMs") or {}).items():
            durations[k] = durations.get(k, 0) + v
    r = {"batch": i + 1, "wall_s": round(wall, 1), "input_rows": rows, "micro_batches": len(progress),
         "duration_ms_by_phase": durations}
    results.append(r)
    print(json.dumps(r))
    if i < int(dbutils.widgets.get("batches")) - 1:
        time.sleep(int(dbutils.widgets.get("pause_s")))

# COMMAND ----------

print(json.dumps(results, indent=2))
display(spark.sql(f"SELECT count(*) AS rows, count(DISTINCT project_id) AS projects, min(ts), max(ts) FROM {TARGET}"))
dbutils.notebook.exit(json.dumps(results))
