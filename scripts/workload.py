"""Generate runtime signals on the seeded hc_test schema so telemetry-based checks have data.

Usage: python3 scripts/workload.py --profile my-profile \
         --endpoint projects/healthcheck/branches/production/endpoints/primary --minutes 10 [--mode all|rewrite]
         [--rewrite-file rewrite.sql]

--mode rewrite runs only the planted correlated subquery (PLANTED_QUERY), so it dominates database time and gets a
top_db_time_query finding for the LLM-rewrite acceptance; with --rewrite-file it runs that SQL instead (the rewrite
copied from the app, with the same $1/$2), so the post-check can mark the rewrite adopted.
"""
import argparse
import hashlib
import os
import json
import random
import subprocess
import threading
import time

import psycopg


# Planted for LLM query rewrites: the subquery runs once per outer row over the same id range (about 2,000 rows each),
# so each call costs ~2,000^2 row visits; aggregating once per status is equivalent and far faster. Only the primary
# key index is used, so the fix loop's index changes on hc_test.orders do not affect it.
PLANTED_QUERY = (
    "SELECT o.id, o.status, o.amount FROM hc_test.orders o "
    "WHERE o.id BETWEEN $1 AND $2 "
    "AND o.amount > (SELECT avg(o2.amount) FROM hc_test.orders o2 "
    "WHERE o2.id BETWEEN $1 AND $2 AND o2.status = o.status) "
    "ORDER BY o.id")
ORDER_IDS = (1600000001, 1601000000)  # seed_test_workload.sql: setval 1600000000, then 1,000,000 orders
RANGE = 2000


def run_text(c, sql, values):
    """Sends the text verbatim with $n placeholders and untyped text parameters (as PREPARE infers them), so the
    statement's query id matches the one the fixer measured on the branch."""
    res = c.pgconn.exec_params(sql.encode(), [str(v).encode() for v in values])
    if res.status != psycopg.pq.ExecStatus.TUPLES_OK:
        raise RuntimeError(res.error_message.decode(errors="replace"))


def planted(dsn, stop, sql):
    with psycopg.connect(dsn, autocommit=True, application_name="hc-rewrite") as c:
        while not stop.is_set():
            lo = random.randint(ORDER_IDS[0], ORDER_IDS[1] - RANGE)
            run_text(c, sql, (lo, lo + RANGE))
            time.sleep(1)


def cli(*args):
    return json.loads(subprocess.check_output(["databricks", *args, "-o", "json"]))


def conninfo(profile, endpoint):
    host = cli("postgres", "get-endpoint", endpoint, "--profile", profile)["status"]["hosts"]["host"]
    token = cli("postgres", "generate-database-credential", endpoint, "--profile", profile)["token"]
    user = cli("current-user", "me", "--profile", profile)["userName"]
    return f"host={host} user={user} password={token} dbname=databricks_postgres sslmode=require"


def oltp(dsn, stop):
    """Point lookups, inserts and updates: the 'normal' workload."""
    with psycopg.connect(dsn, autocommit=True, application_name="hc-oltp") as c:
        while not stop.is_set():
            cid = random.randint(1, 200000)
            c.execute("SELECT * FROM hc_test.customers WHERE id = %s", (cid,)).fetchall()
            c.execute("SELECT count(*) FROM hc_test.orders WHERE customer_id = %s", (cid,)).fetchall()  # seq scan: no FK index
            c.execute("INSERT INTO hc_test.orders (customer_id, status, amount) VALUES (%s, 'new', 9.99)", (cid,))
            c.execute("UPDATE hc_test.orders SET status = 'paid' WHERE id = (SELECT max(id) FROM hc_test.orders)")


def analytics(dsn, stop):
    """Large sorts and hashes with a tiny work_mem, so they spill to disk."""
    with psycopg.connect(dsn, autocommit=True, application_name="hc-analytics") as c:
        c.execute("SET work_mem = '64kB'")
        while not stop.is_set():
            c.execute("SELECT * FROM hc_test.orders ORDER BY note LIMIT 10").fetchall()
            c.execute("SELECT customer_id, sum(amount) FROM hc_test.orders GROUP BY customer_id ORDER BY 2 DESC LIMIT 5").fetchall()
            c.execute("SELECT c.region, count(*) FROM hc_test.orders o JOIN hc_test.customers c ON c.id = o.customer_id GROUP BY 1").fetchall()
            time.sleep(1)


def deadlock(dsn):
    """Two sessions updating the same two rows in opposite order."""
    def tx(first, second, barrier):
        try:
            with psycopg.connect(dsn, application_name="hc-deadlock") as c:
                c.execute("UPDATE hc_test.customers SET region = region WHERE id = %s", (first,))
                barrier.wait()
                c.execute("UPDATE hc_test.customers SET region = region WHERE id = %s", (second,))
                c.commit()
        except psycopg.errors.DeadlockDetected:
            pass
    b = threading.Barrier(2)
    ts = [threading.Thread(target=tx, args=(1, 2, b)), threading.Thread(target=tx, args=(2, 1, b))]
    [t.start() for t in ts]
    [t.join() for t in ts]


def kv_writer(dsn, stop, wid):
    """Large inserts into the GIN-indexed store: when the pending list passes 4 MB the inserting session merges it."""
    with psycopg.connect(dsn, autocommit=True, application_name="hc-kv-writer") as c:
        n = 0
        while not stop.is_set():
            n += 1
            body = " ".join(hashlib.md5(f"{wid}-{n}-{i}".encode()).hexdigest() for i in range(60))
            c.execute("INSERT INTO hc_test.kv_store VALUES (%s, %s, jsonb_build_object('text', %s::text))",
                      (f"user{n % 50}", f"w{wid}-{n}-{time.time_ns()}", body))


def drop_connections(dsn, n):
    """Opens sessions and closes their sockets without a Terminate message, as a dying client or proxy would."""
    for _ in range(n):
        c = psycopg.connect(dsn, application_name="hc-dropped")
        c.execute("SELECT 1")  # leaves a transaction open, so the server logs an unexpected EOF
        os.close(c.pgconn.socket)
        try:
            c.close()
        except Exception:
            pass


def idle_in_tx_blocker(dsn, hold_s):
    """Opens a transaction, locks a row, goes idle; a second session blocks behind it."""
    holder = psycopg.connect(dsn, application_name="hc-idle-in-tx")
    holder.execute("UPDATE hc_test.customers SET email = email WHERE id = 42")

    def blocked():
        with psycopg.connect(dsn, autocommit=True, application_name="hc-blocked") as c:
            c.execute(f"SET statement_timeout = '{int(hold_s)}s'")  # SET can't take bind parameters
            try:
                c.execute("UPDATE hc_test.customers SET email = email WHERE id = 42")
            except psycopg.errors.QueryCanceled:
                pass
    t = threading.Thread(target=blocked)
    t.start()
    return holder, t


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--minutes", type=float, default=10)
    ap.add_argument("--mode", choices=("all", "rewrite"), default="all")
    ap.add_argument("--rewrite-file", help="run this SQL (same $1, $2) instead of PLANTED_QUERY in --mode rewrite")
    a = ap.parse_args()
    dsn = conninfo(a.profile, a.endpoint)
    stop = threading.Event()
    if a.mode == "rewrite":
        sql = open(a.rewrite_file).read().strip().rstrip(";") if a.rewrite_file else PLANTED_QUERY
        threads = [threading.Thread(target=planted, args=(dsn, stop, sql)) for _ in range(2)]
        [t.start() for t in threads]
        time.sleep(a.minutes * 60)
        stop.set()
        [t.join() for t in threads]
        print("rewrite workload done")
        return
    workers = [threading.Thread(target=oltp, args=(dsn, stop)) for _ in range(4)]
    workers.append(threading.Thread(target=analytics, args=(dsn, stop)))
    workers.append(threading.Thread(target=planted, args=(dsn, stop, PLANTED_QUERY)))
    workers += [threading.Thread(target=kv_writer, args=(dsn, stop, i)) for i in range(6)]
    [w.start() for w in workers]
    end = time.time() + a.minutes * 60
    holder = None
    n = 0
    while time.time() < end:
        n += 1
        if n % 3 == 1:
            deadlock(dsn)
            drop_connections(dsn, 5)
        if holder is None:
            holder, bt = idle_in_tx_blocker(dsn, hold_s=90)
            held_at = time.time()
        elif time.time() - held_at > 120:
            holder.rollback()
            holder.close()
            bt.join()
            holder = None
        time.sleep(20)
    stop.set()
    [w.join() for w in workers]
    if holder:
        holder.rollback()
        holder.close()
    print("workload done")


if __name__ == "__main__":
    main()
