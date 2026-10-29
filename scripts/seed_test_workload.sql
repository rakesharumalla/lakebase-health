-- Seeds a test database with deliberate anti-patterns so every health check has something to find.
-- Safe to re-run: drops and recreates the test tables (not the schema, which may hold platform-owned extensions).
CREATE SCHEMA IF NOT EXISTS hc_test;
DROP TABLE IF EXISTS hc_test.orders, hc_test.customers, hc_test.audit_events, hc_test.kv_store CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements SCHEMA public;
CREATE EXTENSION IF NOT EXISTS pgstattuple SCHEMA public;
CREATE EXTENSION IF NOT EXISTS pg_hint_plan;
CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public;
SET search_path = hc_test;

-- Parent / child with an UNINDEXED foreign key (check 20)
CREATE TABLE customers (
  id      bigserial PRIMARY KEY,
  email   text NOT NULL,
  region  text NOT NULL,
  created timestamptz NOT NULL DEFAULT now()
);
-- int4 serial close to exhaustion (check 26)
CREATE TABLE orders (
  id          serial PRIMARY KEY,
  customer_id bigint NOT NULL REFERENCES customers(id),
  status      text NOT NULL,
  amount      numeric(12,2) NOT NULL,
  note        text,
  created     timestamptz NOT NULL DEFAULT now()
);
SELECT setval('orders_id_seq', 1600000000);

-- Duplicate (19b), left-prefix redundant (19c), unused (19a) indexes
CREATE INDEX customers_email_idx   ON customers (email);
CREATE INDEX customers_email_idx2  ON customers (email);
CREATE INDEX customers_region_idx  ON customers (region);
CREATE INDEX customers_region_created_idx ON customers (region, created);
CREATE INDEX orders_note_idx ON orders (note);

-- Table without a primary key (check 29)
CREATE TABLE audit_events (event_ts timestamptz DEFAULT now(), payload text);

INSERT INTO customers (email, region, created)
SELECT 'user' || g || '@example.com', (ARRAY['us','eu','apac','latam'])[1 + g % 4], now() - (g || ' minutes')::interval
FROM generate_series(1, 200000) g;

INSERT INTO orders (customer_id, status, amount, note, created)
SELECT 1 + (g % 200000), (ARRAY['new','paid','shipped','cancelled'])[1 + g % 4],
       round((random() * 500)::numeric, 2), md5(g::text), now() - (g || ' seconds')::interval
FROM generate_series(1, 1000000) g;

INSERT INTO audit_events (payload) SELECT md5(g::text) FROM generate_series(1, 10000) g;

-- Invalid index (19d): a unique index build that fails on duplicates leaves an INVALID index behind
-- (run separately because CREATE INDEX CONCURRENTLY can't run inside a transaction block)

-- Dead tuples / bloat (checks 22, 23): churn a large share of orders, with autovacuum off on that table
ALTER TABLE orders SET (autovacuum_enabled = false);
UPDATE orders SET status = 'refunded' WHERE id % 3 = 0;
DELETE FROM orders WHERE id % 10 = 0;

-- LLM query rewrites: scripts/workload.py PLANTED_QUERY is a correlated subquery on orders (average amount per
-- status over an id range, recomputed for every outer row). It needs only the orders primary key, so nothing is
-- planted here; run `workload.py --mode rewrite` to make it the top query.

-- PUBLIC CREATE on a schema (check 29)
GRANT CREATE ON SCHEMA hc_test TO PUBLIC;

ANALYZE customers;
ANALYZE audit_events;

-- GIN fast-update pending list (gin_pending_list, gin_cleanup_lock_wait): a key-value store shaped like the
-- customer's (prefix, key) primary key and a trigram index over the whole value. Autovacuum is off so the pending
-- list is only merged by writers. kv_title_trgm gets a 64 MB limit and is filled to roughly half by the seed;
-- workload writes omit "title", so only kv_value_trgm (4 MB limit) is merged inline during the workload.
CREATE TABLE kv_store (prefix text NOT NULL, key text NOT NULL, value jsonb NOT NULL, PRIMARY KEY (prefix, key))
  WITH (autovacuum_enabled = false);
CREATE INDEX kv_value_trgm ON kv_store USING gin ((value::text) public.gin_trgm_ops) WITH (fastupdate = on);
CREATE INDEX kv_title_trgm ON kv_store USING gin ((value->>'title') public.gin_trgm_ops)
  WITH (fastupdate = on, gin_pending_list_limit = 65536);
INSERT INTO kv_store
SELECT 'user' || (g % 50), 'seed' || g,
       jsonb_build_object('title', (SELECT string_agg(md5((g * 31 + i)::text), ' ') FROM generate_series(1, 25) i))
FROM generate_series(1, 1200) g;
