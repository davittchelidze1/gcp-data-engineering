-- Day 6: snapshots, clones, time travel.

-- metadata-only copies: a snapshot is read-only, a clone is writable
CREATE SNAPSHOT TABLE day6.dim_snapshot
CLONE day6.dim_customer
OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 2 DAY));

CREATE TABLE day6.dim_clone CLONE day6.dim_customer;

-- time travel. Capture "now" as unix micros: a formatted timestamp string with no
-- zone was parsed in a different zone and landed in the future.
SELECT UNIX_MICROS(CURRENT_TIMESTAMP()) AS marker;   -- e.g. 1788687926544409

DELETE FROM day6.dim_customer WHERE customer_id = 2;

SELECT COUNT(*) AS rows_now  FROM day6.dim_customer;                                           -- 5
SELECT COUNT(*) AS rows_then FROM day6.dim_customer
  FOR SYSTEM_TIME AS OF TIMESTAMP_MICROS(1788687926544409);                                    -- 7

-- A statement can't read a table with time travel and write to that same table.
-- Restore from the snapshot (or via a staging table) instead:
INSERT INTO day6.dim_customer
SELECT * FROM day6.dim_snapshot WHERE customer_id = 2;
