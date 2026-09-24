-- 000001_init_schema.down.sql
-- Reverse of the up migration. Drop in FK-safe order.

DROP TABLE IF EXISTS transfers;
DROP TABLE IF EXISTS entries;
DROP TABLE IF EXISTS accounts;
