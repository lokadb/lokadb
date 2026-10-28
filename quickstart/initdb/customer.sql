-- The organization's own database, prepared by its IT before the move.
-- lokadb_app: owns the moved tables; the cloud reads and writes through it (postgres_fdw) and
-- the proxy replays DDL with it. lokadb_dml: the proxy's direct path, DML only.
CREATE ROLE lokadb_app LOGIN PASSWORD 'lokadb_app';
CREATE ROLE lokadb_dml LOGIN PASSWORD 'lokadb_dml';
CREATE DATABASE cust_demo;
\c cust_demo
GRANT CREATE ON SCHEMA public TO lokadb_app;
