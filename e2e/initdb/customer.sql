-- the role the cloud keeps for the organization's database: move-org -customer-app makes it
-- the owner of the moved tables (postgres_fdw, the proxy's DDL replay); nothing else
CREATE ROLE lokadb_app LOGIN PASSWORD 'lokadb_app';
-- the proxy's direct path (profile fdw+direct, customers_app): DML on the organization's
-- tables alone, which org-profile -direct-role grants; it owns and creates nothing
CREATE ROLE lokadb_dml LOGIN PASSWORD 'lokadb_dml';
CREATE DATABASE cust_laravel;
CREATE DATABASE cust_django;
CREATE DATABASE cust_goapp;
CREATE DATABASE cust_uuidapp;
-- databases an organization prepared with something of its own already in them
CREATE DATABASE cust_badapp;
CREATE DATABASE cust_guardapp;
-- an empty database: move-org creates the extensions, types and functions richapp needs
CREATE DATABASE cust_richapp;
-- The organizations of the stand allow a logical move: the role creates in public, where
-- logical replication writes (and the proxy replays CREATE INDEX as it). Without this grant
-- move-org puts the tables in schema lokadb and moves by triggers (defects M7).
\c cust_laravel
GRANT CREATE ON SCHEMA public TO lokadb_app;
\c cust_django
GRANT CREATE ON SCHEMA public TO lokadb_app;
\c cust_goapp
GRANT CREATE ON SCHEMA public TO lokadb_app;
\c cust_uuidapp
GRANT CREATE ON SCHEMA public TO lokadb_app;
\c cust_richapp
GRANT CREATE ON SCHEMA public TO lokadb_app;
\c cust_badapp
CREATE TABLE documents (id int PRIMARY KEY, note text);
\c cust_guardapp
CREATE TABLE documents (id int PRIMARY KEY, note text);
INSERT INTO documents VALUES (1, 'the organization''s own table');
-- a role that may connect but not create anything
CREATE ROLE weak LOGIN PASSWORD 'weak';
