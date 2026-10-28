-- The SaaS database: an application role, the demo schema, two organizations.
CREATE ROLE app LOGIN PASSWORD 'app';
CREATE DATABASE demo OWNER app;
\c demo
CREATE EXTENSION postgres_fdw;
CREATE EXTENSION pg_stat_statements;
CREATE TABLE organizations (id bigint PRIMARY KEY, name text NOT NULL);
CREATE TABLE documents (
  id bigserial PRIMARY KEY,
  organization_id bigint NOT NULL REFERENCES organizations,
  title text NOT NULL,
  body text NOT NULL DEFAULT ''
);
CREATE INDEX ON documents (organization_id, id);
CREATE TABLE document_versions (
  id bigserial PRIMARY KEY,
  document_id bigint NOT NULL REFERENCES documents ON DELETE CASCADE,
  content text NOT NULL
);
INSERT INTO organizations VALUES (1, 'Acme (moves to its own database)'), (2, 'Globex (stays in the cloud)');
INSERT INTO documents (organization_id, title, body)
  SELECT 1 + g % 2, 'seed ' || g, repeat('x', 200) FROM generate_series(1, 2000) g;
INSERT INTO document_versions (document_id, content) SELECT id, 'v1' FROM documents;
-- seeded as postgres (pg_stat_statements then keeps these one-off statements out of the report),
-- then handed to the application's role
ALTER TABLE organizations OWNER TO app;
ALTER TABLE documents OWNER TO app;
ALTER TABLE document_versions OWNER TO app;
ALTER SEQUENCE documents_id_seq OWNER TO app;
ALTER SEQUENCE document_versions_id_seq OWNER TO app;

\c postgres
-- the relay's registry: enrolment tokens, issued certificates, organization endpoints
CREATE DATABASE controlplane;
