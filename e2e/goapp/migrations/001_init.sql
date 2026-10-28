CREATE TABLE organizations (id bigserial PRIMARY KEY, name text NOT NULL);
CREATE TABLE projects (id bigserial PRIMARY KEY, organization_id bigint NOT NULL REFERENCES organizations (id), name text NOT NULL);
CREATE TABLE documents (
  id bigserial PRIMARY KEY,
  organization_id bigint NOT NULL REFERENCES organizations (id),
  project_id bigint NOT NULL REFERENCES projects (id),
  slug text NOT NULL,
  title text NOT NULL,
  body text NOT NULL DEFAULT '',
  UNIQUE (organization_id, slug)
);
CREATE INDEX documents_project_id_idx ON documents (project_id);
CREATE TABLE document_versions (id bigserial PRIMARY KEY, document_id bigint NOT NULL REFERENCES documents (id) ON DELETE CASCADE, content text NOT NULL);
CREATE TABLE comments (id bigserial PRIMARY KEY, document_id bigint NOT NULL REFERENCES documents (id) ON DELETE CASCADE, author text NOT NULL, body text NOT NULL);
CREATE TABLE shares (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), organization_id bigint NOT NULL REFERENCES organizations (id), document_id bigint NOT NULL REFERENCES documents (id) ON DELETE CASCADE, token text NOT NULL);
CREATE TABLE share_visits (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), share_id uuid NOT NULL REFERENCES shares (id) ON DELETE CASCADE, who text NOT NULL);
