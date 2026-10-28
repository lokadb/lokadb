package main

// Block 5 (moving an organization): the extensions the tables use that are packages of their
// own (pgvector, PostGIS) go with the organization: move-org installs them in its database at
// the cloud's version, and refuses before it changes anything when the server lacks the package
// or the role may not install them (proxy/internal/admin/extensions.go).

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "MV28", name: "pgvector: a vector column with hnsw and ivfflat indexes moves, the extension is installed at the cloud's version, move-back leaves it", run: checkPgvectorMoves},
		check{customerHost: "customergis", id: "MV29", name: "PostGIS: a geometry column with a gist index moves, the extension is installed at the cloud's version", run: checkPostgisMoves},
		check{id: "MV30", name: "the organization's server has no PostGIS package: check (before and after enable) and move-org refuse with the command, before anything changes", run: checkExtensionPackageMissing},
		check{id: "MV31", name: "the organization's role may not install earthdistance (not trusted): check (before and after enable) and move-org refuse with the command, before anything changes", run: checkExtensionNoRights},
	)
}

// extVersion: the version of extension name in the database at dsn ("" if not installed).
func extVersion(e *env, dsn, name string) string {
	v, err := one(e.ctx, dsn, "SELECT coalesce((SELECT extversion FROM pg_extension WHERE extname = '"+name+"'), '')")
	if err != nil {
		return "?"
	}
	return v
}

// MV28. Organization 1's documents have a vector(3) column with an hnsw and an ivfflat index.
// The organization's database (customer) has the pgvector package but not the extension.
func checkPgvectorMoves(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE EXTENSION vector",
		"ALTER TABLE documents ADD COLUMN embedding vector(3)",
		"UPDATE documents SET embedding = ('[' || id || ',' || (id % 7) || ',1]')::vector",
		"CREATE INDEX documents_emb_hnsw ON documents USING hnsw (embedding vector_l2_ops)",
		"CREATE INDEX documents_emb_ivf ON documents USING ivfflat (embedding vector_cosine_ops) WITH (lists = 2)"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	cloudV := extVersion(e, e.cloud(), "vector")
	if out, err := e.move(1, "-mode", "trigger"); err != nil {
		return false, "move-org failed: " + lastErr(out), nil
	}
	custV := extVersion(e, e.customer(), "vector")
	hnsw, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM pg_indexes WHERE indexdef LIKE '%USING hnsw%' AND indexdef LIKE '%vector_l2_ops%'")
	ivf, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM pg_indexes WHERE indexdef LIKE '%USING ivfflat%' AND indexdef LIKE '%vector_cosine_ops%'")
	near, err := one(e.ctx, e.cloud(), "SELECT id FROM documents WHERE organization_id = 1 ORDER BY embedding <-> '[2,2,1]' LIMIT 1")
	if err != nil {
		return false, "nearest neighbour through the cloud: " + oneLine(err.Error()), nil
	}
	rows, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE embedding IS NOT NULL")
	if out, err := e.lokadb(e.moveBackArgs(1)...); err != nil {
		return false, "move-back failed: " + lastErr(out), nil
	}
	after := extVersion(e, e.customer(), "vector")
	ok := cloudV != "" && custV == cloudV && hnsw == "1" && ivf == "1" && near != "" && rows == "100" && after == cloudV
	return ok, fmt.Sprintf("pgvector %s in the cloud, %s at the organization; hnsw indexes %s, ivfflat %s; nearest neighbour of [2,2,1]: document %s; embeddings at the organization: %s; after move-back the extension there: %s",
		cloudV, custV, hnsw, ivf, near, rows, after), nil
}

// MV29. Organization 1's documents have a geometry(Point,4326) column with a gist index.
func checkPostgisMoves(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE EXTENSION postgis",
		"ALTER TABLE documents ADD COLUMN geom geometry(Point, 4326)",
		"UPDATE documents SET geom = ST_SetSRID(ST_MakePoint(id, id % 10), 4326)",
		"CREATE INDEX documents_geom ON documents USING gist (geom)"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	cloudV := extVersion(e, e.cloud(), "postgis")
	if out, err := e.move(1, "-mode", "trigger"); err != nil {
		return false, "move-org failed: " + lastErr(out), nil
	}
	custV := extVersion(e, e.customer(), "postgis")
	gist, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM pg_indexes WHERE indexdef LIKE '%USING gist%' AND indexdef LIKE '%geom%'")
	near, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE ST_DWithin(geom, ST_SetSRID(ST_MakePoint(10, 0), 4326), 5)")
	txt, err := one(e.ctx, e.cloud(), "SELECT ST_AsText(geom) FROM documents WHERE organization_id = 1 AND id = 12")
	if err != nil {
		return false, "a geometry through the cloud: " + oneLine(err.Error()), nil
	}
	ok := cloudV != "" && custV == cloudV && gist == "1" && near != "0" && txt == "POINT(12 2)"
	return ok, fmt.Sprintf("PostGIS %s in the cloud, %s at the organization; gist indexes %s; points within 5 of (10,0) at the organization: %s; document 12 through the cloud: %s",
		cloudV, custV, gist, near, txt), nil
}

// extensionRefused is the part of MV30 and MV31 they share: the extension, the table with a
// vector column, then check and move-org against the organization's database at customer.
func extensionRefused(e *env, customer string, setup []string, wantCode string, wantWords ...string) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(), setup...); err != nil {
		return false, "", err
	}
	// check before enable, and again after it (LokaDB Cloud's way: enable once, organizations
	// move later; check lists the enabled tables' dependencies as before enable, block 11)
	out, _ := e.checkWith(spec, "-customer", customer)
	found := reported(out, wantCode)
	if err := e.enable(); err != nil {
		return false, "", err
	}
	after, _ := e.checkWith(spec, "-customer", customer)
	foundAfter := reported(after, wantCode)
	p := mustSpecPath(e)
	mv, moveErr := e.lokadb("move-org", "-db", e.cloud(), "-spec", p, "-owner", "app", "-org", "1", "-mode", "trigger", "-customer", customer)
	var missing []string
	for _, w := range wantWords {
		if !strings.Contains(mv, w) {
			missing = append(missing, w)
		}
	}
	inCloud, _ := one(e.ctx, e.cloud(), orgInCloud)
	moved, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM lokadb.orgs WHERE org = 1")
	tables, _ := one(e.ctx, customer, "SELECT count(*) FROM pg_class WHERE relkind = 'r' AND relnamespace NOT IN ('pg_catalog'::regnamespace, 'information_schema'::regnamespace)")
	split, _ := one(e.ctx, e.cloud(), "SELECT coalesce(to_regclass('documents__o1')::text, 'none')")
	ok := found && foundAfter && moveErr != nil && len(missing) == 0 && moved == "0" && tables == "0" && split == "none" && inCloud == "100"
	var codes []string
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, "[customer") || strings.Contains(l, "Verdict") {
			codes = append(codes, strings.TrimSpace(l)[:min(len(strings.TrimSpace(l)), 90)])
		}
	}
	return ok, fmt.Sprintf("check: %s %v (%s) [%s]; check after enable: %v (%s); move-org: %s; missing in the messages: %v; organization 1 in the cloud: %s documents, moved %s, split out of DEFAULT: %s; tables made at the organization: %s",
		wantCode, found, finding(out, wantCode), strings.Join(codes, " | "), foundAfter, finding(after, wantCode), lastErr(mv), missing, inCloud, moved, split, tables), nil
}

// MV30. The cloud has PostGIS, the organization's server (the plain customer) has pgvector only.
func checkExtensionPackageMissing(e *env) (bool, string, error) {
	return extensionRefused(e, e.customer(), []string{"CREATE EXTENSION postgis", "ALTER TABLE documents ADD COLUMN geom geometry(Point, 4326)"},
		"customer_extension_unavailable", "extension postgis is not available", "Install its package")
}

// MV31. The cloud has earthdistance (a type earth over cube) in a column; the organization's role
// has CREATE on the database only: earthdistance is not a trusted extension, so only a
// superuser installs it (cube is trusted).
func checkExtensionNoRights(e *env) (bool, string, error) {
	ro := "ext_" + e.db
	if err := role(e, "customer", ro); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.customer(), "GRANT CREATE ON DATABASE "+e.db+" TO "+ro, "GRANT CREATE ON SCHEMA public TO "+ro); err != nil {
		return false, "", err
	}
	return extensionRefused(e, dsn("customer", e.db, ro),
		[]string{"CREATE EXTENSION cube", "CREATE EXTENSION earthdistance", "ALTER TABLE documents ADD COLUMN loc earth"},
		"customer_extension_privilege", "extension earthdistance cannot be installed", "as a superuser", "CREATE EXTENSION IF NOT EXISTS")
}

// mustSpecPath: the file of the tables spec.
func mustSpecPath(e *env) string {
	p, _ := e.specFile(spec)
	return p
}
