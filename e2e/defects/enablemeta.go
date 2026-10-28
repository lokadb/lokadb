package main

// What enable changes and disable (block 6) must give back is kept in lokadb.tables: the
// table's owner before enable and its identity columns with their sequence parameters.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "EM1", name: "enable records the table's owner and identity columns in lokadb.tables", run: checkEnableMeta},
	)
}

func checkEnableMeta(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE notes (id bigint GENERATED ALWAYS AS IDENTITY (START 5 INCREMENT 2 CACHE 3) PRIMARY KEY, organization_id bigint NOT NULL, body text)",
		"ALTER TABLE notes OWNER TO reader",
		"INSERT INTO notes (organization_id, body) VALUES (1, 'a'), (2, 'b')"); err != nil {
		return false, "", err
	}
	if err := e.enableSpec(`{"tables": [{"name": "notes", "tenant_column": "organization_id"}]}`); err != nil {
		return false, "", err
	}
	got, err := one(e.ctx, e.cloud(), `SELECT orig_owner || ' ' || (identities = '[{"col": "id", "generated": "a", "start": 5, "increment": 2,
		"min": 1, "max": 9223372036854775807, "cache": 3, "cycle": false}]'::jsonb)::text || ' ' || identities::text
		FROM lokadb.tables WHERE table_name = 'notes'`)
	if err != nil {
		return false, "", err
	}
	return strings.HasPrefix(got, "reader true "), fmt.Sprintf("orig_owner, identities as expected, identities: %s", got), nil
}
