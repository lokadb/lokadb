package main

// Block 4, schemas: the tables of other schemas linked to the LokaDB tables are not a
// limitation of lokadb any more (item 9): check advises them as it advises those of public.

import (
	"encoding/json"
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "X13", name: "tables outside schema public linked to the LokaDB tables (child by FK, organization column): check advises them as tables of public, by their schema, not as a limitation", run: checkOutsidePublic},
	)
}

// X13. Schema app: audit references documents by FK, notes has the organization column,
// events is published with documents. Schema "app.x" has a dot in its name: logs has the
// organization column. They are advised like tables of public (cloud_references,
// tenant_table_in_cloud), by key; no finding calls them a limitation of lokadb.
func checkOutsidePublic(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE SCHEMA app",
		"CREATE TABLE app.audit (id bigserial PRIMARY KEY, document_id bigint REFERENCES documents ON DELETE CASCADE, note text)",
		"CREATE TABLE app.notes (id bigserial PRIMARY KEY, organization_id bigint, body text)",
		"CREATE TABLE app.events (id bigserial PRIMARY KEY, payload text)",
		"CREATE PUBLICATION x13_outbox FOR TABLE documents, app.events",
		`CREATE SCHEMA "app.x"`,
		`CREATE TABLE "app.x".logs (id bigserial PRIMARY KEY, organization_id bigint, line text)`); err != nil {
		return false, "", err
	}
	_, code := e.checkWith(spec)
	js, _ := e.checkWith(spec, "-json")
	var rep struct {
		Findings []struct {
			Code     string   `json:"code"`
			Object   string   `json:"object"`
			Message  string   `json:"message"`
			Examples []string `json:"examples"`
			Subject  *struct {
				Kind, Schema, Table string
			} `json:"subject"`
		} `json:"findings"`
	}
	i := max(strings.Index(js, "{"), 0) // a decoder: the log after the report is no JSON
	if err := json.NewDecoder(strings.NewReader(js[i:])).Decode(&rep); err != nil {
		return false, "check -json: " + err.Error(), nil
	}
	limits := 0
	for _, f := range rep.Findings {
		if f.Code == "table_outside_public" || f.Code == "table_not_in_public" || f.Code == "tables_not_in_public" {
			limits++
		}
	}
	text := func(code string) string { // the finding's message and examples
		for _, f := range rep.Findings {
			if f.Code == code {
				return f.Message + " [" + strings.Join(f.Examples, ", ") + "]"
			}
		}
		return "-"
	}
	refs, tenant := text("cloud_references"), text("tenant_table_in_cloud")
	ok := limits == 0 && strings.Contains(refs, "app.audit") && strings.Contains(tenant, "app.notes") && strings.Contains(tenant, `"app.x".logs`)
	return ok, fmt.Sprintf("check (exit %d): findings about a limitation %d; cloud_references %s; tenant_table_in_cloud %s", code, limits, refs, tenant), nil
}
