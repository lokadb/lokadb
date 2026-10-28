package admin

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// DefaultCustomerSchema is where move-org puts an organization's tables when schema public of
// its database cannot take them: a table of the customer's has one of their names, or the
// roles may not create tables there (or give them to the application's role).
const DefaultCustomerSchema = "lokadb"

// SchemaChoice is where move-org creates the organization's tables in its database.
type SchemaChoice struct {
	Schema string // "" when no schema can take them
	Create bool   // move-org creates the schema (owned by the application's role)
	// Refused: why each schema tried cannot take them, in the order tried.
	Refused []SchemaRefusal
}

// SchemaRefusal is why a schema cannot take the organization's tables.
type SchemaRefusal struct {
	Schema string
	Step   string // "clash": a table of the customer's has the name; "create"; "owner": giving it to the app role
	Detail string
}

func (r SchemaRefusal) String() string {
	switch r.Step {
	case "clash":
		return fmt.Sprintf("schema %s already has a table %s that lokadb did not create", r.Schema, r.Detail)
	case "owner":
		return fmt.Sprintf("in schema %s the tables cannot be given to the application's role (%s)", r.Schema, r.Detail)
	case "app_create":
		return fmt.Sprintf("%s may not create in schema %s, and the proxy creates the indexes of later migrations as %s", r.Detail, r.Schema, r.Detail)
	}
	return fmt.Sprintf("schema %s: %s", r.Schema, r.Detail)
}

// ChooseCustomerSchema decides where the organization's tables go, by probes that are rolled
// back: requested, if given; else public, when the admin role (cust) can create tables there,
// give them to appRole ("" or the admin role itself: no transfer) and no table of the
// customer's has one of names; else DefaultCustomerSchema, made by move-org if missing.
// Logical replication needs the cloud's schema name, public (see chooseMode).
func ChooseCustomerSchema(ctx context.Context, cust *pgx.Conn, requested, appRole string, names []string, org int64) (SchemaChoice, error) {
	ownLike, retLike := lokadbTableLikes(org)
	candidates := []string{"public", DefaultCustomerSchema}
	if requested != "" {
		candidates = []string{requested}
	}
	r, err := rowsOf(ctx, cust, `SELECT current_user::text`)
	if err != nil {
		return SchemaChoice{}, err
	}
	if appRole == r[0][0] {
		appRole = ""
	}
	var choice SchemaChoice
	for _, s := range candidates {
		exists, err := rowsOf(ctx, cust, `SELECT (to_regnamespace($1) IS NOT NULL)::text`, q(s))
		if err != nil {
			return choice, err
		}
		if exists[0][0] == "true" {
			clash, err := rowsOf(ctx, cust, `SELECT c.relname::text FROM pg_class c
				WHERE c.relnamespace = to_regnamespace($1) AND c.relname::text = ANY($2)
				  AND NOT (coalesce(obj_description(c.oid, 'pg_class'), '') LIKE $3
				       OR ($4 <> '' AND coalesce(obj_description(c.oid, 'pg_class'), '') LIKE $4)) ORDER BY 1 LIMIT 1`, q(s), names, ownLike, retLike)
			if err != nil {
				return choice, err
			}
			if len(clash) > 0 {
				choice.Refused = append(choice.Refused, SchemaRefusal{Schema: s, Step: "clash", Detail: clash[0][0]})
				continue
			}
			// block 7: the proxy replays CREATE INDEX there as appRole, and an index needs
			// CREATE on the schema (public has none for other roles since Postgres 15). lokadb
			// grants nothing in the customer's schema: another one, made for appRole.
			if appRole != "" {
				may, err := rowsOf(ctx, cust, `SELECT has_schema_privilege($1, $2, 'CREATE')::text`, appRole, s)
				if err != nil {
					return choice, err
				}
				if may[0][0] != "true" {
					choice.Refused = append(choice.Refused, SchemaRefusal{Schema: s, Step: "app_create", Detail: appRole})
					continue
				}
			}
		}
		var steps []string
		if exists[0][0] != "true" {
			create := "CREATE SCHEMA " + q(s)
			if appRole != "" {
				create += " AUTHORIZATION " + q(appRole)
			}
			steps = append(steps, create)
		}
		table := q(s) + "." + ProbeName("table")
		steps = append(steps, "CREATE TABLE "+table+" (id bigint PRIMARY KEY)")
		if err := Probe(ctx, cust, steps...); err != nil {
			choice.Refused = append(choice.Refused, SchemaRefusal{Schema: s, Step: "create", Detail: firstLine(err.Error())})
			continue
		}
		if appRole != "" {
			if err := Probe(ctx, cust, append(steps, fmt.Sprintf("ALTER TABLE %s OWNER TO %s", table, q(appRole)))...); err != nil {
				choice.Refused = append(choice.Refused, SchemaRefusal{Schema: s, Step: "owner", Detail: firstLine(err.Error())})
				continue
			}
		}
		choice.Schema, choice.Create = s, exists[0][0] != "true"
		return choice, nil
	}
	return choice, nil
}

// GrantFor: the GRANT that lets the organization's tables go to public, where a logical
// move needs them, if what kept them out is only that the application's role may not create
// there (block 7); "" otherwise.
func (c SchemaChoice) GrantFor() string {
	for _, r := range c.Refused {
		if r.Schema == "public" && r.Step == "app_create" {
			return fmt.Sprintf("GRANT CREATE ON SCHEMA public TO %s", q(r.Detail))
		}
	}
	return ""
}

// Refusals: the reasons of c, one sentence.
func (c SchemaChoice) Refusals() string {
	var out []string
	for _, r := range c.Refused {
		out = append(out, r.String())
	}
	return strings.Join(out, "; ")
}

// OtherSchemaClashes: the tables of schemas other than public (tables: keys) go to the same
// schema in the organization's database, with no other schema to fall back on. Each table of
// the organization's own there with the name of one of them (or of its __o<org> partition) is
// a refusal: move-org leaves it alone and does not move.
func OtherSchemaClashes(ctx context.Context, cust *pgx.Conn, tables []string, org int64) ([]SchemaRefusal, error) {
	var out []SchemaRefusal
	ownLike, retLike := lokadbTableLikes(org)
	for _, t := range tables {
		s, n := SplitKey(t)
		if s == "public" {
			continue
		}
		names := []string{n}
		if org > 0 {
			names = append(names, hn(n, fmt.Sprintf("__o%d", org)))
		}
		clash, err := rowsOf(ctx, cust, `SELECT c.relname::text FROM pg_class c
			WHERE c.relnamespace = to_regnamespace($1) AND c.relname::text = ANY($2)
			  AND NOT (coalesce(obj_description(c.oid, 'pg_class'), '') LIKE $3
			       OR ($4 <> '' AND coalesce(obj_description(c.oid, 'pg_class'), '') LIKE $4)) ORDER BY 1`, q(s), names, ownLike, retLike)
		if err != nil {
			return nil, err
		}
		for _, c := range clash {
			out = append(out, SchemaRefusal{Schema: s, Step: "clash", Detail: c[0]})
		}
	}
	return out, nil
}
