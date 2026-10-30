package trig

import "testing"

func TestRewrite(t *testing.T) {
	cases := []struct{ in, want string }{
		{"BEGIN INSERT INTO audit (tbl, op) VALUES (TG_TABLE_NAME, TG_OP); RETURN NEW; END",
			"BEGIN INSERT INTO audit (tbl, op) VALUES (lokadb.tg_name(TG_RELID, TG_TABLE_NAME), TG_OP); RETURN NEW; END"},
		{"BEGIN PERFORM pg_notify(tg_table_schema || '.' || TG_RelName, NEW.id::text); RETURN NULL; END",
			"BEGIN PERFORM pg_notify(lokadb.tg_schema(TG_RELID, TG_TABLE_SCHEMA) || '.' || lokadb.tg_name(TG_RELID, TG_TABLE_NAME), NEW.id::text); RETURN NULL; END"},
		{"BEGIN EXECUTE format('INSERT INTO %I.log VALUES ($1)', TG_TABLE_SCHEMA) USING TG_RELID::regclass; RETURN NEW; END",
			"BEGIN EXECUTE format('INSERT INTO %I.log VALUES ($1)', lokadb.tg_schema(TG_RELID, TG_TABLE_SCHEMA)) USING lokadb.tg_relid(TG_RELID)::regclass; RETURN NEW; END"},
		// left alone: literals, quoted identifiers, comments, nested dollar strings, fields
		{"BEGIN -- TG_TABLE_NAME here\n RAISE NOTICE 'TG_TABLE_NAME is %', 1; /* TG_RELID /* nested */ */ x := \"tg_relid\"; y := r.tg_relid; z := $q$TG_TABLE_NAME$q$; RETURN NEW; END",
			"BEGIN -- TG_TABLE_NAME here\n RAISE NOTICE 'TG_TABLE_NAME is %', 1; /* TG_RELID /* nested */ */ x := \"tg_relid\"; y := r.tg_relid; z := $q$TG_TABLE_NAME$q$; RETURN NEW; END"},
		{"BEGIN x := E'it\\'s TG_TABLE_NAME'; y := TG_TABLE_NAME; RETURN NEW; END",
			"BEGIN x := E'it\\'s TG_TABLE_NAME'; y := lokadb.tg_name(TG_RELID, TG_TABLE_NAME); RETURN NEW; END"},
		{"BEGIN RETURN NEW; END", "BEGIN RETURN NEW; END"},
	}
	for _, c := range cases {
		got := Rewrite(c.in)
		if got != c.want {
			t.Errorf("Rewrite(%q)\n got %q\nwant %q", c.in, got, c.want)
		}
		if Rewrite(got) != got {
			t.Errorf("Rewrite is not idempotent on %q", got)
		}
	}
	// Restore gives the variables back (TG_RELNAME as its synonym TG_TABLE_NAME)
	in := "BEGIN INSERT INTO audit VALUES (TG_TABLE_NAME, TG_TABLE_SCHEMA, TG_RELID); RETURN NEW; END"
	if got := Restore(Rewrite(in)); got != in {
		t.Errorf("Restore(Rewrite) = %q", got)
	}
	if !Mentions("x := TG_RELID") || Mentions("RETURN NEW") || !Rewritten(Rewrite("x := TG_RELID")) {
		t.Error("Mentions / Rewritten")
	}
}
