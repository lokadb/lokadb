package admin

// Sorted returns the spec with every child table after its parent table, the rest in the
// order given. enable and move-org handle the tables in this order. A table whose parent is
// not in the spec, or that is in a cycle of parents, keeps its place: lokadb check reports it.
func (s Spec) Sorted() Spec {
	// a table named with its schema, public.orders, is the key orders (rel.RelKey): enable's
	// steps after the COMMIT went by the name as the spec wrote it and found no organization
	// column for it (review 3 of check, L5), check said table_missing
	tables := make([]SpecTable, len(s.Tables))
	for i, t := range s.Tables {
		t.Name, t.ParentTable = RelKey(SplitKey(t.Name)), parentKey(t.ParentTable)
		tables[i] = t
	}
	s.Tables = tables
	in := map[string]bool{}
	for _, t := range s.Tables {
		in[t.Name] = true
	}
	placed := map[string]bool{}
	out := make([]SpecTable, 0, len(s.Tables))
	left := s.Tables
	for len(left) > 0 {
		var next []SpecTable
		for _, t := range left {
			if t.ParentTable == "" || placed[t.ParentTable] || !in[t.ParentTable] {
				out = append(out, t)
				placed[t.Name] = true
			} else {
				next = append(next, t)
			}
		}
		if len(next) == len(left) { // a cycle: nothing more can be placed
			out = append(out, next...)
			break
		}
		left = next
	}
	return Spec{Tables: out, Routing: s.Routing}
}
