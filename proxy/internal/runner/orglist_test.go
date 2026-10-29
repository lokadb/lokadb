package runner

import "testing"

// The column that names a customer in the list: the name, else the e-mail, else a title, slug or username.
func TestPickLabel(t *testing.T) {
	have := func(c ...string) map[string]bool {
		m := map[string]bool{}
		for _, x := range c {
			m[x] = true
		}
		return m
	}
	for _, c := range []struct {
		cols map[string]bool
		want string
	}{
		{have("id", "name", "email", "slug"), "name"},
		{have("id", "email", "slug", "username"), "email"},
		{have("id", "title", "slug"), "title"},
		{have("id", "slug", "username"), "slug"},
		{have("id", "username"), "username"},
		{have("id", "plan"), ""},
	} {
		want := c.want
		if got := pickLabel(c.cols); got != want {
			t.Errorf("%v: %q, want %q", c.cols, got, want)
		}
	}
}
