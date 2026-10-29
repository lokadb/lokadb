package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/lokadb/lokadb/proxy/internal/cloudproto"
)

var (
	kvPassword  = regexp.MustCompile(`(?i)(password\s*=\s*)('(\\.|[^'])*'|\S+)`)
	urlPassword = regexp.MustCompile(`(?i)(postgres(ql)?://[^:/@\s]*:)[^@\s]*@`)
)

// What Postgres puts of a row into an error: the DETAIL lines of a violated constraint and the
// value a conversion choked on. The column and constraint names stay (they say what failed); the
// values are the customer's data and do not leave the runner.
var (
	// Key (a, b)=(1, x) already exists. / is not present in table "t". / is still referenced from table "t". /
	// conflicts with existing key (a, b)=(1, y).   The value may hold parentheses: it ends at the known tail.
	keyValues = regexp.MustCompile(`(?s)(Key \([^)
]*\)=)\(.*?\)(\s+(?:already exists|is not present|is still referenced|conflicts with))`)
	existingKey = regexp.MustCompile(`(?s)(existing key \([^)
]*\)=)\(.*?\)(\.|$|
)`)
	keyAny = regexp.MustCompile(`(Key \([^)
]*\)=)\(([^
]*)`)
	failingRow = regexp.MustCompile(`(Failing row contains )\([^
]*\)`)
	quotedValueMsg = regexp.MustCompile(`(?i)((?:invalid input syntax for (?:type )?[^:
"]*|invalid input value for enum [^:
"]*|malformed [a-z ]*literal|date/time field value out of range|invalid [a-z ]* format|invalid value for parameter[^:
"]*): )"(?:[^"\
]|\.)*"`)
	outOfRange = regexp.MustCompile(`(value )"[^"
]*"( is out of range for type)`)
)

// Redact hides the passwords of DSNs in a line and the row values of database errors: whatever
// leaves the runner goes through it.
func Redact(s string) string {
	s = kvPassword.ReplaceAllString(s, "${1}***")
	s = urlPassword.ReplaceAllString(s, "${1}***@")
	return stripValues(s)
}

// stripValues: "Key (email)=(ann@example.com) already exists." becomes "Key (email)=(…) already exists."
func stripValues(s string) string {
	s = keyValues.ReplaceAllString(s, "${1}(…)${2}")
	s = existingKey.ReplaceAllString(s, "${1}(…)${2}")
	s = keyAny.ReplaceAllStringFunc(s, func(m string) string { // a shape not known above: the rest of the line goes
		if strings.Contains(m, "=(…)") {
			return m
		}
		return keyAny.ReplaceAllString(m, "${1}(…)")
	})
	s = failingRow.ReplaceAllString(s, "${1}(…)")
	s = quotedValueMsg.ReplaceAllString(s, `${1}"…"`)
	return outOfRange.ReplaceAllString(s, `${1}"…"${2}`)
}

// redactJSON: every string of a JSON document (a check report, a move's result) goes through Redact.
func redactJSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return raw
	}
	var v any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber() // numbers (a 64-bit id) come back exactly as they were
	if dec.Decode(&v) != nil {
		return raw
	}
	out, err := json.Marshal(redactValue(v))
	if err != nil {
		return raw
	}
	return out
}

func redactValue(v any) any {
	switch x := v.(type) {
	case string:
		return Redact(x)
	case []any:
		for i := range x {
			x[i] = redactValue(x[i])
		}
	case map[string]any:
		for k, e := range x {
			x[k] = redactValue(e)
		}
	}
	return v
}

// events: a job's progress lines waiting to be sent, in order.
type events struct {
	mu   sync.Mutex
	seq  int64
	buf  []cloudproto.Event
	wake chan struct{}
}

func newEvents() *events { return &events{wake: make(chan struct{}, 1)} }

func (e *events) add(kind, msg string, attrs map[string]any) {
	e.mu.Lock()
	e.seq++
	e.buf = append(e.buf, cloudproto.Event{Seq: e.seq, At: time.Now().UTC(), Kind: kind, Msg: msg, Attrs: attrs})
	e.mu.Unlock()
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

// pending: the events not yet acknowledged (at most n).
func (e *events) pending(n int) []cloudproto.Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.buf) > n {
		return append([]cloudproto.Event{}, e.buf[:n]...)
	}
	return append([]cloudproto.Event{}, e.buf...)
}

// ack drops the events up to seq.
func (e *events) ack(seq int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	i := 0
	for i < len(e.buf) && e.buf[i].Seq <= seq {
		i++
	}
	e.buf = e.buf[i:]
}

// eventHandler: a slog.Handler that turns the open module's log records into a job's events
// (Info progress, Warn warning, Error error; Debug dropped), redacted, and passes them on to
// the runner's own log.
type eventHandler struct {
	ev    *events
	next  slog.Handler
	attrs []slog.Attr
	group string
}

func eventLogger(ev *events, next *slog.Logger) *slog.Logger {
	return slog.New(&eventHandler{ev: ev, next: next.Handler()})
}

func (h *eventHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return l >= slog.LevelInfo || h.next.Enabled(ctx, l)
}

func (h *eventHandler) Handle(ctx context.Context, r slog.Record) error {
	if h.next.Enabled(ctx, r.Level) {
		_ = h.next.Handle(ctx, r)
	}
	if r.Level < slog.LevelInfo {
		return nil
	}
	kind := "progress"
	switch {
	case r.Level >= slog.LevelError:
		kind = "error"
	case r.Level >= slog.LevelWarn:
		kind = "warning"
	}
	attrs := map[string]any{}
	add := func(prefix string, a slog.Attr) {
		if a.Key == "job" {
			return
		}
		attrs[prefix+a.Key] = Redact(a.Value.Resolve().String())
	}
	for _, a := range h.attrs {
		add("", a)
	}
	prefix := ""
	if h.group != "" {
		prefix = h.group + "."
	}
	r.Attrs(func(a slog.Attr) bool { add(prefix, a); return true })
	if len(attrs) == 0 {
		attrs = nil
	}
	h.ev.add(kind, Redact(r.Message), attrs)
	return nil
}

func (h *eventHandler) WithAttrs(as []slog.Attr) slog.Handler {
	c := *h
	c.attrs = append(append([]slog.Attr{}, h.attrs...), as...)
	c.next = h.next.WithAttrs(as)
	return &c
}

func (h *eventHandler) WithGroup(name string) slog.Handler {
	c := *h
	if c.group != "" {
		name = c.group + "." + name
	}
	c.group = name
	c.next = h.next.WithGroup(name)
	return &c
}
