package runner

import (
	"context"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/lokadb/lokadb/proxy/internal/cloudproto"
)

var (
	kvPassword  = regexp.MustCompile(`(?i)(password\s*=\s*)('(\\.|[^'])*'|\S+)`)
	urlPassword = regexp.MustCompile(`(?i)(postgres(ql)?://[^:/@\s]*:)[^@\s]*@`)
)

// Redact hides the passwords of DSNs in a line: whatever leaves the runner goes through it.
func Redact(s string) string {
	s = kvPassword.ReplaceAllString(s, "${1}***")
	return urlPassword.ReplaceAllString(s, "${1}***@")
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
