package proxy

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgproto3"
)

// The cloud's answers copied as bytes where the proxy only passes them on (BACKLOG block 9,
// item 15; third review of performance, perf3-22). pgproto3 decoded every message and the proxy
// encoded it again for the client: 240 ns a narrow row, 1.8 µs a row of 20 columns
// (BenchmarkRelayRows), a large part of the proxy's CPU in a big SELECT or COPY OUT. Now the
// relay reads the cloud's frames itself (frameReader) once the hijacked connection's Frontend
// holds no bytes of its own; a DataRow or CopyData whose statement goes to the client (mFwd, no
// error being skipped) is appended to the client's buffer as it came, every other message is
// decoded as before (decodeBackend).

// clientOut: what the proxy writes to the client, buffered (pgproto3.Backend's Send and Flush,
// and Raw for a frame passed on as it came).
type clientOut struct {
	w   io.Writer
	buf []byte
}

func newClientOut(w io.Writer) *clientOut { return &clientOut{w: w} }

func (c *clientOut) Send(m pgproto3.BackendMessage) {
	if b, err := m.Encode(c.buf); err == nil {
		c.buf = b
	}
}

// Raw appends a frame of type typ and body as it came from the cloud.
func (c *clientOut) Raw(typ byte, body []byte) {
	c.buf = append(c.buf, typ, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(c.buf[len(c.buf)-4:], uint32(len(body)+4))
	c.buf = append(c.buf, body...)
}

func (c *clientOut) Flush() error {
	if len(c.buf) == 0 {
		return nil
	}
	_, err := c.w.Write(c.buf)
	if cap(c.buf) > 1<<20 { // a big result's buffer is not kept for the session's life
		c.buf = nil
	} else {
		c.buf = c.buf[:0]
	}
	return err
}

// frameReader reads the cloud's messages as frames: type, body (valid until the next read).
type frameReader struct {
	r    *bufio.Reader
	hdr  [5]byte
	body []byte
}

func newFrameReader(c io.Reader) *frameReader {
	return &frameReader{r: bufio.NewReaderSize(c, 64<<10)}
}

// maxFrame: a message of the cloud's longer than this is taken for a broken stream.
const maxFrame = 1 << 30

func (f *frameReader) next() (byte, []byte, error) {
	if _, err := io.ReadFull(f.r, f.hdr[:]); err != nil {
		return 0, nil, err
	}
	n := int(binary.BigEndian.Uint32(f.hdr[1:])) - 4
	if n < 0 || n > maxFrame {
		return 0, nil, fmt.Errorf("a message of the cloud's of %d bytes", n)
	}
	if cap(f.body) < n {
		f.body = make([]byte, n)
	}
	f.body = f.body[:n]
	if _, err := io.ReadFull(f.r, f.body); err != nil {
		return 0, nil, err
	}
	if cap(f.body) > 1<<20 { // the next small message does not keep a big one's buffer
		b := f.body
		f.body = nil
		return f.hdr[0], b, nil
	}
	return f.hdr[0], f.body, nil
}

// buffered: bytes read from the cloud and not yet taken (a connection with some is not pooled).
func (f *frameReader) buffered() int { return f.r.Buffered() }

// decodeBackend: the message of a frame, as pgproto3's Frontend would decode it (a new value
// each time; its byte fields may point into body).
func decodeBackend(typ byte, body []byte) (pgproto3.BackendMessage, error) {
	var m pgproto3.BackendMessage
	switch typ {
	case '1':
		m = &pgproto3.ParseComplete{}
	case '2':
		m = &pgproto3.BindComplete{}
	case '3':
		m = &pgproto3.CloseComplete{}
	case 'A':
		m = &pgproto3.NotificationResponse{}
	case 'c':
		m = &pgproto3.CopyDone{}
	case 'C':
		m = &pgproto3.CommandComplete{}
	case 'd':
		m = &pgproto3.CopyData{}
	case 'D':
		m = &pgproto3.DataRow{}
	case 'E':
		m = &pgproto3.ErrorResponse{}
	case 'G':
		m = &pgproto3.CopyInResponse{}
	case 'H':
		m = &pgproto3.CopyOutResponse{}
	case 'I':
		m = &pgproto3.EmptyQueryResponse{}
	case 'K':
		m = &pgproto3.BackendKeyData{}
	case 'n':
		m = &pgproto3.NoData{}
	case 'N':
		m = &pgproto3.NoticeResponse{}
	case 's':
		m = &pgproto3.PortalSuspended{}
	case 'S':
		m = &pgproto3.ParameterStatus{}
	case 't':
		m = &pgproto3.ParameterDescription{}
	case 'T':
		m = &pgproto3.RowDescription{}
	case 'V':
		m = &pgproto3.FunctionCallResponse{}
	case 'W':
		m = &pgproto3.CopyBothResponse{}
	case 'Z':
		m = &pgproto3.ReadyForQuery{}
	case 'v':
		m = &pgproto3.NegotiateProtocolVersion{}
	default:
		return nil, fmt.Errorf("a message of the cloud's of unknown type %q", typ)
	}
	if err := m.Decode(body); err != nil {
		return nil, err
	}
	return m, nil
}

// passRaw: a frame of type typ goes to the client as it came: a row or copied data of a
// statement whose answers are the client's, with no error being skipped (route would deliver
// it with toClient, unchanged).
func (s *session) passRaw(typ byte, skip bool) bool {
	if (typ != 'D' && typ != 'd') || skip {
		return false
	}
	h := s.q.head()
	return h == nil || h.mode == mFwd
}

// toClientRaw is toClient for a frame passed on as it came.
func (s *session) toClientRaw(typ byte, body []byte) {
	s.cliMu.Lock()
	if s.direct.owedParse.CompareAndSwap(true, false) {
		s.cliOut.Send(&pgproto3.ParseComplete{})
	}
	s.cliOut.Raw(typ, body)
	s.pending += len(body) + 5
	s.cliMu.Unlock()
}
