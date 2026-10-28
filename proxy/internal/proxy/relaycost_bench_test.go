package proxy

import (
	"bytes"
	"io"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// What relaying the cloud's rows costs (BACKLOG block 9, item 15; perf3-22): each DataRow decoded
// by pgproto3 and encoded again for the client, against a copy of its bytes.
func relayRows(rows, cols, width int) []byte {
	var buf []byte
	vals := make([][]byte, cols)
	for i := range vals {
		vals[i] = bytes.Repeat([]byte("x"), width)
	}
	for i := 0; i < rows; i++ {
		buf, _ = (&pgproto3.DataRow{Values: vals}).Encode(buf)
	}
	buf, _ = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
	return buf
}

func BenchmarkRelayRows(b *testing.B) {
	for _, c := range []struct {
		name              string
		rows, cols, width int
	}{{"narrow", 10000, 3, 8}, {"wide", 10000, 20, 40}} {
		in := relayRows(c.rows, c.cols, c.width)
		b.Run(c.name+"/decode-encode", func(b *testing.B) {
			b.SetBytes(int64(len(in)))
			for i := 0; i < b.N; i++ {
				fe := pgproto3.NewFrontend(bytes.NewReader(in), io.Discard)
				be := pgproto3.NewBackend(nil, io.Discard)
				n := 0
				for {
					m, err := fe.Receive()
					if err != nil {
						b.Fatal(err)
					}
					be.Send(m)
					if n++; n%200 == 0 { // the relay flushes every 64 KB or so
						_ = be.Flush()
					}
					if _, ok := m.(*pgproto3.ReadyForQuery); ok {
						break
					}
				}
				_ = be.Flush()
			}
		})
		b.Run(c.name+"/frames", func(b *testing.B) { // the relay's way now: frameReader, clientOut.Raw
			b.SetBytes(int64(len(in)))
			for i := 0; i < b.N; i++ {
				fr := newFrameReader(bytes.NewReader(in))
				out := newClientOut(io.Discard)
				n := 0
				for {
					typ, body, err := fr.next()
					if err != nil {
						b.Fatal(err)
					}
					out.Raw(typ, body)
					if n++; n%200 == 0 {
						_ = out.Flush()
					}
					if typ == 'Z' {
						break
					}
				}
				_ = out.Flush()
			}
		})
	}
}
