package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
)

// prefixConn replays bytes that were already read off the wire before the
// connection was handed over, then reads straight from the connection.
type prefixConn struct {
	net.Conn
	reader io.Reader
}

func (c *prefixConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

// hijackWithBuffered takes ownership of the TCP/TLS connection after a protocol
// upgrade and returns a net.Conn that still carries whatever the client had
// already sent.
//
// Discarding the *bufio.ReadWriter returned by Hijack silently drops those
// bytes. net/http runs a one-byte background read on an active connection to
// notice a client going away; when the client sends its HTTP/2 preface right
// after the 101 response, that read can consume the leading "P".
// (*conn).hijackLocked deliberately pushes the stolen byte back into the
// bufio.Reader it hands to the caller, so the byte is only lost if the caller
// throws that reader away. The HTTP/2 server then sees a truncated preface
// ("RI * HTTP/2.0...") and closes the connection, which surfaces on the Proxmox
// side as "backup register image failed: broken pipe".
func hijackWithBuffered(w http.ResponseWriter) (net.Conn, error) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil, fmt.Errorf("protocol upgrade is not supported by this connection")
	}
	conn, bufrw, err := hj.Hijack()
	if err != nil {
		return nil, err
	}
	if bufrw == nil || bufrw.Reader == nil {
		return conn, nil
	}
	buffered := bufrw.Reader.Buffered()
	if buffered == 0 {
		return conn, nil
	}
	pending := make([]byte, buffered)
	if _, err := io.ReadFull(bufrw.Reader, pending); err != nil {
		conn.Close()
		return nil, fmt.Errorf("read %d buffered bytes after upgrade: %w", buffered, err)
	}
	return &prefixConn{
		Conn:   conn,
		reader: io.MultiReader(bytes.NewReader(pending), conn),
	}, nil
}
