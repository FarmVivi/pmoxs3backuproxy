package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"
)

// hijackableRecorder is an http.ResponseWriter that hands over a fixed
// connection and buffered reader, mirroring what net/http does after an
// upgrade.
type hijackableRecorder struct {
	http.ResponseWriter
	conn  net.Conn
	bufrw *bufio.ReadWriter
	err   error
}

func (h *hijackableRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h.err != nil {
		return nil, nil, h.err
	}
	return h.conn, h.bufrw, nil
}

type plainRecorder struct {
	http.ResponseWriter
}

func bufferedReadWriter(t *testing.T, pending string, conn net.Conn) *bufio.ReadWriter {
	t.Helper()
	reader := bufio.NewReader(io.MultiReader(strings.NewReader(pending), conn))
	if pending != "" {
		// Peek is what fills the buffer; Buffered() is zero until then, exactly
		// like a reader that has already absorbed client bytes.
		if _, err := reader.Peek(len(pending)); err != nil {
			t.Fatalf("peek pending bytes: %v", err)
		}
	}
	return bufio.NewReadWriter(reader, bufio.NewWriter(conn))
}

func TestHijackWithBufferedReplaysPendingBytesBeforeConnection(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	bufrw := bufferedReadWriter(t, "PRI", server)
	conn, err := hijackWithBuffered(&hijackableRecorder{conn: server, bufrw: bufrw})
	if err != nil {
		t.Fatalf("hijackWithBuffered: %v", err)
	}

	go func() {
		client.Write([]byte(" * HTTP/2.0"))
		client.Close()
	}()

	got, err := io.ReadAll(conn)
	if err != nil && err != io.EOF {
		t.Fatalf("read upgraded connection: %v", err)
	}
	if want := "PRI * HTTP/2.0"; string(got) != want {
		t.Fatalf("upgraded connection read %q, want %q", got, want)
	}
}

func TestHijackWithBufferedReturnsConnectionUntouchedWhenNothingBuffered(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	bufrw := bufferedReadWriter(t, "", server)
	conn, err := hijackWithBuffered(&hijackableRecorder{conn: server, bufrw: bufrw})
	if err != nil {
		t.Fatalf("hijackWithBuffered: %v", err)
	}
	if conn != server {
		t.Fatalf("expected the raw connection to be returned unwrapped")
	}
}

func TestHijackWithBufferedRejectsNonHijackableWriter(t *testing.T) {
	if _, err := hijackWithBuffered(&plainRecorder{}); err == nil {
		t.Fatal("expected an error for a writer that cannot be hijacked")
	}
}

func TestHijackWithBufferedPropagatesHijackFailure(t *testing.T) {
	want := fmt.Errorf("hijack refused")
	if _, err := hijackWithBuffered(&hijackableRecorder{err: want}); err == nil {
		t.Fatal("expected the hijack error to be propagated")
	}
}

// upgradeServer runs the real net/http server on a real socket, upgrades like
// the backup and reader endpoints do, and serves HTTP/2 on the hijacked
// connection. handlerDelay widens the window during which net/http's one-byte
// background read can swallow the start of the client preface.
func upgradeServer(t *testing.T, handlerDelay time.Duration) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if handlerDelay > 0 {
				time.Sleep(handlerDelay)
			}
			w.Header().Add("Upgrade", "proxmox-backup-protocol-v1")
			w.WriteHeader(http.StatusSwitchingProtocols)
			conn, err := hijackWithBuffered(w)
			if err != nil {
				t.Errorf("hijackWithBuffered: %v", err)
				return
			}
			h2 := &http2.Server{}
			go h2.ServeConn(conn, &http2.ServeConnOpts{
				Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					io.WriteString(w, "upgraded")
				}),
			})
		}),
	}
	go srv.Serve(listener)
	t.Cleanup(func() {
		srv.Close()
		listener.Close()
	})
	return listener
}

// speakHTTP2AfterUpgrade performs the upgrade the way the Proxmox client does.
// When eager is true the HTTP/2 preface is written immediately after the
// request, without waiting for the 101 response, which is what makes the
// leading byte race observable.
func speakHTTP2AfterUpgrade(addr string, eager bool, prefaceDelay time.Duration) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}

	request := "GET //api2/json/backup HTTP/1.1\r\nHost: proxy\r\n" +
		"Connection: Upgrade\r\nUpgrade: proxmox-backup-protocol-v1\r\n\r\n"
	if _, err := io.WriteString(conn, request); err != nil {
		return err
	}

	reader := bufio.NewReader(conn)
	if eager {
		// Land the preface inside the window between the server reading the
		// request and hijacking the connection: that is where net/http's
		// one-byte background read can swallow its first byte.
		time.Sleep(prefaceDelay)
		if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
			return err
		}
		framer := http2.NewFramer(conn, reader)
		if err := framer.WriteSettings(); err != nil {
			return err
		}
	}

	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		return fmt.Errorf("read upgrade response: %w", err)
	}
	if response.StatusCode != http.StatusSwitchingProtocols {
		return fmt.Errorf("upgrade returned %s", response.Status)
	}

	framer := http2.NewFramer(conn, reader)
	if !eager {
		if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
			return err
		}
		if err := framer.WriteSettings(); err != nil {
			return err
		}
	}

	// A server that rejected the preface closes the connection instead of
	// answering with its own SETTINGS frame.
	frame, err := framer.ReadFrame()
	if err != nil {
		return fmt.Errorf("read first HTTP/2 frame: %w", err)
	}
	if _, ok := frame.(*http2.SettingsFrame); !ok {
		return fmt.Errorf("first frame is %T, want *http2.SettingsFrame", frame)
	}
	return nil
}

func TestUpgradeSurvivesPrefaceSentBeforeSwitchingProtocols(t *testing.T) {
	listener := upgradeServer(t, 200*time.Millisecond)
	// Whether the background read wins the race is a scheduling matter, so a
	// single attempt proves little; repeat until it is no longer luck.
	for attempt := 0; attempt < 20; attempt++ {
		if err := speakHTTP2AfterUpgrade(listener.Addr().String(), true, 50*time.Millisecond); err != nil {
			t.Fatalf("attempt %d: eager HTTP/2 preface rejected after upgrade: %v", attempt, err)
		}
	}
}

func TestUpgradeAcceptsPrefaceSentAfterSwitchingProtocols(t *testing.T) {
	listener := upgradeServer(t, 0)
	if err := speakHTTP2AfterUpgrade(listener.Addr().String(), false, 0); err != nil {
		t.Fatalf("well-behaved HTTP/2 client rejected after upgrade: %v", err)
	}
}

func TestUpgradeIsRaceFreeUnderConcurrentSessions(t *testing.T) {
	listener := upgradeServer(t, 200*time.Millisecond)
	const clients = 32

	var wg sync.WaitGroup
	errs := make(chan error, clients)
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := speakHTTP2AfterUpgrade(listener.Addr().String(), i%2 == 0, 20*time.Millisecond); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)

	var failures []string
	for err := range errs {
		failures = append(failures, err.Error())
	}
	if len(failures) > 0 {
		t.Fatalf("%d/%d concurrent upgrades failed: %s",
			len(failures), clients, strings.Join(failures, "; "))
	}
}

func TestPrefixConnForwardsConnectionMethods(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	wrapped := &prefixConn{Conn: server, reader: bytes.NewReader(nil)}
	if wrapped.LocalAddr() != server.LocalAddr() {
		t.Fatal("LocalAddr is not forwarded to the underlying connection")
	}
	if err := wrapped.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("SetDeadline is not forwarded: %v", err)
	}
}
