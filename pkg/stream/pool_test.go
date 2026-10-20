package stream_test

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// poolFake is a minimal rpc.Client whose behaviour can be changed while a
// test runs. Only the methods the pool tests exercise are implemented;
// embedding the interface keeps the rest out of the way.
type poolFake struct {
	rpc.Client

	mu     sync.Mutex
	err    error
	latest uint32
	calls  int
	closed int
}

func (f *poolFake) GetLatestLedger(ctx context.Context) (protocol.GetLatestLedgerResponse, error) {
	if err := ctx.Err(); err != nil {
		return protocol.GetLatestLedgerResponse{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return protocol.GetLatestLedgerResponse{}, f.err
	}
	return protocol.GetLatestLedgerResponse{Sequence: f.latest}, nil
}

func (f *poolFake) GetEvents(ctx context.Context, _ protocol.GetEventsRequest) (protocol.GetEventsResponse, error) {
	if err := ctx.Err(); err != nil {
		return protocol.GetEventsResponse{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return protocol.GetEventsResponse{}, f.err
	}
	return protocol.GetEventsResponse{LatestLedger: f.latest}, nil
}

func (f *poolFake) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed++
	return nil
}

func (f *poolFake) setError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *poolFake) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *poolFake) closeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// newFakePool builds a pool whose URL maps to the supplied fake. The
// background probe is disabled unless a test opts back in, keeping the suite
// deterministic and free of timing races.
func newFakePool(t *testing.T, fakes map[string]*poolFake, opts ...stream.PoolOption) *stream.Pool {
	t.Helper()

	urls := make([]string, 0, len(fakes))
	for url := range fakes {
		urls = append(urls, url)
	}
	// Sort so the first endpoint is deterministic: several tests rely on the
	// failing endpoint being tried before the healthy one.
	sort.Strings(urls)
	opts = append([]stream.PoolOption{
		stream.WithHealthInterval(0),
		stream.WithClientFactory(func(url string) rpc.Client {
			return fakes[url]
		}),
	}, opts...)

	p, err := stream.NewPool(urls, opts...)
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func TestPoolRejectsNoEndpoints(t *testing.T) {
	t.Parallel()

	if _, err := stream.NewPool(nil); err == nil {
		t.Fatal("NewPool(nil) error = nil, want a validation failure")
	}
	if _, err := stream.NewPool([]string{"", "   "}); err == nil {
		t.Fatal("NewPool(blank URLs) error = nil, want a validation failure")
	}
}

func TestPoolDeduplicatesURLs(t *testing.T) {
	t.Parallel()

	fake := &poolFake{latest: 7}
	p, err := stream.NewPool(
		[]string{"https://a.example", " https://a.example ", "https://a.example"},
		stream.WithHealthInterval(0),
		stream.WithClientFactory(func(string) rpc.Client { return fake }))
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	if got := p.URLs(); len(got) != 1 {
		t.Fatalf("URLs() = %v, want one deduplicated endpoint", got)
	}
}

func TestPoolRoundRobins(t *testing.T) {
	t.Parallel()

	fakes := map[string]*poolFake{
		"https://a.example": {latest: 1},
		"https://b.example": {latest: 2},
		"https://c.example": {latest: 3},
	}
	p, err := stream.NewPool([]string{"https://a.example", "https://b.example", "https://c.example"},
		stream.WithHealthInterval(0),
		stream.WithClientFactory(func(url string) rpc.Client { return fakes[url] }))
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	for range 9 {
		if _, err := p.GetLatestLedger(context.Background()); err != nil {
			t.Fatalf("GetLatestLedger() error = %v", err)
		}
	}
	for url, fake := range fakes {
		if got := fake.callCount(); got != 3 {
			t.Errorf("%s handled %d calls, want 3", url, got)
		}
	}
}

func TestPoolFailsOverAndSkipsUnhealthyEndpoint(t *testing.T) {
	t.Parallel()

	bad := &poolFake{err: errors.New("dial tcp: connection refused")}
	good := &poolFake{latest: 42}
	p := newFakePool(t, map[string]*poolFake{
		"https://bad.example":  bad,
		"https://good.example": good,
	})

	resp, err := p.GetLatestLedger(context.Background())
	if err != nil {
		t.Fatalf("GetLatestLedger() error = %v, want failover to succeed", err)
	}
	if resp.Sequence != 42 {
		t.Errorf("Sequence = %d, want 42 from the healthy endpoint", resp.Sequence)
	}
	if got := p.Healthy(); got != 1 {
		t.Errorf("Healthy() = %d, want 1 after the failure", got)
	}

	// The failed endpoint is in cooldown, so subsequent calls should not even
	// try it. This is what keeps a dead backend from doubling every request.
	badCalls := bad.callCount()
	for range 3 {
		if _, err := p.GetLatestLedger(context.Background()); err != nil {
			t.Fatalf("GetLatestLedger() error = %v", err)
		}
	}
	if bad.callCount() != badCalls {
		t.Errorf("unhealthy endpoint was retried %d time(s) during cooldown", bad.callCount()-badCalls)
	}
}

func TestPoolRecoversEndpointThroughHealthCheck(t *testing.T) {
	t.Parallel()

	bad := &poolFake{err: errors.New("connection reset by peer")}
	good := &poolFake{latest: 9}
	p := newFakePool(t, map[string]*poolFake{
		"https://bad.example":  bad,
		"https://good.example": good,
	})

	if _, err := p.GetLatestLedger(context.Background()); err != nil {
		t.Fatalf("GetLatestLedger() error = %v", err)
	}
	if got := p.Healthy(); got != 1 {
		t.Fatalf("Healthy() = %d, want 1 before recovery", got)
	}

	bad.setError(nil)
	p.HealthCheck(context.Background())

	if got := p.Healthy(); got != 2 {
		t.Errorf("Healthy() = %d, want 2 after the probe succeeded", got)
	}
}

func TestPoolHealthCheckLeavesHealthyEndpointsAlone(t *testing.T) {
	t.Parallel()

	fake := &poolFake{latest: 1}
	p := newFakePool(t, map[string]*poolFake{"https://a.example": fake})

	p.HealthCheck(context.Background())
	if got := fake.callCount(); got != 0 {
		t.Errorf("probed a healthy endpoint %d time(s), want 0", got)
	}
}

func TestPoolPermanentErrorStopsFailover(t *testing.T) {
	t.Parallel()

	bad := &poolFake{err: rpc.Permanent(errors.New("invalid cursor"))}
	good := &poolFake{latest: 1}
	p := newFakePool(t, map[string]*poolFake{
		"https://bad.example":  bad,
		"https://good.example": good,
	})

	_, err := p.GetLatestLedger(context.Background())
	if !rpc.IsPermanent(err) {
		t.Fatalf("error = %v, want a permanent failure", err)
	}
	if got := good.callCount(); got != 0 {
		t.Errorf("tried another endpoint %d time(s) after a permanent error", got)
	}
}

func TestPoolReportsEveryEndpointFailure(t *testing.T) {
	t.Parallel()

	p := newFakePool(t, map[string]*poolFake{
		"https://a.example": {err: errors.New("503 Service Unavailable")},
		"https://b.example": {err: errors.New("504 Gateway Timeout")},
	})

	_, err := p.GetEvents(context.Background(), protocol.GetEventsRequest{})
	if err == nil {
		t.Fatal("GetEvents() error = nil, want an error")
	}
	for _, want := range []string{"https://a.example", "https://b.example"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %s", err, want)
		}
	}
	if !rpc.IsRetryable(err) {
		t.Errorf("error = %v, want it to remain retryable for the tailer", err)
	}
}

func TestPoolReturnsCancellationWithoutTryingEndpoints(t *testing.T) {
	t.Parallel()

	fake := &poolFake{latest: 1}
	p := newFakePool(t, map[string]*poolFake{"https://a.example": fake})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := p.GetLatestLedger(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if got := fake.callCount(); got != 0 {
		t.Errorf("called the endpoint %d time(s) with a cancelled context", got)
	}
}

func TestPoolCloseStopsProbeAndClosesClients(t *testing.T) {
	t.Parallel()

	fake := &poolFake{latest: 1}
	p, err := stream.NewPool([]string{"https://a.example"},
		stream.WithHealthInterval(time.Millisecond),
		stream.WithClientFactory(func(string) rpc.Client { return fake }))
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close() error = %v, want idempotence", err)
	}
	if got := fake.closeCount(); got != 1 {
		t.Errorf("underlying client closed %d time(s), want 1", got)
	}
}
