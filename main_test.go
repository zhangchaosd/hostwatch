package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/ed25519"
	cryptorand "crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
)

func testPassword(value string) *string { return &value }

func testHost(id int, name string) Host {
	return Host{
		ID: id, Name: name, Address: "127.0.0.1", Port: 22, Username: "tester",
		AuthType: "password", Password: testPassword("plain-password"), Position: id - 1,
	}
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := newStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	return store
}

const testMetricOutput = `__SYSINFO__
node-1
4
16384
1048576
__STAT__
cpu 100 0 50 850 0 0 0 0
__MEM__
MemTotal: 1000 kB
MemAvailable: 250 kB
__NET__
lo: 10 0 0 0 0 0 0 0 10 0 0 0 0 0 0 0
eth0: 1000 0 0 0 0 0 0 0 2000 0 0 0 0 0 0 0
__DISK__
/dev/sda1 100000 42000 58000 42% /
`

type testSSHServer struct {
	listener    net.Listener
	config      *ssh.ServerConfig
	output      string
	hangCommand bool
	done        chan struct{}
	closeOnce   sync.Once
	wg          sync.WaitGroup
	connections atomic.Int64
}

func startTestSSHServer(t *testing.T, hangCommand bool) *testSSHServer {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(cryptorand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	config := &ssh.ServerConfig{
		PasswordCallback: func(ssh.ConnMetadata, []byte) (*ssh.Permissions, error) {
			return nil, nil
		},
	}
	config.AddHostKey(signer)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &testSSHServer{
		listener: listener, config: config, output: testMetricOutput,
		hangCommand: hangCommand, done: make(chan struct{}),
	}
	server.wg.Add(1)
	go server.acceptLoop()
	t.Cleanup(server.close)
	return server
}

func (server *testSSHServer) close() {
	server.closeOnce.Do(func() {
		close(server.done)
		_ = server.listener.Close()
	})
	server.wg.Wait()
}

func (server *testSSHServer) acceptLoop() {
	defer server.wg.Done()
	for {
		rawConnection, err := server.listener.Accept()
		if err != nil {
			return
		}
		server.connections.Add(1)
		server.wg.Add(1)
		go server.handleConnection(rawConnection)
	}
}

func (server *testSSHServer) handleConnection(rawConnection net.Conn) {
	defer server.wg.Done()
	defer rawConnection.Close()
	connection, channels, requests, err := ssh.NewServerConn(rawConnection, server.config)
	if err != nil {
		return
	}
	defer connection.Close()
	go ssh.DiscardRequests(requests)
	for channelRequest := range channels {
		if channelRequest.ChannelType() != "session" {
			_ = channelRequest.Reject(ssh.UnknownChannelType, "session only")
			continue
		}
		channel, channelRequests, err := channelRequest.Accept()
		if err != nil {
			return
		}
		server.wg.Add(1)
		go server.handleSession(channel, channelRequests)
	}
}

func (server *testSSHServer) handleSession(channel ssh.Channel, requests <-chan *ssh.Request) {
	defer server.wg.Done()
	defer channel.Close()
	for request := range requests {
		if request.Type != "exec" {
			_ = request.Reply(false, nil)
			continue
		}
		_ = request.Reply(true, nil)
		if server.hangCommand {
			<-server.done
			return
		}
		_, _ = channel.Write([]byte(server.output))
		_, _ = channel.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{0}))
		return
	}
}

func (server *testSSHServer) host(t *testing.T, id int) Host {
	t.Helper()
	host, port, err := net.SplitHostPort(server.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	portNumber, err := net.LookupPort("tcp", port)
	if err != nil {
		t.Fatal(err)
	}
	password := "test"
	return Host{
		ID: id, Name: "test", Address: host, Port: portNumber, Username: "tester",
		AuthType: "password", Password: &password,
	}
}

func TestMetricStoreRingAndGlobalLimits(t *testing.T) {
	store := newMetricStoreWithLimits(3, 4)
	now := float64(time.Now().Unix())
	for i := 1; i <= 4; i++ {
		store.add(1, Metric{Timestamp: now + float64(i), CPUPercent: float64(i)}, 60)
	}
	points := store.get(1, 60, nil, 10)
	if len(points) != 3 || points[0].CPUPercent != 2 || points[2].CPUPercent != 4 {
		t.Fatalf("per-host ring order is wrong: %#v", points)
	}
	store.add(2, Metric{Timestamp: now + 5, CPUPercent: 5}, 60)
	store.add(2, Metric{Timestamp: now + 6, CPUPercent: 6}, 60)
	points = store.get(1, 60, nil, 10)
	if len(points) != 2 || points[0].CPUPercent != 2.5 || points[1].CPUPercent != 4 || store.total != 4 {
		t.Fatalf("global compaction is wrong: points=%#v total=%d", points, store.total)
	}
}

func TestMetricStorePruningAndDownsampling(t *testing.T) {
	store := newMetricStoreWithLimits(100, 100)
	now := float64(time.Now().Unix())
	store.add(1, Metric{Timestamp: now - 7200}, 60)
	if points := store.get(1, 60, nil, 10); len(points) != 0 {
		t.Fatalf("expired point was not pruned: %#v", points)
	}
	for i := 0; i < 20; i++ {
		store.add(1, Metric{Timestamp: now + float64(i), CPUPercent: float64(i)}, 60)
	}
	points := store.get(1, 60, nil, 5)
	if len(points) != 5 || points[0].CPUPercent != 0 || points[4].CPUPercent != 19 {
		t.Fatalf("downsampling must preserve endpoints: %#v", points)
	}
}

func TestMetricSequenceSnapshotIsLossless(t *testing.T) {
	store := newMetricStoreWithLimits(1000, 1000)
	now := float64(time.Now().Unix())
	for index := 0; index < 500; index++ {
		store.add(1, Metric{Timestamp: now + float64(index)/1000}, 60)
	}
	cursor := uint64(0)
	seen := make(map[uint64]bool)
	for {
		payload, next := store.getAll([]int{1}, 60, nil, &cursor, 1000)
		for _, metric := range payload["1"] {
			if metric.Sequence <= cursor || metric.Sequence > next {
				t.Fatalf("sequence outside snapshot boundary: cursor=%d metric=%d next=%d", cursor, metric.Sequence, next)
			}
			if seen[metric.Sequence] {
				t.Fatalf("duplicate sequence %d", metric.Sequence)
			}
			seen[metric.Sequence] = true
		}
		if next == cursor {
			break
		}
		cursor = next
	}
	if len(seen) != 500 || cursor != 500 {
		t.Fatalf("incremental snapshot lost metrics: seen=%d cursor=%d", len(seen), cursor)
	}
}

func TestIncrementalSnapshotAllocationsStayBounded(t *testing.T) {
	store := newMetricStoreWithLimits(1000, 100000)
	now := float64(time.Now().Unix())
	hostIDs := make([]int, 100)
	for hostIndex := range hostIDs {
		hostIDs[hostIndex] = hostIndex + 1
		for pointIndex := 0; pointIndex < 20; pointIndex++ {
			store.add(hostIndex+1, Metric{Timestamp: now + float64(pointIndex)}, 60)
		}
	}
	cursor := uint64(1990)
	allocations := testing.AllocsPerRun(100, func() {
		store.getAll(hostIDs, 60, nil, &cursor, 480)
	})
	if allocations > 10 {
		t.Fatalf("incremental snapshot allocations regressed: %.1f", allocations)
	}
}

func TestCollectorReusesSSHConnection(t *testing.T) {
	server := startTestSSHServer(t, false)
	collector := newCollector()
	defer collector.reset()
	host := server.host(t, 1)
	for index := 0; index < 2; index++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, _, err := collector.collect(ctx, host, time.Second, index == 0)
		cancel()
		if err != nil {
			t.Fatalf("collection %d failed: %v", index, err)
		}
	}
	if connections := server.connections.Load(); connections != 1 {
		t.Fatalf("expected one reused SSH connection, got %d", connections)
	}
}

func TestCollectorConnectionReuseCanBeDisabled(t *testing.T) {
	t.Setenv("HOSTWATCH_MAX_IDLE_SSH", "0")
	server := startTestSSHServer(t, false)
	collector := newCollector()
	defer collector.reset()
	host := server.host(t, 1)
	for index := 0; index < 2; index++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, _, err := collector.collect(ctx, host, time.Second, index == 0)
		cancel()
		if err != nil {
			t.Fatalf("collection %d failed: %v", index, err)
		}
	}
	if connections := server.connections.Load(); connections != 2 {
		t.Fatalf("expected two non-reused SSH connections, got %d", connections)
	}
}

func TestCollectorCommandDeadline(t *testing.T) {
	server := startTestSSHServer(t, true)
	collector := newCollector()
	defer collector.reset()
	host := server.host(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, _, err := collector.collect(ctx, host, 150*time.Millisecond, false)
	if err == nil {
		t.Fatal("hanging SSH command must time out")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("SSH deadline took too long: %s", elapsed)
	}
}

func TestHostUpdateCancelsStaleCollection(t *testing.T) {
	server := startTestSSHServer(t, true)
	store := newTestStore(t)
	created, err := store.createHost(server.host(t, 0))
	if err != nil {
		t.Fatal(err)
	}
	poller := newPoller(store)
	defer poller.close()
	result := make(chan HostStatus, 1)
	go func() { result <- poller.collectHost(created.ID, true) }()
	deadline := time.Now().Add(time.Second)
	for server.connections.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	poller.hostUpdated(created.ID)
	select {
	case status := <-result:
		if status.State != "pending" {
			t.Fatalf("stale collection returned unexpected status: %#v", status)
		}
	case <-time.After(time.Second):
		t.Fatal("updated host did not cancel the stale collection")
	}
	points, _, _ := poller.metrics.stats()
	if points != 0 {
		t.Fatalf("stale collection wrote %d metrics", points)
	}
}

func TestPollerShutdownCancelsActiveSSH(t *testing.T) {
	t.Setenv("HOSTWATCH_MAX_CONCURRENT", "2")
	server := startTestSSHServer(t, true)
	store := newTestStore(t)
	if _, err := store.createHost(server.host(t, 0)); err != nil {
		t.Fatal(err)
	}
	poller := newPoller(store)
	poller.start()
	deadline := time.Now().Add(time.Second)
	for server.connections.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	started := time.Now()
	poller.close()
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("poller shutdown took too long: %s", elapsed)
	}
}

func TestSlowHostDoesNotDelayHealthyHostSchedule(t *testing.T) {
	t.Setenv("HOSTWATCH_MAX_CONCURRENT", "2")
	slowServer := startTestSSHServer(t, true)
	fastServer := startTestSSHServer(t, false)
	store := newTestStore(t)
	if _, err := store.createHost(slowServer.host(t, 0)); err != nil {
		t.Fatal(err)
	}
	fastHost := fastServer.host(t, 0)
	fastHost.Name = "fast"
	createdFast, err := store.createHost(fastHost)
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.data.Settings.RefreshInterval = 1
	store.data.Settings.SSHTimeout = 3
	store.mu.Unlock()

	poller := newPoller(store)
	poller.start()
	defer poller.close()
	deadline := time.Now().Add(2500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if points := poller.metrics.get(createdFast.ID, 60, nil, 10); len(points) >= 2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("healthy host was delayed by slow host; points=%d", len(poller.metrics.get(createdFast.ID, 60, nil, 10)))
}

func TestPollerRetriesHostsWhenQueueIsFull(t *testing.T) {
	t.Setenv("HOSTWATCH_MAX_CONCURRENT", "1")
	store := newTestStore(t)
	password := "secret"
	for index := 0; index < 4; index++ {
		_, err := store.createHost(Host{
			Name: fmt.Sprintf("host-%d", index), Address: "127.0.0.1", Port: 22,
			Username: "root", AuthType: "password", Password: &password,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	poller := newPoller(store)
	defer poller.close()
	poller.scheduleDue()

	poller.mu.Lock()
	defer poller.mu.Unlock()
	retryable := 0
	for id, running := range poller.running {
		if !running {
			retryable++
			if !poller.nextDue[id].IsZero() {
				t.Fatalf("host %d dropped from a full queue was delayed until %s", id, poller.nextDue[id])
			}
		}
	}
	if retryable == 0 {
		t.Fatal("test did not saturate the poll queue")
	}
}

func TestConfigMigrationCreatesBackup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	legacy := configFile{
		Version: 1, Settings: defaultSettings(), Hosts: []Host{testHost(1, "legacy")},
	}
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0644); err != nil {
		t.Fatal(err)
	}
	store, err := newStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if store.data.Version != currentConfigVersion || store.data.NextHostID != 2 {
		t.Fatalf("legacy config was not migrated: %#v", store.data)
	}
	backup, err := os.ReadFile(path + ".bak")
	if err != nil || !bytes.Contains(backup, []byte(`"version":1`)) {
		t.Fatalf("legacy backup missing: %v %s", err, backup)
	}
}

func TestConfigRecoversFromBackup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	store, err := newStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.createHost(testHost(0, "recover-me")); err != nil {
		t.Fatal(err)
	}
	if err := store.updateSettings(Settings{RefreshInterval: 20, HistoryMinutes: 30, SSHTimeout: 5}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{broken"), 0644); err != nil {
		t.Fatal(err)
	}
	recovered, err := newStore(path)
	if err != nil {
		t.Fatal(err)
	}
	hosts, _ := recovered.listHosts(true)
	if len(hosts) != 1 || hosts[0].Name != "recover-me" {
		t.Fatalf("backup recovery lost hosts: %#v", hosts)
	}
	primary, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := decodeConfig(primary); err != nil {
		t.Fatalf("primary config was not repaired: %v", err)
	}
}

func TestConfigImportExportAndValidation(t *testing.T) {
	store := newTestStore(t)
	legacy := configFile{Version: 1, Settings: defaultSettings(), Hosts: []Host{testHost(1, "imported")}}
	raw, _ := json.Marshal(legacy)
	if err := store.importConfig(raw); err != nil {
		t.Fatal(err)
	}
	exported, err := store.exportConfig()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(exported, []byte(`"version": 2`)) || !bytes.Contains(exported, []byte("plain-password")) {
		t.Fatalf("unexpected export: %s", exported)
	}
	if err := store.importConfig([]byte(`{"version":99}`)); err == nil {
		t.Fatal("future config version must be rejected")
	}
	hosts, _ := store.listHosts(true)
	if len(hosts) != 1 || hosts[0].Name != "imported" {
		t.Fatalf("failed import changed active config: %#v", hosts)
	}
}

func TestStoreCRUDAndReorder(t *testing.T) {
	store := newTestStore(t)
	first, err := store.createHost(testHost(0, "first"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.createHost(testHost(0, "second"))
	if err != nil {
		t.Fatal(err)
	}
	name := "first-edited"
	if _, err := store.updateHost(first.ID, HostPatch{Name: &name}); err != nil {
		t.Fatal(err)
	}
	if err := store.reorder([]int{second.ID, first.ID}); err != nil {
		t.Fatal(err)
	}
	_, hosts := store.listHosts(false)
	if hosts[0].ID != second.ID || hosts[1].Name != name {
		t.Fatalf("unexpected host order: %#v", hosts)
	}
	deleted, err := store.deleteHost(second.ID)
	if err != nil || !deleted {
		t.Fatalf("delete failed: deleted=%v err=%v", deleted, err)
	}
}

func TestConfigHTTPImportExport(t *testing.T) {
	store := newTestStore(t)
	poller := newPoller(store)
	handler := (&App{store: store, poller: poller}).routes()
	config := configFile{Version: currentConfigVersion, NextHostID: 2, Settings: defaultSettings(), Hosts: []Host{testHost(1, "api")}}
	raw, _ := json.Marshal(config)

	request := httptest.NewRequest(http.MethodPut, "/api/config", bytes.NewReader(raw))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("import status=%d body=%s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodGet, "/api/config", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Header().Get("Content-Disposition"), "hostwatch-config.json") {
		t.Fatalf("export response is wrong: status=%d headers=%v", response.Code, response.Header())
	}
	if !bytes.Contains(response.Body.Bytes(), []byte("plain-password")) {
		t.Fatal("exported API config must include plaintext credentials")
	}
}

func TestMetricAPIUsesSequenceCursor(t *testing.T) {
	store := newTestStore(t)
	created, err := store.createHost(testHost(0, "metrics"))
	if err != nil {
		t.Fatal(err)
	}
	poller := newPoller(store)
	defer poller.close()
	poller.metrics.add(created.ID, Metric{Timestamp: float64(time.Now().Unix()), CPUPercent: 12}, 60)
	handler := (&App{store: store, poller: poller}).routes()

	request := httptest.NewRequest(http.MethodGet, "/api/metrics?since_seq=0&max_points=10", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("metrics status=%d body=%s", response.Code, response.Body.String())
	}
	var payload struct {
		Metrics      map[string][]Metric `json:"metrics"`
		NextSequence uint64              `json:"next_sequence"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.NextSequence != 1 || len(payload.Metrics["1"]) != 1 || payload.Metrics["1"][0].Sequence != 1 {
		t.Fatalf("unexpected sequence payload: %#v", payload)
	}

	request = httptest.NewRequest(http.MethodGet, "/api/metrics?since_seq=1&max_points=10", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Metrics["1"]) != 0 || payload.NextSequence != 1 {
		t.Fatalf("cursor returned duplicates: %#v", payload)
	}

	request = httptest.NewRequest(http.MethodGet, "/api/snapshot?since_seq=0&max_points=10", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var snapshot struct {
		Hosts        []json.RawMessage   `json:"hosts"`
		Metrics      map[string][]Metric `json:"metrics"`
		NextSequence uint64              `json:"next_sequence"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || len(snapshot.Hosts) != 1 || len(snapshot.Metrics["1"]) != 1 || snapshot.NextSequence != 1 {
		t.Fatalf("unexpected snapshot: status=%d payload=%#v", response.Code, snapshot)
	}

	request = httptest.NewRequest(http.MethodGet, "/api/snapshot?since_seq=0&max_points=10", nil)
	request.Header.Set("Accept-Encoding", "gzip")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("snapshot was not compressed: headers=%v", response.Header())
	}
	reader, err := gzip.NewReader(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	decompressed, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()
	if err := json.Unmarshal(decompressed, &snapshot); err != nil || snapshot.NextSequence != 1 {
		t.Fatalf("invalid compressed snapshot: err=%v payload=%s", err, decompressed)
	}
}

func TestDecodeJSONRejectsTrailingAndOversizedBodies(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"one"}{"name":"two"}`))
	var target map[string]string
	if err := decodeJSON(request, &target); err == nil {
		t.Fatal("multiple JSON objects must be rejected")
	}
	request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(strings.Repeat("x", maxJSONBodyBytes+1)))
	if err := decodeJSON(request, &target); err == nil {
		t.Fatal("oversized JSON body must be rejected")
	}
}

func TestManualRefreshIsAsynchronous(t *testing.T) {
	store := newTestStore(t)
	created, err := store.createHost(testHost(0, "refresh"))
	if err != nil {
		t.Fatal(err)
	}
	poller := newPoller(store)
	defer poller.close()
	handler := (&App{store: store, poller: poller}).routes()
	request := httptest.NewRequest(http.MethodPost, "/api/hosts/"+strconv.Itoa(created.ID)+"/refresh", nil)
	response := httptest.NewRecorder()
	started := time.Now()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("refresh status=%d body=%s", response.Code, response.Body.String())
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("asynchronous refresh blocked for %s", elapsed)
	}
}

func TestMetricAndSystemInfoParsing(t *testing.T) {
	sample := `__SYSINFO__
node-1
8
32768
1048576
__STAT__
cpu 100 0 50 850 0 0 0 0
__MEM__
MemTotal: 1000 kB
MemAvailable: 250 kB
__NET__
lo: 10 0 0 0 0 0 0 0 10 0 0 0 0 0 0 0
eth0: 1000 0 0 0 0 0 0 0 2000 0 0 0 0 0 0 0
__DISK__
/dev/sda1 100000 42000 58000 42% /
`
	collector := newCollector()
	metric, err := collector.parseMetric(1, sample)
	if err != nil || metric.MemoryPercent != 75 || metric.DiskPercent != 42 {
		t.Fatalf("metric parse failed: metric=%#v err=%v", metric, err)
	}
	info, err := parseSystemInfo(sample)
	if err != nil || info.Hostname != "node-1" || info.CPUCores != 8 || info.MemoryBytes != 32768*1024 {
		t.Fatalf("system info parse failed: info=%#v err=%v", info, err)
	}
}

func TestRetryBackoff(t *testing.T) {
	cases := []struct {
		failures int
		want     time.Duration
	}{{1, 15 * time.Second}, {2, 30 * time.Second}, {3, time.Minute}, {10, maxBackoff}}
	for _, test := range cases {
		if got := retryBackoff(5, test.failures); got != test.want {
			t.Fatalf("failures=%d got=%s want=%s", test.failures, got, test.want)
		}
	}
}

func TestListenAddressIncludesIPv6Wildcard(t *testing.T) {
	t.Setenv("HOSTWATCH_HOST", "")
	t.Setenv("HOSTWATCH_PORT", "8000")
	if got := listenAddress(); got != ":8000" {
		t.Fatalf("default listen address=%q", got)
	}
	t.Setenv("HOSTWATCH_HOST", "::1")
	t.Setenv("HOSTWATCH_PORT", "9000")
	if got := listenAddress(); got != "[::1]:9000" {
		t.Fatalf("IPv6 listen address=%q", got)
	}
}

func BenchmarkMetricStoreSnapshot100Hosts(b *testing.B) {
	store := newMetricStoreWithLimits(1000, 100000)
	now := float64(time.Now().Unix())
	hostIDs := make([]int, 100)
	for hostIndex := range hostIDs {
		hostIDs[hostIndex] = hostIndex + 1
		for pointIndex := 0; pointIndex < 480; pointIndex++ {
			store.add(hostIndex+1, Metric{
				Timestamp: now + float64(pointIndex), CPUPercent: float64(pointIndex % 100),
			}, 60)
		}
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		store.getAll(hostIDs, 60, nil, nil, 480)
	}
}

func BenchmarkMetricStoreIncremental100Hosts(b *testing.B) {
	store := newMetricStoreWithLimits(1000, 100000)
	now := float64(time.Now().Unix())
	hostIDs := make([]int, 100)
	for hostIndex := range hostIDs {
		hostIDs[hostIndex] = hostIndex + 1
		for pointIndex := 0; pointIndex < 480; pointIndex++ {
			store.add(hostIndex+1, Metric{Timestamp: now + float64(pointIndex)}, 60)
		}
	}
	cursor := uint64(47900)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		store.getAll(hostIDs, 60, nil, &cursor, 480)
	}
}
