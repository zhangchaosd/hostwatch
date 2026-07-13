package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
	if len(points) != 2 || points[0].CPUPercent != 3 || store.total != 4 {
		t.Fatalf("global eviction is wrong: points=%#v total=%d", points, store.total)
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
