package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/crypto/ssh"
)

const (
	maxPointsPerHost   = 20000
	maxTotalPoints     = 100000
	defaultChartPoints = 480
	maxChartPoints     = 1000
	systemInfoTTL      = 6 * time.Hour
	maxBackoff         = 5 * time.Minute
)

// version is overridden from a release tag with -ldflags "-X main.version=...".
var version = "0.2.0"

const systemInfoCommand = "printf '__SYSINFO__\\n'\n" +
	"hostname\n" +
	"getconf _NPROCESSORS_ONLN 2>/dev/null || nproc\n" +
	"awk '/^MemTotal:/ {print $2}' /proc/meminfo\n" +
	"df -Pk / | tail -n 1 | awk '{print $2}'\n"

const metricCommand = "printf '__STAT__\\n'\n" +
	"head -n 1 /proc/stat\n" +
	"printf '__MEM__\\n'\n" +
	"awk '/^(MemTotal|MemAvailable):/ {print}' /proc/meminfo\n" +
	"printf '__NET__\\n'\n" +
	"cat /proc/net/dev\n" +
	"printf '__DISK__\\n'\n" +
	"df -P / | tail -n 1\n"

const htmlAsset = "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"
const cssAsset = "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"
const jsAsset = "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"

var (
	indexHTML = mustDecodeAsset(htmlAsset)
	styleCSS  = mustDecodeAsset(cssAsset)
	appJS     = mustDecodeAsset(jsAsset)
)

type Settings struct {
	RefreshInterval int `json:"refresh_interval"`
	HistoryMinutes  int `json:"history_minutes"`
	SSHTimeout      int `json:"ssh_timeout"`
}

func defaultSettings() Settings {
	return Settings{RefreshInterval: 15, HistoryMinutes: 60, SSHTimeout: 10}
}

func (s Settings) validate() error {
	if s.RefreshInterval < 5 || s.RefreshInterval > 3600 {
		return errors.New("采集频率必须在 5 到 3600 秒之间")
	}
	if s.HistoryMinutes < 5 || s.HistoryMinutes > 1440 {
		return errors.New("展示时长必须在 5 到 1440 分钟之间")
	}
	if s.SSHTimeout < 3 || s.SSHTimeout > 120 {
		return errors.New("SSH 超时必须在 3 到 120 秒之间")
	}
	return nil
}

type Host struct {
	ID         int     `json:"id"`
	Name       string  `json:"name"`
	Address    string  `json:"address"`
	Port       int     `json:"port"`
	Username   string  `json:"username"`
	AuthType   string  `json:"auth_type"`
	Password   *string `json:"password"`
	PrivateKey *string `json:"private_key"`
	Passphrase *string `json:"passphrase"`
	Position   int     `json:"position"`
	CreatedAt  float64 `json:"created_at"`
}

type PublicHost struct {
	ID            int    `json:"id"`
	Name          string `json:"name"`
	Address       string `json:"address"`
	Port          int    `json:"port"`
	Username      string `json:"username"`
	AuthType      string `json:"auth_type"`
	Position      int    `json:"position"`
	HasPassword   bool   `json:"has_password"`
	HasPrivateKey bool   `json:"has_private_key"`
}

type HostPatch struct {
	Name       *string `json:"name"`
	Address    *string `json:"address"`
	Port       *int    `json:"port"`
	Username   *string `json:"username"`
	AuthType   *string `json:"auth_type"`
	Password   *string `json:"password"`
	PrivateKey *string `json:"private_key"`
	Passphrase *string `json:"passphrase"`
}

type configFile struct {
	Version    int      `json:"version"`
	NextHostID int      `json:"next_host_id"`
	Settings   Settings `json:"settings"`
	Hosts      []Host   `json:"hosts"`
}

type Store struct {
	mu   sync.RWMutex
	path string
	data configFile
}

func newStore(path string) (*Store, error) {
	s := &Store{path: path}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) load() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		s.data = configFile{Version: 1, NextHostID: 1, Settings: defaultSettings(), Hosts: []Host{}}
		return s.saveLocked()
	}
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, &s.data); err != nil {
		return fmt.Errorf("读取 config.json 失败: %w", err)
	}
	if s.data.Version == 0 {
		s.data.Version = 1
	}
	if s.data.NextHostID < 1 {
		s.data.NextHostID = 1
		for _, h := range s.data.Hosts {
			if h.ID >= s.data.NextHostID {
				s.data.NextHostID = h.ID + 1
			}
		}
	}
	if s.data.Settings.RefreshInterval == 0 {
		s.data.Settings = defaultSettings()
	}
	return s.data.Settings.validate()
}

func (s *Store) saveLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func publicHost(h Host) PublicHost {
	return PublicHost{
		ID: h.ID, Name: h.Name, Address: h.Address, Port: h.Port, Username: h.Username,
		AuthType: h.AuthType, Position: h.Position,
		HasPassword:   h.Password != nil && *h.Password != "",
		HasPrivateKey: h.PrivateKey != nil && *h.PrivateKey != "",
	}
}

func (s *Store) listHosts(private bool) ([]Host, []PublicHost) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	hosts := append([]Host(nil), s.data.Hosts...)
	sort.Slice(hosts, func(i, j int) bool {
		if hosts[i].Position == hosts[j].Position {
			return hosts[i].ID < hosts[j].ID
		}
		return hosts[i].Position < hosts[j].Position
	})
	if private {
		return hosts, nil
	}
	out := make([]PublicHost, len(hosts))
	for i, h := range hosts {
		out[i] = publicHost(h)
	}
	return nil, out
}

func (s *Store) getHost(id int) (Host, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, h := range s.data.Hosts {
		if h.ID == id {
			return h, true
		}
	}
	return Host{}, false
}

func validateHost(h Host) error {
	h.Name = strings.TrimSpace(h.Name)
	h.Address = strings.TrimSpace(h.Address)
	h.Username = strings.TrimSpace(h.Username)
	if h.Name == "" || h.Address == "" || h.Username == "" {
		return errors.New("名称、地址和用户名不能为空")
	}
	if h.Port < 1 || h.Port > 65535 {
		return errors.New("SSH 端口必须在 1 到 65535 之间")
	}
	if h.AuthType != "password" && h.AuthType != "key" {
		return errors.New("认证方式必须是 password 或 key")
	}
	if h.AuthType == "password" && (h.Password == nil || *h.Password == "") {
		return errors.New("密码登录必须填写密码")
	}
	if h.AuthType == "key" && (h.PrivateKey == nil || *h.PrivateKey == "") {
		return errors.New("密钥登录必须填写私钥")
	}
	return nil
}

func (s *Store) createHost(h Host) (PublicHost, error) {
	if h.Port == 0 {
		h.Port = 22
	}
	if err := validateHost(h); err != nil {
		return PublicHost{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	h.Name = strings.TrimSpace(h.Name)
	h.Address = strings.TrimSpace(h.Address)
	h.Username = strings.TrimSpace(h.Username)
	h.ID = s.data.NextHostID
	s.data.NextHostID++
	h.Position = len(s.data.Hosts)
	h.CreatedAt = float64(time.Now().UnixNano()) / 1e9
	s.data.Hosts = append(s.data.Hosts, h)
	if err := s.saveLocked(); err != nil {
		return PublicHost{}, err
	}
	return publicHost(h), nil
}

func (s *Store) updateHost(id int, p HostPatch) (PublicHost, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.data.Hosts {
		h := &s.data.Hosts[i]
		if h.ID != id {
			continue
		}
		candidate := *h
		if p.Name != nil {
			candidate.Name = strings.TrimSpace(*p.Name)
		}
		if p.Address != nil {
			candidate.Address = strings.TrimSpace(*p.Address)
		}
		if p.Port != nil {
			candidate.Port = *p.Port
		}
		if p.Username != nil {
			candidate.Username = strings.TrimSpace(*p.Username)
		}
		if p.AuthType != nil {
			candidate.AuthType = *p.AuthType
		}
		if p.Password != nil {
			candidate.Password = p.Password
		}
		if p.PrivateKey != nil {
			candidate.PrivateKey = p.PrivateKey
		}
		if p.Passphrase != nil {
			candidate.Passphrase = p.Passphrase
		}
		if err := validateHost(candidate); err != nil {
			return PublicHost{}, err
		}
		previous := *h
		*h = candidate
		if err := s.saveLocked(); err != nil {
			*h = previous
			return PublicHost{}, err
		}
		return publicHost(*h), nil
	}
	return PublicHost{}, os.ErrNotExist
}

func (s *Store) deleteHost(id int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	found := false
	out := s.data.Hosts[:0]
	for _, h := range s.data.Hosts {
		if h.ID == id {
			found = true
			continue
		}
		out = append(out, h)
	}
	if !found {
		return false
	}
	s.data.Hosts = out
	sort.Slice(s.data.Hosts, func(i, j int) bool { return s.data.Hosts[i].Position < s.data.Hosts[j].Position })
	for i := range s.data.Hosts {
		s.data.Hosts[i].Position = i
	}
	return s.saveLocked() == nil
}

func (s *Store) reorder(ids []int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(ids) != len(s.data.Hosts) {
		return errors.New("排序列表必须包含全部主机")
	}
	positions := make(map[int]int, len(ids))
	for i, id := range ids {
		if _, exists := positions[id]; exists {
			return errors.New("排序列表包含重复主机")
		}
		positions[id] = i
	}
	for i := range s.data.Hosts {
		pos, ok := positions[s.data.Hosts[i].ID]
		if !ok {
			return errors.New("排序列表必须包含全部主机")
		}
		s.data.Hosts[i].Position = pos
	}
	sort.Slice(s.data.Hosts, func(i, j int) bool { return s.data.Hosts[i].Position < s.data.Hosts[j].Position })
	return s.saveLocked()
}

func (s *Store) settings() Settings {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data.Settings
}

func (s *Store) updateSettings(settings Settings) error {
	if err := settings.validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Settings = settings
	return s.saveLocked()
}

type Metric struct {
	Timestamp     float64 `json:"timestamp"`
	CPUPercent    float64 `json:"cpu_percent"`
	MemoryPercent float64 `json:"memory_percent"`
	NetworkRXMbps float64 `json:"network_rx_mbps"`
	NetworkTXMbps float64 `json:"network_tx_mbps"`
	DiskPercent   float64 `json:"disk_percent"`
}

type MetricStore struct {
	mu    sync.Mutex
	data  map[int][]Metric
	total int
}

func newMetricStore() *MetricStore {
	return &MetricStore{data: make(map[int][]Metric)}
}

func (m *MetricStore) add(hostID int, metric Metric, historyMinutes int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneHostLocked(hostID, float64(time.Now().UnixNano())/1e9-float64(historyMinutes*60))
	history := m.data[hostID]
	if len(history) >= maxPointsPerHost {
		history = history[1:]
		m.total--
	}
	history = append(history, metric)
	m.data[hostID] = history
	m.total++
	for m.total > maxTotalPoints {
		oldestID, oldestTime := 0, math.MaxFloat64
		for id, points := range m.data {
			if len(points) > 0 && points[0].Timestamp < oldestTime {
				oldestID, oldestTime = id, points[0].Timestamp
			}
		}
		if oldestID == 0 {
			break
		}
		m.data[oldestID] = m.data[oldestID][1:]
		m.total--
		if len(m.data[oldestID]) == 0 {
			delete(m.data, oldestID)
		}
	}
}

func (m *MetricStore) pruneHostLocked(hostID int, cutoff float64) {
	points := m.data[hostID]
	index := 0
	for index < len(points) && points[index].Timestamp < cutoff {
		index++
	}
	if index > 0 {
		m.total -= index
		points = points[index:]
	}
	if len(points) == 0 {
		delete(m.data, hostID)
	} else {
		m.data[hostID] = points
	}
}

func (m *MetricStore) get(hostID int, historyMinutes int, since *float64, limit int) []Metric {
	m.mu.Lock()
	defer m.mu.Unlock()
	cutoff := float64(time.Now().UnixNano())/1e9 - float64(historyMinutes*60)
	m.pruneHostLocked(hostID, cutoff)
	points := m.data[hostID]
	selected := make([]Metric, 0, len(points))
	for _, point := range points {
		if since == nil || point.Timestamp > *since {
			selected = append(selected, point)
		}
	}
	if limit < 2 {
		limit = 2
	}
	if limit > maxChartPoints {
		limit = maxChartPoints
	}
	if len(selected) <= limit {
		return selected
	}
	out := make([]Metric, 0, limit)
	last := len(selected) - 1
	lastIndex := -1
	for i := 0; i < limit; i++ {
		index := int(math.Round(float64(i*last) / float64(limit-1)))
		if index != lastIndex {
			out = append(out, selected[index])
			lastIndex = index
		}
	}
	return out
}

func (m *MetricStore) remove(hostID int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.total -= len(m.data[hostID])
	delete(m.data, hostID)
}

func (m *MetricStore) pruneAll(historyMinutes int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cutoff := float64(time.Now().UnixNano())/1e9 - float64(historyMinutes*60)
	for id := range m.data {
		m.pruneHostLocked(id, cutoff)
	}
}

type SystemInfo struct {
	Hostname    string `json:"hostname"`
	CPUCores    int    `json:"cpu_cores"`
	MemoryBytes int64  `json:"memory_bytes"`
	DiskBytes   int64  `json:"disk_bytes"`
}

type counters struct {
	total, idle, rx, tx uint64
	timestamp           time.Time
}

type Collector struct {
	mu       sync.Mutex
	previous map[int]counters
}

func newCollector() *Collector {
	return &Collector{previous: make(map[int]counters)}
}

func (c *Collector) collect(host Host, timeout time.Duration, includeInfo bool) (Metric, *SystemInfo, error) {
	auth, err := sshAuth(host)
	if err != nil {
		return Metric{}, nil, err
	}
	config := &ssh.ClientConfig{
		User: host.Username, Auth: []ssh.AuthMethod{auth},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), Timeout: timeout,
	}
	address := fmt.Sprintf("%s:%d", host.Address, host.Port)
	client, err := ssh.Dial("tcp", address, config)
	if err != nil {
		return Metric{}, nil, fmt.Errorf("SSH 连接失败: %w", err)
	}
	defer client.Close()
	session, err := client.NewSession()
	if err != nil {
		return Metric{}, nil, fmt.Errorf("创建 SSH 会话失败: %w", err)
	}
	defer session.Close()
	command := metricCommand
	if includeInfo {
		command = systemInfoCommand + metricCommand
	}
	output, err := session.CombinedOutput(command)
	if err != nil {
		return Metric{}, nil, fmt.Errorf("远端采集失败: %w", err)
	}
	metric, err := c.parseMetric(host.ID, string(output))
	if err != nil {
		return Metric{}, nil, err
	}
	var info *SystemInfo
	if includeInfo {
		parsed, parseErr := parseSystemInfo(string(output))
		if parseErr != nil {
			return Metric{}, nil, parseErr
		}
		info = &parsed
	}
	return metric, info, nil
}

func sshAuth(host Host) (ssh.AuthMethod, error) {
	if host.AuthType == "password" {
		if host.Password == nil {
			return nil, errors.New("缺少 SSH 密码")
		}
		return ssh.Password(*host.Password), nil
	}
	if host.PrivateKey == nil {
		return nil, errors.New("缺少 SSH 私钥")
	}
	var signer ssh.Signer
	var err error
	if host.Passphrase != nil && *host.Passphrase != "" {
		signer, err = ssh.ParsePrivateKeyWithPassphrase([]byte(*host.PrivateKey), []byte(*host.Passphrase))
	} else {
		signer, err = ssh.ParsePrivateKey([]byte(*host.PrivateKey))
	}
	if err != nil {
		return nil, fmt.Errorf("私钥格式或口令无效: %w", err)
	}
	return ssh.PublicKeys(signer), nil
}

func splitSections(output string) map[string][]string {
	sections := make(map[string][]string)
	current := ""
	for _, raw := range strings.Split(output, "\n") {
		line := strings.TrimSpace(raw)
		if strings.HasPrefix(line, "__") && strings.HasSuffix(line, "__") {
			current = strings.Trim(line, "_")
			sections[current] = []string{}
		} else if current != "" && line != "" {
			sections[current] = append(sections[current], raw)
		}
	}
	return sections
}

func parseUint(value string) (uint64, error) {
	return strconv.ParseUint(strings.TrimSpace(value), 10, 64)
}

func (c *Collector) parseMetric(hostID int, output string) (Metric, error) {
	sections := splitSections(output)
	stat := sections["STAT"]
	memLines := sections["MEM"]
	netLines := sections["NET"]
	diskLines := sections["DISK"]
	if len(stat) == 0 || len(memLines) == 0 || len(netLines) == 0 || len(diskLines) == 0 {
		return Metric{}, errors.New("无法解析远端资源数据")
	}
	cpuFields := strings.Fields(stat[0])
	if len(cpuFields) < 5 {
		return Metric{}, errors.New("无法解析 CPU 数据")
	}
	var total uint64
	var values []uint64
	for _, field := range cpuFields[1:] {
		value, err := parseUint(field)
		if err != nil {
			return Metric{}, err
		}
		values = append(values, value)
		total += value
	}
	idle := values[3]
	if len(values) > 4 {
		idle += values[4]
	}

	var memTotal, memAvailable uint64
	for _, line := range memLines {
		fields := strings.Fields(strings.ReplaceAll(line, ":", ""))
		if len(fields) < 2 {
			continue
		}
		value, err := parseUint(fields[1])
		if err != nil {
			return Metric{}, err
		}
		if fields[0] == "MemTotal" {
			memTotal = value
		}
		if fields[0] == "MemAvailable" {
			memAvailable = value
		}
	}
	if memTotal == 0 {
		return Metric{}, errors.New("无法解析内存数据")
	}
	var rx, tx uint64
	for _, line := range netLines {
		if !strings.Contains(line, ":") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if strings.TrimSpace(parts[0]) == "lo" {
			continue
		}
		fields := strings.Fields(parts[1])
		if len(fields) < 9 {
			continue
		}
		rxValue, rxErr := parseUint(fields[0])
		txValue, txErr := parseUint(fields[8])
		if rxErr != nil || txErr != nil {
			return Metric{}, errors.New("无法解析网络数据")
		}
		rx += rxValue
		tx += txValue
	}
	diskFields := strings.Fields(diskLines[len(diskLines)-1])
	if len(diskFields) < 5 {
		return Metric{}, fmt.Errorf("无法解析磁盘数据: %q", diskLines[len(diskLines)-1])
	}
	diskPercent, err := strconv.ParseFloat(strings.TrimSuffix(diskFields[len(diskFields)-2], "%"), 64)
	if err != nil {
		return Metric{}, err
	}

	now := time.Now()
	current := counters{total: total, idle: idle, rx: rx, tx: tx, timestamp: now}
	c.mu.Lock()
	previous, exists := c.previous[hostID]
	c.previous[hostID] = current
	c.mu.Unlock()

	cpuPercent, rxMbps, txMbps := 0.0, 0.0, 0.0
	if exists && total > previous.total {
		totalDelta := total - previous.total
		idleDelta := idle - previous.idle
		cpuPercent = (1 - float64(idleDelta)/float64(totalDelta)) * 100
		elapsed := now.Sub(previous.timestamp).Seconds()
		if elapsed > 0 {
			if rx >= previous.rx {
				rxMbps = float64(rx-previous.rx) * 8 / elapsed / 1e6
			}
			if tx >= previous.tx {
				txMbps = float64(tx-previous.tx) * 8 / elapsed / 1e6
			}
		}
	}
	return Metric{
		Timestamp:     float64(now.UnixNano()) / 1e9,
		CPUPercent:    round2(clamp(cpuPercent, 0, 100)),
		MemoryPercent: round2(clamp((1-float64(memAvailable)/float64(memTotal))*100, 0, 100)),
		NetworkRXMbps: round3(rxMbps), NetworkTXMbps: round3(txMbps),
		DiskPercent: round2(clamp(diskPercent, 0, 100)),
	}, nil
}

func parseSystemInfo(output string) (SystemInfo, error) {
	lines := splitSections(output)["SYSINFO"]
	if len(lines) < 4 {
		return SystemInfo{}, errors.New("无法解析远端主机基本信息")
	}
	cores, err := strconv.Atoi(strings.TrimSpace(lines[1]))
	if err != nil {
		return SystemInfo{}, err
	}
	memoryKB, err := strconv.ParseInt(strings.TrimSpace(lines[2]), 10, 64)
	if err != nil {
		return SystemInfo{}, err
	}
	diskKB, err := strconv.ParseInt(strings.TrimSpace(lines[3]), 10, 64)
	if err != nil {
		return SystemInfo{}, err
	}
	return SystemInfo{
		Hostname: strings.TrimSpace(lines[0]), CPUCores: cores,
		MemoryBytes: memoryKB * 1024, DiskBytes: diskKB * 1024,
	}, nil
}

func (c *Collector) forget(hostID int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.previous, hostID)
}

func clamp(value, low, high float64) float64 {
	if value < low {
		return low
	}
	if value > high {
		return high
	}
	return value
}
func round2(value float64) float64 { return math.Round(value*100) / 100 }
func round3(value float64) float64 { return math.Round(value*1000) / 1000 }

type HostStatus struct {
	State        string      `json:"state"`
	Error        *string     `json:"error"`
	LastSuccess  float64     `json:"last_success,omitempty"`
	LastAttempt  float64     `json:"last_attempt,omitempty"`
	LatencyMS    int64       `json:"latency_ms,omitempty"`
	RetryAt      float64     `json:"retry_at,omitempty"`
	FailureCount int         `json:"failure_count,omitempty"`
	SystemInfo   *SystemInfo `json:"system_info,omitempty"`
}

type Poller struct {
	store      *Store
	metrics    *MetricStore
	collector  *Collector
	mu         sync.Mutex
	statuses   map[int]HostStatus
	systemInfo map[int]SystemInfo
	infoTimes  map[int]time.Time
	failures   map[int]int
	nextRetry  map[int]time.Time
	hostLocks  map[int]*sync.Mutex
	semaphore  chan struct{}
	wake       chan struct{}
	stop       chan struct{}
	stopOnce   sync.Once
}

func newPoller(store *Store) *Poller {
	return &Poller{
		store: store, metrics: newMetricStore(), collector: newCollector(),
		statuses: make(map[int]HostStatus), systemInfo: make(map[int]SystemInfo),
		infoTimes: make(map[int]time.Time), failures: make(map[int]int),
		nextRetry: make(map[int]time.Time), hostLocks: make(map[int]*sync.Mutex),
		semaphore: make(chan struct{}, 10), wake: make(chan struct{}, 1), stop: make(chan struct{}),
	}
}

func (p *Poller) start() { go p.loop() }
func (p *Poller) close() { p.stopOnce.Do(func() { close(p.stop) }) }
func (p *Poller) wakeNow() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *Poller) loop() {
	for {
		select {
		case <-p.stop:
			return
		default:
		}
		p.collectAll()
		interval := time.Duration(p.store.settings().RefreshInterval) * time.Second
		timer := time.NewTimer(interval)
		select {
		case <-p.stop:
			timer.Stop()
			return
		case <-p.wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (p *Poller) collectAll() {
	hosts, _ := p.store.listHosts(true)
	var wg sync.WaitGroup
	maxJitter := time.Duration(p.store.settings().RefreshInterval) * time.Second / 10
	if maxJitter > 2*time.Second {
		maxJitter = 2 * time.Second
	}
	if maxJitter < 200*time.Millisecond {
		maxJitter = 200 * time.Millisecond
	}
	for _, host := range hosts {
		wg.Add(1)
		go func(h Host) {
			defer wg.Done()
			jitter := time.Duration(rand.Int63n(int64(maxJitter)))
			select {
			case <-p.stop:
				return
			case <-time.After(jitter):
			}
			p.collectHost(h.ID, false)
		}(host)
	}
	wg.Wait()
}

func (p *Poller) hostLock(id int) *sync.Mutex {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.hostLocks[id] == nil {
		p.hostLocks[id] = &sync.Mutex{}
	}
	return p.hostLocks[id]
}

func (p *Poller) collectHost(id int, force bool) HostStatus {
	p.mu.Lock()
	if !force && time.Now().Before(p.nextRetry[id]) {
		status := p.statuses[id]
		p.mu.Unlock()
		return status
	}
	p.mu.Unlock()

	lock := p.hostLock(id)
	lock.Lock()
	defer lock.Unlock()

	p.mu.Lock()
	if !force && time.Now().Before(p.nextRetry[id]) {
		status := p.statuses[id]
		p.mu.Unlock()
		return status
	}
	p.mu.Unlock()

	host, ok := p.store.getHost(id)
	if !ok {
		return HostStatus{State: "missing", Error: stringPointer("主机不存在")}
	}
	settings := p.store.settings()
	now := time.Now()
	p.mu.Lock()
	info, hasInfo := p.systemInfo[id]
	infoTime := p.infoTimes[id]
	current := p.statuses[id]
	current.State, current.Error = "collecting", nil
	if hasInfo {
		current.SystemInfo = &info
	}
	p.statuses[id] = current
	p.mu.Unlock()

	p.semaphore <- struct{}{}
	started := time.Now()
	metric, collectedInfo, err := p.collector.collect(
		host, time.Duration(settings.SSHTimeout)*time.Second,
		!hasInfo || now.Sub(infoTime) >= systemInfoTTL,
	)
	<-p.semaphore

	p.mu.Lock()
	defer p.mu.Unlock()
	if err == nil {
		if collectedInfo != nil {
			p.systemInfo[id] = *collectedInfo
			p.infoTimes[id] = time.Now()
			info = *collectedInfo
			hasInfo = true
		}
		p.metrics.add(id, metric, settings.HistoryMinutes)
		delete(p.failures, id)
		delete(p.nextRetry, id)
		status := HostStatus{
			State: "online", LastSuccess: metric.Timestamp,
			LatencyMS: time.Since(started).Milliseconds(),
		}
		if hasInfo {
			status.SystemInfo = &info
		}
		p.statuses[id] = status
		return status
	}
	failures := p.failures[id] + 1
	p.failures[id] = failures
	base := time.Duration(settings.RefreshInterval) * time.Second
	if base < 15*time.Second {
		base = 15 * time.Second
	}
	exponent := failures - 1
	if exponent > 5 {
		exponent = 5
	}
	backoff := base * time.Duration(1<<exponent)
	if backoff > maxBackoff {
		backoff = maxBackoff
	}
	retryAt := time.Now().Add(backoff)
	p.nextRetry[id] = retryAt
	message := err.Error()
	status := HostStatus{
		State: "error", Error: &message, LastAttempt: float64(time.Now().UnixNano()) / 1e9,
		RetryAt: float64(retryAt.UnixNano()) / 1e9, FailureCount: failures,
	}
	if hasInfo {
		status.SystemInfo = &info
	}
	p.statuses[id] = status
	if failures == 1 || failures == 2 || failures == 4 || failures%10 == 0 {
		log.Printf("host %d collection failed (%d failures, retry in %s): %v", id, failures, backoff, err)
	}
	return status
}

func (p *Poller) status(id int) HostStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	status, ok := p.statuses[id]
	if !ok {
		status = HostStatus{State: "pending"}
	}
	if info, exists := p.systemInfo[id]; exists {
		status.SystemInfo = &info
	}
	return status
}

func (p *Poller) removeHost(id int) {
	p.mu.Lock()
	delete(p.statuses, id)
	delete(p.systemInfo, id)
	delete(p.infoTimes, id)
	delete(p.failures, id)
	delete(p.nextRetry, id)
	delete(p.hostLocks, id)
	p.mu.Unlock()
	p.metrics.remove(id)
	p.collector.forget(id)
}

func (p *Poller) hostUpdated(id int) {
	p.mu.Lock()
	delete(p.systemInfo, id)
	delete(p.infoTimes, id)
	delete(p.failures, id)
	delete(p.nextRetry, id)
	p.statuses[id] = HostStatus{State: "pending"}
	p.mu.Unlock()
	p.collector.forget(id)
}

func stringPointer(value string) *string { return &value }

type App struct {
	store  *Store
	poller *Poller
}

func (a *App) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", a.index)
	mux.HandleFunc("GET /static/style.css", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
		w.Write([]byte(styleCSS))
	})
	mux.HandleFunc("GET /static/app.js", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
		w.Write([]byte(appJS))
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /api/hosts", a.listHosts)
	mux.HandleFunc("POST /api/hosts", a.createHost)
	mux.HandleFunc("PATCH /api/hosts/{id}", a.updateHost)
	mux.HandleFunc("DELETE /api/hosts/{id}", a.deleteHost)
	mux.HandleFunc("POST /api/hosts/reorder", a.reorderHosts)
	mux.HandleFunc("POST /api/hosts/{id}/refresh", a.refreshHost)
	mux.HandleFunc("GET /api/hosts/{id}/metrics", a.hostMetrics)
	mux.HandleFunc("GET /api/metrics", a.allMetrics)
	mux.HandleFunc("GET /api/settings", a.getSettings)
	mux.HandleFunc("PUT /api/settings", a.updateSettings)
	mux.HandleFunc("GET /api/dashboard", a.dashboard)
	return requestLogger(mux)
}

func (a *App) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(indexHTML))
}

func (a *App) listHosts(w http.ResponseWriter, r *http.Request) {
	_, hosts := a.store.listHosts(false)
	writeJSON(w, http.StatusOK, hosts)
}

func decodeJSON(r *http.Request, target any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func (a *App) createHost(w http.ResponseWriter, r *http.Request) {
	var host Host
	if err := decodeJSON(r, &host); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	created, err := a.store.createHost(host)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err)
		return
	}
	go a.poller.collectHost(created.ID, true)
	writeJSON(w, http.StatusCreated, created)
}

func parseID(r *http.Request) (int, error) {
	return strconv.Atoi(r.PathValue("id"))
}

func (a *App) updateHost(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var patch HostPatch
	if err := decodeJSON(r, &patch); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	updated, err := a.store.updateHost(id, patch)
	if errors.Is(err, os.ErrNotExist) {
		writeError(w, http.StatusNotFound, errors.New("主机不存在"))
		return
	}
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err)
		return
	}
	a.poller.hostUpdated(id)
	go a.poller.collectHost(id, true)
	writeJSON(w, http.StatusOK, updated)
}

func (a *App) deleteHost(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if !a.store.deleteHost(id) {
		writeError(w, http.StatusNotFound, errors.New("主机不存在"))
		return
	}
	a.poller.removeHost(id)
	w.WriteHeader(http.StatusNoContent)
}

func (a *App) reorderHosts(w http.ResponseWriter, r *http.Request) {
	var body struct {
		HostIDs []int `json:"host_ids"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := a.store.reorder(body.HostIDs); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *App) refreshHost(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if _, ok := a.store.getHost(id); !ok {
		writeError(w, http.StatusNotFound, errors.New("主机不存在"))
		return
	}
	writeJSON(w, http.StatusOK, a.poller.collectHost(id, true))
}

func parseMetricQuery(r *http.Request) (*float64, int, error) {
	var since *float64
	if raw := r.URL.Query().Get("since"); raw != "" {
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil || value < 0 {
			return nil, 0, errors.New("since 参数无效")
		}
		since = &value
	}
	limit := defaultChartPoints
	if raw := r.URL.Query().Get("max_points"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 2 || value > maxChartPoints {
			return nil, 0, errors.New("max_points 参数无效")
		}
		limit = value
	}
	return since, limit, nil
}

func (a *App) hostMetrics(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if _, ok := a.store.getHost(id); !ok {
		writeError(w, http.StatusNotFound, errors.New("主机不存在"))
		return
	}
	since, limit, err := parseMetricQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	settings := a.store.settings()
	writeJSON(w, http.StatusOK, a.poller.metrics.get(id, settings.HistoryMinutes, since, limit))
}

func (a *App) allMetrics(w http.ResponseWriter, r *http.Request) {
	since, limit, err := parseMetricQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	privateHosts, _ := a.store.listHosts(true)
	settings := a.store.settings()
	metrics := make(map[string][]Metric, len(privateHosts))
	for _, host := range privateHosts {
		metrics[strconv.Itoa(host.ID)] = a.poller.metrics.get(host.ID, settings.HistoryMinutes, since, limit)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"metrics": metrics, "server_time": float64(time.Now().UnixNano()) / 1e9,
	})
}

func (a *App) getSettings(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.store.settings())
}

func (a *App) updateSettings(w http.ResponseWriter, r *http.Request) {
	var settings Settings
	if err := decodeJSON(r, &settings); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := a.store.updateSettings(settings); err != nil {
		writeError(w, http.StatusUnprocessableEntity, err)
		return
	}
	a.poller.metrics.pruneAll(settings.HistoryMinutes)
	a.poller.wakeNow()
	writeJSON(w, http.StatusOK, settings)
}

func (a *App) dashboard(w http.ResponseWriter, r *http.Request) {
	_, hosts := a.store.listHosts(false)
	items := make([]map[string]any, 0, len(hosts))
	for _, host := range hosts {
		raw, _ := json.Marshal(host)
		var item map[string]any
		json.Unmarshal(raw, &item)
		item["status"] = a.poller.status(host.ID)
		items = append(items, item)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"settings": a.store.settings(), "hosts": items,
		"server_time": float64(time.Now().UnixNano()) / 1e9,
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"detail": err.Error()})
}

func requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		next.ServeHTTP(w, r)
		if !strings.HasPrefix(r.URL.Path, "/api/dashboard") && !strings.HasPrefix(r.URL.Path, "/api/metrics") {
			log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(started).Round(time.Millisecond))
		}
	})
}

func mustDecodeAsset(encoded string) string {
	compressed, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		panic(err)
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		panic(err)
	}
	defer reader.Close()
	raw, err := io.ReadAll(reader)
	if err != nil {
		panic(err)
	}
	return string(raw)
}

func runSelfTest() error {
	if !strings.Contains(indexHTML, "HostWatch") || len(styleCSS) < 100 || len(appJS) < 100 {
		return errors.New("embedded web assets are invalid")
	}
	sample := `__SYSINFO__
test-host
4
16777216
104857600
__STAT__
cpu 100 0 50 850 0 0 0 0
__MEM__
MemTotal: 1000 kB
MemAvailable: 400 kB
__NET__
lo: 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0
eth0: 1000 0 0 0 0 0 0 0 2000 0 0 0 0 0 0 0
__DISK__
/dev/sda1 100000 42000 58000 42% /
`
	collector := newCollector()
	metric, err := collector.parseMetric(1, sample)
	if err != nil || metric.MemoryPercent != 60 || metric.DiskPercent != 42 {
		return fmt.Errorf("metric parser self-test failed: %v", err)
	}
	info, err := parseSystemInfo(sample)
	if err != nil || info.CPUCores != 4 || info.Hostname != "test-host" {
		return fmt.Errorf("system info parser self-test failed: %v", err)
	}
	metrics := newMetricStore()
	now := float64(time.Now().UnixNano()) / 1e9
	for i := 0; i < 100; i++ {
		metrics.add(1, Metric{Timestamp: now + float64(i)}, 60)
	}
	if len(metrics.get(1, 60, nil, 10)) != 10 {
		return errors.New("metric downsampling self-test failed")
	}
	temp, err := os.MkdirTemp("", "hostwatch-self-test-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temp)
	store, err := newStore(filepath.Join(temp, "config.json"))
	if err != nil {
		return err
	}
	password := "plain"
	_, err = store.createHost(Host{Name: "test", Address: "127.0.0.1", Port: 22, Username: "root", AuthType: "password", Password: &password})
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(filepath.Join(temp, "config.json"))
	if err != nil || !strings.Contains(string(raw), "\"plain\"") {
		return errors.New("JSON store self-test failed")
	}
	return nil
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func main() {
	selfTest := flag.Bool("self-test", false, "run built-in checks and exit")
	versionFlag := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *versionFlag {
		fmt.Println(version)
		return
	}
	if *selfTest {
		if err := runSelfTest(); err != nil {
			log.Fatal(err)
		}
		fmt.Println("HostWatch self-test passed")
		return
	}

	dataDir := envOrDefault("HOSTWATCH_DATA_DIR", "data")
	store, err := newStore(filepath.Join(dataDir, "config.json"))
	if err != nil {
		log.Fatal(err)
	}
	poller := newPoller(store)
	poller.start()
	defer poller.close()

	address := envOrDefault("HOSTWATCH_HOST", "0.0.0.0") + ":" + envOrDefault("HOSTWATCH_PORT", "8000")
	server := &http.Server{
		Addr: address, Handler: (&App{store: store, poller: poller}).routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		log.Printf("HostWatch %s listening on http://%s", version, address)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}
