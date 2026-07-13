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
	"net"
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
	currentConfigVersion = 2
	maxConfigBytes       = 8 << 20
	maxPointsPerHost     = 20000
	maxTotalPoints       = 100000
	defaultChartPoints   = 480
	maxChartPoints       = 1000
	systemInfoTTL        = 6 * time.Hour
	maxBackoff           = 5 * time.Minute
)

// version is overridden from a release tag with -ldflags "-X main.version=...".
var version = "0.3.0"

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

func defaultConfig() configFile {
	return configFile{Version: currentConfigVersion, NextHostID: 1, Settings: defaultSettings(), Hosts: []Host{}}
}

func cloneConfig(config configFile) configFile {
	cloned := config
	cloned.Hosts = append([]Host(nil), config.Hosts...)
	return cloned
}

func decodeConfig(raw []byte) (configFile, bool, error) {
	if len(raw) > maxConfigBytes {
		return configFile{}, false, fmt.Errorf("配置文件超过 %d 字节限制", maxConfigBytes)
	}
	var config configFile
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return configFile{}, false, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("配置文件包含多个 JSON 值")
		}
		return configFile{}, false, err
	}
	migrated := false
	if config.Version == 0 {
		config.Version = 1
		migrated = true
	}
	if config.Version < 1 || config.Version > currentConfigVersion {
		return configFile{}, false, fmt.Errorf("不支持的配置版本 %d", config.Version)
	}
	for config.Version < currentConfigVersion {
		switch config.Version {
		case 1:
			config.Version = 2
			migrated = true
		default:
			return configFile{}, false, fmt.Errorf("无法迁移配置版本 %d", config.Version)
		}
	}
	if config.Settings.RefreshInterval == 0 {
		config.Settings = defaultSettings()
		migrated = true
	}
	if err := config.Settings.validate(); err != nil {
		return configFile{}, false, err
	}
	if config.Hosts == nil {
		config.Hosts = []Host{}
		migrated = true
	}
	sort.SliceStable(config.Hosts, func(i, j int) bool {
		if config.Hosts[i].Position == config.Hosts[j].Position {
			return config.Hosts[i].ID < config.Hosts[j].ID
		}
		return config.Hosts[i].Position < config.Hosts[j].Position
	})
	seenIDs := make(map[int]struct{}, len(config.Hosts))
	maxID := 0
	for i := range config.Hosts {
		host := &config.Hosts[i]
		if host.ID < 1 {
			return configFile{}, false, errors.New("主机 ID 必须为正整数")
		}
		if _, exists := seenIDs[host.ID]; exists {
			return configFile{}, false, fmt.Errorf("主机 ID %d 重复", host.ID)
		}
		seenIDs[host.ID] = struct{}{}
		if host.Port == 0 {
			host.Port = 22
			migrated = true
		}
		host.Name = strings.TrimSpace(host.Name)
		host.Address = strings.TrimSpace(host.Address)
		host.Username = strings.TrimSpace(host.Username)
		if err := validateHost(*host); err != nil {
			return configFile{}, false, fmt.Errorf("主机 %d 配置无效: %w", host.ID, err)
		}
		if host.Position != i {
			host.Position = i
			migrated = true
		}
		if host.ID > maxID {
			maxID = host.ID
		}
	}
	if config.NextHostID <= maxID {
		config.NextHostID = maxID + 1
		migrated = true
	}
	if config.NextHostID < 1 {
		config.NextHostID = 1
		migrated = true
	}
	return config, migrated, nil
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
		s.data = defaultConfig()
		return s.writePrimaryLocked()
	}
	if err != nil {
		return err
	}
	config, migrated, primaryErr := decodeConfig(raw)
	if primaryErr != nil {
		backupRaw, backupReadErr := os.ReadFile(s.path + ".bak")
		if backupReadErr != nil {
			return fmt.Errorf("读取 config.json 失败: %w；备份不可用: %v", primaryErr, backupReadErr)
		}
		backup, _, backupErr := decodeConfig(backupRaw)
		if backupErr != nil {
			return fmt.Errorf("读取 config.json 失败: %w；备份损坏: %v", primaryErr, backupErr)
		}
		s.data = backup
		log.Printf("config.json 损坏，已从 %s 恢复", s.path+".bak")
		return s.writePrimaryLocked()
	}
	s.data = config
	if migrated {
		return s.saveLocked()
	}
	return nil
}

func writeAtomic(path string, raw []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (s *Store) marshalLocked() ([]byte, error) {
	raw, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return nil, err
	}
	raw = append(raw, '\n')
	return raw, nil
}

func (s *Store) writePrimaryLocked() error {
	raw, err := s.marshalLocked()
	if err != nil {
		return err
	}
	return writeAtomic(s.path, raw)
}

func (s *Store) saveLocked() error {
	if current, err := os.ReadFile(s.path); err == nil {
		if _, _, configErr := decodeConfig(current); configErr == nil {
			if err := writeAtomic(s.path+".bak", current); err != nil {
				return fmt.Errorf("写入配置备份失败: %w", err)
			}
		}
	}
	return s.writePrimaryLocked()
}

func (s *Store) exportConfig() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.marshalLocked()
}

func (s *Store) importConfig(raw []byte) error {
	config, _, err := decodeConfig(raw)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	previous := cloneConfig(s.data)
	s.data = config
	if err := s.saveLocked(); err != nil {
		s.data = previous
		return err
	}
	return nil
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
	previous := cloneConfig(s.data)
	h.Name = strings.TrimSpace(h.Name)
	h.Address = strings.TrimSpace(h.Address)
	h.Username = strings.TrimSpace(h.Username)
	h.ID = s.data.NextHostID
	s.data.NextHostID++
	h.Position = len(s.data.Hosts)
	h.CreatedAt = float64(time.Now().UnixNano()) / 1e9
	s.data.Hosts = append(s.data.Hosts, h)
	if err := s.saveLocked(); err != nil {
		s.data = previous
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

func (s *Store) deleteHost(id int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	previous := cloneConfig(s.data)
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
		return false, nil
	}
	s.data.Hosts = out
	sort.Slice(s.data.Hosts, func(i, j int) bool { return s.data.Hosts[i].Position < s.data.Hosts[j].Position })
	for i := range s.data.Hosts {
		s.data.Hosts[i].Position = i
	}
	if err := s.saveLocked(); err != nil {
		s.data = previous
		return false, err
	}
	return true, nil
}

func (s *Store) reorder(ids []int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	previous := cloneConfig(s.data)
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
	if err := s.saveLocked(); err != nil {
		s.data = previous
		return err
	}
	return nil
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
	previous := s.data.Settings
	s.data.Settings = settings
	if err := s.saveLocked(); err != nil {
		s.data.Settings = previous
		return err
	}
	return nil
}

type Metric struct {
	Timestamp     float64 `json:"timestamp"`
	CPUPercent    float64 `json:"cpu_percent"`
	MemoryPercent float64 `json:"memory_percent"`
	NetworkRXMbps float64 `json:"network_rx_mbps"`
	NetworkTXMbps float64 `json:"network_tx_mbps"`
	DiskPercent   float64 `json:"disk_percent"`
}

type metricRing struct {
	points []Metric
	start  int
	size   int
}

func (r *metricRing) at(index int) Metric {
	return r.points[(r.start+index)%len(r.points)]
}

func (r *metricRing) grow(limit int) {
	capacity := len(r.points) * 2
	if capacity == 0 {
		capacity = 64
	}
	if capacity > limit {
		capacity = limit
	}
	next := make([]Metric, capacity)
	for i := 0; i < r.size; i++ {
		next[i] = r.at(i)
	}
	r.points = next
	r.start = 0
}

func (r *metricRing) append(point Metric, limit int) bool {
	if r.size < limit {
		if r.size == len(r.points) {
			r.grow(limit)
		}
		index := (r.start + r.size) % len(r.points)
		r.points[index] = point
		r.size++
		return false
	}
	r.points[r.start] = point
	r.start = (r.start + 1) % len(r.points)
	return true
}

func (r *metricRing) oldest() (Metric, bool) {
	if r.size == 0 {
		return Metric{}, false
	}
	return r.points[r.start], true
}

func (r *metricRing) popOldest() bool {
	if r.size == 0 {
		return false
	}
	r.start = (r.start + 1) % len(r.points)
	r.size--
	if r.size == 0 {
		r.start = 0
	}
	return true
}

func (r *metricRing) prune(cutoff float64) int {
	removed := 0
	for r.size > 0 {
		oldest, _ := r.oldest()
		if oldest.Timestamp >= cutoff {
			break
		}
		r.popOldest()
		removed++
	}
	return removed
}

type MetricStore struct {
	mu           sync.Mutex
	data         map[int]*metricRing
	total        int
	perHostLimit int
	totalLimit   int
}

func newMetricStore() *MetricStore {
	return newMetricStoreWithLimits(maxPointsPerHost, maxTotalPoints)
}

func newMetricStoreWithLimits(perHostLimit, totalLimit int) *MetricStore {
	if perHostLimit < 1 {
		perHostLimit = 1
	}
	if totalLimit < 1 {
		totalLimit = 1
	}
	return &MetricStore{
		data: make(map[int]*metricRing), perHostLimit: perHostLimit, totalLimit: totalLimit,
	}
}

func (m *MetricStore) add(hostID int, metric Metric, historyMinutes int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneHostLocked(hostID, float64(time.Now().UnixNano())/1e9-float64(historyMinutes*60))
	ring := m.data[hostID]
	if ring == nil {
		ring = &metricRing{}
		m.data[hostID] = ring
	}
	if !ring.append(metric, m.perHostLimit) {
		m.total++
	}
	for m.total > m.totalLimit {
		oldestID, oldestTime, found := 0, math.MaxFloat64, false
		for id, candidate := range m.data {
			if oldest, ok := candidate.oldest(); ok && oldest.Timestamp < oldestTime {
				oldestID, oldestTime, found = id, oldest.Timestamp, true
			}
		}
		if !found {
			break
		}
		if m.data[oldestID].popOldest() {
			m.total--
		}
		if m.data[oldestID].size == 0 {
			delete(m.data, oldestID)
		}
	}
}

func (m *MetricStore) pruneHostLocked(hostID int, cutoff float64) {
	ring := m.data[hostID]
	if ring == nil {
		return
	}
	m.total -= ring.prune(cutoff)
	if ring.size == 0 {
		delete(m.data, hostID)
	}
}

func (m *MetricStore) get(hostID int, historyMinutes int, since *float64, limit int) []Metric {
	m.mu.Lock()
	defer m.mu.Unlock()
	cutoff := float64(time.Now().UnixNano())/1e9 - float64(historyMinutes*60)
	m.pruneHostLocked(hostID, cutoff)
	ring := m.data[hostID]
	if ring == nil {
		return []Metric{}
	}
	selected := make([]Metric, 0, ring.size)
	for i := 0; i < ring.size; i++ {
		point := ring.at(i)
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
	if ring := m.data[hostID]; ring != nil {
		m.total -= ring.size
	}
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

func (m *MetricStore) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[int]*metricRing)
	m.total = 0
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

func (c *Collector) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.previous = make(map[int]counters)
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
	generation uint64
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
	generation := p.generation
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
	if generation != p.generation {
		return HostStatus{State: "pending"}
	}
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
	backoff := retryBackoff(settings.RefreshInterval, failures)
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

func retryBackoff(refreshInterval, failures int) time.Duration {
	base := time.Duration(refreshInterval) * time.Second
	if base < 15*time.Second {
		base = 15 * time.Second
	}
	if failures < 1 {
		failures = 1
	}
	exponent := failures - 1
	if exponent > 5 {
		exponent = 5
	}
	backoff := base * time.Duration(1<<exponent)
	if backoff > maxBackoff {
		return maxBackoff
	}
	return backoff
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

func (p *Poller) reset() {
	p.mu.Lock()
	p.generation++
	p.statuses = make(map[int]HostStatus)
	p.systemInfo = make(map[int]SystemInfo)
	p.infoTimes = make(map[int]time.Time)
	p.failures = make(map[int]int)
	p.nextRetry = make(map[int]time.Time)
	p.hostLocks = make(map[int]*sync.Mutex)
	p.mu.Unlock()
	p.metrics.reset()
	p.collector.reset()
	p.wakeNow()
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
	mux.HandleFunc("GET /api/config", a.exportConfig)
	mux.HandleFunc("PUT /api/config", a.importConfig)
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
	deleted, err := a.store.deleteHost(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if !deleted {
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

func (a *App) exportConfig(w http.ResponseWriter, r *http.Request) {
	raw, err := a.store.exportConfig()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="hostwatch-config.json"`)
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(raw); err != nil {
		log.Printf("write config export: %v", err)
	}
}

func (a *App) importConfig(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxConfigBytes+1))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if len(raw) > maxConfigBytes {
		writeError(w, http.StatusRequestEntityTooLarge, errors.New("配置文件过大"))
		return
	}
	if err := a.store.importConfig(raw); err != nil {
		writeError(w, http.StatusUnprocessableEntity, err)
		return
	}
	a.poller.reset()
	_, hosts := a.store.listHosts(false)
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok", "host_count": len(hosts), "settings": a.store.settings(),
	})
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

func listenAddress() string {
	return net.JoinHostPort(os.Getenv("HOSTWATCH_HOST"), envOrDefault("HOSTWATCH_PORT", "8000"))
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

	address := listenAddress()
	server := &http.Server{
		Addr: address, Handler: (&App{store: store, poller: poller}).routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		displayAddress := address
		if strings.HasPrefix(displayAddress, ":") {
			displayAddress = "[::]" + displayAddress
		}
		log.Printf("HostWatch %s listening on http://%s", version, displayAddress)
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
