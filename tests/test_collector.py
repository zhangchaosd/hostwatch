import time

from hostwatch.collector import InMemoryMetricStore, PollingService, SSHCollector
from hostwatch.models import HostCreate, Metric
from hostwatch.storage import JsonStore


SAMPLE_1 = """__SYSINFO__
web-01
4
1000
100000
__STAT__
cpu  100 0 50 850 0 0 0 0 0 0
__MEM__
MemTotal:       1000 kB
MemAvailable:    400 kB
__NET__
Inter-|   Receive                                                |  Transmit
 face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed
    lo: 100 0 0 0 0 0 0 0 100 0 0 0 0 0 0 0
  eth0: 1000000 0 0 0 0 0 0 0 2000000 0 0 0 0 0 0 0
__DISK__
/dev/sda1 100000 42000 58000 42% /
"""

SAMPLE_2 = """__SYSINFO__
web-01
4
1000
100000
__STAT__
cpu  130 0 60 910 0 0 0 0 0 0
__MEM__
MemTotal:       1000 kB
MemAvailable:    250 kB
__NET__
Inter-|   Receive                                                |  Transmit
 face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed
    lo: 1000 0 0 0 0 0 0 0 1000 0 0 0 0 0 0 0
  eth0: 2000000 0 0 0 0 0 0 0 2500000 0 0 0 0 0 0 0
__DISK__
/dev/sda1 100000 43000 57000 43% /
"""


def test_parse_first_sample(monkeypatch):
    monkeypatch.setattr("hostwatch.collector.time.time", lambda: 100.0)
    metric = SSHCollector()._parse(1, SAMPLE_1)
    assert metric.cpu_percent == 0
    assert metric.memory_percent == 60
    assert metric.disk_percent == 42
    assert metric.network_rx_mbps == 0


def test_parse_system_info():
    info = SSHCollector()._parse_system_info(SAMPLE_1)
    assert info == {
        "hostname": "web-01",
        "cpu_cores": 4,
        "memory_bytes": 1000 * 1024,
        "disk_bytes": 100000 * 1024,
    }


def test_parse_counter_deltas(monkeypatch):
    timestamps = iter([100.0, 102.0])
    monkeypatch.setattr("hostwatch.collector.time.time", lambda: next(timestamps))
    collector = SSHCollector()
    collector._parse(1, SAMPLE_1)
    metric = collector._parse(1, SAMPLE_2)
    assert metric.cpu_percent == 40
    assert metric.memory_percent == 75
    assert metric.disk_percent == 43
    assert metric.network_rx_mbps == 4
    assert metric.network_tx_mbps == 2


def make_metric(timestamp: float) -> Metric:
    return Metric(
        timestamp=timestamp, cpu_percent=10, memory_percent=20,
        network_rx_mbps=1, network_tx_mbps=2, disk_percent=30,
    )


def test_memory_store_prunes_by_time_and_hard_limit():
    store = InMemoryMetricStore(max_points_per_host=3)
    for timestamp in (10, 20, 30, 40):
        store.add(1, make_metric(timestamp), history_minutes=5)
    assert [item["timestamp"] for item in store.get(1, 5, now=40)] == [20, 30, 40]

    assert store.get(1, 1, now=101) == []


def test_memory_store_remove_host():
    store = InMemoryMetricStore()
    store.add(1, make_metric(100), history_minutes=5)
    store.remove(1)
    assert store.get(1, 5, now=100) == []


def test_memory_store_enforces_fleet_wide_limit():
    store = InMemoryMetricStore(max_points_per_host=10, max_total_points=3)
    store.add(1, make_metric(1), history_minutes=5)
    store.add(2, make_metric(2), history_minutes=5)
    store.add(1, make_metric(3), history_minutes=5)
    store.add(2, make_metric(4), history_minutes=5)
    assert [item["timestamp"] for item in store.get(1, 5, now=4)] == [3]
    assert [item["timestamp"] for item in store.get(2, 5, now=4)] == [2, 4]


def test_memory_store_incremental_read_and_downsampling():
    store = InMemoryMetricStore(max_points_per_host=200)
    for timestamp in range(100):
        store.add(1, make_metric(timestamp), history_minutes=10)
    points = store.get(1, 10, now=100, since=50, max_points=10)
    assert len(points) == 10
    assert points[0]["timestamp"] == 51
    assert points[-1]["timestamp"] == 99


def make_polling_service(tmp_path):
    store = JsonStore(tmp_path / "config.json")
    host = store.create_host(HostCreate(
        name="Test", address="127.0.0.1", username="monitor",
        auth_type="password", password="plain",
    ))
    return PollingService(store), host["id"]


async def test_polling_backoff_and_force_refresh(tmp_path):
    service, host_id = make_polling_service(tmp_path)
    attempts = 0

    async def fail_collect(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        raise RuntimeError("offline")

    service.collector.collect = fail_collect
    first = await service.collect_host(host_id, force=True)
    assert first["failure_count"] == 1
    assert first["retry_at"] > time.time()
    await service.collect_host(host_id)
    assert attempts == 1
    second = await service.collect_host(host_id, force=True)
    assert second["failure_count"] == 2
    assert attempts == 2


async def test_system_info_is_cached_between_samples(tmp_path):
    service, host_id = make_polling_service(tmp_path)
    include_flags = []

    async def collect(*args, include_system_info=False, **kwargs):
        include_flags.append(include_system_info)
        metric = make_metric(time.time())
        info = {
            "hostname": "test-host", "cpu_cores": 4,
            "memory_bytes": 16 * 1024**3, "disk_bytes": 100 * 1024**3,
        } if include_system_info else None
        return metric, info

    service.collector.collect = collect
    await service.collect_host(host_id, force=True)
    await service.collect_host(host_id, force=True)
    assert include_flags == [True, False]
    assert service.get_status(host_id)["system_info"]["cpu_cores"] == 4
