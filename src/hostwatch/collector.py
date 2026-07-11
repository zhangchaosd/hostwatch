from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

import asyncssh

from .models import Metric
from .storage import JsonStore

logger = logging.getLogger(__name__)
MAX_POINTS_PER_HOST = 20_000
MAX_TOTAL_POINTS = 100_000
DEFAULT_CHART_POINTS = 480
MAX_CHART_POINTS = 1_000
SYSTEM_INFO_TTL = 6 * 60 * 60
MAX_BACKOFF_SECONDS = 5 * 60
METRIC_FIELDS = (
    "timestamp", "cpu_percent", "memory_percent", "network_rx_mbps",
    "network_tx_mbps", "disk_percent",
)
StoredMetric = tuple[float, float, float, float, float, float]

SYSTEM_INFO_COMMAND = r"""
printf '__SYSINFO__\n'
hostname
getconf _NPROCESSORS_ONLN 2>/dev/null || nproc
awk '/^MemTotal:/ {print $2}' /proc/meminfo
df -Pk / | tail -n 1 | awk '{print $2}'
"""

METRIC_COMMAND = r"""
printf '__STAT__\n'
head -n 1 /proc/stat
printf '__MEM__\n'
awk '/^(MemTotal|MemAvailable):/ {print}' /proc/meminfo
printf '__NET__\n'
cat /proc/net/dev
printf '__DISK__\n'
df -P / | tail -n 1
"""


@dataclass
class PreviousCounters:
    cpu_total: int
    cpu_idle: int
    rx_bytes: int
    tx_bytes: int
    timestamp: float


class InMemoryMetricStore:
    """Time-windowed metric history which is never written to disk."""

    def __init__(
        self,
        max_points_per_host: int = MAX_POINTS_PER_HOST,
        max_total_points: int = MAX_TOTAL_POINTS,
    ) -> None:
        self._max_points_per_host = max_points_per_host
        self._max_total_points = max_total_points
        self._point_count = 0
        # Compact tuples use much less memory than keeping Pydantic objects or dicts.
        self._metrics: dict[int, deque[StoredMetric]] = {}

    def add(self, host_id: int, metric: Metric, history_minutes: int) -> None:
        self._prune_host(host_id, history_minutes, now=metric.timestamp)
        history = self._metrics.get(host_id)
        if history is None:
            history = deque()
            self._metrics[host_id] = history
        if len(history) >= self._max_points_per_host:
            history.popleft()
            self._point_count -= 1
        history.append(tuple(getattr(metric, field) for field in METRIC_FIELDS))  # type: ignore[arg-type]
        self._point_count += 1
        self._enforce_global_limit()

    def get(
        self,
        host_id: int,
        history_minutes: int,
        now: float | None = None,
        since: float | None = None,
        max_points: int = DEFAULT_CHART_POINTS,
    ) -> list[dict[str, Any]]:
        self._prune_host(host_id, history_minutes, now=now)
        history = self._metrics.get(host_id, ())
        selected = [metric for metric in history if since is None or metric[0] > since]
        selected = self._downsample(selected, max_points)
        return [dict(zip(METRIC_FIELDS, metric, strict=True)) for metric in selected]

    @staticmethod
    def _downsample(metrics: list[StoredMetric], max_points: int) -> list[StoredMetric]:
        limit = max(2, min(MAX_CHART_POINTS, max_points))
        if len(metrics) <= limit:
            return metrics
        last_index = len(metrics) - 1
        indexes = {
            round(index * last_index / (limit - 1))
            for index in range(limit)
        }
        return [metrics[index] for index in sorted(indexes)]

    def prune_all(self, history_minutes: int, now: float | None = None) -> None:
        current_time = time.time() if now is None else now
        for host_id in list(self._metrics):
            self._prune_host(host_id, history_minutes, now=current_time)

    def remove(self, host_id: int) -> None:
        history = self._metrics.pop(host_id, None)
        if history:
            self._point_count -= len(history)

    def _prune_host(self, host_id: int, history_minutes: int, now: float | None = None) -> None:
        history = self._metrics.get(host_id)
        if not history:
            return
        current_time = time.time() if now is None else now
        cutoff = current_time - history_minutes * 60
        while history and history[0][0] < cutoff:
            history.popleft()
            self._point_count -= 1
        if not history:
            self._metrics.pop(host_id, None)

    def _enforce_global_limit(self) -> None:
        while self._point_count > self._max_total_points and self._metrics:
            oldest_host_id = min(
                self._metrics,
                key=lambda host_id: self._metrics[host_id][0][0],
            )
            history = self._metrics[oldest_host_id]
            history.popleft()
            self._point_count -= 1
            if not history:
                self._metrics.pop(oldest_host_id, None)


class SSHCollector:
    def __init__(self) -> None:
        self._previous: dict[int, PreviousCounters] = {}

    async def collect(
        self,
        host: dict[str, Any],
        timeout: int,
        include_system_info: bool = False,
    ) -> tuple[Metric, dict[str, Any] | None]:
        connect_args: dict[str, Any] = {
            "host": host["address"],
            "port": host["port"],
            "username": host["username"],
            "known_hosts": None,
            "login_timeout": timeout,
        }
        if host["auth_type"] == "password":
            connect_args["password"] = host["password"]
            connect_args["client_keys"] = None
        else:
            try:
                key = asyncssh.import_private_key(host["private_key"], host.get("passphrase"))
            except (KeyError, asyncssh.KeyImportError) as exc:
                raise RuntimeError(f"私钥格式或口令无效：{exc}") from exc
            connect_args["client_keys"] = [key]

        try:
            async with asyncio.timeout(timeout + 2):
                async with asyncssh.connect(**connect_args) as connection:
                    command = (SYSTEM_INFO_COMMAND if include_system_info else "") + METRIC_COMMAND
                    result = await connection.run(command, check=True, timeout=timeout)
        except TimeoutError as exc:
            raise RuntimeError(f"连接或采集超时（{timeout} 秒）") from exc
        except asyncssh.HostKeyNotVerifiable as exc:
            raise RuntimeError("主机密钥校验失败，请先将主机加入运行用户的 known_hosts") from exc
        except asyncssh.PermissionDenied as exc:
            raise RuntimeError("SSH 认证失败，请检查用户名和凭据") from exc
        except (OSError, asyncssh.Error) as exc:
            raise RuntimeError(f"SSH 连接失败：{exc}") from exc

        output = str(result.stdout)
        system_info = self._parse_system_info(output) if include_system_info else None
        return self._parse(host["id"], output), system_info

    def _parse(self, host_id: int, output: str) -> Metric:
        sections = self._sections(output)

        try:
            cpu_parts = sections["STAT"][0].split()[1:]
            cpu_values = [int(value) for value in cpu_parts]
            cpu_total = sum(cpu_values)
            cpu_idle = cpu_values[3] + (cpu_values[4] if len(cpu_values) > 4 else 0)

            mem = {}
            for line in sections["MEM"]:
                key, value, *_ = line.replace(":", "").split()
                mem[key] = int(value)
            memory_percent = (1 - mem["MemAvailable"] / mem["MemTotal"]) * 100

            rx_bytes = 0
            tx_bytes = 0
            for line in sections["NET"]:
                if ":" not in line:
                    continue
                interface, values = line.split(":", 1)
                if interface.strip() == "lo":
                    continue
                fields = values.split()
                rx_bytes += int(fields[0])
                tx_bytes += int(fields[8])

            disk_fields = sections["DISK"][-1].split()
            disk_percent = float(disk_fields[-2].rstrip("%"))
        except (KeyError, IndexError, ValueError, ZeroDivisionError) as exc:
            raise RuntimeError("无法解析远端资源数据，请确认目标是标准 Linux 系统") from exc

        now = time.time()
        previous = self._previous.get(host_id)
        cpu_percent = 0.0
        rx_mbps = 0.0
        tx_mbps = 0.0
        if previous:
            total_delta = cpu_total - previous.cpu_total
            idle_delta = cpu_idle - previous.cpu_idle
            if total_delta > 0:
                cpu_percent = (1 - idle_delta / total_delta) * 100
            elapsed = now - previous.timestamp
            if elapsed > 0:
                rx_mbps = max(0, rx_bytes - previous.rx_bytes) * 8 / elapsed / 1_000_000
                tx_mbps = max(0, tx_bytes - previous.tx_bytes) * 8 / elapsed / 1_000_000

        self._previous[host_id] = PreviousCounters(
            cpu_total=cpu_total, cpu_idle=cpu_idle, rx_bytes=rx_bytes,
            tx_bytes=tx_bytes, timestamp=now,
        )
        return Metric(
            timestamp=now,
            cpu_percent=round(min(100, max(0, cpu_percent)), 2),
            memory_percent=round(min(100, max(0, memory_percent)), 2),
            network_rx_mbps=round(rx_mbps, 3),
            network_tx_mbps=round(tx_mbps, 3),
            disk_percent=round(min(100, max(0, disk_percent)), 2),
        )

    @staticmethod
    def _sections(output: str) -> dict[str, list[str]]:
        sections: dict[str, list[str]] = {}
        current: str | None = None
        for raw_line in output.splitlines():
            line = raw_line.strip()
            if line.startswith("__") and line.endswith("__"):
                current = line.strip("_")
                sections[current] = []
            elif current:
                sections[current].append(raw_line)
        return sections

    def _parse_system_info(self, output: str) -> dict[str, Any]:
        sections = self._sections(output)
        try:
            hostname = sections["SYSINFO"][0].strip()
            cpu_cores = int(sections["SYSINFO"][1].strip())
            mem_total_kib = int(sections["SYSINFO"][2].strip())
            disk_total_kib = int(sections["SYSINFO"][3].strip())
        except (KeyError, IndexError, ValueError) as exc:
            raise RuntimeError("无法解析远端主机基本信息") from exc
        return {
            "hostname": hostname,
            "cpu_cores": cpu_cores,
            "memory_bytes": mem_total_kib * 1024,
            "disk_bytes": disk_total_kib * 1024,
        }

    def forget(self, host_id: int) -> None:
        self._previous.pop(host_id, None)


class PollingService:
    def __init__(self, store: JsonStore) -> None:
        self.store = store
        self.collector = SSHCollector()
        self.metrics = InMemoryMetricStore()
        self.statuses: dict[int, dict[str, Any]] = {}
        self._task: asyncio.Task[None] | None = None
        self._wake = asyncio.Event()
        self._stop = asyncio.Event()
        self._semaphore = asyncio.Semaphore(10)
        self._host_locks: dict[int, asyncio.Lock] = {}
        self._system_info: dict[int, dict[str, Any]] = {}
        self._system_info_times: dict[int, float] = {}
        self._failure_counts: dict[int, int] = {}
        self._next_retry_at: dict[int, float] = {}

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="hostwatch-poller")

    async def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._task:
            await self._task
            self._task = None

    def wake(self) -> None:
        self._wake.set()

    async def _run(self) -> None:
        while not self._stop.is_set():
            await self.collect_all()
            interval = self.store.get_settings().refresh_interval
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=interval)
            except TimeoutError:
                pass

    async def collect_all(self) -> None:
        hosts = self.store.list_hosts(include_credentials=True)
        if not hosts:
            return
        interval = self.store.get_settings().refresh_interval
        max_jitter = min(2.0, max(0.2, interval * 0.1))
        await asyncio.gather(*(
            self.collect_host(
                host["id"], host=host, jitter=random.uniform(0, max_jitter)
            )
            for host in hosts
        ))

    async def collect_host(
        self,
        host_id: int,
        host: dict[str, Any] | None = None,
        force: bool = False,
        jitter: float = 0,
    ) -> dict[str, Any]:
        now = time.time()
        if not force and now < self._next_retry_at.get(host_id, 0):
            return self.get_status(host_id)
        if jitter > 0:
            await asyncio.sleep(jitter)
        lock = self._host_locks.setdefault(host_id, asyncio.Lock())
        async with lock:
            if not force and time.time() < self._next_retry_at.get(host_id, 0):
                return self.get_status(host_id)
            return await self._collect_host_unlocked(host_id, host)

    async def _collect_host_unlocked(
        self, host_id: int, host: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        if host is None:
            host = self.store.get_host(host_id, include_credentials=True)
        if host is None:
            return {"state": "missing", "error": "主机不存在"}
        settings = self.store.get_settings()
        started = time.monotonic()
        now = time.time()
        include_system_info = (
            host_id not in self._system_info
            or now - self._system_info_times.get(host_id, 0) >= SYSTEM_INFO_TTL
        )
        self.statuses[host_id] = {
            **self.statuses.get(host_id, {}), "state": "collecting", "error": None,
        }
        async with self._semaphore:
            try:
                metric, collected_system_info = await self.collector.collect(
                    host,
                    timeout=settings.ssh_timeout,
                    include_system_info=include_system_info,
                )
                if self.store.get_host(host_id) is None:
                    return {"state": "missing", "error": "主机已删除"}
                if collected_system_info is not None:
                    self._system_info[host_id] = collected_system_info
                    self._system_info_times[host_id] = metric.timestamp
                self.metrics.add(host_id, metric, settings.history_minutes)
                self._failure_counts.pop(host_id, None)
                self._next_retry_at.pop(host_id, None)
                status = {
                    "state": "online", "error": None, "last_success": metric.timestamp,
                    "latency_ms": round((time.monotonic() - started) * 1000),
                    "system_info": self._system_info.get(host_id),
                }
            except Exception as exc:  # Keep one unreachable host from stopping the poller.
                failures = self._failure_counts.get(host_id, 0) + 1
                self._failure_counts[host_id] = failures
                base_interval = max(15, settings.refresh_interval)
                backoff = min(MAX_BACKOFF_SECONDS, base_interval * (2 ** min(failures - 1, 5)))
                retry_at = time.time() + backoff
                self._next_retry_at[host_id] = retry_at
                if failures in {1, 2, 4} or failures % 10 == 0:
                    logger.warning(
                        "Failed to collect host %s (%s failures, retry in %ss): %s",
                        host_id, failures, backoff, exc,
                    )
                status = {
                    **self.statuses.get(host_id, {}), "state": "error", "error": str(exc),
                    "last_attempt": time.time(), "retry_at": retry_at,
                    "failure_count": failures,
                    "system_info": self._system_info.get(host_id),
                }
            self.statuses[host_id] = status
            return status

    def remove_host(self, host_id: int) -> None:
        self.statuses.pop(host_id, None)
        self.collector.forget(host_id)
        self.metrics.remove(host_id)
        self._system_info.pop(host_id, None)
        self._system_info_times.pop(host_id, None)
        self._failure_counts.pop(host_id, None)
        self._next_retry_at.pop(host_id, None)
        lock = self._host_locks.get(host_id)
        if lock is not None and not lock.locked():
            self._host_locks.pop(host_id, None)

    def get_status(self, host_id: int) -> dict[str, Any]:
        status = dict(self.statuses.get(host_id, {"state": "pending", "error": None}))
        if host_id in self._system_info:
            status["system_info"] = self._system_info[host_id]
        return status

    def get_metrics(
        self,
        host_id: int,
        history_minutes: int | None = None,
        since: float | None = None,
        max_points: int = DEFAULT_CHART_POINTS,
    ) -> list[dict[str, Any]]:
        retention = history_minutes or self.store.get_settings().history_minutes
        return self.metrics.get(
            host_id, retention, since=since, max_points=max_points
        )

    def get_all_metrics(
        self,
        host_ids: list[int],
        history_minutes: int,
        since: float | None,
        max_points: int,
    ) -> dict[str, list[dict[str, Any]]]:
        return {
            str(host_id): self.get_metrics(
                host_id, history_minutes, since=since, max_points=max_points
            )
            for host_id in host_ids
        }

    def host_updated(self, host_id: int) -> None:
        self._system_info.pop(host_id, None)
        self._system_info_times.pop(host_id, None)
        self._failure_counts.pop(host_id, None)
        self._next_retry_at.pop(host_id, None)
        self.collector.forget(host_id)
        self.statuses[host_id] = {"state": "pending", "error": None}

    def update_retention(self, history_minutes: int) -> None:
        self.metrics.prune_all(history_minutes)
