from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any

from .models import HostCreate, HostUpdate, Settings, SettingsUpdate


DEFAULT_SETTINGS = {
    "refresh_interval": 15,
    "history_minutes": 60,
    "ssh_timeout": 10,
}


class JsonStore:
    """Plain-text JSON persistence for host configuration and settings."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._data = self._load()

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            data = {
                "version": 1,
                "next_host_id": 1,
                "settings": dict(DEFAULT_SETTINGS),
                "hosts": [],
            }
            self._write(data)
            return data
        data = json.loads(self.path.read_text(encoding="utf-8"))
        data.setdefault("version", 1)
        data.setdefault("next_host_id", 1)
        data.setdefault("settings", {})
        data["settings"] = {**DEFAULT_SETTINGS, **data["settings"]}
        data.setdefault("hosts", [])
        return data

    def _write(self, data: dict[str, Any]) -> None:
        temporary_path = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary_path, self.path)

    def _save(self) -> None:
        self._write(self._data)

    @staticmethod
    def _public_host(host: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": host["id"],
            "name": host["name"],
            "address": host["address"],
            "port": host["port"],
            "username": host["username"],
            "auth_type": host["auth_type"],
            "position": host["position"],
            "has_password": bool(host.get("password")),
            "has_private_key": bool(host.get("private_key")),
        }

    def list_hosts(self, include_credentials: bool = False) -> list[dict[str, Any]]:
        with self._lock:
            hosts = sorted(self._data["hosts"], key=lambda host: (host["position"], host["id"]))
            if include_credentials:
                return [dict(host) for host in hosts]
            return [self._public_host(host) for host in hosts]

    def get_host(self, host_id: int, include_credentials: bool = False) -> dict[str, Any] | None:
        with self._lock:
            host = next((item for item in self._data["hosts"] if item["id"] == host_id), None)
            if host is None:
                return None
            return dict(host) if include_credentials else self._public_host(host)

    def create_host(self, data: HostCreate) -> dict[str, Any]:
        with self._lock:
            host_id = self._data["next_host_id"]
            self._data["next_host_id"] = host_id + 1
            host = {
                "id": host_id,
                "name": data.name,
                "address": data.address,
                "port": data.port,
                "username": data.username,
                "auth_type": data.auth_type,
                "password": data.password,
                "private_key": data.private_key,
                "passphrase": data.passphrase,
                "position": len(self._data["hosts"]),
                "created_at": time.time(),
            }
            self._data["hosts"].append(host)
            self._save()
            return self._public_host(host)

    def update_host(self, host_id: int, data: HostUpdate) -> dict[str, Any] | None:
        with self._lock:
            host = next((item for item in self._data["hosts"] if item["id"] == host_id), None)
            if host is None:
                return None
            values = data.model_dump(exclude_unset=True)
            effective_auth_type = values.get("auth_type", host["auth_type"])
            effective_password = values.get("password", host.get("password"))
            effective_private_key = values.get("private_key", host.get("private_key"))
            if effective_auth_type == "password" and not effective_password:
                raise ValueError("密码登录必须保留或提供密码")
            if effective_auth_type == "key" and not effective_private_key:
                raise ValueError("密钥登录必须保留或提供私钥")
            host.update(values)
            self._save()
            return self._public_host(host)

    def delete_host(self, host_id: int) -> bool:
        with self._lock:
            original_length = len(self._data["hosts"])
            self._data["hosts"] = [
                host for host in self._data["hosts"] if host["id"] != host_id
            ]
            if len(self._data["hosts"]) == original_length:
                return False
            self._normalize_positions()
            self._save()
            return True

    def reorder_hosts(self, host_ids: list[int]) -> bool:
        with self._lock:
            existing_ids = [host["id"] for host in self._data["hosts"]]
            if len(host_ids) != len(existing_ids) or set(host_ids) != set(existing_ids):
                return False
            positions = {host_id: position for position, host_id in enumerate(host_ids)}
            for host in self._data["hosts"]:
                host["position"] = positions[host["id"]]
            self._data["hosts"].sort(key=lambda host: host["position"])
            self._save()
            return True

    def _normalize_positions(self) -> None:
        self._data["hosts"].sort(key=lambda host: (host["position"], host["id"]))
        for position, host in enumerate(self._data["hosts"]):
            host["position"] = position

    def get_settings(self) -> Settings:
        with self._lock:
            return Settings.model_validate(self._data["settings"])

    def update_settings(self, settings: SettingsUpdate) -> Settings:
        with self._lock:
            self._data["settings"] = settings.model_dump()
            self._save()
            return Settings.model_validate(self._data["settings"])

    def close(self) -> None:
        pass
