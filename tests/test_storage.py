import json
from pathlib import Path

from hostwatch.models import HostCreate, HostUpdate, SettingsUpdate
from hostwatch.storage import JsonStore


def make_store(tmp_path: Path) -> JsonStore:
    return JsonStore(tmp_path / "config.json")


def host(name: str, address: str) -> HostCreate:
    return HostCreate(
        name=name, address=address, username="monitor",
        auth_type="password", password="plain-secret",
    )


def test_host_credentials_are_plain_text_json(tmp_path):
    store = make_store(tmp_path)
    created = store.create_host(host("Web 01", "10.0.0.1"))
    assert created["has_password"] is True
    private = store.get_host(created["id"], include_credentials=True)
    assert private["password"] == "plain-secret"
    saved = json.loads((tmp_path / "config.json").read_text(encoding="utf-8"))
    assert saved["hosts"][0]["password"] == "plain-secret"


def test_store_survives_reload(tmp_path):
    store = make_store(tmp_path)
    created = store.create_host(host("Web 01", "10.0.0.1"))
    reloaded = make_store(tmp_path)
    assert reloaded.get_host(created["id"], include_credentials=True)["password"] == "plain-secret"


def test_update_host_preserves_omitted_credentials(tmp_path):
    store = make_store(tmp_path)
    created = store.create_host(host("Web 01", "10.0.0.1"))
    updated = store.update_host(
        created["id"], HostUpdate(name="Web 01 renamed", address="10.0.0.9")
    )
    assert updated["name"] == "Web 01 renamed"
    private = store.get_host(created["id"], include_credentials=True)
    assert private["address"] == "10.0.0.9"
    assert private["password"] == "plain-secret"


def test_reorder_and_delete(tmp_path):
    store = make_store(tmp_path)
    first = store.create_host(host("One", "10.0.0.1"))
    second = store.create_host(host("Two", "10.0.0.2"))
    assert store.reorder_hosts([second["id"], first["id"]])
    assert [item["id"] for item in store.list_hosts()] == [second["id"], first["id"]]
    assert not store.reorder_hosts([first["id"]])
    assert store.delete_host(second["id"])
    assert store.list_hosts()[0]["position"] == 0


def test_settings_round_trip(tmp_path):
    store = make_store(tmp_path)
    updated = store.update_settings(SettingsUpdate(
        refresh_interval=30, history_minutes=120, ssh_timeout=8,
    ))
    assert updated.refresh_interval == 30
    assert updated.history_minutes == 120
    reloaded = make_store(tmp_path)
    assert reloaded.get_settings() == updated
