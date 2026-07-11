from fastapi.testclient import TestClient

from hostwatch.main import create_app


def test_health_and_settings(tmp_path):
    app = create_app(tmp_path)
    with TestClient(app) as client:
        assert client.get("/health").json() == {"status": "ok"}
        response = client.get("/api/settings")
        assert response.status_code == 200
        assert response.json()["refresh_interval"] == 15
        assert response.json()["history_minutes"] == 60
        dashboard = client.get("/api/dashboard").json()
        assert dashboard["hosts"] == []
        assert client.get("/api/metrics?since=0&max_points=100").json()["metrics"] == {}
        assert client.get("/").status_code == 200


def test_host_validation(tmp_path):
    app = create_app(tmp_path)
    with TestClient(app) as client:
        response = client.post("/api/hosts", json={
            "name": "No password", "address": "127.0.0.1", "username": "tester",
            "auth_type": "password",
        })
        assert response.status_code == 422
