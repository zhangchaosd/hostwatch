from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator


AuthType = Literal["password", "key"]


class HostCreate(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    address: str = Field(min_length=1, max_length=255)
    port: int = Field(default=22, ge=1, le=65535)
    username: str = Field(min_length=1, max_length=80)
    auth_type: AuthType = "password"
    password: str | None = Field(default=None, max_length=4096)
    private_key: str | None = Field(default=None, max_length=32768)
    passphrase: str | None = Field(default=None, max_length=4096)

    @field_validator("name", "address", "username")
    @classmethod
    def strip_required_text(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("不能只包含空白字符")
        return value

    @model_validator(mode="after")
    def validate_credentials(self) -> "HostCreate":
        if self.auth_type == "password" and not self.password:
            raise ValueError("密码登录必须填写密码")
        if self.auth_type == "key" and not self.private_key:
            raise ValueError("密钥登录必须填写私钥")
        return self


class HostUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=80)
    address: str | None = Field(default=None, min_length=1, max_length=255)
    port: int | None = Field(default=None, ge=1, le=65535)
    username: str | None = Field(default=None, min_length=1, max_length=80)
    auth_type: AuthType | None = None
    password: str | None = Field(default=None, max_length=4096)
    private_key: str | None = Field(default=None, max_length=32768)
    passphrase: str | None = Field(default=None, max_length=4096)

    @field_validator("name", "address", "username")
    @classmethod
    def strip_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("不能只包含空白字符")
        return value


class HostPublic(BaseModel):
    id: int
    name: str
    address: str
    port: int
    username: str
    auth_type: AuthType
    position: int
    has_password: bool
    has_private_key: bool


class ReorderRequest(BaseModel):
    host_ids: list[int]


class SettingsUpdate(BaseModel):
    refresh_interval: int = Field(ge=5, le=3600)
    history_minutes: int = Field(ge=5, le=1440)
    ssh_timeout: int = Field(ge=3, le=120)


class Settings(SettingsUpdate):
    pass


class Metric(BaseModel):
    timestamp: float
    cpu_percent: float
    memory_percent: float
    network_rx_mbps: float
    network_tx_mbps: float
    disk_percent: float
