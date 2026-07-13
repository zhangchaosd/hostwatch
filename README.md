# HostWatch

HostWatch 是一个轻量的多主机 Linux 资源监控网页服务。程序通过 SSH 定时采集目标主机的 CPU、内存、网络和根分区容量使用情况。

应用实现、网页 HTML、CSS 和 JavaScript 全部包含在一个 [`main.go`](main.go) 源文件中。编译后只需分发一个二进制文件。

## 功能

- SSH 密码或 OpenSSH 私钥登录，支持带口令私钥
- 添加、编辑、删除、拖拽排序主机
- CPU、内存、网络收发和根分区容量折线图
- hostname、CPU 核数、内存总量和根分区总容量
- JSON 明文配置，不使用数据库
- 指标仅保存在内存中，支持时间窗口和数量硬上限
- 增量指标接口和图表降采样
- 离线主机指数退避，手动刷新可立即重试
- 暗色、亮色和墨水屏亮色主题
- 局域网访问

## 本地运行

需要 Go 1.24 或更高版本。

```bash
go mod download
go run .
```

浏览器访问 <http://localhost:8000>。程序默认监听 `0.0.0.0:8000`，局域网设备可以使用部署机器的 IP 地址访问。

运行内置检查：

```bash
go run . -self-test
```

编译：

```bash
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o hostwatch .
./hostwatch
```

## 配置

主机和设置保存在 `data/config.json`。密码、私钥和私钥口令均为明文。

| 环境变量 | 默认值 | 用途 |
| --- | --- | --- |
| `HOSTWATCH_HOST` | `0.0.0.0` | HTTP 监听地址 |
| `HOSTWATCH_PORT` | `8000` | HTTP 监听端口 |
| `HOSTWATCH_DATA_DIR` | `./data` | `config.json` 所在目录 |

## GitHub Actions 构建与发布

工作流 [`.github/workflows/build.yml`](.github/workflows/build.yml) 会执行静态检查和内置测试，并生成以下产物：

- `hostwatch-linux-amd64`
- `hostwatch-linux-arm64`
- `hostwatch-darwin-amd64`
- `hostwatch-darwin-arm64`
- `hostwatch-windows-amd64.exe`

可以通过 GitHub CLI 查看和下载构建产物：

```bash
gh run list --workflow Build
gh run watch <run-id>
gh run download <run-id> --dir dist
```

推送 `v*` 版本标签会自动创建 GitHub Release。Release 包含 Linux、macOS、Windows 压缩包以及 `checksums.txt`：

```bash
git tag -a v0.2.0 -m "HostWatch v0.2.0"
git push origin v0.2.0
gh run watch --exit-status
gh release view v0.2.0
gh release download v0.2.0 --dir dist/release
```

标签中的版本号会在编译时写入程序，因此 `v0.2.0` Release 中的二进制执行 `hostwatch -version` 会输出 `0.2.0`。

## 目标主机要求

SSH 用户无需 root 权限，但目标主机需要提供 `/proc/stat`、`/proc/meminfo`、`/proc/net/dev` 以及 `df`、`awk`、`head`、`tail`、`cat`、`hostname` 和 `getconf` 等基础命令。
