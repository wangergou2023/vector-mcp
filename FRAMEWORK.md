# vector-mcp 框架

## 一、二进制双模式

```
/anki/bin/vic-cloud  (同一个 ELF, 不同 flag 决定模式)
│
├── 无参数 (默认)       → Gateway 模式 + MCP 服务
│     ├── gRPC gateway :443
│     ├── engineProtoManager (IPC to vic-anim)
│     ├── rpcService (~150 RPC, 完整)
│     ├── MCP server (stdin/stdout JSON-RPC)
│     ├── robot client (连自己的 gateway, EventStream)
│     ├── audio subscriber (mic_sock)
│     └── speaker socket listener
│
└── --client-only       → 纯 MCP 客户端
      ├── MCP server (stdin/stdout JSON-RPC)
      ├── robot client (连 localhost:443)
      ├── audio subscriber (mic_sock)
      └── speaker socket listener
```

## 二、进程拓扑

```
开机 → systemd
  │
  ├── vic-anim.service (固件)
  │     ├── 传感器/电机/动画
  │     ├── mic_sock server
  │     └── _engine_gateway_proto_server_ (IPC)
  │
  ├── vic-cloud.service → /anki/bin/vic-cloud  [Gateway 模式]
  │     ├── listen :443 (TLS gRPC)
  │     ├── engineProtoManager → vic-anim IPC
  │     └── MCP server (此时无 client 连接)
  │
  └── daima.service → /data/daima/bin/daima
        ├── 20s 延迟等 gateway 就绪
        ├── popen /data/daima/bin/robot-mcp --client-only  [Client 模式]
        │     ├── gRPC client → localhost:443
        │     ├── MCP server → stdin/stdout → daima-agent
        │     ├── audio subscriber → mic_sock
        │     └── speaker socket → /tmp/daima_spk.sock
        │
        └── daima-agent (agent loop / LLM / ASR / TTS / Web UI :1234)
```

## 三、代码来源

```
github.com/kercre123/vic-cloudless (原版)
  ├── cloud/gateway.go          →  保留 (gRPC 服务器)
  ├── cloud/message_handler.go   →  保留 (rpcService, 裁剪 DAS/switchboard/token)
  ├── cloud/ipc_manager.go       →  保留 (engineProtoManager, IPC to vic-anim)
  ├── cloud/config.go            →  保留 (Port=443, 证书路径, checkAuth 简化)
  ├── cloud/multilimiter.go      →  保留
  ├── internal/proto/            →  保留 (完整 ExternalInterface 生成代码)
  ├── internal/ipc/              →  保留 (Unix socket)
  ├── internal/clad/             →  保留 (CLAD 消息)
  ├── internal/log/              →  保留 (改 Println→stderr)
  ├── internal/robot/            →  保留 (证书路径)
  ├── internal/util/             →  保留 (最小化: 仅 util.go)
  │
  ├── cloud/voice 等             →  删除 (VOSK ASR, 用 daima 替代)
  ├── cloud/cloudproc 等         →  删除 (云端服务)
  ├── cloud/jdocs, token 等      →  删除 (设置存储)
  │
  └── 新增:
      ├── cloud/main.go           →  Gateway+Client 双模式入口
      ├── cloud/robot.go          →  gRPC client (adapt extint 类型)
      ├── cloud/tools.go          →  20 MCP 工具
      ├── cloud/transport.go      →  MCP JSON-RPC 传输层
      ├── cloud/audio.go          →  mic_sock 音频订阅
      └── cloud/audio_socket.go   →  PCM 播放 socket

github.com/wire-os/robot-mcp (旧，可废弃)
  ├── 功能已全部迁移到 vector-mcp
  └── daima-agent 不再依赖此仓库
```

## 四、daima-agent 只做一件事

```
popen /data/daima/bin/robot-mcp --client-only
```

通过 MCP stdin/stdout 控制机器人，不关心 gateway 实现细节。

## 五、部署

```bash
cd github.com/wangergou2023/vector-mcp/cloud
GOOS=linux GOARCH=arm GOARM=7 CGO_ENABLED=0 go build -ldflags="-s -w" -o ../build/vic-cloud .

# 部署到机器人 (替换原 vic-cloud)
scp build/vic-cloud root@<ip>:/anki/bin/vic-cloud
scp build/vic-cloud root@<ip>:/data/daima/bin/robot-mcp

# daima-agent
cd wire-os/daima-agent && ./build-arm.sh
scp build-arm/daima root@<ip>:/data/daima/bin/daima

# 重启
systemctl restart vic-cloud
systemctl restart daima
```

## 六、关键设计决策

| 决策 | 原因 |
|------|------|
| 一个二进制、两个模式 | Gateway 模式和 Client 模式共享同一代码库，不用维护两个仓库 |
| gateway 内建 MCP server | 不需要额外进程，简化部署 |
| 同进程 gRPC client 连 localhost:443 | 简单可靠，不需要 in-process 调用改造 |
| Client 模式 skip TLS verify | localhost 自签名证书，验证无意义 |
| log.Print* → stderr | 避免污染 MCP stdout 协议 |
| engineProtoManager 保留 | 所有机器人指令必须通过此 IPC 发到 vic-anim |
