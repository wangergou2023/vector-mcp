# vector-mcp

Vector 机器人 MCP (Model Context Protocol) 服务器——单二进制，双模式运行。

## 架构

```
daima-agent (C, popen)
  │
  ├── stdin/stdout (MCP JSON‑RPC)
  │
  └── robot-mcp --client-only (Go)
        │
        ├── gRPC → vic-cloud gateway (:443)
        │           ├── engineProtoManager IPC
        │           └── engineCladManager IPC
        │
        └── Unix socket → PCM 播放 (/tmp/daima_spk.sock)
```

## 模式

| 模式 | 命令 | 用途 |
|------|------|------|
| Gateway | `./vic-cloud` | 运行在机器人上，启动 gRPC 服务＋MCP |
| Client | `./vic-cloud --client-only` | daima-agent popen，连 localhost:443 |

## 构建

```bash
cd cloud
GOOS=linux GOARCH=arm GOARM=7 CGO_ENABLED=0 go build -ldflags="-s -w" -o ../build/vic-cloud .
```

## 部署

```bash
# 部署 client 二进制
scp build/vic-cloud root@<robot-ip>:/data/daima/bin/robot-mcp

# 部署 gateway 二进制（覆盖原有 vic-cloud）
ssh root@<robot-ip> "systemctl stop vic-cloud; mount -o rw,remount /"
scp build/vic-cloud root@<robot-ip>:/anki/bin/vic-cloud
ssh root@<robot-ip> "chmod +x /anki/bin/vic-cloud; mount -o remount,ro /; systemctl start vic-cloud"
```

## 工具列表

19 个 MCP 工具，覆盖运动、动画、传感器、电源。

| 分类 | 工具 |
|------|------|
| 运动 | `robot_drive_straight` `robot_turn_in_place` `robot_drive_wheels` `robot_stop` |
| 头部/手臂 | `robot_set_head_angle` `robot_set_lift_height` |
| 动画 | `robot_play_animation` `robot_app_intent` |
| 电源 | `robot_drive_on_charger` `robot_drive_off_charger` `robot_get_battery` |
| 音频 | `robot_set_volume` `robot_play_pcm` `robot_cancel_playback` |
| 传感器 | `robot_get_sensors` `mic_get_direction` |
| 系统 | `robot_subscribe_audio` `robot_unsubscribe_audio` |
| 控制 | `robot_activity_start` `robot_activity_end` |

## 关键实现

- **BehaviorControl**：`StartForegroundActivity()` 自动获取，`StopForegroundActivity()` 释放。每个 motor handler 内部自动走 BC。
- **动画**：`PlayAnimation()` 直传引擎文件名（如 `anim_holiday_hny_fireworks_01`），不走触发器。
- **AppIntent**：clad 通道即发即返，用于充电、打招呼等内置行为。

## 来源

Fork 自 [vic-cloudless](https://github.com/Switch-modder/vector-cloudless)，去掉 VOSK/DAS/switchboard，加入 MCP 工具框架。
