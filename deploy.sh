#!/bin/bash
# Quick deploy: build + push robot-mcp (client-only binary) to robot.
# Usage: ./deploy.sh <robot-ip>
set -e

ROBOT_IP="${1:?用法: $0 <robot-ip>}"
SSH_KEY="${SSH_KEY:-ssh_root_key}"

echo "=== Build ==="
cd "$(dirname "$0")/cloud"
GOOS=linux GOARCH=arm GOARM=7 CGO_ENABLED=0 go build -ldflags="-s -w" -o ../build/vic-cloud .
echo "  vic-cloud $(ls -lh ../build/vic-cloud | awk '{print $5}')"

echo "=== Deploy ==="
ssh -i "$SSH_KEY" root@$ROBOT_IP "systemctl stop daima; sleep 1"
scp -i "$SSH_KEY" ../build/vic-cloud root@$ROBOT_IP:/data/daima/bin/robot-mcp
ssh -i "$SSH_KEY" root@$ROBOT_IP "chmod +x /data/daima/bin/robot-mcp; systemctl start daima; sleep 3; systemctl is-active daima"

echo "=== Done ==="
