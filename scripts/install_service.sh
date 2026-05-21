#!/bin/bash
# binance-chase-daemon systemd 服务安装脚本
# 用法: sudo bash scripts/install_service.sh

SERVICE_NAME="binance-chase-daemon"
USER=$(who am i | awk '{print $1}')
USER_HOME=$(eval echo ~$USER)
PYTHON_BIN=$(which python3)
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

cat > /tmp/${SERVICE_NAME}.service << SERVICE
[Unit]
Description=🍗 Binance Chase Trader — WS Data Daemon
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${USER}
ExecStart=${PYTHON_BIN} -m binance_chase daemon
WorkingDirectory=${USER_HOME}/binance-chase-trader
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
SERVICE

sudo mv /tmp/${SERVICE_NAME}.service ${SERVICE_FILE}
sudo systemctl daemon-reload
sudo systemctl enable ${SERVICE_NAME}
echo "✅ 服务已安装: ${SERVICE_FILE}"
echo "   启动: sudo systemctl start ${SERVICE_NAME}"
echo "   状态: sudo systemctl status ${SERVICE_NAME}"
echo "   日志: journalctl -u ${SERVICE_NAME} -f"
