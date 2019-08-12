#!/bin/bash
pkill -f trade_monitor.py
echo "service start...."
cd /home/trade/monitor_server/py3env/bin
source activate
cd /home/trade/monitor_server/auto_monitor
nohup python -u trade_monitor.py > ../trade_monitor.log 2>&1 &