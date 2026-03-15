#!/bin/bash
# 持续同步脚本 - 每批 50 个，间隔 10 秒
cd ~/.openclaw/workspace/cmbc-wealth

while true; do
    echo "[$(date '+%H:%M:%S')] 检查待同步产品..."
    
    # 获取待处理产品数量
    count=$(sqlite3 cmbc_wealth.db "SELECT COUNT(*) FROM products WHERE code NOT IN (SELECT DISTINCT prd_code FROM net_values) LIMIT 50")
    
    if [ "$count" -eq 0 ]; then
        echo "所有产品已同步完成!"
        break
    fi
    
    echo "本轮处理 $count 个产品..."
    
    # 运行同步
    python3 -c "
import sqlite3, time, sys
sys.path.insert(0, '.')
from scraper import get_net_values, get_announcements, save_net_values, save_announcements, get_db, init_db

init_db()
conn = get_db()
products = [r[0] for r in conn.execute('SELECT code FROM products WHERE code NOT IN (SELECT DISTINCT prd_code FROM net_values) LIMIT 50').fetchall()]
conn.close()

for i, code in enumerate(products):
    try:
        result = get_net_values(code)
        if result.get('returnCode', {}).get('code') == 'AAAAAAA':
            save_net_values(code, result.get('list', []))
        result = get_announcements(code)
        if result.get('returnCode', {}).get('code') == 'AAAAAAA':
            save_announcements(code, result.get('list', []))
    except Exception as e:
        print(f'Error: {code}')
    time.sleep(0.5)
print(f'Done: {len(products)} products')
" >> sync.log 2>&1
    
    # 统计
    nav_count=$(sqlite3 cmbc_wealth.db "SELECT COUNT(DISTINCT prd_code) FROM net_values")
    echo "  净值产品: $nav_count"
    
    echo "等待 10 秒..."
    sleep 10
done
