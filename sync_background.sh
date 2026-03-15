#!/bin/bash
# 后台同步脚本

cd ~/.openclaw/workspace/cmbc-wealth

echo "=== 开始后台同步 ===" 
echo "时间: $(date)"

# 1. 同步净值和公告（分页）
echo "开始同步净值和公告..."
python3 << 'PYEOF'
import sqlite3
import requests
import time

DB_PATH = "cmbc_wealth.db"
BASE_URL = "https://www.cmbcwm.com.cn/gw/po_web"
HEADERS = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/x-www-form-urlencoded"}

# 获取所有产品
conn = sqlite3.connect(DB_PATH)
products = [row[0] for row in conn.execute("SELECT code FROM products").fetchall()]
conn.close()

print(f"产品总数: {len(products)}")

# 逐个同步（分页）
for i, prd_code in enumerate(products):
    try:
        # 净值
        all_nav = []
        page = 1
        while True:
            url = f"{BASE_URL}/BTADailyQry"
            data = {"chart_type": "0", "real_prd_code": prd_code, 
                    "pageNo": str(page), "pageSize": "50"}
            resp = requests.post(url, data=data, headers=HEADERS, timeout=20)
            result = resp.json()
            if result.get("returnCode", {}).get("code") != "AAAAAAA":
                break
            nav_list = result.get("list", [])
            all_nav.extend(nav_list)
            total = result.get("totalSize", 0)
            if len(all_nav) >= total:
                break
            page += 1
        
        # 保存净值
        if all_nav:
            conn = sqlite3.connect(DB_PATH)
            for nav in all_nav:
                if not isinstance(nav, dict): continue
                try:
                    conn.execute("""
                        INSERT OR REPLACE INTO net_values (prd_code, nav_date, nav, tot_nav, update_time)
                        VALUES (?, ?, ?, ?, datetime('now'))
                    """, (prd_code, str(nav.get("ISS_DATE","")), nav.get("NAV",""), nav.get("TOT_NAV","")))
                except: pass
            conn.commit()
            conn.close()
        
        # 公告
        all_ann = []
        page = 1
        while True:
            url = f"{BASE_URL}/BTAFileQry"
            data = {"real_prd_code": prd_code, "SAMJ_TYPE": "8",
                    "pageNo": str(page), "pageSize": "50"}
            resp = requests.post(url, data=data, headers=HEADERS, timeout=20)
            result = resp.json()
            if result.get("returnCode", {}).get("code") != "AAAAAAA":
                break
            ann_list = result.get("list", [])
            all_ann.extend(ann_list)
            total = result.get("totalSize", 0)
            if len(all_ann) >= total:
                break
            page += 1
        
        # 保存公告
        if all_ann:
            conn = sqlite3.connect(DB_PATH)
            for ann in all_ann:
                try:
                    filename = ann.get("K_FILENAME", "")
                    pdf_url = f"https://static.cmbc.com.cn/mb/samj/{filename}" if filename else ""
                    import re
                    match = re.search(r'(\d{8})', filename)
                    ann_date = match.group(1) if match else ""
                    conn.execute("""
                        INSERT OR REPLACE INTO announcements
                        (prd_code, ann_date, ann_name, pdf_url, pdf_filename, update_time)
                        VALUES (?, ?, ?, ?, ?, datetime('now'))
                    """, (prd_code, ann_date, filename.replace(".pdf",""), pdf_url, filename))
                except: pass
            conn.commit()
            conn.close()
        
        if (i+1) % 100 == 0:
            print(f"进度: {i+1}/{len(products)}")
    except Exception as e:
        print(f"错误 {prd_code}: {e}")
    
    time.sleep(0.3)

print(f"同步完成!")
PYEOF

echo "净值/公告同步完成"
echo "净值记录: $(sqlite3 cmbc_wealth.db 'SELECT COUNT(*) FROM net_values')"
echo "公告记录: $(sqlite3 cmbc_wealth.db 'SELECT COUNT(*) FROM announcements')"

# 2. 批量处理 PDF
echo "开始处理 PDF..."
python3 sync_all.py --pdf-only --limit 5000

echo "=== 后台同步完成 ==="
echo "时间: $(date)"
