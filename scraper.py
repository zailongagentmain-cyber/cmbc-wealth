#!/usr/bin/env python3
"""
民生理财数据抓取 + 存储 + 定时更新
CMBC Wealth Scraper
"""
import requests
import json
import time
import os
import sqlite3
from datetime import datetime
from pathlib import Path
import argparse

BASE_URL = "https://www.cmbcwm.com.cn/gw/po_web"

# 配置
DATA_DIR = Path(os.path.expanduser("~/.openclaw/workspace/cmbc-wealth"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "cmbc_wealth.db"
PDF_DIR = DATA_DIR / "pdfs"
PDF_DIR.mkdir(exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Referer": "https://www.cmbcwm.com.cn/grlc/index.htm"
}


def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            code TEXT PRIMARY KEY,
            name TEXT,
            nav REAL,
            nav_date TEXT,
            tot_nav REAL,
            risk_level INTEGER,
            benchmark TEXT,
            status TEXT,
            estal_date TEXT,
            first_amt REAL,
            update_time TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS net_values (
            prd_code TEXT,
            nav_date TEXT,
            nav REAL,
            tot_nav REAL,
            income REAL,
            week_rate REAL,
            update_time TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(prd_code, nav_date)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS announcements (
            prd_code TEXT,
            ann_date TEXT,
            ann_name TEXT,
            pdf_url TEXT,
            pdf_filename TEXT,
            download_status INTEGER DEFAULT 0,
            update_time TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(prd_code, ann_date)
        )
    """)
    
    conn.commit()
    conn.close()
    print(f"数据库: {DB_PATH}")


def save_products(products):
    conn = get_db()
    cursor = conn.cursor()
    
    for p in products:
        cursor.execute("""
            INSERT OR REPLACE INTO products 
            (code, name, nav, nav_date, tot_nav, risk_level, benchmark, status, estal_date, first_amt, update_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """, (
            p.get("REAL_PRD_CODE"),
            p.get("PRD_NAME"),
            p.get("NAV"),
            p.get("NAV_DATE"),
            p.get("TOT_NAV"),
            p.get("RISK_LEVEL"),
            p.get("BENCHMARK_CUSTO"),
            p.get("STATUS"),
            p.get("ESTAB_DATE"),
            p.get("PFIRST_AMT")
        ))
    
    conn.commit()
    conn.close()


def save_net_values(prd_code, net_values):
    if not net_values:
        return 0
    
    conn = get_db()
    cursor = conn.cursor()
    
    for nav in net_values:
        cursor.execute("""
            INSERT OR REPLACE INTO net_values
            (prd_code, nav_date, nav, tot_nav, income, week_rate, update_time)
            VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
        """, (
            prd_code,
            nav.get("ISS_DATE"),
            nav.get("NAV"),
            nav.get("TOT_NAV"),
            nav.get("INCOME"),
            nav.get("WEEK_CLIENTRATE")
        ))
    
    conn.commit()
    conn.close()
    return len(net_values)


def save_announcements(prd_code, announcements):
    if not announcements:
        return 0
    
    conn = get_db()
    cursor = conn.cursor()
    base_url = "https://static.cmbc.com.cn/mb/samj/"
    
    for ann in announcements:
        filename = ann.get("K_FILENAME")
        pdf_url = base_url + filename if filename else ""
        
        cursor.execute("""
            INSERT OR REPLACE INTO announcements
            (prd_code, ann_date, ann_name, pdf_url, pdf_filename, update_time)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
        """, (
            prd_code,
            ann.get("BUSINESS_DATE"),
            ann.get("K_INFNAME"),
            pdf_url,
            filename
        ))
    
    conn.commit()
    conn.close()
    return len(announcements)


# API
def get_products(page_no=1, page_size=100):
    url = f"{BASE_URL}/BTAProductQry"
    data = {"pageNo": page_no, "pageSize": page_size}
    resp = requests.post(url, data=data, headers=HEADERS, timeout=30)
    return resp.json()


def get_net_values(prd_code, begin_date="", end_date=""):
    url = f"{BASE_URL}/BTADailyQry"
    data = {"chart_type": "0", "real_prd_code": prd_code, "begin_date": begin_date, "end_date": end_date}
    resp = requests.post(url, data=data, headers=HEADERS, timeout=30)
    return resp.json()


def get_announcements(prd_code, samj_type="8"):
    url = f"{BASE_URL}/BTAFileQry"
    data = {"real_prd_code": prd_code, "SAMJ_TYPE": samj_type}
    resp = requests.post(url, data=data, headers=HEADERS, timeout=30)
    return resp.json()


def download_pdf(url, prd_code, ann_date):
    if not url:
        return None
    filename = f"{prd_code}_{ann_date}.pdf"
    save_path = PDF_DIR / filename
    if save_path.exists():
        return str(save_path)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code == 200:
            with open(save_path, 'wb') as f:
                f.write(resp.content)
            return str(save_path)
    except:
        pass
    return None


# 主流程
def sync_products():
    """同步产品列表"""
    print("同步产品列表...")
    page = 1
    total_products = 0
    
    while True:
        result = get_products(page, 100)
        if result.get("returnCode", {}).get("code") != "AAAAAAA":
            break
        
        products = result.get("list", [])
        if not products:
            break
            
        save_products(products)
        total_products += len(products)
        print(f"  第{page}页: +{len(products)}")
        
        if len(products) < 100:
            break
        page += 1
        time.sleep(0.3)
    
    print(f"  总计: {total_products} 只产品")


def sync_details(codes=None, limit=100):
    """同步产品详情"""
    conn = get_db()
    
    if codes:
        code_list = codes
    else:
        cursor = conn.execute("SELECT code FROM products LIMIT ?", (limit,))
        code_list = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    
    print(f"同步产品详情 ({len(code_list)} 只)...")
    
    for i, code in enumerate(code_list):
        # 净值
        result = get_net_values(code)
        if result.get("returnCode", {}).get("code") == "AAAAAAA":
            nav_list = result.get("list", [])
            save_net_values(code, nav_list)
        
        # 公告
        result = get_announcements(code)
        if result.get("returnCode", {}).get("code") == "AAAAAAA":
            ann_list = result.get("list", [])
            save_announcements(code, ann_list)
        
        if (i + 1) % 10 == 0:
            print(f"  进度: {i+1}/{len(code_list)}")
        
        time.sleep(0.3)
    
    print(f"  完成: {len(code_list)} 只")


def download_pdfs(limit=50):
    """下载 PDF"""
    conn = get_db()
    cursor = conn.execute("""
        SELECT prd_code, ann_date, pdf_url 
        FROM announcements 
        WHERE download_status = 0 AND pdf_url LIKE '%.pdf'
        LIMIT ?
    """, (limit,))
    rows = cursor.fetchall()
    conn.close()
    
    print(f"下载 PDF ({len(rows)} 个)...")
    
    for prd_code, ann_date, pdf_url in rows:
        path = download_pdf(pdf_url, prd_code, ann_date)
        if path:
            conn = get_db()
            conn.execute("""
                UPDATE announcements SET download_status = 1 WHERE prd_code = ? AND ann_date = ?
            """, (prd_code, ann_date))
            conn.commit()
            conn.close()
    
    print(f"  完成")


def main():
    parser = argparse.ArgumentParser(description="民生理财数据抓取")
    parser.add_argument("--products", action="store_true", help="同步产品列表")
    parser.add_argument("--details", action="store_true", help="同步产品详情")
    parser.add_argument("--download", action="store_true", help="下载PDF")
    parser.add_argument("--limit", type=int, default=100, help="详情数量限制")
    parser.add_argument("--all", action="store_true", help="完整同步")
    args = parser.parse_args()
    
    print("=" * 50)
    print("民生理财数据抓取")
    print("=" * 50)
    
    init_db()
    
    if args.all or args.products:
        sync_products()
    
    if args.all or args.details:
        sync_details(limit=args.limit)
    
    if args.all or args.download:
        download_pdfs()
    
    # 统计
    conn = get_db()
    print("\n数据统计:")
    print(f"  产品: {conn.execute('SELECT COUNT(*) FROM products').fetchone()[0]}")
    print(f"  净值: {conn.execute('SELECT COUNT(*) FROM net_values').fetchone()[0]}")
    print(f"  公告: {conn.execute('SELECT COUNT(*) FROM announcements').fetchone()[0]}")
    conn.close()


if __name__ == "__main__":
    main()
