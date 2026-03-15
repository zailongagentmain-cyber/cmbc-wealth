#!/usr/bin/env python3
"""
完整数据同步工作流
1. 同步净值数据（分页）
2. 同步公告数据（分页）
3. 验证：净值条数 vs 公告条数
4. 下载 PDF
5. 解析 PDF 并存入数据库
6. 验证：公告状态 vs PDF解析结果
"""
import sqlite3
import requests
import os
import re
import time
from pathlib import Path
from pypdf import PdfReader

DB_PATH = os.path.join(os.path.dirname(__file__), "cmbc_wealth.db")
PDF_DIR = os.path.join(os.path.dirname(__file__), "pdfs")
BASE_URL = "https://www.cmbcwm.com.cn/gw/po_web"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/x-www-form-urlencoded"
}
os.makedirs(PDF_DIR, exist_ok=True)

def get_db():
    return sqlite3.connect(DB_PATH)

# ========== 净值同步 ==========
def get_net_values_paged(prd_code, begin_date="", end_date="", page_size=50):
    """获取净值（支持大数据量）"""
    all_nav = []
    page = 1
    while True:
        url = f"{BASE_URL}/BTADailyQry"
        data = {"chart_type": "0", "real_prd_code": prd_code, 
                "begin_date": begin_date, "end_date": end_date,
                "pageNo": str(page), "pageSize": str(page_size)}
        try:
            resp = requests.post(url, data=data, headers=HEADERS, timeout=30)
            result = resp.json()
            if result.get("returnCode", {}).get("code") != "AAAAAAA":
                break
            nav_list = result.get("list", [])
            all_nav.extend(nav_list)
            total = result.get("totalSize", 0)
            if len(all_nav) >= total:
                break
        except:
            break
        page += 1
    return all_nav

def save_net_values(prd_code, nav_list):
    if not nav_list:
        return 0
    conn = get_db()
    count = 0
    for nav in nav_list:
        # 处理字典类型
        if not isinstance(nav, dict):
            continue
        nav_date = str(nav.get("ISS_DATE", ""))
        nav_val = nav.get("NAV", "")
        tot_nav = nav.get("TOT_NAV", "")
        try:
            conn.execute("""
                INSERT OR REPLACE INTO net_values (prd_code, nav_date, nav, tot_nav, update_time)
                VALUES (?, ?, ?, ?, datetime('now'))
            """, (prd_code, nav_date, nav_val, tot_nav))
            count += 1
        except:
            pass
    conn.commit()
    conn.close()
    return count

# ========== 公告同步 ==========
def get_announcements_paged(prd_code, samj_type="8", page_size=50):
    """获取公告（分页）"""
    all_ann = []
    page = 1
    while True:
        url = f"{BASE_URL}/BTAFileQry"
        data = {"real_prd_code": prd_code, "SAMJ_TYPE": samj_type,
                "pageNo": str(page), "pageSize": str(page_size)}
        try:
            resp = requests.post(url, data=data, headers=HEADERS, timeout=30)
            result = resp.json()
            if result.get("returnCode", {}).get("code") != "AAAAAAA":
                break
            ann_list = result.get("list", [])
            all_ann.extend(ann_list)
            total = result.get("totalSize", 0)
            if len(all_ann) >= total:
                break
        except:
            break
        page += 1
    return all_ann

def save_announcements(prd_code, ann_list):
    if not ann_list:
        return 0
    conn = get_db()
    count = 0
    base_url = "https://static.cmbc.com.cn/mb/samj/"
    for ann in ann_list:
        filename = ann.get("K_FILENAME", "")
        pdf_url = base_url + filename if filename else ""
        # 从文件名提取日期
        match = re.search(r'(\d{8})', filename)
        ann_date = match.group(1) if match else ""
        ann_name = filename.replace(".pdf", "") if filename else ""
        try:
            conn.execute("""
                INSERT OR REPLACE INTO announcements
                (prd_code, ann_date, ann_name, pdf_url, pdf_filename, update_time)
                VALUES (?, ?, ?, ?, ?, datetime('now'))
            """, (prd_code, ann_date, ann_name, pdf_url, filename))
            count += 1
        except:
            pass
    conn.commit()
    conn.close()
    return count

# ========== PDF 解析 ==========
def parse_pdf_content(text):
    """从 PDF 文本提取关键数据"""
    result = {'asset_nav': None, 'share_nav': None, 'tot_nav': None, 
              'purchase_price': None, 'redemption_price': None}
    
    match = re.search(r'资产净值[：:\s]*([\d,]+\.?\d*)', text)
    if match: result['asset_nav'] = float(match.group(1).replace(',', ''))
    
    match = re.search(r'份额净值[：:\s]*([\d,]+\.?\d*)', text)
    if match: result['share_nav'] = float(match.group(1).replace(',', ''))
    
    match = re.search(r'份额累计净值[：:\s]*([\d,]+\.?\d*)', text)
    if match: result['tot_nav'] = float(match.group(1).replace(',', ''))
    
    match = re.search(r'申购/赎回价格[：:\s]*([\d,]+\.?\d*)', text)
    if match:
        price = float(match.group(1).replace(',', ''))
        result['purchase_price'] = price
        result['redemption_price'] = price
    
    return result

def download_and_parse_pdf(pdf_url, prd_code, ann_date):
    try:
        resp = requests.get(pdf_url, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            return None
        
        filename = f"{prd_code}_{ann_date}.pdf"
        path = os.path.join(PDF_DIR, filename)
        with open(path, 'wb') as f:
            f.write(resp.content)
        
        reader = PdfReader(path)
        text = reader.pages[0].extract_text() if reader.pages else ""
        data = parse_pdf_content(text)
        os.remove(path)
        return data
    except Exception as e:
        return None

def process_pdfs(limit=50):
    """处理 PDF：解析并存入数据库"""
    conn = get_db()
    cursor = conn.execute("""
        SELECT prd_code, ann_date, pdf_url 
        FROM announcements 
        WHERE download_status = 0 AND pdf_url LIKE "%净值公告%" AND pdf_url LIKE '%.pdf'
        LIMIT ?
    """, (limit,))
    rows = cursor.fetchall()
    conn.close()
    
    success = 0
    for prd_code, ann_date, pdf_url in rows:
        data = download_and_parse_pdf(pdf_url, prd_code, ann_date)
        conn = get_db()
        if data:
            conn.execute("""
                INSERT OR REPLACE INTO pdf_parsed_data 
                (prd_code, ann_date, asset_nav, share_nav, tot_nav, purchase_price, redemption_price)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (prd_code, ann_date, data['asset_nav'], data['share_nav'], 
                  data['tot_nav'], data['purchase_price'], data['redemption_price']))
            conn.execute("UPDATE announcements SET download_status = 1 WHERE prd_code = ? AND ann_date = ?",
                        (prd_code, ann_date))
            success += 1
        else:
            conn.execute("UPDATE announcements SET download_status = -1 WHERE prd_code = ? AND ann_date = ?",
                        (prd_code, ann_date))
        conn.commit()
        conn.close()
        time.sleep(0.3)
    return success

# ========== 验证 ==========
def validate_data():
    """验证数据一致性"""
    conn = get_db()
    
    # 1. 统计每个产品的净值条数和公告条数
    # 注意：使用分页获取后，净值和公告应该接近（差异<10%）
    cursor = conn.execute("""
        SELECT 
            n.prd_code,
            n.nav_count,
            a.ann_count,
            ROUND(ABS(n.nav_count - a.ann_count) * 100.0 / n.nav_count, 1) as diff_pct
        FROM (
            SELECT prd_code, COUNT(*) as nav_count FROM net_values GROUP BY prd_code
        ) n
        JOIN (
            SELECT prd_code, COUNT(*) as ann_count FROM announcements GROUP BY prd_code
        ) a ON n.prd_code = a.prd_code
        WHERE diff_pct > 10
        ORDER BY diff_pct DESC
        LIMIT 10
    """)
    diff_rows = cursor.fetchall()
    
    # 2. 统计公告状态
    cursor = conn.execute("SELECT download_status, COUNT(*) FROM announcements GROUP BY download_status")
    status_counts = cursor.fetchall()
    
    # 3. 验证 PDF 解析是否正确存入（关键验证！）
    # 公告状态=1 的记录，必须在 pdf_parsed_data 中有对应记录
    cursor = conn.execute("""
        SELECT COUNT(*) FROM announcements a
        LEFT JOIN pdf_parsed_data p ON a.prd_code = p.prd_code AND a.ann_date = p.ann_date
        WHERE a.download_status = 1 AND p.prd_code IS NULL
    """)
    missing_pdf = cursor.fetchone()[0]
    
    # 4. 正确存入的数量
    cursor = conn.execute("""
        SELECT COUNT(*) FROM announcements a
        JOIN pdf_parsed_data p ON a.prd_code = p.prd_code AND a.ann_date = p.ann_date
        WHERE a.download_status = 1
    """)
    correct_pdf = cursor.fetchone()[0]
    
    conn.close()
    
    return {
        'diff_rows': diff_rows,
        'status_counts': status_counts,
        'missing_pdf': missing_pdf,
        'correct_pdf': correct_pdf
    }

# ========== 主流程 ==========
def sync_product(prd_code):
    """同步单个产品"""
    print(f"同步 {prd_code}...")
    
    # 净值
    nav_list = get_net_values_paged(prd_code, "20200101", "20260315")
    saved_nav = save_net_values(prd_code, nav_list)
    print(f"  净值: {saved_nav} 条")
    
    # 公告
    ann_list = get_announcements_paged(prd_code, "8")
    saved_ann = save_announcements(prd_code, ann_list)
    print(f"  公告: {saved_ann} 条")
    
    time.sleep(0.5)
    return saved_nav, saved_ann

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--product", help="单个产品代码")
    parser.add_argument("--limit", type=int, default=10, help="产品数量")
    parser.add_argument("--pdf-only", action="store_true", help="仅处理PDF")
    parser.add_argument("--validate", action="store_true", help="仅验证")
    args = parser.parse_args()
    
    if args.validate:
        print("=== 数据验证 ===")
        result = validate_data()
        print(f"净值/公告差异>50的产品: {len(result['diff_rows'])}")
        for row in result['diff_rows'][:5]:
            print(f"  {row[0]}: 净值={row[1]}, 公告={row[2]}, 差={row[3]}")
        print(f"\n公告状态: {result['status_counts']}")
        print(f"已标记已处理但未存入PDF数据库: {result['missing_pdf']}")
    elif args.pdf_only:
        print(f"处理 PDF (最多{args.limit}个)...")
        count = process_pdfs(args.limit)
        print(f"完成: {count} 个")
    elif args.product:
        sync_product(args.product)
    else:
        print("请指定参数: --product 或 --validate 或 --pdf-only")
