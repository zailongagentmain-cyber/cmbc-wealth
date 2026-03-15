#!/usr/bin/env python3
"""
PDF 解析器 - 提取净值公告中的关键信息并存储到数据库
"""
import sqlite3
import requests
import os
import re
from pypdf import PdfReader
from datetime import datetime
import time
import argparse

DB_PATH = os.path.join(os.path.dirname(__file__), "cmbc_wealth.db")
PDF_DIR = os.path.join(os.path.dirname(__file__), "pdfs")
os.makedirs(PDF_DIR, exist_ok=True)

def get_db():
    return sqlite3.connect(DB_PATH)

def parse_pdf_content(text):
    """从 PDF 文本提取关键数据"""
    result = {
        'asset_nav': None,      # 资产净值
        'share_nav': None,     # 份额净值
        'tot_nav': None,      # 累计净值
        'purchase_price': None,  # 申购价格
        'redemption_price': None  # 赎回价格
    }
    
    # 提取资产净值
    match = re.search(r'资产净值[：:\s]*([\d,]+\.?\d*)', text)
    if match:
        result['asset_nav'] = float(match.group(1).replace(',', ''))
    
    # 提取份额净值
    match = re.search(r'份额净值[：:\s]*([\d,]+\.?\d*)', text)
    if match:
        result['share_nav'] = float(match.group(1).replace(',', ''))
    
    # 提取累计净值
    match = re.search(r'份额累计净值[：:\s]*([\d,]+\.?\d*)', text)
    if match:
        result['tot_nav'] = float(match.group(1).replace(',', ''))
    
    # 提取申购/赎回价格
    match = re.search(r'申购/赎回价格[：:\s]*([\d,]+\.?\d*)', text)
    if match:
        price = float(match.group(1).replace(',', ''))
        result['purchase_price'] = price
        result['redemption_price'] = price
    
    return result

def download_and_parse_pdf(pdf_url, prd_code, ann_date):
    """下载并解析 PDF"""
    try:
        # 下载
        resp = requests.get(pdf_url, timeout=20)
        if resp.status_code != 200:
            return None
        
        filename = f"{prd_code}_{ann_date}.pdf"
        path = os.path.join(PDF_DIR, filename)
        
        with open(path, 'wb') as f:
            f.write(resp.content)
        
        # 解析
        reader = PdfReader(path)
        text = reader.pages[0].extract_text() if reader.pages else ""
        
        data = parse_pdf_content(text)
        
        # 删除本地文件
        os.remove(path)
        
        return data
        
    except Exception as e:
        print(f"  解析错误: {e}")
        if path and os.path.exists(path):
            os.remove(path)
        return None

def process_announcements(limit=50):
    """处理待解析的公告"""
    conn = get_db()
    cursor = conn.execute("""
        SELECT prd_code, ann_date, pdf_url 
        FROM announcements 
        WHERE download_status = 0 AND pdf_url LIKE '%.pdf'
        LIMIT ?
    """, (limit,))
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        print("没有待处理的公告")
        return 0
    
    success = 0
    
    for prd_code, ann_date, pdf_url in rows:
        data = download_and_parse_pdf(pdf_url, prd_code, ann_date)
        
        if data:
            conn = get_db()
            # 存储解析结果
            conn.execute("""
                INSERT OR REPLACE INTO pdf_parsed_data 
                (prd_code, ann_date, asset_nav, share_nav, tot_nav, purchase_price, redemption_price)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                prd_code, ann_date,
                data['asset_nav'], data['share_nav'], data['tot_nav'],
                data['purchase_price'], data['redemption_price']
            ))
            
            # 标记为已处理
            conn.execute("""
                UPDATE announcements SET download_status = 1 
                WHERE prd_code = ? AND ann_date = ?
            """, (prd_code, ann_date))
            
            conn.commit()
            conn.close()
            success += 1
            print(f"  ✓ {prd_code} {ann_date}: asset={data['asset_nav']}")
        else:
            # 标记失败
            conn = get_db()
            conn.execute("""
                UPDATE announcements SET download_status = -1 
                WHERE prd_code = ? AND ann_date = ?
            """, (prd_code, ann_date))
            conn.commit()
            conn.close()
        
        time.sleep(0.3)
    
    return success

def main():
    parser = argparse.ArgumentParser(description="PDF 解析器")
    parser.add_argument("--limit", type=int, default=50, help="每次处理数量")
    parser.add_argument("--all", action="store_true", help="处理所有")
    args = parser.parse_args()
    
    print(f"开始处理 PDF 解析...")
    
    if args.all:
        total = 0
        while True:
            count = process_announcements(args.limit)
            if count == 0:
                break
            total += count
            print(f"已完成 {total} 个")
    else:
        process_announcements(args.limit)
    
    print("完成!")

if __name__ == "__main__":
    main()
