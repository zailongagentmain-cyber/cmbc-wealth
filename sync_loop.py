#!/usr/bin/env python3
"""
民生理财数据持续同步脚本
运行方式: python3 sync_loop.py
输出: 重定向到 scraper.log
"""
import sqlite3
import time
import sys
from pathlib import Path

# 添加当前目录到路径
sys.path.insert(0, str(Path(__file__).parent))

from scraper import get_net_values, get_announcements, save_net_values, save_announcements, get_db, init_db

BATCH_SIZE = 50  # 每批处理数量
SLEEP_SECONDS = 5  # 每批间隔秒数

def get_pending_products():
    """获取还未同步详情的产品"""
    conn = get_db()
    cursor = conn.execute("""
        SELECT code FROM products
        WHERE code NOT IN (SELECT DISTINCT prd_code FROM net_values)
        LIMIT ?
    """, (BATCH_SIZE,))
    products = [row[0] for row in cursor.fetchall()]
    conn.close()
    return products

def process_product(code):
    """处理单个产品"""
    try:
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
        
        return True
    except Exception as e:
        print(f"  错误: {code} - {str(e)[:50]}")
        return False

def main():
    print("=" * 50)
    print("民生理财数据持续同步")
    print("=" * 50)
    
    init_db()
    
    total_synced = 0
    round_num = 0
    
    while True:
        products = get_pending_products()
        
        if not products:
            print("\n所有产品已同步完成!")
            break
        
        round_num += 1
        print(f"\n[{time.strftime('%H:%M:%S')}] 第 {round_num} 轮: 获取 {len(products)} 个产品详情...")
        
        success = 0
        for i, code in enumerate(products):
            if process_product(code):
                success += 1
            
            # 每 10 个打印一次进度
            if (i + 1) % 10 == 0:
                print(f"  进度: {i+1}/{len(products)}")
            
            # 延时，避免请求过快
            time.sleep(0.3)
        
        total_synced += len(products)
        
        # 统计
        conn = get_db()
        nav_count = conn.execute("SELECT COUNT(DISTINCT prd_code) FROM net_values").fetchone()[0]
        ann_count = conn.execute("SELECT COUNT(DISTINCT prd_code) FROM announcements").fetchone()[0]
        conn.close()
        
        print(f"  完成: 本轮 {success}/{len(products)}, 累计净值产品: {nav_count}, 公告产品: {ann_count}")
        
        # 休息一下
        print(f"  等待 {SLEEP_SECONDS} 秒...")
        time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    main()
