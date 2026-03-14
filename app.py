"""
民生理财数据浏览器
运行: streamlit run app.py
"""
import streamlit as st
import sqlite3
import pandas as pd
from pathlib import Path

# 配置
DB_PATH = Path(__file__).parent / "cmbc_wealth.db"

st.set_page_config(
    page_title="民生理财数据浏览器",
    page_icon="📊",
    layout="wide"
)

# 状态映射
STATUS_MAP = {"0": "在售", "1": "停售", "2": "兑付中", "3": "已兑付", "4": "已结束"}
RISK_MAP = {"1": "低风险", "2": "较低风险", "3": "中等风险", "4": "较高风险", "5": "高风险"}

@st.cache_data
def get_products():
    """获取产品列表"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT code, name, nav, tot_nav, risk_level, benchmark, status, estal_date
        FROM products
    """, conn)
    conn.close()
    
    # 转换字段
    df['status'] = df['status'].astype(str).map(STATUS_MAP).fillna(df['status'])
    df['risk_level'] = df['risk_level'].astype(str).map(RISK_MAP).fillna(df['risk_level'])
    return df

@st.cache_data
def get_net_values(code):
    """获取产品净值历史"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT nav_date, nav, tot_nav, income, week_rate
        FROM net_values
        WHERE prd_code = ?
        ORDER BY nav_date DESC
    """, conn, params=(code,))
    conn.close()
    return df

# 标题
st.title("📊 民生理财数据浏览器")
st.markdown("---")

# 获取数据
df = get_products()

# 侧边栏筛选
st.sidebar.header("筛选条件")

# 状态筛选
status_options = ["全部"] + list(STATUS_MAP.values())
selected_status = st.sidebar.selectbox("产品状态", status_options)

# 风险等级筛选
risk_options = ["全部"] + list(RISK_MAP.values())
selected_risk = st.sidebar.selectbox("风险等级", risk_options)

# 业绩基准筛选
benchmark_range = st.sidebar.slider("业绩基准范围", 0.0, 5.0, (0.0, 5.0), 0.1)

# 应用筛选
filtered_df = df.copy()
if selected_status != "全部":
    filtered_df = filtered_df[filtered_df['status'] == selected_status]
if selected_risk != "全部":
    filtered_df = filtered_df[filtered_df['risk_level'] == selected_risk]

# 业绩基准筛选（需要解析范围）
def parse_benchmark(bm):
    if not bm or pd.isna(bm):
        return 0
    bm = str(bm)
    try:
        if '%' in bm:
            parts = bm.replace('%', '').split('-')
            if len(parts) == 2:
                return (float(parts[0]) + float(parts[1])) / 2
            return float(parts[0])
    except:
        pass
    return 0

filtered_df['benchmark_avg'] = filtered_df['benchmark'].apply(parse_benchmark)
filtered_df = filtered_df[
    (filtered_df['benchmark_avg'] >= benchmark_range[0]) & 
    (filtered_df['benchmark_avg'] <= benchmark_range[1])
]

# 统计
col1, col2, col3 = st.columns(3)
col1.metric("产品总数", len(df))
col2.metric("筛选后", len(filtered_df))
col3.metric("平均业绩基准", f"{filtered_df['benchmark_avg'].mean():.2f}%")

# 产品列表
st.subheader(f"产品列表 ({len(filtered_df)} 只)")
st.dataframe(
    filtered_df[['code', 'name', 'nav', 'risk_level', 'benchmark', 'status']],
    use_container_width=True,
    height=500
)

# 产品详情
st.markdown("---")
st.subheader("产品详情")

# 选择产品查看净值走势
selected_code = st.selectbox("选择产品代码", filtered_df['code'].tolist())

if selected_code:
    nav_df = get_net_values(selected_code)
    if not nav_df.empty:
        product_info = filtered_df[filtered_df['code'] == selected_code].iloc[0]
        st.markdown(f"**{product_info['name']}**")
        
        # 净值表格
        st.dataframe(nav_df, use_container_width=True)
    else:
        st.info("暂无净值数据")
