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
        SELECT code, name, nav, tot_nav, nav_date, risk_level, benchmark, status, estal_date
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

# 清除缓存按钮
if st.sidebar.button("🔄 刷新数据"):
    st.cache_data.clear()
    st.rerun()

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
    width="stretch",
    height=500
)

# 产品详情
st.markdown("---")
st.subheader("产品详情")

# 使用 session_state 保存选择的产品
if 'selected_code' not in st.session_state:
    st.session_state.selected_code = None

# 获取所有产品列表
all_products_df = get_products()
all_products = all_products_df['code'].tolist()

# 如果没有选中过，默认选第一个
if st.session_state.selected_code is None:
    st.session_state.selected_code = all_products[0] if all_products else None

# 选择产品查看净值走势
def on_select_change():
    st.session_state.selected_code = st.session_state.product_selector
    st.rerun()

selected = st.selectbox(
    "选择产品代码", 
    all_products,
    index=all_products.index(st.session_state.selected_code) if st.session_state.selected_code in all_products else 0,
    key="product_selector",
    on_change=on_select_change
)

if st.session_state.selected_code:
    # 获取产品信息（不依赖筛选）
    all_products = get_products()
    product_info = all_products[all_products['code'] == st.session_state.selected_code]
    
    if not product_info.empty:
        p = product_info.iloc[0]
        
        # 产品基本信息卡片
        st.markdown("### 📋 产品信息")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("产品代码", p['code'])
        col2.metric("单位净值", f"{p['nav']:.4f}" if p['nav'] else "N/A")
        col3.metric("累计净值", f"{p['tot_nav']:.4f}" if p['tot_nav'] else "N/A")
        col4.metric("净值日期", p['nav_date'] if p['nav_date'] else "N/A")
        
        col1, col2, col3 = st.columns(3)
        col1.metric("风险等级", p['risk_level'])
        col2.metric("产品状态", p['status'])
        col3.metric("成立日期", p['estal_date'] if p['estal_date'] else "N/A")
        
        st.markdown(f"**产品名称:** {p['name']}")
        if p['benchmark']:
            st.markdown(f"**业绩基准:** {p['benchmark']}")
        
        # 历史净值
        st.markdown("### 📈 历史净值")
        nav_df = get_net_values(st.session_state.selected_code)
        
        if not nav_df.empty:
            st.dataframe(nav_df, width="stretch")
        else:
            st.warning(f"产品 {st.session_state.selected_code} 暂无历史净值数据")
    else:
        st.error(f"未找到产品: {st.session_state.selected_code}")
