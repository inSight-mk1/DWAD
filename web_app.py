import streamlit as st
import pandas as pd
import sys
import subprocess
import yaml
from pathlib import Path
import os
from datetime import datetime, timedelta
import plotly.express as px

# Add src to sys.path
SRC_DIR = Path(__file__).resolve().parent / 'src'
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from dwad.utils.config import config
# We might need these if we want to reuse logic, but loading parquets directly is often faster/simpler for read-only dashboard

st.set_page_config(
    page_title="DWAD 板块分析系统",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Helper Functions ---

def load_config():
    """Load project configuration"""
    config_path = Path("config/config.yaml")
    if config_path.exists():
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    return {}

def load_stock_pools():
    """Load stock pools configuration"""
    paths = [Path("config/stock_pools.yaml"), Path("config/stock_pools_example.yaml")]
    for p in paths:
        if p.exists():
            with open(p, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f).get('stock_pools', {})
    return {}

def get_indices_data():
    """Load all index data from parquet files"""
    indices_dir = Path("data/indices")
    data = {}
    if not indices_dir.exists():
        return data
    
    for category_dir in indices_dir.iterdir():
        if category_dir.is_dir():
            for file in category_dir.glob("*.parquet"):
                # Parse name: e.g., "ConceptName_average.parquet"
                name = file.name.replace("_average.parquet", "").replace("_market_cap_weighted.parquet", "")
                full_name = f"{category_dir.name} - {name}"
                
                try:
                    df = pd.read_parquet(file)
                    if not df.empty:
                        data[full_name] = df
                except Exception as e:
                    st.error(f"Error loading {file}: {e}")
    return data

def get_stock_data(symbol):
    """Load stock data for a specific symbol"""
    file_path = Path(f"data/stocks/{symbol}.parquet")
    if file_path.exists():
        try:
            return pd.read_parquet(file_path)
        except:
            return None
    return None

def calculate_returns(df, start_date_ts=None):
    """Calculate returns for a dataframe with a 'date' index or column"""
    if df.empty:
        return None
    
    # Ensure date index
    if 'date' in df.columns:
        df = df.set_index('date')
    
    # 将索引转换为 datetime 类型，防止出现字符串与 Timestamp 比较的错误
    # Convert index to datetime to avoid str vs Timestamp comparison error
    try:
        df.index = pd.to_datetime(df.index)
    except Exception as e:
        st.error(f"日期格式转换失败: {e}")
        return None

    df = df.sort_index()
    if df.empty:
        return None

    current_price = df.iloc[-1]['close_price'] if 'close_price' in df.columns else df.iloc[-1]['index_value']
    
    # Daily Change
    if len(df) > 1:
        prev_price = df.iloc[-2]['close_price'] if 'close_price' in df.columns else df.iloc[-2]['index_value']
        daily_pct = (current_price - prev_price) / prev_price
    else:
        daily_pct = 0.0

    # Period Returns
    periods = {
        '20d': 20,
        '55d': 55,
        '233d': 233
    }
    
    results = {
        'Daily': daily_pct,
        'Current': current_price,
        'Date': df.index[-1]
    }
    
    for name, days in periods.items():
        if len(df) > days:
            past_price = df.iloc[-(days+1)]['close_price'] if 'close_price' in df.columns else df.iloc[-(days+1)]['index_value']
            results[name] = (current_price - past_price) / past_price
        else:
            results[name] = None
            
    # Start Date Return
    if start_date_ts:
        # Find closest date >= start_date_ts
        mask = df.index >= start_date_ts
        filtered = df[mask]
        if not filtered.empty:
            start_price = filtered.iloc[0]['close_price'] if 'close_price' in df.columns else filtered.iloc[0]['index_value']
            results['Since Start'] = (current_price - start_price) / start_price
        else:
            results['Since Start'] = None
            
    return results

# --- Sidebar ---
st.sidebar.title("DWAD 控制面板")

with st.sidebar.expander("🔧 数据操作", expanded=True):
    if st.button("📥 1. 下载数据"):
        with st.status("正在运行下载脚本...", expanded=True) as status:
            st.write("启动 download_data.py ...")
            result = subprocess.run([sys.executable, "scripts/download_data.py"], capture_output=True, text=True)
            st.code(result.stdout)
            if result.returncode == 0:
                status.update(label="下载完成!", state="complete", expanded=False)
            else:
                status.update(label="下载失败", state="error")
                st.error(result.stderr)

    if st.button("📝 2. 提取股池 (CSI & THS)"):
        with st.status("正在提取股池...", expanded=True) as status:
            st.write("运行 CSI 提取...")
            subprocess.run([sys.executable, "scripts/extract_csi_index_pools.py"])
            st.write("运行 THS 提取...")
            subprocess.run([sys.executable, "scripts/extract_ths_index_pools.py"])
            status.update(label="提取完成!", state="complete", expanded=False)

    if st.button("🧮 3. 计算指数"):
        with st.status("正在计算指数...", expanded=True) as status:
            st.write("启动 calculate_index.py ...")
            result = subprocess.run([sys.executable, "scripts/calculate_index.py"], capture_output=True, text=True)
            st.code(result.stdout)
            if result.returncode == 0:
                status.update(label="计算完成!", state="complete", expanded=False)
            else:
                status.update(label="计算失败", state="error")
                st.error(result.stderr)

    if st.button("📊 4. 对比报告"):
        with st.status("生成对比报告...", expanded=True) as status:
            result = subprocess.run([sys.executable, "scripts/compare_indices_multi_period.py"], capture_output=True, text=True)
            if result.returncode == 0:
                status.update(label="报告生成成功!", state="complete", expanded=False)
                st.success("请在下方 '对比报告' 标签页查看或直接打开 reports 目录")
            else:
                status.update(label="生成失败", state="error")
                st.error(result.stderr)

st.sidebar.divider()
st.sidebar.info("提示：操作完成后请刷新页面以加载最新数据")

# --- Main Content ---
st.title("板块分析仪表板")

# Determine default start date (e.g., beginning of current year)
default_start_date = datetime(datetime.now().year, 1, 1).date()

tab1, tab2, tab3 = st.tabs(["🏆 板块排名 (Sector Ranking)", "📋 板块个股 (Sector Stocks)", "📈 对比报告 (Reports)"])

# --- Tab 1: Sector Ranking ---
with tab1:
    col1, col2 = st.columns([1, 3])
    with col1:
        rank_start_date = st.date_input("选择排名起始日期", value=default_start_date)
    with col2:
        st.write("") # Spacer
        if st.button("刷新排名"):
            st.rerun()

    indices = get_indices_data()
    if not indices:
        st.warning("未找到指数数据。请先运行 '计算指数'。")
    else:
        ranking_data = []
        rank_start_ts = pd.Timestamp(rank_start_date)
        
        progress_bar = st.progress(0)
        total = len(indices)
        
        for i, (name, df) in enumerate(indices.items()):
            metrics = calculate_returns(df, rank_start_ts)
            if metrics:
                ranking_data.append({
                    "板块名称": name,
                    "当日涨幅": metrics['Daily'],
                    "当前点位": metrics['Current'],
                    "起点涨幅 (Start-to-Now)": metrics['Since Start'],
                    "20日涨幅": metrics['20d'],
                    "55日涨幅": metrics['55d'],
                    "233日涨幅": metrics['233d'],
                    "最新日期": metrics['Date'].strftime('%Y-%m-%d')
                })
            progress_bar.progress((i + 1) / total)
        
        progress_bar.empty()
        
        if ranking_data:
            df_rank = pd.DataFrame(ranking_data)
            
            # Formatting
            format_cols = ["当日涨幅", "起点涨幅 (Start-to-Now)", "20日涨幅", "55日涨幅", "233日涨幅"]
            
            # Display interactive table
            st.dataframe(
                df_rank.style.format({c: "{:.2%}" for c in format_cols})
                .background_gradient(subset=["当日涨幅"], cmap="RdYlGn", vmin=-0.05, vmax=0.05),
                use_container_width=True,
                height=800,
                column_config={
                    "板块名称": st.column_config.TextColumn("板块名称", width="medium"),
                }
            )
        else:
            st.info("没有符合条件的数据。")

# --- Tab 2: Sector Stocks ---
with tab2:
    stock_pools = load_stock_pools()
    
    if not stock_pools:
        st.warning("未找到股池配置。")
    else:
        # Create selection hierarchy
        pool_categories = list(stock_pools.keys())
        selected_category = st.selectbox("选择股池分类", pool_categories)
        
        if selected_category:
            concepts = stock_pools[selected_category]
            selected_concept = st.selectbox("选择板块/概念", list(concepts.keys()))
            
            # Need to load stock info to map Name -> Symbol
            # Using a direct approach reading stock_info.parquet if available
            stock_info_path = Path("data/metadata/stock_info.parquet")
            name_to_symbol = {}
            if stock_info_path.exists():
                info_df = pd.read_parquet(stock_info_path)
                name_to_symbol = dict(zip(info_df['name'], info_df['symbol']))
            
            if selected_concept:
                stock_names = concepts[selected_concept]
                st.write(f"该板块包含 {len(stock_names)} 只股票")
                
                if st.button("加载个股数据", key="load_stocks"):
                    stock_data_list = []
                    start_ts = pd.Timestamp(rank_start_date) # Use same start date from Tab 1
                    
                    progress_bar = st.progress(0)
                    total_stocks = len(stock_names)
                    
                    for i, stock_name in enumerate(stock_names):
                        symbol = name_to_symbol.get(stock_name)
                        if not symbol:
                            # Try to guess or skip? 
                            # If map is missing, we might have issue. 
                            # Try to find by partial match or assume stock_name IS symbol? 
                            # Usually config has Names (Chinese).
                            pass
                        
                        if symbol:
                            df = get_stock_data(symbol)
                            if df is not None:
                                metrics = calculate_returns(df, start_ts)
                                if metrics:
                                    stock_data_list.append({
                                        "代码": symbol,
                                        "名称": stock_name,
                                        "现价": metrics['Current'],
                                        "当日涨幅": metrics['Daily'],
                                        "起点涨幅": metrics['Since Start'],
                                        "20日涨幅": metrics['20d'],
                                        "55日涨幅": metrics['55d'],
                                        "233日涨幅": metrics['233d']
                                    })
                        progress_bar.progress((i + 1) / total_stocks)
                    
                    progress_bar.empty()
                    
                    if stock_data_list:
                        df_stocks = pd.DataFrame(stock_data_list)
                        
                        st.dataframe(
                            df_stocks.style.format({
                                "当日涨幅": "{:.2%}", 
                                "起点涨幅": "{:.2%}",
                                "20日涨幅": "{:.2%}",
                                "55日涨幅": "{:.2%}",
                                "233日涨幅": "{:.2%}",
                                "现价": "{:.2f}"
                            }).background_gradient(subset=["当日涨幅"], cmap="RdYlGn"),
                            use_container_width=True,
                            height=800
                        )
                    else:
                        st.info("无法加载股票数据，请确保已下载数据且 metadata/stock_info.parquet 存在。")

# --- Tab 3: Reports ---
with tab3:
    st.markdown("### 历史对比报告")
    report_dir = Path("reports")
    if report_dir.exists():
        reports = list(report_dir.glob("*.html"))
        if reports:
            selected_report = st.selectbox("选择报告查看", [r.name for r in reports])
            if selected_report:
                report_path = report_dir / selected_report
                with open(report_path, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                st.components.v1.html(html_content, height=1000, scrolling=True)
        else:
            st.info("暂无 HTML 报告。请运行 '对比数据' 生成。")
    else:
        st.info("reports 目录不存在。")
