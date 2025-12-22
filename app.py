import streamlit as st
import pandas as pd
import yfinance as yf
import json
import os
from datetime import datetime

# 設定頁面配置
st.set_page_config(page_title="資產管家 Web版", layout="wide")

# --- 檔案處理函數 ---
DATA_FILE = 'web_data.json'

def load_data():
    if os.path.exists(DATA_FILE):
        try:
            return json.load(open(DATA_FILE, 'r', encoding='utf-8'))
        except:
            pass
    return {'h': {}, 'cash': 0.0, 'hist': []}

def save_data(data):
    # 注意：在免費雲端上，這個存檔會在重啟後重置
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# --- 核心邏輯 (簡化版) ---
def get_price(ticker):
    try:
        stock = yf.Ticker(ticker)
        # 嘗試 fast_info
        price = stock.fast_info.get('last_price')
        if not price:
            hist = stock.history(period='1d')
            if not hist.empty:
                price = hist['Close'].iloc[-1]
        return price
    except:
        return None

def get_usdtwd():
    try:
        fx = yf.Ticker('USDTWD=X')
        return fx.fast_info.get('last_price') or 32.5
    except:
        return 32.5

# --- 介面開始 ---
st.title("📊 股票資產管家 (Web版)")

# 載入資料
if 'data' not in st.session_state:
    st.session_state.data = load_data()

data = st.session_state.data

# 側邊欄：操作區
with st.sidebar:
    st.header("操作面板")
    
    # 現金管理
    current_cash = data.get('cash', 0.0)
    st.metric("目前現金餘額", f"${int(current_cash):,}")
    
    cash_op = st.number_input("入金/出金 (正存/負提)", value=0.0, step=1000.0)
    if st.button("執行資金異動"):
        data['cash'] += cash_op
        save_data(data)
        st.success("資金已更新")
        st.rerun()

    st.markdown("---")
    st.subheader("新增/交易股票")
    code = st.text_input("股票代碼 (例如 2330.TW, AAPL)").strip().upper()
    shares = st.number_input("股數", min_value=1, value=1000)
    cost = st.number_input("買入單價", min_value=0.0, value=0.0)
    
    if st.button("買入/加碼"):
        if code and cost > 0:
            if 'h' not in data: data['h'] = {}
            
            # 計算成本與扣款 (簡化版，暫不含手續費與融資)
            rate = 1.0 if '.TW' in code else get_usdtwd()
            total_cost = cost * shares * rate
            
            # 扣現金
            data['cash'] -= total_cost
            
            # 更新庫存
            new_lot = {'d': datetime.now().strftime('%Y-%m-%d'), 'p': cost, 's': shares}
            
            if code in data['h']:
                # 平均成本法
                old = data['h'][code]
                total_s = old['s'] + shares
                total_c = (old['c'] * old['s'] + cost * shares) / total_s
                data['h'][code]['s'] = total_s
                data['h'][code]['c'] = total_c
                data['h'][code]['lots'].append(new_lot)
            else:
                data['h'][code] = {'s': shares, 'c': cost, 'lots': [new_lot]}
            
            save_data(data)
            st.success(f"已買入 {code}")
            st.rerun()
        else:
            st.error("請輸入完整資訊")

# 主畫面：報表
st.subheader("資產總覽")

# 這裡需要運算，這在網頁版可能會花一點時間
if st.button("🔄 更新即時股價"):
    with st.spinner('正在抓取最新股價...'):
        usdtwd = get_usdtwd()
        total_mkt_val = 0.0
        total_cost_val = 0.0
        
        table_rows = []
        
        h = data.get('h', {})
        
        for code, info in h.items():
            cur_p = get_price(code)
            
            # --- 修正點 1：加強防呆，如果抓到 NaN (無效數值) 就用成本價 ---
            if cur_p is None or pd.isna(cur_p): 
                cur_p = info['c'] 
            
            rate = 1.0 if '.TW' in code else usdtwd
            
            # 確保運算數值為 float
            s_val = float(info['s'])
            c_val = float(info['c'])
            p_val = float(cur_p)
            
            mkt_val = p_val * s_val * rate
            cost_val = c_val * s_val * rate
            profit = mkt_val - cost_val
            profit_pct = (profit / cost_val * 100) if cost_val else 0
            
            total_mkt_val += mkt_val
            total_cost_val += cost_val
            
            table_rows.append({
                "代碼": code,
                "股數": int(s_val),
                "成本": f"{c_val:.2f}",
                "現價": f"{p_val:.2f}",
                "市值 (TWD)": int(mkt_val),
                "損益 (TWD)": int(profit),
                "報酬率 %": f"{profit_pct:+.2f}%"
            })
        
        net_asset = total_mkt_val + data['cash']
        total_profit = total_mkt_val - total_cost_val
        
        # --- 修正點 2：移除 delta_color 參數避免警告，並確保 total_profit 為數字 ---
        col1, col2, col3 = st.columns(3)
        col1.metric("淨資產總額", f"${int(net_asset):,}")
        col2.metric("證券市值", f"${int(total_mkt_val):,}")
        
        # 這裡加強檢查，如果 total_profit 是無效的，就顯示 0
        safe_profit = int(total_profit) if not pd.isna(total_profit) else 0
        col3.metric("未實現損益", f"${safe_profit:+,}")
        
        # 顯示表格
        if table_rows:
            df = pd.DataFrame(table_rows)
            st.dataframe(df, use_container_width=True)
            
            # 刪除邏輯
            st.markdown("---")
            st.subheader("庫存管理")
            to_del = st.selectbox("選擇要刪除的股票", ["請選擇"] + list(h.keys()))
            if to_del != "請選擇":
                if st.button(f"確定刪除 {to_del} (退回現金)"):
                    # 退回現金邏輯
                    shares = float(h[to_del]['s'])
                    cost = float(h[to_del]['c'])
                    rate = 1.0 if '.TW' in to_del else usdtwd
                    refund = shares * cost * rate
                    data['cash'] += refund
                    del data['h'][to_del]
                    save_data(data)
                    st.success("已刪除並退回本金")
                    st.rerun()

# JSON 檢視 (除錯用)
with st.expander("查看原始資料 (JSON)"):
    st.json(data)
