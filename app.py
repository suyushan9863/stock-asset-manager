import streamlit as st
import pandas as pd
import yfinance as yf
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# 設定頁面配置
st.set_page_config(page_title="資產管家 (雲端版)", layout="wide")

# --- Google Sheets 連線設定 ---
def get_google_sheet_data():
    try:
        # 從 Streamlit Secrets 讀取金鑰
        # 注意：我們假設你在 Secrets 裡存的是 json 字串，key 叫做 service_account_info
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        
        # 解析 Secrets 裡的 JSON 字串
        creds_dict = json.loads(st.secrets["service_account_info"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        
        # 開啟試算表
        sheet_name = st.secrets["spreadsheet_name"]
        sheet = client.open(sheet_name).sheet1
        return sheet
    except Exception as e:
        st.error(f"連線 Google Sheets 失敗: {e}")
        return None

def load_data(sheet):
    if not sheet: return {'h': {}, 'cash': 0.0, 'hist': []}
    try:
        # 讀取 A1 儲存格
        raw_data = sheet.acell('A1').value
        if raw_data:
            return json.loads(raw_data)
    except:
        pass
    # 預設空資料
    return {'h': {}, 'cash': 0.0, 'hist': []}

def save_data(sheet, data):
    if sheet:
        try:
            # 把整包資料轉成 JSON 字串，存回 A1
            json_str = json.dumps(data, ensure_ascii=False)
            sheet.update_acell('A1', json_str)
        except Exception as e:
            st.error(f"存檔失敗: {e}")

# --- 核心邏輯 ---
def get_price(ticker):
    try:
        stock = yf.Ticker(ticker)
        price = stock.fast_info.get('last_price')
        if not price or pd.isna(price):
            hist = stock.history(period='1d')
            if not hist.empty:
                price = hist['Close'].iloc[-1]
        return price
    except:
        return None

def get_usdtwd():
    try:
        fx = yf.Ticker('USDTWD=X')
        p = fx.fast_info.get('last_price')
        return p if p and not pd.isna(p) else 32.5
    except:
        return 32.5

# --- 介面開始 ---
st.title("☁️ 股票資產管家 (Google Sheets 同步版)")

# 初始化連線與資料
if 'sheet' not in st.session_state:
    st.session_state.sheet = get_google_sheet_data()

if 'data' not in st.session_state:
    st.session_state.data = load_data(st.session_state.sheet)

data = st.session_state.data
sheet = st.session_state.sheet

# 檢查連線狀態
if not sheet:
    st.warning("⚠️ 無法連接 Google Sheets，目前僅為暫存模式 (重整後資料會消失)")

# 側邊欄：操作區
with st.sidebar:
    st.header("操作面板")
    
    current_cash = data.get('cash', 0.0)
    st.metric("目前現金餘額", f"${int(current_cash):,}")
    
    cash_op = st.number_input("入金/出金 (正存/負提)", value=0.0, step=1000.0)
    if st.button("執行資金異動"):
        data['cash'] += cash_op
        save_data(sheet, data)
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
            
            rate = 1.0 if '.TW' in code else get_usdtwd()
            total_cost = cost * shares * rate
            
            data['cash'] -= total_cost
            
            new_lot = {'d': datetime.now().strftime('%Y-%m-%d'), 'p': cost, 's': shares}
            
            if code in data['h']:
                old = data['h'][code]
                total_s = old['s'] + shares
                total_c = (old['c'] * old['s'] + cost * shares) / total_s
                data['h'][code]['s'] = total_s
                data['h'][code]['c'] = total_c
                # 確保 lots 欄位存在
                if 'lots' not in data['h'][code]: data['h'][code]['lots'] = []
                data['h'][code]['lots'].append(new_lot)
            else:
                data['h'][code] = {'s': shares, 'c': cost, 'n': code, 'lots': [new_lot]}
            
            save_data(sheet, data)
            st.success(f"已買入 {code}，並同步至雲端")
            st.rerun()
        else:
            st.error("請輸入完整資訊")

# 主畫面
st.subheader("資產總覽")

if st.button("🔄 更新即時股價"):
    with st.spinner('正在連線 Google Sheets 並抓取股價...'):
        usdtwd = get_usdtwd()
        total_mkt_val = 0.0
        total_cost_val = 0.0
        table_rows = []
        
        h = data.get('h', {})
        
        for code, info in h.items():
            cur_p = get_price(code)
            if cur_p is None or pd.isna(cur_p): cur_p = info['c']
            
            rate = 1.0 if '.TW' in code else usdtwd
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
                "市值": int(mkt_val),
                "損益": int(profit),
                "報酬率": f"{profit_pct:+.2f}%"
            })
        
        net_asset = total_mkt_val + data['cash']
        total_profit = total_mkt_val - total_cost_val
        
        col1, col2, col3 = st.columns(3)
        col1.metric("淨資產總額", f"${int(net_asset):,}")
        col2.metric("證券市值", f"${int(total_mkt_val):,}")
        safe_profit = int(total_profit) if not pd.isna(total_profit) else 0
        col3.metric("未實現損益", f"${safe_profit:+,}")
        
        if table_rows:
            df = pd.DataFrame(table_rows)
            st.dataframe(df, use_container_width=True)
            
            st.markdown("---")
            st.subheader("庫存管理")
            to_del = st.selectbox("選擇要刪除的股票", ["請選擇"] + list(h.keys()))
            if to_del != "請選擇":
                if st.button(f"確定刪除 {to_del} (退回現金)"):
                    shares = float(h[to_del]['s'])
                    cost = float(h[to_del]['c'])
                    rate = 1.0 if '.TW' in to_del else usdtwd
                    refund = shares * cost * rate
                    data['cash'] += refund
                    del data['h'][to_del]
                    save_data(sheet, data)
                    st.success("已刪除並同步至雲端")
                    st.rerun()
else:
    st.info("請點擊更新按鈕")

# 用來檢查 Secrets 是否設定成功
# st.write(st.secrets["spreadsheet_name"])
