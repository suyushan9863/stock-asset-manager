import streamlit as st
import pandas as pd
import yfinance as yf
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

# 設定頁面配置
st.set_page_config(page_title="全功能資產管家", layout="wide", page_icon="📈")

# --- 股票代碼與名稱對照表 ---
STOCK_MAP = {
    '2330.TW': '台積電', '2317.TW': '鴻海', '2454.TW': '聯發科',
    '2603.TW': '長榮', '2609.TW': '陽明', '2615.TW': '萬海',
    '3231.TW': '緯創', '2382.TW': '廣達', '3017.TW': '奇鋐',
    '2301.TW': '光寶科', '00685L.TW': '群益台指正2', '00670L.TW': '元大NASDAQ正2',
    'NVDA': '輝達', 'AAPL': '蘋果', 'TSLA': '特斯拉', 'AMD': '超微',
    'MSFT': '微軟', 'GOOG': '谷歌', 'AMZN': '亞馬遜'
}

# --- 比較標的清單 (新增) ---
BENCHMARKS = {
    '台灣加權指數': '^TWII',
    '0050 (元大台灣50)': '0050.TW',
    'S&P 500 (美股大盤)': '^GSPC',
    'QQQ (那斯達克100)': 'QQQ',
    '費城半導體指數': '^SOX',
    '台指期 (近月)': 'WTX-PERP.TW' # 若抓不到可視情況調整
}

# --- Google Sheets 連線與資料處理 ---
def get_google_client():
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        secret_str = st.secrets["service_account_info"]
        creds_dict = None
        try:
            creds_dict = json.loads(secret_str, strict=False)
        except json.JSONDecodeError:
            fixed_str = secret_str.replace('\n', '\\n').replace('\r', '')
            creds_dict = json.loads(fixed_str, strict=False)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"連線 Google Sheets 失敗: {e}")
        return None

def get_user_sheet(client, username):
    try:
        spreadsheet_name = st.secrets["spreadsheet_name"]
        spreadsheet = client.open(spreadsheet_name)
        worksheet_name = f"User_{username}"
        try:
            sheet = spreadsheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title=worksheet_name, rows="100", cols="2")
        return sheet
    except Exception as e:
        st.error(f"讀取使用者資料失敗: {e}")
        return None

def get_user_history_sheet(client, username):
    try:
        spreadsheet_name = st.secrets["spreadsheet_name"]
        spreadsheet = client.open(spreadsheet_name)
        worksheet_name = f"Hist_{username}"
        try:
            history_sheet = spreadsheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            history_sheet = spreadsheet.add_worksheet(title=worksheet_name, rows="1000", cols="2")
            history_sheet.append_row(['Date', 'NetAsset'])
        return history_sheet
    except: return None

def load_data(sheet):
    default_data = {'h': {}, 'cash': 0.0, 'history': []}
    if not sheet: return default_data
    try:
        raw_data = sheet.acell('A1').value
        if raw_data:
            data = json.loads(raw_data)
            if 'h' not in data: data['h'] = {}
            if 'cash' not in data: data['cash'] = 0.0
            if 'history' not in data: data['history'] = []
            for code in data.get('h', {}):
                if 'lots' not in data['h'][code]:
                    data['h'][code]['lots'] = [{
                        'd': '初始', 'p': data['h'][code]['c'], 's': data['h'][code]['s'], 'type': '現股', 'debt': 0
                    }]
            return data
    except: pass
    return default_data

def save_data(sheet, data):
    if sheet:
        try:
            json_str = json.dumps(data, ensure_ascii=False)
            sheet.update_acell('A1', json_str)
        except Exception as e: st.error(f"存檔失敗: {e}")

def record_history(client, username, net_asset):
    hist_sheet = get_user_history_sheet(client, username)
    if hist_sheet and net_asset > 0:
        today = datetime.now().strftime('%Y-%m-%d')
        try:
            last_row = hist_sheet.get_all_values()[-1]
            if last_row[0] == today: return 
        except: pass
        hist_sheet.append_row([today, int(net_asset)])

# --- 核心計算邏輯 ---
@st.cache_data(ttl=60)
def get_price_data(ticker):
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period='2d')
        if len(hist) >= 1:
            price = hist['Close'].iloc[-1]
            prev_close = hist['Close'].iloc[-2] if len(hist) >= 2 else price
            change_val = price - prev_close
            change_pct = (change_val / prev_close * 100) if prev_close else 0
            return price, change_val, change_pct
        
        price = stock.fast_info.get('last_price')
        if price and not pd.isna(price):
             prev = stock.info.get('previousClose', price)
             change_val = price - prev
             change_pct = (change_val / prev * 100) if prev else 0
             return price, change_val, change_pct
        return None, 0, 0
    except: return None, 0, 0

@st.cache_data(ttl=300)
def get_usdtwd():
    try:
        fx = yf.Ticker('USDTWD=X')
        p = fx.fast_info.get('last_price')
        return p if p and not pd.isna(p) else 32.5
    except: return 32.5

# --- 新增：取得歷史區間的標的走勢 (用於繪圖) ---
@st.cache_data(ttl=3600) # 快取1小時
def get_benchmark_history(ticker, start_date, end_date):
    try:
        data = yf.download(ticker, start=start_date, end=end_date)
        if not data.empty:
            # 只留 Close，並正規化 Index
            df = data[['Close']].copy()
            df.index = df.index.tz_localize(None) # 移除時區以便對齊
            return df
    except: pass
    return None

# --- 登入介面 ---
if 'current_user' not in st.session_state:
    st.session_state.current_user = None

if not st.session_state.current_user:
    st.markdown("<h1 style='text-align: center;'>🔐 股票資產管家</h1>", unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1,2,1])
    with col2:
        user_input = st.text_input("使用者名稱 (例如: Kevin)", key="login_input")
        if st.button("登入 / 註冊", use_container_width=True):
            if user_input.strip():
                st.session_state.current_user = user_input.strip()
                st.rerun()
            else: st.error("請輸入名稱")
    st.stop()

# --- 主程式 ---
username = st.session_state.current_user

with st.sidebar:
    st.info(f"👤 User: **{username}**")
    if st.button("登出"):
        st.session_state.current_user = None
        if 'data' in st.session_state: del st.session_state.data
        if 'sheet' in st.session_state: del st.session_state.sheet
        st.rerun()
    st.markdown("---")

if 'client' not in st.session_state: st.session_state.client = get_google_client()
if 'sheet' not in st.session_state or st.session_state.get('sheet_user') != username:
    if st.session_state.client:
        st.session_state.sheet = get_user_sheet(st.session_state.client, username)
        st.session_state.sheet_user = username
        st.session_state.data = load_data(st.session_state.sheet)
    else: st.session_state.sheet = None

client = st.session_state.client
sheet = st.session_state.sheet
data = st.session_state.data

if not sheet:
    st.error("⚠️ 無法取得資料，請檢查 Secrets 設定。")
    st.stop()

st.title(f"📈 資產管家 - {username}")

# --- 側邊欄 ---
with st.sidebar:
    st.header("💰 資金與交易")
    st.metric("現金餘額", f"${int(data.get('cash', 0)):,}")
    with st.expander("💵 資金存提"):
        cash_op = st.number_input("金額 (正存/負提)", step=1000.0)
        if st.button("執行異動"):
            data['cash'] += cash_op
            save_data(sheet, data)
            st.success("資金已更新"); st.rerun()

    st.markdown("---")
    
    st.subheader("🔵 買入股票")
    code_in = st.text_input("買入代碼 (如 2330.TW)").strip().upper()
    c1, c2 = st.columns(2)
    shares_in = c1.number_input("買入股數", min_value=1, value=1000, step=100)
    cost_in = c2.number_input("買入單價", min_value=0.0, value=0.0, step=0.1, format="%.2f")
    trade_type = st.radio("類別", ["現股", "融資"], horizontal=True)
    margin_ratio = 1.0
    if trade_type == "融資":
        margin_ratio = st.slider("自備款成數", 0.1, 0.9, 0.4, 0.1)

    if st.button("確認買入", type="primary"):
        if code_in and cost_in > 0:
            if 'h' not in data: data['h'] = {}
            rate = 1.0 if ('.TW' in code_in or '.TWO' in code_in) else get_usdtwd()
            total_twd = cost_in * shares_in * rate
            cash_needed = total_twd * margin_ratio
            debt_created = total_twd - cash_needed
            
            if data['cash'] < cash_needed:
                 st.error(f"現金不足！需 ${int(cash_needed):,}，現有 ${int(data['cash']):,}")
            else:
                data['cash'] -= cash_needed
                new_lot = {'d': datetime.now().strftime('%Y-%m-%d'), 'p': cost_in, 's': shares_in, 'type': trade_type, 'debt': debt_created}
                if code_in in data['h']:
                    if 'lots' not in data['h'][code_in]: data['h'][code_in]['lots'] = []
                    lots = data['h'][code_in]['lots']
                    lots.append(new_lot)
                    tot_s = sum(l['s'] for l in lots)
                    tot_c_val = sum(l['s'] * l['p'] for l in lots)
                    data['h'][code_in]['s'] = tot_s
                    data['h'][code_in]['c'] = tot_c_val / tot_s if tot_s else 0
                    data['h'][code_in]['lots'] = lots
                else:
                    data['h'][code_in] = {'s': shares_in, 'c': cost_in, 'n': code_in, 'lots': [new_lot]}
                save_data(sheet, data)
                st.success(f"買入成功！{code_in}"); st.rerun()
        else: st.error("資料不完整")

    st.markdown("---")

    st.subheader("🔴 賣出股票")
    holdings_list = list(data.get('h', {}).keys())
    if holdings_list:
        sell_code = st.selectbox("賣出代碼", ["請選擇"] + holdings_list, key="sell_select")
        if sell_code != "請選擇":
            current_hold = data['h'][sell_code]['s']
            st.caption(f"持有: {current_hold} 股")
            sc1, sc2 = st.columns(2)
            sell_qty = sc1.number_input("賣出股數", min_value=1, max_value=int(current_hold), value=int(current_hold), step=100)
            sell_price = sc2.number_input("賣出單價", min_value=0.0, value=0.0, step=0.1, format="%.2f")
            
            if st.button("確認賣出"):
                if sell_price > 0:
                    info = data['h'][sell_code]
                    lots = info.get('lots', [])
                    rate = 1.0 if ('.TW' in sell_code or '.TWO' in sell_code) else get_usdtwd()
                    sell_revenue = sell_qty * sell_price * rate
                    remain_to_sell = sell_qty
                    total_cost_basis = 0
