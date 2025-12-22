import streamlit as st
import pandas as pd
import yfinance as yf
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import plotly.express as px

# 設定頁面配置
st.set_page_config(page_title="全功能資產管家", layout="wide", page_icon="📈")

# --- 股票代碼與名稱對照表 (可自行擴充) ---
STOCK_MAP = {
    '2330.TW': '台積電', '2317.TW': '鴻海', '2454.TW': '聯發科',
    '2603.TW': '長榮', '2609.TW': '陽明', '2615.TW': '萬海',
    '3231.TW': '緯創', '2382.TW': '廣達', '3017.TW': '奇鋐',
    '2301.TW': '光寶科', '00685L.TW': '群益台指正2', '00670L.TW': '元大NASDAQ正2',
    'NVDA': '輝達', 'AAPL': '蘋果', 'TSLA': '特斯拉', 'AMD': '超微'
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

def get_main_sheet(client):
    try:
        sheet_name = st.secrets["spreadsheet_name"]
        return client.open(sheet_name).sheet1
    except: return None

def get_history_sheet(client):
    try:
        sheet_name = st.secrets["spreadsheet_name"]
        spreadsheet = client.open(sheet_name)
        try:
            history_sheet = spreadsheet.worksheet('History')
        except gspread.exceptions.WorksheetNotFound:
            history_sheet = spreadsheet.add_worksheet(title='History', rows="1000", cols="2")
            history_sheet.append_row(['Date', 'NetAsset'])
        return history_sheet
    except: return None

def load_data(sheet):
    if not sheet: return {'h': {}, 'cash': 0.0}
    try:
        raw_data = sheet.acell('A1').value
        if raw_data:
            data = json.loads(raw_data)
            for code in data.get('h', {}):
                if 'lots' not in data['h'][code]:
                    data['h'][code]['lots'] = [{
                        'd': '初始', 'p': data['h'][code]['c'], 's': data['h'][code]['s'], 'type': '現股', 'debt': 0
                    }]
            return data
    except: pass
    return {'h': {}, 'cash': 0.0}

def save_data(sheet, data):
    if sheet:
        try:
            json_str = json.dumps(data, ensure_ascii=False)
            sheet.update_acell('A1', json_str)
        except Exception as e: st.error(f"存檔失敗: {e}")

def record_history(client, net_asset):
    hist_sheet = get_history_sheet(client)
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

# --- 介面開始 ---
st.title("📈 全功能股票資產管家")

if 'client' not in st.session_state: st.session_state.client = get_google_client()
if 'sheet' not in st.session_state:
    st.session_state.sheet = get_main_sheet(st.session_state.client) if st.session_state.client else None

client = st.session_state.client
sheet = st.session_state.sheet

if 'data' not in st.session_state: st.session_state.data = load_data(sheet)
data = st.session_state.data

if not sheet:
    st.error("⚠️ 無法連接 Google Sheets，請檢查 Secrets 設定。")
    st.stop()

# --- 側邊欄：交易面板 ---
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
    st.subheader("下單交易")
    code_in = st.text_input("代碼 (如 2330.TW)").strip().upper()
    col1, col2 = st.columns(2)
    shares_in = col1.number_input("股數", min_value=1, value=1000, step=100)
    cost_in = col2.number_input("成交單價", min_value=0.0, value=0.0, step=0.1, format="%.2f")
    trade_type = st.radio("交易類別", ["現股", "融資"], horizontal=True)
    
    margin_ratio = 1.0
    debt = 0
    if trade_type == "融資":
