import streamlit as st
import pd as pd
import yfinance as yf
import requests
import time
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
import urllib3

# 忽略 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Version Control ---
APP_VERSION = "v6.8 (Restore Original Layout)"

# 設定頁面配置 (恢復初始樣式)
st.set_page_config(page_title=f"資產管家 Pro {APP_VERSION}", layout="wide", page_icon="📈")

# --- Google Sheets 連線與資料處理 ---
def get_google_client():
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        secret_info = st.secrets["service_account_info"]
        creds_dict = json.loads(secret_info) if isinstance(secret_info, str) else secret_info
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"連線失敗: {e}"); return None

def get_worksheet(client, sheet_name, rows="100", cols="10", default_header=None):
    try:
        spreadsheet = client.open(st.secrets["spreadsheet_name"])
        try: return spreadsheet.worksheet(sheet_name)
        except:
            ws = spreadsheet.add_worksheet(title=sheet_name, rows=rows, cols=cols)
            if default_header: ws.append_row(default_header)
            return ws
    except: return None

# --- 核心數據邏輯 ---
def load_data(client, username):
    # 保持原有的數據讀取邏輯
    # ... (此處省略部分重複的讀取函式內容以節省長度，請沿用你最原本的 load_data)
    pass

# --- 修正後的股價抓取 (確保 00670L 準確 + 名稱鎖定) ---
def fetch_price(code, current_name):
    is_tw = ('.TW' in code) or ('.TWO' in code) or (code.isdigit())
    res = {'p': 0, 'chg': 0, 'pct': 0, 'n': current_name}
    
    if is_tw:
        clean = code.replace('.TW', '').replace('.TWO', '')
        url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=tse_{clean}.tw|otc_{clean}.tw&json=1"
        try:
            r = requests.get(url, timeout=3).json()
            if 'msgArray' in r and r['msgArray']:
                item = r['msgArray'][0]
                z = item.get('z', item.get('b', item.get('y', '0'))).split('_')[0]
                res['p'] = float(z)
                y_close = float(item.get('y', 0))
                res['chg'] = res['p'] - y_close
                res['pct'] = (res['chg']/y_close*100) if y_close>0 else 0
                # 只有當原本沒有名稱時才採用 API 的名稱
                if not current_name or current_name == code:
                    res['n'] = item.get('n', code)
                return res
        except: pass
    
    # 若台股 API 失敗或為美股則用 yfinance
    yf_code = f"{code}.TW" if is_tw and '.' not in code else code
    try:
        t = yf.Ticker(yf_code)
        p = t.history(period="1d")['Close'].iloc[-1]
        prev = t.info.get('regularMarketPreviousClose', p)
        res['p'] = p; res['chg'] = p - prev; res['pct'] = (p-prev)/prev*100
        if not current_name or current_name == code:
            res['n'] = t.info.get('shortName', code)
    except: pass
    return res

# --- 登入頁面 (恢復你最初的版本) ---
if 'current_user' not in st.session_state: st.session_state.current_user = None

if not st.session_state.current_user:
    st.markdown(f"<h1 style='text-align: center;'>🔐 資產管家 Pro</h1>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1,2,1])
    with c2:
        with st.form("login"):
            u = st.text_input("User")
            p = st.text_input("Password", type="password")
            if st.form_submit_button("Login", use_container_width=True):
                if u in st.secrets["passwords"] and str(st.secrets["passwords"][u]) == str(p):
                    st.session_state.current_user = u; st.rerun()
                else: st.error("登入失敗")
    st.stop()

# --- 主介面與側邊欄 (恢復原始配置) ---
username = st.session_state.current_user
client = get_google_client()
# ... (此處讀取數據)

with st.sidebar:
    st.title(f"👤 {username}")
    if st.button("Logout"): st.session_state.current_user = None; st.rerun()
    st.markdown("---")
    # 恢復原本的側邊欄功能區 (買入/賣出/現金異動)
    # ... 

# --- 主面板更新按鈕與顯示 ---
st.title("📈 資產管家")

if st.button("🔄 更新即時股價", type="primary", use_container_width=True):
    with st.spinner("更新中..."):
        new_quotes = {}
        for code, info in data['h'].items():
            # 傳入當前名稱，確保不會被隨意覆寫
            res = fetch_price(code, info.get('n', ''))
            new_quotes[code] = res
            if res['p'] > 0:
                info['last_p'] = res['p']
                info['n'] = res['n'] # 更新後回存
        st.session_state.quotes = new_quotes
        # 儲存至雲端...
        st.rerun()

# ... (下方恢復你原本的 Tabs 與數據表格顯示邏輯)
