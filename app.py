import streamlit as st
import pandas as pd
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
APP_VERSION = "v6.6 (Fix: Permanent Name Protection)"

# 自動清除舊快取與 Session State
if 'app_version' not in st.session_state or st.session_state.app_version != APP_VERSION:
    st.cache_data.clear()
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.session_state.app_version = APP_VERSION

# 設定頁面配置
st.set_page_config(page_title=f"資產管家 Pro {APP_VERSION}", layout="wide", page_icon="📈")

# --- Google Sheets 連線與資料處理 ---
def get_google_client():
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        secret_info = st.secrets["service_account_info"]
        if isinstance(secret_info, str):
            creds_dict = json.loads(secret_info, strict=False)
        else:
            creds_dict = secret_info
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"連線 Google Sheets 失敗: {e}")
        return None

def get_worksheet(client, sheet_name, rows="100", cols="10", default_header=None):
    try:
        spreadsheet = client.open(st.secrets["spreadsheet_name"])
        try:
            return spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=sheet_name, rows=rows, cols=cols)
            if default_header: ws.append_row(default_header)
            return ws
    except Exception as e:
        st.sidebar.error(f"讀取資料表 {sheet_name} 失敗: {str(e)}")
        return None

# --- 資料讀寫核心 ---
def load_data(client, username):
    default = {'h': {}, 'cash': 0.0, 'principal': 0.0, 'history': [], 'asset_history': []}
    if not client or not username: return default
    
    def clean_num(val):
        try:
            if isinstance(val, (int, float)): return float(val)
            if not val: return 0.0
            s = str(val).replace(',', '').replace('$', '').replace(' ', '').replace('%', '').strip()
            return float(s)
        except: return 0.0

    user_ws = get_worksheet(client, f"User_{username}")
    h_data = {}
    if user_ws:
        try:
            all_rows = user_ws.get_all_records()
            for r in all_rows:
                code = str(r.get('Code', '')).strip()
                if not code: continue
                try: lots = json.loads(r.get('Lots_Data', '[]'))
                except: lots = []
                
                if lots:
                    calc_shares = sum(float(l.get('s', 0)) for l in lots)
                    calc_cost_val = sum(float(l.get('s', 0)) * float(l.get('p', 0)) for l in lots)
                    calc_avg_cost = (calc_cost_val / calc_shares) if calc_shares > 0 else 0.0
                    final_s = calc_shares
                    final_c = calc_avg_cost
                else:
                    final_s = clean_num(r.get('Shares', 0))
                    final_c = clean_num(r.get('AvgCost', 0))
                
                # 讀取時記錄名稱
                h_data[code] = {
                    'n': str(r.get('Name', '')), 
                    'ex': r.get('Exchange', ''),
                    's': final_s, 'c': final_c,
                    'last_p': clean_num(r.get('LastPrice', 0)),
                    'lots': lots
                }
        except Exception as e:
            st.error(f"庫存資料解析失敗: {e}")

    acc_ws = get_worksheet(client, f"Account_{username}", rows="20", cols="2")
    acc_data = {}
    if acc_ws:
        try:
            for row in acc_ws.get_all_values():
                if len(row) >= 2: acc_data[row[0]] = row[1]
        except: pass

    hist_ws = get_worksheet(client, f"Realized_{username}", default_header=['Date', 'Code', 'Name', 'Qty', 'BuyCost', 'SellRev', 'Profit', 'ROI'])
    hist_data = []
    if hist_ws:
        try:
            raw_rows = hist_ws.get_all_values()
            if len(raw_rows) > 1:
                for row in raw_rows[1:]:
                    row += [''] * (8 - len(row))
                    hist_data.append({'Date': str(row[0]), 'Code': str(row[1]), 'Name': str(row[2]), 'Qty': row[3], 'BuyCost': row[4], 'SellRev': row[5], 'Profit': row[6], 'ROI': row[7]})
        except: pass

    asset_ws = get_worksheet(client, f"Hist_{username}", default_header=['Date', 'NetAsset', 'Principal'])
    asset_history = []
    if asset_ws:
        try:
            raw_rows = asset_ws.get_all_values()
            if len(raw_rows) > 1:
                for row in raw_rows[1:]:
                    if len(row) >= 2:
                        asset_history.append({'Date': str(row[0]), 'NetAsset': clean_num(row[1]), 'Principal': clean_num(row[2]) if len(row) > 2 else clean_num(row[1])})
        except: pass

    return {
        'h': h_data, 'cash': clean_num(acc_data.get('Cash', 0)),
        'principal': clean_num(acc_data.get('Principal', 0)),
        'last_update': acc_data.get('LastUpdate', ''),
        'usdtwd': clean_num(acc_data.get('USDTWD', 32.5)),
        'history': hist_data, 'asset_history': asset_history
    }

def save_data(client, username, data):
    if not client: return
    acc_ws = get_worksheet(client, f"Account_{username}")
    if acc_ws:
        acc_ws.clear()
        acc_ws.update('A1', [['Key', 'Value'], ['Cash', data['cash']], ['Principal', data['principal']], ['LastUpdate', data.get('last_update', '')], ['USDTWD', data.get('usdtwd', 32.5)]])

    user_ws = get_worksheet(client, f"User_{username}")
    if user_ws:
        headers = ['Code', 'Name', 'Exchange', 'Shares', 'AvgCost', 'Lots_Data', 'LastPrice']
        rows = [headers]
        for code, info in data.get('h', {}).items():
            current_p = info.get('last_p', 0)
            if current_p == 0: current_p = info.get('c', 0)
            # 確保儲存時使用當前的 info['n']，而不是隨便被覆寫的名稱
            rows.append([code, info.get('n', ''), info.get('ex', ''), float(info.get('s', 0)), float(info.get('c', 0)), json.dumps(info.get('lots', []), ensure_ascii=False), float(current_p)])
        user_ws.clear()
        user_ws.update('A1', rows)

# --- 股價抓取核心 ---
def fetch_stock_price_robust(code, exchange=''):
    code = str(code).strip().upper()
    is_tw = ('.TW' in code) or ('.TWO' in code) or (code.isdigit())
    
    if is_tw:
        clean_code = code.replace('.TW', '').replace('.TWO', '')
        queries = [f"tse_{clean_code}.tw", f"otc_{clean_code}.tw"]
        try:
            ts = int(time.time() * 1000)
            url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={'|'.join(queries)}&json=1&delay=0&_={ts}"
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, verify=False, timeout=3)
            data = r.json()
            if 'msgArray' in data and len(data['msgArray']) > 0:
                item = data['msgArray'][0]
                z = item.get('z', '-')
                if z == '-': z = item.get('b', '').split('_')[0]
                if z == '-' or z == '': z = item.get('y', '0')
                price = float(z)
                y_close = float(item.get('y', 0))
                return {'p': price, 'chg': price - y_close, 'pct': ((price - y_close)/y_close*100) if y_close > 0 else 0, 'n': item.get('n', code)}
        except: pass

    yf_code = code
    if is_tw and '.TW' not in yf_code and '.TWO' not in yf_code: yf_code = f"{code}.TW"
    try:
        t = yf.Ticker(yf_code)
        hist = t.history(period="1d")
        if not hist.empty:
            price = hist['Close'].iloc[-1]
            try: prev_close = t.info.get('regularMarketPreviousClose', price)
            except: prev_close = price
            return {'p': price, 'chg': price - prev_close, 'pct': (price - prev_close)/prev_close*100, 'n': t.info.get('shortName', code)}
    except: pass
    return {'p': 0, 'chg': 0, 'pct': 0, 'n': code}

def update_prices_and_sync_names(data):
    """
    核心邏輯：更新股價，但『只有在原有名稱為空』時才更新名稱
    """
    progress_bar = st.progress(0)
    total = len(data['h'])
    new_quotes = {}
    
    for i, (code, info) in enumerate(data['h'].items()):
        res = fetch_stock_price_robust(code, info.get('ex', ''))
        new_quotes[code] = res
        
        # 股價一定更新
        if res['p'] > 0:
            info['last_p'] = res['p']
            
            # 【名稱保護邏輯】
            # 只有當目前名稱長度小於 2 (代表可能是空的或是代碼)
            # 或者目前名稱包含英文 (簡單判斷是否為 yfinance 抓到的英文名) 且雲端原本是空的
            # 我們才使用 API 抓到的名稱
            current_name = str(info.get('n', '')).strip()
            
            # 如果目前已經有中文(非純代碼)，就不准覆寫
            if not current_name or current_name == code:
                info['n'] = res['n']
        
        progress_bar.progress((i + 1) / total)
    progress_bar.empty()
    return new_quotes

# (中間介面部分與之前相同，略作簡化以確保邏輯正確)
# --- 主程式 ---
if 'current_user' not in st.session_state: st.session_state.current_user = None

if not st.session_state.current_user:
    st.markdown(f"<h1 style='text-align: center;'>🔐 資產管家 Pro {APP_VERSION}</h1>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1,2,1])
    with c2:
        with st.form("login"):
            u = st.text_input("User")
            p = st.text_input("Password", type="password")
            if st.form_submit_button("Login", use_container_width=True):
                users = st.secrets.get("passwords", {})
                if u in users and str(users[u]) == str(p):
                    st.session_state.current_user = u.strip()
                    st.rerun()
    st.stop()

username = st.session_state.current_user
client = get_google_client()
if 'data' not in st.session_state or st.session_state.get('loaded_user') != username:
    st.session_state.data = load_data(client, username)
    st.session_state.loaded_user = username
data = st.session_state.data

# --- Sidebar ---
with st.sidebar:
    st.title(f"👤 {username}")
    if st.button("Logout"):
        st.session_state.current_user = None; st.rerun()
    st.metric("💵 現金", f"${int(data['cash']):,}")
    
    with st.expander("🔵 買入股票"):
        b_code = st.text_input("代碼").upper().strip()
        b_qty = st.number_input("股數", min_value=1, value=1000)
        b_price = st.number_input("單價", min_value=0.0)
        if st.button("確認買入"):
            info = fetch_stock_price_robust(b_code)
            is_tw = ('.TW' in b_code or '.TWO' in b_code or b_code.isdigit())
            if b_code not in data['h']:
                # 買入時預設名稱：如果是台股先留白，等更新時抓中文；美股才直接抓 API 名稱
                init_name = "" if is_tw else info['n']
                data['h'][b_code] = {'n': init_name, 'ex': 'tse' if is_tw else 'US', 's': 0, 'c': 0, 'lots': []}
            h = data['h'][b_code]
            h['lots'].append({'d': datetime.now().strftime('%Y-%m-%d'), 'p': b_price, 's': b_qty, 'debt': 0})
            h['s'] = sum(l['s'] for l in h['lots'])
            h['c'] = sum(l['s']*l['p'] for l in h['lots']) / h['s']
            save_data(client, username, data)
            st.success("成功"); st.rerun()

# --- 主面板 ---
st.title("📈 資產管家")
if st.button("🔄 更新即時股價", type="primary", use_container_width=True):
    with st.spinner("同步數據中..."):
        # 使用新開發的同步函式，保護名稱不被覆寫
        st.session_state.quotes = update_prices_and_sync_names(data)
        data['last_update'] = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
        save_data(client, username, data)
        st.rerun()

# --- 表格計算邏輯 ---
quotes = st.session_state.get('quotes', {})
table_rows = []
total_mkt = 0

for code, info in data['h'].items():
    q = quotes.get(code, {'p': info.get('last_p', 0), 'chg': 0, 'pct': 0})
    curr_p = q['p'] if q['p'] > 0 else info.get('last_p', 0)
    mkt_val = info['s'] * curr_p
    total_mkt += mkt_val
    table_rows.append({
        "股票代碼": code, "公司名稱": info.get('n', code), "股數": info['s'], "成本": info['c'], "現價": curr_p,
        "日損益%": q.get('pct', 0)/100, "日損益": q.get('chg', 0) * info['s'],
        "總損益": (curr_p - info['c']) * info['s'], "市值": mkt_val
    })

if table_rows:
    df = pd.DataFrame(table_rows)
    st.dataframe(df.style.format({
        "股數": "{:,.0f}", "成本": "{:,.2f}", "現價": "{:.2f}", "日損益%": "{:+.2%}", "日損益": "{:+,.0f}", "總損益": "{:+,.0f}", "市值": "{:,.0f}"
    }), use_container_width=True, hide_index=True)
