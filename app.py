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
APP_VERSION = "v6.9 (Restore All & Name Protection)"

# 自動清除舊快取與 Session State
if 'app_version' not in st.session_state or st.session_state.app_version != APP_VERSION:
    st.cache_data.clear()
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.session_state.app_version = APP_VERSION

# 設定頁面配置 (恢復初始樣式)
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
                
                h_data[code] = {
                    'n': str(r.get('Name', '')), 'ex': r.get('Exchange', ''),
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
                    hist_data.append({
                        'Date': str(row[0]), 'Code': str(row[1]), 'Name': str(row[2]), 
                        'Qty': row[3], 'BuyCost': row[4], 'SellRev': row[5], 
                        'Profit': row[6], 'ROI': row[7]
                    })
        except: pass

    asset_ws = get_worksheet(client, f"Hist_{username}", default_header=['Date', 'NetAsset', 'Principal'])
    asset_history = []
    if asset_ws:
        try:
            raw_rows = asset_ws.get_all_values()
            if len(raw_rows) > 1:
                for row in raw_rows[1:]:
                    if len(row) >= 2:
                        asset_history.append({
                            'Date': str(row[0]),
                            'NetAsset': clean_num(row[1]),
                            'Principal': clean_num(row[2]) if len(row) > 2 else clean_num(row[1])
                        })
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
            rows.append([
                code, info.get('n', ''), info.get('ex', ''),
                float(info.get('s', 0)), float(info.get('c', 0)),
                json.dumps(info.get('lots', []), ensure_ascii=False),
                float(current_p)
            ])
        user_ws.clear()
        user_ws.update('A1', rows)

# --- 股價抓取核心 (包含 00670L 修正與名稱保護) ---
def fetch_stock_price_robust(code, current_name):
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
                z = item.get('z', item.get('b', item.get('y', '0'))).split('_')[0]
                price = float(z); y_close = float(item.get('y', 0))
                # 僅在原本沒名字時更新
                final_name = current_name if current_name and current_name != code else item.get('n', code)
                return {'p': price, 'chg': price - y_close, 'pct': ((price - y_close)/y_close*100) if y_close > 0 else 0, 'n': final_name}
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
            final_name = current_name if current_name and current_name != code else t.info.get('shortName', code)
            return {'p': price, 'chg': price - prev_close, 'pct': (price - prev_close)/prev_close*100, 'n': final_name}
    except: pass
    return {'p': 0, 'chg': 0, 'pct': 0, 'n': current_name if current_name else code}

# --- 登入頁面 (恢復最初版本) ---
if 'current_user' not in st.session_state: st.session_state.current_user = None

if not st.session_state.current_user:
    st.markdown(f"<h1 style='text-align: center;'>🔐 資產管家 Pro</h1>", unsafe_allow_html=True)
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
                else: st.error("登入失敗")
    st.stop()

# --- 主程式流程 ---
username = st.session_state.current_user
client = get_google_client()
if 'data' not in st.session_state or st.session_state.get('loaded_user') != username:
    st.session_state.data = load_data(client, username)
    st.session_state.loaded_user = username
data = st.session_state.data

# --- Sidebar (恢復最初版本) ---
with st.sidebar:
    st.title(f"👤 {username}")
    if st.button("Logout"):
        st.session_state.current_user = None; st.rerun()
    st.markdown("---")
    st.metric("💵 現金", f"${int(data['cash']):,}")
    
    with st.expander("💰 存入/取出資金"):
        amt = st.number_input("金額", step=1000.0)
        if st.button("執行"):
            data['cash'] += amt; data['principal'] += amt
            save_data(client, username, data); st.rerun()
            
    with st.expander("🔵 買入股票", expanded=True):
        b_code = st.text_input("代碼").upper().strip()
        b_qty = st.number_input("股數", min_value=1, value=1000)
        b_price = st.number_input("單價", min_value=0.0)
        if st.button("確認買入", type="primary"):
            info = fetch_stock_price_robust(b_code, "")
            is_tw = ('.TW' in b_code or '.TWO' in b_code or b_code.isdigit())
            if b_code not in data['h']:
                data['h'][b_code] = {'n': info['n'], 'ex': 'tse' if is_tw else 'US', 's': 0, 'c': 0, 'lots': []}
            h = data['h'][b_code]
            h['lots'].append({'d': datetime.now().strftime('%Y-%m-%d'), 'p': b_price, 's': b_qty, 'debt': 0})
            h['s'] = sum(l['s'] for l in h['lots'])
            h['c'] = sum(l['s']*l['p'] for l in h['lots']) / h['s']
            save_data(client, username, data); st.rerun()

# --- 主面板 (恢復最初版本) ---
st.title("📈 資產管家")

if st.button("🔄 更新即時股價", type="primary", use_container_width=True):
    with st.spinner("更新中..."):
        new_q = {}
        for code, info in data['h'].items():
            res = fetch_stock_price_robust(code, info.get('n', ''))
            new_q[code] = res
            if res['p'] > 0:
                info['last_p'] = res['p']
                info['n'] = res['n'] # 保護後的名稱回存
        st.session_state.quotes = new_q
        save_data(client, username, data); st.rerun()

# --- 數據表格與 Tabs (恢復最初版本) ---
quotes = st.session_state.get('quotes', {})
total_mkt = 0; day_gain = 0; table_rows = []

for code, info in data['h'].items():
    q = quotes.get(code, {'p': info['last_p'], 'chg': 0, 'pct': 0})
    curr_p = q['p'] if q['p'] > 0 else info['last_p']
    mkt = info['s'] * curr_p
    total_mkt += mkt
    day_gain += q['chg'] * info['s']
    table_rows.append({
        "股票代碼": code, "公司名稱": info['n'], "股數": info['s'], "成本": info['c'], "現價": curr_p,
        "日損益%": q['pct']/100, "日損益": q['chg']*info['s'],
        "總損益%": (curr_p/info['c']-1) if info['c']>0 else 0,
        "總損益": (curr_p - info['c']) * info['s'], "市值": mkt, "mkt_raw": mkt
    })

for r in table_rows: r["占比"] = r["mkt_raw"] / total_mkt if total_mkt > 0 else 0
net_asset = data['cash'] + total_mkt

# 指標欄
k1, k2, k3, k4 = st.columns(4)
k1.metric("💰 淨資產", f"${net_asset:,.0f}")
k2.metric("💵 現金餘額", f"${data['cash']:,.0f}")
k3.metric("📊 證券市值", f"${total_mkt:,.0f}")
k4.metric("📉 投入本金", f"${data['principal']:,.0f}")

# 分頁
tab1, tab2, tab3, tab4 = st.tabs(["📋 庫存明細", "🗺️ 熱力圖", "📊 資產走勢", "📜 已實現損益"])

with tab1:
    if table_rows:
        df = pd.DataFrame(table_rows).drop(columns=['mkt_raw'])
        st.dataframe(df.style.format({
            "股數": "{:,.0f}", "成本": "{:,.2f}", "現價": "{:.2f}", "日損益%": "{:+.2%}", "日損益": "{:+,.0f}",
            "總損益%": "{:+.2%}", "總損益": "{:+,.0f}", "市值": "{:,.0f}", "占比": "{:.1%}"
        }), use_container_width=True, hide_index=True)

with tab2:
    if table_rows:
        fig = px.treemap(pd.DataFrame(table_rows), path=['股票代碼'], values='mkt_raw', color='日損益%', color_continuous_scale='RdYlGn_r', color_continuous_midpoint=0)
        st.plotly_chart(fig, use_container_width=True)

with tab3:
    if data['asset_history']:
        df_h = pd.DataFrame(data['asset_history'])
        st.line_chart(df_h.set_index('Date')['NetAsset'])

with tab4:
    if data['history']:
        st.dataframe(pd.DataFrame(data['history']), use_container_width=True, hide_index=True)
