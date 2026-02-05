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
APP_VERSION = "v6.7 (Full UI + Name Lock)"

# 自動清除舊快取
if 'app_version' not in st.session_state or st.session_state.app_version != APP_VERSION:
    st.cache_data.clear()
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.session_state.app_version = APP_VERSION

st.set_page_config(page_title=f"資產管家 Pro {APP_VERSION}", layout="wide", page_icon="📈")

# --- Google Sheets 連線 ---
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

# --- 資料讀寫核心 ---
def load_data(client, username):
    def clean_num(val):
        try: return float(str(val).replace(',', '').replace('$', '').replace('%', '').strip())
        except: return 0.0

    user_ws = get_worksheet(client, f"User_{username}")
    h_data = {}
    if user_ws:
        all_rows = user_ws.get_all_records()
        for r in all_rows:
            code = str(r.get('Code', '')).strip()
            if not code: continue
            try: lots = json.loads(r.get('Lots_Data', '[]'))
            except: lots = []
            h_data[code] = {
                'n': str(r.get('Name', '')), 'ex': r.get('Exchange', ''),
                's': clean_num(r.get('Shares', 0)), 'c': clean_num(r.get('AvgCost', 0)),
                'last_p': clean_num(r.get('LastPrice', 0)), 'lots': lots
            }

    acc_ws = get_worksheet(client, f"Account_{username}")
    acc_data = {}
    if acc_ws:
        for row in acc_ws.get_all_values():
            if len(row) >= 2: acc_data[row[0]] = row[1]

    hist_ws = get_worksheet(client, f"Realized_{username}", default_header=['Date', 'Code', 'Name', 'Qty', 'BuyCost', 'SellRev', 'Profit', 'ROI'])
    realized = []
    if hist_ws:
        rows = hist_ws.get_all_values()
        if len(rows) > 1:
            for r in rows[1:]: realized.append({'Date': r[0], 'Code': r[1], 'Name': r[2], 'Qty': r[3], 'Profit': r[6]})

    asset_ws = get_worksheet(client, f"Hist_{username}", default_header=['Date', 'NetAsset', 'Principal'])
    asset_hist = []
    if asset_ws:
        rows = asset_ws.get_all_values()
        for r in rows[1:]: asset_hist.append({'Date': r[0], 'NetAsset': clean_num(r[1]), 'Principal': clean_num(r[2])})

    return {
        'h': h_data, 'cash': clean_num(acc_data.get('Cash', 0)),
        'principal': clean_num(acc_data.get('Principal', 0)),
        'usdtwd': clean_num(acc_data.get('USDTWD', 32.5)),
        'history': realized, 'asset_history': asset_hist
    }

def save_data(client, username, data):
    acc_ws = get_worksheet(client, f"Account_{username}")
    if acc_ws:
        acc_ws.clear()
        acc_ws.update('A1', [['Key', 'Value'], ['Cash', data['cash']], ['Principal', data['principal']], ['USDTWD', data.get('usdtwd', 32.5)]])
    user_ws = get_worksheet(client, f"User_{username}")
    if user_ws:
        rows = [['Code', 'Name', 'Exchange', 'Shares', 'AvgCost', 'Lots_Data', 'LastPrice']]
        for code, info in data['h'].items():
            rows.append([code, info['n'], info['ex'], info['s'], info['c'], json.dumps(info['lots']), info.get('last_p', 0)])
        user_ws.clear(); user_ws.update('A1', rows)

# --- 股價抓取 ---
def fetch_price(code):
    is_tw = ('.TW' in code) or ('.TWO' in code) or (code.isdigit())
    if is_tw:
        clean = code.replace('.TW', '').replace('.TWO', '')
        url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=tse_{clean}.tw|otc_{clean}.tw&json=1"
        try:
            r = requests.get(url, timeout=3).json()
            if 'msgArray' in r and r['msgArray']:
                item = r['msgArray'][0]
                z = item.get('z', item.get('b', item.get('y', '0'))).split('_')[0]
                price = float(z); y_close = float(item.get('y', 0))
                return {'p': price, 'chg': price - y_close, 'pct': (price-y_close)/y_close*100 if y_close>0 else 0, 'n': item.get('n', code)}
        except: pass
    
    yf_code = f"{code}.TW" if is_tw and '.' not in code else code
    try:
        t = yf.Ticker(yf_code)
        p = t.history(period="1d")['Close'].iloc[-1]
        prev = t.info.get('regularMarketPreviousClose', p)
        return {'p': p, 'chg': p - prev, 'pct': (p-prev)/prev*100, 'n': t.info.get('shortName', code)}
    except: return {'p': 0, 'chg': 0, 'pct': 0, 'n': code}

# --- 主程式 ---
if 'current_user' not in st.session_state: st.session_state.current_user = None
if not st.session_state.current_user:
    u = st.text_input("User"); p = st.text_input("Password", type="password")
    if st.button("Login"):
        if str(st.secrets["passwords"].get(u)) == p: st.session_state.current_user = u; st.rerun()
    st.stop()

username = st.session_state.current_user
client = get_google_client()
if 'data' not in st.session_state: st.session_state.data = load_data(client, username)
data = st.session_state.data

# Sidebar 買入/賣出略 (維持原邏輯)
with st.sidebar:
    st.metric("💵 現金", f"${int(data['cash']):,}")
    if st.button("Logout"): st.session_state.current_user = None; st.rerun()

# 主介面
st.title("📈 資產管家")
if st.button("🔄 更新即時股價", type="primary", use_container_width=True):
    with st.spinner("同步中..."):
        new_q = {}
        for code, info in data['h'].items():
            res = fetch_price(code)
            new_q[code] = res
            if res['p'] > 0:
                info['last_p'] = res['p']
                # 名稱保護邏輯：雲端沒名字才更新
                if not info['n'] or info['n'] == code: info['n'] = res['n']
        st.session_state.quotes = new_q
        save_data(client, username, data)
        st.rerun()

# 計算表格
quotes = st.session_state.get('quotes', {})
rows = []; total_mkt = 0; day_gain = 0
for code, info in data['h'].items():
    q = quotes.get(code, {'p': info['last_p'], 'chg': 0, 'pct': 0})
    curr_p = q['p'] if q['p'] > 0 else info['last_p']
    mkt = info['s'] * curr_p
    total_mkt += mkt
    day_gain += q['chg'] * info['s']
    rows.append({
        "股票代碼": code, "公司名稱": info['n'], "股數": info['s'], "成本": info['c'], "現價": curr_p,
        "日損益%": q['pct']/100, "日損益": q['chg']*info['s'],
        "總損益": (curr_p - info['c']) * info['s'], "總損益%": (curr_p/info['c']-1) if info['c']>0 else 0,
        "市值": mkt, "mkt_raw": mkt
    })

for r in rows: r["占比"] = r["mkt_raw"] / total_mkt if total_mkt > 0 else 0
net_asset = data['cash'] + total_mkt
roi = (net_asset / data['principal'] - 1) * 100 if data['principal'] > 0 else 0

# 顯示績效指標
k1, k2, k3, k4 = st.columns(4)
k1.metric("💰 淨資產", f"${net_asset:,.0f}")
k2.metric("📅 今日損益", f"${day_gain:,.0f}")
k3.metric("🏆 總報酬率", f"{roi:+.2f}%")
k4.metric("📉 投入本金", f"${data['principal']:,.0f}")

# 頁籤補回
t1, t2, t3, t4 = st.tabs(["📋 庫存明細", "🗺️ 熱力圖", "📊 資產走勢", "📜 已實現損益"])

with t1:
    if rows:
        df = pd.DataFrame(rows).drop(columns=['mkt_raw'])
        st.dataframe(df.style.format({
            "股數": "{:,.0f}", "成本": "{:,.2f}", "現價": "{:.2f}", 
            "日損益%": "{:+.2%}", "日損益": "{:+,.0f}", "總損益%": "{:+.2%}", 
            "總損益": "{:+,.0f}", "市值": "{:,.0f}", "占比": "{:.1%}"
        }), use_container_width=True, hide_index=True)

with t2:
    if rows:
        fig = px.treemap(pd.DataFrame(rows), path=['股票代碼'], values='mkt_raw', color='日損益%', color_continuous_scale='RdYlGn_r', color_continuous_midpoint=0)
        st.plotly_chart(fig, use_container_width=True)

with t3:
    if data['asset_history']:
        df_h = pd.DataFrame(data['asset_history'])
        st.line_chart(df_h.set_index('Date')['NetAsset'])

with t4:
    if data['history']: st.table(pd.DataFrame(data['history']))
