import streamlit as st
import pandas as pd
import yfinance as yf
import requests
import time
import json
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
import urllib3

# 忽略 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Version Control ---
APP_VERSION = "v7.6 (Fix: Cash Logic & Market Detection)"

# 自動清除舊快取與 Session State
if 'app_version' not in st.session_state or st.session_state.app_version != APP_VERSION:
    st.cache_data.clear()
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.session_state.app_version = APP_VERSION

# 設定頁面配置
st.set_page_config(page_title=f"資產管家 Pro {APP_VERSION}", layout="wide", page_icon="🛡️")

# --- Helper Functions ---
def is_taiwan_stock(code):
    """判斷是否為台股格式 (純數字、含 .TW 或 .TWO)"""
    c = str(code).upper().strip()
    return c.isdigit() or '.TW' in c or '.TWO' in c

# --- Google Sheets 連線與資料處理 ---
def get_google_client():
    try:
        scope = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        secret_info = st.secrets["service_account_info"]
        
        if isinstance(secret_info, str):
            creds_dict = json.loads(secret_info, strict=False)
        else:
            creds_dict = dict(secret_info)
            
        if 'private_key' in creds_dict:
            creds_dict['private_key'] = creds_dict['private_key'].replace('\\n', '\n')
            
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"❌ Google Sheet 連線失敗: {e}")
        st.stop()

def get_ws_ci(spreadsheet, title):
    target = str(title).strip().lower()
    for ws in spreadsheet.worksheets():
        if ws.title.lower() == target:
            return ws
    raise gspread.exceptions.WorksheetNotFound(title)

def get_worksheet(spreadsheet, sheet_name, rows="100", cols="10", default_header=None):
    try:
        return get_ws_ci(spreadsheet, sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=sheet_name, rows=rows, cols=cols)
        if default_header: ws.append_row(default_header)
        return ws
    except Exception as e:
        st.error(f"讀取資料表 {sheet_name} 失敗: {str(e)}")
        st.stop()

def load_data(client, username):
    username = username.strip()
    default = {'h': {}, 'cash': 0.0, 'principal': 0.0, 'history': [], 'asset_history': [], 'is_legacy': False}
    
    if not client or not username: return default

    try:
        spreadsheet = client.open(st.secrets["spreadsheet_name"])
    except Exception as e:
        st.error(f"❌ 無法開啟試算表: {e}")
        st.stop()

    def clean_num(val):
        try:
            if isinstance(val, (int, float)): return float(val)
            if not val: return 0.0
            s = str(val).replace(',', '').replace('$', '').replace(' ', '').replace('%', '').strip()
            return float(s)
        except: return 0.0

    h_data = {}
    legacy_json = None
    is_legacy = False
    
    try:
        try:
            user_ws = get_ws_ci(spreadsheet, f"User_{username}")
        except gspread.exceptions.WorksheetNotFound:
            return default
            
        all_rows_vals = user_ws.get_all_values()
        if all_rows_vals and len(all_rows_vals) > 0:
            first_cell = str(all_rows_vals[0][0]).strip()
            if first_cell.startswith('{') and "Code" not in all_rows_vals[0]:
                is_legacy = True
        
        if is_legacy:
            try:
                raw_json = all_rows_vals[0][0]
                legacy_json = json.loads(raw_json)
                if isinstance(legacy_json, str): legacy_json = json.loads(legacy_json)
                raw_h = legacy_json.get('h', {})
                for code, info in raw_h.items():
                    h_data[code] = {
                        'n': info.get('n', code), 'ex': info.get('ex', ''),
                        's': clean_num(info.get('s', 0)), 'c': clean_num(info.get('c', 0)),
                        'last_p': 0, 'lots': info.get('lots', [])
                    }
            except Exception as e:
                st.error(f"⚠️ 舊版資料解析失敗: {e}")
        else:
            all_records = user_ws.get_all_records()
            for r in all_records:
                code = str(r.get('Code', '')).strip()
                if not code: continue
                try: lots = json.loads(r.get('Lots_Data', '[]'))
                except: lots = []
                final_s = clean_num(r.get('Shares', 0))
                final_c = clean_num(r.get('AvgCost', 0))
                if lots:
                    calc_s = sum(float(l.get('s', 0)) for l in lots)
                    calc_val = sum(float(l.get('s', 0)) * float(l.get('p', 0)) for l in lots)
                    final_s = calc_s
                    final_c = (calc_val / calc_s) if calc_s > 0 else 0.0
                h_data[code] = {
                    'n': r.get('Name', ''), 'ex': r.get('Exchange', ''),
                    's': final_s, 'c': final_c,
                    'last_p': clean_num(r.get('LastPrice', 0)),
                    'lots': lots
                }
    except Exception as e:
        st.error(f"⚠️ 讀取庫存資料發生錯誤: {e}")
        st.stop()

    acc_data = {}
    cash_val = clean_num(legacy_json.get('cash', 0)) if legacy_json else 0.0
    principal_val = clean_num(legacy_json.get('principal', 0)) if legacy_json else 0.0
    last_update_val = ""
    usdtwd_val = 32.5

    try:
        acc_ws = get_ws_ci(spreadsheet, f"Account_{username}")
        for row in acc_ws.get_all_values():
            if len(row) >= 2: acc_data[row[0]] = row[1]
        if acc_data:
            cash_val = clean_num(acc_data.get('Cash', cash_val))
            principal_val = clean_num(acc_data.get('Principal', principal_val))
            last_update_val = acc_data.get('LastUpdate', '')
            usdtwd_val = clean_num(acc_data.get('USDTWD', 32.5))
    except gspread.exceptions.WorksheetNotFound:
        pass
    except Exception as e:
        st.error(f"⚠️ 讀取帳戶資金失敗: {e}")

    hist_data = []
    asset_history = []
    if legacy_json and 'history' in legacy_json:
        for h in legacy_json['history']:
            hist_data.append({
                'Date': h.get('d'), 'Code': h.get('code'), 'Name': h.get('name'),
                'Qty': h.get('qty'), 'BuyCost': h.get('buy_cost'), 
                'SellRev': h.get('sell_rev'), 'Profit': h.get('profit'), 'ROI': h.get('roi')
            })

    try:
        h_ws = get_ws_ci(spreadsheet, f"Realized_{username}")
        raw_h = h_ws.get_all_values()
        if len(raw_h) > 1:
            hist_data = [] 
            for row in raw_h[1:]:
                row += [''] * (8 - len(row))
                hist_data.append({'Date': str(row[0]), 'Code': str(row[1]), 'Name': str(row[2]), 'Qty': row[3], 'BuyCost': row[4], 'SellRev': row[5], 'Profit': row[6], 'ROI': row[7]})
    except: pass

    try:
        a_ws = get_ws_ci(spreadsheet, f"Hist_{username}")
        raw_a = a_ws.get_all_values()
        if len(raw_a) > 1:
            for row in raw_a[1:]:
                if len(row) >= 2:
                    asset_history.append({'Date': str(row[0]), 'NetAsset': clean_num(row[1]), 'Principal': clean_num(row[2]) if len(row)>2 else clean_num(row[1])})
    except: pass

    return {
        'h': h_data, 'cash': cash_val, 'principal': principal_val,
        'last_update': last_update_val, 'usdtwd': usdtwd_val,
        'history': hist_data, 'asset_history': asset_history, 'is_legacy': is_legacy
    }

def save_data(client, username, data):
    username = username.strip()
    if not client: return
    if data['cash'] == 0 and data['principal'] == 0 and not data['h']:
        st.toast("⚠️ 偵測到資料異常為空，自動攔截存檔！", icon="🛡️")
        return
    try:
        spreadsheet = client.open(st.secrets["spreadsheet_name"])
        acc_ws = get_worksheet(spreadsheet, f"Account_{username}")
        acc_ws.clear()
        acc_ws.update('A1', [['Key', 'Value'], ['Cash', data['cash']], ['Principal', data['principal']], ['LastUpdate', data.get('last_update', '')], ['USDTWD', data.get('usdtwd', 32.5)]])
        user_ws = get_worksheet(spreadsheet, f"User_{username}")
        headers = ['Code', 'Name', 'Exchange', 'Shares', 'AvgCost', 'Lots_Data', 'LastPrice']
        rows = [headers]
        for code, info in data.get('h', {}).items():
            current_p = info.get('last_p', 0)
            if current_p == 0: current_p = info.get('c', 0)
            rows.append([code, info.get('n', ''), info.get('ex', ''), float(info.get('s', 0)), float(info.get('c', 0)), json.dumps(info.get('lots', []), ensure_ascii=False), float(current_p)])
        user_ws.clear()
        user_ws.update('A1', rows)
    except Exception as e:
        st.error(f"❌ 存檔失敗: {e}")

def log_transaction(client, username, action, code, amount, shares, memo=""):
    try:
        spreadsheet = client.open(st.secrets["spreadsheet_name"])
        ws = get_worksheet(spreadsheet, f"Audit_{username}", default_header=['Time', 'Action', 'Code', 'Amount', 'Shares', 'Memo'])
        ts = (datetime.utcnow() + timedelta(hours=8)).strftime('%Y/%m/%d %H:%M:%S')
        ws.append_row([ts, action, code, amount, shares, memo])
    except: pass

def record_asset_history(client, username, net_asset, principal):
    try:
        spreadsheet = client.open(st.secrets["spreadsheet_name"])
        ws = get_worksheet(spreadsheet, f"Hist_{username}", default_header=['Date', 'NetAsset', 'Principal'])
        today = (datetime.utcnow() + timedelta(hours=8)).strftime('%Y-%m-%d')
        all_vals = ws.get_all_values()
        if len(all_vals) > 1 and all_vals[-1][0] == today:
            row_idx = len(all_vals)
            ws.update(f"B{row_idx}:C{row_idx}", [[net_asset, principal]])
        else:
            ws.append_row([today, net_asset, principal])
    except: pass

def get_audit_logs(client, username, limit=50):
    try:
        spreadsheet = client.open(st.secrets["spreadsheet_name"])
        ws = get_ws_ci(spreadsheet, f"Audit_{username}")
        vals = ws.get_all_values()
        if len(vals) > 1: return vals[1:][-limit:][::-1]
    except: pass
    return []

# --- 股價抓取核心 ---
@st.cache_data(ttl=300)
def get_usdtwd():
    try:
        t = yf.Ticker("USDTWD=X")
        return t.history(period="1d")['Close'].iloc[-1]
    except: return 32.5

@st.cache_data(ttl=3600)
def get_benchmark_data(start_date):
    benchmarks = {}
    target_tickers = [('0050.TW', '台灣50'), ('SPY', 'S&P 500'), ('QQQ', 'NASDAQ 100')]
    for code, name in target_tickers:
        try:
            t = yf.Ticker(code)
            hist = t.history(start=start_date)
            if not hist.empty:
                start_val = hist['Close'].iloc[0]
                if start_val > 0: benchmarks[name] = ((hist['Close'] / start_val) - 1) * 100
        except: pass
    return benchmarks

def fetch_stock_price_robust(code, exchange=''):
    code = str(code).strip().upper()
    is_tw = is_taiwan_stock(code)
    
    if is_tw:
        clean_code = code.replace('.TW', '').replace('.TWO', '')
        queries = [f"tse_{clean_code}.tw", f"otc_{clean_code}.tw"]
        try:
            ts = int(time.time() * 1000)
            url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={'|'.join(queries)}&json=1&delay=0&_={ts}"
            headers = {"User-Agent": "Mozilla/5.0"}
            r = requests.get(url, headers=headers, verify=False, timeout=3)
            data = r.json()
            if 'msgArray' in data and len(data['msgArray']) > 0:
                item = data['msgArray'][0]
                z = item.get('z', '-')
                if z == '-': z = item.get('b', '').split('_')[0]
                if z == '-' or z == '': z = item.get('y', '0')
                price = float(z)
                y_close = float(item.get('y', 0))
                if price > 0:
                    chg = price - y_close
                    pct = (chg / y_close * 100) if y_close > 0 else 0
                    return {'p': price, 'chg': chg, 'pct': pct, 'n': item.get('n', code), 'src': 'TWSE'}
        except Exception: pass

    yf_code = code
    if is_tw and '.TW' not in yf_code and '.TWO' not in yf_code: yf_code = f"{code}.TW"
    try:
        t = yf.Ticker(yf_code)
        hist = t.history(period="1d")
        if not hist.empty:
            price = hist['Close'].iloc[-1]
            try: prev_close = t.info.get('regularMarketPreviousClose', price)
            except: prev_close = price
            fetched_name = t.info.get('shortName') or t.info.get('longName') or code
            if price > 0:
                chg = price - prev_close
                pct = (chg / prev_close * 100) if prev_close > 0 else 0
                return {'p': price, 'chg': chg, 'pct': pct, 'n': fetched_name, 'src': 'Yahoo'}
    except Exception: pass
    return {'p': 0, 'chg': 0, 'pct': 0, 'n': code, 'src': 'Fail'}

def update_prices_batch(portfolio):
    results = {}
    progress_bar = st.progress(0)
    total = len(portfolio)
    for i, (code, info) in enumerate(portfolio.items()):
        ex = info.get('ex', '')
        res = fetch_stock_price_robust(code, ex)
        results[code] = res
        progress_bar.progress((i + 1) / total)
    progress_bar.empty()
    return results

@st.dialog("📋 異動歷程")
def show_audit_log_modal(logs):
    if logs:
        st.dataframe(pd.DataFrame(logs, columns=['時間', '動作', '代碼', '金額', '股數', '備註']), use_container_width=True, hide_index=True)
    else: st.info("無紀錄")

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
                user_match = None
                for k, v in users.items():
                    if str(k).strip().lower() == u.strip().lower() and str(v) == str(p):
                        user_match = k
                        break
                if user_match:
                    st.session_state.current_user = user_match
                    st.rerun()
                else: st.error("Failed")
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
        st.session_state.current_user = None; st.session_state.data = None; st.rerun()
    st.markdown("---")
    st.metric("💵 現金", f"${int(data['cash']):,}")
    
    with st.expander("💰 存入/取出資金"):
        amt = st.number_input("金額 (+存 / -取)", step=1000.0)
        if st.button("執行"):
            data['cash'] += amt
            data['principal'] += amt
            save_data(client, username, data)
            log_transaction(client, username, "資金異動", "CASH", amt, 0)
            st.success("已更新"); time.sleep(0.5); st.rerun()
            
    with st.expander("🔵 買入股票", expanded=True):
        b_code = st.text_input("代碼 (例: 2330, AAPL)").upper().strip()
        b_qty = st.number_input("股數", min_value=1, value=1000, step=100)
        b_price = st.number_input("單價", min_value=0.0, step=0.1, format="%.2f")
        b_type = st.radio("類型", ["現股", "融資"], horizontal=True)
        b_ratio = 1.0
        if b_type == "融資": b_ratio = st.slider("自備成數", 0.1, 1.0, 0.4)
        
        if st.button("確認買入", type="primary"):
            if b_code and b_price > 0:
                # [重要修正] 先判定市場決定匯率
                is_tw = is_taiwan_stock(b_code)
                rate = 1.0 if is_tw else get_usdtwd()
                
                # 計算成本
                cost_twd = b_qty * b_price * rate
                cash_need = cost_twd * b_ratio
                debt = cost_twd - cash_need
                
                if data['cash'] >= cash_need:
                    # 抓取名稱資訊 (若失敗不影響判斷)
                    info = fetch_stock_price_robust(b_code)
                    ex_type = 'tse' if is_tw else 'US'
                    
                    data['cash'] -= cash_need
                    new_lot = {'d': datetime.now().strftime('%Y-%m-%d'), 'p': b_price, 's': b_qty, 'debt': debt}
                    
                    if b_code not in data['h']:
                        data['h'][b_code] = {'n': info['n'], 'ex': ex_type, 's': 0, 'c': 0, 'lots': []}
                    
                    h = data['h'][b_code]
                    h['lots'].append(new_lot)
                    tot_s = sum(l['s'] for l in h['lots'])
                    tot_c = sum(l['s'] * l['p'] for l in h['lots'])
                    h['s'] = tot_s
                    h['c'] = tot_c / tot_s if tot_s else 0
                    
                    save_data(client, username, data)
                    log_transaction(client, username, "買入", b_code, b_price, b_qty)
                    st.success(f"買入 {b_code} 成功"); time.sleep(1); st.rerun()
                else: 
                    st.error(f"現金不足！需要 {int(cash_need):,} 元，但帳面僅有 {int(data['cash']):,} 元")
    
    with st.expander("🔴 賣出股票"):
        holdings = list(data['h'].keys())
        s_code = st.selectbox("選擇股票", ["請選擇"] + holdings)
        if s_code != "請選擇":
            h_curr = data['h'][s_code]
            st.caption(f"持有: {h_curr['s']} 股")
            s_qty = st.number_input("賣出股數", 1, int(h_curr['s']), int(h_curr['s']))
            s_price = st.number_input("賣出價格", 0.0)
            if st.button("確認賣出"):
                is_tw = is_taiwan_stock(s_code)
                rate = 1.0 if is_tw else get_usdtwd()
                rev_twd = s_qty * s_price * rate
                cost_basis = 0; debt_payback = 0; remain = s_qty; new_lots = []
                for lot in h_curr['lots']:
                    if remain > 0:
                        take = min(lot['s'], remain)
                        cost_basis += take * lot['p'] * rate
                        l_debt = lot.get('debt', 0)
                        debt_payback += l_debt * (take / lot['s']) if lot['s'] else 0
                        lot['s'] -= take
                        lot['debt'] -= l_debt * (take / lot['s']) if lot['s'] else 0
                        remain -= take
                        if lot['s'] > 0: new_lots.append(lot)
                    else: new_lots.append(lot)
                
                profit = rev_twd - cost_basis
                data['cash'] += (rev_twd - debt_payback)
                h_curr['lots'] = new_lots
                h_curr['s'] -= s_qty
                if h_curr['s'] > 0: h_curr['c'] = sum(l['s'] * l['p'] for l in new_lots) / h_curr['s']
                else: del data['h'][s_code]
                
                try:
                    spreadsheet = client.open(st.secrets["spreadsheet_name"])
                    ws_hist = get_worksheet(spreadsheet, f"Realized_{username}", default_header=['Date', 'Code', 'Name', 'Qty', 'BuyCost', 'SellRev', 'Profit', 'ROI'])
                    ws_hist.append_row([datetime.now().strftime('%Y-%m-%d'), s_code, h_curr.get('n'), s_qty, cost_basis, rev_twd, profit, (profit/cost_basis*100) if cost_basis else 0])
                except: pass
                save_data(client, username, data)
                log_transaction(client, username, "賣出", s_code, s_price, s_qty)
                st.success("賣出成功"); time.sleep(1); st.rerun()

    if st.button("📋 異動歷程"):
        logs = get_audit_logs(client, username)
        show_audit_log_modal(logs)

# --- Main Page ---
st.title(f"📈 資產管家")

quotes = st.session_state.get('quotes', {})
total_mkt = 0; total_cost = 0; total_debt = 0; day_gain = 0
table_rows = []

for code, info in data['h'].items():
    if info['s'] < 0.01: continue 
    q = quotes.get(code)
    if q and q['p'] > 0: curr_p = q['p']; info['last_p'] = curr_p 
    else:
        curr_p = info.get('last_p', 0)
        if curr_p == 0: curr_p = info.get('c', 0)
        q = {'chg': 0, 'pct': 0, 'n': info.get('n', code)}
    
    is_tw = is_taiwan_stock(code)
    rate = 1.0 if is_tw else data.get('usdtwd', 32.5)
    
    qty = info['s']; cost = info['c']
    mkt_val = qty * curr_p * rate
    cost_val = qty * cost * rate
    stock_debt = sum(l.get('debt', 0) for l in info['lots'])
    
    total_mkt += mkt_val
    total_cost += cost_val
    total_debt += stock_debt
    day_gain += (q.get('chg', 0) * qty * rate)
    
    p_gain = mkt_val - cost_val
    p_roi = (p_gain / (cost_val - stock_debt)) if (cost_val - stock_debt) > 0 else 0
    
    table_rows.append({
        "股票代碼": code, "公司名稱": info.get('n'), "股數": qty, 
        "成本": cost, "現價": curr_p,
        "日損益%": q.get('pct', 0) / 100, "日損益": q.get('chg', 0) * qty * rate,
        "總損益%": p_roi, "總損益": p_gain, "市值": mkt_val, "mkt_val_raw": mkt_val
    })

for row in table_rows: row["占比"] = (row["mkt_val_raw"] / total_mkt) if total_mkt > 0 else 0

net_asset = data['cash'] + total_mkt - total_debt
roi_pct = ((net_asset - data['principal']) / data['principal'] * 100) if data['principal'] else 0

if st.button("🔄 更新即時股價", type="primary", use_container_width=True):
    with st.spinner("更新中..."):
        data['usdtwd'] = get_usdtwd()
        st.session_state.quotes = update_prices_batch(data['h'])
        data['last_update'] = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
        save_data(client, username, data)
        record_asset_history(client, username, net_asset, data['principal'])
        st.rerun()

st.subheader("🏦 資產概況")
k1, k2, k3, k4 = st.columns(4)
k1.metric("💰 淨資產", f"${net_asset:,.0f}")
k2.metric("💵 現金餘額", f"${data['cash']:,.0f}")
k3.metric("📊 證券市值", f"${total_mkt:,.0f}")
k4.metric("📉 投入本金", f"${data['principal']:,.0f}")

st.subheader("📈 績效表現")
total_realized = sum(float(str(r.get('Profit', 0)).replace(',','')) for r in data.get('history', []))
total_profit_all = (net_asset - data['principal']) 
kp1, kp2, kp3, kp4 = st.columns(4)
kp1.metric("📅 今日損益", f"${day_gain:,.0f}")
kp2.metric("💰 總損益 (含已實現)", f"${total_profit_all:,.0f}")
kp3.metric("🏆 總報酬率 (ROI)", f"{roi_pct:+.2f}%")
kp4.metric("📥 其中已實現", f"${total_realized:,.0f}")

tab1, tab2, tab3, tab4 = st.tabs(["📋 庫存明細", "🗺️ 熱力圖", "📊 資產走勢", "📜 已實現損益"])

def style_color(v):
    try: return 'color: red' if float(v) > 0 else 'color: green' if float(v) < 0 else ''
    except: return ''

with tab1:
    if table_rows:
        df = pd.DataFrame(table_rows).drop(columns=['mkt_val_raw'])
        df = df[["股票代碼", "公司名稱", "股數", "成本", "現價", "日損益%", "日損益", "總損益%", "總損益", "市值", "占比"]]
        st.dataframe(df.style.format({
            "股數": "{:,.0f}", "成本": "{:,.2f}", "現價": "{:.2f}",
            "日損益%": "{:+.2%}", "日損益": "{:+,.0f}",
            "總損益%": "{:+.2%}", "總損益": "{:+,.0f}", "市值": "{:,.0f}", "占比": "{:.1%}"
        }).map(style_color, subset=['日損益%', '日損益', '總損益%', '總損益']), use_container_width=True, hide_index=True, height=500)
    else: st.info("尚無庫存")

with tab2:
    if table_rows:
        fig = px.treemap(pd.DataFrame(table_rows), path=['股票代碼'], values='mkt_val_raw', color='日損益%', color_continuous_scale='RdYlGn_r', color_continuous_midpoint=0)
        st.plotly_chart(fig, use_container_width=True)
    else: st.info("尚無資料")

with tab3:
    hist_data = data.get('asset_history', [])
    if hist_data:
        df_h = pd.DataFrame(hist_data)
        df_h['Date'] = pd.to_datetime(df_h['Date'])
        view_type = st.radio("顯示模式", ["💰 淨資產走勢 (金額)", "📈 累計報酬率比較 (%)"], horizontal=True)
        fig_trend = go.Figure()
        if view_type == "💰 淨資產走勢 (金額)":
            fig_trend.add_trace(go.Scatter(x=df_h['Date'], y=df_h['NetAsset'], name='淨資產', fill='tozeroy', line=dict(color='#00CC96')))
            fig_trend.add_trace(go.Scatter(x=df_h['Date'], y=df_h['Principal'], name='投入本金', line=dict(color='#EF553B', dash='dot')))
        else:
            df_h['ROI'] = ((df_h['NetAsset'] - df_h['Principal']) / df_h['Principal']) * 100
            fig_trend.add_trace(go.Scatter(x=df_h['Date'], y=df_h['ROI'], name='投資組合', line=dict(color='#00CC96', width=3)))
            start_date = df_h['Date'].iloc[0].strftime('%Y-%m-%d')
            benchmarks = get_benchmark_data(start_date)
            for i, (name, series) in enumerate(benchmarks.items()):
                fig_trend.add_trace(go.Scatter(x=series.index, y=series.values, name=name, line=dict(width=1.5, dash='dot')))
        st.plotly_chart(fig_trend, use_container_width=True)
    else: st.info("尚無歷史資產資料")

with tab4:
    if data.get('history'):
        st.dataframe(pd.DataFrame(data['history']), use_container_width=True, hide_index=True)
    else: st.info("尚無已實現紀錄")
