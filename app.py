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
APP_VERSION = "v4.4 (Data Parsing Fix)"

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
    except: return None

# --- 資料讀寫核心 (大幅強化容錯率) ---
def load_data(client, username):
    default = {'h': {}, 'cash': 0.0, 'principal': 0.0, 'history': [], 'asset_history': []}
    if not client or not username: return default
    
    # 1. 讀取 User Sheet (庫存)
    user_ws = get_worksheet(client, f"User_{username}")
    h_data = {}
    if user_ws:
        all_rows = user_ws.get_all_records() # 庫存結構較複雜，維持 get_all_records
        for r in all_rows:
            code = str(r.get('Code', '')).strip()
            if not code: continue
            try: lots = json.loads(r.get('Lots_Data', '[]'))
            except: lots = []
            h_data[code] = {
                'n': r.get('Name', ''), 'ex': r.get('Exchange', ''),
                's': float(r.get('Shares', 0) or 0), 'c': float(r.get('AvgCost', 0) or 0),
                'lots': lots
            }

    # 2. 讀取 Account Sheet (資金)
    acc_ws = get_worksheet(client, f"Account_{username}", rows="20", cols="2")
    acc_data = {}
    if acc_ws:
        for row in acc_ws.get_all_values():
            if len(row) >= 2: acc_data[row[0]] = row[1]

    # 3. 讀取 Realized History (已實現損益) - 改用 get_all_values 強制解析
    # 預期欄位: Date(0), Code(1), Name(2), Qty(3), BuyCost(4), SellRev(5), Profit(6), ROI(7)
    hist_ws = get_worksheet(client, f"Realized_{username}", default_header=['Date', 'Code', 'Name', 'Qty', 'BuyCost', 'SellRev', 'Profit', 'ROI'])
    hist_data = []
    if hist_ws:
        raw_rows = hist_ws.get_all_values()
        if len(raw_rows) > 1: # 確保有資料 (跳過標題)
            for row in raw_rows[1:]:
                # 補齊長度避免 index out of range
                row += [''] * (8 - len(row))
                hist_data.append({
                    'Date': row[0], 'Code': row[1], 'Name': row[2], 'Qty': row[3],
                    'BuyCost': row[4], 'SellRev': row[5], 'Profit': row[6], 'ROI': row[7]
                })

    # 4. 讀取 Asset History (資產走勢) - 改用 get_all_values 強制解析
    # 預期欄位: Date(0), NetAsset(1), Principal(2)
    asset_ws = get_worksheet(client, f"Hist_{username}", default_header=['Date', 'NetAsset', 'Principal'])
    asset_history = []
    if asset_ws:
        raw_rows = asset_ws.get_all_values()
        if len(raw_rows) > 1:
            for row in raw_rows[1:]:
                if len(row) >= 2: # 至少要有日期和淨值
                    asset_history.append({
                        'Date': row[0],
                        'NetAsset': row[1],
                        'Principal': row[2] if len(row) > 2 else row[1]
                    })

    return {
        'h': h_data,
        'cash': float(acc_data.get('Cash', 0)),
        'principal': float(acc_data.get('Principal', 0)),
        'last_update': acc_data.get('LastUpdate', ''),
        'usdtwd': float(acc_data.get('USDTWD', 32.5)),
        'history': hist_data,
        'asset_history': asset_history
    }

def save_data(client, username, data):
    if not client: return
    
    # 存資金
    acc_ws = get_worksheet(client, f"Account_{username}")
    if acc_ws:
        acc_ws.clear()
        acc_ws.update('A1', [['Key', 'Value'], ['Cash', data['cash']], ['Principal', data['principal']], ['LastUpdate', data.get('last_update', '')], ['USDTWD', data.get('usdtwd', 32.5)]])

    # 存庫存
    user_ws = get_worksheet(client, f"User_{username}")
    if user_ws:
        headers = ['Code', 'Name', 'Exchange', 'Shares', 'AvgCost', 'Lots_Data']
        rows = [headers]
        for code, info in data.get('h', {}).items():
            rows.append([
                code, info.get('n', ''), info.get('ex', ''),
                info.get('s', 0), info.get('c', 0),
                json.dumps(info.get('lots', []), ensure_ascii=False)
            ])
        user_ws.clear()
        user_ws.update('A1', rows)

def log_transaction(client, username, action, code, amount, shares, memo=""):
    ws = get_worksheet(client, f"Audit_{username}", default_header=['Time', 'Action', 'Code', 'Amount', 'Shares', 'Memo'])
    if ws:
        ts = (datetime.utcnow() + timedelta(hours=8)).strftime('%Y/%m/%d %H:%M:%S')
        ws.append_row([ts, action, code, amount, shares, memo])

def record_asset_history(client, username, net_asset, principal):
    ws = get_worksheet(client, f"Hist_{username}", default_header=['Date', 'NetAsset', 'Principal'])
    if ws:
        today = datetime.now().strftime('%Y-%m-%d')
        all_vals = ws.get_all_values()
        # 簡單邏輯：如果最後一筆是今天，則更新；否則新增
        if len(all_vals) > 1 and all_vals[-1][0] == today:
            row_idx = len(all_vals)
            ws.update(f"B{row_idx}:C{row_idx}", [[net_asset, principal]])
        else:
            ws.append_row([today, net_asset, principal])

def get_audit_logs(client, username, limit=50):
    ws = get_worksheet(client, f"Audit_{username}")
    if ws:
        vals = ws.get_all_values()
        if len(vals) > 1: return vals[1:][-limit:][::-1]
    return []

# --- 股價抓取核心 ---
@st.cache_data(ttl=300)
def get_usdtwd():
    try:
        t = yf.Ticker("USDTWD=X")
        return t.history(period="1d")['Close'].iloc[-1]
    except: return 32.5

def fetch_stock_price_robust(code, exchange=''):
    code = str(code).strip().upper()
    is_tw = (exchange in ['tse', 'otc', 'TW', 'TWO']) or \
            (code.endswith('.TW')) or (code.endswith('.TWO')) or \
            (code.replace('.TW','').replace('.TWO','').isdigit())
    
    if is_tw:
        clean_code = code.replace('.TW', '').replace('.TWO', '')
        queries = [f"tse_{clean_code}.tw", f"otc_{clean_code}.tw"]
        try:
            ts = int(time.time() * 1000)
            url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={'|'.join(queries)}&json=1&delay=0&_={ts}"
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
            r = requests.get(url, headers=headers, verify=False, timeout=5)
            data = r.json()
            if 'msgArray' in data:
                for item in data['msgArray']:
                    if item.get('n'): 
                        z = item.get('z', '-')
                        if z == '-': z = item.get('b', '').split('_')[0]
                        if z == '-' or z == '': z = item.get('a', '').split('_')[0]
                        if z == '-' or z == '': z = item.get('y', '0')
                        try: price = float(z)
                        except: price = 0.0
                        y_close = float(item.get('y', 0))
                        chg = price - y_close if price > 0 else 0
                        pct = (chg / y_close * 100) if y_close > 0 else 0
                        return {'p': price, 'chg': chg, 'pct': pct, 'n': item.get('n', code)}
        except Exception: pass

    try:
        yf_code = code
        if is_tw and '.TW' not in yf_code and '.TWO' not in yf_code: yf_code = f"{code}.TW"
        t = yf.Ticker(yf_code)
        price = 0.0; prev_close = 0.0
        if hasattr(t, 'fast_info') and 'last_price' in t.fast_info:
            price = t.fast_info['last_price']
            prev_close = t.fast_info.get('previous_close', 0)
        if price == 0 or price is None:
            hist = t.history(period="5d")
            if not hist.empty:
                price = hist['Close'].iloc[-1]
                prev_close = hist['Close'].iloc[-2] if len(hist) > 1 else price
        if price and price > 0:
            chg = price - prev_close
            pct = (chg / prev_close * 100) if prev_close > 0 else 0
            name = code
            try: name = t.info.get('shortName') or t.info.get('longName') or code
            except: pass
            return {'p': price, 'chg': chg, 'pct': pct, 'n': name}
    except Exception: pass
    return {'p': 0, 'chg': 0, 'pct': 0, 'n': code}

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
    else:
        st.info("無紀錄")

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
                    st.session_state.current_user = u; st.rerun()
                else: st.error("Failed")
    st.stop()

username = st.session_state.current_user
client = get_google_client()
if not client: st.error("Google Client Error"); st.stop()

if 'data' not in st.session_state or st.session_state.get('loaded_user') != username:
    st.session_state.data = load_data(client, username)
    st.session_state.loaded_user = username
data = st.session_state.data

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
                info = fetch_stock_price_robust(b_code)
                is_tw = info['p'] > 0 and ('.TW' in b_code or '.TWO' in b_code or b_code.isdigit())
                ex_type = 'tse' if is_tw else 'US'
                rate = 1.0 if is_tw else get_usdtwd()
                cost_twd = b_qty * b_price * rate
                cash_need = cost_twd * b_ratio
                debt = cost_twd - cash_need
                
                if data['cash'] >= cash_need:
                    data['cash'] -= cash_need
                    new_lot = {'d': datetime.now().strftime('%Y-%m-%d'), 'p': b_price, 's': b_qty, 'debt': debt}
                    if b_code not in data['h']: data['h'][b_code] = {'n': info['n'], 'ex': ex_type, 's': 0, 'c': 0, 'lots': []}
                    h = data['h'][b_code]
                    h['lots'].append(new_lot)
                    tot_s = sum(l['s'] for l in h['lots'])
                    tot_c = sum(l['s'] * l['p'] for l in h['lots'])
                    h['s'] = tot_s
                    h['c'] = tot_c / tot_s if tot_s else 0
                    save_data(client, username, data)
                    log_transaction(client, username, "買入", b_code, b_price, b_qty)
                    st.success(f"買入 {b_code} 成功"); time.sleep(1); st.rerun()
                else: st.error("現金不足")
    
    with st.expander("🔴 賣出股票"):
        holdings = list(data['h'].keys())
        s_code = st.selectbox("選擇股票", ["請選擇"] + holdings)
        if s_code != "請選擇":
            h_curr = data['h'][s_code]
            st.caption(f"持有: {h_curr['s']} 股")
            s_qty = st.number_input("賣出股數", 1, int(h_curr['s']), int(h_curr['s']))
            s_price = st.number_input("賣出價格", 0.0)
            if st.button("確認賣出"):
                is_tw = (h_curr.get('ex') in ['tse', 'otc']) or str(s_code).replace('.TW','').isdigit()
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
                if h_curr['s'] <= 0: del data['h'][s_code]
                
                ws_hist = get_worksheet(client, f"Realized_{username}")
                if ws_hist:
                    ws_hist.append_row([datetime.now().strftime('%Y-%m-%d'), s_code, h_curr.get('n'), s_qty, cost_basis, rev_twd, profit, (profit/cost_basis*100) if cost_basis else 0])
                save_data(client, username, data)
                log_transaction(client, username, "賣出", s_code, s_price, s_qty)
                st.success("賣出成功"); time.sleep(1); st.rerun()

    if st.button("📋 異動歷程"):
        logs = get_audit_logs(client, username)
        show_audit_log_modal(logs)

st.title(f"📈 資產管家")

# 準備計算資料
quotes = st.session_state.get('quotes', {})
total_mkt = 0; total_cost = 0; total_debt = 0; day_gain = 0
table_rows = []

for code, info in data['h'].items():
    q = quotes.get(code, {'p': info['c'], 'chg': 0, 'pct': 0, 'n': info.get('n', code)})
    if q['n'] and q['n'] != code: info['n'] = q['n']
    
    s_code = str(code).upper()
    is_tw = (info.get('ex') in ['tse', 'otc', 'TW', 'TWO']) or \
            s_code.endswith('.TW') or s_code.endswith('.TWO') or \
            s_code.replace('.TW','').replace('.TWO','').isdigit()
    
    rate = 1.0 if is_tw else data.get('usdtwd', 32.5)
    
    qty = info['s']
    cost = info['c']
    curr_p = q['p'] if q['p'] > 0 else cost
    
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
        "代碼": code, "名稱": info.get('n'), "股數": qty, 
        "成本": cost, "現價": curr_p,
        "日損益": q.get('chg', 0), "日漲跌幅": q.get('pct', 0) / 100,
        "總損益": p_gain, "報酬率": p_roi, "市值": mkt_val, "mkt_val_raw": mkt_val
    })

net_asset = data['cash'] + total_mkt - total_debt
roi_pct = ((net_asset - data['principal']) / data['principal'] * 100) if data['principal'] else 0

# 更新股價與紀錄
if st.button("🔄 更新即時股價", type="primary", use_container_width=True):
    with st.spinner("更新中..."):
        data['usdtwd'] = get_usdtwd()
        st.session_state.quotes = update_prices_batch(data['h'])
        data['last_update'] = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
        save_data(client, username, data)
        record_asset_history(client, username, net_asset, data['principal']) # 記錄資產走勢
        st.rerun()

# --- 恢復完整面板 (資產 + 績效) ---
st.subheader("🏦 資產概況")
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("💰 淨資產", f"${net_asset:,.0f}")
k2.metric("💵 現金餘額", f"${data['cash']:,.0f}")
k3.metric("📊 證券市值", f"${total_mkt:,.0f}")
k4.metric("📉 投入本金", f"${data['principal']:,.0f}")
k5.metric("💳 融資金額", f"${total_debt:,.0f}")

st.subheader("📈 績效表現")

# [Fix] 更強健的數值解析邏輯，處理 $ 符號與逗號
def safe_parse_profit(val):
    try:
        s = str(val).replace(',', '').replace('$', '').replace(' ', '')
        return float(s)
    except: return 0.0

total_realized = sum(safe_parse_profit(r.get('Profit', 0)) for r in data.get('history', []))
day_pct = (day_gain / (total_mkt - day_gain)) * 100 if (total_mkt - day_gain) > 0 else 0

kp1, kp2, kp3, kp4 = st.columns(4)
kp1.metric("📅 今日損益", f"${day_gain:,.0f}", f"{day_pct:+.2f}%")
kp2.metric("💰 總損益 (含已實現)", f"${(net_asset - data['principal']):,.0f}")
kp3.metric("🏆 總報酬率", f"{roi_pct:+.2f}%")
kp4.metric("📥 已實現損益", f"${total_realized:,.0f}")

st.markdown("---")

# --- UI 頁籤恢復 ---
tab1, tab2, tab3, tab4 = st.tabs(["📋 庫存明細", "🗺️ 熱力圖", "📊 資產走勢", "📜 已實現損益"])

def style_color(v):
    try: return 'color: red' if float(v) > 0 else 'color: green' if float(v) < 0 else ''
    except: return ''

with tab1:
    if table_rows:
        df = pd.DataFrame(table_rows).drop(columns=['mkt_val_raw'])
        st.dataframe(
            df.style.format({
                "股數": "{:,.0f}", "成本": "{:,.2f}", "現價": "{:.2f}",
                "日損益": "{:+.2f}", "日漲跌幅": "{:+.2%}",
                "總損益": "{:+,.0f}", "報酬率": "{:+.2%}", "市值": "{:,.0f}"
            }).map(style_color, subset=['日損益', '日漲跌幅', '總損益', '報酬率']),
            use_container_width=True, hide_index=True, height=500
        )
    else:
        st.info("尚無庫存，請從左側新增。")

with tab2:
    if table_rows:
        df_tree = pd.DataFrame(table_rows)
        fig = px.treemap(
            df_tree, path=['代碼'], values='mkt_val_raw', color='日漲跌幅',
            color_continuous_scale='RdYlGn_r', color_continuous_midpoint=0,
            hover_data=['名稱', '總損益', '報酬率']
        )
        fig.update_layout(margin=dict(t=0, l=0, r=0, b=0))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("尚無資料")

with tab3:
    hist_data = data.get('asset_history', [])
    if hist_data:
        df_h = pd.DataFrame(hist_data)
        df_h['Date'] = pd.to_datetime(df_h['Date'], errors='coerce')
        df_h = df_h.dropna(subset=['Date']) # 移除解析失敗的日期
        
        # 安全解析數值，處理可能的空白或異常字元
        def safe_float(x):
            try: return float(str(x).replace(',', ''))
            except: return 0.0
            
        df_h['NetAsset'] = df_h['NetAsset'].apply(safe_float)
        df_h['Principal'] = df_h['Principal'].apply(safe_float)
        
        fig_trend = go.Figure()
        fig_trend.add_trace(go.Scatter(x=df_h['Date'], y=df_h['NetAsset'], name='淨資產', fill='tozeroy'))
        fig_trend.add_trace(go.Scatter(x=df_h['Date'], y=df_h['Principal'], name='本金', line=dict(dash='dot')))
        st.plotly_chart(fig_trend, use_container_width=True)
    else:
        st.info("尚無歷史資產資料 (請執行一次更新即時股價以建立紀錄)")

with tab4:
    realized = data.get('history', [])
    if realized:
        df_r = pd.DataFrame(realized)
        st.dataframe(df_r, use_container_width=True, hide_index=True)
    else:
        st.info("尚無已實現損益紀錄")
