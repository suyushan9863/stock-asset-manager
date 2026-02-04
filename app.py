import streamlit as st
import pandas as pd
import yfinance as yf
import requests
import time
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
import urllib3

# 忽略 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 設定頁面配置 (注意：這裡加了 v2.0 方便您確認更新成功)
st.set_page_config(page_title="全功能資產管家 Pro v2.0", layout="wide", page_icon="📈")

# --- 股票代碼與名稱對照表 (可自行擴充) ---
STOCK_MAP = {
    '2330.TW': '台積電', '2317.TW': '鴻海', '2454.TW': '聯發科',
    '2603.TW': '長榮', '2609.TW': '陽明', '2615.TW': '萬海',
    '3231.TW': '緯創', '2382.TW': '廣達', '3017.TW': '奇鋐',
    '2301.TW': '光寶科', '6488.TWO': '環球晶', '8271.TWO': '宇瞻',
    '00685L.TW': '群益台指正2', '00670L.TW': '元大NASDAQ正2',
    'NVDA': '輝達', 'AAPL': '蘋果', 'TSLA': '特斯拉', 'AMD': '超微',
    'MSFT': '微軟', 'GOOG': '谷歌', 'AMZN': '亞馬遜',
    '0050.TW': '元大台灣50', 'SPY': 'S&P 500', 'QQQ': '納斯達克100','2303.TW': '聯電'
}

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
            history_sheet = spreadsheet.add_worksheet(title=worksheet_name, rows="1000", cols="3")
            history_sheet.append_row(['Date', 'NetAsset', 'Principal'])
        return history_sheet
    except: return None

def load_data(sheet):
    default_data = {'h': {}, 'cash': 0.0, 'principal': 0.0, 'history': []}
    if not sheet: return default_data
    try:
        raw_data = sheet.acell('A1').value
        if raw_data:
            data = json.loads(raw_data)
            if 'h' not in data: data['h'] = {}
            if 'cash' not in data: data['cash'] = 0.0
            if 'history' not in data: data['history'] = []
            if 'principal' not in data: data['principal'] = data.get('cash', 0.0)
            
            # 資料清洗與相容性處理
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

def record_history(client, username, net_asset, current_principal):
    hist_sheet = get_user_history_sheet(client, username)
    if hist_sheet and net_asset > 0:
        today = datetime.now().strftime('%Y-%m-%d')
        try:
            all_values = hist_sheet.get_all_values()
            if len(all_values) > 0 and len(all_values[0]) < 3:
                 hist_sheet.update_cell(1, 3, 'Principal')

            if len(all_values) > 1:
                last_row = all_values[-1]
                if last_row[0] == today:
                    row_index = len(all_values)
                    hist_sheet.update_cell(row_index, 2, int(net_asset))
                    hist_sheet.update_cell(row_index, 3, int(current_principal))
                    return
        except: pass
        hist_sheet.append_row([today, int(net_asset), int(current_principal)])

# --- 核心計算邏輯 ---

@st.cache_data(ttl=300)
def get_usdtwd():
    try:
        data = yf.download("USDTWD=X", period="1d", progress=False)
        if not data.empty:
            p = data['Close'].iloc[-1]
            if isinstance(p, pd.Series): p = p.iloc[0]
            return float(p)
        return 32.5
    except: return 32.5

def fetch_twse_realtime(codes):
    """
    更新版：加入 User-Agent 偽裝成瀏覽器，解決 Streamlit Cloud 被擋的問題。
    """
    if not codes: return {}
    
    query_parts = []
    for c in codes:
        c_upper = c.upper()
        if '.TW' in c_upper and '.TWO' not in c_upper:
            # 上市
            raw = c_upper.replace('.TW', '')
            query_parts.append(f"tse_{raw}.tw")
        elif '.TWO' in c_upper:
            # 上櫃
            raw = c_upper.replace('.TWO', '')
            query_parts.append(f"otc_{raw}.tw")
    
    if not query_parts: return {}
    
    query_str = "|".join(query_parts)
    timestamp = int(time.time() * 1000)
    url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={query_str}&json=1&delay=0&_={timestamp}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": "https://mis.twse.com.tw/stock/fibest.jsp?stock=2330",
        "Connection": "keep-alive"
    }

    results = {}
    try:
        session = requests.Session()
        response = session.get(url, headers=headers, verify=False, timeout=10)
        
        if response.status_code != 200:
            st.error(f"證交所連線被拒 (Code {response.status_code})")
            return {}

        data = response.json()
        
        if 'msgArray' in data:
            for item in data['msgArray']:
                exchange = item.get('ex', '')
                code_raw = item.get('c', '')
                
                if exchange == 'tse':
                    original_code = f"{code_raw}.TW"
                elif exchange == 'otc':
                    original_code = f"{code_raw}.TWO"
                else:
                    original_code = code_raw

                try:
                    price_str = item.get('z', '-')
                    if price_str == '-':
                        bid = item.get('b', '').split('_')[0]
                        ask = item.get('a', '').split('_')[0]
                        if bid and bid != '-': price_str = bid
                        elif ask and ask != '-': price_str = ask
                    
                    price = float(price_str) if price_str and price_str != '-' else 0.0
                    prev_close = float(item.get('y', 0.0))
                    
                    if price > 0 and prev_close > 0:
                        change_val = price - prev_close
                        change_pct = (change_val / prev_close * 100)
                    else:
                        change_val = 0; change_pct = 0
                        
                    results[original_code] = {'p': price, 'chg': change_val, 'chg_pct': change_pct, 'realtime': True}
                except:
                    results[original_code] = {'p': 0, 'chg': 0, 'chg_pct': 0, 'realtime': False}
                    
    except Exception as e:
        pass
        
    return results

@st.cache_data(ttl=10) 
def get_batch_market_data(codes, usdtwd_rate):
    if not codes: return {}
    
    tw_query = [c for c in codes if '.TW' in c or '.TWO' in c]
    other_query = [c for c in codes if c not in tw_query]
    
    results = {}
    
    # 1. 台股
    if tw_query:
        tw_results = fetch_twse_realtime(tw_query)
        results.update(tw_results)

    # 2. 美股
    if other_query:
        try:
            yf_data = yf.download(other_query, period="5d", group_by='ticker', progress=False, auto_adjust=False)
            for code in other_query:
                try:
                    hist = yf_data if len(other_query) == 1 else yf_data[code]
                    if 'Close' in hist.columns:
                        clean = hist['Close'].dropna()
                        if not clean.empty:
                            price = float(clean.iloc[-1])
                            prev_close = float(clean.iloc[-2]) if len(clean) >= 2 else price
                            
                            change_val = price - prev_close
                            change_pct = (change_val / prev_close * 100) if prev_close else 0
                            
                            results[code] = {'p': price, 'chg': change_val, 'chg_pct': change_pct}
                        else:
                            if code not in results: results[code] = {'p': 0, 'chg': 0, 'chg_pct': 0}
                except:
                    if code not in results: results[code] = {'p': 0, 'chg': 0, 'chg_pct': 0}
        except: pass

    # 防呆
    for c in codes:
        if c not in results:
             results[c] = {'p': 0, 'chg': 0, 'chg_pct': 0}

    # 3. 手動更新覆蓋
    if 'manual_prices' in st.session_state:
        for m_code, m_price in st.session_state.manual_prices.items():
            if m_code in results and m_price > 0:
                results[m_code]['p'] = m_price
                results[m_code]['chg'] = 0
                results[m_code]['chg_pct'] = 0
            elif m_code not in results and m_price > 0:
                results[m_code] = {'p': m_price, 'chg': 0, 'chg_pct': 0}

    return results

@st.cache_data(ttl=3600)
def get_benchmark_data(start_date):
    tickers = ['0050.TW', 'SPY', 'QQQ']
    try:
        df = yf.download(tickers, start=start_date, group_by='ticker', progress=False, auto_adjust=False)
        benchmarks = {}
        for t in tickers:
            sub_df = df if len(tickers) == 1 else df[t]
            if 'Close' in sub_df.columns:
                series = sub_df['Close'].dropna()
                if not series.empty:
                    start_val = series.iloc[0]
                    if start_val > 0:
                        benchmarks[t] = ((series / start_val) - 1) * 100
        return benchmarks
    except: return {}

# --- 登入介面 ---
if 'current_user' not in st.session_state:
    st.session_state.current_user = None

if not st.session_state.current_user:
    st.markdown("<h1 style='text-align: center;'>🔐 股票資產管家 Pro v2.0</h1>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1,2,1])
    with c2:
        with st.form("login_form"):
            user_input = st.text_input("使用者名稱")
            pwd_input = st.text_input("密碼", type="password")
            submit = st.form_submit_button("登入", use_container_width=True)
            
            if submit:
                users_db = st.secrets.get("passwords", {})
                if user_input in users_db and str(users_db[user_input]) == str(pwd_input):
                    st.session_state.current_user = user_input
                    st.success("登入成功！")
                    st.rerun()
                else:
                    st.error("帳號或密碼錯誤")
    st.stop()

# --- 主程式 ---
username = st.session_state.current_user

with st.sidebar:
    st.info(f"👤 User: **{username}**")
    if st.button("登出"):
        st.session_state.current_user = None
        if 'data' in st.session_state: del st.session_state.data
        if 'sheet' in st.session_state: del st.session_state.sheet
        if 'dashboard_data' in st.session_state: del st.session_state.dashboard_data
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

# --- 側邊欄：資金與下單 ---
with st.sidebar:
    st.header("💰 資金與交易")
    st.metric("現金餘額", f"${int(data.get('cash', 0)):,}")
    
    with st.expander("⚙️ 系統設定 / 本金校正"):
        st.info("若報酬率計算異常，請點擊下方按鈕進行自動校正。")
        if st.button("🔄 自動校正本金"):
            current_stock_cost = 0
            for code, info in data.get('h', {}).items():
                s = info.get('s', 0)
                c = info.get('c', 0)
                debt = sum(l.get('debt', 0) for l in info.get('lots', []))
                rate = 1.0 if ('.TW' in code or '.TWO' in code) else get_usdtwd()
                current_stock_cost += (s * c * rate) - debt
            
            new_principal = data['cash'] + current_stock_cost
            data['principal'] = new_principal
            save_data(sheet, data)
            st.success(f"本金已校正為: ${int(new_principal):,}")
            st.rerun()

    with st.expander("💵 資金存提 (影響本金)"):
        cash_op = st.number_input("金額 (正存/負提)", step=1000.0)
        if st.button("執行異動"):
            data['cash'] += cash_op
            if 'principal' not in data: data['principal'] = 0.0
            data['principal'] += cash_op 
            save_data(sheet, data)
            st.success("資金已更新"); st.rerun()

    st.markdown("---")
    
    st.subheader("🔵 買入股票")
    code_in = st.text_input("買入代碼 (如 2330.TW, 6488.TWO)").strip().upper()
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
                    total_debt_repaid = 0
                    new_lots = []
                    for lot in lots:
                        if remain_to_sell > 0:
                            take_qty = min(lot['s'], remain_to_sell)
                            lot_cost = take_qty * lot['p'] * rate
                            lot_debt = lot.get('debt', 0) * (take_qty / lot['s']) if lot['s'] > 0 else 0
                            total_cost_basis += lot_cost
                            total_debt_repaid += lot_debt
                            lot['s'] -= take_qty
                            lot['debt'] -= lot_debt
                            remain_to_sell -= take_qty
                            if lot['s'] > 0: new_lots.append(lot)
                        else: new_lots.append(lot)
                    
                    realized_profit = sell_revenue - total_cost_basis
                    realized_roi = (realized_profit / total_cost_basis * 100) if total_cost_basis else 0
                    cash_back = sell_revenue - total_debt_repaid
                    data['cash'] += cash_back
                    
                    if new_lots:
                        data['h'][sell_code]['lots'] = new_lots
                        data['h'][sell_code]['s'] -= sell_qty
                        ts = sum(l['s'] for l in new_lots)
                        tc = sum(l['s']*l['p'] for l in new_lots)
                        data['h'][sell_code]['c'] = tc / ts if ts else 0
                    else: del data['h'][sell_code]
                    
                    if 'history' not in data: data['history'] = []
                    data['history'].append({
                        'd': datetime.now().strftime('%Y-%m-%d'), 'code': sell_code,
                        'name': STOCK_MAP.get(sell_code, sell_code), 'qty': sell_qty,
                        'buy_cost': total_cost_basis, 'sell_rev': sell_revenue,
                        'profit': realized_profit, 'roi': realized_roi
                    })
                    save_data(sheet, data)
                    st.success(f"賣出成功"); st.balloons(); st.rerun()

    st.markdown("---")
    
    # 修正/刪除
    with st.expander("🔧 修正/刪除 (含刪除退款)"):
        del_list = list(data.get('h', {}).keys())
        if del_list:
            to_del_code = st.selectbox("選擇要處理的股票", ["請選擇"] + del_list)
            
            if to_del_code != "請選擇":
                info = data['h'][to_del_code]
                current_s = info.get('s', 0)
                current_c = info.get('c', 0)
                rate = 1.0 if ('.TW' in to_del_code or '.TWO' in to_del_code) else get_usdtwd()
                total_cost_basis = current_s * current_c * rate
                
                st.write(f"📊 持有股數: {current_s}, 平均成本: {current_c}")
                st.write(f"💰 估算原始投入成本: ${int(total_cost_basis):,}")

                col_del_1, col_del_2 = st.columns(2)
                
                with col_del_1:
                    if st.button("❌ 僅刪除代碼", type="secondary"):
                        del data['h'][to_del_code]
                        save_data(sheet, data)
                        st.success(f"已刪除 {to_del_code}"); time.sleep(1); st.rerun()

                with col_del_2:
                    if st.button("💸 刪除並退回現金", type="primary"):
                        data['cash'] += total_cost_basis
                        del data['h'][to_del_code]
                        save_data(sheet, data)
                        st.success(f"已刪除並退款"); time.sleep(1); st.rerun()

    st.markdown("---")
    
    # 手動更新
    with st.expander("🆘 手動更新股價 (API 失敗時用)"):
        st.caption("如果 6488.TWO 抓不到價格，請在此手動輸入。")
        man_code = st.selectbox("選擇股票", list(data.get('h', {}).keys()), key="man_update_sel")
        man_price = st.number_input("輸入現價", min_value=0.0, step=0.5, key="man_update_price")
        
        if st.button("強制更新價格"):
            if 'manual_prices' not in st.session_state:
                st.session_state.manual_prices = {}
            st.session_state.manual_prices[man_code] = man_price
            st.success(f"{man_code} 價格暫時設定為 {man_price}")
            st.rerun()

    st.markdown("---")

    # 強制修改本金
    with st.expander("⚙️ 進階：強制修改本金"):
        st.info(f"目前系統記錄本金: ${int(data.get('principal', 0)):,}")
        st.caption("手動補回現金後，請在此修正為您真正投入的總金額。")
        
        real_principal = st.number_input("設定正確本金", value=float(data.get('principal', 0)), step=10000.0)
        
        if st.button("確認修正本金"):
            data['principal'] = real_principal
            save_data(sheet, data)
            st.success(f"本金已修正為 ${int(real_principal):,}")
            time.sleep(1)
            st.rerun()

# --- 資料更新按鈕 ---
if 'dashboard_data' not in st.session_state:
    st.session_state.dashboard_data = None

if st.button("🔄 更新即時報價 (極速版)", type="primary", use_container_width=True):
    with st.spinner('正在同步市場數據 (台股即時+美股)...'):
        usdtwd = get_usdtwd()
        h = data.get('h', {})
        batch_prices = get_batch_market_data(list(h.keys()), usdtwd)
        
        temp_list = []
        total_mkt_val = 0.0
        total_cost_val = 0.0
        total_debt = 0.0
        total_day_profit = 0.0
        
        for code, info in h.items():
            market_info = batch_prices.get(code, {'p': info['c'], 'chg': 0, 'chg_pct': 0})
            cur_p = market_info['p'] if market_info['p'] > 0 else info['c']
            
            rate = 1.0 if ('.TW' in code or '.TWO' in code) else usdtwd
            s_val = float(info['s'])
            c_val = float(info['c'])
            p_val = float(cur_p)
            
            mkt_val = p_val * s_val * rate
            cost_val = c_val * s_val * rate
            stock_debt = sum(l.get('debt', 0) for l in info.get('lots', []))
            actual_principal = cost_val - stock_debt
            
            total_profit_val = mkt_val - cost_val
            total_profit_pct = (total_profit_val / actual_principal * 100) if actual_principal > 0 else 0
            
            day_profit_val = market_info['chg'] * s_val * rate
            total_day_profit += day_profit_val
            
            total_mkt_val += mkt_val
            total_cost_val += cost_val
            total_debt += stock_debt

            name = STOCK_MAP.get(code, code)
            temp_list.append({
                "raw_code": code, "股票代碼": code, "公司名稱": name,
                "股數": int(s_val), "成本": c_val, "現價": p_val,
                "日損益%": market_info['chg_pct'] / 100, "日損益": day_profit_val,
                "總損益%": total_profit_pct / 100, "總損益": total_profit_val,
                "市值": mkt_val, "mkt_val_raw": mkt_val
            })

        final_rows = []
        for item in temp_list:
            weight = (item['mkt_val_raw'] / total_mkt_val) if total_mkt_val > 0 else 0
            item["占比"] = weight
            final_rows.append(item)

        net_asset = (total_mkt_val + data['cash']) - total_debt
        unrealized_profit = total_mkt_val - total_cost_val
        
        # 取得已實現損益
        total_realized_profit = sum(r.get('profit', 0) for r in data.get('history', []))
        
        # === 關鍵修改：總損益 = 未實現 + 已實現 ===
        total_profit_sum = unrealized_profit + total_realized_profit
        
        current_principal = data.get('principal', data['cash'])
        if client: record_history(client, username, net_asset, current_principal)

        # === 關鍵修改：ROI = (總損益 / 本金) ===
        roi_basis = current_principal if current_principal > 0 else 1
        total_roi_pct = (total_profit_sum / roi_basis) * 100

        st.session_state.dashboard_data = {
            'net_asset': net_asset,
            'cash': data.get('cash', 0),
            'total_mkt_val': total_mkt_val,
            'current_principal': current_principal,
            'total_day_profit': total_day_profit,
            'unrealized_profit': unrealized_profit,
            'total_realized_profit': total_realized_profit,
            'total_profit_sum': total_profit_sum,  # 新增欄位
            'total_roi_pct': total_roi_pct,        # 新的 ROI
            'final_rows': final_rows,
            'temp_list': temp_list
        }

# --- 顯示層 ---
if st.session_state.dashboard_data:
    d = st.session_state.dashboard_data
    
    st.subheader("🏦 資產概況")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("💰 淨資產", f"${int(d['net_asset']):,}")
    k2.metric("💵 現金餘額", f"${int(d['cash']):,}")
    k3.metric("📊 證券市值", f"${int(d['total_mkt_val']):,}")
    k4.metric("📉 投入本金", f"${int(d['current_principal']):,}")
    st.markdown("---")
    
    st.subheader("📈 績效表現")
    kp1, kp2, kp3, kp4 = st.columns(4)
    kp1.metric("📅 今日損益", f"${int(d['total_day_profit']):+,}")
    
    # 這裡就是您要的：合併顯示總損益
    kp2.metric("💰 總損益 (已+未)", f"${int(d['total_profit_sum']):+,}")
    
    # 這裡就是修正後的 ROI (會是正數)
    kp3.metric("🏆 總報酬率 (ROI)", f"{d['total_roi_pct']:+.2f}%")
    
    # 第四欄顯示已實現供參考
    kp4.metric("📥 其中已實現", f"${int(d['total_realized_profit']):+,}")

    tab1, tab2, tab3, tab4 = st.tabs(["📋 庫存明細", "🗺️ 熱力圖", "📊 資產走勢", "📜 已實現損益"])
    
    def color_profit(val):
        color = 'red' if val > 0 else 'green' if val < 0 else 'black'
        return f'color: {color}'

    with tab1:
        if d['final_rows']:
            df = pd.DataFrame(d['final_rows'])
            cols = ['股票代碼', '公司名稱', '股數', '成本', '現價', '日損益%', '日損益', '總損益%', '總損益', '市值', '占比']
            df = df[cols]
            styler = df.style.format({
                '股數': '{:,}', '成本': '{:,.2f}', '現價': '{:,.2f}',
                '日損益%': '{:+.2%}', '日損益': '{:+,.0f}',
                '總損益%': '{:+.2%}', '總損益': '{:+,.0f}',
                '市值': '{:,.0f}', '占比': '{:.1%}'
            }).map(color_profit, subset=['日損益%', '日損益', '總損益%', '總損益'])
            st.dataframe(styler, use_container_width=True, height=500, hide_index=True)
        else: st.info("無庫存資料")

    with tab2:
        if d['temp_list']:
            df_tree = pd.DataFrame(d['temp_list'])
            fig_tree = px.treemap(
                df_tree, path=['股票代碼'], values='mkt_val_raw', color='日損益%',
                color_continuous_scale='RdYlGn_r', color_continuous_midpoint=0,
                custom_data=['公司名稱', '日損益%']
            )
            fig_tree.update_traces(texttemplate="%{label}<br>%{customdata[0]}<br>%{customdata[1]:+.2%}", textposition="middle center")
            st.plotly_chart(fig_tree, use_container_width=True)
        else: st.info("無數據")

    with tab3:
        st.caption("ℹ️ 資產走勢分析：可切換查看「獲利金額」或「報酬率」")
        
        if client:
            hs = get_user_history_sheet(client, username)
            if hs:
                hvals = hs.get_all_values()
                if len(hvals) > 1:
                    headers = hvals[0]
                    dfh = pd.DataFrame(hvals[1:], columns=headers)
                    
                    dfh['Date'] = pd.to_datetime(dfh['Date'])
                    dfh['NetAsset'] = pd.to_numeric(dfh['NetAsset'], errors='coerce').fillna(0)
                    
                    if 'Principal' in dfh.columns:
                        dfh['Principal'] = pd.to_numeric(dfh['Principal'], errors='coerce').fillna(0)
                    else:
                        dfh['Principal'] = dfh['NetAsset'] 

                    dfh['Principal'] = dfh.apply(lambda x: x['NetAsset'] if x['Principal'] == 0 else x['Principal'], axis=1)
                    dfh = dfh.sort_values('Date')

                    dfh['Profit_Val'] = dfh['NetAsset'] - dfh['Principal']
                    dfh['ROI_Pct'] = (dfh['Profit_Val'] / dfh['Principal']) * 100
                    
                    view_type = st.radio("顯示模式", ["💰 總損益金額 (TWD)", "📈 累計報酬率 (%)"], horizontal=True)

                    fig = go.Figure()

                    if view_type == "💰 總損益金額 (TWD)":
                        fig.add_trace(go.Scatter(
                            x=dfh['Date'], y=dfh['Profit_Val'],
                            mode='lines+markers', name='總損益金額',
                            line=dict(color='#d62728', width=3),
                            fill='tozeroy', 
                            fillcolor='rgba(214, 39, 40, 0.1)',
                            hovertemplate='<b>日期</b>: %{x|%Y-%m-%d}<br><b>損益</b>: $%{y:,.0f}<extra></extra>'
                        ))
                        yaxis_format = ",.0f"
                        y_title = "損益金額 (TWD)"
                        
                    else:
                        fig.add_trace(go.Scatter(
                            x=dfh['Date'], y=dfh['ROI_Pct'],
                            mode='lines+markers', name='我的報酬率',
                            line=dict(color='#d62728', width=3),
                            hovertemplate='<b>日期</b>: %{x|%Y-%m-%d}<br><b>報酬率</b>: %{y:.2f}%<extra></extra>'
                        ))

                        if not dfh.empty:
                            start_date = dfh['Date'].min().strftime('%Y-%m-%d')
                            benchmarks = get_benchmark_data(start_date)
                            colors = {'0050.TW': 'blue', 'SPY': 'green', 'QQQ': 'purple'}
                            for name, series in benchmarks.items():
                                aligned_series = series[series.index >= dfh['Date'].min()]
                                fig.add_trace(go.Scatter(
                                    x=aligned_series.index, y=aligned_series.values,
                                    mode='lines', name=name,
                                    line=dict(color=colors.get(name, 'gray'), width=1, dash='dot'),
                                    hovertemplate=f'<b>{name}</b>: %{{y:.2f}}%<extra></extra>'
                                ))
                        yaxis_format = ".2f"
                        y_title = "累計報酬率 (%)"

                    fig.update_layout(
                        xaxis_title="日期", 
                        yaxis_title=y_title,
                        hovermode="x unified",
                        yaxis=dict(tickformat=yaxis_format),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                        height=500
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("尚無歷史資料，請先執行一次「更新即時報價」。")
        else:
            st.error("無法讀取歷史資料 (Client Error)")

    with tab4:
        history = data.get('history', [])
        if history:
            df_hist = pd.DataFrame(history[::-1])
            st.subheader(f"累計已實現損益: ${int(d['total_realized_profit']):+,}")
            if not df_hist.empty:
                df_hist = df_hist[['d', 'code', 'name', 'qty', 'buy_cost', 'sell_rev', 'profit', 'roi']]
                df_hist.columns = ['日期', '代碼', '名稱', '賣出股數', '總成本', '賣出收入', '獲利金額', '報酬率%']
                df_hist['報酬率%'] = df_hist['報酬率%'] / 100
                styler_h = df_hist.style.format({
                    '賣出股數': '{:,}', '總成本': '{:,.0f}', '賣出收入': '{:,.0f}',
                    '獲利金額': '{:+,.0f}', '報酬率%': '{:+.2%}'
                }).map(color_profit, subset=['獲利金額', '報酬率%'])
                st.dataframe(styler_h, use_container_width=True, hide_index=True)
        else: st.info("尚無賣出紀錄")

else:
    st.info("👆 請點擊上方按鈕，開始載入您的投資組合數據")
