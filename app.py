import streamlit as st
import pandas as pd
import yfinance as yf
import requests # 新增 requests 用於手動抓取
import time
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
import urllib3

# 忽略 SSL 警告 (解決 Streamlit Cloud 連線證交所失敗的問題)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 設定頁面配置
st.set_page_config(page_title="全功能資產管家 Pro", layout="wide", page_icon="📈")

# --- 股票代碼與名稱對照表 ---
STOCK_MAP = {
    '2330.TW': '台積電', '2317.TW': '鴻海', '2454.TW': '聯發科',
    '2603.TW': '長榮', '2609.TW': '陽明', '2615.TW': '萬海',
    '3231.TW': '緯創', '2382.TW': '廣達', '3017.TW': '奇鋐',
    '2301.TW': '光寶科', '00685L.TW': '群益台指正2', '00670L.TW': '元大NASDAQ正2',
    'NVDA': '輝達', 'AAPL': '蘋果', 'TSLA': '特斯拉', 'AMD': '超微',
    'MSFT': '微軟', 'GOOG': '谷歌', 'AMZN': '亞馬遜',
    '0050.TW': '元大台灣50', 'SPY': 'S&P 500', 'QQQ': '納斯達克100'
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
            
            # 資料清洗
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

# --- 核心計算邏輯 (混合引擎 + SSL修復) ---

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
    手動連線證交所 API，並強制 verify=False 繞過 SSL 錯誤。
    取代 twstock 套件以解決 Streamlit Cloud 連線問題。
    """
    if not codes: return {}
    
    # 1. 組合查詢字串 (tse_2330.tw|otc_8271.tw)
    query_parts = []
    for c in codes:
        if '.TW' in c:
            # 上市
            raw = c.replace('.TW', '')
            query_parts.append(f"tse_{raw}.tw")
        elif '.TWO' in c:
            # 上櫃
            raw = c.replace('.TWO', '')
            query_parts.append(f"otc_{raw}.tw")
    
    if not query_parts: return {}
    
    query_str = "|".join(query_parts)
    timestamp = int(time.time() * 1000)
    url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={query_str}&json=1&delay=0&_={timestamp}"
    
    results = {}
    try:
        # 關鍵：verify=False 忽略憑證錯誤
        response = requests.get(url, verify=False, timeout=5)
        data = response.json()
        
        if 'msgArray' in data:
            for item in data['msgArray']:
                # 判斷是上市還是上櫃來還原代碼
                exchange = item.get('ex', '')
                code_raw = item.get('c', '')
                
                if exchange == 'tse':
                    original_code = f"{code_raw}.TW"
                elif exchange == 'otc':
                    original_code = f"{code_raw}.TWO"
                else:
                    original_code = code_raw # fallback

                # 解析價格 (z: 最近成交, y: 昨收)
                try:
                    price_str = item.get('z', '-')
                    if price_str == '-': # 若無成交，找最佳買賣價
                        price_str = item.get('b', '').split('_')[0]
                    
                    price = float(price_str) if price_str and price_str != '-' else 0.0
                    prev_close = float(item.get('y', 0.0))
                    
                    # 計算漲跌
                    if price > 0 and prev_close > 0:
                        change_val = price - prev_close
                        change_pct = (change_val / prev_close * 100)
                    else:
                        change_val = 0
                        change_pct = 0
                        
                    results[original_code] = {'p': price, 'chg': change_val, 'chg_pct': change_pct, 'realtime': True}
                except:
                    results[original_code] = {'p': 0, 'chg': 0, 'chg_pct': 0, 'realtime': False}
                    
    except Exception as e:
        st.error(f"證交所連線錯誤 (Handled): {e}")
        
    return results

@st.cache_data(ttl=10) 
def get_batch_market_data(codes, usdtwd_rate):
    """
    混合雙引擎：
    1. 台股 -> 使用手動 requests (verify=False)
    2. 美股 -> 使用 yfinance
    """
    if not codes: return {}
    
    tw_query = [c for c in codes if '.TW' in c or '.TWO' in c]
    other_query = [c for c in codes if c not in tw_query]
    
    results = {}
    
    # --- 引擎 1: 台股 (手動 requests) ---
    if tw_query:
        tw_results = fetch_twse_realtime(tw_query)
        results.update(tw_results)

    # --- 引擎 2: 美股 / 補漏 (yfinance) ---
    # 如果有美股，或者台股抓失敗，用 yfinance 補
    # 這裡我們只查美股，台股失敗就算了(避免重複變慢)，或者也可以把失敗的加進來
    # 簡單起見，只查美股
    
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

    # 確保所有 code 都有回傳值 (防呆)
    for c in codes:
        if c not in results:
             results[c] = {'p': 0, 'chg': 0, 'chg_pct': 0}

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
    st.markdown("<h1 style='text-align: center;'>🔐 股票資產管家 Pro</h1>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1,2,1])
    with c2:
        with st.form("login_form"):
            user_input = st.text_input("使用者名稱 (例如: Kevin)")
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
        st.info("若報酬率計算異常(水平線)，請點擊下方按鈕。")
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
    with st.expander("🔧 修正/刪除 (含刪除退款功能)"):
        del_list = list(data.get('h', {}).keys())
        if del_list:
            to_del_code = st.selectbox("選擇要處理的股票", ["請選擇"] + del_list)
            
            if to_del_code != "請選擇":
                # 取得該股票當前資訊
                info = data['h'][to_del_code]
                current_s = info.get('s', 0)
                current_c = info.get('c', 0)
                # 計算剩餘總成本 (這是當初從現金扣掉的錢)
                # 注意：這裡簡單估算剩餘股數的成本，若有融資需另外扣除債務，這裡簡化為現股邏輯
                rate = 1.0 if ('.TW' in to_del_code or '.TWO' in to_del_code) else get_usdtwd()
                total_cost_basis = current_s * current_c * rate
                
                st.write(f"📊 持有股數: {current_s}, 平均成本: {current_c}")
                st.write(f"💰 估算原始投入成本: ${int(total_cost_basis):,}")

                col_del_1, col_del_2 = st.columns(2)
                
                # 選項 A: 僅刪除紀錄 (錢不退回) - 適用於資料輸入錯誤，且你已經手動調整過現金
                with col_del_1:
                    if st.button("❌ 僅刪除代碼 (不退錢)", type="secondary"):
                        del data['h'][to_del_code]
                        save_data(sheet, data)
                        st.success(f"已刪除 {to_del_code}，現金未變動。")
                        time.sleep(1)
                        st.rerun()

                # 選項 B: 刪除並退款 (救星) - 適用於買錯了想直接復原
                with col_del_2:
                    if st.button("💸 刪除並退回現金 (復原)", type="primary"):
                        # 加回現金
                        data['cash'] += total_cost_basis
                        # 刪除庫存
                        del data['h'][to_del_code]
                        save_data(sheet, data)
                        st.success(f"已刪除 {to_del_code}，並將 ${int(total_cost_basis):,} 加回現金！")
                        time.sleep(1)
                        st.rerun()
st.markdown("---")
    with st.expander("⚙️ 進階：強制修改本金"):
        st.info(f"目前系統記錄本金: ${int(data.get('principal', 0)):,}")
        st.caption("因為手動補回現金會導致本金虛增，請在此修正為您「真正」從口袋拿出來的總金額。")
        
        # 讓您可以直接輸入正確的本金
        real_principal = st.number_input("設定正確本金", value=float(data.get('principal', 0)), step=10000.0)
        
        if st.button("確認修正本金"):
            data['principal'] = real_principal
            save_data(sheet, data)
            st.success(f"本金已修正為 ${int(real_principal):,}")
            time.sleep(1)
            st.rerun()


# --- 資料更新按鈕 ---
# 初始化 session state 中的 dashboard_data
if 'dashboard_data' not in st.session_state:
    st.session_state.dashboard_data = None

# 按鈕只負責「計算並存入 State」
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
        
        # 紀錄歷史與本金
        current_principal = data.get('principal', data['cash'])
        if client: record_history(client, username, net_asset, current_principal)

        # 計算 ROI
        total_realized_profit = sum(r.get('profit', 0) for r in data.get('history', []))
        roi_basis = current_principal if current_principal > 0 else 1
        total_roi_pct = ((net_asset - current_principal) / roi_basis) * 100

        # 將計算結果存入 session_state
        st.session_state.dashboard_data = {
            'net_asset': net_asset,
            'cash': data.get('cash', 0),
            'total_mkt_val': total_mkt_val,
            'current_principal': current_principal,
            'total_day_profit': total_day_profit,
            'unrealized_profit': unrealized_profit,
            'total_realized_profit': total_realized_profit,
            'total_roi_pct': total_roi_pct,
            'final_rows': final_rows,
            'temp_list': temp_list
        }

# --- 顯示層 ---
if st.session_state.dashboard_data:
    # 從 state 取出資料
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
    kp2.metric("📄 未實現損益", f"${int(d['unrealized_profit']):+,}")
    kp3.metric("💰 已實現損益", f"${int(d['total_realized_profit']):+,}")
    kp4.metric("🏆 總報酬率 (ROI)", f"{d['total_roi_pct']:+.2f}%")

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
        st.caption("ℹ️ 資產走勢分析：可切換查看「獲利金額」或「報酬率」 (已排除入金造成的資產虛增)")
        
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

                    # 避免本金為 0
                    dfh['Principal'] = dfh.apply(lambda x: x['NetAsset'] if x['Principal'] == 0 else x['Principal'], axis=1)
                    dfh = dfh.sort_values('Date')

                    # [核心公式] 損益 = 淨資產 - 本金
                    dfh['Profit_Val'] = dfh['NetAsset'] - dfh['Principal']
                    dfh['ROI_Pct'] = (dfh['Profit_Val'] / dfh['Principal']) * 100
                    
                    # 這裡切換 Radio Button 時，因為外層不在 button 內，所以圖表不會消失
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
