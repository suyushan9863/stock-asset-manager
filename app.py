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
APP_VERSION = "v2.6 (Hotfix)"

# 設定頁面配置
st.set_page_config(page_title=f"全功能資產管家 Pro {APP_VERSION}", layout="wide", page_icon="📈")

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
            sheet = spreadsheet.add_worksheet(title=worksheet_name, rows="100", cols="10")
        return sheet
    except Exception as e:
        st.error(f"讀取使用者資料失敗: {e}")
        return None

def get_account_sheet(client, username):
    try:
        spreadsheet_name = st.secrets["spreadsheet_name"]
        spreadsheet = client.open(spreadsheet_name)
        worksheet_name = f"Account_{username}"
        try:
            sheet = spreadsheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title=worksheet_name, rows="20", cols="2")
        return sheet
    except: return None

def get_audit_sheet(client, username):
    try:
        spreadsheet_name = st.secrets["spreadsheet_name"]
        spreadsheet = client.open(spreadsheet_name)
        worksheet_name = f"Audit_{username}"
        try:
            sheet = spreadsheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title=worksheet_name, rows="1000", cols="6")
            sheet.append_row(['Time', 'Action', 'Code', 'Amount', 'Shares', 'Memo'])
        return sheet
    except: return None

def log_transaction(client, username, action, code, amount, shares, memo=""):
    try:
        sheet = get_audit_sheet(client, username)
        if sheet:
            now_ts = (datetime.utcnow() + timedelta(hours=8)).strftime('%Y/%m/%d %H:%M:%S')
            sheet.append_row([now_ts, action, code, amount, shares, memo])
    except: pass

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

def get_price_sync_sheet(client):
    try:
        spreadsheet_name = st.secrets["spreadsheet_name"]
        spreadsheet = client.open(spreadsheet_name)
        worksheet_name = "Price_Sync"
        try:
            sheet = spreadsheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title=worksheet_name, rows="100", cols="5")
        return sheet
    except: return None

def sync_us_prices_via_sheet(client, codes_dict):
    if not codes_dict or not client: return {}
    
    sync_sheet = get_price_sync_sheet(client)
    if not sync_sheet: return {}
    
    results = {}
    try:
        rows_to_write = [['Code', 'Price', 'Change', 'ChangePct', 'Name']]
        
        for c, info in codes_dict.items():
            ex = info.get('ex', 'US')
            if ex == 'PCX': ex = 'NYSEARCA'
            q_code = f"{ex}:{c}" if ex in ['NASDAQ', 'NYSE', 'NYSEARCA', 'AMEX'] else c
            
            rows_to_write.append([
                c,
                f'=GOOGLEFINANCE("{q_code}", "price")',
                f'=GOOGLEFINANCE("{q_code}", "change")',
                f'=GOOGLEFINANCE("{q_code}", "changepct")',
                f'=GOOGLEFINANCE("{q_code}", "name")'
            ])
            
        sync_sheet.clear()
        sync_sheet.update('A1', rows_to_write, value_input_option='USER_ENTERED')
        
        time.sleep(2.5) 
        
        try:
            raw_values = sync_sheet.get_all_values(value_render_option='UNFORMATTED_VALUE')
        except:
             time.sleep(2)
             raw_values = sync_sheet.get_all_values(value_render_option='UNFORMATTED_VALUE')

        for row in raw_values[1:]:
            if len(row) >= 5:
                r_code = row[0]
                r_price = row[1]
                r_chg = row[2]
                r_pct = row[3]
                r_name = row[4]
                
                try:
                    price = float(r_price) if isinstance(r_price, (int, float)) else 0.0
                    chg = float(r_chg) if isinstance(r_chg, (int, float)) else 0.0
                    pct = float(r_pct) if isinstance(r_pct, (int, float)) else 0.0
                    name = str(r_name) if r_name and r_name != '#N/A' else r_code
                except:
                    price = 0; chg = 0; pct = 0; name = r_code
                
                if r_code in codes_dict:
                      results[r_code] = {'p': price, 'chg': chg, 'chg_pct': pct, 'n': name}
                      
    except Exception as e:
        pass
        
    return results

def load_data(client, username):
    default_data = {'h': {}, 'cash': 0.0, 'principal': 0.0, 'history': []}
    if not client or not username: return default_data
    
    user_sheet = get_user_sheet(client, username)
    if not user_sheet: return default_data
    
    try:
        a1_val = user_sheet.acell('A1').value
        if a1_val and a1_val.startswith('{'):
            try:
                legacy_data = json.loads(a1_val)
                if 'h' not in legacy_data: legacy_data['h'] = {}
                return migrate_legacy_data(client, username, legacy_data)
            except: pass
            
        acc_sheet = get_account_sheet(client, username)
        acc_data = {}
        if acc_sheet:
            records = acc_sheet.get_all_values()
            for row in records:
                if len(row) >= 2:
                    acc_data[row[0]] = row[1]
        
        all_rows = user_sheet.get_all_values()
        h_data = {}
        
        if len(all_rows) > 1:
            headers = [str(h).strip() for h in all_rows[0]]
            idx_map = {h: i for i, h in enumerate(headers)}
            
            for row in all_rows[1:]:
                def get_val(col_names, default=''):
                    if isinstance(col_names, str): col_names = [col_names]
                    for cn in col_names:
                        if cn in idx_map and idx_map[cn] < len(row):
                            return row[idx_map[cn]]
                    return default

                code = str(get_val(['Code', '股票代碼'], '')).strip()
                if not code: continue
                
                try:
                    lots = json.loads(get_val(['Lots_Data', '明細', 'Lots'], '[]'))
                except: lots = []
                
                h_data[code] = {
                    'n': get_val(['Name', '公司名稱'], ''),
                    'ex': get_val(['Exchange', '交易所'], ''),
                    's': float(get_val(['Shares', '股數'], 0) or 0),
                    'c': float(get_val(['AvgCost', '平均成本'], 0) or 0),
                    'last_p': float(get_val(['LastPrice', '現價', '最後價格'], 0) or 0),
                    'last_chg': float(get_val(['LastChg', '最後漲跌'], 0) or 0),
                    'last_chg_pct': float(get_val(['LastChgPct', '最後漲跌幅'], 0) or 0),
                    'lots': lots
                }
            
        return {
            'h': h_data,
            'cash': float(acc_data.get('Cash', 0.0)),
            'principal': float(acc_data.get('Principal', 0.0)),
            'last_update': acc_data.get('LastUpdate', ''),
            'usdtwd': float(acc_data.get('USDTWD', 32.5)),
            'history': [] 
        }

    except Exception as e:
        pass
        
    return default_data

def migrate_legacy_data(client, username, data):
    save_data(client, username, data)
    return data

def save_data(client, username, data):
    if not client or not username: return
    
    try:
        acc_sheet = get_account_sheet(client, username)
        if acc_sheet:
            acc_rows = [
                ['Key', 'Value'],
                ['Cash', data.get('cash', 0.0)],
                ['Principal', data.get('principal', 0.0)],
                ['LastUpdate', data.get('last_update', '')],
                ['USDTWD', data.get('usdtwd', 32.5)]
            ]
            acc_sheet.clear()
            acc_sheet.update('A1', acc_rows)
            
        user_sheet = get_user_sheet(client, username)
        if user_sheet:
            try:
                existing_rows = user_sheet.get_all_values()
                if existing_rows:
                    current_headers = existing_rows[0]
                    if 'BuyType' not in current_headers and '交易類別' not in current_headers:
                        current_headers.insert(5, 'BuyType')
                    if 'BuyRatio' not in current_headers and '自備成數' not in current_headers:
                        current_headers.insert(6, 'BuyRatio')
                else:
                    current_headers = ['Code', 'Name', 'Exchange', 'Shares', 'AvgCost', 'BuyType', 'BuyRatio', 'LastPrice', 'LastChg', 'LastChgPct', 'Lots_Data']
            except:
                current_headers = ['Code', 'Name', 'Exchange', 'Shares', 'AvgCost', 'BuyType', 'BuyRatio', 'LastPrice', 'LastChg, LastChgPct', 'Lots_Data']
            
            h_map = {h.strip(): i for i, h in enumerate(current_headers)}
            
            def find_idx(aliases):
                for a in aliases:
                    if a in h_map: return h_map[a]
                return -1

            idx_code = find_idx(['Code', '股票代碼'])
            idx_name = find_idx(['Name', '公司名稱'])
            idx_ex = find_idx(['Exchange', '交易所'])
            idx_shares = find_idx(['Shares', '股數'])
            idx_cost = find_idx(['AvgCost', '平均成本'])
            idx_type = find_idx(['BuyType', '交易類別'])
            idx_ratio = find_idx(['BuyRatio', '自備成數'])
            idx_p = find_idx(['LastPrice', '現價', '最後價格'])
            idx_chg = find_idx(['LastChg', '最後漲跌'])
            idx_pct = find_idx(['LastChgPct', '最後漲跌幅'])
            idx_lots = find_idx(['Lots_Data', '明細', 'Lots'])

            rows = [current_headers]
            for code, info in data.get('h', {}).items():
                new_row = [''] * len(current_headers)
                if idx_code != -1: new_row[idx_code] = code
                if idx_name != -1: new_row[idx_name] = info.get('n', '')
                if idx_ex != -1: new_row[idx_ex] = info.get('ex', '')
                if idx_shares != -1: new_row[idx_shares] = info.get('s', 0)
                if idx_shares != -1: new_row[idx_shares] = info.get('s', 0)
                if idx_cost != -1: new_row[idx_cost] = info.get('c', 0)
                
                is_tw_s = (info.get('ex') in ['tse', 'otc', 'TW', 'TWO']) or (str(code)[0].isdigit())
                r_s = 1.0 if is_tw_s else 32.5
                
                cost_twd = info.get('s',0) * info.get('c',0) * r_s
                temp_lots = info.get('lots', [])
                tot_d = sum(l.get('debt', 0) for l in temp_lots)

                if tot_d > 0 and cost_twd > 0:
                      b_type = "融資"
                      net_r = (cost_twd - tot_d) / cost_twd
                      b_ratio_str = f"{net_r:.0%}"
                else:
                      b_type = "現股"
                      b_ratio_str = "100%"

                if idx_type != -1: new_row[idx_type] = b_type
                if idx_ratio != -1: new_row[idx_ratio] = b_ratio_str

                if idx_p != -1: new_row[idx_p] = info.get('last_p', 0)
                if idx_chg != -1: new_row[idx_chg] = info.get('last_chg', 0)
                if idx_pct != -1: new_row[idx_pct] = info.get('last_chg_pct', 0)
                if idx_lots != -1: new_row[idx_lots] = json.dumps(info.get('lots', []), ensure_ascii=False)
                rows.append(new_row)
            
            user_sheet.clear()
            user_sheet.update('A1', rows, value_input_option='USER_ENTERED')
            
    except Exception as e: st.error(f"存檔失敗: {e}")

# --- Audit Log Helper ---
def get_recent_audit_logs(client, username, limit=50):
    try:
        sheet = get_audit_sheet(client, username)
        if sheet:
            all_rows = sheet.get_all_values()
            if len(all_rows) <= 1: return []
            
            headers = all_rows[0]
            data_rows = all_rows[1:]
            
            recents = data_rows[-limit:]
            return [dict(zip(headers, r)) for r in recents][::-1] 
    except: pass
    return []

@st.dialog("📋 交易異動紀錄 (最近 50 筆)")
def show_audit_log_modal(audit_data):
    if audit_data:
        df = pd.DataFrame(audit_data)
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("尚無異動紀錄")

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
    更新版 (v2.6 Hotfix): 修復 API 抓取邏輯與代碼映射
    """
    if not codes: return {}
    
    query_str = "|".join(codes)
    timestamp = int(time.time() * 1000)
    url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={query_str}&json=1&delay=0&_={timestamp}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }

    results = {}
    try:
        session = requests.Session()
        response = session.get(url, headers=headers, verify=False, timeout=5)
        
        if response.status_code != 200:
            return {}

        data = response.json()
        
        if 'msgArray' in data:
            for item in data['msgArray']:
                c = item.get('c', '')
                ex = item.get('ex', '')
                
                # 嘗試多種價格欄位
                price_str = item.get('z', '-')
                if price_str == '-':
                    bid = item.get('b', '').split('_')[0]
                    ask = item.get('a', '').split('_')[0]
                    if bid and bid != '-' and bid != '0.00': price_str = bid
                    elif ask and ask != '-' and ask != '0.00': price_str = ask
                
                try:
                    price = float(price_str) if price_str and price_str != '-' else 0.0
                except: price = 0.0
                
                prev_close = float(item.get('y', 0.0))
                change_val = price - prev_close if price > 0 else 0.0
                change_pct = (change_val / prev_close * 100) if prev_close > 0 else 0.0

                res_obj = {'p': price, 'chg': change_val, 'chg_pct': change_pct, 'realtime': True}

                # 同時儲存純代碼與帶後綴代碼，確保映射成功
                results[c] = res_obj
                if ex == 'tse': results[f"{c}.TW"] = res_obj
                elif ex == 'otc': results[f"{c}.TWO"] = res_obj
                    
    except Exception as e:
        pass
        
    return results

def get_batch_market_data(portfolio_dict, usdtwd_rate):
    if not portfolio_dict: return {}
    
    tw_query = []
    other_query_dict = {} 
    
    # --- [v2.6 Fix] 增強路由邏輯 ---
    for code, info in portfolio_dict.items():
        ex = info.get('ex', '')
        s_code = str(code).strip()
        
        # 判斷是否為台股：1. Exchange 明確 2. Exchange 為空但全數字
        is_tw = (ex in ['tse', 'otc', 'TW', 'TWO']) or (not ex and s_code.isdigit())

        if is_tw:
            prefix = 'otc' if ex in ['otc', 'TWO'] else 'tse'
            tw_query.append(f"{prefix}_{code}.tw")
        else:
            other_query_dict[code] = info
    # -------------------------------

    results = {}
    
    # 1. 台股
    if tw_query:
        raw_tw_results = fetch_twse_realtime(tw_query)
        for raw_k, v in raw_tw_results.items():
            # 嘗試正規化 Key (移除 .TW)
            pure_k = raw_k.replace('.TW', '').replace('.TWO', '')
            results[pure_k] = v
            # 保留原始 Key 以防萬一
            results[raw_k] = v

    # 2. 美股 (透過 Google Sheet Sync)
    if other_query_dict:
        try:
             scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
             creds = ServiceAccountCredentials.from_json_keyfile_dict(st.secrets["service_account_info"], scope)
             temp_client = gspread.authorize(creds)
             
             us_results = sync_us_prices_via_sheet(temp_client, other_query_dict)
             results.update(us_results)
             
        except Exception as e:
             pass

    # 防呆
    for c in portfolio_dict.keys():
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

def update_dashboard_data(use_realtime=True):
    if 'data' not in st.session_state or st.session_state.data is None:
        return

    data = st.session_state.data
    client = st.session_state.client
    username = st.session_state.current_user
    
    if use_realtime:
        with st.spinner('正在同步市場數據 (台股即時+美股)...'):
            usdtwd = get_usdtwd()
            h = data.get('h', {})
            batch_prices = get_batch_market_data(h, usdtwd)
            
            now_ts = (datetime.utcnow() + timedelta(hours=8)).strftime('%Y/%m/%d %H:%M:%S')
            data['last_update'] = now_ts
            data['usdtwd'] = usdtwd
    else:
        usdtwd = data.get('usdtwd', 32.5)
        h = data.get('h', {})
        batch_prices = {} 
        now_ts = data.get('last_update', '尚無更新紀錄') 

    temp_list = []
    total_mkt_val = 0.0
    total_cost_val = 0.0
    total_debt = 0.0
    total_day_profit = 0.0
    
    for code, info in h.items():
        if not info.get('ex'):
            if use_realtime:
                try:
                    _, _, _, resolved_ex = resolve_stock_info(code)
                    if resolved_ex:
                        info['ex'] = resolved_ex
                except: pass
            if str(code)[0].isdigit() and not info.get('ex'):
                 info['ex'] = 'tse'

        if use_realtime:
            market_info = batch_prices.get(code, {'p': info['c'], 'chg': 0, 'chg_pct': 0})
            
            info['last_p'] = market_info['p']
            info['last_chg'] = market_info['chg']
            info['last_chg_pct'] = market_info['chg_pct']
        else:
            last_p = info.get('last_p', info['c'])
            last_chg = info.get('last_chg', 0)
            last_chg_pct = info.get('last_chg_pct', 0)
            market_info = {'p': last_p, 'chg': last_chg, 'chg_pct': last_chg_pct}

        # --- [v2.6 Fix] 價格為 0 時的強力防呆 (防止損益 -200%) ---
        fetched_price = float(market_info.get('p', 0))
        cur_p = fetched_price if fetched_price > 0.01 else float(info.get('c', 0))
        # ----------------------------------------------------
        
        ex_val = info.get('ex', '')
        s_code = str(code).strip()
        is_tw_stock = (ex_val in ['tse', 'otc', 'TW', 'TWO']) or (s_code and s_code[0].isdigit())
        rate = 1.0 if is_tw_stock else usdtwd

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
        total_cost_val += cost_val
        total_debt += stock_debt

        if stock_debt > 1:
            disp_type = "融資"
            disp_ratio = (cost_val - stock_debt) / cost_val if cost_val else 1.0
        else:
            disp_type = "現股"
            disp_ratio = 1.0

        stock_name = info.get('n', code)
        
        if stock_name == code:
            _, fetched_name, _, _ = resolve_stock_info(code)
            if fetched_name != code:
                stock_name = fetched_name
                data['h'][code]['n'] = stock_name 

        temp_list.append({
            "raw_code": code, "股票代碼": code, "公司名稱": stock_name, "Exchange": ex_val,
            "交易類別": disp_type, "自備成數": f"{disp_ratio:.0%}",
            "股數": int(s_val), "成本": c_val, "現價": p_val,
            "日損益%": market_info['chg_pct'] / 100, "日損益": day_profit_val,
            "總損益%": total_profit_pct / 100, "總損益": total_profit_val,
            "市值": mkt_val, "mkt_val_raw": mkt_val
        })

    final_rows = []
    for item in temp_list:
        weight = (item['mkt_val_raw'] / total_mkt_val) if total_mkt_val > 0 else 0
        item["投資比例"] = weight
        final_rows.append(item)

    net_asset = (total_mkt_val + data['cash']) - total_debt
    unrealized_profit = total_mkt_val - total_cost_val
    
    total_realized_profit = sum(r.get('profit', 0) for r in data.get('history', []))
    total_profit_sum = unrealized_profit + total_realized_profit
    
    current_principal = data.get('principal', data['cash'])
    
    if use_realtime: 
        save_data(client, username, data)
        if client: record_history(client, username, net_asset, current_principal)

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
        'total_profit_sum': total_profit_sum,
        'total_profit_sum': total_profit_sum,
        'total_roi_pct': total_roi_pct,
        'total_debt': total_debt,
        'final_rows': final_rows,
        'temp_list': temp_list,
        'last_update_ts': now_ts,
        'usdtwd': usdtwd
    }

def resolve_stock_info(user_input):
    """
    輸入: 股票代碼 (e.g. "2330", "2330.TW", "NVDA")
    輸出: (final_code, stock_name, success, exchange_type)
    """
    user_input = user_input.strip().upper()
    if not user_input:
        return "", "", False, ""

    if user_input[0].isdigit():
        candidates = []
        if '.TW' in user_input or '.TWO' in user_input:
            candidates.append(user_input)
        else:
            candidates.append(f"tse_{user_input}.tw")
            candidates.append(f"otc_{user_input}.tw")

        query_list = []
        for c in candidates:
            if 'tse_' in c or 'otc_' in c:
                query_list.append(c)
            elif '.TW' in c:
                query_list.append(f"tse_{c.replace('.TW', '')}.tw")
            elif '.TWO' in c:
                query_list.append(f"otc_{c.replace('.TWO', '')}.tw")
        
        try:
            timestamp = int(time.time() * 1000)
            q_str = "|".join(query_list)
            url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={q_str}&json=1&delay=0&_={timestamp}"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            }
            res = requests.get(url, headers=headers, verify=False, timeout=5)
            data = res.json()
            
            if 'msgArray' in data:
                for item in data['msgArray']:
                    if 'n' in item and 'c' in item:
                        name = item['n']
                        code = item['c']
                        ex_key = item.get('ex', '')
                        final_ex = ex_key if ex_key in ['tse', 'otc'] else 'tse'
                        return code, name, True, final_ex
        except:
            pass
            
        return user_input.split('.')[0], user_input, True, "tse"

    else:
        try:
            t = yf.Ticker(user_input)
            ex_type = "US"
            name = user_input
            
            try:
                info = t.info
                yf_ex = info.get('exchange', '').upper()
                name = info.get('shortName') or info.get('longName') or user_input
                
                if yf_ex in ['NMS', 'NGM', 'NCM', 'NASDAQ']:
                    ex_type = "NASDAQ"
                elif yf_ex in ['NYQ', 'NYS', 'NYSE']:
                    ex_type = "NYSE"
                elif yf_ex in ['PCX', 'PNK', 'ASE', 'ASEX', 'NCM', 'NGM']:
                      ex_type = "NYSEARCA" if yf_ex == 'PCX' else "NASDAQ" if yf_ex in ['NCM', 'NGM'] else "NYSE"
                else:
                    ex_type = yf_ex
            except:
                scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
                creds = ServiceAccountCredentials.from_json_keyfile_dict(st.secrets["service_account_info"], scope)
                temp_client = gspread.authorize(creds)
                
                res_dict = sync_us_prices_via_sheet(temp_client, [user_input])
                if user_input in res_dict:
                    name = res_dict[user_input].get('n', user_input)
                    ex_type = "NASDAQ" 
            
            return user_input, name, True, ex_type
        except Exception as e:
            return user_input, user_input, True, "US"

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
    st.markdown(f"<h1 style='text-align: center;'>🔐 股票資產管家 Pro {APP_VERSION}</h1>", unsafe_allow_html=True)
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

# --- 模態對話框 (Dialog) ---
@st.dialog("📜 版本修改歷程")
def show_changelog():
    st.markdown("""
    **v2.6 Hotfix Update**
    1.  **修復 API 路由**: 修正因無法正確識別台股而導致價格歸零的問題。
    2.  **損益顯示正常化**: 修正因現價為 0 導致 ROI 顯示為 -200% 的計算錯誤 (自動 fallback 至成本)。
    3.  **系統穩定性**: 修復 Pandas KeyError (DataFrame 欄位缺失) 導致的程式崩潰問題。
    4.  **證交所連線**: 優化連線標頭與查詢邏輯，提升台股報價成功率。

    **v2.5 UI/UX Polish**
    * 介面互動優化、訊息顯示修復、資金更新即時性增強。
    """)

# --- 主程式 ---
username = st.session_state.current_user

with st.sidebar:
    if st.button("📜 版本修改歷程", use_container_width=True):
        show_changelog()
        
    st.info(f"👤 User: **{username}**")
    if st.button("登出"):
        st.session_state.current_user = None
        if 'data' in st.session_state: del st.session_state.data
        if 'dashboard_data' in st.session_state: del st.session_state.dashboard_data
        st.rerun()
    st.markdown("---")

if 'client' not in st.session_state: st.session_state.client = get_google_client()

if 'data' not in st.session_state or st.session_state.get('loaded_user') != username:
    if st.session_state.client:
        st.session_state.data = load_data(st.session_state.client, username)
        st.session_state.loaded_user = username
    else: st.session_state.data = None

client = st.session_state.client
data = st.session_state.data

if not client:
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
                ex = info.get('ex', 'US')
                rate = 1.0 if ex in ['TW', 'TWO'] else get_usdtwd()
                current_stock_cost += (s * c * rate) - debt
            
            new_principal = data['cash'] + current_stock_cost
            data['principal'] = new_principal
            save_data(client, username, data)
            log_transaction(client, username, "系統自動校正本金", "全部", new_principal, 0, "系統自動檢查")
            
            st.success(f"本金已校正為: ${int(new_principal):,}")
            st.rerun()

    with st.expander("💵 資金存提 (影響本金)"):
        if "fund_op_val" not in st.session_state: st.session_state.fund_op_val = 0.0
        
        if st.session_state.get("reset_fund"):
             st.session_state.fund_op_val = 0.0
             st.session_state.reset_fund = False
             
        cash_op = st.number_input("金額 (正存/負提)", step=1000.0, key="fund_op_val")
        if st.button("執行異動"):
            data['cash'] += cash_op
            if 'principal' not in data: data['principal'] = 0.0
            data['principal'] += cash_op 
            save_data(client, username, data)
            log_transaction(client, username, "資金存提", "現金", cash_op, 0, "存入/提款")
            
            update_dashboard_data(use_realtime=False)
            
            st.success("資金已更新")
            st.session_state.reset_fund = True
            st.rerun()

    st.markdown("---")
    
    st.subheader("🔵 買入股票")
    if "buy_code_in" not in st.session_state: st.session_state.buy_code_in = ""
    if "buy_shares_in" not in st.session_state: st.session_state.buy_shares_in = 1000
    if "buy_cost_in" not in st.session_state: st.session_state.buy_cost_in = 0.0

    if st.session_state.get("reset_buy"):
        st.session_state.buy_code_in = ""
        st.session_state.buy_shares_in = 1000
        st.session_state.buy_cost_in = 0.0
        st.session_state.reset_buy = False
        
    code_in = st.text_input("買入代碼 (如 2330, 6488)", key="buy_code_in").strip().upper()
    
    shares_in = st.number_input("買入股數", min_value=1, step=100, key="buy_shares_in")
    cost_in = st.number_input("買入單價", min_value=0.0, step=0.1, format="%.2f", key="buy_cost_in")
    trade_type = st.radio("類別", ["現股", "融資"], horizontal=True, key="buy_type_in")
    margin_ratio = 1.0
    if trade_type == "融資":
        margin_ratio = st.slider("自備款成數", 0.1, 0.9, 0.4, 0.1, key="buy_margin_in")

    if st.button("確認買入", type="primary"):
        if code_in and cost_in > 0:
            if 'h' not in data: data['h'] = {}
            
            checked_code, checked_name, is_valid, ex_type = resolve_stock_info(code_in)
            if not is_valid:
                st.warning(f"⚠️無法驗證代碼 {code_in}，將使用原始輸入，且無法自動抓價。")
                checked_code = code_in
                checked_name = code_in
                ex_type = 'US'
            
            final_code = checked_code
            
            s_code = str(final_code).strip()
            if s_code and s_code[0].isdigit() and ex_type not in ['tse', 'otc']:
                ex_type = 'tse'
            
            rate = 1.0 if ex_type in ['tse', 'otc'] else get_usdtwd()
            
            final_cost = cost_in
            fetched_p = 0
            q_info = {}

            with st.spinner(f"正在抓取 {final_code} 即時報價 (更新市場資訊)..."):
                 temp_h = {final_code: {'ex': ex_type}}
                 q_prices = get_batch_market_data(temp_h, rate)
                 q_info = q_prices.get(final_code, {})
                 fetched_p = q_info.get('p', 0)
            
            total_twd = final_cost * shares_in * rate
            cash_needed = total_twd * margin_ratio
            debt_created = total_twd - cash_needed
            
            if data['cash'] < cash_needed:
                 st.error(f"現金不足！需 ${int(cash_needed):,}，現有 ${int(data['cash']):,}")
            else:
                data['cash'] -= cash_needed
                
                trade_type_str = "現股" if trade_type == "現股" else "融資"
                
                new_lot = {
                    'd': datetime.now().strftime('%Y-%m-%d'), 
                    'p': final_cost, 
                    's': shares_in, 
                    'type': trade_type_str, 
                    'debt': debt_created,
                    'ratio': margin_ratio
                }
                
                if final_code in data['h']:
                    if 'lots' not in data['h'][final_code]: data['h'][final_code]['lots'] = []
                    lots = data['h'][final_code]['lots']
                    lots.append(new_lot)
                    tot_s = sum(l['s'] for l in lots)
                    tot_c_val = sum(l['s'] * float(l['p']) for l in lots)
                    data['h'][final_code]['s'] = tot_s
                    data['h'][final_code]['c'] = tot_c_val / tot_s if tot_s else 0
                    data['h'][final_code]['lots'] = lots
                    data['h'][final_code]['n'] = checked_name
                    data['h'][final_code]['ex'] = ex_type
                else:
                    data['h'][final_code] = {'s': shares_in, 'c': final_cost, 'n': checked_name, 'lots': [new_lot], 'ex': ex_type}
                
                if fetched_p > 0:
                    data['h'][final_code]['last_p'] = fetched_p
                    data['h'][final_code]['last_chg'] = q_info.get('chg', 0)
                    data['h'][final_code]['last_chg_pct'] = q_info.get('chg_pct', 0)

                save_data(client, username, data)
                log_msg = f"新增庫存 ({datetime.now().strftime('%Y-%m-%d')})"
                if fetched_p > 0: log_msg += f" [參考市價: {fetched_p}]"
                log_transaction(client, username, "買入", f"{final_code}_{checked_name}", final_cost, shares_in, log_msg)

                update_dashboard_data(use_realtime=False)
                
                msg = f"買入成功！{checked_name} ({final_code}) 以單價 {final_cost} 成交"
