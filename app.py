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
APP_VERSION = "v2.5"

# 設定頁面配置 (注意：這裡加了版號方便您確認更新成功)
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
            # Change: Default create tabular structure (empty)
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
            # Time (UTC+8)
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

def get_realized_sheet(client, username):
    try:
        spreadsheet_name = st.secrets["spreadsheet_name"]
        spreadsheet = client.open(spreadsheet_name)
        worksheet_name = f"Realized_{username}"
        try:
            sheet = spreadsheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title=worksheet_name, rows="1000", cols="8")
            sheet.append_row(['Date', 'Code', 'Name', 'Qty', 'BuyCost', 'SellRev', 'Profit', 'ROI'])
        return sheet
    except: return None

def sync_us_prices_via_sheet(client, codes_dict):
    if not codes_dict or not client: return {}
    
    sync_sheet = get_price_sync_sheet(client)
    if not sync_sheet: return {}
    
    results = {}
    try:
        # 1. 準備寫入資料
        # Header: Code, Price, Change, ChangePct, Name
        rows_to_write = [['Code', 'Price', 'Change', 'ChangePct', 'Name']]
        
        # 傳入的是 dict: {c: {'ex': 'NASDAQ', ...}}
        for c, info in codes_dict.items():
            ex = info.get('ex', 'US')
            
            # Google Finance 格式: EXCHANGE:CODE
            # 若 ex 為 US_UNKNOWN 或其他，嘗試只傳 CODE
            if ex == 'PCX': ex = 'NYSEARCA'
            
            q_code = f"{ex}:{c}" if ex in ['NASDAQ', 'NYSE', 'NYSEARCA', 'AMEX'] else c
            
            rows_to_write.append([
                c, # Key for lookup later (Pure Code)
                f'=GOOGLEFINANCE("{q_code}", "price")',
                f'=GOOGLEFINANCE("{q_code}", "change")',
                f'=GOOGLEFINANCE("{q_code}", "changepct")',
                f'=GOOGLEFINANCE("{q_code}", "name")'
            ])
            
        # 2. 清空並寫入 (Batch update)
        sync_sheet.clear()
        sync_sheet.update('A1', rows_to_write, value_input_option='USER_ENTERED')
        
        # 3. 等待 Google 計算 (重要!)
        time.sleep(2.5) 
        
        # 4. 讀取數值 (使用 UNFORMATTED_VALUE 取得原始數字)
        try:
            # 讀取 B2:E(N) 的範圍
            end_row = len(codes) + 1
            # gspread get_values with value_render_option (needs newer gspread, or default is usually fine but formatted)
            # 這裡簡單讀取 entire sheet values
            raw_values = sync_sheet.get_all_values(value_render_option='UNFORMATTED_VALUE')
        except:
             # Retry once
             time.sleep(2)
             raw_values = sync_sheet.get_all_values(value_render_option='UNFORMATTED_VALUE')

        # 5. 解析回傳值
        # raw_values[0] 是 header, 從 1 開始
        # Col Index: 0=Code, 1=Price, 2=Change, 3=ChangePct, 4=Name
        for row in raw_values[1:]:
            if len(row) >= 5:
                r_code = row[0]
                r_price = row[1]
                r_chg = row[2]
                r_pct = row[3]
                r_name = row[4]
                
                # 處理錯誤或 Loading
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
        print(f"Sync Logic Error: {e}")
        pass
        
    return results

def load_data(client, username):
    default_data = {'h': {}, 'cash': 0.0, 'principal': 0.0, 'history': []}
    if not client or not username: return default_data
    
    user_sheet = get_user_sheet(client, username)
    if not user_sheet: return default_data
    
    try:
        # Check A1 for legacy JSON
        a1_val = user_sheet.acell('A1').value
        if a1_val and a1_val.startswith('{'):
            # Legacy format detected -> Migrate
            try:
                legacy_data = json.loads(a1_val)
                # Ensure structure
                if 'h' not in legacy_data: legacy_data['h'] = {}
                return migrate_legacy_data(client, username, legacy_data)
            except: pass
            
        # Standard Tabular Load
        # 1. Load Account Metadata
        acc_sheet = get_account_sheet(client, username)
        acc_data = {}
        if acc_sheet:
            records = acc_sheet.get_all_values()
            # records should be [['Key', 'Value'], ['Cash', '100'], ...]
            for row in records:
                if len(row) >= 2:
                    acc_data[row[0]] = row[1]
        
        # 2. Load Portfolio Table
        # Headers: Code, Name, Exchange, Shares, AvgCost, LastPrice, LastChg, LastChgPct, Lots_Data
        # Use get_all_values for robust header handling
        all_rows = user_sheet.get_all_values()
        h_data = {}
        
        if len(all_rows) > 1:
            headers = [str(h).strip() for h in all_rows[0]]
            # Map headers to indices
            idx_map = {h: i for i, h in enumerate(headers)}
            
            for row in all_rows[1:]:
                # Helper to safely get cell value with alias support
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
            
        # 3. Load Realized History
        h_history = []
        rel_sheet = get_realized_sheet(client, username)
        if rel_sheet:
            all_h_rows = rel_sheet.get_all_values()
            if len(all_h_rows) > 1:
                h_headers = [str(h).strip() for h in all_h_rows[0]]
                h_idx = {h: i for i, h in enumerate(h_headers)}
                
                for row in all_h_rows[1:]:
                    def get_h_val(col_names, default=''):
                        if isinstance(col_names, str): col_names = [col_names]
                        for cn in col_names:
                            if cn in h_idx and h_idx[cn] < len(row):
                                return row[h_idx[cn]]
                        return default
                    
                    h_history.append({
                        'd': get_h_val(['Date', '日期']),
                        'code': get_h_val(['Code', '代碼']),
                        'name': get_h_val(['Name', '名稱']),
                        'qty': float(get_h_val(['Qty', '賣出股數'], 0) or 0),
                        'buy_cost': float(get_h_val(['BuyCost', '總成本'], 0) or 0),
                        'sell_rev': float(get_h_val(['SellRev', '賣出收入'], 0) or 0),
                        'profit': float(get_h_val(['Profit', '獲利金額'], 0) or 0),
                        'roi': float(get_h_val(['ROI', '報酬率%'], 0) or 0)
                    })
            
        return {
            'h': h_data,
            'cash': float(acc_data.get('Cash', 0.0)),
            'principal': float(acc_data.get('Principal', 0.0)),
            'last_update': acc_data.get('LastUpdate', ''),
            'usdtwd': float(acc_data.get('USDTWD', 32.5)),
            'history': h_history
        }

    except Exception as e:
        # print(f"Load Error: {e}")
        pass
        
    return default_data

def migrate_legacy_data(client, username, data):
    # Perform migration: Save data in new format
    # This acts as a "Save" which overwrites User sheet with table and creates Account sheet
    save_data(client, username, data)
    return data

def save_data(client, username, data):
    if not client or not username: return
    
    try:
        # 1. Save Account Metadata
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
            
        # 2. Save Portfolio Table
        user_sheet = get_user_sheet(client, username)
        if user_sheet:
            # Try to preserve existing headers if possible, but ENSURE new headers exist
            try:
                existing_rows = user_sheet.get_all_values()
                if existing_rows:
                    current_headers = existing_rows[0]
                    # Check and append new headers if missing
                    if 'BuyType' not in current_headers and '交易類別' not in current_headers:
                        current_headers.insert(5, 'BuyType') # Insert after AvgCost
                    if 'BuyRatio' not in current_headers and '自備成數' not in current_headers:
                        current_headers.insert(6, 'BuyRatio')
                else:
                    current_headers = ['Code', 'Name', 'Exchange', 'Shares', 'AvgCost', 'BuyType', 'BuyRatio', 'LastPrice', 'LastChg', 'LastChgPct', 'Lots_Data']
            except:
                current_headers = ['Code', 'Name', 'Exchange', 'Shares', 'AvgCost', 'BuyType', 'BuyRatio', 'LastPrice', 'LastChg, LastChgPct', 'Lots_Data']
            
            # Map headers to indices for row construction
            h_map = {h.strip(): i for i, h in enumerate(current_headers)}
            
            # Helper to find index by multiple aliases
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
                
                # Derive Type/Ratio from Lots
                # If any debt > 0 => 融資 for display? Or Mixed?
                # User wants "BuyType" in column. If mixed, maybe "混和"?
                # But let's check total debt.
                temp_lots = info.get('lots', [])
                tot_d = sum(l.get('debt', 0) for l in temp_lots)
                tot_c_chk = sum(l['s'] * float(l['p']) for l in temp_lots)
                # Rate consideration for saving? Header usually stores raw if columns are generic.
                # Just store string representation.
                
                if tot_d > 1: # Tolerance
                     b_type = "融資"
                     # Net Ratio = (Cost - Debt) / Cost
                     val_ratio = (tot_c_chk - (tot_d / (rate if 'rate' in locals() else 1.0))) / tot_c_chk if tot_c_chk else 1.0
                     # Wait, debt is in TWD usually if we calculated (Price*Shares*Rate). 
                     # Actually in Buy: debt_created = total_twd - cash_needed. YES debt is TWD.
                     # But 'c' (AvgCost) is Original Currency.
                     # So we need strict calculation.
                     
                     # Re-calc Debt in Original Currency? No, Debt is TWD value.
                     # Let's just use the boolean for Type and maybe ratio string.
                     
                     # Let's simplify: If there is debt, it's Margin.
                     b_ratio_str = "Mixed"
                     # Try to get weighted ratio?
                     # Let's just check the *last* lot or dominent? 
                     # For display in Sheet, let's just put "融資" if any debt.
                     
                else:
                     b_type = "現股"
                     b_ratio_str = "100%"
                     
                # Actually, let's do it properly in save_data loop:
                # Need `rate` to normalize debt (TWD) vs Cost (USD/TWD)
                is_tw_s = (info.get('ex') in ['tse', 'otc', 'TW', 'TWO']) or (str(code)[0].isdigit())
                r_s = 1.0 if is_tw_s else 32.5 # Approximate if not passed? 
                # Ideally save_data shouldn't depend on live usdtwd?
                # Using 32.5 fallback is safer than 0.
                
                cost_twd = info.get('s',0) * info.get('c',0) * r_s
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
            
        # 3. Save Realized History
        history = data.get('history', [])
        if history:
            rel_sheet = get_realized_sheet(client, username)
            if rel_sheet:
                # Prepare rows for realized history
                # Header: Date, Code, Name, Qty, BuyCost, SellRev, Profit, ROI
                h_rows = [['Date', 'Code', 'Name', 'Qty', 'BuyCost', 'SellRev', 'Profit', 'ROI']]
                for r in history:
                    h_rows.append([
                        r.get('d', ''),
                        r.get('code', ''),
                        r.get('name', ''),
                        r.get('qty', 0),
                        r.get('buy_cost', 0),
                        r.get('sell_rev', 0),
                        r.get('profit', 0),
                        r.get('roi', 0)
                    ])
                rel_sheet.clear()
                rel_sheet.update('A1', h_rows)
            
    except Exception as e: st.error(f"存檔失敗: {e}")

# --- Audit Log Helper ---
def get_recent_audit_logs(client, username, limit=50):
    try:
        sheet = get_audit_sheet(client, username)
        if sheet:
            # Get all values
            all_rows = sheet.get_all_values()
            if len(all_rows) <= 1: return []
            
            headers = all_rows[0]
            data_rows = all_rows[1:]
            
            # Recents
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

def update_dashboard_data(use_realtime=True):
    # 讀取 Session 中的資料
    if 'data' not in st.session_state or st.session_state.data is None:
        return

    data = st.session_state.data
    client = st.session_state.client
    username = st.session_state.current_user
    
    # 決定是否抓取即時資料
    if use_realtime:
        with st.spinner('正在同步市場數據 (台股即時+美股)...'):
            usdtwd = get_usdtwd()
            h = data.get('h', {})
            batch_prices = get_batch_market_data(h, usdtwd)
            
            # 建立時間戳記並存檔 (UTC+8)
            now_ts = (datetime.utcnow() + timedelta(hours=8)).strftime('%Y/%m/%d %H:%M:%S')
            data['last_update'] = now_ts
            data['usdtwd'] = usdtwd
    else:
        # 離線模式：匯率給定預設值，價格使用快照或成本
        usdtwd = data.get('usdtwd', 32.5)
        h = data.get('h', {})
        batch_prices = {} 
        # 嘗試讀取最後更新時間
        now_ts = data.get('last_update', '尚無更新紀錄') 

    temp_list = []
    total_mkt_val = 0.0
    total_cost_val = 0.0
    total_debt = 0.0
    total_day_profit = 0.0
    
    for code, info in h.items():
        # --- Self-Healing: Missing Exchange Data ---
        if not info.get('ex'):
            # Only try to resolve if realtime
            if use_realtime:
                try:
                    _, _, _, resolved_ex = resolve_stock_info(code)
                    if resolved_ex:
                        info['ex'] = resolved_ex
                except: pass
            # Fallback for offline or simple cases
            if str(code)[0].isdigit() and not info.get('ex'):
                 info['ex'] = 'tse'

        # 取得市價資訊
        if use_realtime:
            market_info = batch_prices.get(code, {'p': info['c'], 'chg': 0, 'chg_pct': 0})
            
            # 儲存快照至 data (供下次離線使用)
            info['last_p'] = market_info['p']
            info['last_chg'] = market_info['chg']
            info['last_chg_pct'] = market_info['chg_pct']
        else:
            # 離線模式：優先使用儲存的快照價格，若無則回退到成本
            last_p = info.get('last_p', info['c'])
            last_chg = info.get('last_chg', 0)
            last_chg_pct = info.get('last_chg_pct', 0)
            market_info = {'p': last_p, 'chg': last_chg, 'chg_pct': last_chg_pct}

        cur_p = market_info['p'] if market_info['p'] > 0 else info['c']
        
        # 判斷匯率 (加強版: 若代碼第一個字為數字，強制視為台股)
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

        # Derive Display Type/Ratio for Dashboard
        if stock_debt > 1:
            disp_type = "融資"
            disp_ratio = (cost_val - stock_debt) / cost_val if cost_val else 1.0
        else:
            disp_type = "現股"
            disp_ratio = 1.0

        # 顯示名稱邏輯
        stock_name = info.get('n', code)
        
        # 若名稱等於代碼，嘗試自動補全一次 (僅限 session)
        if stock_name == code:
            _, fetched_name, _, _ = resolve_stock_info(code)
            if fetched_name != code:
                stock_name = fetched_name
                # 這裡選擇不強寫回 Sheet，避免每次 Refresh 都大量寫入
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
    
    # 取得已實現損益
    total_realized_profit = sum(r.get('profit', 0) for r in data.get('history', []))
    
    # === 關鍵修改：總損益 = 未實現 + 已實現 ===
    total_profit_sum = unrealized_profit + total_realized_profit
    
    current_principal = data.get('principal', data['cash'])
    
    # === 僅在即時更新時寫入資料庫與歷史紀錄 ===
    if use_realtime: 
        save_data(client, username, data)
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
        'total_profit_sum': total_profit_sum,
        'total_profit_sum': total_profit_sum,
        'total_roi_pct': total_roi_pct,
        'total_debt': total_debt,
        'final_rows': final_rows,
        'temp_list': temp_list,
        'last_update_ts': now_ts,
        'usdtwd': usdtwd
    }

# 移除 cache 因為需要連線 Google Sheet (side effect)
def resolve_stock_info(user_input):
    """
    輸入: 股票代碼 (e.g. "2330", "2330.TW", "NVDA")
    輸出: (final_code, stock_name, success, exchange_type)
    exchange_type: 'TW', 'TWO', 'NASDAQ', 'NYSE', 'US' (fallback)
    """
    user_input = user_input.strip().upper()
    if not user_input:
        return "", "", False, ""

    # 1. 台股邏輯 (開頭為 0-9)
    if user_input[0].isdigit():
        # 若使用者未輸入後綴，嘗試自動偵測
        candidates = []
        if '.TW' in user_input or '.TWO' in user_input:
            candidates.append(user_input)
        else:
            # 優先猜 TSE, 再猜 OTC
            candidates.append(f"tse_{user_input}.tw")
            candidates.append(f"otc_{user_input}.tw")

        # 這裡的 candidates 若是純代碼 (無 tse_) 會在下面處理
        # 為了配合 fetch_api，調整 query 格式
        query_list = []
        for c in candidates:
            if 'tse_' in c or 'otc_' in c:
                query_list.append(c)
            elif '.TW' in c:
                query_list.append(f"tse_{c.replace('.TW', '')}.tw")
            elif '.TWO' in c:
                query_list.append(f"otc_{c.replace('.TWO', '')}.tw")
        
        # 呼叫 TWSE API
        try:
            timestamp = int(time.time() * 1000)
            q_str = "|".join(query_list)
            url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={q_str}&json=1&delay=0&_={timestamp}"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Referer": "https://mis.twse.com.tw/stock/fibest.jsp?stock=2330"
            }
            res = requests.get(url, headers=headers, verify=False, timeout=5)
            data = res.json()
            
            if 'msgArray' in data:
                for item in data['msgArray']:
                    if 'n' in item and 'c' in item:
                        name = item['n']
                        code = item['c']
                        ex_key = item.get('ex', '')
                        
                        # 判定交易所 (修改: 直接回傳 tse/otc，不轉為 TW/TWO)
                        final_ex = ex_key if ex_key in ['tse', 'otc'] else ('tse' if ex_key == 'tse' else 'otc' if ex_key == 'otc' else 'tse')
                        # The line above is redundant, simplified below:
                        final_ex = ex_key if ex_key in ['tse', 'otc'] else 'tse'

                        # 回傳 純代碼, 名稱, True, 交易所
                        return code, name, True, final_ex
        except:
            pass
            
        # 若 API 失敗但格式正確，回傳原值 (無名稱)
        # 簡易判斷: 4碼以上通常是上市櫃 -> 預設 tse
        return user_input.split('.')[0], user_input, True, "tse"

    # 2. 美股邏輯 (非數字開頭) - 需解析具體交易所 (NASDAQ/NYSE)
    else:
        try:
            # 優先使用 yfinance.info.exchange 來取得交易所資訊 (需要一點時間但只在新增時跑)
            t = yf.Ticker(user_input)
            
            # 預設值
            ex_type = "US"
            name = user_input
            
            # 嘗試取得詳細資訊
            try:
                # 使用 fast_info 比較快，但 exchange 可能簡寫
                # info 比較完整
                info = t.info
                yf_ex = info.get('exchange', '').upper()
                name = info.get('shortName') or info.get('longName') or user_input
                
                # 映射 Exchange Code
                # NMS, NGM, NCM -> NASDAQ
                # NYQ, NYS -> NYSE
                if yf_ex in ['NMS', 'NGM', 'NCM', 'NASDAQ']:
                    ex_type = "NASDAQ"
                elif yf_ex in ['NYQ', 'NYS', 'NYSE']:
                    ex_type = "NYSE"
                elif yf_ex in ['PCX', 'PNK', 'ASE', 'ASEX', 'NCM', 'NGM']: # Added common variations
                     ex_type = "NYSEARCA" if yf_ex == 'PCX' else "NASDAQ" if yf_ex in ['NCM', 'NGM'] else "NYSE"
                else:
                    # 其他 (AMEX etc)
                    ex_type = yf_ex
            except:
                # 若 yf 失敗，嘗試 fallback 到 Sheet Sync 抓名稱 (但 Exchange 只能猜)
                scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
                creds = ServiceAccountCredentials.from_json_keyfile_dict(st.secrets["service_account_info"], scope)
                temp_client = gspread.authorize(creds)
                
                res_dict = sync_us_prices_via_sheet(temp_client, [user_input])
                if user_input in res_dict:
                    name = res_dict[user_input].get('n', user_input)
                    # 無法確切得知交易所，預設 NASDAQ (常見科技股) 或 US
                    ex_type = "NASDAQ" # 暫定
            
            return user_input, name, True, ex_type
        except Exception as e:
            # print(f"US Resolve Error: {e}")
            return user_input, user_input, True, "US"

def fetch_twse_realtime(codes):
    """
    更新版：加入 User-Agent 偽裝成瀏覽器，解決 Streamlit Cloud 被擋的問題。
    """
    if not codes: return {}
    
    query_parts = []
    for c in codes:
        # get_batch_market_data now passes "tse_2330.tw" or "otc_6488.tw" directly
        # or "2330.TW" (legacy)
        query_parts.append(c)
    
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

# 移除 cache 以支援 side-effect (寫入 sheet) 與 session state 存取
# 修改: 接受 portfolio_dict (h) 而非僅 codes list
def get_batch_market_data(portfolio_dict, usdtwd_rate):
    if not portfolio_dict: return {}
    
    tw_query = []
    other_query_dict = {} # {code: info}
    
    # 路由邏輯
    for code, info in portfolio_dict.items():
        ex = info.get('ex', '')
        if ex in ['tse', 'otc']:
            # 直接使用 ex_code.tw 格式
            tw_query.append(f"{ex}_{code}.tw")
        elif ex == 'TW': # 相容舊資料
            tw_query.append(f"tse_{code}.tw")
        elif ex == 'TWO': # 相容舊資料
            tw_query.append(f"otc_{code}.tw")
        else:
            # US or Others
            other_query_dict[code] = info
    
    results = {}
    
    # 1. 台股
    if tw_query:
        # fetch_twse_realtime 回傳的 Key 是 "2330.TW"
        # 我們需要轉回 "2330"
        raw_tw_results = fetch_twse_realtime(tw_query)
        for raw_k, v in raw_tw_results.items():
            pure_k = raw_k.replace('.TW', '').replace('.TWO', '')
            results[pure_k] = v

    # 2. 美股 (透過 Google Sheet Sync)
    if other_query_dict:
        try:
             scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
             creds = ServiceAccountCredentials.from_json_keyfile_dict(st.secrets["service_account_info"], scope)
             temp_client = gspread.authorize(creds)
             
             # sync_us_prices_via_sheet 現在需要處理 dict (獲取 ex)
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
    **v2.5 UI/UX Polish & Fixes**
    1.  **介面互動優化**: 執行買賣、資金存提或刪除後，輸入欄位會自動重置，保持介面清爽。
    2.  **訊息顯示修復**: 修正「賣出成功」訊息閃退問題，並統一買賣成功提示，明確顯示「成交單價」。
    3.  **資金更新即時性**: 修正資金存提後，本金與現金餘額未即時刷新顯示的問題。
    4.  **防呆機制增強**: 手動更新股價新增「請選擇」預設選項，防止誤觸更新。
    5.  **異動紀錄中文化**: 系統稽核紀錄 (Audit Log) 全面繁體中文化，且股票欄位優化顯示為「代碼_名稱」。
    6.  **匯率資訊持久化**: 新增美元匯率儲存欄位，離線模式下優先使用上次同步的真實匯率，提升資產估值準確度。
    7.  **融資交易功能**: 支援融資買賣 (設定自備款成數)，自動計算融資負債，並於資產概況顯示總融資金額。
    8.  **已實現損益持久化**: 新增專屬 `Realized` 工作表儲存已結清交易，修復歷史紀錄重啟後消失的問題，並支援從舊版 JSON 資料自動遷移。

    **v2.4 Robust Sync & UI Polish**
    1. **交易所資料同步**: 修復買入/賣出/刪除後 Grid 欄位缺失問題，並支援動態標頭（自動識別「交易所」或 "Exchange"），不更動原始試算表格式。
    2. **UI 佈局優化**: 將買入與賣出的「股數」與「單價」輸入框調整為獨立兩行顯示，提升輸入體驗度。
    3. **系統穩定性**: 整合重複的核心計算函式，並修復解包 (Unpacking) 錯誤導致的程式停滯。
    4. **介面清理**: 移除偵錯資訊 (Debug Info) 區塊，優化側邊欄操作流程。
    5. **進階工具**: 包含手動價格買入優化、檢視最近 50 筆異動歷程、以及測試用的資料重置功能。

    **v2.3 Audit & Tabular Storage**
    1. **交易審計紀錄 (Audit Log)**: 新增專屬工作表 `Audit_{User}`，完整記錄所有交易操作。
    2. **表格化資料儲存**: 資料儲存由 JSON 遷移至清晰的試算表格，提升可視化管理能力。

    **v2.1 Refactor Update**
    1. **資料結構重構**: 將股票代碼與交易所欄位完全分離 (Ex: `2330.TW` -> `2330` + `TW`)，優化顯示並支援自動遷移舊資料。
    2. **交易所自動識別**: 新增美股時，系統自動透過 yfinance 辨識並記錄所屬交易所 (NASDAQ/NYSE)，確保資料精確度。
    
    **v2.0 Features**
    1. **動態代碼解析**: 移除舊版硬編碼對照表，支援自動識別台股 (.TW/.TWO) 與美股 (整合 Google Finance)。
    2. **資料持久化**: 登入即載入上次最後更新的市場報價 (Offline Mode)，大幅提升開啟速度。
    3. **Google Finance Sync**: 美股報價改由 Google Sheet 內的 `=GOOGLEFINANCE()` 函數即時運算，確保資料穩定性。
    4. **即時體驗優化**: 交易動作 (買賣/修正) 後自動刷新 Grid，並顯示最後更新時間 (Taipei Time)。
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
        if 'sheet' in st.session_state: del st.session_state.sheet
        if 'dashboard_data' in st.session_state: del st.session_state.dashboard_data
        st.rerun()
    st.markdown("---")

if 'client' not in st.session_state: st.session_state.client = get_google_client()

# sheet_user check might not be needed as strictly anymore for data loading, 
# but good for ensuring we are on right user.
# load_data now takes client/username directly.
if 'data' not in st.session_state or st.session_state.get('loaded_user') != username:
    if st.session_state.client:
        # 這裡 sheet 變數可能不再是必須傳遞的重點，但為了相容舊邏輯保留
        # 不過 save/load 已經改版
        st.session_state.data = load_data(st.session_state.client, username)
        st.session_state.loaded_user = username
        # st.session_state.sheet = get_user_sheet(...) # Still useful if we need direct sheet access elsewhere?
        # save_data uses get_user_sheet internally now using client/username.
    else: st.session_state.data = None

client = st.session_state.client
# sheet object is less critical now as save/load handle it internally, but let's keep it if needed for record_history or legacy
# actually record_history uses client.
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
            st.session_state.data = load_data(client, username) # 確保「寫入後讀取」一致性
            # Audit Log
            log_transaction(client, username, "系統自動校正本金", "全部", new_principal, 0, "系統自動檢查")
            
            st.success(f"本金已校正為: ${int(new_principal):,}")
            st.rerun()

    with st.expander("💵 資金存提 (影響本金)"):
        # Init State
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
            st.session_state.data = load_data(client, username) # 確保「寫入後讀取」一致性
            # Audit Log
            log_transaction(client, username, "資金存提", "現金", cash_op, 0, "存入/提款")
            
            # 交易後強制更新 Grid
            update_dashboard_data(use_realtime=False)
            
            st.success("資金已更新")
            st.session_state.reset_fund = True
            st.rerun()

    st.markdown("---")
    
    st.subheader("🔵 買入股票")
    # Init State
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
            
            # 1. Resolve Code & Ex
            checked_code, checked_name, is_valid, ex_type = resolve_stock_info(code_in)
            if not is_valid:
                st.warning(f"⚠️無法驗證代碼 {code_in}，將使用原始輸入，且無法自動抓價。")
                checked_code = code_in
                checked_name = code_in
                ex_type = 'US'
            
            final_code = checked_code
            
            # --- 強制修正邏輯 ---
            s_code = str(final_code).strip()
            if s_code and s_code[0].isdigit() and ex_type not in ['tse', 'otc']:
                ex_type = 'tse'
            
            rate = 1.0 if ex_type in ['tse', 'otc'] else get_usdtwd()
            
            # 2. Determine Final Price
            final_cost = cost_in
            fetched_p = 0
            q_info = {}

            with st.spinner(f"正在抓取 {final_code} 即時報價 (更新市場資訊)..."):
                 temp_h = {final_code: {'ex': ex_type}}
                 q_prices = get_batch_market_data(temp_h, rate)
                 q_info = q_prices.get(final_code, {})
                 fetched_p = q_info.get('p', 0)
            
            # 3. Proceed to Buy
            total_twd = final_cost * shares_in * rate
            cash_needed = total_twd * margin_ratio
            debt_created = total_twd - cash_needed
            
            if data['cash'] < cash_needed:
                 st.error(f"現金不足！需 ${int(cash_needed):,}，現有 ${int(data['cash']):,}")
            else:
                data['cash'] -= cash_needed
                
                # Margin Logic: Debt = Total - CashNeeded
                # CashNeeded = Total * Ratio
                # Debt = Total * (1 - Ratio)
                # Matches user formula: (Price*Shares) * (1-Ratio)
                
                trade_type_str = "現股" if trade_type == "現股" else "融資"
                # If Cash (Ratio=1.0), Debt=0
                
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
                st.session_state.data = load_data(client, username) # 確保「寫入後讀取」一致性
                # Audit Log
                log_msg = f"新增庫存 ({datetime.now().strftime('%Y-%m-%d')})"
                if fetched_p > 0: log_msg += f" [參考市價: {fetched_p}]"
                log_transaction(client, username, "買入", f"{final_code}_{checked_name}", final_cost, shares_in, log_msg)

                update_dashboard_data(use_realtime=False)
                
                msg = f"買入成功！{checked_name} ({final_code}) 以單價 {final_cost} 成交"
                st.success(msg)
                
                # Reset Inputs
                st.session_state.reset_buy = True
                
                time.sleep(1) 
                st.rerun()
        else: st.error("請輸入代碼")

    st.markdown("---")

    st.subheader("🔴 賣出股票")
    # Init State
    if "sell_price_in" not in st.session_state: st.session_state.sell_price_in = 0.0
    if st.session_state.get("reset_sell"):
        st.session_state.sell_select = "請選擇"
        st.session_state.sell_price_in = 0.0
        st.session_state.reset_sell = False
        
    holdings_list = list(data.get('h', {}).keys())
    if holdings_list:
        sell_code = st.selectbox("賣出代碼", ["請選擇"] + holdings_list, key="sell_select")
        if sell_code != "請選擇":
            current_hold = data['h'][sell_code]['s']
            st.caption(f"持有: {current_hold} 股")
            sell_qty = st.number_input("賣出股數", min_value=1, max_value=int(current_hold), value=int(current_hold), step=100, key="sell_qty_in")
            sell_price = st.number_input("賣出單價", min_value=0.0, step=0.1, format="%.2f", key="sell_price_in")
            
            if st.button("確認賣出"):
                if sell_price > 0:
                    info = data['h'][sell_code]
                    lots = info.get('lots', [])
                    ex = info.get('ex', 'US')
                    is_tw_stock = (ex in ['tse', 'otc', 'TW', 'TWO'])
                    rate = 1.0 if is_tw_stock else get_usdtwd()
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
                    
                    h_name = data['h'][sell_code].get('n', sell_code)

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
                        'name': h_name, 'qty': sell_qty,
                        'buy_cost': total_cost_basis, 'sell_rev': sell_revenue,
                        'profit': realized_profit, 'roi': realized_roi
                    })

                    save_data(client, username, data)
                    st.session_state.data = load_data(client, username) # 確保「寫入後讀取」一致性
                    update_dashboard_data(use_realtime=False)
                    st.success(f"賣出成功！{h_name} ({sell_code}) 以單價 {sell_price} 成交"); st.balloons()
                    
                    # Reset Inputs
                    st.session_state.reset_sell = True
                    
                    time.sleep(1)
                    st.rerun()

    st.markdown("---")
    
    # 修正/刪除
    with st.expander("🔧 修正/刪除 (含刪除退款)"):
        if st.session_state.get("reset_del"):
            st.session_state.del_select = "請選擇"
            st.session_state.reset_del = False
            
        del_list = list(data.get('h', {}).keys())
        if del_list:
            to_del_code = st.selectbox("選擇要處理的股票", ["請選擇"] + del_list, key="del_select")
            
            if to_del_code != "請選擇":
                info = data['h'][to_del_code]
                current_s = info.get('s', 0)
                current_c = info.get('c', 0)
                h_name_del = info.get('n', to_del_code)
                ex = info.get('ex', 'US')
                is_tw_stock = (ex in ['tse', 'otc', 'TW', 'TWO'])
                rate = 1.0 if is_tw_stock else get_usdtwd()
                total_cost_basis = current_s * current_c * rate
                
                st.write(f"📊 持有股數: {current_s}, 平均成本: {current_c}")
                st.write(f"💰 估算原始投入成本: ${int(total_cost_basis):,}")

                col_del_1, col_del_2 = st.columns(2)
                
                with col_del_1:
                    if st.button("❌ 僅刪除代碼", type="secondary"):
                        del data['h'][to_del_code]
                        save_data(client, username, data)
                        st.session_state.data = load_data(client, username) # 確保「寫入後讀取」一致性
                        # Audit Log
                        log_transaction(client, username, "刪除代碼", f"{to_del_code}_{h_name_del}", 0, 0, "移除庫存")
                        
                        update_dashboard_data(use_realtime=False)
                        st.success(f"已刪除 {to_del_code}")
                        st.session_state.reset_del = True
                        time.sleep(1); st.rerun()

                with col_del_2:
                    if st.button("💸 刪除並退回現金", type="primary"):
                        # Margin Support: Refund = Cost - Debt
                        total_debt = sum(l.get('debt', 0) for l in info.get('lots', []))
                        refund_val = total_cost_basis - total_debt
                        
                        data['cash'] += refund_val
                        del data['h'][to_del_code]
                        save_data(client, username, data)
                        st.session_state.data = load_data(client, username) # 確保「寫入後讀取」一致性
                        # Audit Log
                        log_transaction(client, username, "刪除退款", f"{to_del_code}_{h_name_del}", refund_val, 0, f"移除並退還現金 (原成本 {int(total_cost_basis)} - 融資 {int(total_debt)})")
                        
                        update_dashboard_data(use_realtime=False)
                        st.success(f"已刪除並退款")
                        st.session_state.reset_del = True
                        time.sleep(1); st.rerun()

    st.markdown("---")
    
    # 手動更新
    with st.expander("🆘 手動更新股價 (API 失敗時用)"):
        st.caption("如果 6488.TWO 抓不到價格，請在此手動輸入。")
        # Init
        if "man_update_price" not in st.session_state: st.session_state.man_update_price = 0.0

        if st.session_state.get("reset_man"):
            st.session_state.man_update_sel = "請選擇"
            st.session_state.man_update_price = 0.0
            st.session_state.reset_man = False
            
        # Add "請選擇"
        man_code = st.selectbox("選擇股票", ["請選擇"] + list(data.get('h', {}).keys()), key="man_update_sel")
        man_price = st.number_input("輸入現價", min_value=0.0, step=0.5, key="man_update_price")
        
        if st.button("強制更新價格"):
            if man_code != "請選擇":
                if 'manual_prices' not in st.session_state:
                    st.session_state.manual_prices = {}
                st.session_state.manual_prices[man_code] = man_price
                st.success(f"{man_code} 價格暫時設定為 {man_price}")
                
                # Reset
                st.session_state.reset_man = True
                st.rerun()
            else:
                 st.error("請先選擇股票")

    st.markdown("---")

    # 強制修改本金
    with st.expander("⚙️ 進階：強制修改本金"):
        st.info(f"目前系統記錄本金: ${int(data.get('principal', 0)):,}")
        st.caption("手動補回現金後，請在此修正為您真正投入的總金額。")
        
        real_principal = st.number_input("設定正確本金", value=float(data.get('principal', 0)), step=10000.0, key="mod_principal_in")
        
        if st.button("確認修正本金"):
            current_stock_cost = 0
            for code, info in data.get('h', {}).items():
                s = info.get('s', 0)
                c = info.get('c', 0)
                debt = sum(l.get('debt', 0) for l in info.get('lots', []))
                ex = info.get('ex', '')
                is_tw = (ex in ['tse', 'otc', 'TW', 'TWO']) or ('.TW' in code or '.TWO' in code)
                rate = 1.0 if is_tw else get_usdtwd()
                current_stock_cost += (s * c * rate) - debt
            
            new_cash = real_principal - current_stock_cost
            
            data['principal'] = real_principal
            data['cash'] = new_cash 
            
            save_data(client, username, data)
            st.session_state.data = load_data(client, username) # 確保「寫入後讀取」一致性
            # Audit Log
            log_transaction(client, username, "修正本金", "現金", real_principal, 0, f"重設本金。現金調整為 {int(new_cash)}")
            
            update_dashboard_data(use_realtime=False)
            st.success(f"本金已修正為 ${int(real_principal):,} (現金重算為 ${int(new_cash):,})")
            
            # Reset is tricky here as default value comes from data, but we updated data. 
            # Ideally it stays as is to show current value, or verify logic.
            # User requested reset, let's keep the widget showing the NEW value (which IS the default now)
            # Or reset to 0? Usually principal input should show current. 
            # Let's Skip reset for this specific "Configuration" field as it mirrors state, 
            # UNLESS user wants it to go back to 0 (which would be weird for principal view).
            # "相關欄位設定回預設值" -> For principal modification, "default" is current principal.
            # So updating data['principal'] effective updates the default for next render.
            
            time.sleep(1)
            st.rerun()

    # 檢視異動紀錄按鈕
    if st.button("📋 檢視異動歷程 (近50筆)"):
        with st.spinner("讀取中..."):
            audit_logs = get_recent_audit_logs(client, username, 50)
        show_audit_log_modal(audit_logs)

    st.markdown("---")

    # 清空所有資料
    with st.expander("💀 清空所有資料 (測試用)"):
        st.warning("⚠️ 此操作將永久刪除所有庫存、歷史紀錄與資金設定！")
        if st.session_state.get("reset_clear"):
            st.session_state.clear_verify = ""
            st.session_state.reset_clear = False
            
        confirm_txt = st.text_input("請輸入 '清空' 以確認執行", key="clear_verify")
        
        if st.button("確認清空", type="primary"):
            if confirm_txt == "清空":
                # Reset Logic
                data['h'] = {}
                data['names'] = {}
                data['cash'] = 0.0
                data['principal'] = 0.0
                data['history'] = []
                data['last_update'] = ""
                
                save_data(client, username, data)
                st.session_state.data = load_data(client, username) # 確保「寫入後讀取」一致性
                
                try:
                    audit_sheet = get_audit_sheet(client, username)
                    if audit_sheet: audit_sheet.clear()
                    audit_sheet.append_row(['Time', 'Action', 'Code', 'Amount', 'Shares', 'Memo'])
                    log_transaction(client, username, "資料清空", "全部", 0, 0, "強制重置 - 清除紀錄")
                except: pass
                
                st.session_state.dashboard_data = None
                
                st.success("以此重置所有資料！")
                st.session_state.reset_clear = True
                time.sleep(1)
                st.rerun()
            else:
                st.error("驗證碼錯誤，未執行。")

# --- 資料更新按鈕 ---
if 'dashboard_data' not in st.session_state:
    st.session_state.dashboard_data = None

# 自動載入 (若尚未有儀表板資料)
if st.session_state.dashboard_data is None:
    update_dashboard_data(use_realtime=False)

if st.button("🔄 更新即時報價", type="primary", use_container_width=True):
    update_dashboard_data(use_realtime=True)

# --- 顯示層 ---
if st.session_state.dashboard_data:
    d = st.session_state.dashboard_data
    
    st.subheader("🏦 資產概況")
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("💰 淨資產", f"${int(d['net_asset']):,}")
    k2.metric("💵 現金餘額", f"${int(d['cash']):,}")
    k3.metric("📊 證券市值", f"${int(d['total_mkt_val']):,}")
    k4.metric("📉 投入本金", f"${int(d['current_principal']):,}")
    # New Margin Metric
    total_debt_disp = d.get('total_debt', 0.0)
    k5.metric("💳 融資金額", f"${int(total_debt_disp):,}")
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
        # 顯示更新時間於表格上方
        usdtwd_val = d.get('usdtwd', 32.5)
        st.caption(f"🇺🇸 美元匯率: {usdtwd_val:.2f} | 🕒 資訊更新時間: {d.get('last_update_ts', '---')}")
        
        if d['final_rows']:
            df = pd.DataFrame(d['final_rows'])
            
            # Rename Exchange to 交易所 if present, otherwise add empty
            if 'Exchange' in df.columns:
                df.rename(columns={'Exchange': '交易所'}, inplace=True)
            elif '交易所' not in df.columns:
                df['交易所'] = ''
                
            cols = ['交易所', '股票代碼', '公司名稱', '交易類別', '自備成數', '股數', '成本', '現價', '日損益%', '日損益', '總損益%', '總損益', '市值', '投資比例']
            
            # Ensure all cols exist
            for c in cols:
                if c not in df.columns: df[c] = ''
                
            df = df[cols]
            styler = df.style.format({
                '股數': '{:,}', '成本': '{:,.2f}', '現價': '{:,.2f}',
                '日損益%': '{:+.2%}', '日損益': '{:+,.0f}',
                '總損益%': '{:+.2%}', '總損益': '{:+,.0f}',
                '市值': '{:,.0f}', '投資比例': '{:.1%}'
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
