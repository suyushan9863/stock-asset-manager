import streamlit as st
import pandas as pd
import yfinance as yf
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import plotly.express as px

# 設定頁面配置
st.set_page_config(page_title="全功能資產管家", layout="wide", page_icon="📈")

# --- 股票代碼與名稱對照表 ---
STOCK_MAP = {
    '2330.TW': '台積電', '2317.TW': '鴻海', '2454.TW': '聯發科',
    '2603.TW': '長榮', '2609.TW': '陽明', '2615.TW': '萬海',
    '3231.TW': '緯創', '2382.TW': '廣達', '3017.TW': '奇鋐',
    '2301.TW': '光寶科', '00685L.TW': '群益台指正2', '00670L.TW': '元大NASDAQ正2',
    'NVDA': '輝達', 'AAPL': '蘋果', 'TSLA': '特斯拉', 'AMD': '超微',
    'MSFT': '微軟', 'GOOG': '谷歌', 'AMZN': '亞馬遜'
}

# --- Google Sheets 連線與資料處理 ---
def get_google_client():
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        secret_str = st.secrets["service_account_info"]
        creds_dict = None
        try:
            creds_dict = json.loads(secret_str, strict=False)
        except json.JSONDecodeError:
            fixed_str = secret_str.replace('\n', '\\n').replace('\r', '')
            creds_dict = json.loads(fixed_str, strict=False)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"連線 Google Sheets 失敗: {e}")
        return None

def get_main_sheet(client):
    try:
        sheet_name = st.secrets["spreadsheet_name"]
        return client.open(sheet_name).sheet1
    except: return None

def get_history_sheet(client):
    try:
        sheet_name = st.secrets["spreadsheet_name"]
        spreadsheet = client.open(sheet_name)
        try:
            history_sheet = spreadsheet.worksheet('History')
        except gspread.exceptions.WorksheetNotFound:
            history_sheet = spreadsheet.add_worksheet(title='History', rows="1000", cols="2")
            history_sheet.append_row(['Date', 'NetAsset'])
        return history_sheet
    except: return None

def load_data(sheet):
    # 預設資料結構
    default_data = {'h': {}, 'cash': 0.0, 'history': []}
    if not sheet: return default_data
    try:
        raw_data = sheet.acell('A1').value
        if raw_data:
            data = json.loads(raw_data)
            # 確保欄位齊全
            if 'h' not in data: data['h'] = {}
            if 'cash' not in data: data['cash'] = 0.0
            if 'history' not in data: data['history'] = []
            
            # 資料結構防呆 (Lots)
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

def record_history(client, net_asset):
    hist_sheet = get_history_sheet(client)
    if hist_sheet and net_asset > 0:
        today = datetime.now().strftime('%Y-%m-%d')
        try:
            last_row = hist_sheet.get_all_values()[-1]
            if last_row[0] == today: return 
        except: pass
        hist_sheet.append_row([today, int(net_asset)])

# --- 核心計算邏輯 ---
@st.cache_data(ttl=60)
def get_price_data(ticker):
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period='2d')
        if len(hist) >= 1:
            price = hist['Close'].iloc[-1]
            prev_close = hist['Close'].iloc[-2] if len(hist) >= 2 else price
            change_val = price - prev_close
            change_pct = (change_val / prev_close * 100) if prev_close else 0
            return price, change_val, change_pct
        
        price = stock.fast_info.get('last_price')
        if price and not pd.isna(price):
             prev = stock.info.get('previousClose', price)
             change_val = price - prev
             change_pct = (change_val / prev * 100) if prev else 0
             return price, change_val, change_pct
        return None, 0, 0
    except: return None, 0, 0

@st.cache_data(ttl=300)
def get_usdtwd():
    try:
        fx = yf.Ticker('USDTWD=X')
        p = fx.fast_info.get('last_price')
        return p if p and not pd.isna(p) else 32.5
    except: return 32.5

# --- 介面開始 ---
st.title("📈 全功能股票資產管家")

if 'client' not in st.session_state: st.session_state.client = get_google_client()
if 'sheet' not in st.session_state:
    st.session_state.sheet = get_main_sheet(st.session_state.client) if st.session_state.client else None

client = st.session_state.client
sheet = st.session_state.sheet

if 'data' not in st.session_state: st.session_state.data = load_data(sheet)
data = st.session_state.data

if not sheet:
    st.error("⚠️ 無法連接 Google Sheets，請檢查 Secrets 設定。")
    st.stop()

# --- 側邊欄 ---
with st.sidebar:
    st.header("💰 資金與交易")
    st.metric("現金餘額", f"${int(data.get('cash', 0)):,}")
    
    with st.expander("💵 資金存提"):
        cash_op = st.number_input("金額 (正存/負提)", step=1000.0)
        if st.button("執行異動"):
            data['cash'] += cash_op
            save_data(sheet, data)
            st.success("資金已更新"); st.rerun()

    st.markdown("---")
    
    # === 買入區塊 ===
    st.subheader("🔵 買入股票")
    code_in = st.text_input("買入代碼 (如 2330.TW)").strip().upper()
    c1, c2 = st.columns(2)
    shares_in = c1.number_input("買入股數", min_value=1, value=1000, step=100)
    cost_in = c2.number_input("買入單價", min_value=0.0, value=0.0, step=0.1, format="%.2f")
    trade_type = st.radio("類別", ["現股", "融資"], horizontal=True, key="buy_type")
    
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

    # === 賣出區塊 (新增) ===
    st.subheader("🔴 賣出股票")
    # 建立持有股票選單
    holdings_list = list(data.get('h', {}).keys())
    if holdings_list:
        sell_code = st.selectbox("選擇賣出代碼", ["請選擇"] + holdings_list)
        
        if sell_code != "請選擇":
            current_hold = data['h'][sell_code]['s']
            st.caption(f"目前持有: {current_hold} 股")
            
            sc1, sc2 = st.columns(2)
            sell_qty = sc1.number_input("賣出股數", min_value=1, max_value=int(current_hold), value=int(current_hold), step=100)
            sell_price = sc2.number_input("賣出單價", min_value=0.0, value=0.0, step=0.1, format="%.2f")
            
            if st.button("確認賣出 (實現損益)"):
                if sell_price > 0:
                    info = data['h'][sell_code]
                    lots = info.get('lots', [])
                    
                    rate = 1.0 if ('.TW' in sell_code or '.TWO' in sell_code) else get_usdtwd()
                    
                    # 計算總賣出收入
                    sell_revenue = sell_qty * sell_price * rate
                    
                    # FIFO 扣庫存邏輯
                    remain_to_sell = sell_qty
                    total_cost_basis = 0
                    total_debt_repaid = 0
                    new_lots = []
                    
                    for lot in lots:
                        if remain_to_sell > 0:
                            take_qty = min(lot['s'], remain_to_sell)
                            
                            # 計算此批次成本與負債
                            lot_cost = take_qty * lot['p'] * rate
                            lot_debt = lot.get('debt', 0) * (take_qty / lot['s']) if lot['s'] > 0 else 0
                            
                            total_cost_basis += lot_cost
                            total_debt_repaid += lot_debt
                            
                            # 更新批次
                            lot['s'] -= take_qty
                            lot['debt'] -= lot_debt
                            remain_to_sell -= take_qty
                            
                            if lot['s'] > 0: new_lots.append(lot)
                        else:
                            new_lots.append(lot)
                    
                    # 計算已實現損益
                    realized_profit = sell_revenue - total_cost_basis
                    realized_roi = (realized_profit / total_cost_basis * 100) if total_cost_basis else 0
                    
                    # 更新現金 (收入 - 償還負債)
                    cash_back = sell_revenue - total_debt_repaid
                    data['cash'] += cash_back
                    
                    # 更新庫存
                    if new_lots:
                        data['h'][sell_code]['lots'] = new_lots
                        data['h'][sell_code]['s'] -= sell_qty
                        # 重新計算均價
                        ts = sum(l['s'] for l in new_lots)
                        tc = sum(l['s']*l['p'] for l in new_lots)
                        data['h'][sell_code]['c'] = tc / ts if ts else 0
                    else:
                        del data['h'][sell_code]
                    
                    # 寫入歷史紀錄
                    if 'history' not in data: data['history'] = []
                    data['history'].append({
                        'd': datetime.now().strftime('%Y-%m-%d'),
                        'code': sell_code,
                        'name': STOCK_MAP.get(sell_code, sell_code),
                        'qty': sell_qty,
                        'buy_cost': total_cost_basis,
                        'sell_rev': sell_revenue,
                        'profit': realized_profit,
                        'roi': realized_roi
                    })
                    
                    save_data(sheet, data)
                    st.success(f"賣出成功！獲利: ${int(realized_profit):,}")
                    st.balloons()
                    st.rerun()
                else:
                    st.error("請輸入賣出價格")
    else:
        st.info("目前無庫存可賣")


# --- 主畫面 ---
if st.button("🔄 更新即時報價與走勢", type="primary", use_container_width=True):
    with st.spinner('正在連線交易所抓取最新數據...'):
        usdtwd = get_usdtwd()
        h = data.get('h', {})
        
        temp_list = []
        total_mkt_val = 0.0
        total_cost_val = 0.0
        total_debt = 0.0

        for code, info in h.items():
            cur_p, change_val, change_pct = get_price_data(code)
            if cur_p is None or pd.isna(cur_p): cur_p = info['c']
            
            rate = 1.0 if ('.TW' in code or '.TWO' in code) else usdtwd
            s_val = float(info['s'])
            c_val = float(info['c'])
            p_val = float(cur_p)
            
            mkt_val = p_val * s_val * rate
            cost_val = c_val * s_val * rate
            total_profit_val = mkt_val - cost_val
            total_profit_pct = (total_profit_val / cost_val * 100) if cost_val else 0
            day_profit_val = change_val * s_val * rate
            
            stock_debt = sum(l.get('debt', 0) for l in info.get('lots', []))
            
            total_mkt_val += mkt_val
            total_cost_val += cost_val
            total_debt += stock_debt

            name = STOCK_MAP.get(code, code)

            temp_list.append({
                "raw_code": code,
                "股票代碼": code,
                "公司名稱": name,
                "股數": int(s_val),
                "成本": c_val,
                "現價": p_val,
                "日損益%": change_pct / 100,
                "日損益": day_profit_val,
                "總損益%": total_profit_pct / 100,
                "總損益": total_profit_val,
                "市值": mkt_val,
                "mkt_val_raw": mkt_val
            })

        final_rows = []
        for item in temp_list:
            weight = (item['mkt_val_raw'] / total_mkt_val) if total_mkt_val > 0 else 0
            item["占比"] = weight
            final_rows.append(item)

        net_asset = (total_mkt_val + data['cash']) - total_debt
        unrealized_profit = total_mkt_val - total_cost_val
        if client: record_history(client, net_asset)

        # 計算已實現總損益
        total_realized = sum(r.get('profit', 0) for r in data.get('history', []))

        # --- KPI ---
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("💰 淨資產", f"${int(net_asset):,}")
        k2.metric("📊 總市值", f"${int(total_mkt_val):,}")
        k3.metric("💸 總負債", f"${int(total_debt):,}", delta_color="inverse")
        k4.metric("未實現損益", f"${int(unrealized_profit):+,}", delta_color="normal")
        k5.metric("已實現損益", f"${int(total_realized):+,}", delta=(int(total_realized) if total_realized!=0 else None))

        # --- Tabs ---
        tab1, tab2, tab3, tab4 = st.tabs(["📋 庫存明細", "🗺️ 熱力圖", "📈 走勢圖", "📜 已實現損益"])

        def color_profit(val):
            color = 'red' if val > 0 else 'green' if val < 0 else 'black'
            return f'color: {color}'

        with tab1:
            if final_rows:
                df = pd.DataFrame(final_rows)
                cols = ['股票代碼', '公司名稱', '股數', '成本', '現價', '日損益%', '日損益', '總損益%', '總損益', '市值', '占比']
                df = df[cols]
                styler = df.style.format({
                    '股數': '{:,}', '成本': '{:,.2f}', '現價': '{:,.2f}',
                    '日損益%': '{:+.2%}', '日損益': '{:+,.0f}',
                    '總損益%': '{:+.2%}', '總損益': '{:+,.0f}',
                    '市值': '{:,.0f}', '占比': '{:.1%}'
                }).map(color_profit, subset=['日損益%', '日損益', '總損益%', '總損益'])
                st.dataframe(styler, use_container_width=True, height=500, hide_index=True)
            else:
                st.info("無庫存資料")

        with tab2:
            if temp_list:
                df_tree = pd.DataFrame(temp_list)
                fig_tree = px.treemap(
                    df_tree, path=['股票代碼'], values='mkt_val_raw', color='日損益%',
                    color_continuous_scale='RdYlGn_r', color_continuous_midpoint=0,
                    custom_data=['公司名稱', '日損益%']
                )
                fig_tree.update_traces(texttemplate="%{label}<br>%{customdata[0]}<br>%{customdata[1]:+.2%}", textposition="middle center")
                st.plotly_chart(fig_tree, use_container_width=True)
            else: st.info("無數據")

        with tab3:
            if client:
                hs = get_history_sheet(client)
                if hs:
                    hvals = hs.get_all_values()
                    if len(hvals) > 1:
                        dfh = pd.DataFrame(hvals[1:], columns=hvals[0])
                        dfh['Date'] = pd.to_datetime(dfh['Date'])
                        dfh['NetAsset'] = pd.to_numeric(dfh['NetAsset'])
                        dfh = dfh.set_index('Date')
                        fig = px.line(dfh, y='NetAsset', markers=True)
                        fig.update_traces(line_color='#1f77b4', line_width=3)
                        st.plotly_chart(fig, use_container_width=True)
                    else: st.info("累積數據不足")
            else: st.error("無法讀取歷史")

        with tab4:
            history = data.get('history', [])
            if history:
                # 反轉列表，讓最新的在最上面
                df_hist = pd.DataFrame(history[::-1])
                st.subheader(f"累計已實現損益: ${int(total_realized):+,}")
                
                if not df_hist.empty:
                    df_hist = df_hist[['d', 'code', 'name', 'qty', 'buy_cost', 'sell_rev', 'profit', 'roi']]
                    df_hist.columns = ['日期', '代碼', '名稱', '賣出股數', '總成本', '賣出收入', '獲利金額', '報酬率%']
                    
                    # 格式化
                    df_hist['報酬率%'] = df_hist['報酬率%'] / 100
                    
                    styler_h = df_hist.style.format({
                        '賣出股數': '{:,}',
                        '總成本': '{:,.0f}',
                        '賣出收入': '{:,.0f}',
                        '獲利金額': '{:+,.0f}',
                        '報酬率%': '{:+.2%}'
                    }).map(color_profit, subset=['獲利金額', '報酬率%'])
                    
                    st.dataframe(styler_h, use_container_width=True, hide_index=True)
            else:
                st.info("尚無賣出紀錄")

else:
    st.info("👆 請點擊上方按鈕更新")
