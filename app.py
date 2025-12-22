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

# --- 股票代碼與名稱對照表 (可自行擴充) ---
STOCK_MAP = {
    '2330.TW': '台積電', '2317.TW': '鴻海', '2454.TW': '聯發科',
    '2603.TW': '長榮', '2609.TW': '陽明', '2615.TW': '萬海',
    '3231.TW': '緯創', '2382.TW': '廣達', '3017.TW': '奇鋐',
    '2301.TW': '光寶科', '00685L.TW': '群益台指正2', '00670L.TW': '元大NASDAQ正2',
    'NVDA': '輝達', 'AAPL': '蘋果', 'TSLA': '特斯拉', 'AMD': '超微'
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
    if not sheet: return {'h': {}, 'cash': 0.0}
    try:
        raw_data = sheet.acell('A1').value
        if raw_data:
            data = json.loads(raw_data)
            for code in data.get('h', {}):
                if 'lots' not in data['h'][code]:
                    data['h'][code]['lots'] = [{
                        'd': '初始', 'p': data['h'][code]['c'], 's': data['h'][code]['s'], 'type': '現股', 'debt': 0
                    }]
            return data
    except: pass
    return {'h': {}, 'cash': 0.0}

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

# --- 側邊欄：交易面板 ---
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
    st.subheader("下單交易")
    code_in = st.text_input("代碼 (如 2330.TW)").strip().upper()
    col1, col2 = st.columns(2)
    shares_in = col1.number_input("股數", min_value=1, value=1000, step=100)
    cost_in = col2.number_input("成交單價", min_value=0.0, value=0.0, step=0.1, format="%.2f")
    trade_type = st.radio("交易類別", ["現股", "融資"], horizontal=True)
    
    margin_ratio = 1.0
    debt = 0
    if trade_type == "融資":
        margin_ratio = st.slider("自備款成數", 0.1, 0.9, 0.4, 0.1)

    if st.button("買入 / 加碼確認", type="primary"):
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
                st.success(f"交易成功！{code_in}"); st.balloons(); st.rerun()
        else: st.error("資料不完整")

# --- 主畫面 ---
if st.button("🔄 更新即時報價與走勢", type="primary", use_container_width=True):
    with st.spinner('正在連線交易所抓取最新數據...'):
        usdtwd = get_usdtwd()
        h = data.get('h', {})
        
        # 1. 先跑一輪計算總市值，為了算「占比」
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
            
            # 市值與損益
            mkt_val = p_val * s_val * rate
            cost_val = c_val * s_val * rate
            total_profit_val = mkt_val - cost_val
            total_profit_pct = (total_profit_val / cost_val * 100) if cost_val else 0
            
            # 日損益 (簡單估算：今日漲跌金額 * 股數 * 匯率)
            day_profit_val = change_val * s_val * rate
            
            # 負債
            stock_debt = sum(l.get('debt', 0) for l in info.get('lots', []))
            
            total_mkt_val += mkt_val
            total_cost_val += cost_val
            total_debt += stock_debt

            # 抓取中文名稱 (如果沒有就顯示代碼)
            name = STOCK_MAP.get(code, code)

            temp_list.append({
                "raw_code": code, # 隱藏欄位，用於排序或連結
                "股票代碼": code,
                "公司名稱": name,
                "股數": int(s_val),
                "成本": c_val,
                "現價": p_val,
                "日損益%": change_pct / 100, # 除100以便後續格式化
                "日損益": day_profit_val,
                "總損益%": total_profit_pct / 100,
                "總損益": total_profit_val,
                "市值": mkt_val,
                "mkt_val_raw": mkt_val # 用於計算占比
            })

        # 2. 計算占比並整理表格
        final_rows = []
        for item in temp_list:
            weight = (item['mkt_val_raw'] / total_mkt_val) if total_mkt_val > 0 else 0
            item["占比"] = weight
            final_rows.append(item)

        # 3. 總計數據
        net_asset = (total_mkt_val + data['cash']) - total_debt
        unrealized_profit = total_mkt_val - total_cost_val
        if client: record_history(client, net_asset)

        # --- 顯示 KPI ---
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("💰 淨資產", f"${int(net_asset):,}")
        k2.metric("📊 總市值", f"${int(total_mkt_val):,}")
        k3.metric("💸 總負債", f"${int(total_debt):,}", delta_color="inverse")
        k4.metric("未實現損益", f"${int(unrealized_profit):+,}", delta_color="normal")

        # --- Tabs ---
        tab1, tab2, tab3 = st.tabs(["📋 庫存明細", "🗺️ 熱力圖", "📈 走勢圖"])

        with tab1:
            if final_rows:
                df = pd.DataFrame(final_rows)
                
                # 設定顯示欄位順序
                cols = ['股票代碼', '公司名稱', '股數', '成本', '現價', '日損益%', '日損益', '總損益%', '總損益', '市值', '占比']
                df = df[cols]

                # Pandas Styler: 依據圖片風格設計
                # 定義格式化函數
                def color_profit(val):
                    color = 'red' if val > 0 else 'green' if val < 0 else 'black'
                    return f'color: {color}'

                # 建立 Styler
                styler = df.style.format({
                    '股數': '{:,}',
                    '成本': '{:,.2f}',
                    '現價': '{:,.2f}',
                    '日損益%': '{:+.2%}',
                    '日損益': '{:+,.0f}',
                    '總損益%': '{:+.2%}',
                    '總損益': '{:+,.0f}',
                    '市值': '{:,.0f}',
                    '占比': '{:.1%}'
                })
                
                # 套用顏色 (針對數值欄位)
                styler = styler.map(color_profit, subset=['日損益%', '日損益', '總損益%', '總損益'])
                
                # 顯示表格
                st.dataframe(styler, use_container_width=True, height=500, hide_index=True)
                
                # 刪除功能
                st.markdown("---")
                to_del = st.selectbox("選擇要刪除的股票", ["請選擇"] + [r['股票代碼'] for r in final_rows])
                if to_del != "請選擇" and st.button(f"確認刪除 {to_del}"):
                    # 計算退回金額
                    t_back = 0
                    rate = 1.0 if ('.TW' in to_del or '.TWO' in to_del) else usdtwd
                    for l in h[to_del].get('lots', []):
                        cost_t = l['p'] * l['s'] * rate
                        debt = l.get('debt', 0)
                        t_back += (cost_t - debt)
                    data['cash'] += t_back
                    del data['h'][to_del]
                    save_data(sheet, data)
                    st.success("已刪除"); st.rerun()
            else:
                st.info("無庫存資料")

        with tab2:
            if temp_list:
                df_tree = pd.DataFrame(temp_list)
                # 修正：使用 'RdYlGn_r' (紅-黃-綠 反轉)，讓紅色代表高數值(漲)，綠色代表低數值(跌)
                fig_tree = px.treemap(
                    df_tree, 
                    path=['股票代碼'], 
                    values='mkt_val_raw',
                    color='日損益%',
                    color_continuous_scale='RdYlGn_r', 
                    color_continuous_midpoint=0,
                    custom_data=['公司名稱', '日損益%']
                )
                fig_tree.update_traces(
                    texttemplate="%{label}<br>%{customdata[0]}<br>%{customdata[1]:+.2%}",
                    textposition="middle center"
                )
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

else:
    st.info("👆 請點擊上方按鈕更新")
