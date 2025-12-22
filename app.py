import streamlit as st
import pandas as pd
import yfinance as yf
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

# 設定頁面配置 (加入 icon)
st.set_page_config(page_title="全功能資產管家", layout="wide", page_icon="📈")

# --- Google Sheets 連線與資料處理 ---
def get_google_client():
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds_dict = json.loads(st.secrets["service_account_info"])
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

# 新增：取得或建立歷史紀錄分頁
def get_history_sheet(client):
    try:
        sheet_name = st.secrets["spreadsheet_name"]
        spreadsheet = client.open(sheet_name)
        try:
            # 嘗試開啟名為 'History' 的分頁
            history_sheet = spreadsheet.worksheet('History')
        except gspread.exceptions.WorksheetNotFound:
            # 如果不存在就建立一個新的，並加上標題列
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
            # 資料結構升級防呆：確保舊資料有 lots 欄位
            for code in data.get('h', {}):
                if 'lots' not in data['h'][code]:
                    # 假設舊資料都是現股，無負債
                    data['h'][code]['lots'] = [{
                        'd': '初始', 
                        'p': data['h'][code]['c'], 
                        's': data['h'][code]['s'],
                        'type': '現股',
                        'debt': 0
                    }]
            return data
    except:
        pass
    return {'h': {}, 'cash': 0.0}

def save_data(sheet, data):
    if sheet:
        try:
            json_str = json.dumps(data, ensure_ascii=False)
            sheet.update_acell('A1', json_str)
        except Exception as e:
            st.error(f"存檔失敗: {e}")

# 新增：紀錄歷史淨值
def record_history(client, net_asset):
    hist_sheet = get_history_sheet(client)
    if hist_sheet and net_asset > 0:
        today = datetime.now().strftime('%Y-%m-%d')
        # 讀取最後一筆紀錄，避免重複紀錄同一天
        try:
            last_row = hist_sheet.get_all_values()[-1]
            last_date = last_row[0]
            if last_date == today:
                # 今天已記錄過，更新數值即可 (選擇性功能，這裡先不更新，每日一記)
                return 
        except:
            pass # 可能是新表，沒資料
            
        hist_sheet.append_row([today, int(net_asset)])

# --- 核心計算邏輯 ---
@st.cache_data(ttl=60) # 快取 60 秒，避免頻繁抓取
def get_price_data(ticker):
    try:
        stock = yf.Ticker(ticker)
        # 嘗試抓取包含漲跌幅的完整資訊
        hist = stock.history(period='2d') # 抓兩天資料來算漲跌
        if len(hist) >= 1:
            price = hist['Close'].iloc[-1]
            prev_close = hist['Close'].iloc[-2] if len(hist) >= 2 else price
            change_pct = ((price - prev_close) / prev_close * 100) if prev_close else 0
            return price, change_pct
        
        # 如果 history 失敗，嘗試 fast_info (但 fast_info 常常沒有漲跌資訊)
        price = stock.fast_info.get('last_price')
        if price and not pd.isna(price):
             # 嘗試從 info 抓昨收
             prev = stock.info.get('previousClose', price)
             change_pct = ((price - prev) / prev * 100) if prev else 0
             return price, change_pct
             
        return None, 0
    except:
        return None, 0

@st.cache_data(ttl=300)
def get_usdtwd():
    try:
        fx = yf.Ticker('USDTWD=X')
        p = fx.fast_info.get('last_price')
        return p if p and not pd.isna(p) else 32.5
    except:
        return 32.5

# --- 介面開始 ---
st.title("📈 全功能股票資產管家")

# 初始化狀態
if 'client' not in st.session_state:
    st.session_state.client = get_google_client()

# 修正：無論 client 是否連線成功，都要初始化 sheet 變數，避免後面報錯
if 'sheet' not in st.session_state:
    if st.session_state.client:
        st.session_state.sheet = get_main_sheet(st.session_state.client)
    else:
        st.session_state.sheet = None

client = st.session_state.client
sheet = st.session_state.sheet

if 'data' not in st.session_state:
    st.session_state.data = load_data(sheet)

data = st.session_state.data

if not sheet:
    st.error("⚠️ 嚴重錯誤：無法連接 Google Sheets。請檢查 Secrets 設定。")
    st.stop()

# --- 側邊欄：交易面板 ---
with st.sidebar:
    st.header("💰 資金與交易")
    
    current_cash = data.get('cash', 0.0)
    st.metric("現金餘額", f"${int(current_cash):,}", help="可用於買股的現金")
    
    with st.expander("💵 資金存提"):
        cash_op = st.number_input("金額 (正存/負提)", step=1000.0)
        if st.button("執行異動"):
            data['cash'] += cash_op
            save_data(sheet, data)
            st.success(f"資金已更新，目前: ${int(data['cash']):,}")
            st.rerun()

    st.markdown("---")
    st.subheader("下單交易")
    code = st.text_input("代碼 (如 2330.TW, NVDA)").strip().upper()
    
    col_s1, col_s2 = st.columns(2)
    shares = col_s1.number_input("股數", min_value=1, value=1000, step=100)
    cost = col_s2.number_input("成交單價", min_value=0.0, value=0.0, step=0.1, format="%.2f")
    
    # 新增：交易類別選擇
    trade_type = st.radio("交易類別", ["現股", "融資"], horizontal=True)
    margin_ratio = 1.0
    debt = 0
    
    if trade_type == "融資":
        margin_ratio = st.slider("自備款成數 (例如 0.4 = 四成)", 0.1, 0.9, 0.4, 0.1)
        st.caption(f"融資成數: {1-margin_ratio:.1f} (向券商借款)")

    if st.button("買入 / 加碼確認", type="primary"):
        if code and cost > 0:
            if 'h' not in data: data['h'] = {}
            
            rate = 1.0 if ('.TW' in code or '.TWO' in code) else get_usdtwd()
            
            # 計算總成本、自備款與負債
            raw_cost_twd = cost * shares * rate
            cash_needed = raw_cost_twd * margin_ratio
            debt_created = raw_cost_twd - cash_needed
            
            if data['cash'] < cash_needed:
                 st.error(f"現金不足！需要 ${int(cash_needed):,}，目前只有 ${int(data['cash']):,}")
            else:
                # 扣除現金
                data['cash'] -= cash_needed
                
                # 建立新的批次紀錄 (Lot)
                new_lot = {
                    'd': datetime.now().strftime('%Y-%m-%d'),
                    'p': cost,
                    's': shares,
                    'type': trade_type,
                    'debt': debt_created # 記錄這筆交易產生的負債
                }
                
                # 更新庫存結構
                if code in data['h']:
                    # 確保 lots 存在
                    if 'lots' not in data['h'][code]: data['h'][code]['lots'] = []
                    lots = data['h'][code]['lots']
                    lots.append(new_lot)
                    
                    # 重新計算加權平均成本
                    total_s = sum(l['s'] for l in lots)
                    total_c_val = sum(l['s'] * l['p'] for l in lots)
                    data['h'][code]['s'] = total_s
                    data['h'][code]['c'] = total_c_val / total_s if total_s else 0
                    data['h'][code]['lots'] = lots
                else:
                    data['h'][code] = {'s': shares, 'c': cost, 'n': code, 'lots': [new_lot]}
                
                save_data(sheet, data)
                st.success(f"交易成功！{code} {shares}股 ({trade_type})，自備款: ${int(cash_needed):,}，負債: ${int(debt_created):,}")
                st.balloons()
                st.rerun()
        else:
            st.error("請輸入完整的代碼與價格")

# --- 主畫面 ---

# 更新按鈕區塊 (核心功能)
if st.button("🔄 更新即時報價與走勢", type="primary", use_container_width=True):
    with st.spinner('正在連線交易所抓取最新數據，並計算損益與繪圖...'):
        usdtwd = get_usdtwd()
        total_mkt_val = 0.0
        total_cost_val = 0.0
        total_debt = 0.0 # 新增：總負債
        
        table_rows = []
        treemap_data = [] # 熱力圖資料
        
        h = data.get('h', {})
        
        for code, info in h.items():
            # 1. 抓價與漲跌幅
            cur_p, change_pct = get_price_data(code)
            if cur_p is None or pd.isna(cur_p): cur_p = info['c'] # 防呆：抓不到用成本
            
            rate = 1.0 if ('.TW' in code or '.TWO' in code) else usdtwd
            s_val = float(info['s'])
            c_val = float(info['c'])
            p_val = float(cur_p)
            
            # 2. 計算市值與損益
            mkt_val = p_val * s_val * rate
            cost_val = c_val * s_val * rate
            profit = mkt_val - cost_val
            profit_pct = (profit / cost_val * 100) if cost_val else 0
            
            total_mkt_val += mkt_val
            total_cost_val += cost_val
            
            # 3. 計算該檔股票的總負債 (累加所有 lots 的 debt)
            stock_debt = sum(l.get('debt', 0) for l in info.get('lots', []))
            total_debt += stock_debt

            # 4. 準備表格資料
            table_rows.append({
                "代碼": code,
                "持有股數": int(s_val),
                "平均成本": f"{c_val:.2f}",
                "現價": f"{p_val:.2f}",
                "漲跌幅": f"{change_pct:+.2f}%",
                "市值 (TWD)": int(mkt_val),
                "融資負債": int(stock_debt),
                "未實現損益": int(profit),
                "報酬率": f"{profit_pct:+.2f}%"
            })
            
            # 5. 準備熱力圖資料 (市值大於0才加入)
            if mkt_val > 0:
                treemap_data.append({
                    'ticker': code,
                    'market_value': mkt_val,
                    'daily_change': change_pct,
                    'label_text': f"{code}\n{change_pct:+.2f}%"
                })
        
        # 6. 計算最終淨資產
        net_asset = (total_mkt_val + data['cash']) - total_debt
        total_profit = total_mkt_val - total_cost_val

        # 7. 紀錄歷史走勢到 Google Sheets
        if client:
            record_history(client, net_asset)
        
        # --- 展示層 ---
        
        # KPI 指標卡
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        kpi1.metric("💰 淨資產總額", f"${int(net_asset):,}", help="市值 + 現金 - 負債")
        kpi2.metric("📊 證券總市值", f"${int(total_mkt_val):,}")
        kpi3.metric("💸 融資總負債", f"${int(total_debt):,}", delta_color="inverse")
        safe_profit = int(total_profit) if not pd.isna(total_profit) else 0
        kpi4.metric("損益 (未實現)", f"${safe_profit:+,}", delta=(safe_profit if safe_profit!=0 else None))

        # 分頁展示視圖
        tab1, tab2, tab3 = st.tabs(["📋 庫存明細", "🗺️ 市場熱力圖", "📈 資產走勢圖"])

        with tab1:
            if table_rows:
                df_table = pd.DataFrame(table_rows)
                # 設定樣式：負債顯示紅色
                st.dataframe(df_table.style.format({"融資負債": "{:,}", "市值 (TWD)": "{:,}", "未實現損益": "{:+,.0f}"})
                             .applymap(lambda v: 'color: red;' if isinstance(v, int) and v > 0 else None, subset=['融資負債'])
                             , use_container_width=True, height=400)
                
                st.markdown("---")
                st.subheader("🗑️ 刪除庫存")
                to_del = st.selectbox("選擇要刪除的股票 (將退回原始自備款)", ["請選擇"] + list(h.keys()))
                if to_del != "請選擇":
                    if st.button(f"確認刪除 {to_del}"):
                        # 簡化版刪除：退回所有批次的「原始自備款」總和
                        total_equity_back = 0
                        is_tw = ('.TW' in to_del or '.TWO' in to_del)
                        rate = 1.0 if is_tw else usdtwd
                        
                        for l in h[to_del].get('lots', []):
                            cost_twd = l['p'] * l['s'] * rate
                            debt = l.get('debt', 0)
                            equity = cost_twd - debt # 這一批的自備款
                            total_equity_back += equity
                            
                        data['cash'] += total_equity_back
                        del data['h'][to_del]
                        save_data(sheet, data)
                        st.success(f"已刪除 {to_del}，退回自備款約 ${int(total_equity_back):,}")
                        st.rerun()
            else:
                st.info("目前沒有庫存，請由左側新增。")

        with tab2:
            if treemap_data:
                df_tree = pd.DataFrame(treemap_data)
                fig_tree = px.treemap(
                    df_tree, 
                    path=['ticker'], 
                    values='market_value',
                    color='daily_change',
                    color_continuous_scale='RdGn_r', # 紅跌綠漲 (反向)
                    color_continuous_midpoint=0,
                    hover_data=['ticker', 'market_value', 'daily_change'],
                    custom_data=['label_text']
                )
                fig_tree.update_traces(textposition="middle center", texttemplate="%{customdata[0]}")
                fig_tree.update_layout(margin=dict(t=20, l=10, r=10, b=10), height=500)
                st.plotly_chart(fig_tree, use_container_width=True)
            else:
                st.info("無足夠數據繪製熱力圖")

        with tab3:
            st.subheader("淨資產歷史走勢 (NAV)")
            if client:
                hist_sheet = get_history_sheet(client)
                if hist_sheet:
                    hist_data = hist_sheet.get_all_values()
                    if len(hist_data) > 1:
                        df_hist = pd.DataFrame(hist_data[1:], columns=hist_data[0])
                        df_hist['Date'] = pd.to_datetime(df_hist['Date'])
                        df_hist['NetAsset'] = pd.to_numeric(df_hist['NetAsset'])
                        df_hist = df_hist.set_index('Date')
                        
                        fig_line = px.line(df_hist, y='NetAsset', markers=True)
                        fig_line.update_traces(line_color='#1f77b4', line_width=3)
                        fig_line.update_layout(
                            xaxis_title="日期", yaxis_title="淨資產 (TWD)",
                            hovermode="x unified", height=500
                        )
                        st.plotly_chart(fig_line, use_container_width=True)
                    else:
                        st.info("尚無足夠的歷史紀錄，請每天按一次更新按鈕來累積數據。")
            else:
                st.error("無法讀取歷史紀錄")

else:
    # 初始畫面提示
    st.info("👆 請點擊上方「🔄 更新即時報價與走勢」按鈕，開始計算最新資產狀況。")
    st.caption("說明：每次點擊更新時，系統會自動將當下的「淨資產總額」記錄到 Google Sheets 的 'History' 分頁中，用於繪製走勢圖。")

# 除錯用 (可註解掉)
# with st.expander("查看原始資料 (JSON)"):
#     st.json(data)
