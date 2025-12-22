# --- 側邊欄：資金與下單 (包含本金校正功能) ---
with st.sidebar:
    st.header("💰 資金與交易")
    st.metric("現金餘額", f"${int(data.get('cash', 0)):,}")
    
    # [新增] 系統設定區塊 - 用來修正本金
    with st.expander("⚙️ 系統設定 / 本金校正"):
        st.info("若剛升級或是報酬率計算異常，請點擊下方按鈕校正。")
        if st.button("🔄 自動校正本金"):
            # 邏輯：本金應該等於 = 現金 + 所有持股的總成本
            current_stock_cost = 0
            for code, info in data.get('h', {}).items():
                # 計算持股成本 (股數 * 平均成本)
                # 注意：這邊抓的是成本價，不是現價，這樣才是對的「投入本金」
                s = info.get('s', 0)
                c = info.get('c', 0)
                # 扣除融資負債的影響 (本金 = 總成本 - 借來的錢)
                debt = sum(l.get('debt', 0) for l in info.get('lots', []))
                
                # 若是台股，成本大致計算 (假設匯率1，若有精確需求可再細化)
                # 這裡做個簡化：直接加總台幣成本
                rate = 1.0 if ('.TW' in code or '.TWO' in code) else get_usdtwd()
                current_stock_cost += (s * c * rate) - debt
            
            # 新的本金 = 現金 + 股票權益成本
            new_principal = data['cash'] + current_stock_cost
            data['principal'] = new_principal
            save_data(sheet, data)
            st.success(f"本金已校正為: ${int(new_principal):,}")
            st.rerun()

    with st.expander("💵 資金存提 (影響本金)"):
        cash_op = st.number_input("金額 (正存/負提)", step=1000.0)
        if st.button("執行異動"):
            data['cash'] += cash_op
            # 更新本金紀錄
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
            # 取得即時匯率
            rate = 1.0 if ('.TW' in code_in or '.TWO' in code_in) else get_usdtwd()
            
            total_twd = cost_in * shares_in * rate
            cash_needed = total_twd * margin_ratio
            debt_created = total_twd - cash_needed
            
            if data['cash'] < cash_needed:
                 st.error(f"現金不足！需 ${int(cash_needed):,}，現有 ${int(data['cash']):,}")
            else:
                data['cash'] -= cash_needed
                # 注意：買入操作「不會」增加本金，因為只是 現金 -> 股票 的轉換
                
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
                    
                    # 賣出獲利/虧損 會自然反映在 NetAsset 變化，本金不需要變動
                    
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
    with st.expander("🔧 修正/刪除"):
        del_list = list(data.get('h', {}).keys())
        if del_list:
            to_del_code = st.selectbox("刪除", ["請選擇"] + del_list)
            if to_del_code != "請選擇" and st.button("強制刪除"):
                del data['h'][to_del_code]
                save_data(sheet, data)
                st.rerun()
