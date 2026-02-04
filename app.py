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
APP_VERSION = "v2.7 (Safe Mode)"

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
        except gspread.exceptions.Worksheet
