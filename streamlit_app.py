import streamlit as st
import yfinance as yf
import pandas as pd
import requests
import io
import concurrent.futures
from datetime import datetime

# --- 1. CONFIGURATION ---
st.set_page_config(layout="wide", page_title="TARA Institutional Scanner", page_icon="💎")

# TARA Branding - Dark Mode & Gold Accents
st.markdown("""
    <style>
    .stApp { background-color: #0e1117; color: #ffffff; }
    h1, h2, h3 { color: #FFD700 !important; }
    div[data-testid="stMetricValue"] { color: #00FFFF; font-weight: bold; }
    .stDataFrame { border: 1px solid #444; }
    </style>
    """, unsafe_allow_html=True)

st.title("💎 TARA INSTITUTIONAL SCANNER")
st.markdown("### The 'Pathology Lab' for NSE Cash (2,200+ Stocks)")
st.caption("Powered by TARA Proprietary Logic | Institutional Swing Grade")

# --- 2. DATA ENGINE (High Reliability) ---

@st.cache_data(ttl=3600)  # Cache list for 1 hour to save time
def get_nse_tickers():
    """Fetches the official active stock list from NSE."""
    try:
        # Official NSE Source
        url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)
        df = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
        
        # Clean & Format for yfinance
        df = df[df['SERIES'] == 'EQ']  # Only Equity
        tickers = [f"{symbol}.NS" for symbol in df['SYMBOL'].tolist()]
        return tickers
    except Exception as e:
        st.error(f"⚠️ Error fetching NSE List: {e}. Using backup list.")
        return ["RELIANCE.NS", "HDFCBANK.NS", "INFY.NS", "ITC.NS", "SBIN.NS"] # Fallback

# --- 3. THE TARA 'SMART MONEY' FORMULA ---

def analyze_stock(ticker):
    """
    Apply TARA Equity Lab Logic:
    1. Consistency (>60% Green)
    2. Efficiency (Path Quality > 0.3)
    3. Yearly VWAP (Institutional Base)
    """
    try:
        # Fetch Data (Fast Mode)
        df = yf.download(ticker, period="1y", interval="1d", progress=False, threads=False)
        
        if len(df) < 50: return None
        
        # --- A. LIQUIDITY FILTER (Critical for 2200 stocks) ---
        # Ignore stocks with < ₹50 Lakhs turnover or Price < 10 (Penny stocks)
        avg_vol = df['Volume'].iloc[-5:].mean()
        price = df['Close'].iloc[-1]
        if (avg_vol * price) < 5000000 or price < 10: return None

        # --- B. YEARLY VWAP ---
        # Calculate from start of current year
        current_year = df.index[-1].year
        df_y = df[df.index.year == current_year].copy()
        
        if df_y.empty: return None
        
        # VWAP Formula
        df_y['VP'] = df_y['Close'] * df_y['Volume']
        yvwap_val = df_y['VP'].cumsum() / df_y['Volume'].cumsum()
        current_yvwap = yvwap_val.iloc[-1]

        # --- C. FACTOR 1: CONSISTENCY (60% Rule) ---
        # Are 12 out of last 20 candles Green?
        green_candles = (df['Close'] > df['Open']).astype(int).tail(20).sum()
        consistency = (green_candles / 20) * 100

        # --- D. FACTOR 2: EFFICIENCY (Smart Money Path) ---
        # Net Move / Total Distance
        lookback = 20
        net_change = abs(df['Close'].iloc[-1] - df['Close'].iloc[-lookback])
        total_path = abs(df['Close'].diff()).tail(lookback).sum()
        efficiency = net_change / total_path if total_path != 0 else 0

        # --- E. DIAGNOSIS ---
        status = "NEUTRAL"
        
        # Criteria: Above YVWAP AND High Consistency AND Efficient Move
        if price > current_yvwap and consistency >= 60 and efficiency >= 0.30:
            status = "💎 INSTITUTIONAL BUY"
        elif price > current_yvwap and consistency >= 50:
            status = "📝 WATCHLIST"
            
        if status == "NEUTRAL": return None # Don't return junk to save memory

        return {
            "Symbol": ticker.replace(".NS", ""),
            "Price": round(price, 2),
            "Status": status,
            "Consistency": f"{int(consistency)}%",
            "Efficiency": round(efficiency, 2),
            "YVWAP %": f"{round(((price - current_yvwap)/current_yvwap)*100, 1)}%"
        }

    except:
        return None

# --- 4. THE SCANNING ENGINE (Multi-Threaded) ---

def run_scan(ticker_list):
    results = []
    # Progress Bar
    progress_text = st.empty()
    bar = st.progress(0)
    
    # THREADING: Scan 50 stocks at once instead of 1 by 1
    # This makes it 10x Faster
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        future_to_ticker = {executor.submit(analyze_stock, t): t for t in ticker_list}
        
        completed = 0
        total = len(ticker_list)
        
        for future in concurrent.futures.as_completed(future_to_ticker):
            data = future.result()
            if data:
                results.append(data)
            
            completed += 1
            if completed % 50 == 0: # Update UI every 50 stocks
                bar.progress(completed / total)
                progress_text.text(f"Scanning... {completed}/{total} stocks analyzed")
    
    bar.empty()
    progress_text.empty()
    return pd.DataFrame(results)

# --- 5. USER INTERFACE ---

# Sidebar Controls
universe = st.sidebar.radio("Select Universe:", ["Nifty 50 (Demo)", "Nifty 500 (Standard)", "Full NSE Cash (Deep Scan)"])

# Load Tickers
all_tickers = get_nse_tickers()

if universe == "Nifty 50 (Demo)":
    # Static fast list for demo
    targets = ["RELIANCE.NS", "HDFCBANK.NS", "INFY.NS", "TCS.NS", "ITC.NS", "SBIN.NS", "BHARTIARTL.NS"] * 2 
    # (In real deployment, use a real Nifty 50 list logic or strict slice)
    targets = all_tickers[:50] 
elif universe == "Nifty 500 (Standard)":
    targets = all_tickers[:500]
else:
    targets = all_tickers # All 2200+

st.info(f"Target Universe: {len(targets)} Stocks. 'Full Scan' may take 2-4 minutes.")

if st.button("🚀 START TARA SCAN", type="primary"):
    with st.spinner("Analyzing Market Structure... Please Wait..."):
        df_results = run_scan(targets)
    
    if not df_results.empty:
        # SPLIT RESULTS
        df_diamonds = df_results[df_results['Status'] == "💎 INSTITUTIONAL BUY"]
        df_watch = df_results[df_results['Status'] == "📝 WATCHLIST"]
        
        # 1. DIAMOND TABLE
        st.subheader(f"💎 DIAMOND SETUPS ({len(df_diamonds)})")
        st.markdown("*Criteria: Above Yearly VWAP + Consistency > 60% + Efficiency > 0.3*")
        if not df_diamonds.empty:
            st.dataframe(
                df_diamonds.style.applymap(lambda x: 'color: #00FF00; font-weight: bold', subset=['Status']),
                use_container_width=True, 
                hide_index=True
            )
        else:
            st.info("No Diamond setups found right now.")
            
        # 2. WATCHLIST TABLE
        st.subheader(f"📝 WATCHLIST ({len(df_watch)})")
        st.markdown("*Criteria: Above Yearly VWAP + Consistency > 50%*")
        if not df_watch.empty:
            st.dataframe(
                df_watch.style.applymap(lambda x: 'color: #FFD700;', subset=['Status']),
                use_container_width=True, 
                hide_index=True
            )
    else:
        st.warning("No stocks met the TARA criteria today.")
