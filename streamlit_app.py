import streamlit as st
import yfinance as yf
import pandas as pd
import pandas_ta as ta
import requests
import io
import concurrent.futures
import time

# --- 1. CONFIGURATION ---
st.set_page_config(layout="wide", page_title="TARA Institutional Scanner", page_icon="💎")

st.markdown("""
    <style>
    .stApp { background-color: #0e1117; color: #ffffff; }
    h1, h2, h3 { color: #FFD700 !important; }
    div[data-testid="stMetricValue"] { color: #00FFFF; font-weight: bold; }
    .stDataFrame { border: 1px solid #444; }
    .debug-box { font-family: monospace; font-size: 12px; color: #aaa; }
    </style>
    """, unsafe_allow_html=True)

st.title("💎 TARA INSTITUTIONAL SCANNER")
st.markdown("### The 'Pathology Lab' for NSE Cash (2,200+ Stocks)")

# --- 2. DATA ENGINE (Robust) ---
@st.cache_data(ttl=3600)
def get_nse_tickers():
    try:
        url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        response = requests.get(url, headers=headers, timeout=10)
        
        # Read and Clean CSV
        df = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
        df.columns = df.columns.str.strip() # FIX: Remove hidden spaces
        
        # Filter for Equity Series (EQ)
        if 'SERIES' in df.columns:
            df = df[df['SERIES'] == 'EQ']
            
        tickers = [f"{symbol}.NS" for symbol in df['SYMBOL'].tolist()]
        return tickers
        
    except Exception as e:
        st.error(f"⚠️ Network Error fetching NSE List: {e}. Using backup.")
        return ["RELIANCE.NS", "HDFCBANK.NS", "INFY.NS", "ITC.NS", "TCS.NS", "SBIN.NS"]

# --- 3. THE TARA LOGIC ---
def analyze_stock(ticker):
    try:
        # Download Data (Small history to prevent timeouts)
        # Using threads=False inside yf to prevent conflicts with our outer executor
        df = yf.download(ticker, period="6mo", interval="1d", progress=False, threads=False)
        
        # CHECK 1: DATA EXISTENCE
        if df.empty or len(df) < 20: 
            return {"Ticker": ticker, "Reason": "No Data/Too New"}
        
        # Handle Multi-level columns in new yfinance versions
        try:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
        except:
            pass

        # CHECK 2: LIQUIDITY (Skip Penny/Dead Stocks)
        # Turnover > 50 Lakhs OR Price > 20
        avg_vol = df['Volume'].iloc[-5:].mean()
        price = float(df['Close'].iloc[-1])
        
        if (avg_vol * price) < 2500000: # Lowered to 25L for broader scan
            return {"Ticker": ticker, "Reason": "Illiquid"}

        # --- CALCULATIONS ---
        
        # A. VWAP (Yearly Approx)
        current_year = df.index[-1].year
        df_y = df[df.index.year == current_year].copy()
        if df_y.empty: 
             # Fallback to 200 SMA if Jan 1st just passed
             current_yvwap = df['Close'].rolling(50).mean().iloc[-1]
        else:
             yvwap = (df_y['Close'] * df_y['Volume']).cumsum() / df_y['Volume'].cumsum()
             current_yvwap = yvwap.iloc[-1]

        # B. TARA Formulas
        # Consistency: Green Candles in last 20
        green_candles = (df['Close'] > df['Open']).astype(int).tail(20).sum()
        consistency = (green_candles / 20) * 100
        
        # Efficiency: Net Move / Total Path
        lookback = 20
        net = abs(df['Close'].iloc[-1] - df['Close'].iloc[-lookback])
        path = abs(df['Close'].diff()).tail(lookback).sum()
        efficiency = net / path if path != 0 else 0
        
        # C. STATUS ASSIGNMENT
        status = "NEUTRAL"
        
        # 1. DIAMOND (Strict Smart Money)
        if price > current_yvwap and consistency >= 60 and efficiency >= 0.30:
            status = "💎 INSTITUTIONAL BUY"
            
        # 2. GOLD (Developing)
        elif price > current_yvwap and consistency >= 50:
            status = "📝 WATCHLIST"
            
        # 3. BLUE (Broad Trend - Good for testing)
        elif price > current_yvwap:
            status = "🔵 TRENDING (Test)"

        if status == "NEUTRAL": 
            return {"Ticker": ticker, "Reason": "Failed Criteria"}

        return {
            "Symbol": ticker.replace(".NS", ""),
            "Price": round(price, 2),
            "Status": status,
            "Consistency": f"{int(consistency)}%",
            "Efficiency": round(efficiency, 2),
            "YVWAP": round(current_yvwap, 2)
        }

    except Exception as e:
        return {"Ticker": ticker, "Reason": f"Error: {str(e)}"}

# --- 4. SCANNER ENGINE ---
def run_scan(ticker_list, max_threads):
    results = []
    skipped_log = [] # To store why stocks failed
    
    bar = st.progress(0)
    status_text = st.empty()
    
    total = len(ticker_list)
    
    # SAFETY: Use fewer threads to avoid Yahoo Ban
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        future_to_ticker = {executor.submit(analyze_stock, t): t for t in ticker_list}
        completed = 0
        
        for future in concurrent.futures.as_completed(future_to_ticker):
            res = future.result()
            
            # If valid result dictionary
            if "Status" in res:
                results.append(res)
            else:
                skipped_log.append(res) # Store failure reason
                
            completed += 1
            if completed % 10 == 0:
                bar.progress(completed / total)
                status_text.text(f"Scanning... {completed}/{total} stocks analyzed")
                
    bar.empty()
    status_text.empty()
    return results, skipped_log

# --- 5. USER INTERFACE ---
with st.sidebar:
    st.header("⚙️ Scanner Settings")
    universe = st.radio("Universe:", ["Nifty 50 (Fast)", "Nifty 500 (Medium)", "Full NSE Cash (Slow)"])
    
    # DEBUG MODE
    show_all_trends = st.checkbox("Show 'Trending' (Test Mode)", value=True, help="Shows all stocks above VWAP, even if they fail Consistency check. Useful to verify data is loading.")
    
    # SAFETY THROTTLE
    speed_mode = st.selectbox("Scan Speed:", ["Safe (8 Threads)", "Fast (20 Threads)", "Turbo (50 Threads - Risk Ban)"], index=0)
    
    if speed_mode == "Safe (8 Threads)": threads = 8
    elif speed_mode == "Fast (20 Threads)": threads = 20
    else: threads = 50

# LOAD DATA
all_tickers = get_nse_tickers()

if universe == "Nifty 50 (Fast)": targets = all_tickers[:50]
elif universe == "Nifty 500 (Medium)": targets = all_tickers[:500]
else: targets = all_tickers

st.info(f"Targeting {len(targets)} Stocks. Speed: {speed_mode}")

if st.button("🚀 START SCAN", type="primary"):
    with st.spinner("Analyzing Market Structure..."):
        valid_data, logs = run_scan(targets, threads)
    
    if valid_data:
        df = pd.DataFrame(valid_data)
        
        # 1. DIAMOND
        st.subheader("💎 DIAMOND SETUPS (High Precision)")
        diamonds = df[df['Status'] == "💎 INSTITUTIONAL BUY"]
        if not diamonds.empty:
            st.dataframe(diamonds.style.applymap(lambda x: 'color: #00FF00', subset=['Status']), use_container_width=True)
        else:
            st.info("No Diamond setups found. Market conditions may be weak.")

        # 2. WATCHLIST
        st.subheader("📝 WATCHLIST (Developing)")
        watch = df[df['Status'] == "📝 WATCHLIST"]
        if not watch.empty:
            st.dataframe(watch.style.applymap(lambda x: 'color: #FFD700', subset=['Status']), use_container_width=True)
        else:
            st.info("No Watchlist setups found.")
            
        # 3. TRENDING (Test Mode)
        if show_all_trends:
            st.subheader("🔵 ALL TRENDING (Debug View)")
            st.caption("Stocks simply trading above Yearly VWAP (No filter). If this is empty, Yahoo Finance is blocked.")
            trending = df[df['Status'] == "🔵 TRENDING (Test)"]
            if not trending.empty:
                st.dataframe(trending, use_container_width=True)
            else:
                st.error("No Data Found. Yahoo Finance might be blocking this IP.")
                
        # 4. DEBUG LOGS (Expandable)
        with st.expander("System Logs (Why were stocks skipped?)"):
            st.write(f"Total Scanned: {len(targets)}")
            st.write(f"Valid Results: {len(valid_data)}")
            if logs:
                st.dataframe(pd.DataFrame(logs).head(100)) # Show first 100 failures
                
    else:
        st.error("Scan returned 0 results. Check System Logs below.")
        with st.expander("System Logs"):
            st.dataframe(pd.DataFrame(logs))
