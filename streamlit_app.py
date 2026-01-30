import streamlit as st
import yfinance as yf
import pandas as pd
import requests
import io
import concurrent.futures

# --- 1. CONFIGURATION ---
st.set_page_config(layout="wide", page_title="TARA Institutional Scanner", page_icon="💎")

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

# --- 2. DATA ENGINE (FIXED for NSE Spaces) ---
@st.cache_data(ttl=3600)
def get_nse_tickers():
    try:
        url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)
        
        # Read CSV
        df = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
        
        # --- THE FIX: Clean Column Names (Remove hidden spaces) ---
        df.columns = df.columns.str.strip()
        
        # Filter for Equity Series (EQ)
        if 'SERIES' in df.columns:
            df = df[df['SERIES'] == 'EQ']
            
        tickers = [f"{symbol}.NS" for symbol in df['SYMBOL'].tolist()]
        return tickers
        
    except Exception as e:
        # Graceful Fallback if NSE site is down
        st.warning(f"⚠️ NSE Fetch Error: {e}. Using Top 50 Stocks Backup.")
        return ["RELIANCE.NS", "HDFCBANK.NS", "INFY.NS", "ITC.NS", "TCS.NS", "SBIN.NS", "BHARTIARTL.NS"]

# --- 3. THE TARA LOGIC ---
def analyze_stock(ticker):
    try:
        # Fast Download (No Threads inside threads)
        df = yf.download(ticker, period="1y", interval="1d", progress=False, threads=False)
        
        if len(df) < 50: return None
        
        # Liquidity Filter (> 50L Turnover)
        avg_vol = df['Volume'].iloc[-5:].mean()
        price = df['Close'].iloc[-1]
        if (avg_vol * price) < 5000000: return None # Skip illiquid stocks

        # VWAP & Logic
        current_year = df.index[-1].year
        df_y = df[df.index.year == current_year].copy()
        if df_y.empty: return None
        
        yvwap = (df_y['Close'] * df_y['Volume']).cumsum() / df_y['Volume'].cumsum()
        current_yvwap = yvwap.iloc[-1]
        
        # TARA Formulas
        green_candles = (df['Close'] > df['Open']).astype(int).tail(20).sum()
        consistency = (green_candles / 20) * 100
        
        lookback = 20
        net = abs(df['Close'].iloc[-1] - df['Close'].iloc[-lookback])
        path = abs(df['Close'].diff()).tail(lookback).sum()
        efficiency = net / path if path != 0 else 0
        
        status = "NEUTRAL"
        if price > current_yvwap and consistency >= 60 and efficiency >= 0.30:
            status = "💎 INSTITUTIONAL BUY"
        elif price > current_yvwap and consistency >= 50:
            status = "📝 WATCHLIST"
            
        if status == "NEUTRAL": return None

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

# --- 4. SCANNER ENGINE ---
def run_scan(ticker_list):
    results = []
    bar = st.progress(0)
    status_text = st.empty()
    
    # Batch size for visual progress
    total = len(ticker_list)
    
    # ThreadPool for Speed (Scan 50 at a time)
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        future_to_ticker = {executor.submit(analyze_stock, t): t for t in ticker_list}
        completed = 0
        
        for future in concurrent.futures.as_completed(future_to_ticker):
            data = future.result()
            if data:
                results.append(data)
            completed += 1
            if completed % 10 == 0:
                bar.progress(completed / total)
                status_text.text(f"Scanning... {completed}/{total} stocks")
                
    bar.empty()
    status_text.empty()
    return pd.DataFrame(results)

# --- 5. UI ---
universe = st.sidebar.radio("Select Universe:", ["Nifty 50 (Demo)", "Nifty 500", "Full NSE Cash"])
all_tickers = get_nse_tickers()

if universe == "Nifty 50 (Demo)":
    targets = all_tickers[:50]
elif universe == "Nifty 500":
    targets = all_tickers[:500]
else:
    targets = all_tickers

st.info(f"Targeting {len(targets)} Stocks.")

if st.button("🚀 START SCAN", type="primary"):
    with st.spinner("Analyzing Market Structure..."):
        df_results = run_scan(targets)
    
    if not df_results.empty:
        st.subheader("💎 DIAMOND SETUPS")
        st.dataframe(df_results[df_results['Status'] == "💎 INSTITUTIONAL BUY"], use_container_width=True, hide_index=True)
        
        st.subheader("📝 WATCHLIST")
        st.dataframe(df_results[df_results['Status'] == "📝 WATCHLIST"], use_container_width=True, hide_index=True)
    else:
        st.warning("No stocks found.")
