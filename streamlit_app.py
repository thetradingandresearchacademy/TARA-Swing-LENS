import streamlit as st
import yfinance as yf
import pandas as pd
import requests
import io
import concurrent.futures
import os

# --- 1. CONFIGURATION & BRANDING ---
st.set_page_config(layout="wide", page_title="TARA Swing Scanner", page_icon="💎")

# Custom CSS for Premium "TARA Gold" UI
st.markdown("""
    <style>
    /* Dark Background & Gold Accents */
    .stApp { background-color: #050505; color: #e0e0e0; }
    
    /* Headers */
    h1, h2, h3 { color: #FFD700 !important; font-family: 'Helvetica Neue', sans-serif; }
    
    /* Metrics */
    div[data-testid="stMetricValue"] { color: #00FFFF; font-size: 28px !important; }
    div[data-testid="stMetricLabel"] { color: #aaaaaa; }
    
    /* Tables */
    .stDataFrame { border-radius: 10px; border: 1px solid #333; }
    
    /* Buttons */
    .stButton>button {
        background-color: #FFD700; color: black; font-weight: bold; border-radius: 8px;
        border: none; padding: 10px 24px; transition: all 0.3s;
    }
    .stButton>button:hover { background-color: #FFEA00; box-shadow: 0px 0px 10px #FFD700; color: black; }
    
    /* Footer */
    .footer {
        position: fixed; bottom: 0; left: 0; width: 100%;
        background-color: #111; color: #666; text-align: center;
        padding: 10px; font-size: 12px; border-top: 1px solid #333;
    }
    </style>
    """, unsafe_allow_html=True)

# --- 2. HEADER SECTION ---
col1, col2 = st.columns([1, 4])

with col1:
    # Display Logo if it exists
    if os.path.exists("TARA-LOGO.jpeg"):
        st.image("TARA-LOGO.jpeg", width=120)
    else:
        st.warning("Upload TARA-LOGO.jpeg to GitHub")

with col2:
    st.title("INTRADAY to SWING CARRY SCANNER")
    st.caption("Powered by TARA SWINGLAB FRAMEWORK | Institutional Swing Grade")

st.divider()

# --- 3. DATA ENGINE ---
@st.cache_data(ttl=3600)
def get_nse_tickers():
    try:
        url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        
        df = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
        df.columns = df.columns.str.strip() # Fix hidden spaces
        
        if 'SERIES' in df.columns:
            df = df[df['SERIES'] == 'EQ']
            
        tickers = [f"{symbol}.NS" for symbol in df['SYMBOL'].tolist()]
        return tickers
    except Exception as e:
        st.error(f"⚠️ Network Error fetching NSE List. Using Backup.")
        return ["RELIANCE.NS", "HDFCBANK.NS", "INFY.NS", "ITC.NS", "TCS.NS", "SBIN.NS"]

# --- 4. TARA LOGIC ---
def analyze_stock(ticker):
    try:
        # Download Data
        df = yf.download(ticker, period="6mo", interval="1d", progress=False, threads=False)
        
        if df.empty or len(df) < 50: return None
        
        # FIX: Handle MultiIndex columns
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # PRE-FILTERS (Price & Liquidity)
        avg_vol = df['Volume'].iloc[-5:].mean()
        price = float(df['Close'].iloc[-1])
        
        # RULE 1: Price must be >= 20 (Ignore Penny Stocks)
        if price < 20: return None
        
        # RULE 2: Turnover must be >= 10 Lakhs (Ignore Illiquid)
        if (avg_vol * price) < 1000000: return None

        # --- CALCULATIONS ---
        
        # A. TARA MAGNET (Yearly VWAP)
        current_year = df.index[-1].year
        df_y = df[df.index.year == current_year].copy()
        
        if df_y.empty: 
             tara_magnet = df['Close'].rolling(50).mean().iloc[-1]
        else:
             tara_magnet = (df_y['Close'] * df_y['Volume']).cumsum() / df_y['Volume'].cumsum()
             tara_magnet = tara_magnet.iloc[-1]

        # B. TARA FORMULAS
        # Consistency
        green_candles = (df['Close'] > df['Open']).astype(int).tail(20).sum()
        consistency = (green_candles / 20) * 100
        
        # Efficiency (Star Rating)
        lookback = 20
        net = abs(df['Close'].iloc[-1] - df['Close'].iloc[-lookback])
        path = abs(df['Close'].diff()).tail(lookback).sum()
        efficiency_score = net / path if path != 0 else 0
        
        # Convert Efficiency to Stars
        if efficiency_score > 0.30: stars = "⭐⭐⭐ (High)"
        elif efficiency_score > 0.15: stars = "⭐⭐ (Med)"
        else: stars = "⭐ (Low)"

        # C. STATUS ASSIGNMENT
        status = "NEUTRAL"
        
        # Logic: Price > Magnet AND Consistency > 50%
        if price > tara_magnet and consistency >= 60 and efficiency_score >= 0.25:
            status = "💎 APT ENTRY (Diamond)"
        elif price > tara_magnet and consistency >= 50:
            status = "📝 WATCHLIST (Gold)"
            
        if status == "NEUTRAL": return None

        return {
            "Symbol": ticker.replace(".NS", ""),
            "Price": f"{price:.2f}",  # FORCE 2 DECIMALS (String)
            "Status": status,
            "Consistency": f"{int(consistency)}%",
            "Efficiency": stars,
            "TARA Magnet": f"{tara_magnet:.2f}", # FORCE 2 DECIMALS (String)
            "Magnet Dist": f"{((price - tara_magnet)/tara_magnet)*100:.2f}%" # FORCE 2 DECIMALS
        }

    except:
        return None

# --- 5. SCANNER ENGINE ---
def run_scan(ticker_list, max_threads):
    results = []
    bar = st.progress(0)
    status_text = st.empty()
    total = len(ticker_list)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        future_to_ticker = {executor.submit(analyze_stock, t): t for t in ticker_list}
        completed = 0
        
        for future in concurrent.futures.as_completed(future_to_ticker):
            res = future.result()
            if res: results.append(res)
            completed += 1
            if completed % 5 == 0:
                bar.progress(completed / total)
                status_text.text(f"Scanning... {completed}/{total}")
                
    bar.empty()
    status_text.empty()
    return pd.DataFrame(results)

# --- 6. USER INTERFACE ---
with st.sidebar:
    st.header("⚙️ Scanner Settings")
    universe = st.radio("Select Universe:", ["Nifty 50 (Demo)", "Nifty 500", "Full NSE Cash"])
    speed = st.selectbox("Scan Speed:", ["Safe Mode (8 Threads)", "Turbo Mode (20 Threads)"])
    threads = 8 if "Safe" in speed else 20

# Fetch Data
all_tickers = get_nse_tickers()
if universe == "Nifty 50 (Demo)": targets = all_tickers[:50]
elif universe == "Nifty 500": targets = all_tickers[:500]
else: targets = all_tickers

st.info(f"Targeting {len(targets)} Stocks. Speed: {speed}")

if st.button("🚀 START TARA SCAN", type="primary"):
    with st.spinner("Analyzing Market Structure..."):
        df = run_scan(targets, threads)
    
    if not df.empty:
        # DISPLAY DIAMONDS
        diamonds = df[df['Status'].str.contains("Diamond")]
        if not diamonds.empty:
            st.subheader(f"💎 DIAMOND SETUPS ({len(diamonds)})")
            st.dataframe(
                diamonds.style.applymap(lambda x: 'color: #00FF00; font-weight: bold', subset=['Status']),
                use_container_width=True, hide_index=True
            )
        else:
            st.warning("No Diamond Setups found.")
            
        # DISPLAY WATCHLIST
        gold = df[df['Status'].str.contains("Gold")]
        if not gold.empty:
            st.subheader(f"📝 WATCHLIST ({len(gold)})")
            st.dataframe(
                gold.style.applymap(lambda x: 'color: #FFD700;', subset=['Status']),
                use_container_width=True, hide_index=True
            )
    else:
        st.error("No stocks met the criteria. Check if Yahoo Finance is blocking connection.")

# --- 7. FOOTER (SEBI DISCLAIMER) ---
st.markdown("""
    <div class="footer">
    <strong>DISCLAIMER (SEBI COMPLIANCE):</strong><br>
    The information provided by the 'TARA Swing Scanner' is for <strong>EDUCATIONAL PURPOSES ONLY</strong>. 
    It relies on historical data and algorithmic formulas (VWAP, Consistency) to identify patterns. 
    It does NOT constitute financial advice, investment recommendations, or a tip service. 
    Trading in the stock market involves significant risk. Please consult a SEBI registered investment advisor before making any financial decisions. 
    We are not responsible for any profits or losses incurred based on this data.
    </div>
    """, unsafe_allow_html=True)
