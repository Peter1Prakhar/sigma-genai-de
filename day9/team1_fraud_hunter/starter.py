import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "shared"))

import streamlit as st
import duckdb
import pandas as pd
from bedrock_helper import call_nova_lite, call_nova_pro

# Database Setup
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "shared", "sigma_platform.duckdb")

st.set_page_config(page_title="Fraud Hunter", layout="wide")
st.title("Fraud Hunter")
st.caption("Sigma DataTech AI Ops Platform — Day 9")

# --- Initialize Session State ---
if 'data' not in st.session_state:
    st.session_state.data = None
if 'evaluations' not in st.session_state:
    st.session_state.evaluations = {}
if 'verdicts' not in st.session_state:
    st.session_state.verdicts = {}

# --- Data Loading ---
@st.cache_resource
def get_db_connection():
    return duckdb.connect(DB_PATH, read_only=True)

conn = get_db_connection()

def load_pending_transactions(limit=5):
    # Adjust table and column names based on your actual duckdb schema
    query = f"""
        SELECT transaction_id, customer_id, amount, merchant, location, timestamp, status 
        FROM transactions 
        WHERE status = 'PENDING' 
        LIMIT {limit}
    """
    try:
        return conn.execute(query).df()
    except Exception as e:
        st.error(f"Failed to load data. Please verify your table schema. Error: {e}")
        return pd.DataFrame()

# --- Load Data Button ---
if st.button("Fetch Next Batch of Transactions"):
    st.session_state.data = load_pending_transactions()
    st.session_state.evaluations = {}

if st.session_state.data is not None and not st.session_state.data.empty:
    df = st.session_state.data
    
    st.markdown("### Transaction Queue")
    st.dataframe(df, use_container_width=True)
    
    st.markdown("---")
    
    # Iterate through the transactions
    for index, row in df.iterrows():
        txn_id = row['transaction_id']
        txn_details = row.to_dict()
        
        st.subheader(f"Transaction ID: {txn_id}")
        st.write(f"**Customer:** {row['customer_id']} | **Amount:** ${row['amount']} | **Merchant:** {row['merchant']} | **Location:** {row['location']}")
        
        col1, col2, col3 = st.columns(3)
        
        # --- ROUND 1: AI Prosecutor (Nova Pro) ---
        with col1:
            st.markdown("#### Round 1: AI Prosecutor")
            if st.button(f"Prosecute Txn {txn_id}", key=f"pros_{txn_id}"):
                with st.spinner("Nova Pro analyzing..."):
                    prompt = f"""
                    You are a strict fraud detection AI. Review this transaction: {txn_details}.
                    Flag as CRITICAL, HIGH, MEDIUM, or LOW severity. Provide a 1-line reason.
                    Format: [SEVERITY] - [Reason]
                    """
                    prosecutor_flag = call_nova_pro(prompt)
                    
                    if txn_id not in st.session_state.evaluations:
                        st.session_state.evaluations[txn_id] = {}
                    st.session_state.evaluations[txn_id]['prosecutor'] = prosecutor_flag
            
            if txn_id in st.session_state.evaluations and 'prosecutor' in st.session_state.evaluations[txn_id]:
                st.info(st.session_state.evaluations[txn_id]['prosecutor'])
        
        # --- ROUND 2: AI Defense Lawyer (Nova Lite) ---
        with col2:
            st.markdown("#### Round 2: AI Defense")
            
            # Defense is only available if prosecutor has made a claim
            if txn_id in st.session_state.evaluations and 'prosecutor' in st.session_state.evaluations[txn_id]:
                if st.button(f"Defend Txn {txn_id}", key=f"def_{txn_id}"):
                    with st.spinner("Nova Lite analyzing..."):
                        prosecutor_claim = st.session_state.evaluations[txn_id]['prosecutor']
                        prompt = f"""
                        You are a defense AI protecting legitimate customers from being blocked. 
                        Transaction details: {txn_details}. 
                        The AI Prosecutor flagged it as: {prosecutor_claim}.
                        Provide a strong, logical 1-2 sentence argument for why this transaction might be perfectly legitimate.
                        """
                        defense_argument = call_nova_lite(prompt)
                        st.session_state.evaluations[txn_id]['defense'] = defense_argument
                
                if 'defense' in st.session_state.evaluations[txn_id]:
                    st.success(st.session_state.evaluations[txn_id]['defense'])
            else:
                st.warning("Run Prosecutor first.")

        # --- ROUND 3: Human Verdict ---
        with col3:
            st.markdown("#### Round 3: Your Verdict")
            if txn_id in st.session_state.evaluations and 'defense' in st.session_state.evaluations[txn_id]:
                decision = st.radio(
                    "Final Decision:",
                    ["FRAUD", "INVESTIGATE", "LEGITIMATE"],
                    key=f"verdict_{txn_id}",
                    index=1 # Default to investigate
                )
                if st.button("Submit Verdict", key=f"sub_{txn_id}"):
                    st.session_state.verdicts[txn_id] = decision
                    st.toast(f"Verdict saved for {txn_id}!")
                
                if txn_id in st.session_state.verdicts:
                    st.write(f"**Current Status:** {st.session_state.verdicts[txn_id]}")
            else:
                st.warning("Run both AIs before judging.")
        
        st.markdown("---")