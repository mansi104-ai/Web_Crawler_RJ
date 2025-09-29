import streamlit as st
import subprocess
import os
import json
import pandas as pd
from datetime import datetime, timedelta
import time
import glob
import base64
from pathlib import Path
import sys
from typing import Dict, List, Optional

# Page configuration
st.set_page_config(
    page_title="Property Scraper Dashboard",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for Gemini-style interface
def load_css():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Google+Sans:wght@300;400;500;600;700&display=swap');
    .main { font-family: 'Google Sans', sans-serif; }
    /* (CSS rules — unchanged from your version above) */
    </style>
    """, unsafe_allow_html=True)

# Session state initialization
def init_session_state():
    if 'scraping_status' not in st.session_state:
        st.session_state.scraping_status = {}
    if 'history' not in st.session_state:
        st.session_state.history = load_history()
    if 'current_process' not in st.session_state:
        st.session_state.current_process = None

# Data management functions
def load_history() -> List[Dict]:
    try:
        if os.path.exists('scraping_history.json'):
            with open('scraping_history.json', 'r') as f:
                history = json.load(f)
                return sorted(history, key=lambda x: x.get('timestamp', ''), reverse=True)[:10]
    except Exception as e:
        st.error(f"Error loading history: {e}")
    return []

def save_history(entry: Dict):
    try:
        history = load_history()
        history.insert(0, entry)
        history = history[:10]
        with open('scraping_history.json', 'w') as f:
            json.dump(history, f, indent=2)
        st.session_state.history = history
    except Exception as e:
        st.error(f"Error saving history: {e}")

def run_scraper_async(site: str):
    try:
        start_time = datetime.now()
        cmd = [sys.executable, 'app.py', '--site', site]

        # Mock result for demo
        result = {
            'site': site,
            'timestamp': datetime.now().isoformat(),
            'duration': 45.5,
            'success': True,
            'output_file': f'output/{site}_mock_result.xlsx',
            'stdout': f'Successfully scraped {site}',
            'stderr': '',
            'return_code': 0
        }

        st.session_state.scraping_status[site] = {
            'status': 'completed',
            'start_time': start_time,
            'end_time': datetime.now(),
            'progress': 100,
            'result': result
        }
        save_history(result)
        return result

    except Exception as e:
        error_result = {
            'site': site,
            'timestamp': datetime.now().isoformat(),
            'duration': 0,
            'success': False,
            'output_file': None,
            'error': str(e)
        }
        st.session_state.scraping_status[site] = {
            'status': 'error',
            'start_time': datetime.now(),
            'progress': 0,
            'error': str(e)
        }
        save_history(error_result)
        return error_result

def check_scraper_requests():
    for site in ['99acres', 'nobroker', 'magicbricks']:
        request_key = f'run_request_{site}'
        if st.session_state.get(request_key, False):
            st.session_state[request_key] = False
            st.session_state.scraping_status[site] = {
                'status': 'running',
                'start_time': datetime.now(),
                'progress': 25
            }
            st.success(f"Started scraping {site}!")

def get_file_download_link(file_path: str, file_name: str) -> str:
    try:
        with open(file_path, "rb") as f:
            bytes_data = f.read()
        b64 = base64.b64encode(bytes_data).decode()
        return f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{file_name}">📥 Download {file_name}</a>'
    except Exception as e:
        return f"Error creating download link: {e}"

# UI Components
def render_header():
    st.markdown("""
    <div class="header-container">
        <div class="header-title">🏠 Property Scraper Dashboard</div>
        <div class="header-subtitle">Extract property data from 99acres, NoBroker, and MagicBricks</div>
    </div>
    """, unsafe_allow_html=True)

def render_website_cards():
    st.markdown("### 🎯 Select Website to Scrape")
    col1, col2, col3 = st.columns(3)
    websites = {
        '99acres': {
            'icon': '🏢',
            'description': 'Extract property listings from 99acres.com...',
            'features': ['Building names & prices', 'Image analysis', 'Nearby places extraction', 'Amenities detection']
        },
        'nobroker': {
            'icon': '🏠',
            'description': 'Scrape NoBroker listings...',
            'features': ['1 BHK/RK filtering', 'Owner verification', 'EMI calculations', 'Location mapping']
        },
        'magicbricks': {
            'icon': '🏗️',
            'description': 'Advanced MagicBricks scraper...',
            'features': ['Advanced image OCR', 'Property type detection', 'Furnishing details', 'Contact extraction']
        }
    }
    columns = [col1, col2, col3]
    for i, (site, info) in enumerate(websites.items()):
        with columns[i]:
            current_status = st.session_state.scraping_status.get(site, {})
            status = current_status.get('status', 'idle')
            status_class = f"status-{status}"
            status_text = status.title()
            if status == 'running':
                elapsed = datetime.now() - current_status.get('start_time', datetime.now())
                status_text = f"Running ({elapsed.seconds}s)"
            card_html = f"""
            <div class="website-card" id="card-{site}">
                <div class="card-header">
                    <div class="card-icon">{info['icon']}</div>
                    <div class="card-title">{site.title()}</div>
                </div>
                <div class="card-description">{info['description']}</div>
                <ul class="card-features">
                    {''.join([f'<li>{feature}</li>' for feature in info['features']])}
                </ul>
                <div class="status-badge {status_class}">{status_text}</div>
            </div>
            """
            st.markdown(card_html, unsafe_allow_html=True)
            if status != 'running':
                if st.button(f"🚀 Start {site.title()}", key=f"btn_{site}"):
                    with st.spinner(f'Starting {site} scraper...'):
                        st.session_state.scraping_status[site] = {
                            'status': 'running',
                            'start_time': datetime.now(),
                            'progress': 0
                        }
                        st.success(f"Started {site} scraper! Check status below.")
                        st.session_state[f'run_request_{site}'] = True
            else:
                if st.button(f"⏹️ Stop {site.title()}", key=f"stop_{site}"):
                    if st.session_state.current_process:
                        st.session_state.current_process.terminate()
                    st.session_state.scraping_status[site]['status'] = 'stopped'

def render_current_status():
    active_scrapers = {k: v for k, v in st.session_state.scraping_status.items() if v.get('status') == 'running'}
    if active_scrapers:
        st.markdown("### ⚡ Current Activities")
        for site, status in active_scrapers.items():
            elapsed = datetime.now() - status.get('start_time', datetime.now())
            progress_html = f"""
            <div class="progress-container">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <strong>🔄 {site.title()} Scraper</strong>
                    <span>{elapsed.seconds}s elapsed</span>
                </div>
                <div class="progress-bar"><div class="progress-fill" style="width: 45%;"></div></div>
                <small>Processing property listings...</small>
            </div>
            """
            st.markdown(progress_html, unsafe_allow_html=True)

def render_history():
    st.markdown("### 📊 Recent Activities")
    if not st.session_state.history:
        st.info("No scraping history available yet.")
        return
    for entry in st.session_state.history:
        timestamp = datetime.fromisoformat(entry['timestamp'])
        time_ago = datetime.now() - timestamp
        if time_ago.days > 0:
            time_str = f"{time_ago.days}d ago"
        elif time_ago.seconds > 3600:
            time_str = f"{time_ago.seconds // 3600}h ago"
        elif time_ago.seconds > 60:
            time_str = f"{time_ago.seconds // 60}m ago"
        else:
            time_str = "Just now"
        success_icon = "✅" if entry.get('success', False) else "❌"
        duration_str = f"{entry.get('duration', 0):.1f}s"
        history_html = f"""
        <div class="history-card">
            <div class="history-header">
                <div class="history-title">{success_icon} {entry['site'].title()} Scraper</div>
                <div class="history-time">{time_str}</div>
            </div>
            <div class="history-stats">
                <span>⏱️ Duration: {duration_str}</span>
                <span>📅 {timestamp.strftime('%Y-%m-%d %H:%M')}</span>
            </div>
        </div>
        """
        st.markdown(history_html, unsafe_allow_html=True)
        if entry.get('output_file') and os.path.exists(entry['output_file']):
            file_name = os.path.basename(entry['output_file'])
            st.markdown(get_file_download_link(entry['output_file'], file_name), unsafe_allow_html=True)
        if not entry.get('success', False) and entry.get('error'):
            st.error(f"Error: {entry['error']}")

def render_metrics():
    st.markdown("### 📈 Summary Metrics")
    col1, col2, col3, col4 = st.columns(4)
    total_runs = len(st.session_state.history)
    successful_runs = sum(1 for h in st.session_state.history if h.get('success', False))
    avg_duration = (sum(h.get('duration', 0) for h in st.session_state.history) / total_runs) if total_runs else 0
    last_run = st.session_state.history[0]['timestamp'] if st.session_state.history else "N/A"
    with col1:
        st.markdown(f"<div class='metric-card'><div class='metric-value'>{total_runs}</div><div class='metric-label'>Total Runs</div></div>", unsafe_allow_html=True)
    with col2:
        st.markdown(f"<div class='metric-card'><div class='metric-value'>{successful_runs}</div><div class='metric-label'>Successful Runs</div></div>", unsafe_allow_html=True)
    with col3:
        st.markdown(f"<div class='metric-card'><div class='metric-value'>{avg_duration:.1f}s</div><div class='metric-label'>Avg Duration</div></div>", unsafe_allow_html=True)
    with col4:
        st.markdown(f"<div class='metric-card'><div class='metric-value'>{last_run}</div><div class='metric-label'>Last Run</div></div>", unsafe_allow_html=True)

# Main
def main():
    load_css()
    init_session_state()
    render_header()
    render_website_cards()
    check_scraper_requests()
    render_current_status()
    render_history()
    render_metrics()

if __name__ == "__main__":
    main()
