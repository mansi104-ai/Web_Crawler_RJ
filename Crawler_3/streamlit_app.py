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
    page_title="Nomad ",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for Gemini-style interface
def load_css():
    st.markdown("""
    <style>
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Google+Sans:wght@300;400;500;600;700&display=swap');
    
    /* Global Styles */
    .main {
        font-family: 'Google Sans', sans-serif;
    }
    
    /* Header Styling */
    .header-container {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 15px;
        margin-bottom: 2rem;
        color: white;
        text-align: center;
    }
    
    .header-title {
        font-size: 2.5rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
    }
    
    .header-subtitle {
        font-size: 1.2rem;
        opacity: 0.9;
        font-weight: 400;
    }
    
    /* Website Card Styling */
    .website-card {
        background: white;
        border-radius: 20px;
        padding: 2rem;
        margin: 1rem;
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
        transition: all 0.3s ease;
        border: 1px solid rgba(255,255,255,0.2);
        cursor: pointer;
        position: relative;
        overflow: hidden;
    }
    
    .website-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 12px 48px rgba(0,0,0,0.15);
    }
    
    .website-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 4px;
        background: linear-gradient(90deg, #ff6b6b, #4ecdc4, #45b7d1);
    }
    
    .card-header {
        display: flex;
        align-items: center;
        margin-bottom: 1rem;
    }
    
    .card-icon {
        font-size: 2.5rem;
        margin-right: 1rem;
    }
    
    .card-title {
        font-size: 1.8rem;
        font-weight: 600;
        color: #2c3e50;
        margin: 0;
    }
    
    .card-description {
        color: #7f8c8d;
        margin-bottom: 1.5rem;
        font-size: 1rem;
        line-height: 1.5;
    }
    
    .card-features {
        list-style: none;
        padding: 0;
        margin: 0;
    }
    
    .card-features li {
        padding: 0.3rem 0;
        color: #5a6c7d;
        font-size: 0.9rem;
    }
    
    .card-features li::before {
        content: "✓";
        color: #27ae60;
        font-weight: bold;
        margin-right: 0.5rem;
    }
    
    /* Status Badge */
    .status-badge {
        display: inline-block;
        padding: 0.3rem 0.8rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 500;
        margin-top: 1rem;
    }
    
    .status-running {
        background: #fff3cd;
        color: #856404;
        border: 1px solid #ffeaa7;
    }
    
    .status-completed {
        background: #d4edda;
        color: #155724;
        border: 1px solid #c3e6cb;
    }
    
    .status-error {
        background: #f8d7da;
        color: #721c24;
        border: 1px solid #f5c6cb;
    }
    
    .status-idle {
        background: #e2e3e5;
        color: #383d41;
        border: 1px solid #d6d8db;
    }
    
    /* Progress Bar */
    .progress-container {
        background: #f8f9fa;
        border-radius: 10px;
        padding: 1rem;
        margin: 1rem 0;
        border-left: 4px solid #007bff;
    }
    
    .progress-bar {
        background: #e9ecef;
        border-radius: 10px;
        height: 8px;
        overflow: hidden;
        margin: 0.5rem 0;
    }
    
    .progress-fill {
        background: linear-gradient(90deg, #007bff, #0056b3);
        height: 100%;
        transition: width 0.3s ease;
    }
    
    /* History Section */
    .history-card {
        background: #f8f9fa;
        border-radius: 10px;
        padding: 1rem;
        margin: 0.5rem 0;
        border-left: 4px solid #28a745;
    }
    
    .history-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 0.5rem;
    }
    
    .history-title {
        font-weight: 600;
        color: #2c3e50;
    }
    
    .history-time {
        color: #6c757d;
        font-size: 0.9rem;
    }
    
    .history-stats {
        display: flex;
        gap: 1rem;
        font-size: 0.85rem;
        color: #495057;
    }
    
    /* Metrics Cards */
    .metric-card {
        background: white;
        border-radius: 10px;
        padding: 1.5rem;
        text-align: center;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
    }
    
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        color: #2c3e50;
    }
    
    .metric-label {
        color: #7f8c8d;
        font-size: 0.9rem;
        margin-top: 0.5rem;
    }
    
    /* Buttons */
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 25px;
        padding: 0.7rem 2rem;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 5px 15px rgba(0,0,0,0.2);
    }
    
    /* Sidebar Styling */
    .css-1d391kg {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
    }
    
    /* Hide Streamlit Styling */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display:none;}
    
    /* Data Table Styling */
    .dataframe {
        border-radius: 10px;
        overflow: hidden;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
    }
    
    /* Alert Styling */
    .alert {
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
    }
    
    .alert-info {
        background: #d1ecf1;
        border-left: 4px solid #17a2b8;
        color: #0c5460;
    }
    
    .alert-success {
        background: #d4edda;
        border-left: 4px solid #28a745;
        color: #155724;
    }
    
    .alert-warning {
        background: #fff3cd;
        border-left: 4px solid #ffc107;
        color: #856404;
    }
    
    .alert-error {
        background: #f8d7da;
        border-left: 4px solid #dc3545;
        color: #721c24;
    }
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
    """Load scraping history from JSON file"""
    try:
        if os.path.exists('scraping_history.json'):
            with open('scraping_history.json', 'r') as f:
                history = json.load(f)
                # Sort by timestamp, most recent first
                return sorted(history, key=lambda x: x.get('timestamp', ''), reverse=True)[:10]
    except Exception as e:
        st.error(f"Error loading history: {e}")
    return []

def save_history(entry: Dict):
    """Save new entry to history"""
    try:
        history = load_history()
        history.insert(0, entry)
        history = history[:10]  # Keep only last 10 entries
        
        with open('scraping_history.json', 'w') as f:
            json.dump(history, f, indent=2)
        
        st.session_state.history = history
    except Exception as e:
        st.error(f"Error saving history: {e}")

async def run_scraper_async(website: str):
    start_time = datetime.now()
    st.session_state.scraper_status[website] = {
        "running": True,
        "progress": 0,
        "start_time": start_time
    }

    try:
        # Map website names to their CLI arguments
        site_args = {
            "99acres": "acres",
            "NoBroker": "nobroker",
            "MagicBricks": "magicbricks"
        }

        # Launch the scraper process
        process = await asyncio.create_subprocess_exec(
            "python", "app.py", "--site", site_args[website],
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        stdout, stderr = await process.communicate()
        duration = (datetime.now() - start_time).total_seconds()

        success = (process.returncode == 0)
        results = {
            "stdout": stdout.decode().strip(),
            "stderr": stderr.decode().strip()
        }

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        success = False
        results = {"error": str(e)}

    # Store history
    st.session_state.history.insert(0, {
        "website": website,
        "status": "completed" if success else "failed",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "duration": duration,
        "results": results,
        "success": success
    })

    # Mark as finished
    st.session_state.scraper_status[website]["running"] = False

def check_scraper_requests():
    """Check for pending scraper requests and handle them"""
    for site in ['99acres', 'nobroker', 'magicbricks']:
        request_key = f'run_request_{site}'
        if st.session_state.get(request_key, False):
            # Clear the request flag
            st.session_state[request_key] = False
            
            # Update status to show it's starting
            st.session_state.scraping_status[site] = {
                'status': 'running',
                'start_time': datetime.now(),
                'progress': 25
            }
            
            # In a real implementation, you would start the subprocess here
            # For demo purposes, we'll just show it's running
            st.success(f"Started scraping {site}!")

def get_file_download_link(file_path: str, file_name: str) -> str:
    """Generate download link for file"""
    try:
        with open(file_path, "rb") as f:
            bytes_data = f.read()
        b64 = base64.b64encode(bytes_data).decode()
        return f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{file_name}">📥 Download {file_name}</a>'
    except Exception as e:
        return f"Error creating download link: {e}"

# Main interface components
def render_header():
    """Render the main header"""
    st.markdown("""
    <div class="header-container">
        <div class="header-title">Nomad</div>
        <div class="header-subtitle">Extract property data from 99acres, NoBroker, and MagicBricks</div>
    </div>
    """, unsafe_allow_html=True)

def render_website_cards():
    """Render website selection cards"""
    st.markdown("### 🎯 Select Website to Scrape")
    
    col1, col2, col3 = st.columns(3)
    
    websites = {
        '99acres': {
            'icon': '🏢',
            'description': 'Extract property listings from 99acres.com with comprehensive data including images, amenities, and nearby places.',
            'features': ['Building names & prices', 'Image analysis', 'Nearby places extraction', 'Amenities detection']
        },
        'nobroker': {
            'icon': '🏠',
            'description': 'Scrape NoBroker listings with enhanced filtering for 1 BHK and 1 RK properties.',
            'features': ['1 BHK/RK filtering', 'Owner verification', 'EMI calculations', 'Location mapping']
        },
        'magicbricks': {
            'icon': '🏗️',
            'description': 'Advanced MagicBricks scraper with image processing and OCR capabilities.',
            'features': ['Advanced image OCR', 'Property type detection', 'Furnishing details', 'Contact extraction']
        }
    }
    
    columns = [col1, col2, col3]
    
    for i, (site, info) in enumerate(websites.items()):
        with columns[i]:
            # Get current status
            current_status = st.session_state.scraping_status.get(site, {})
            status = current_status.get('status', 'idle')
            
            # Status badge
            status_class = f"status-{status}"
            status_text = status.title()
            
            if status == 'running':
                progress = current_status.get('progress', 0)
                elapsed = datetime.now() - current_status.get('start_time', datetime.now())
                status_text = f"Running ({elapsed.seconds}s)"
            
            # Card HTML
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
            
            # Action button
            if status != 'running':
                if st.button(f"🚀 Start {site.title()}", key=f"btn_{site}"):
                    with st.spinner(f'Starting {site} scraper...'):
                        # Set status to running immediately
                        st.session_state.scraping_status[site] = {
                            'status': 'running',
                            'start_time': datetime.now(),
                            'progress': 0
                        }
                        st.success(f"Started {site} scraper! Check status below.")
                        # Use session state to track request instead of threading
                        st.session_state[f'run_request_{site}'] = True
                        # st.rerun()
            else:
                if st.button(f"⏹️ Stop {site.title()}", key=f"stop_{site}", width='stretch'):
                    if st.session_state.current_process:
                        st.session_state.current_process.terminate()
                    st.session_state.scraping_status[site]['status'] = 'stopped'
                   
                    

def render_current_status():
    """Render current scraping status"""
    active_scrapers = {k: v for k, v in st.session_state.scraping_status.items() 
                      if v.get('status') == 'running'}
    
    if active_scrapers:
        st.markdown("### ⚡ Current Activities")
        
        for site, status in active_scrapers.items():
            start_time = status.get('start_time', datetime.now())
            elapsed = datetime.now() - start_time
            
            progress_html = f"""
            <div class="progress-container">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <strong>🔄 {site.title()} Scraper</strong>
                    <span>{elapsed.seconds}s elapsed</span>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill" style="width: 45%;"></div>
                </div>
                <small>Processing property listings...</small>
            </div>
            """
            
            st.markdown(progress_html, unsafe_allow_html=True)

def render_history():
    """Render scraping history"""
    st.markdown("### 📊 Recent Activities")
    
    if not st.session_state.history:
        st.info("No scraping history available yet.")
        return
    
    for i, entry in enumerate(st.session_state.history):
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
        
        # Show download link if file exists
        if entry.get('output_file') and os.path.exists(entry['output_file']):
            file_name = os.path.basename(entry['output_file'])
            download_link = get_file_download_link(entry['output_file'], file_name)
            st.markdown(download_link, unsafe_allow_html=True)
        
        # Show error if failed
        if not entry.get('success', False) and entry.get('error'):
            st.error(f"Error: {entry['error']}")

def render_metrics():
    """Render summary metrics"""
    st.markdown("### 📈 Summary Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Calculate metrics
    total_runs = len(st.session_state.history)
    successful_runs = sum(1 for h in st.session_state.history if h.get('success', False))
    avg_duration = sum(h.get('duration', 0) for h in st.session_state.history) / max(1, total_runs)
    active_scrapers = sum(1 for s in st.session_state.scraping_status.values() 
                         if s.get('status') == 'running')
    
    metrics = [
        ("Total Runs", total_runs, "🔢"),
        ("Successful", successful_runs, "✅"),
        ("Avg Duration", f"{avg_duration:.1f}s", "⏱️"),
        ("Active Now", active_scrapers, "🔄")
    ]
    
    for i, (label, value, icon) in enumerate(metrics):
        with [col1, col2, col3, col4][i]:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value">{icon} {value}</div>
                <div class="metric-label">{label}</div>
            </div>
            """, unsafe_allow_html=True)

def render_data_viewer():
    """Render data viewer for recent results"""
    st.markdown("### 📋 Data Viewer")
    
    # Find recent Excel files
    excel_files = []
    for pattern in ['*.xlsx', 'output/*.xlsx', 'enhanced_output/*.xlsx']:
        excel_files.extend(glob.glob(pattern))
    
    if not excel_files:
        st.info("No Excel files found. Run a scraper to generate data.")
        return
    
    # Sort by modification time
    excel_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    
    # File selector
    selected_file = st.selectbox(
        "Select file to view:",
        excel_files,
        format_func=lambda x: f"{os.path.basename(x)} ({datetime.fromtimestamp(os.path.getmtime(x)).strftime('%Y-%m-%d %H:%M')})"
    )
    
    if selected_file:
        try:
            # Load Excel file
            excel_data = pd.ExcelFile(selected_file)
            
            # Sheet selector
            sheet_name = st.selectbox("Select sheet:", excel_data.sheet_names)
            
            if sheet_name:
                df = pd.read_excel(selected_file, sheet_name=sheet_name)
                
                # Display basic info
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Rows", len(df))
                with col2:
                    st.metric("Columns", len(df.columns))
                with col3:
                    st.metric("Size", f"{os.path.getsize(selected_file) / 1024:.1f} KB")
                
                # Display data
                st.dataframe(df, width='stretch', height=400)
                
                # Download button
                with open(selected_file, "rb") as file:
                    st.download_button(
                        label="📥 Download File",
                        data=file.read(),
                        file_name=os.path.basename(selected_file),
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                    
        except Exception as e:
            st.error(f"Error loading file: {e}")

def render_settings():
    """Render settings panel"""
    with st.sidebar:
        st.markdown("### ⚙️ Settings")
        
        # Auto-refresh toggle
        auto_refresh = st.checkbox("Auto-refresh", value=True)
        if auto_refresh:
            refresh_interval = st.slider("Refresh interval (seconds)", 1, 10, 3)
            time.sleep(refresh_interval)
            # st.rerun()
        
        # Clear history button
        if st.button("🗑️ Clear History"):
            st.session_state.history = []
            if os.path.exists('scraping_history.json'):
                os.remove('scraping_history.json')
            st.success("History cleared!")
            # st.rerun()
        
        # System info
        # st.markdown("### ℹ️ System Info")
        # st.info(f"Python: {sys.version.split()[0]}")
        # st.info(f"Working Directory: {os.getcwd()}")
        
        # Recent logs
        st.markdown("### 📝 Recent Logs")
        log_files = glob.glob("*.log") + glob.glob("logs/*.log")
        if log_files:
            log_file = st.selectbox("Select log:", log_files)
            if st.button("View Log"):
                try:
                    with open(log_file, 'r') as f:
                        log_content = f.read()
                        st.text_area("Log Content", log_content[-2000:], height=200)  # Last 2000 chars
                except Exception as e:
                    st.error(f"Error reading log: {e}")

# Main application
def main():
    # Initialize
    load_css()
    init_session_state()
    
    # Check for pending scraper requests
    check_scraper_requests()
    
    # Render components
    render_header()
    render_website_cards()
    render_current_status()
    
    # Create tabs for different sections
    tab1, tab2, tab3 = st.tabs(["📊 Dashboard", "📋 Data Viewer", "📈 Analytics"])
    
    with tab1:
        col1, col2 = st.columns([2, 1])
        with col1:
            render_history()
        with col2:
            render_metrics()
    
    with tab2:
        render_data_viewer()
    
    with tab3:
        st.markdown("### 📊 Scraping Analytics")
        if st.session_state.history:
            # Create charts
            df_history = pd.DataFrame(st.session_state.history)
            
            # Success rate over time
            df_history['date'] = pd.to_datetime(df_history['timestamp']).dt.date
            daily_stats = df_history.groupby('date').agg({
                'success': 'mean',
                'duration': 'mean'
            }).reset_index()
            
            if len(daily_stats) > 1:
                st.line_chart(daily_stats.set_index('date')['success'])
                st.bar_chart(daily_stats.set_index('date')['duration'])
            else:
                st.info("Need more data for analytics charts.")
        else:
            st.info("No data available for analytics yet.")
    
    # Render settings in sidebar
    render_settings()
    
    # Footer
    st.markdown("---")
    st.markdown("🚀 **Nomad for Nobroker, Magicbricks and 99acres** | Built with Streamlit")

if __name__ == "__main__":
    main()