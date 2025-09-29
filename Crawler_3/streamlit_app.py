import streamlit as st
import subprocess
import os
import json
import pandas as pd
from datetime import datetime
import glob
import sys
from pathlib import Path

st.set_page_config(page_title="Property Scraper", page_icon="🏠", layout="wide")

# Initialize session state
if 'history' not in st.session_state:
    st.session_state.history = []
if 'running' not in st.session_state:
    st.session_state.running = {}

# Load history from file
def load_history():
    try:
        if os.path.exists('scraping_history.json'):
            with open('scraping_history.json', 'r') as f:
                return json.load(f)[:10]
    except:
        pass
    return []

def save_to_history(entry):
    history = load_history()
    history.insert(0, entry)
    history = history[:10]
    with open('scraping_history.json', 'w') as f:
        json.dump(history, f, indent=2)

# Custom CSS
st.markdown("""
<style>
    .main {background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 2rem;}
    .stButton>button {
        width: 100%;
        background: white;
        color: #667eea;
        font-weight: bold;
        border-radius: 10px;
        padding: 1rem;
        border: none;
        font-size: 1.1rem;
    }
    .stButton>button:hover {
        background: #f0f0f0;
        transform: translateY(-2px);
        box-shadow: 0 5px 15px rgba(0,0,0,0.2);
    }
    .card {
        background: white;
        padding: 2rem;
        border-radius: 15px;
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
        margin: 1rem 0;
    }
    h1, h2, h3 {color: white !important;}
</style>
""", unsafe_allow_html=True)

# Header
st.title("🏠 Property Scraper Dashboard")
st.markdown("**Extract property data from 99acres, NoBroker, and MagicBricks**")
st.markdown("---")

# Main scraping section
st.markdown("## 🚀 Start Scraping")

col1, col2, col3 = st.columns(3)

websites = {
    '99acres': {'icon': '🏢', 'desc': 'Comprehensive property data'},
    'nobroker': {'icon': '🏠', 'desc': '1 BHK/RK focused'},
    'magicbricks': {'icon': '🏗️', 'desc': 'Advanced image processing'}
}

def run_scraper(site):
    """Actually run the scraper"""
    try:
        st.info(f"🔄 Starting {site} scraper...")
        start_time = datetime.now()
        
        # Show the command being run
        cmd = [sys.executable, 'app.py', '--site', site]
        st.code(' '.join(cmd), language='bash')
        
        # Create a placeholder for output
        output_placeholder = st.empty()
        output_text = []
        
        # Run the actual command
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Read output line by line
        for line in iter(process.stdout.readline, ''):
            if line:
                output_text.append(line.strip())
                # Show last 20 lines
                output_placeholder.text_area(
                    "Live Output:", 
                    '\n'.join(output_text[-20:]), 
                    height=300
                )
        
        process.wait()
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Find output files
        output_dirs = ['output', 'enhanced_output', '99acres_output', '.']
        output_files = []
        for dir_path in output_dirs:
            if os.path.exists(dir_path):
                pattern = os.path.join(dir_path, f"*{site}*.xlsx")
                output_files.extend(glob.glob(pattern))
        
        # Get the most recent file
        if output_files:
            output_files.sort(key=os.path.getmtime, reverse=True)
            output_file = output_files[0]
            success = True
            st.success(f"✅ Scraping completed in {duration:.1f}s!")
            st.success(f"📁 Output file: {output_file}")
            
            # Download button
            with open(output_file, 'rb') as f:
                st.download_button(
                    label=f"📥 Download {os.path.basename(output_file)}",
                    data=f.read(),
                    file_name=os.path.basename(output_file),
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        else:
            output_file = None
            success = False
            st.warning("⚠️ Scraping completed but no output file found")
        
        # Save to history
        entry = {
            'site': site,
            'timestamp': datetime.now().isoformat(),
            'duration': duration,
            'success': success,
            'output_file': output_file,
            'return_code': process.returncode
        }
        save_to_history(entry)
        
        return success
        
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        return False

# Website buttons
for i, (site, info) in enumerate(websites.items()):
    with [col1, col2, col3][i]:
        st.markdown(f"### {info['icon']} {site.title()}")
        st.caption(info['desc'])
        
        if st.button(f"Start {site}", key=f"btn_{site}"):
            with st.spinner(f"Running {site} scraper..."):
                run_scraper(site)

st.markdown("---")

# History section
st.markdown("## 📊 Scraping History")

history = load_history()

if history:
    for entry in history:
        with st.expander(
            f"{entry['site'].title()} - {entry['timestamp'][:16]} "
            f"({'✅' if entry.get('success') else '❌'})"
        ):
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Duration", f"{entry.get('duration', 0):.1f}s")
            with col2:
                st.metric("Status", "Success" if entry.get('success') else "Failed")
            with col3:
                st.metric("Return Code", entry.get('return_code', 'N/A'))
            
            if entry.get('output_file') and os.path.exists(entry['output_file']):
                st.write(f"**File:** {entry['output_file']}")
                with open(entry['output_file'], 'rb') as f:
                    st.download_button(
                        label=f"Download {os.path.basename(entry['output_file'])}",
                        data=f.read(),
                        file_name=os.path.basename(entry['output_file']),
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"dl_{entry['timestamp']}"
                    )
else:
    st.info("No scraping history yet. Run a scraper to get started!")

st.markdown("---")

# Data viewer
st.markdown("## 📋 View Results")

# Find all Excel files
excel_files = []
for pattern in ['*.xlsx', 'output/*.xlsx', 'enhanced_output/*.xlsx', '99acres_output/*.xlsx']:
    excel_files.extend(glob.glob(pattern))

if excel_files:
    excel_files.sort(key=os.path.getmtime, reverse=True)
    
    selected_file = st.selectbox(
        "Select file to view:",
        excel_files,
        format_func=lambda x: f"{os.path.basename(x)} ({datetime.fromtimestamp(os.path.getmtime(x)).strftime('%Y-%m-%d %H:%M')})"
    )
    
    if selected_file:
        try:
            excel_file = pd.ExcelFile(selected_file)
            sheet = st.selectbox("Select sheet:", excel_file.sheet_names)
            
            if sheet:
                df = pd.read_excel(selected_file, sheet_name=sheet)
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Rows", len(df))
                with col2:
                    st.metric("Columns", len(df.columns))
                with col3:
                    st.metric("File Size", f"{os.path.getsize(selected_file)/1024:.1f} KB")
                
                st.dataframe(df, height=400)
                
        except Exception as e:
            st.error(f"Error loading file: {e}")
else:
    st.info("No Excel files found. Run a scraper first!")

# Sidebar
with st.sidebar:
    st.markdown("## ⚙️ Settings")
    
    if st.button("🗑️ Clear History"):
        if os.path.exists('scraping_history.json'):
            os.remove('scraping_history.json')
        st.success("History cleared!")
        st.rerun()
    
    st.markdown("---")
    st.markdown("## ℹ️ System Info")
    st.info(f"**Python:** {sys.version.split()[0]}")
    st.info(f"**Directory:** {os.getcwd()}")
    
    st.markdown("---")
    st.markdown("## 📝 Logs")
    log_files = glob.glob("*.log") + glob.glob("logs/*.log")
    if log_files:
        selected_log = st.selectbox("Select log:", log_files)
        if st.button("View Log"):
            try:
                with open(selected_log, 'r') as f:
                    st.text_area("Log Content", f.read()[-2000:], height=300)
            except Exception as e:
                st.error(f"Error: {e}")