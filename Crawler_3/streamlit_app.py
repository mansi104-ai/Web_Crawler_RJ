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

# Detect if running on Streamlit Cloud
IS_CLOUD = os.path.exists('/mount/src') or 'STREAMLIT_SHARING' in os.environ

# Database file path
DATABASE_FILE = "properties_database.xlsx"

# Initialize session state
if 'history' not in st.session_state:
    st.session_state.history = []
if 'running' not in st.session_state:
    st.session_state.running = {}

def get_unique_key(row, site):
    """Generate a unique key for each property listing"""
    # Use different fields based on site
    if site == 'acres':
        return f"{row.get('Title', '')}_{row.get('Price', '')}_{row.get('Location', '')}".lower().strip()
    elif site == 'nobroker':
        return f"{row.get('Title', '')}_{row.get('Rent', '')}_{row.get('Address', '')}".lower().strip()
    elif site == 'magicbricks':
        return f"{row.get('Title', '')}_{row.get('Price', '')}_{row.get('Location', '')}".lower().strip()
    else:
        # Fallback: use first 3 columns
        cols = list(row.keys())[:3]
        return '_'.join([str(row.get(col, '')) for col in cols]).lower().strip()

def merge_to_database(new_file, site):
    """Merge new scraped data into the main database, avoiding duplicates"""
    try:
        # Read new data
        new_df = pd.read_excel(new_file)
        if new_df.empty:
            st.warning("No data in new file")
            return 0
        
        # Add metadata columns
        new_df['scrape_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        new_df['source_site'] = site
        
        # Generate unique keys
        new_df['unique_key'] = new_df.apply(lambda row: get_unique_key(row, site), axis=1)
        
        # Load existing database
        if os.path.exists(DATABASE_FILE):
            db_df = pd.read_excel(DATABASE_FILE)
            if 'unique_key' not in db_df.columns:
                db_df['unique_key'] = db_df.apply(lambda row: get_unique_key(row, row.get('source_site', site)), axis=1)
        else:
            db_df = pd.DataFrame()
        
        # Find new listings (not in database)
        if not db_df.empty:
            new_listings = new_df[~new_df['unique_key'].isin(db_df['unique_key'])]
        else:
            new_listings = new_df
        
        # Append to database
        if not new_listings.empty:
            updated_db = pd.concat([db_df, new_listings], ignore_index=True)
            updated_db.to_excel(DATABASE_FILE, index=False)
            st.success(f" Added {len(new_listings)} new listings to database")
            st.info(f"Total listings in database: {len(updated_db)}")
            return len(new_listings)
        else:
            st.info("No new listings found (all duplicates)")
            return 0
            
    except Exception as e:
        st.error(f"Error merging to database: {e}")
        return 0

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
st.title(" Property Scraper Dashboard")
st.markdown("**Extract property data from 99acres, NoBroker, and MagicBricks**")

# Database stats
if os.path.exists(DATABASE_FILE):
    db_df = pd.read_excel(DATABASE_FILE)
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(" Total Listings", len(db_df))
    with col2:
        if 'source_site' in db_df.columns:
            st.metric(" Sites Scraped", db_df['source_site'].nunique())
    with col3:
        if 'scrape_date' in db_df.columns:
            st.metric(" Last Updated", db_df['scrape_date'].max()[:10])

if IS_CLOUD:
    st.error("""
     **You're running on Streamlit Cloud - Scrapers won't work here!**
    
    **Why:** Selenium requires Chrome/ChromeDriver which aren't available on Streamlit Cloud.
    
    **Solutions:**
    1. **Run Locally:** Download the code and run `streamlit run streamlit_app.py` on your computer
    2. **Deploy to Railway/Render:** These platforms support Chrome automation
    3. **For now:** Upload existing Excel files below to view them
    """)
else:
    st.success("✅ Running locally - All features available!")

st.markdown("---")

# Main scraping section
st.markdown("##  Start Scraping")

col1, col2, col3 = st.columns(3)

websites = {
    'acres': {'icon': '🏢', 'desc': 'Comprehensive property data'},
    'nobroker': {'icon': '🏠', 'desc': '1 BHK/RK focused'},
    'magicbricks': {'icon': '🏗️', 'desc': 'Advanced image processing'}
}

def run_scraper(site):
    """Run the scraper - handles both local and cloud environments"""
    try:
        if IS_CLOUD:
            st.error("⚠️ **Running on Streamlit Cloud**")
            st.warning("""
            **Selenium-based scrapers cannot run on Streamlit Cloud** because:
            - Chrome/ChromeDriver are not available
            - Browser automation is not supported
            - No display server available
            
            **To use this scraper:**
            1. Run locally: `streamlit run streamlit_app.py`
            2. Or deploy to Railway/Render/AWS EC2 with Chrome installed
            
            **For now, you can:**
            - View sample data below
            - Upload and view existing Excel files
            """)
            return False
        
        st.info(f"Starting {site} scraper...")
        start_time = datetime.now()
        
        # Find app.py
        app_path = None
        for possible_path in ['app.py', '../app.py', './app.py']:
            if os.path.exists(possible_path):
                app_path = os.path.abspath(possible_path)
                break
        
        if not app_path:
            st.error(f"Cannot find app.py in: {os.getcwd()}")
            return False
        
        # Show command
        cmd = [sys.executable, app_path, '--site', site]
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
        
        # Show full output
        with st.expander("View Complete Output", expanded=False):
            st.text_area("Full Output:", '\n'.join(output_text), height=400)
        
        # Find output files
        output_dirs = ['output', 'enhanced_output', '99acres_output', '.']
        output_files = []
        for dir_path in output_dirs:
            if os.path.exists(dir_path):
                pattern = os.path.join(dir_path, f"*{site}*.xlsx")
                output_files.extend(glob.glob(pattern))
        
        # Get the most recent file
        new_listings_count = 0
        if output_files:
            output_files.sort(key=os.path.getmtime, reverse=True)
            output_file = output_files[0]
            success = True
            st.success(f" Scraping completed in {duration:.1f}s!")
            st.success(f" Output file: {output_file}")
            
            # Merge to database
            st.info("Merging to database...")
            new_listings_count = merge_to_database(output_file, site)
            
            # Show preview of new data
            if new_listings_count > 0:
                with st.expander(" Preview New Listings", expanded=True):
                    preview_df = pd.read_excel(output_file).head(5)
                    st.dataframe(preview_df)
            
        else:
            output_file = None
            success = False
            st.warning(" Scraping completed but no output file found")
        
        # Save to history
        entry = {
            'site': site,
            'timestamp': datetime.now().isoformat(),
            'duration': duration,
            'success': success,
            'new_listings': new_listings_count,
            'return_code': process.returncode,
            'output': '\n'.join(output_text)  # Save full output
        }
        save_to_history(entry)
        
        return success
        
    except Exception as e:
        st.error(f" Error: {str(e)}")
        import traceback
        st.code(traceback.format_exc())
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
st.markdown("##  Scraping History")

history = load_history()

if history:
    for entry in history:
        with st.expander(
            f"{entry['site'].title()} - {entry['timestamp'][:16]} "
            f"({True if entry.get('success') else False}) - "
            f"{entry.get('new_listings', 0)} new"
        ):
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Duration", f"{entry.get('duration', 0):.1f}s")
            with col2:
                st.metric("New Listings", entry.get('new_listings', 0))
            with col3:
                st.metric("Status", "Success" if entry.get('success') else "Failed")
            with col4:
                st.metric("Return Code", entry.get('return_code', 'N/A'))
            
            # Show output if available
            if 'output' in entry and entry['output']:
                with st.expander(" View Output"):
                    st.text_area("Output:", entry['output'], height=300, key=f"output_{entry['timestamp']}")
else:
    st.info("No scraping history yet. Run a scraper to get started!")

st.markdown("---")

# Data viewer - Show database
st.markdown("##  View Database")

if os.path.exists(DATABASE_FILE):
    try:
        df = pd.read_excel(DATABASE_FILE)
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Listings", len(df))
        with col2:
            if 'source_site' in df.columns:
                st.metric("Sites", df['source_site'].nunique())
        with col3:
            st.metric("File Size", f"{os.path.getsize(DATABASE_FILE)/1024:.1f} KB")
        with col4:
            if 'scrape_date' in df.columns:
                unique_dates = pd.to_datetime(df['scrape_date']).dt.date.nunique()
                st.metric("Scrape Sessions", unique_dates)
        
        # Filters
        col1, col2, col3 = st.columns(3)
        with col1:
            if 'source_site' in df.columns:
                sites = ['All'] + sorted(df['source_site'].unique().tolist())
                selected_site = st.selectbox("Filter by Site:", sites)
                if selected_site != 'All':
                    df = df[df['source_site'] == selected_site]
        
        with col2:
            if 'scrape_date' in df.columns:
                df['scrape_date'] = pd.to_datetime(df['scrape_date'])
                date_range = st.date_input("Filter by Date Range:", [])
                if len(date_range) == 2:
                    df = df[(df['scrape_date'].dt.date >= date_range[0]) & 
                           (df['scrape_date'].dt.date <= date_range[1])]
        
        with col3:
            search_term = st.text_input("🔍 Search in listings:")
            if search_term:
                mask = df.astype(str).apply(lambda x: x.str.contains(search_term, case=False, na=False)).any(axis=1)
                df = df[mask]
        
        st.info(f"Showing {len(df)} listings")
        
        # Column selector
        if not df.empty:
            all_cols = df.columns.tolist()
            default_cols = [col for col in all_cols if col not in ['unique_key']][:10]
            selected_cols = st.multiselect("Select columns to display:", all_cols, default=default_cols)
            
            if selected_cols:
                st.dataframe(df[selected_cols], height=400, use_container_width=True)
            else:
                st.dataframe(df, height=400, use_container_width=True)
        
        # Download button
        col1, col2 = st.columns(2)
        with col1:
            with open(DATABASE_FILE, 'rb') as f:
                st.download_button(
                    label=" Download Complete Database",
                    data=f.read(),
                    file_name=DATABASE_FILE,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        
        with col2:
            if not df.empty:
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label=" Download as CSV",
                    data=csv,
                    file_name="properties_database.csv",
                    mime="text/csv"
                )
        
        # Statistics
        with st.expander("📈 Database Statistics"):
            col1, col2 = st.columns(2)
            
            with col1:
                if 'source_site' in df.columns:
                    st.markdown("**Listings by Site:**")
                    site_counts = df['source_site'].value_counts()
                    for site, count in site_counts.items():
                        st.write(f"- {site}: {count}")
            
            with col2:
                if 'scrape_date' in df.columns:
                    st.markdown("**Listings by Date:**")
                    df['scrape_date'] = pd.to_datetime(df['scrape_date'])
                    date_counts = df['scrape_date'].dt.date.value_counts().sort_index(ascending=False).head(5)
                    for date, count in date_counts.items():
                        st.write(f"- {date}: {count}")
            
    except Exception as e:
        st.error(f"Error loading database: {e}")
        import traceback
        st.code(traceback.format_exc())
else:
    st.info("No database found. Run a scraper to create it!")
    st.markdown("""
    ###  How it works:
    1. Click on any scraper button above
    2. The scraper will collect property listings
    3. New listings are automatically added to the database
    4. Duplicates are automatically filtered out
    5. View and filter all listings here
    """)

# Sidebar
with st.sidebar:
    st.markdown("## Settings")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button(" Clear History"):
            if os.path.exists('scraping_history.json'):
                os.remove('scraping_history.json')
                st.success("History cleared!")
                st.rerun()
    
    with col2:
        if st.button(" Reset DB"):
            if os.path.exists(DATABASE_FILE):
                # Backup first
                backup_name = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                os.rename(DATABASE_FILE, backup_name)
                st.success(f"Database backed up as {backup_name}")
                st.rerun()
    
    st.markdown("---")
    st.markdown("## Database Info")
    if os.path.exists(DATABASE_FILE):
        try:
            db_df = pd.read_excel(DATABASE_FILE)
            st.info(f"**Total Records:** {len(db_df)}")
            st.info(f"**File Size:** {os.path.getsize(DATABASE_FILE)/1024:.1f} KB")
            st.info(f"**Columns:** {len(db_df.columns)}")
            if 'source_site' in db_df.columns:
                st.info(f"**Sites:** {', '.join(db_df['source_site'].unique())}")
        except:
            st.warning("Could not load database info")
    else:
        st.info("No database yet")
    
    st.markdown("---")
    st.markdown("## System Info")
    st.info(f"**Python:** {sys.version.split()[0]}")
    st.info(f"**Directory:** {os.getcwd()}")
    st.info(f"**Platform:** {'Cloud' if IS_CLOUD else 'Local'}")
    
    st.markdown("---")
    st.markdown("##  Logs")
    log_files = glob.glob("*.log") + glob.glob("logs/*.log")
    if log_files:
        selected_log = st.selectbox("Select log:", log_files)
        if st.button("View Log"):
            try:
                with open(selected_log, 'r') as f:
                    st.text_area("Log Content", f.read()[-2000:], height=300)
            except Exception as e:
                st.error(f"Error: {e}")
    
    st.markdown("---")
    st.markdown("##  Tips")
    st.markdown("""
    - Run scrapers regularly to keep database updated
    - Use filters to find specific properties
    - Export to CSV for analysis in Excel
    - Database automatically removes duplicates
    """)