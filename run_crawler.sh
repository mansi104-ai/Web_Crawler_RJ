#!/bin/bash

# Real Estate Web Crawler - Linux/Mac Runner
# Make executable with: chmod +x run_crawler.sh

echo "==============================================="
echo "   Real Estate Web Crawler"
echo "   Starting crawl session..."
echo "==============================================="
echo

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_error() {
    echo -e "${RED}ERROR: $1${NC}"
}

print_success() {
    echo -e "${GREEN}$1${NC}"
}

print_warning() {
    echo -e "${YELLOW}WARNING: $1${NC}"
}

print_info() {
    echo -e "${BLUE}$1${NC}"
}

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    if ! command -v python &> /dev/null; then
        print_error "Python is not installed or not in PATH"
        echo "Please install Python 3.8+ first"
        echo "Ubuntu/Debian: sudo apt install python3 python3-pip"
        echo "macOS: brew install python3"
        echo "Or download from: https://python.org"
        exit 1
    else
        PYTHON_CMD="python"
    fi
else
    PYTHON_CMD="python3"
fi

# Check Python version
PYTHON_VERSION=$($PYTHON_CMD --version 2>&1)
print_info "Using: $PYTHON_VERSION"
echo

# Check if required files exist
if [ ! -f "crawler.py" ]; then
    print_error "crawler.py not found in current directory"
    echo "Please ensure all files are in the same folder"
    exit 1
fi

if [ ! -f "config.yaml" ]; then
    print_error "config.yaml not found in current directory"
    echo "Please ensure all files are in the same folder"
    exit 1
fi

if [ ! -f "requirements.txt" ]; then
    print_error "requirements.txt not found in current directory"
    echo "Please ensure all files are in the same folder"
    exit 1
fi

# Create output and logs directories if they don't exist
mkdir -p output
mkdir -p logs

# Check if pip is available
if ! command -v pip3 &> /dev/null; then
    if ! command -v pip &> /dev/null; then
        print_error "pip is not installed"
        echo "Please install pip first"
        echo "Ubuntu/Debian: sudo apt install python3-pip"
        echo "macOS: Usually comes with Python"
        exit 1
    else
        PIP_CMD="pip"
    fi
else
    PIP_CMD="pip3"
fi

# Check if virtual environment is recommended
if [ -z "$VIRTUAL_ENV" ]; then
    print_warning "Not running in a virtual environment"
    echo "Consider creating one with:"
    echo "  python3 -m venv venv"
    echo "  source venv/bin/activate"
    echo "  then run this script again"
    echo
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Setup virtual environment first, then run again"
        exit 1
    fi
fi

# Check if dependencies are installed
print_info "Checking dependencies..."
$PYTHON_CMD -c "import aiohttp, pandas, selenium, yaml, bs4" 2>/dev/null
if [ $? -ne 0 ]; then
    echo
    print_info "Installing required dependencies..."
    echo "This may take a few minutes on first run..."
    
    # Install with user flag if not in virtual env
    if [ -z "$VIRTUAL_ENV" ]; then
        $PIP_CMD install --user -r requirements.txt
    else
        $PIP_CMD install -r requirements.txt
    fi
    
    if [ $? -ne 0 ]; then
        echo
        print_error "Failed to install dependencies"
        echo "Please check your internet connection and try again"
        echo "You may need to run: sudo $PIP_CMD install -r requirements.txt"
        exit 1
    fi
    
    print_success "Dependencies installed successfully!"
    echo
fi

# Check for Chrome browser (for Selenium)
print_info "Checking for Chrome browser..."
if command -v google-chrome &> /dev/null; then
    CHROME_VERSION=$(google-chrome --version 2>/dev/null)
    print_success "Found: $CHROME_VERSION"
elif command -v chromium-browser &> /dev/null; then
    CHROME_VERSION=$(chromium-browser --version 2>/dev/null)
    print_success "Found: $CHROME_VERSION"
elif command -v "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" &> /dev/null; then
    CHROME_VERSION=$("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" --version 2>/dev/null)
    print_success "Found: $CHROME_VERSION"
else
    print_warning "Chrome browser not found"
    echo "Please install Google Chrome for best results:"
    echo "Ubuntu/Debian: sudo apt install google-chrome-stable"
    echo "macOS: brew install --cask google-chrome"
    echo "Or download from: https://www.google.com/chrome/"
    echo
    echo "Continuing anyway... (may fail)"
    sleep 3
fi

# Check for ChromeDriver or webdriver-manager
print_info "Checking ChromeDriver setup..."
if command -v chromedriver &> /dev/null; then
    CHROMEDRIVER_VERSION=$(chromedriver --version 2>/dev/null | head -n1)
    print_success "Found: $CHROMEDRIVER_VERSION"
else
    print_info "ChromeDriver not in PATH, using webdriver-manager (automatic)"
fi

echo

# Start the crawler
print_info "Starting Real Estate Crawler..."
echo
echo "==============================================="

# Run the crawler and capture exit code
$PYTHON_CMD crawler.py
CRAWLER_EXIT_CODE=$?

# Check exit code and provide feedback
echo
if [ $CRAWLER_EXIT_CODE -eq 0 ]; then
    echo "==============================================="
    print_success "CRAWLER COMPLETED SUCCESSFULLY!"
    echo "==============================================="
    echo
    print_info "Results saved to: output/"
    print_info "Logs saved to: logs/"
    echo
    echo "Check the Excel file in the output folder for your data"
    
    # List generated files
    if ls output/*.xlsx 1> /dev/null 2>&1; then
        echo
        print_info "Generated files:"
        ls -la output/*.xlsx | tail -5
    fi
    
elif [ $CRAWLER_EXIT_CODE -eq 130 ]; then
    echo "==============================================="
    print_warning "CRAWLER INTERRUPTED BY USER"
    echo "==============================================="
else
    echo "==============================================="
    print_error "CRAWLER FAILED"
    echo "==============================================="
    echo "Check the logs folder for detailed error information"
    
    # Show recent log entries if available
    if ls logs/*.log 1> /dev/null 2>&1; then
        echo
        print_info "Recent log entries:"
        tail -10 logs/*.log | tail -5
    fi
fi

echo
echo "Script completed. Check output and logs folders for results."

# On macOS, optionally open output folder
if [[ "$OSTYPE" == "darwin"* ]]; then
    read -p "Open output folder? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        open output/
    fi
fi