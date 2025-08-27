@echo off
echo ===============================================
echo   Real Estate Web Crawler
echo   Starting crawl session...
echo ===============================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.8+ and add it to your PATH
    echo Download from: https://python.org
    pause
    exit /b 1
)

REM Display Python version
echo Python version:
python --version
echo.

REM Check if required files exist
if not exist "crawler.py" (
    echo ERROR: crawler.py not found in current directory
    echo Please ensure all files are in the same folder
    pause
    exit /b 1
)

if not exist "config.yaml" (
    echo ERROR: config.yaml not found in current directory
    echo Please ensure all files are in the same folder
    pause
    exit /b 1
)

if not exist "requirements.txt" (
    echo ERROR: requirements.txt not found in current directory
    echo Please ensure all files are in the same folder
    pause
    exit /b 1
)

REM Create output and logs directories if they don't exist
if not exist "output" mkdir output
if not exist "logs" mkdir logs

REM Check if dependencies are installed
echo Checking dependencies...
python -c "import aiohttp, pandas, selenium, yaml, bs4" 2>nul
if errorlevel 1 (
    echo.
    echo Installing required dependencies...
    echo This may take a few minutes on first run...
    pip install -r requirements.txt
    if errorlevel 1 (
        echo.
        echo ERROR: Failed to install dependencies
        echo Please check your internet connection and try again
        pause
        exit /b 1
    )
    echo Dependencies installed successfully!
    echo.
)

REM Check Chrome browser
echo Checking for Chrome browser...
where chrome.exe >nul 2>&1
if errorlevel 1 (
    where "C:\Program Files\Google\Chrome\Application\chrome.exe" >nul 2>&1
    if errorlevel 1 (
        where "C:\Program Files (x86)\Google\Chrome\Application\chrome.exe" >nul 2>&1
        if errorlevel 1 (
            echo WARNING: Chrome browser not found
            echo Please install Google Chrome for best results
            echo Download from: https://www.google.com/chrome/
            echo.
            echo Continuing anyway... (may fail)
            timeout /t 3 >nul
        )
    )
)

REM Start the crawler
echo Starting Real Estate Crawler...
echo.
echo ===============================================
python crawler.py

REM Check exit code
if errorlevel 1 (
    echo.
    echo ===============================================
    echo CRAWLER FAILED
    echo Check the logs folder for detailed error information
    echo ===============================================
) else (
    echo.
    echo ===============================================
    echo CRAWLER COMPLETED SUCCESSFULLY!
    echo ===============================================
    echo.
    echo Results saved to: output\
    echo Logs saved to: logs\
    echo.
    echo Check the Excel file in the output folder for your data
)

echo.
echo Press any key to exit...
pause >nul