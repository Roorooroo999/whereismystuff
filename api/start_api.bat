@echo off
echo Starting Where's My Stuff API...
echo.
echo API will be available at: http://localhost:8000
echo API Docs: http://localhost:8000/docs
echo.

cd /d "%~dp0"
pip install -r requirements.txt
uvicorn server:app --reload --port 8000
