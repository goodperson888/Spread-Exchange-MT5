@echo off
cd /d "%~dp0"
if exist "dist\GoldPairLocal\GoldPairLocal.exe" (
  "dist\GoldPairLocal\GoldPairLocal.exe"
) else (
  python launcher.py
)
pause
