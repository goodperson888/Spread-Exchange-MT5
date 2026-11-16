@echo off
setlocal
cd /d "%~dp0"
set "GOLD_PAIR_DATA_DIR=%~dp0data"
if exist "%LOCALAPPDATA%\GoldPairLocal\trading.sqlite3" if not exist "%GOLD_PAIR_DATA_DIR%\trading.sqlite3" (
  echo Existing trading data was found in:
  echo "%LOCALAPPDATA%\GoldPairLocal"
  echo.
  echo Close ALL GoldPairLocal windows and processes first.
  echo Copy the ENTIRE contents of that folder into:
  echo "%GOLD_PAIR_DATA_DIR%"
  echo Keep the original folder as a backup. Do not delete database or WAL files.
  echo Then run this script again. See README for Chinese instructions.
  pause
  exit /b 1
)
echo Data directory: "%GOLD_PAIR_DATA_DIR%"
if exist "GoldPairLocal.exe" (
  "GoldPairLocal.exe"
) else (
  python launcher.py
)
pause
