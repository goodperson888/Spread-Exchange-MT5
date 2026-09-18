$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot
if ($env:OS -ne 'Windows_NT') { throw 'Build on Windows 64-bit.' }
# The build environment is isolated from the end user's terminal and Python.
py -3.11 -m venv .venv-build
if ($LASTEXITCODE -ne 0) { throw 'Install Python 3.11 x64 on this build computer.' }
$pythonExe = Join-Path $PSScriptRoot '.venv-build\Scripts\python.exe'
& $pythonExe -m pip install -r requirements-windows.txt
if ($LASTEXITCODE -ne 0) { throw 'Dependency installation failed.' }
& $pythonExe -m unittest discover -s tests -p 'test_*.py'
if ($LASTEXITCODE -ne 0) { throw 'Tests failed.' }
& $pythonExe -m PyInstaller --noconfirm --clean --onedir --name GoldPairLocal --collect-all MetaTrader5 --collect-all numpy --add-data 'index.html;.' --add-data 'app.js;.' --add-data 'trading-app.js;.' --add-data 'echarts.min.js;.' --add-data 'style.css;.' --add-data 'config.example.json;.' launcher.py
if ($LASTEXITCODE -ne 0) { throw 'Build failed.' }
& '.\dist\GoldPairLocal\GoldPairLocal.exe' --self-test
if ($LASTEXITCODE -ne 0) { throw 'Executable self-test failed.' }
Copy-Item README.md '.\dist\GoldPairLocal\README.md'
Compress-Archive -Path '.\dist\GoldPairLocal' -DestinationPath '.\dist\GoldPairLocal-Windows.zip' -Force
Write-Host 'Built dist\GoldPairLocal-Windows.zip. Test with a logged-in MT5 terminal before distributing.'
