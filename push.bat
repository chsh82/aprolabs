@echo off
cd /d "%~dp0"
git add -A
if "%~1"=="" (
    git commit -m "update"
) else (
    git commit -m "%~1"
)
git push origin main
