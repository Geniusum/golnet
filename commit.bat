@echo off
setlocal enabledelayedexpansion

if "%~1"=="" (
    set "commit_message=Commit"
) else (
    set "commit_message=%~1"
)

git add .
git commit -m "!commit_message!"
git push origin
