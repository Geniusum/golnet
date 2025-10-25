@echo off
setlocal enabledelayedexpansion

REM
set THREADS=4

powershell -Command ^
  "$files = Get-ChildItem -Filter '*.mp3';" ^
  "$jobs = @();" ^
  "foreach ($f in $files) {" ^
  "  while ((Get-Job -State Running).Count -ge %THREADS%) { Start-Sleep -Milliseconds 500 }" ^
  "  $jobs += Start-Job -ScriptBlock { ffmpeg -y -i $using:f.FullName -b:a 128k ('tmp_' + $using:f.Name); Move-Item -Force ('tmp_' + $using:f.Name) $using:f.FullName }" ^
  "}" ^
  "Wait-Job * | Out-Null; Receive-Job * | Out-Default; Remove-Job *"
