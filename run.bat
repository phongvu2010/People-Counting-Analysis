@echo off

.venv\Scripts\uvicorn.exe app.main:app --host 0.0.0.0 --port 8000

pause