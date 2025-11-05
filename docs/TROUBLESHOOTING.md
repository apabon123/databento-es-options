### Troubleshooting

#### "No API key found" or "Set DATABENTO_API_KEY in your environment."
The script looks for `.env` at the project root and reads `DATABENTO_API_KEY`.

Fix:
```powershell
Copy-Item .env.example .env
# Edit .env and set your key
# Or set for the current PowerShell session only:
$env:DATABENTO_API_KEY = "your_real_key_here"
```

#### ipykernel/python environment mismatch in Cursor/Jupyter
If notebooks or the REPL cannot import packages (e.g., `databento`), install the kernel from your env and select it.

Fix:
```powershell
# From the activated env
python -m pip install ipykernel
python -m ipykernel install --user --name databento-es-options
```

#### Cost exceeds budget / no prompt appears with --dry-run
- With `--dry-run`, the script only estimates and exits (no prompt).
- With `--max-budget USD`, the script aborts if the estimate is higher than your budget.

Fix:
```powershell
# Use the wrapper scripts with --yes to skip confirmation
python scripts/download/download_and_ingest_options.py --weeks 1 --yes
python scripts/download/download_and_ingest_futures.py --weeks 1 --yes
```

#### "Missing required columns" after download (validation failures)
Validation requires columns: `ts_event`, `symbol`, `bid_px`, `ask_px`, `bid_sz`, `ask_sz`.

Fix:
```powershell
# Ensure dependencies are current
pip install -r requirements.txt --upgrade

# Re-run with the wrapper scripts (validation is automatic)
python scripts/download/download_and_ingest_options.py --weeks 1 --force
```

#### Permission errors writing logs or data
Logs are written to `logs/downloader.log`. Data files go to `data/raw/`.

Fix:
```powershell
# Ensure directories exist and are writable
New-Item -ItemType Directory -Force -Path logs, data\raw | Out-Null
# Run PowerShell with appropriate permissions or use a writable project location
```
