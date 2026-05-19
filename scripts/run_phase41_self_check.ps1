$ErrorActionPreference = "Stop"
$env:PYTHONIOENCODING = "utf-8"
python -X utf8 "$PSScriptRoot\run_phase41_self_check.py" @args
