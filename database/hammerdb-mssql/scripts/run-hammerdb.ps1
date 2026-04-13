# Reference script for the Windows guest image (not executed by kube-burner from the repo).
# Install on the VM (e.g. C:\tools\hammerdb-4.12\run-hammerdb.ps1) and schedule at startup.
# It should run HammerDB against local MSSQL and write JSON results to:
#   C:\tools\hammerdb-4.12\results\hammerdb-results.json
# so that check_hammerdb_mssql can poll for that file over SSH.

Write-Host "run-hammerdb.ps1 is a placeholder in cnv-scenarios — replace with your HammerDB automation."
