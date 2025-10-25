# ============================================
# SIMPLE WRAPPER FUNCTIONS 
# ============================================

# Pull from GQ
function pull-gq {
    param(
        [string]$source,
        [string]$dest
    )
    & "C:\dev\home\tools\putty\current\PSCP.EXE" -pw "ex11@29@2gurgaon" "x@15458@7@10.53.12.45:$source" $dest
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✓ File copied from GQ" -ForegroundColor Green
    }
}

# Pull from BQ
function pull-bq {
    param(
        [string]$source,
        [string]$dest
    )
    & "C:\dev\home\tools\putty\current\PSCP.EXE" -pw "YOUR_BQ_PASSWORD" "x@15458@7@10.223.12.182:$source" $dest
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✓ File copied from BQ" -ForegroundColor Green
    }
}

# Push to GQ
function push-gq {
    param(
        [string]$source,
        [string]$dest
    )
    & "C:\dev\home\tools\putty\current\PSCP.EXE" -pw "ex11@29@2gurgaon" $source "x@15458@7@10.53.12.45:$dest"
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✓ File pushed to GQ" -ForegroundColor Green
    }
}

# Push to BQ
function push-bq {
    param(
        [string]$source,
        [string]$dest
    )
    & "C:\dev\home\tools\putty\current\PSCP.EXE" -pw "YOUR_BQ_PASSWORD" $source "x@15458@7@10.223.12.182:$dest"
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✓ File pushed to BQ" -ForegroundColor Green
    }
}

Write-Host "═══════════════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "                      FILE TRANSFER QUICK REFERENCE                        " -ForegroundColor Yellow
Write-Host "═══════════════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""
Write-Host "┌───────────────────────────────────────────────────────────────────────────┐" -ForegroundColor White
Write-Host "│ COMMAND     │ USAGE                                    │ SERVER          │" -ForegroundColor Yellow
Write-Host "├───────────────────────────────────────────────────────────────────────────┤" -ForegroundColor White
Write-Host "│ pull-gq     │ pull-gq <remote> <local>                 │ GQ (10.53...)   │" -ForegroundColor White
Write-Host "│ pull-bq     │ pull-bq <remote> <local>                 │ BQ (10.223...)  │" -ForegroundColor White
Write-Host "│ push-gq     │ push-gq <local> <remote>                 │ GQ (10.53...)   │" -ForegroundColor White
Write-Host "│ push-bq     │ push-bq <local> <remote>                 │ BQ (10.223...)  │" -ForegroundColor White
Write-Host "└───────────────────────────────────────────────────────────────────────────┘" -ForegroundColor White
Write-Host ""
Write-Host "EXAMPLES:" -ForegroundColor Green
Write-Host "  Pull from GQ:" -ForegroundColor Cyan
Write-Host "    pull-gq /home/user/data.csv C:\Downloads\" -ForegroundColor Gray
Write-Host ""
Write-Host "  Pull from BQ to LAN:" -ForegroundColor Cyan
Write-Host "    pull-bq /data/report.xlsx '\\\\server\\share\\'" -ForegroundColor Gray
Write-Host ""
Write-Host "  Push to GQ:" -ForegroundColor Cyan
Write-Host "    push-gq C:\local\file.txt /home/user/uploads/" -ForegroundColor Gray
Write-Host ""
Write-Host "  Push to BQ:" -ForegroundColor Cyan
Write-Host "    push-bq 'C:\data\report.csv' /backup/" -ForegroundColor Gray
Write-Host ""
Write-Host "NOTES:" -ForegroundColor Yellow
Write-Host "  • Use quotes for paths with spaces" -ForegroundColor Gray
Write-Host "  • Remote paths start with /" -ForegroundColor Gray
Write-Host "  • Local/LAN paths use C:\ or \\\\" -ForegroundColor Gray
Write-Host ""
