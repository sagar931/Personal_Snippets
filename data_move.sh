# ============================================
# FILE TRANSFER FUNCTION (PULL ONLY)
# ============================================

function Pull-File {
    # Configuration
    $PSCP = "C:\dev\home\tools\putty\current\PSCP.EXE"
    $SERVERS = @{
        "GQ" = @{
            Host = "x@15458@7@10.53.12.45"
            Password = "ex11@29@2gurgaon"
        }
        "BQ" = @{
            Host = "x@15458@7@10.223.12.182"
            Password = "YOUR_BQ_PASSWORD_HERE"
        }
    }
    
    # Check if PSCP exists
    if (-not (Test-Path $PSCP)) {
        Write-Host "✗ Error: PSCP not found at $PSCP" -ForegroundColor Red
        return
    }
    
    # Display header
    Write-Host ""
    Write-Host "╔════════════════════════════════════════════╗" -ForegroundColor Cyan
    Write-Host "║        📥 PULL FILE FROM SERVER 📥         ║" -ForegroundColor Cyan
    Write-Host "╚════════════════════════════════════════════╝" -ForegroundColor Cyan
    Write-Host ""
    
    # Step 1: Select Server
    Write-Host "Select Server:" -ForegroundColor Yellow
    Write-Host "  GQ - Production Server (10.53.12.45)" -ForegroundColor White
    Write-Host "  BQ - Backup Server (10.223.12.182)" -ForegroundColor White
    Write-Host ""
    $serverChoice = Read-Host "Enter server (GQ or BQ)"
    
    # Validate server choice
    if ($serverChoice -ne "GQ" -and $serverChoice -ne "gq" -and $serverChoice -ne "BQ" -and $serverChoice -ne "bq") {
        Write-Host "✗ Invalid server choice!" -ForegroundColor Red
        return
    }
    
    $serverChoice = $serverChoice.ToUpper()
    $selectedServer = $SERVERS[$serverChoice]
    
    Write-Host "→ Selected Server: " -ForegroundColor Green -NoNewline
    Write-Host "$serverChoice ($($selectedServer.Host))" -ForegroundColor White
    Write-Host ""
    
    # Step 2: Get Source Path
    Write-Host "Enter Source Path on Server (with filename):" -ForegroundColor Yellow
    Write-Host "  Example: /home/user/data/report.csv" -ForegroundColor DarkGray
    Write-Host "  Example: /var/log/application.log" -ForegroundColor DarkGray
    Write-Host "  Example: /data/analytics/output.xlsx" -ForegroundColor DarkGray
    Write-Host ""
    $sourcePath = Read-Host "Source Path"
    
    if ([string]::IsNullOrWhiteSpace($sourcePath)) {
        Write-Host "✗ Source path cannot be empty!" -ForegroundColor Red
        return
    }
    
    Write-Host "→ Source: " -ForegroundColor Green -NoNewline
    Write-Host "$sourcePath" -ForegroundColor White
    Write-Host ""
    
    # Step 3: Get Destination Path
    Write-Host "Enter Destination Path:" -ForegroundColor Yellow
    Write-Host "  Example (Local): C:\Users\Downloads\" -ForegroundColor DarkGray
    Write-Host "  Example (LAN): \\client.barclayscorp.com\dfs-emea\GROUP\PCBRET\FRAUDRSK\Analytics_DHA\RR\Team Members\Sagar\" -ForegroundColor DarkGray
    Write-Host ""
    $destPath = Read-Host "Destination Path"
    
    if ([string]::IsNullOrWhiteSpace($destPath)) {
        Write-Host "✗ Destination path cannot be empty!" -ForegroundColor Red
        return
    }
    
    # Check if it's a LAN path and add quotes
    $isLANPath = $destPath.StartsWith("\\")
    if ($isLANPath) {
        $destPath = "`"$destPath`""
        Write-Host "→ Detected LAN path, quotes added automatically" -ForegroundColor Cyan
    }
    
    Write-Host "→ Destination: " -ForegroundColor Green -NoNewline
    Write-Host "$destPath" -ForegroundColor White
    Write-Host ""
    
    # Step 4: Execute Transfer
    Write-Host "════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "⏳ Copying file from $serverChoice to destination..." -ForegroundColor Yellow
    Write-Host "════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host ""
    
    try {
        # Build remote source path
        $remoteSource = "$($selectedServer.Host):$sourcePath"
        
        # Execute PSCP
        $command = "& `"$PSCP`" -pw $($selectedServer.Password) $remoteSource $destPath"
        Invoke-Expression $command
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host ""
            Write-Host "✓ File successfully copied from $serverChoice to destination!" -ForegroundColor Green
            Write-Host ""
        } else {
            Write-Host ""
            Write-Host "✗ Transfer failed with exit code: $LASTEXITCODE" -ForegroundColor Red
            Write-Host ""
        }
    }
    catch {
        Write-Host ""
        Write-Host "✗ Error during transfer: $($_.Exception.Message)" -ForegroundColor Red
        Write-Host ""
    }
}

# Aliases
Set-Alias ft Pull-File
Set-Alias pull Pull-File
