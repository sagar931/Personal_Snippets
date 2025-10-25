# ============================================
# FILE TRANSFER FUNCTION
# ============================================

function Transfer-File {
    param(
        [string]$Direction,
        [string]$Server,
        [string]$SourcePath,
        [string]$DestPath
    )
    
    # Configuration
    $PSCP = "C:\dev\home\tools\putty\current\PSCP.EXE"
    $SERVERS = @{
        "GQ" = @{
            Host = "x@15458@7@10.53.12.45"
            Password = "ex11@29@2gurgaon"
        }
        "BQ" = @{
            Host = "x@15458@7@10.223.12.182"
            Password = ""  # Add BQ password here
        }
    }
    
    # Check if PSCP exists
    if (-not (Test-Path $PSCP)) {
        Write-Host "✗ Error: PSCP not found at $PSCP" -ForegroundColor Red
        return
    }
    
    # Display header
    Write-Host ""
    Write-Host "----------------------------------------------" -ForegroundColor Cyan
    Write-Host "|     📁 FILE TRANSFER UTILITY 📁            |" -ForegroundColor Cyan
    Write-Host "----------------------------------------------" -ForegroundColor Cyan
    Write-Host ""
    
    # Step 1: Select Direction
    if (-not $Direction) {
        Write-Host "Select Transfer Direction:" -ForegroundColor Yellow
        Write-Host "  1. Pull (Server → Local/LAN)" -ForegroundColor White
        Write-Host "  2. Push (Local/LAN → Server)" -ForegroundColor White
        Write-Host ""
        $dirChoice = Read-Host "Enter choice (1 or 2)"
        
        if ($dirChoice -eq "1") {
            $Direction = "Pull"
        } elseif ($dirChoice -eq "2") {
            $Direction = "Push"
        } else {
            Write-Host "✗ Invalid choice!" -ForegroundColor Red
            return
        }
    }
    
    Write-Host ""
    Write-Host "→ Direction: " -ForegroundColor Green -NoNewline
    Write-Host "$Direction" -ForegroundColor White
    Write-Host ""
    
    # Step 2: Select Server
    if (-not $Server) {
        Write-Host "Select Server:" -ForegroundColor Yellow
        Write-Host "  1. GQ (10.53.12.45)" -ForegroundColor White
        Write-Host "  2. BQ (10.223.12.182)" -ForegroundColor White
        Write-Host ""
        $serverChoice = Read-Host "Enter choice (1 or 2)"
        
        if ($serverChoice -eq "1") {
            $Server = "GQ"
        } elseif ($serverChoice -eq "2") {
            $Server = "BQ"
        } else {
            Write-Host "✗ Invalid choice!" -ForegroundColor Red
            return
        }
    }
    
    $selectedServer = $SERVERS[$Server]
    Write-Host "→ Server: " -ForegroundColor Green -NoNewline
    Write-Host "$Server ($($selectedServer.Host))" -ForegroundColor White
    Write-Host ""
    
    # Step 3: Get Source Path
    if (-not $SourcePath) {
        if ($Direction -eq "Pull") {
            Write-Host "Enter Source Path on Server:" -ForegroundColor Yellow
            Write-Host "  Example: /home/user/data/file.csv" -ForegroundColor DarkGray
        } else {
            Write-Host "Enter Source Path (Local/LAN):" -ForegroundColor Yellow
            Write-Host "  Example: C:\Users\file.csv" -ForegroundColor DarkGray
            Write-Host "  Example: \\client.barclayscorp.com\dfs-emea\...\file.csv" -ForegroundColor DarkGray
        }
        Write-Host ""
        $SourcePath = Read-Host "Source Path"
    }
    
    # Validate source path
    if ($Direction -eq "Push") {
        if (-not (Test-Path $SourcePath)) {
            Write-Host "✗ Error: Source file not found: $SourcePath" -ForegroundColor Red
            return
        }
    }
    
    Write-Host "→ Source: " -ForegroundColor Green -NoNewline
    Write-Host "$SourcePath" -ForegroundColor White
    Write-Host ""
    
    # Step 4: Get Destination Path
    if (-not $DestPath) {
        if ($Direction -eq "Pull") {
            Write-Host "Enter Destination Path (Local/LAN):" -ForegroundColor Yellow
            Write-Host "  Example: C:\Users\Downloads\" -ForegroundColor DarkGray
            Write-Host "  Example: \\client.barclayscorp.com\dfs-emea\..." -ForegroundColor DarkGray
        } else {
            Write-Host "Enter Destination Path on Server:" -ForegroundColor Yellow
            Write-Host "  Example: /home/user/data/" -ForegroundColor DarkGray
        }
        Write-Host ""
        $DestPath = Read-Host "Destination Path"
    }
    
    Write-Host "→ Destination: " -ForegroundColor Green -NoNewline
    Write-Host "$DestPath" -ForegroundColor White
    Write-Host ""
    
    # Step 5: Confirm and Execute
    Write-Host "--------------------------------------------" -ForegroundColor Cyan
    Write-Host "Ready to transfer!" -ForegroundColor Yellow
    Write-Host "--------------------------------------------" -ForegroundColor Cyan
    Write-Host ""
    
    $confirm = Read-Host "Proceed with transfer? (Y/N)"
    
    if ($confirm -ne "Y" -and $confirm -ne "y") {
        Write-Host "✗ Transfer cancelled." -ForegroundColor Red
        return
    }
    
    Write-Host ""
    Write-Host "⏳ Transferring file..." -ForegroundColor Yellow
    Write-Host ""
    
    # Build PSCP command
    try {
        if ($Direction -eq "Pull") {
            # Pull: Server → Local
            $remoteSource = "$($selectedServer.Host):$SourcePath"
            & $PSCP -pw $selectedServer.Password $remoteSource $DestPath
        } else {
            # Push: Local → Server
            $remoteDest = "$($selectedServer.Host):$DestPath"
            & $PSCP -pw $selectedServer.Password $SourcePath $remoteDest
        }
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host ""
            Write-Host "✓ Transfer completed successfully!" -ForegroundColor Green
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

# Alias for quick access
Set-Alias ft Transfer-File

# Helper function for quick pull
function Pull-File {
    param(
        [Parameter(Mandatory=$false)]
        [string]$Server,
        [Parameter(Mandatory=$false)]
        [string]$SourcePath,
        [Parameter(Mandatory=$false)]
        [string]$DestPath
    )
    Transfer-File -Direction "Pull" -Server $Server -SourcePath $SourcePath -DestPath $DestPath
}

# Helper function for quick push
function Push-File {
    param(
        [Parameter(Mandatory=$false)]
        [string]$Server,
        [Parameter(Mandatory=$false)]
        [string]$SourcePath,
        [Parameter(Mandatory=$false)]
        [string]$DestPath
    )
    Transfer-File -Direction "Push" -Server $Server -SourcePath $SourcePath -DestPath $DestPath
}

# Aliases
Set-Alias pull Pull-File
Set-Alias push Push-File


Write-Host "File Transfer:" -ForegroundColor Green
Write-Host ""
Write-Host "  Basic Workflow:" -ForegroundColor Cyan
Write-Host "    ft              " -ForegroundColor Yellow -NoNewline
Write-Host "→ Interactive file transfer utility" -ForegroundColor White
Write-Host "    pull            " -ForegroundColor Yellow -NoNewline
Write-Host "→ Pull file from server to local/LAN" -ForegroundColor White
Write-Host "    push            " -ForegroundColor Yellow -NoNewline
Write-Host "→ Push file from local/LAN to server" -ForegroundColor White
Write-Host ""
Write-Host "  Examples:" -ForegroundColor Cyan
Write-Host "    pull            " -ForegroundColor Yellow -NoNewline
Write-Host "→ Interactive mode with prompts" -ForegroundColor White
Write-Host "    pull -Server 'GQ' -SourcePath '/data/file.csv' -DestPath 'C:\temp\'" -ForegroundColor White
Write-Host "    push -Server 'GQ' -SourcePath 'C:\data.csv' -DestPath '/home/user/'" -ForegroundColor White
Write-Host ""
