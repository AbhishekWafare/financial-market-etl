#!/usr/bin/env bash

set -euo pipefail

# Resolve the project directory automatically.
PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

find_mintty_pid() {
    local title="$1"

    MINTTY_TITLE="$title" powershell.exe -NoProfile -Command '
        $needle = "--title=" + $env:MINTTY_TITLE
        $proc = Get-CimInstance Win32_Process |
            Where-Object {
                $_.Name -eq "mintty.exe" -and
                $_.CommandLine -and
                $_.CommandLine.Contains($needle)
            } |
            Select-Object -First 1
        if ($proc) {
            [Console]::WriteLine($proc.ProcessId)
        }
    ' | tr -d '\r\n'
}

is_mintty_running() {
    local pid="$1"
    local title="$2"

    MINTTY_PID="$pid" MINTTY_TITLE="$title" \
        powershell.exe -NoProfile -Command '
            $targetPid = [int]$env:MINTTY_PID
            $needle = "--title=" + $env:MINTTY_TITLE
            $proc = Get-CimInstance Win32_Process `
                -Filter "ProcessId = $targetPid" `
                -ErrorAction SilentlyContinue
            if (
                $proc -and
                $proc.Name -eq "mintty.exe" -and
                $proc.CommandLine -and
                $proc.CommandLine.Contains($needle)
            ) {
                exit 0
            }
            exit 1
        ' >/dev/null 2>&1
}

stop_dashboard() {
    local title="Financial Market Dashboard"
    local pid=""

    echo
echo "Stopping Streamlit dashboard..."
    pid="$(find_mintty_pid "$title")"

    if [[ ! "$pid" =~ ^[0-9]+$ ]]; then
        echo "Dashboard terminal is already closed."
        return 0
    fi

    echo "Found dashboard terminal (Windows PID: $pid). Closing its process tree..."
    MSYS_NO_PATHCONV=1 taskkill.exe /PID "$pid" /T >/dev/null 2>&1 || true

    for attempt in {1..10}; do
        if ! is_mintty_running "$pid" "$title"; then
            echo "Dashboard terminal closed successfully."
            return 0
        fi
        sleep 1
    done

    echo "Graceful shutdown timed out. Force stopping dashboard terminal..."
    MSYS_NO_PATHCONV=1 taskkill.exe /PID "$pid" /T /F >/dev/null 2>&1 || true
}

echo "=========================================="
echo " STOPPING FINANCIAL MARKET ETL PIPELINE"
echo "=========================================="

for cmd in docker powershell.exe taskkill.exe; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "ERROR: $cmd is not available."
        exit 1
    fi
done

# Stop the local Streamlit process first.
stop_dashboard

# Stop Airflow and PostgreSQL while preserving the named PostgreSQL volume.
echo
echo "Stopping Apache Airflow and PostgreSQL..."
(
    cd "$PROJECT_DIR"
    docker compose down --timeout 60
)

echo
echo "=========================================="
echo " PROJECT STOPPED"
echo "=========================================="
echo "Streamlit dashboard terminal closed."
echo "Airflow and PostgreSQL containers stopped."
echo "PostgreSQL Docker volume has been preserved."
echo
echo "Run ./start-project.sh to start the project again."
echo "Do not use 'docker compose down -v' unless you intentionally want to delete database data."
