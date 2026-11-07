#!/usr/bin/env bash

set -euo pipefail

# Resolve the project directory automatically.
PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
DASHBOARD_FILE="$PROJECT_DIR/dashboard/TaskFlow_app.py"
AIRFLOW_URL="http://localhost:8080"
DASHBOARD_URL="http://localhost:8502"

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

echo "=========================================="
echo " FINANCIAL MARKET ETL PIPELINE"
echo "=========================================="

# Check required applications.
for cmd in docker python mintty curl powershell.exe taskkill.exe; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "ERROR: $cmd is not available."
        exit 1
    fi
done

# Check Docker Desktop.
if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker Desktop is not running. Start Docker Desktop and retry."
    exit 1
fi

# Check project configuration and files.
if [[ ! -f "$PROJECT_DIR/.env" ]]; then
    echo "ERROR: Missing .env file: $PROJECT_DIR/.env"
    echo "Create it first using: cp .env.example .env"
    exit 1
fi

if [[ ! -f "$PROJECT_DIR/docker-compose.yaml" ]]; then
    echo "ERROR: docker-compose.yaml was not found in the project root."
    exit 1
fi

if [[ ! -f "$DASHBOARD_FILE" ]]; then
    echo "ERROR: Dashboard file not found: $DASHBOARD_FILE"
    exit 1
fi

# Prefer the project's Windows virtual environment when it exists.
if [[ -f "$PROJECT_DIR/venv/Scripts/python.exe" ]]; then
    PYTHON_BIN="$PROJECT_DIR/venv/Scripts/python.exe"
elif [[ -f "$PROJECT_DIR/.venv/Scripts/python.exe" ]]; then
    PYTHON_BIN="$PROJECT_DIR/.venv/Scripts/python.exe"
else
    PYTHON_BIN="$(command -v python)"
fi

# Verify dashboard dependencies before starting the services.
if ! "$PYTHON_BIN" -c "import streamlit, pandas, psycopg2, matplotlib, dotenv" >/dev/null 2>&1; then
    echo "ERROR: Dashboard dependencies are missing from: $PYTHON_BIN"
    echo "Activate your project virtual environment and run:"
    echo "  python -m pip install -r requirements.txt"
    exit 1
fi

# Prevent duplicate dashboard windows.
EXISTING_DASHBOARD_PID="$(find_mintty_pid "Financial Market Dashboard")"
if [[ "$EXISTING_DASHBOARD_PID" =~ ^[0-9]+$ ]]; then
    echo "ERROR: Financial Market Dashboard is already running (PID: $EXISTING_DASHBOARD_PID)."
    echo "Run ./stop-project.sh before starting another instance."
    exit 1
fi

# Validate Compose configuration before starting anything.
echo
echo "[1/4] Validating Docker Compose configuration..."
(
    cd "$PROJECT_DIR"
    docker compose config --quiet
)

# Start PostgreSQL and Airflow.
echo
echo "[2/4] Starting PostgreSQL and Apache Airflow..."
(
    cd "$PROJECT_DIR"
    docker compose up -d
)

# Wait for PostgreSQL's configured Docker healthcheck.
echo
echo "Waiting for PostgreSQL to become healthy..."
POSTGRES_HEALTH=""
for attempt in {1..60}; do
    POSTGRES_ID="$(cd "$PROJECT_DIR" && docker compose ps -q postgres)"

    if [[ -n "$POSTGRES_ID" ]]; then
        POSTGRES_HEALTH="$(docker inspect --format '{{.State.Health.Status}}' "$POSTGRES_ID" 2>/dev/null || true)"
    fi

    if [[ "$POSTGRES_HEALTH" == "healthy" ]]; then
        break
    fi

    if (( attempt % 5 == 0 )); then
        echo "Waiting for PostgreSQL... ($attempt/60)"
    fi
    sleep 2
done

if [[ "$POSTGRES_HEALTH" != "healthy" ]]; then
    echo "ERROR: PostgreSQL did not become healthy."
    (cd "$PROJECT_DIR" && docker compose logs --tail=80 postgres)
    exit 1
fi

echo "PostgreSQL is healthy."

# Wait for the Airflow 3 API server and standalone initialization.
echo
echo "Waiting for Airflow initialization..."
AIRFLOW_READY=false
for attempt in {1..60}; do
    if curl -fsS "$AIRFLOW_URL/api/v2/version" >/dev/null 2>&1; then
        AIRFLOW_READY=true
        break
    fi

    if (( attempt % 3 == 0 )); then
        echo "Waiting for Airflow... ($attempt/60)"
    fi
    sleep 5
done

if [[ "$AIRFLOW_READY" != true ]]; then
    echo "ERROR: Airflow API did not become ready."
    echo "Check Airflow logs:"
    (cd "$PROJECT_DIR" && docker compose logs --tail=120 airflow)
    exit 1
fi

echo "Airflow API is ready."

# Display the generated standalone administrator password when available.
echo
echo "=========================================="
echo " AIRFLOW LOGIN"
echo "=========================================="
echo "Airflow URL: $AIRFLOW_URL"
if (
    cd "$PROJECT_DIR"
    docker compose exec -T airflow sh -c 'test -s "${AIRFLOW_HOME:-/opt/airflow}/simple_auth_manager_passwords.json.generated"'
) >/dev/null 2>&1; then
    echo "Username: admin"
    echo "Generated password file contents:"
    (
        cd "$PROJECT_DIR"
        docker compose exec -T airflow sh -c 'cat "${AIRFLOW_HOME:-/opt/airflow}/simple_auth_manager_passwords.json.generated"'
    )
else
    echo "Use the Airflow administrator credentials configured for this installation."
    echo "For startup details, run: docker compose logs airflow"
fi
echo "=========================================="

# Show DAG import errors, if any, without shutting down otherwise healthy services.
echo
echo "Checking Airflow DAG import status..."
(
    cd "$PROJECT_DIR"
    docker compose exec -T airflow airflow dags list-import-errors
) || echo "WARNING: Could not complete the DAG import-error check. Review Airflow logs."

# Launch Streamlit in its own Git Bash window.
echo
echo "[3/4] Starting Streamlit dashboard..."
mintty --title="Financial Market Dashboard" bash -lc '
    cd "$1" || exit 1
    echo "Starting Financial Market Dashboard..."
    echo "Dashboard URL: http://localhost:8502"
    "$2" -u -m streamlit run "$1/dashboard/TaskFlow_app.py" \
        --server.address 127.0.0.1 \
        --server.port 8502
    status=$?
    echo
    echo "Dashboard exited with status: $status"
    read -r -p "Press Enter to close this window..."
' bash "$PROJECT_DIR" "$PYTHON_BIN" &

DASHBOARD_TERMINAL_PID=""
for attempt in {1..30}; do
    DASHBOARD_TERMINAL_PID="$(find_mintty_pid "Financial Market Dashboard")"
    if [[ "$DASHBOARD_TERMINAL_PID" =~ ^[0-9]+$ ]]; then
        break
    fi
    sleep 1
done

if [[ ! "$DASHBOARD_TERMINAL_PID" =~ ^[0-9]+$ ]]; then
    echo "WARNING: Could not identify the dashboard terminal window."
else
    echo "Dashboard terminal PID: $DASHBOARD_TERMINAL_PID"
fi

# Open the UIs in the default Windows browser without blocking this Bash script.
# Start-Process delegates URL handling to Windows and runs in the background.
open_url_async() {
    local url="$1"

    (
        URL_TO_OPEN="$url" powershell.exe -NoProfile -NonInteractive \
            -Command 'Start-Process -FilePath $env:URL_TO_OPEN' \
            >/dev/null 2>&1
    ) </dev/null >/dev/null 2>&1 &
}

echo
echo "[4/4] Opening project interfaces..."
open_url_async "$AIRFLOW_URL"
open_url_async "$DASHBOARD_URL"
echo "Browser launch requests sent."

echo
echo "=========================================="
echo " PROJECT STARTUP COMMANDS COMPLETED"
echo "=========================================="
echo "Airflow UI:   $AIRFLOW_URL"
echo "Dashboard:    $DASHBOARD_URL"
echo "PostgreSQL:   localhost:5433 (default mapping)"
echo
echo "To view service logs:"
echo "  docker compose logs -f airflow"
echo
echo "To view table data in DB:"
echo "  docker compose exec postgres psql -U airflow -d airflow"
echo "   select * from tf_bitcoin_price order by time desc;"
echo
echo "To stop the project:"
echo "  ./stop-project.sh"
echo
echo "Docker Compose startup is complete. Airflow may still need a little time to finish parsing DAG files."
