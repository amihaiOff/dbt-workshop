#!/usr/bin/env bash
set -euo pipefail

echo "=================================================="
echo "DBT Workshop - Starting Container"
echo "=================================================="

# Ensure correct ownership of the project directory
chown -R coder:coder /home/coder/project || true

# Ensure dbt (installed in the venv) is on PATH for code-server terminals
export PATH="/opt/venv/bin:${PATH}"

# Install VS Code extensions on first run
EXTENSIONS_MARKER="/home/coder/.extensions_installed"
if [ ! -f "$EXTENSIONS_MARKER" ]; then
  echo ""
  echo "Installing VS Code extensions (first run only)..."

  # List of extensions to install
  EXTENSIONS=(
    "innoverio.vscode-dbt-power-user"
    "ms-python.python"
    "cweijan.vscode-database-client2"
    "eamodio.gitlens"
  )

  for ext in "${EXTENSIONS[@]}"; do
    echo "  Installing $ext..."
    code-server --install-extension "$ext" --force 2>&1 | grep -i "successfully installed\|already installed" || true
  done

  # Create marker file
  touch "$EXTENSIONS_MARKER"
  echo "✓ Extensions installed"
else
  echo "✓ Extensions already installed"
fi

# Check if we should initialize PostgreSQL
if [ -f "/home/coder/project/scripts/init_postgres.py" ]; then
  echo ""
  echo "Checking PostgreSQL initialization..."

  # Check if data is already loaded by querying the database
  ALREADY_LOADED=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT EXISTS (SELECT FROM information_schema.schemata WHERE schema_name = 'olist_data');" 2>/dev/null || echo "false")

  if [ "$ALREADY_LOADED" = "t" ]; then
    echo "✓ PostgreSQL database already initialized"
  else
    echo "Initializing PostgreSQL database with CSV data..."
    python3 /home/coder/project/scripts/init_postgres.py || echo "Warning: PostgreSQL initialization failed"
  fi
fi

# Run dbt deps and seed
if [ -f "/home/coder/project/dbt_project.yml" ]; then
  echo ""
  echo "Running dbt setup..."
  dbt deps --profiles-dir "/home/coder/project" --project-dir "/home/coder/project" || true
  dbt seed --profiles-dir "/home/coder/project" --project-dir "/home/coder/project" || true
  echo "✓ dbt setup complete"
fi

echo ""
echo "=================================================="
echo "Starting VS Code Server on http://localhost:8080"
echo "Password: workshop"
echo "=================================================="
echo ""

exec /usr/bin/code-server --bind-addr 0.0.0.0:8080 --auth password /home/coder/project

