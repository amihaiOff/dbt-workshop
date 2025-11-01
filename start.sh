#!/usr/bin/env bash
set -euo pipefail

# Ensure correct ownership of the project directory
chown -R coder:coder /home/coder/project || true

# Ensure dbt (installed in the venv) is on PATH for code-server terminals
export PATH="/opt/venv/bin:${PATH}"

# Initialize sqlite DB with seed data only if DB file is missing
DB_FILE="/home/coder/project/data/workshop.db"
if [ -f "/home/coder/project/scripts/init_sqlite.py" ] && [ ! -f "$DB_FILE" ]; then
  echo "Initializing SQLite database at $DB_FILE ..."
  python3 /home/coder/project/scripts/init_sqlite.py || true
fi

if [ -f "/home/coder/project/dbt_project.yml" ]; then
  echo "Running dbt deps && dbt seed..."
  dbt deps || true
  dbt seed --profiles-dir "/home/coder/project" --project-dir "/home/coder/project" || true
fi

echo "Starting code-server on :8080"
exec /usr/bin/code-server --bind-addr 0.0.0.0:8080 --auth password /home/coder/project

