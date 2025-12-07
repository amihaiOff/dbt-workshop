# DBT Workshop Environment

A complete, containerized dbt (data build tool) workshop environment with VS Code Server and PostgreSQL, pre-loaded with Brazilian e-commerce data from Olist.

## What's Inside

- **VS Code Server**: Browser-based IDE accessible at `http://localhost:8080`
- **dbt**: Pre-installed with PostgreSQL adapter for data transformations
- **PostgreSQL 16**: Production-grade database with sample data
- **Sample Dataset**: Brazilian e-commerce data (Olist) with 9 tables and 100K+ rows
- **Pre-installed Extensions**:
  - dbt Power User
  - Database Client (browse & query PostgreSQL)
  - Python
  - GitLens

## Quick Start

### Prerequisites

- Docker Desktop installed and running
- 4GB+ free disk space
- Ports 8080 and 5432 available

### Setup (One Command)

```bash
./setup.sh
```

This script will:
1. Start Docker containers (PostgreSQL + VS Code Server)
2. Build the Docker image with all dependencies
3. Initialize the PostgreSQL database with sample data
4. Set up dbt with all necessary configurations
5. Open VS Code Server in your browser

### Manual Setup

If you prefer to run steps manually:

```bash
# Start the services
docker-compose up -d --build

# View logs
docker-compose logs -f

# Stop the services
docker-compose down

# Stop and remove all data
docker-compose down -v
```

## Accessing the Environment

### VS Code Server
- URL: http://localhost:8080
- Password: `workshop`

### Viewing Database in VS Code

The PostgreSQL connection is **pre-configured and ready to use**:

1. **Open VS Code** at http://localhost:8080 (password: `workshop`)
2. **Find the Database Client icon** in the left sidebar (looks like a database/server icon)
3. **Refresh if needed**: If you don't see the connection immediately, click the refresh icon at the top of the Database Client panel
4. **Look for "DBT Workshop"** connection - it should appear automatically
5. **Click the connection** to expand and connect automatically
6. **Browse your data**:
   - Expand `olist_data` schema
   - See all 9 tables with row counts
   - Right-click any table → "Show Table" to view data
   - Double-click a table to see its structure
7. **Run SQL queries**:
   - Click the "SQL" or "Data" button at the top
   - Write your query
   - Press `Cmd/Ctrl + Enter` or click the play button to execute

**Manual setup:**
- Click on create new connection
- Choose PostgreSQL in the server type
- Use these credentials
   - Host: `localhost` (from your machine) or `postgres` (from dbt-workshop container)
   - Port: `5432`
   - Database: `dbt_workshop`
   - User: `dbt_user`
   - Password: `dbt_password`
- Click connect to check that it works
- Save the connections

**Troubleshooting:**
- If connection doesn't appear: Click the refresh icon (↻) in the Database Client panel
- If asked to connect: Just click the connection name - credentials are pre-filled
- If you see an error: Check that both containers are running with `docker-compose ps`

**Features:**
- Visual table browser with instant data preview
- SQL editor with autocomplete and syntax highlighting
- Export results to CSV/JSON/Excel
- ER diagram visualization
- Table statistics and indexes
- **Zero manual configuration needed!**

### Connection String
```
postgresql://dbt_user:dbt_password@localhost:5432/dbt_workshop
```

## Using with Other IDEs (Cursor, VS Code Desktop, etc.)

While this project includes a browser-based VS Code Server, you can use your preferred IDE like Cursor, VS Code Desktop, or any other editor to work with this dbt project.

### Setup Steps

1. **Start the PostgreSQL database**:
   ```bash
   docker-compose up -d postgres
   ```
   This starts only the PostgreSQL container on `localhost:5432`

2. **Install dbt locally**:
   ```bash
   pip install dbt-core dbt-postgres
   ```

   *Optional: You can create a virtual environment for this project to isolate dependencies:*
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install dbt-core dbt-postgres
   ```

3. **Always run dbt from the project directory**:
   ```bash
   cd /path/to/dbt-workshop
   dbt debug
   dbt run
   ```

   **⚠️ IMPORTANT**: This project has its own `profiles.yml` file. Always run dbt commands from within the project directory, otherwise dbt will use your global `~/.dbt/profiles.yml` instead, which may conflict with your company's dbt configuration.

4. **Test the connection**:
   ```bash
   cd /path/to/dbt-workshop
   dbt debug
   ```

   You should see all checks pass and the connection test succeed.

### IDE-Specific Setup

**For Cursor or VS Code Desktop:**
1. Open the `dbt-workshop` folder in your IDE
2. Install recommended extensions:
   - dbt Power User
   - PostgreSQL client extension
3. Configure the PostgreSQL extension to connect to the database:
   - Host: `localhost`
   - Port: `5432`
   - Database: `dbt_workshop`
   - Username: `dbt_user`
   - Password: `dbt_password`

### Working with dbt

All dbt commands should be run from the project directory:

```bash
cd /path/to/dbt-workshop

# Run all models
dbt run

# Run a specific model
dbt run --select model_name

# Test data quality
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

### Important Notes

- **Always work from the project directory** to ensure dbt uses the project's `profiles.yml`
- **PostgreSQL must be running**: Ensure the database container is up (`docker-compose up -d postgres`)
- **Host connection**: When connecting from your local machine, the `profiles.yml` defaults to `localhost` (the `postgres` hostname is only used inside Docker containers)
- **Port 5432**: Make sure this port isn't already in use by another PostgreSQL instance

### Stopping the Database

When you're done:

```bash
# Stop PostgreSQL (keeps data)
docker-compose stop postgres

# Stop and remove everything (keeps data in volumes)
docker-compose down

# Remove all data
docker-compose down -v
```

## Workshop Session Management

### Quick Session Reset (Recommended for Training)

If you're following the workshop exercises and want to start fresh at a specific session with all solution models in place:

```bash
./reset_to_session.sh <session_number>
```

**Examples:**
```bash
./reset_to_session.sh 1    # Reset to end of Session 1 (foundations)
./reset_to_session.sh 2    # Reset to end of Session 2 (advanced patterns)
```

**What this does:**
1. ✅ Cleans up existing dbt model files
2. ✅ Drops dbt-created tables (stg_*, int_*, mart_*, snap_*)
3. ✅ **Preserves source data** (olist_* tables remain intact)
4. ✅ Creates all solution models for the specified session
5. ✅ Runs dbt to build tables in the database
6. ✅ For Session 2: Takes multiple snapshots to demonstrate SCD Type 2

**Available Sessions:**

| Session | Models Created | Database Tables |
|---------|----------------|-----------------|
| **1** | 6 models | 4 staging + 2 intermediate |
| | `stg_orders`, `stg_customers`, `stg_order_items`, `stg_order_payments` | |
| | `int_customer_landing`, `int_customer_daily_features` | |
| **2** | 8 models + 1 snapshot | All of Session 1 + 1 staging + 1 intermediate + 1 snapshot |
| | `stg_order_items_snapshot`, `int_seller_performance` | |
| | `snap_seller_tier` (with 4 time-based iterations) | |

**When to use this:**
- ✅ Starting a new workshop session
- ✅ Want to catch up to a specific point in the training
- ✅ Need consistent state with other participants
- ✅ Want to see the solutions and explore the code
- ✅ **Fast**: Doesn't reset database volumes (~1 min vs 3-5 min)

**What gets reset:**
- dbt model files (`.sql` files in `models/` and `snapshots/`)
- dbt-created tables in the database

**What is preserved:**
- ✅ Source data (all 9 olist_* tables)
- ✅ VS Code settings and extensions
- ✅ dbt project configuration

### Full Database Reset

If you need to completely reset the database back to its original state with **only** the raw Olist data:

```bash
./setup.sh --reset
```

This command will:
1. Ask for confirmation (type `y` to confirm)
2. Stop all containers
3. Remove the database volume (deletes all data including dbt changes)
4. Restart containers with a fresh database
5. Reload the original Olist dataset

**When to use this:**
- Database is in an inconsistent state
- Want to start completely from scratch
- Need to reload source data
- **Slower**: Takes 3-5 minutes to recreate volumes

**What gets reset:**
- All dbt models in `olist_data` schema are removed
- Any custom tables or views you created
- The database returns to containing only the 9 original Olist tables

**What is preserved:**
- Your `.sql` model files in the `models/` directory
- Your dbt project configuration
- VS Code settings and extensions

**Alternative quick reset:**
You can also manually reset with:
```bash
docker-compose down -v  # Remove volumes
docker-compose up -d    # Restart with fresh data
```

## Using dbt

Once inside VS Code Server terminal:

```bash
# Check dbt installation
dbt --version

# Debug connection
dbt debug

# Run all models
dbt run

# Run a specific model
dbt run --select model_name

# Test data quality
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## Dataset Information

The Olist dataset contains real Brazilian e-commerce data with the following tables:

| Table | Description | Rows |
|-------|-------------|------|
| `olist_customers` | Customer information | ~99K |
| `olist_geolocation` | Brazilian zip codes | ~1M |
| `olist_order_items` | Order line items | ~112K |
| `olist_order_payments` | Payment information | ~103K |
| `olist_order_reviews` | Customer reviews | ~99K |
| `olist_orders` | Order details | ~99K |
| `olist_products` | Product catalog | ~32K |
| `olist_sellers` | Seller information | ~3K |
| `product_category_name_translation` | Category translations | 71 |

All tables are in the `olist_data` schema.

## Project Structure

```
dbt-workshop/
├── README.md                   # This file
├── setup.sh                    # One-command setup script
├── reset_to_session.sh         # Reset to specific workshop session
├── docker-compose.yml          # Docker services configuration
├── Dockerfile                  # VS Code Server + dbt image
├── start.sh                    # Container startup script
├── dbt_project.yml             # dbt project configuration
├── profiles.yml                # dbt connection profiles
├── data/                       # CSV source files
│   ├── olist_customers.csv
│   ├── olist_orders.csv
│   └── ... (9 CSV files total)
├── scripts/
│   ├── init_postgres.py        # PostgreSQL data loader
│   └── init_sqlite.py          # Legacy SQLite loader
├── models/                     # dbt models (transformations)
│   ├── staging/                # Staging layer models
│   ├── intermediate/           # Intermediate layer models
│   └── mart/                   # Mart layer models
├── snapshots/                  # dbt snapshots for SCD Type 2
├── seeds/                      # Additional seed data
└── exercises/                  # Workshop exercises (HTML)
    ├── session1_hands_on.html
    ├── session2_hands_on.html
    └── session3_hands_on.html
```

## Troubleshooting

### Docker daemon not running
```bash
# Start Docker Desktop application
open -a Docker
```

### Port already in use
```bash
# Check what's using the port
lsof -i :8080
lsof -i :5432

# Change ports in docker-compose.yml if needed
```

### Database not initializing
```bash
# Check PostgreSQL logs
docker-compose logs postgres

# Reinitialize database
docker-compose down -v
docker-compose up -d --build
```

### dbt connection issues
```bash
# Inside VS Code Server terminal
dbt debug

# Check environment variables
env | grep POSTGRES
```

### Reset everything
```bash
# Remove all containers and volumes
docker-compose down -v

# Remove Docker images
docker rmi dbt-workshop-dbt-workshop

# Restart fresh
./setup.sh
```

## Development

### Switching Between SQLite and PostgreSQL

The project supports both databases. To use SQLite (legacy):

```bash
# Edit profiles.yml and change target
target: sqlite

# Or specify on command line
dbt run --target sqlite
```

### Adding New Models

1. Create a new `.sql` file in the `models/` directory
2. Write your transformation using dbt syntax
3. Run the model: `dbt run --select your_model_name`

### Modifying the Database

To add your own data:

1. Place CSV files in the `data/` directory
2. Modify `scripts/init_postgres.py` if needed
3. Rebuild: `docker-compose down -v && docker-compose up -d --build`

## Architecture

```
┌─────────────────────────────────────────────────┐
│  Host Machine (Your Computer)                   │
│                                                  │
│  Browser → http://localhost:8080                │
│  DB Client → localhost:5432                     │
└─────────────────┬───────────────────────────────┘
                  │
                  │ Docker Network
                  │
    ┌─────────────┴────────────┐
    │                           │
    ▼                           ▼
┌─────────────┐          ┌──────────────┐
│ dbt-workshop│          │   postgres   │
│             │          │              │
│ - VS Code   │◄────────►│ - PostgreSQL │
│   Server    │  network  │   16-alpine │
│ - dbt-core  │  comms   │ - Port 5432  │
│ - Python    │          │ - olist_data │
│ - Port 8080 │          │              │
└─────────────┘          └──────────────┘
```

## Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [Olist Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [VS Code Server](https://github.com/coder/code-server)

## License

This workshop environment is for educational purposes.

## Support

For issues or questions:
1. Check the Troubleshooting section above
2. Review Docker logs: `docker-compose logs`
3. Verify Docker Desktop is running
4. Ensure ports 8080 and 5432 are available
