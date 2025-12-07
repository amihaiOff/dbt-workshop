#!/usr/bin/env bash
set -euo pipefail

# Parse command line arguments
RESET_MODE=false
if [[ "${1:-}" == "--reset" ]] || [[ "${1:-}" == "-r" ]]; then
    RESET_MODE=true
fi

# Detect Docker Compose command (support both old and new versions)
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
elif docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
else
    echo "Error: Neither 'docker-compose' nor 'docker compose' is available"
    echo "Please install Docker Desktop which includes Docker Compose"
    exit 1
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}ℹ ${NC}$1"
}

print_success() {
    echo -e "${GREEN}✓ ${NC}$1"
}

print_warning() {
    echo -e "${YELLOW}⚠ ${NC}$1"
}

print_error() {
    echo -e "${RED}✗ ${NC}$1"
}

print_header() {
    echo ""
    echo -e "${BLUE}================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================================${NC}"
    echo ""
}

# Check if Docker is installed
check_docker() {
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed"
        print_info "Please install Docker Desktop from: https://www.docker.com/products/docker-desktop"
        exit 1
    fi
    print_success "Docker is installed"
}

# Check if Docker daemon is running
check_docker_running() {
    if ! docker info &> /dev/null; then
        print_error "Docker daemon is not running"
        print_info "Please start Docker Desktop"

        # Try to open Docker Desktop on macOS
        if [[ "$OSTYPE" == "darwin"* ]]; then
            print_info "Attempting to start Docker Desktop..."
            open -a Docker
            print_info "Waiting for Docker to start (this may take 30-60 seconds)..."

            # Wait up to 60 seconds for Docker to start
            for i in {1..60}; do
                if docker info &> /dev/null; then
                    print_success "Docker is now running"
                    return 0
                fi
                sleep 1
            done

            print_error "Docker failed to start automatically"
            print_info "Please start Docker Desktop manually and run this script again"
            exit 1
        else
            print_info "Please start Docker and run this script again"
            exit 1
        fi
    fi
    print_success "Docker daemon is running"
}

# Check if required ports are available
check_ports() {
    local ports_in_use=()

    if lsof -Pi :8080 -sTCP:LISTEN -t >/dev/null 2>&1; then
        ports_in_use+=("8080")
    fi

    if lsof -Pi :5432 -sTCP:LISTEN -t >/dev/null 2>&1; then
        ports_in_use+=("5432")
    fi

    if [ ${#ports_in_use[@]} -gt 0 ]; then
        print_warning "The following ports are in use: ${ports_in_use[*]}"
        print_info "These ports need to be free for the workshop to run"

        for port in "${ports_in_use[@]}"; do
            print_info "Port $port is used by:"
            lsof -i ":$port" | head -2
        done

        read -p "Do you want to continue anyway? (y/N) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            print_info "Please free up the ports and run this script again"
            exit 1
        fi
    else
        print_success "Required ports (8080, 5432) are available"
    fi
}

# Check container status and start if needed
check_and_start_containers() {
    local containers_exist=false
    local containers_running=false

    # Check if containers exist
    if docker ps -a | grep -q "dbt-workshop"; then
        containers_exist=true
    fi

    # Check if containers are running
    if docker ps | grep -q "dbt-workshop"; then
        containers_running=true
    fi

    if [ "$containers_running" = true ]; then
        print_success "Workshop containers are already running"
        print_info "Database state has been preserved from previous session"
        return 0
    elif [ "$containers_exist" = true ]; then
        print_info "Found existing containers - starting them (database state will be preserved)..."
        $DOCKER_COMPOSE start
        print_success "Containers started with preserved database state"
        return 0
    else
        print_info "No existing containers found - building and starting fresh..."
        print_info "Building Docker images (this may take a few minutes on first run)..."
        $DOCKER_COMPOSE build
        print_success "Docker images built successfully"

        print_info "Starting services (PostgreSQL + VS Code Server)..."
        $DOCKER_COMPOSE up -d
        print_success "Services started"
        return 0
    fi
}

# Reset database by removing volumes
reset_database() {
    print_warning "RESETTING DATABASE - All dbt changes and data will be lost!"

    read -p "Are you sure you want to reset the database? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_info "Reset cancelled"
        exit 0
    fi

    print_info "Stopping services and removing database volumes..."
    $DOCKER_COMPOSE down -v 2>/dev/null || true
    print_success "Database volumes removed - fresh data will be loaded on restart"
}

# Wait for services to be healthy
wait_for_services() {
    print_info "Waiting for PostgreSQL to be ready..."

    local max_attempts=30
    local attempt=0

    while [ $attempt -lt $max_attempts ]; do
        if $DOCKER_COMPOSE ps | grep -q "postgres.*healthy"; then
            print_success "PostgreSQL is ready"
            break
        fi

        attempt=$((attempt + 1))
        sleep 1

        if [ $attempt -eq $max_attempts ]; then
            print_error "PostgreSQL failed to start within expected time"
            print_info "Check logs with: $DOCKER_COMPOSE logs postgres"
            exit 1
        fi
    done

    print_info "Waiting for VS Code Server to be ready..."
    sleep 5

    # Check if dbt-workshop container is running
    if docker ps | grep -q "dbt-workshop"; then
        print_success "VS Code Server is ready"
    else
        print_error "VS Code Server failed to start"
        print_info "Check logs with: $DOCKER_COMPOSE logs dbt-workshop"
        exit 1
    fi
}

# Show connection information
show_info() {
    print_header "Setup Complete!"

    echo "Your dbt workshop environment is ready!"
    echo ""
    echo "VS Code Server:"
    echo "  URL: http://localhost:8080"
    echo "  Password: workshop"
    echo ""
    echo "PostgreSQL Database:"
    echo "  Host: localhost"
    echo "  Port: 5432"
    echo "  Database: dbt_workshop"
    echo "  User: dbt_user"
    echo "  Password: dbt_password"
    echo "  Schema: olist_data"
    echo ""
    echo "Connection String:"
    echo "  postgresql://dbt_user:dbt_password@localhost:5432/dbt_workshop"
    echo ""

    print_info "Opening VS Code Server in your browser..."

    # Open browser based on OS
    if [[ "$OSTYPE" == "darwin"* ]]; then
        open http://localhost:8080
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        xdg-open http://localhost:8080 2>/dev/null || true
    elif [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]]; then
        start http://localhost:8080
    fi

    echo ""
    print_success "Setup complete! Happy learning!"
    echo ""
    echo "Useful commands:"
    echo "  $DOCKER_COMPOSE logs -f          # View logs"
    echo "  $DOCKER_COMPOSE down             # Stop services"
    echo "  $DOCKER_COMPOSE down -v          # Stop and remove data"
    echo "  $DOCKER_COMPOSE restart          # Restart services"
    echo ""
}

# Main execution
main() {
    if [ "$RESET_MODE" = true ]; then
        print_header "DBT Workshop Database Reset"

        print_info "Checking prerequisites..."
        check_docker
        check_docker_running

        reset_database

        print_info "Starting services with fresh database..."
        check_and_start_containers
        wait_for_services

        print_header "Database Reset Complete!"
        echo "Your database has been reset to its original state."
        echo "All dbt models, transformations, and changes have been removed."
        echo ""
        echo "VS Code Server: http://localhost:8080"
        echo "Password: workshop"
        echo ""
    else
        print_header "DBT Workshop Setup"

        print_info "Checking prerequisites..."
        check_docker
        check_docker_running
        check_ports

        print_info "Setting up workshop environment..."
        check_and_start_containers
        wait_for_services

        show_info
    fi
}

# Run main function
main
