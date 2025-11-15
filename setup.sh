#!/usr/bin/env bash
set -euo pipefail

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

# Stop and remove existing containers
cleanup_existing() {
    if docker ps -a | grep -q "dbt-workshop"; then
        print_info "Removing existing workshop containers..."
        docker-compose down 2>/dev/null || true
        print_success "Cleaned up existing containers"
    fi
}

# Build and start containers
start_services() {
    print_info "Building Docker images (this may take a few minutes on first run)..."
    docker-compose build
    print_success "Docker images built successfully"

    print_info "Starting services (PostgreSQL + VS Code Server)..."
    docker-compose up -d
    print_success "Services started"
}

# Wait for services to be healthy
wait_for_services() {
    print_info "Waiting for PostgreSQL to be ready..."

    local max_attempts=30
    local attempt=0

    while [ $attempt -lt $max_attempts ]; do
        if docker-compose ps | grep -q "postgres.*healthy"; then
            print_success "PostgreSQL is ready"
            break
        fi

        attempt=$((attempt + 1))
        sleep 1

        if [ $attempt -eq $max_attempts ]; then
            print_error "PostgreSQL failed to start within expected time"
            print_info "Check logs with: docker-compose logs postgres"
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
        print_info "Check logs with: docker-compose logs dbt-workshop"
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
    echo "  docker-compose logs -f          # View logs"
    echo "  docker-compose down             # Stop services"
    echo "  docker-compose down -v          # Stop and remove data"
    echo "  docker-compose restart          # Restart services"
    echo ""
}

# Main execution
main() {
    print_header "DBT Workshop Setup"

    print_info "Checking prerequisites..."
    check_docker
    check_docker_running
    check_ports

    print_info "Setting up workshop environment..."
    cleanup_existing
    start_services
    wait_for_services

    show_info
}

# Run main function
main
