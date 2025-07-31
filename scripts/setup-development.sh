#!/bin/bash

# Equity Swaps Microservices - Development Setup Script
# This script sets up the local development environment

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to check Java version
check_java_version() {
    if command_exists java; then
        java_version=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2 | cut -d'.' -f1)
        if [[ "$java_version" -ge 23 ]]; then
            print_success "Java $java_version detected"
            return 0
        else
            print_error "Java 23+ required, found Java $java_version"
            return 1
        fi
    else
        print_error "Java not found"
        return 1
    fi
}

# Function to check Maven version
check_maven_version() {
    if command_exists mvn; then
        maven_version=$(mvn -v | head -n 1 | cut -d' ' -f3)
        print_success "Maven $maven_version detected"
        return 0
    else
        print_error "Maven not found"
        return 1
    fi
}

# Function to check Docker version
check_docker_version() {
    if command_exists docker; then
        docker_version=$(docker --version | cut -d' ' -f3 | cut -d',' -f1)
        print_success "Docker $docker_version detected"
        
        # Check if Docker is running
        if docker info >/dev/null 2>&1; then
            print_success "Docker daemon is running"
            return 0
        else
            print_error "Docker daemon is not running"
            return 1
        fi
    else
        print_error "Docker not found"
        return 1
    fi
}

# Function to check Docker Compose version
check_docker_compose_version() {
    if command_exists docker-compose || docker compose version >/dev/null 2>&1; then
        if command_exists docker-compose; then
            compose_version=$(docker-compose --version | cut -d' ' -f3 | cut -d',' -f1)
            print_success "Docker Compose $compose_version detected"
        else
            compose_version=$(docker compose version --short)
            print_success "Docker Compose $compose_version detected"
        fi
        return 0
    else
        print_error "Docker Compose not found"
        return 1
    fi
}

# Function to setup environment variables
setup_environment() {
    print_status "Setting up environment variables..."
    
    # Create .env file if it doesn't exist
    if [[ ! -f .env ]]; then
        cat > .env << EOF
# Database Configuration
DB_URL=jdbc:postgresql://localhost:5432/equity_swaps
DB_USERNAME=equity_user
DB_PASSWORD=equity_pass

# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=redis_pass

# Kafka Configuration
KAFKA_BROKERS=localhost:9092

# CDM Configuration
CDM_EVENT_STORE_URL=jdbc:postgresql://localhost:5432/cdm_events

# Logging
LOG_LEVEL=DEBUG

# Application Profiles
SPRING_PROFILES_ACTIVE=local
EOF
        print_success "Created .env file"
    else
        print_warning ".env file already exists"
    fi
}

# Function to create log directories
setup_directories() {
    print_status "Creating necessary directories..."
    
    directories=(
        "logs"
        "docker/postgres/init"
        "docker/prometheus"
        "docker/grafana/provisioning/datasources"
        "docker/grafana/provisioning/dashboards"
        "docker/grafana/dashboards"
        "k8s/secrets"
        "scripts/test-data"
        "target"
    )
    
    for dir in "${directories[@]}"; do
        mkdir -p "$dir"
        print_success "Created directory: $dir"
    done
}

# Function to setup database initialization scripts
setup_database_scripts() {
    print_status "Setting up database initialization scripts..."
    
    cat > docker/postgres/init/01-create-databases.sql << 'EOF'
-- Create main application database
CREATE DATABASE equity_swaps;
CREATE DATABASE cdm_events;

-- Create users and grant permissions
CREATE USER equity_user WITH PASSWORD 'equity_pass';
GRANT ALL PRIVILEGES ON DATABASE equity_swaps TO equity_user;
GRANT ALL PRIVILEGES ON DATABASE cdm_events TO equity_user;

-- Connect to equity_swaps database and create schemas
\c equity_swaps;
CREATE SCHEMA IF NOT EXISTS positions;
CREATE SCHEMA IF NOT EXISTS lots;
CREATE SCHEMA IF NOT EXISTS cashflows;
CREATE SCHEMA IF NOT EXISTS reference_data;

GRANT ALL PRIVILEGES ON SCHEMA positions TO equity_user;
GRANT ALL PRIVILEGES ON SCHEMA lots TO equity_user;
GRANT ALL PRIVILEGES ON SCHEMA cashflows TO equity_user;
GRANT ALL PRIVILEGES ON SCHEMA reference_data TO equity_user;

-- Connect to cdm_events database and create schemas
\c cdm_events;
CREATE SCHEMA IF NOT EXISTS events;
CREATE SCHEMA IF NOT EXISTS audit;

GRANT ALL PRIVILEGES ON SCHEMA events TO equity_user;
GRANT ALL PRIVILEGES ON SCHEMA audit TO equity_user;
EOF
    
    print_success "Created database initialization script"
}

# Function to setup Prometheus configuration
setup_prometheus_config() {
    print_status "Setting up Prometheus configuration..."
    
    cat > docker/prometheus/prometheus.yml << 'EOF'
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'position-service'
    static_configs:
      - targets: ['position-service:8080']
    metrics_path: '/actuator/prometheus'
    scrape_interval: 10s

  - job_name: 'lot-service'
    static_configs:
      - targets: ['lot-service:8080']
    metrics_path: '/actuator/prometheus'
    scrape_interval: 10s

  - job_name: 'cashflow-service'
    static_configs:
      - targets: ['cashflow-service:8080']
    metrics_path: '/actuator/prometheus'
    scrape_interval: 10s

  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  - job_name: 'redis'
    static_configs:
      - targets: ['redis:6379']

  - job_name: 'postgres'
    static_configs:
      - targets: ['postgres:5432']
EOF
    
    print_success "Created Prometheus configuration"
}

# Function to setup Grafana datasources
setup_grafana_config() {
    print_status "Setting up Grafana configuration..."
    
    cat > docker/grafana/provisioning/datasources/prometheus.yml << 'EOF'
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: true
EOF
    
    cat > docker/grafana/provisioning/dashboards/dashboards.yml << 'EOF'
apiVersion: 1

providers:
  - name: 'default'
    orgId: 1
    folder: ''
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    allowUiUpdates: true
    options:
      path: /var/lib/grafana/dashboards
EOF
    
    print_success "Created Grafana configuration"
}

# Function to create sample test data
setup_test_data() {
    print_status "Setting up test data..."
    
    cat > scripts/test-data/sample-trades.json << 'EOF'
[
  {
    "tradeId": "TRADE_001",
    "portfolioId": "PORTFOLIO_001",
    "instrumentId": "AAPL",
    "quantity": 100,
    "price": 150.00,
    "side": "BUY",
    "executionTime": "2024-01-15T10:30:00Z"
  },
  {
    "tradeId": "TRADE_002",
    "portfolioId": "PORTFOLIO_001",
    "instrumentId": "GOOGL",
    "quantity": 50,
    "price": 2800.00,
    "side": "BUY",
    "executionTime": "2024-01-15T11:00:00Z"
  },
  {
    "tradeId": "TRADE_003",
    "portfolioId": "PORTFOLIO_002",
    "instrumentId": "MSFT",
    "quantity": 200,
    "price": 380.00,
    "side": "BUY",
    "executionTime": "2024-01-15T11:30:00Z"
  }
]
EOF
    
    cat > scripts/test-data/sample-equity-swaps.json << 'EOF'
[
  {
    "swapId": "SWAP_001",
    "portfolioId": "PORTFOLIO_001",
    "equityTicker": "AAPL",
    "notionalAmount": 1000000,
    "currency": "USD",
    "effectiveDate": "2024-01-15",
    "terminationDate": "2024-07-15",
    "resetFrequency": "MONTHLY"
  },
  {
    "swapId": "SWAP_002",
    "portfolioId": "PORTFOLIO_002",
    "equityTicker": "GOOGL",
    "notionalAmount": 500000,
    "currency": "USD",
    "effectiveDate": "2024-01-15",
    "terminationDate": "2024-12-15",
    "resetFrequency": "QUARTERLY"
  }
]
EOF
    
    print_success "Created test data files"
}

# Function to install development tools
install_dev_tools() {
    print_status "Setting up development tools..."
    
    # Create IDE configuration directories
    mkdir -p .vscode
    mkdir -p .idea
    
    # VSCode configuration
    cat > .vscode/settings.json << 'EOF'
{
    "java.configuration.updateBuildConfiguration": "automatic",
    "java.compile.nullAnalysis.mode": "automatic",
    "java.format.settings.url": "https://raw.githubusercontent.com/google/styleguide/gh-pages/eclipse-java-google-style.xml",
    "java.saveActions.organizeImports": true,
    "files.exclude": {
        "**/target": true,
        "**/.mvn": true
    },
    "editor.formatOnSave": true,
    "editor.codeActionsOnSave": {
        "source.organizeImports": true
    }
}
EOF
    
    cat > .vscode/launch.json << 'EOF'
{
    "version": "0.2.0",
    "configurations": [
        {
            "type": "java",
            "name": "Position Service",
            "request": "launch",
            "mainClass": "com.equityswaps.position.PositionServiceApplication",
            "projectName": "position-service",
            "args": "--spring.profiles.active=local",
            "vmArgs": "--enable-preview -Dspring.threads.virtual.enabled=true"
        },
        {
            "type": "java",
            "name": "Lot Service",
            "request": "launch",
            "mainClass": "com.equityswaps.lot.LotServiceApplication",
            "projectName": "lot-service",
            "args": "--spring.profiles.active=local",
            "vmArgs": "--enable-preview -Dspring.threads.virtual.enabled=true"
        },
        {
            "type": "java",
            "name": "Cashflow Service",
            "request": "launch",
            "mainClass": "com.equityswaps.cashflow.CashflowServiceApplication",
            "projectName": "cashflow-service",
            "args": "--spring.profiles.active=local",
            "vmArgs": "--enable-preview -Dspring.threads.virtual.enabled=true"
        }
    ]
}
EOF
    
    print_success "Created IDE configuration files"
}

# Function to build the project
build_project() {
    print_status "Building the project..."
    
    if mvn clean compile; then
        print_success "Project compiled successfully"
    else
        print_error "Project compilation failed"
        return 1
    fi
}

# Function to start infrastructure services
start_infrastructure() {
    print_status "Starting infrastructure services..."
    
    if docker-compose up -d postgres redis kafka zookeeper; then
        print_success "Infrastructure services started"
        
        # Wait for services to be ready
        print_status "Waiting for services to be ready..."
        sleep 30
        
        # Check service health
        if docker-compose ps | grep -q "Up (healthy)"; then
            print_success "All services are healthy"
        else
            print_warning "Some services may not be fully ready yet"
        fi
    else
        print_error "Failed to start infrastructure services"
        return 1
    fi
}

# Function to run health checks
run_health_checks() {
    print_status "Running health checks..."
    
    # Check PostgreSQL
    if docker exec equity-swaps-postgres pg_isready -U equity_user > /dev/null 2>&1; then
        print_success "PostgreSQL is ready"
    else
        print_error "PostgreSQL is not ready"
    fi
    
    # Check Redis
    if docker exec equity-swaps-redis redis-cli -a redis_pass ping > /dev/null 2>&1; then
        print_success "Redis is ready"
    else
        print_error "Redis is not ready"
    fi
    
    # Check Kafka
    if docker exec equity-swaps-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then
        print_success "Kafka is ready"
    else
        print_error "Kafka is not ready"
    fi
}

# Main setup function
main() {
    echo "=================================================="
    echo "Equity Swaps Microservices - Development Setup"
    echo "=================================================="
    echo
    
    print_status "Checking prerequisites..."
    
    # Check all prerequisites
    prerequisites_ok=true
    
    if ! check_java_version; then
        prerequisites_ok=false
        print_error "Please install Java 23 or higher"
    fi
    
    if ! check_maven_version; then
        prerequisites_ok=false
        print_error "Please install Maven 3.9 or higher"
    fi
    
    if ! check_docker_version; then
        prerequisites_ok=false
        print_error "Please install and start Docker"
    fi
    
    if ! check_docker_compose_version; then
        prerequisites_ok=false
        print_error "Please install Docker Compose"
    fi
    
    if [[ "$prerequisites_ok" != true ]]; then
        print_error "Prerequisites not met. Please install required tools and try again."
        exit 1
    fi
    
    print_success "All prerequisites met!"
    echo
    
    # Setup development environment
    setup_environment
    setup_directories
    setup_database_scripts
    setup_prometheus_config
    setup_grafana_config
    setup_test_data
    install_dev_tools
    
    echo
    print_status "Building project..."
    if build_project; then
        echo
        print_status "Starting infrastructure services..."
        if start_infrastructure; then
            echo
            run_health_checks
            
            echo
            echo "=================================================="
            print_success "Development environment setup complete!"
            echo "=================================================="
            echo
            echo "Services available at:"
            echo "  • Position Service:    http://localhost:8081"
            echo "  • Lot Service:         http://localhost:8082"
            echo "  • Cashflow Service:    http://localhost:8083"
            echo "  • Kafka UI:            http://localhost:8090"
            echo "  • Prometheus:          http://localhost:9090"
            echo "  • Grafana:             http://localhost:3000 (admin/admin)"
            echo "  • PgAdmin:             http://localhost:8080 (admin@equityswaps.com/admin)"
            echo "  • Jaeger:              http://localhost:16686"
            echo
            echo "Next steps:"
            echo "  1. Run 'mvn spring-boot:run' in each service directory to start the services"
            echo "  2. Use 'docker-compose logs -f [service]' to view logs"
            echo "  3. Import test data using scripts in scripts/test-data/"
            echo "  4. Check the README.md for API documentation and usage examples"
            echo
        else
            print_error "Failed to start infrastructure services"
            exit 1
        fi
    else
        print_error "Failed to build project"
        exit 1
    fi
}

# Handle script arguments
case "${1:-}" in
    --help|-h)
        echo "Usage: $0 [options]"
        echo "Options:"
        echo "  --help, -h     Show this help message"
        echo "  --clean        Clean up all Docker resources before setup"
        exit 0
        ;;
    --clean)
        print_status "Cleaning up Docker resources..."
        docker-compose down -v
        docker system prune -f
        ;;
esac

# Run main setup
main