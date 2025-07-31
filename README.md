# Equity Swaps Microservices Platform

A high-performance microservices platform for equity swaps position management, lot tracking, and cashflow generation, built with Java 23 and aligned with the FINOS Common Domain Model (CDM).

## 🚀 Features

- **Position Management**: Real-time position tracking and P&L calculation
- **Lot Management**: FIFO/LIFO/Average cost accounting with corporate action support
- **Cashflow Engine**: Automated cashflow calculation and regulatory reporting
- **CDM Alignment**: Full integration with FINOS Common Domain Model
- **High Performance**: Leverages Java 23 Virtual Threads for massive concurrency
- **Event-Driven**: Kafka-based event streaming architecture
- **Real-time**: Redis caching for low-latency operations

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Position       │    │  Lot Management │    │  Cashflow       │
│  Service        │    │  Service        │    │  Engine         │
│  (Port 8081)    │    │  (Port 8082)    │    │  (Port 8083)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
         ┌─────────────────────────────────────────────────┐
         │              Event Bus (Kafka)                   │
         └─────────────────────────────────────────────────┘
                                 │
         ┌─────────────────────────────────────────────────┐
         │         CDM Event Processor                     │
         │    ┌─────────────┐  ┌─────────────┐             │
         │    │ Event Store │  │ Redis Cache │             │
         │    │(PostgreSQL) │  │             │             │
         │    └─────────────┘  └─────────────┘             │
         └─────────────────────────────────────────────────┘
```

## 🛠️ Technology Stack

- **Java 23** with Virtual Threads (Project Loom)
- **Spring Boot 3.2+** with WebFlux
- **PostgreSQL** for persistent storage
- **Redis** for caching and real-time data
- **Apache Kafka** for event streaming
- **Docker** and **Kubernetes** for deployment
- **FINOS CDM** for financial data modeling

## 📋 Prerequisites

- Java 23 (OpenJDK recommended)
- Docker and Docker Compose
- Maven 3.9+
- kubectl (for Kubernetes deployment)

## 🚀 Quick Start

### Local Development with Docker Compose

1. **Clone the repository**
   ```bash
   git clone https://github.com/your-org/equity-swaps-microservices.git
   cd equity-swaps-microservices
   ```

2. **Start infrastructure services**
   ```bash
   docker-compose up -d postgres redis kafka zookeeper
   ```

3. **Build and run services**
   ```bash
   # Build all services
   mvn clean package
   
   # Start services
   docker-compose up -d
   ```

4. **Verify services are running**
   ```bash
   curl http://localhost:8081/actuator/health  # Position Service
   curl http://localhost:8082/actuator/health  # Lot Service
   curl http://localhost:8083/actuator/health  # Cashflow Service
   ```

### API Usage Examples

#### Position Management
```bash
# Get positions for a portfolio
curl http://localhost:8081/api/v1/positions/PORTFOLIO_001

# Update position with new trade
curl -X POST http://localhost:8081/api/v1/positions/update \
  -H "Content-Type: application/json" \
  -d '{
    "tradeId": "TRADE_001",
    "portfolioId": "PORTFOLIO_001",
    "instrumentId": "AAPL",
    "quantity": 100,
    "price": 150.00,
    "side": "BUY",
    "executionTime": "2024-01-15T10:30:00Z"
  }'

# Get P&L report
curl http://localhost:8081/api/v1/positions/PORTFOLIO_001/pnl
```

#### Lot Management
```bash
# Get lots for a portfolio
curl http://localhost:8082/api/v1/lots/PORTFOLIO_001

# Allocate sale using FIFO
curl -X POST http://localhost:8082/api/v1/lots/allocate/fifo \
  -H "Content-Type: application/json" \
  -d '{
    "portfolioId": "PORTFOLIO_001",
    "instrumentId": "AAPL",
    "quantity": 50
  }'

# Process stock split
curl -X POST http://localhost:8082/api/v1/lots/corporate-action/stock-split \
  -H "Content-Type: application/json" \
  -d '{
    "instrumentId": "AAPL",
    "splitRatio": 2.0,
    "effectiveDate": "2024-01-20"
  }'
```

#### Cashflow Engine
```bash
# Calculate cashflows for a swap
curl -X POST http://localhost:8083/api/v1/cashflows/calculate/SWAP_001

# Get cashflow schedule
curl http://localhost:8083/api/v1/cashflows/schedule/SWAP_001

# Get upcoming cashflows
curl http://localhost:8083/api/v1/cashflows/schedule/upcoming
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_URL` | PostgreSQL connection URL | `jdbc:postgresql://localhost:5432/equity_swaps` |
| `DB_USERNAME` | Database username | `equity_user` |
| `DB_PASSWORD` | Database password | `equity_pass` |
| `REDIS_HOST` | Redis host | `localhost` |
| `REDIS_PORT` | Redis port | `6379` |
| `KAFKA_BROKERS` | Kafka broker list | `localhost:9092` |
| `CDM_EVENT_STORE_URL` | CDM event store URL | `postgresql://localhost:5432/cdm_events` |

### Application Properties

Key configuration options in `application.yml`:

```yaml
spring:
  threads:
    virtual:
      enabled: true  # Enable Virtual Threads

equity-swaps:
  position-service:
    cache-ttl: PT15M
    batch-size: 100
  lot-service:
    default-accounting-method: FIFO
  cashflow-service:
    calculation-window: P30D
    settlement-delay: P2D
```

## 🏭 Production Deployment

### Kubernetes

1. **Apply namespace and secrets**
   ```bash
   kubectl apply -f k8s/namespace.yaml
   kubectl apply -f k8s/secrets.yaml
   ```

2. **Deploy infrastructure**
   ```bash
   kubectl apply -f k8s/postgres.yaml
   kubectl apply -f k8s/redis.yaml
   kubectl apply -f k8s/kafka.yaml
   ```

3. **Deploy services**
   ```bash
   kubectl apply -f k8s/position-service.yaml
   kubectl apply -f k8s/lot-service.yaml
   kubectl apply -f k8s/cashflow-service.yaml
   ```

4. **Verify deployment**
   ```bash
   kubectl get pods -n equity-swaps
   kubectl get services -n equity-swaps
   ```

### Monitoring

The platform includes comprehensive monitoring:

- **Health Checks**: `/actuator/health` endpoints
- **Metrics**: Prometheus metrics at `/actuator/prometheus`
- **Distributed Tracing**: Micrometer integration
- **Application Logs**: Structured JSON logging

## 🧪 Testing

### Unit Tests
```bash
mvn test
```

### Integration Tests
```bash
mvn verify -P integration-tests
```

### Load Testing
```bash
# Start services and run load tests
./scripts/load-test.sh
```

## 📖 API Documentation

API documentation is available at:
- Position Service: http://localhost:8081/swagger-ui.html
- Lot Service: http://localhost:8082/swagger-ui.html
- Cashflow Service: http://localhost:8083/swagger-ui.html

## 🔄 CDM Integration

This platform is fully aligned with the FINOS Common Domain Model:

- **Business Events**: All trades processed as CDM business events
- **Data Models**: CDM-compliant data structures
- **Functions**: Uses CDM calculation functions
- **Regulatory Reporting**: CDM Digital Regulatory Reporting (DRR) framework

### CDM Event Flow

1. Trade execution creates CDM `ExecutionPrimitive`
2. Contract formation generates CDM `BusinessEvent`
3. Position updates use CDM `EquityPerformance` calculations
4. Cashflows calculated with CDM `ResolveCashflow` functions
5. Regulatory reports generated via CDM DRR framework

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Follow Java 23 best practices
- Use Virtual Threads for I/O operations
- Maintain CDM compliance
- Include comprehensive tests
- Update documentation

## 📝 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

- **Documentation**: [Wiki](https://github.com/your-org/equity-swaps-microservices/wiki)
- **Issues**: [GitHub Issues](https://github.com/your-org/equity-swaps-microservices/issues)
- **Discussions**: [GitHub Discussions](https://github.com/your-org/equity-swaps-microservices/discussions)

## 🎯 Roadmap

- [ ] Real-time risk analytics
- [ ] Machine learning for P&L prediction
- [ ] Multi-currency support
- [ ] Advanced corporate actions
- [ ] Blockchain settlement integration
- [ ] GraphQL API support

---

**Built with ❤️ using Java 23 and FINOS CDM**