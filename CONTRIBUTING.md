# Contributing to Equity Swaps Microservices Platform

Thank you for your interest in contributing to the Equity Swaps Microservices Platform! This document provides guidelines and instructions for contributing to this project.

## 🤝 Code of Conduct

This project adheres to a code of conduct. By participating, you are expected to uphold this code:

- Be respectful and inclusive
- Focus on constructive feedback
- Help others learn and grow
- Maintain professionalism in all interactions

## 🚀 Getting Started

### Prerequisites

Before contributing, ensure you have:

- **Java 23** or higher (OpenJDK recommended)
- **Maven 3.9+** for build management
- **Docker** and **Docker Compose** for local development
- **Git** for version control
- An IDE with Java 23 support (IntelliJ IDEA, Eclipse, or VS Code)

### Setting Up Development Environment

1. **Fork and clone the repository**
   ```bash
   git clone https://github.com/your-username/equity-swaps-microservices.git
   cd equity-swaps-microservices
   ```

2. **Run the setup script**
   ```bash
   ./scripts/setup-development.sh
   ```

3. **Start infrastructure services**
   ```bash
   docker-compose up -d postgres redis kafka zookeeper
   ```

4. **Build the project**
   ```bash
   mvn clean compile
   ```

## 🏗️ Project Structure

```
equity-swaps-microservices/
├── common/                    # Shared libraries and utilities
├── position-service/          # Position management microservice
├── lot-service/              # Lot management microservice
├── cashflow-service/         # Cashflow generation engine
├── cdm-processor/            # CDM event processing
├── k8s/                      # Kubernetes deployment manifests
├── scripts/                  # Development and deployment scripts
├── docker/                   # Docker configuration files
└── docs/                     # Documentation
```

## 🔄 Development Workflow

### Branching Strategy

We use **GitFlow** with the following branches:

- `main` - Production-ready code
- `develop` - Integration branch for features
- `feature/*` - New features and enhancements
- `bugfix/*` - Bug fixes
- `hotfix/*` - Critical production fixes
- `release/*` - Release preparation

### Making Changes

1. **Create a feature branch**
   ```bash
   git checkout develop
   git pull origin develop
   git checkout -b feature/your-feature-name
   ```

2. **Follow coding standards**
   - Use Java 23 features appropriately (Records, Pattern Matching, Virtual Threads)
   - Follow Google Java Style Guide
   - Maintain CDM compliance
   - Write comprehensive tests

3. **Commit your changes**
   ```bash
   git add .
   git commit -m "feat: add position aggregation functionality

   - Implement real-time position aggregation
   - Add CDM event processing for position updates
   - Include comprehensive unit tests
   - Update API documentation

   Closes #123"
   ```

### Commit Message Format

We follow the **Conventional Commits** specification:

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

**Examples:**
```bash
feat(position): add real-time P&L calculation
fix(lot): correct FIFO allocation algorithm
docs(api): update swagger documentation
test(cashflow): add integration tests for reset events
```

## 🧪 Testing

### Running Tests

```bash
# Unit tests
mvn test

# Integration tests
mvn verify -P integration-tests

# All tests with coverage
mvn clean verify
```

### Writing Tests

- **Unit Tests**: Test individual components in isolation
- **Integration Tests**: Test component interactions
- **Contract Tests**: Verify API contracts
- **End-to-End Tests**: Test complete user scenarios

#### Test Structure

```java
@ExtendWith(MockitoExtension.class)
class PositionServiceTest {
    
    @Mock
    private PositionRepository positionRepository;
    
    @InjectMocks
    private PositionService positionService;
    
    @Test
    @DisplayName("Should update position when valid trade is provided")
    void shouldUpdatePositionWhenValidTrade() {
        // Given
        var trade = createSampleTrade();
        
        // When
        var result = positionService.updatePosition(trade);
        
        // Then
        assertThat(result).isNotNull();
        assertThat(result.quantity()).isEqualTo(BigDecimal.valueOf(100));
    }
}
```

### CDM Compliance Testing

Ensure all CDM-related functionality is tested:

```java
@Test
void shouldGenerateCDMCompliantBusinessEvent() {
    // Test CDM business event generation
    var businessEvent = cdmProcessor.processExecution(trade);
    
    assertThat(businessEvent.eventIdentifier()).isNotNull();
    assertThat(businessEvent.primitives()).isNotEmpty();
    // Validate CDM structure compliance
}
```

## 📚 Documentation

### Code Documentation

- Use JavaDoc for all public APIs
- Include examples in documentation
- Document CDM alignment and business logic

```java
/**
 * Calculates the unrealized P&L for a position using CDM EquityPerformance function.
 *
 * @param position The current position to calculate P&L for
 * @param currentPrice The current market price of the instrument
 * @return Money representing the unrealized P&L
 * @throws IllegalArgumentException if position or currentPrice is null
 * 
 * @see CDMFunctions#equityPerformance(LocalDate, Money, BigDecimal)
 */
public Money calculateUnrealizedPnL(Position position, BigDecimal currentPrice) {
    // Implementation
}
```

### API Documentation

- Update OpenAPI specifications for API changes
- Include request/response examples
- Document error conditions and status codes

### Architecture Documentation

- Update architecture diagrams for significant changes
- Document CDM integration patterns
- Maintain deployment guides

## 🔒 Security

### Security Guidelines

- Never commit sensitive information (passwords, keys, tokens)
- Use environment variables for configuration
- Follow OWASP security practices
- Scan dependencies for vulnerabilities

### Security Testing

```bash
# Run OWASP dependency check
mvn org.owasp:dependency-check-maven:check

# Run Trivy security scan
trivy fs .
```

## 🏷️ Code Style

### Java Coding Standards

- Follow Google Java Style Guide
- Use meaningful variable and method names
- Keep methods focused and small
- Prefer composition over inheritance
- Use Java 23 features appropriately

### Formatting

```bash
# Format code using Maven
mvn spotless:apply

# Check formatting
mvn spotless:check
```

### Static Analysis

```bash
# Run SpotBugs
mvn spotbugs:check

# Run PMD
mvn pmd:check

# Run Checkstyle
mvn checkstyle:check
```

## 🚀 Performance

### Performance Guidelines

- Leverage Virtual Threads for I/O-bound operations
- Use reactive programming patterns where appropriate
- Implement proper caching strategies
- Monitor and optimize database queries

### Performance Testing

```bash
# Run JMH benchmarks
mvn exec:java -Dexec.mainClass="org.openjdk.jmh.Main"

# Load testing with custom scripts
./scripts/load-test.sh
```

## 🐛 Bug Reports

When reporting bugs, please include:

1. **Environment details** (Java version, OS, etc.)
2. **Steps to reproduce** the issue
3. **Expected behavior** vs **actual behavior**
4. **Error messages** and stack traces
5. **Relevant logs** and configuration

### Bug Report Template

```markdown
## Bug Description
Brief description of the bug.

## Environment
- Java Version: 23
- Maven Version: 3.9.x
- OS: Ubuntu 22.04
- Docker Version: 24.x

## Steps to Reproduce
1. Start the application
2. Send POST request to /api/v1/positions/update
3. Observe the error

## Expected Behavior
The position should be updated successfully.

## Actual Behavior
500 Internal Server Error is returned.

## Logs
```
[ERROR] 2024-01-15 10:30:00 - PositionService: Failed to update position
java.lang.NullPointerException: ...
```

## Additional Context
Any additional information that might be helpful.
```

## 💡 Feature Requests

When requesting features:

1. **Describe the use case** and business value
2. **Provide examples** of how it would be used
3. **Consider CDM alignment** and compliance
4. **Discuss implementation approach** if you have ideas

## 📝 Pull Request Process

### Before Submitting

1. **Update documentation** for any API changes
2. **Add or update tests** for new functionality
3. **Run the full test suite** locally
4. **Check code quality** with static analysis tools
5. **Ensure CDM compliance** for financial logic

### Pull Request Template

```markdown
## Description
Brief description of changes made.

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## CDM Compliance
- [ ] Changes align with FINOS CDM standards
- [ ] CDM business events are properly generated
- [ ] Financial calculations use CDM functions

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] Manual testing completed

## Checklist
- [ ] Code follows project style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] No breaking changes (or clearly documented)
```

### Review Process

1. **Automated checks** must pass (CI/CD pipeline)
2. **Code review** by at least one maintainer
3. **Security review** for security-related changes
4. **CDM compliance review** for financial logic
5. **Performance review** for performance-critical changes

## 🎯 Areas for Contribution

### High Priority
- CDM function implementations
- Performance optimizations
- Security enhancements
- Test coverage improvements

### Medium Priority
- API enhancements
- Documentation improvements
- Developer tooling
- Monitoring and observability

### Good First Issues
- Bug fixes with clear reproduction steps
- Documentation updates
- Code formatting improvements
- Test additions

## 🏆 Recognition

Contributors will be recognized in:
- Project README
- Release notes
- Contributors page
- Community acknowledgments

## 📞 Getting Help

- **GitHub Discussions**: For general questions and discussions
- **GitHub Issues**: For bug reports and feature requests
- **Slack Channel**: `#equity-swaps-dev` for real-time discussions
- **Office Hours**: Bi-weekly contributor meetings (see calendar)

## 📜 License

By contributing to this project, you agree that your contributions will be licensed under the Apache License 2.0.

---

Thank you for contributing to the Equity Swaps Microservices Platform! 🚀