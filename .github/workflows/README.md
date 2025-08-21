# GitHub Actions Workflows

This directory contains GitHub Actions workflows for CI/CD automation.

## Workflow Structure

### Main Workflow: `push_pr.yml`
- **Trigger**: Pull requests and pushes to main/release branches
- **Purpose**: Main CI workflow that orchestrates all tests
- **Features**:
  - Concurrency control to cancel in-progress runs
  - Path filtering to skip documentation changes
  - Uses matrix strategy for parallel testing

### Matrix Test Workflow: `test_matrix.yml`
- **Purpose**: Unified testing workflow for all data sources
- **Strategy**: Matrix strategy to run tests in parallel
- **Supported Data Sources**:
  - Doris (with disk space optimization)
  - StarRocks
  - ClickHouse

## Matrix Configuration

The matrix strategy provides the following benefits:

### Parallel Execution
- All three data source tests run in parallel
- Reduces total CI time significantly
- Independent failure handling with `fail-fast: false`

### Configurable Parameters
Each data source has specific configurations:

| Data Source | Test Class | Free Disk | Timeout | Notes |
|-------------|------------|-----------|---------|-------|
| Doris | `Doris2OBTest` | ✅ | 30 min | Requires disk cleanup |
| StarRocks | `StarRocks2OBTest` | ❌ | 20 min | Standard test |
| ClickHouse | `ClickHouse2OBTest` | ❌ | 20 min | Standard test |

### Features
- **Caching**: Maven packages are cached for faster builds
- **Artifacts**: Test results are uploaded as artifacts
- **Timeout**: Configurable timeouts per data source
- **Memory**: Optimized JVM memory settings
- **Error Handling**: Graceful failure handling

## Usage

### Local Development
```bash
# Run specific test
mvn test -Dtest=com.oceanbase.omt.Doris2OBTest

# Run all tests
mvn test
```

### CI/CD
The workflows automatically run on:
- Pull requests (excluding docs changes)
- Pushes to main branch
- Pushes to release-* branches

### Manual Trigger
You can manually trigger the workflow with custom parameters:
```yaml
# Example: Run with custom Maven options
test_matrix:
  uses: ./.github/workflows/test_matrix.yml
  with:
    module: flink-omt
    maven_opts: "-DskipIntegrationTests=false"
```

## Benefits of Matrix Strategy

1. **Simplified Maintenance**: Single workflow file instead of three
2. **Parallel Execution**: Faster CI completion
3. **Consistent Configuration**: Shared setup and teardown
4. **Better Error Handling**: Independent failure management
5. **Scalability**: Easy to add new data sources
6. **Resource Optimization**: Conditional disk cleanup only for Doris

## Future Enhancements

- Add support for more data sources
- Implement test result aggregation
- Add performance benchmarking
- Support for different Java versions
- Integration with external test environments
