# dbt-airflow-template

This template provides a production-ready setup for running dbt with
Apache Airflow using Docker. It includes a complete development
environment with PostgreSQL, dbt, and Airflow configured to work
together seamlessly.

## Prerequisites

- Docker and Docker Compose
- Python 3.12 or later
- `make` command-line tool
- Git

## Quick Start

1. Create a new repository from this template
2. Clone your new repository
3. Start the development environment:
```bash
make init        # Create virtual environment and install dependencies
make docker-up   # Start all services
```
4. Access Airflow at http://localhost:18080
   - Username: `admin`
   - Password: `admin`

5. See [docs/example-workflow.md](docs/example-workflow.md) for a
   guide on running and fixing the example dbt workflow. This dbt
   example comes from `dbt init` and has an intentional validation
   error.

## Project Structure

```
dbt-airflow-template/
├── airflow/
│   ├── dags/           # Airflow DAG definitions
│   ├── dbt/           # dbt project files
│   ├── logs/          # Airflow and dbt logs
│   └── plugins/       # Airflow plugins
├── docker/            # Docker configuration files
├── docs/             # Documentation
└── Makefile          # Development automation
```

## Development Workflow

### Local Development

1. Activate the virtual environment:
```bash
source .venv/bin/activate
```

2. Make changes to your dbt models in `airflow/dbt/models/`

3. Test dbt models:
```bash
make test-dbt
```

### Docker Environment

The development environment includes:
- Airflow Webserver (http://localhost:18080)
- Airflow Scheduler
- PostgreSQL (port 15432)

Common commands:
```bash
make docker-up      # Start services
make docker-down    # Stop services
make docker-logs    # View logs
make docker-clean   # Remove containers and volumes
```

## Configuration

### Environment Variables

Create a `.env` file in the root directory to override default
settings:

```env
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
POSTGRES_HOST=postgres
```

### dbt Configuration

- dbt profiles are stored in `airflow/dbt/profiles.yml`
- Models are located in `airflow/dbt/models/`
- The default target database is the same PostgreSQL instance used by
  Airflow

### Airflow Configuration

- DAGs are stored in `airflow/dags/`
- The example DAG (`example_dbt_dag.py`) demonstrates how to:
  - Run dbt commands in Airflow
  - Handle dependencies between dbt tasks
  - Implement proper error handling

## Production Deployment

1. Build the production Docker image:
```bash
make docker-build DOCKER_REGISTRY=your-registry DOCKER_PACKAGE=your-package DOCKER_TAG=your-tag
```

2. Push the image to your registry:
```bash
make docker-push DOCKER_REGISTRY=your-registry DOCKER_PACKAGE=your-package DOCKER_TAG=your-tag
```

## Maintenance

- Logs are stored in `airflow/logs/`
- Dependencies are managed through `pyproject.toml`
- Docker requirements are automatically compiled to
  `docker/requirements.txt`

## Troubleshooting

1. If Airflow fails to start:
   - Check logs: `make docker-logs`
   - Ensure PostgreSQL is healthy: `make docker-ps`
   - Try cleaning and restarting: `make docker-clean docker-up`

2. If dbt tests fail:
   - Verify database connections in `profiles.yml`
   - Check dbt logs in `airflow/logs/`
   - Run `make test-dbt` for detailed error messages

## Contributing

1. Create a feature branch
2. Make your changes
3. Test thoroughly
4. Submit a pull request

## License

This template is distributed under the MIT license. See `LICENSE` file
for more information.
