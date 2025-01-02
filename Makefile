.PHONY: init docker-up docker-down docker-logs docker-build docker-push test-dbt clean

# Load environment variables if .env exists
ifneq (,$(wildcard .env))
    include .env
    export
endif

# Virtual environment settings
VENV := .venv
PYTHON := $(VENV)/bin/python
UV := $(VENV)/bin/uv
PRE_COMMIT := $(VENV)/bin/pre-commit
DBT := $(VENV)/bin/dbt

# Docker settings
DOCKER := BUILDKIT_PROGRESS=plain docker
UID := $(shell id -u)
DOCKER_COMPOSE := UID=$(UID) $(DOCKER) compose --ansi never --progress=plain -f docker/docker-compose.yml
PACKAGE ?= $(if $(DOCKER_PACKAGE),$(DOCKER_PACKAGE),dbt-airflow)
TAG ?= $(if $(DOCKER_TAG),$(DOCKER_TAG),latest)
REGISTRY ?= $(if $(DOCKER_REGISTRY),$(DOCKER_REGISTRY)/,)

$(VENV)/bin/uv:
	python -m venv $(VENV)
	$(PYTHON) -m pip install --quiet uv

install: $(VENV)/bin/uv
	$(UV) pip install --quiet --editable .
	$(UV) pip install --quiet pre-commit
	$(PRE_COMMIT) install
	mkdir -p airflow/logs

install-dev: install
	$(UV) pip install --quiet --editable ".[dev]"

install-emacs: install-dev
	$(UV) pip install --quiet --editable ".[emacs]"

pre-commit: $(VENV)/bin/pre-commit
	$(PRE_COMMIT) run --all-files

pre-commit-upgrade: $(VENV)/bin/pre-commit
	$(PRE_COMMIT) autoupdate

# Although docker/requirements.txt is generated we commit
# it as a locked pip install for the container
docker/requirements.txt: pyproject.toml
	$(UV) pip compile -q pyproject.toml -o $@

docker-up: docker/requirements.txt
	$(DOCKER_COMPOSE) up -d

docker-down:
	$(DOCKER_COMPOSE) down

docker-ps:
	$(DOCKER_COMPOSE) ps

docker-logs:
	$(DOCKER_COMPOSE) logs --no-color -f

docker-clean:
	$(DOCKER_COMPOSE) down -v

docker-build: docker/requirements.txt
	$(DOCKER) build \
	    -f docker/Dockerfile \
	    --target prod \
	    --platform linux/amd64 \
	    -t $(REGISTRY)$(PACKAGE):$(TAG) \
	    .

docker-push: build
	docker push $(REGISTRY)$(PACKAGE):$(TAG)

test-dbt:
	cd airflow/dbt && ../../$(DBT) test

clean: docker-clean
	rm -rf .venv
	rm -rf airflow/logs
