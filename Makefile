.PHONY: init docker-up docker-down docker-logs docker-build docker-push test-dbt clean

# Load environment variables if .env exists
ifneq (,$(wildcard .env))
    include .env
    export
endif

# Virtual environment settings
VENV_NAME := .venv
PYTHON := $(VENV_NAME)/bin/python
UV := $(VENV_NAME)/bin/uv
DBT := $(VENV_NAME)/bin/dbt
DOCKER := BUILDKIT_PROGRESS=plain docker
DOCKER_COMPOSE := $(DOCKER) compose --progress=plain -f docker/docker-compose.yml
PACKAGE ?= $(if $(DOCKER_PACKAGE),$(DOCKER_PACKAGE),dbt-airflow)
TAG ?= $(if $(DOCKER_TAG),$(DOCKER_TAG),latest)
REGISTRY ?= $(if $(DOCKER_REGISTRY),$(DOCKER_REGISTRY)/,)

init:
	python -m venv $(VENV_NAME)
	$(PYTHON) -m pip install --quiet uv
	$(UV) pip install --no-progress -e .
	mkdir -p airflow/logs

# Even though this is built we don't clean it
# and we commit as pinned Dockerfile config
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
