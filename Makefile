.DEFAULT_GOAL := help
py_version ?= 3.12
PYTHON = python${py_version}

.PHONY: help
help:  ## Shows this help message
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target> <arg=value>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m  %s\033[0m\n\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ 🛠  Testing and development
.PHONY: install
install: ## Installs package with development dependencies
	uv sync --all-extras --upgrade

.PHONY: badge
badge:
	uv run genbadge coverage -i coverage.xml

.PHONY: run-tests
run-tests:
	uv run pytest -x --cov=taskiq_aio_sqs --cov-report term-missing --cov-fail-under=95 --cov-report xml:coverage.xml

.PHONY: test-only ## Run only some tests (usage: make test-only filter=test_name)
test-only: ministack-init
	uv run pytest -vk "$(filter)" || true
	$(MAKE) ministack-stop

.PHONY: test ## Run testing and coverage.
test: ministack-init run-tests ministack-stop badge ## Run testing and coverage.

.PHONY: test-ci
test-ci: run-tests ## Run testing and coverage.

.PHONY: ministack-init
ministack-init: ## Starts ministack AWS emulator
	uv run ministack &
	sleep 2
	curl -f http://localhost:4566/_ministack/health > /dev/null || (echo "MiniStack failed to start"; exit 1)

.PHONY: ministack-stop
ministack-stop: ## Stops ministack AWS emulator
	uv run ministack stop

# Backwards compatibility aliases
.PHONY: localstack-init
localstack-init: ministack-init

.PHONY: localstack-stop
localstack-stop: ministack-stop

##@ 👷 Quality
.PHONY: ruff-check
ruff-check: ## Runs ruff without fixing issues
	uv run ruff check

.PHONY: ruff-format
ruff-format: ## Runs style checkers fixing issues
	uv run ruff format; uv run ruff check --fix

.PHONY: typing
typing: ## Runs pyright static type checking
	uv run pyright taskiq_aio_sqs/

.PHONY: check
check: ruff-check typing ## Runs all quality checks without fixing issues

.PHONY: style
style: ruff-format
