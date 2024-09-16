.DEFAULT_GOAL := help
py_version ?= 3.11
PYTHON = python${py_version}

.PHONY: help
help:  ## Shows this help message
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target> <arg=value>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m  %s\033[0m\n\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ 🛠  Testing and development
.PHONY: install
install: ## Installs package with development dependencies
	$(PYTHON) -m pip install .[dev]

.PHONY: badge
badge:
	genbadge coverage -i coverage.xml

.PHONY: run-tests
run-tests:
	$(PYTHON) -m pytest --cov=taskiq_aio_sqs --cov-report term-missing --cov-fail-under=95 --cov-report xml:coverage.xml

.PHONY: test
test: localstack-init run-tests localstack-stop badge ## Run testing and coverage.

.PHONY: test-ci
test: localstack-init run-tests localstack-stop ## Run testing and coverage.

.PHONY: localstack-init
localstack-init: ## Starts localstack with init script
	SQS_ENABLE_MESSAGE_RETENTION_PERIOD=1 localstack start -d --no-banner; localstack wait -t 45

.PHONY: localstack-stop
localstack-stop: ## Starts localstack with init script
	localstack stop

##@ 👷 Quality
.PHONY: ruff-check
ruff-check: ## Runs ruff without fixing issues
	$(PYTHON) -m ruff check

.PHONY: ruff-format
ruff-format: ## Runs style checkers fixing issues
	$(PYTHON) -m ruff format; $(PYTHON) -m ruff check --fix

.PHONY: typing
typing: ## Runs pyright static type checking
	$(PYTHON) -m pyright taskiq_aio_sqs/

.PHONY: check
check: ruff-check typing ## Runs all quality checks without fixing issues

.PHONY: style
style: ruff-format
