.PHONY: dockerbuild dockerpush test testonce ruff lint pre-commit-check requirements-update requirements setup
DOCKER ?= docker
IMAGENAME ?= eodhp-data-transfer-events
DOCKERREPO ?= public.ecr.aws/eodh
VERSION ?= 0.1.0

.SILENT:
MAKEFLAGS += --no-print-directory

.PHONY: container-build
container-build:
	$(DOCKER) build -t $(DOCKERREPO)/${IMAGENAME}:$(VERSION) .

.PHONY: container-push
container-push:
	$(DOCKER) push $(DOCKERREPO)/${IMAGENAME}:$(VERSION)

.PHONY: run-dev
run-dev:
	python billing_scanner/app.py

.PHONY: run
run:
	python billing_scanner/app.py

.PHONY: ruff
ruff:
	./venv/bin/ruff check .

.PHONY: fmt
fmt:
	./venv/bin/ruff check . --select I --fix
	./venv/bin/ruff format .

test:
	./venv/bin/ptw eodhp_data_transfer_events

testonce:
	./venv/bin/pytest

validate-pyproject:
	validate-pyproject pyproject.toml

lint: ruff validate-pyproject

requirements.txt: venv pyproject.toml
	./venv/bin/pip-compile

requirements-dev.txt: venv pyproject.toml
	./venv/bin/pip-compile --extra dev -o requirements-dev.txt

requirements: requirements.txt requirements-dev.txt

requirements-update: venv
	./venv/bin/pip-compile -U
	./venv/bin/pip-compile --extra dev -o requirements-dev.txt -U

venv:
	virtualenv -p python3.12 venv
	./venv/bin/pip3 install pip-tools

.make-venv-installed: venv requirements.txt requirements-dev.txt
	./venv/bin/pip3 install -r requirements.txt -r requirements-dev.txt
	touch .make-venv-installed

.git/hooks/pre-commit:
	./venv/bin/pre-commit install
	curl -o .pre-commit-config.yaml https://raw.githubusercontent.com/EO-DataHub/github-actions/main/.pre-commit-config-python.yaml

setup: venv requirements .make-venv-installed .git/hooks/pre-commit