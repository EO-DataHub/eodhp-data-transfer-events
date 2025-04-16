.PHONY: dockerbuild dockerpush test testonce ruff lint pre-commit-check requirements-update requirements setup
DOCKER ?= docker
IMAGENAME ?= eodhp-accounting-cloudfront
DOCKERREPO ?= public.ecr.aws/eodh
VERSION ?= latest

.SILENT:
MAKEFLAGS += --no-print-directory

.PHONY: dockerbuild
dockerbuild:
	$(DOCKER) build -t $(DOCKERREPO)/${IMAGENAME}:$(VERSION) .

.PHONY: dockerpush
dockerpush:
	$(DOCKER) push $(DOCKERREPO)/${IMAGENAME}:$(VERSION)

.PHONY: run-dev
run-dev:
	PYTHONPATH=. ./venv/bin/python billing_scanner/__main__.py

.PHONY: run
run:
	PYTHONPATH=. ./venv/bin/python billing_scanner/__main__.py 
.PHONY: ruff
ruff:
	./venv/bin/ruff check .

.PHONY: fmt
fmt:
	./venv/bin/ruff check . --select I --fix
	./venv/bin/ruff format .

test:
	./venv/bin/ptw tests 

testonce:
	./venv/bin/pytest

validate-pyproject:
	validate-pyproject pyproject.toml

lint: ruff black isort validate-pyproject

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
	./venv/bin/python -m ensurepip -U
	./venv/bin/pip3 install pip-tools

.make-venv-installed: venv requirements.txt requirements-dev.txt
	./venv/bin/pip3 install -r requirements.txt -r requirements-dev.txt
	touch .make-venv-installed

.git/hooks/pre-commit:
	./venv/bin/pre-commit install
	curl -o .pre-commit-config.yaml https://raw.githubusercontent.com/EO-DataHub/github-actions/main/.pre-commit-config-python.yaml

setup: venv requirements .make-venv-installed .git/hooks/pre-commit