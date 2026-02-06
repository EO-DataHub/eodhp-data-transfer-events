# UK EO Data Hub Platform: Data transfer billing events

This component collects billing events for CloudFront-based data transfer by workspaces. It reads CloudFront access logs from S3, aggregates bytes transferred per workspace and egress category (in-region, inter-region, internet), and publishes a single BillingEvent per group to a configured Pulsar topic.

# Development of this component

## Getting started

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

### Install via makefile

```commandline
make setup
```

This will install dependencies via `uv sync` and set up `pre-commit` hooks.

It's safe and fast to run `make setup` repeatedly as it will only update things if they have changed.

After `make setup` you can run `make pre-commit` to run pre-commit checks on staged changes and
`make pre-commit-all` to run them on all files.

## Building and testing

This component uses `pytest` for tests, `ruff` for linting and formatting, and `pyright` for type checking.

A number of `make` targets are defined:
* `make test`: run tests continuously (via `pytest-watcher`)
* `make testonce`: run tests once
* `make check`: run all linting, formatting, and type checks
* `make format`: auto-fix linting issues and reformat code
* `make dockerbuild`: build a `latest` Docker image (use `make dockerbuild VERSION=1.2.3` for a release image)
* `make dockerpush`: push a Docker image - normally this should be done only via the build system and its GitHub actions
* `make run`: run the billing scanner locally

## Managing requirements

Dependencies are specified in `pyproject.toml`. To add or update dependencies:

* Edit `pyproject.toml` to add/modify dependencies
* Run `uv sync` (or `make update`) to update the lockfile and install
* Commit both `pyproject.toml` and `uv.lock`

## Releasing

Ensure that `make check` and `make testonce` work correctly and produce no errors before continuing.

Releases tagged `latest` and targeted at development environments can be created from the `main` branch. Releases for
installation in non-development environments should be created from a Git tag named using semantic versioning. For
example, using

* `git tag v1.2.3`
* `git push --tags`

Docker images will be built automatically after pushing to the EO-DataHub repos via GitHub Actions.
