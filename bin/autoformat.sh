#!/bin/bash -xe

PROJ_ROOT="$(dirname "${BASH_SOURCE[0]}")/.."
cd "${PROJ_ROOT}"

pdm run ruff check --fix ./src ./tests ./docs
pdm run ruff format ./src ./tests ./docs
pdm run pyright