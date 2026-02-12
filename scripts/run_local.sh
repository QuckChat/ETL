#!/usr/bin/env bash
set -euo pipefail

export PYTHONPATH="$(pwd)/src:${PYTHONPATH:-}"
python -m pipeline.job --config conf/pipeline.yml --source sample
python -m pipeline.job --config conf/pipeline.yml
