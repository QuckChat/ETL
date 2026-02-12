$ErrorActionPreference = 'Stop'

Write-Host 'Running source diagnostics...'
python scripts/doctor.py --fix

Write-Host 'Running ETL with sample source...'
python -m pipeline.job --config conf/pipeline.yml --source sample
