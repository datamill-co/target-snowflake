#!/usr/bin/env bash

python -m venv venv--target-snowflake
source /code/venv--target-snowflake/bin/activate

pip install -e .[tests]

echo -e "\n\nINFO: Dev environment ready."

tail -f /dev/null
