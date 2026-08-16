---
rules:
  - "**/*.py" => uv run --extra dev ruff check . && uv run python -W error::ResourceWarning -m unittest discover -s tests -q
  - pyproject.toml => uv run --extra dev ruff check . && uv run python -W error::ResourceWarning -m unittest discover -s tests -q
  - uv.lock => uv run --extra dev ruff check . && uv run python -W error::ResourceWarning -m unittest discover -s tests -q
  - deploy/compose.yml => docker compose -f deploy/compose.yml config --quiet
  - deploy/Dockerfile => docker compose -f deploy/compose.yml config --quiet
  - deploy/nats-stream.json => uv run python -m json.tool deploy/nats-stream.json > /dev/null
  - schemas/*.json => uv run python -m json.tool schemas/events-v1.json > /dev/null && uv run python -m json.tool schemas/events-v2.json > /dev/null
---
Fast deterministic pre-commit gates. The broker-loss acceptance remains a CI
gate because it builds containers and intentionally stops a broker.
