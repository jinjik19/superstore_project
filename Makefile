.PHONY: lint type-check format check up down restart logs

lint:
	poetry run ruff check src dags

format:
	poetry run ruff format src dags

type-check:
	poetry run mypy src dags

check: lint type-check

up:
	docker-compose up -d

down:
	docker-compose down

restart:
	docker-compose down && docker-compose up -d

logs:
	docker-compose logs -f airflow-scheduler
