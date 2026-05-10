.PHONY: build up down restart logs test clean

build:
	docker-compose build

up:
	docker-compose up -d

down:
	docker-compose down

restart: down up

logs:
	docker-compose logs -f

logs-coordinator:
	docker-compose logs -f coordinator

logs-dashboard:
	docker-compose logs -f dashboard

test:
	PYTHONPATH=. python3 tests/test_state.py
	PYTHONPATH=. python3 tests/test_participant_state.py

clean:
	docker-compose down -v --rmi all --remove-orphans
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -name "*.pyc" -delete
	rm -f data/*.db
