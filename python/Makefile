SHELL := /bin/bash
PWD := $(shell pwd)

up:
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --attach tests
.PHONY: up

local:
	cd src/tests
	echo "$(PWD)"
	MOM_HOST="localhost" PYTHONPATH="$(PWD)/src" pytest
.PHONY: local

down:
	docker compose -f docker-compose.yaml stop -t 1
	docker compose -f docker-compose.yaml down
.PHONY: down

logs:
	docker compose -f docker-compose.yaml logs
.PHONY: logs

test:
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans \
		--abort-on-container-exit --exit-code-from tests --attach tests; \
	TEST_EXIT_CODE=$$?; \
	docker compose -f docker-compose.yaml down -t 5; \
	exit $$TEST_EXIT_CODE
.PHONY: test
