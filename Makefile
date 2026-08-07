.PHONY: up down test smoke demo-shift

up:
	docker compose up --build

down:
	docker compose down -v

test:
	./mvnw verify

smoke:
	./scripts/smoke-test.sh

demo-shift:
	PRODUCER_LEVEL_SHIFT_ENABLED=true docker compose up --build
