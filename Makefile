CARGO = cargo
PSQL = psql
DB_NAME = main
SCHEMA_DB_NAME = information_schema
DB_USER = postgres
DB_PASSWORD = postgres
DB_HOST = localhost
DB_PORT = 5432

test: 	
	@echo "Running tests..."
	@DB_ADDRESS="postgres://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/test_db" \
	SCHEMA_DB_ADDRESS="postgres://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/test_schema" \
	SCHEMA_TABLE_NAME="measures" \
	RUST_LOG=trace \
	$(CARGO) test

run: 	
	@echo "Running in prod mode..."
	@DB_ADDRESS="postgres://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/$(DB_NAME)" \
	SCHEMA_DB_ADDRESS="postgres://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/$(SCHEMA_DB_NAME)" \
	SCHEMA_TABLE_NAME="measures" \
	RUST_LOG=info \
	$(CARGO) run

debug-run: 	
	@echo "Running in debug mode..."
	@DB_ADDRESS="postgres://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/test_db" \
	SCHEMA_DB_ADDRESS="postgres://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/test_schema" \
	SCHEMA_TABLE_NAME="measures" \
	RUST_LOG=info \
	$(CARGO) run

setup-db:
	@echo "Starting Postgres container..."
	@docker run --name postgres -e POSTGRES_PASSWORD=$(DB_PASSWORD) -p $(DB_PORT):$(DB_PORT) -d postgres
	@echo "Waiting for Postgres to be ready..."
	@sleep 2

create-db:
	@echo "Creating databases $(DB_NAME) and $(SCHEMA_DB_NAME)..."
	@$(PSQL) postgres://$(DB_USER):$(DB_PASSWORD)@127.0.0.1:$(DB_PORT) -c "CREATE DATABASE $(DB_NAME)"
	@$(PSQL) postgres://$(DB_USER):$(DB_PASSWORD)@127.0.0.1:$(DB_PORT) -c "CREATE DATABASE $(SCHEMA_DB_NAME)"

populate-db:
	@echo "Populating main database..."
	@$(PSQL) postgres://$(DB_USER):$(DB_PASSWORD)@127.0.0.1:$(DB_PORT)/$(DB_NAME) -f ./scripts/populate_db.sql

populate-schema:
	@echo "Populating schema database..."
	@$(PSQL) postgres://$(DB_USER):$(DB_PASSWORD)@127.0.0.1:$(DB_PORT)/$(SCHEMA_DB_NAME) -f ./scripts/populate_schema.sql

teardown:
	@echo "Stopping and removing containers..."
	@docker stop postgres
	@docker rm postgres

clean:
	@echo "Cleaning up..."
	@docker stop postgres
	@docker rm postgres
	@rm -rf target

.PHONY: run test setup teardown run-proxy populate-db populate-schema clean
