CARGO = cargo
PSQL = psql
DB_NAME = main
DB_USER = postgres
DB_PASSWORD = postgres
DB_HOST = localhost
DB_PORT = 5432

# Section: Postgres & Local
test: 
	@echo "Running tests with Postgres and local storage..."
	RUST_LOG=trace \
	$(CARGO) test

local-run:
	@echo "Running in local mode with Postgres and Local Semantic Store"
	RUST_LOG=trace \
	$(CARGO) run --bin local

production-run:
	@echo "Running in production mode with Snowflake and S3 Semantic Store"
	RUST_LOG=info \
	$(CARGO) run --bin production

# Shared commands for both configurations
setup-postgres:
	@echo "Starting Postgres container..."
	@docker run --name postgres -e POSTGRES_PASSWORD=$(DB_PASSWORD) -p $(DB_PORT):$(DB_PORT) -d postgres
	@echo "Waiting for Postgres to be ready..."
	@sleep 2

create-postgres:
	@echo "Creating databases $(DB_NAME) and $(SCHEMA_DB_NAME)..."
	@$(PSQL) postgres://$(DB_USER):$(DB_PASSWORD)@127.0.0.1:$(DB_PORT) -c "CREATE DATABASE $(DB_NAME)"

populate-postgres:
	@echo "Populating main database..."
	@$(PSQL) postgres://$(DB_USER):$(DB_PASSWORD)@127.0.0.1:$(DB_PORT)/$(DB_NAME) -f ./scripts/populate_db.sql

teardown:
	@echo "Stopping and removing containers..."
	@docker stop postgres
	@docker rm postgres

clean:
	@echo "Cleaning up..."
	@docker stop postgres
	@docker rm postgres
	@rm -rf target

.PHONY: test local-run production-run setup-postgres create-postgres populate-postgres teardown clean
