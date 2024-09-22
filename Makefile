CARGO = cargo
PSQL = psql
DB_NAME = test_db
DB_USER = postgres
DB_PASSWORD = postgres
DB_HOST = localhost
DB_PORT = 5432

# setup-db
# 

# pub const SCHEMA_DB_ADDRESS: &str =
#     "postgres://postgres:postgres@localhost:5432/information_schema";
# pub const SCHEMA_TABLE_NAME: &str = "measures";
# pub const DB_ADDRESS: &str = "postgres://postgres:postgres@localhost:5432/main";
# pub const SERVER_ADDR: &str = "127.0.0.1:5433";

test: 	
	@echo "Running tests..."
	@DB_ADDRESS="postgres://postgres:postgres@localhost:5432/test_db" \
	SCHEMA_DB_ADDRESS="postgres://postgres:postgres@localhost:5432/test_schema" \
	SCHEMA_TABLE_NAME="measures" \
	RUST_LOG=info \
	RUST_BACKTRACE=1 \
	$(CARGO) test

setup-db:
	@echo "Starting Postgres container..."
	@docker run --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres
	@echo "Waiting for Postgres to be ready..."
	@sleep 2

.PHONY: all test setup teardown run-proxy populate-db run-tests clean

# test: setup create-db populate-db run-proxy run-tests

setup:
	@echo "Starting Postgres container..."
	@docker run --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres
	@echo "Waiting for Postgres to be ready..."
	@sleep 2  # Give Postgres some time to start

run-proxy:
	@echo "Building and running the proxy server..."
	@$(CARGO) run &
	@sleep 2  # Give the proxy some time to start

create-db:
	@echo "Creating the database..."
	@$(PSQL) postgres://postgres:postgres@127.0.0.1:5432 -c "CREATE DATABASE $(DB_NAME)"

populate-db:
	@echo "Populating the database with sample data..."
	@$(PSQL) postgres://postgres:postgres@127.0.0.1:5432/$(DB_NAME) -f ./scripts/populate_db.sql

run-tests:
	@echo "Running tests..."
	@./scripts/run_tests.sh

teardown:
	@echo "Stopping and removing containers..."
	@$(DOCKER_COMPOSE) down

clean:
	@echo "Cleaning up..."
	@$(DOCKER_COMPOSE) down -v
	@rm -rf target

populate-schema:
	@echo "Populating the database with schema..."
	@$(PSQL) postgres://postgres:postgres@127.0.0.1:5432 -f ./scripts/populate_schema.sql