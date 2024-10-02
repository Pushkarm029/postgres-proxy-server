CARGO = cargo
PSQL = psql
DB_NAME = main
DB_USER = postgres
DB_PASSWORD = postgres
DB_HOST = localhost
DB_PORT = 5432
# Section: Postgres & Local
local-pg-test: 
	@echo "Running tests with Postgres and local storage..."
	RUST_LOG=trace \
	$(CARGO) test

local-pg-run:
	@echo "Running in production mode with Postgres and local storage..."
	RUST_LOG=info \
	$(CARGO) run

# Section: S3 & Snowflake
s3-snowflake-test:
	@echo "Running tests with S3 and Snowflake..."
	@DATA_STORE="snowflake" \
	SEMANTIC_MODEL_STORE="s3" \
	SNOWFLAKE_ACCOUNT="$(SNOWFLAKE_ACCOUNT)" \
	SNOWFLAKE_USER="$(SNOWFLAKE_USER)" \
	SNOWFLAKE_PASSWORD="$(SNOWFLAKE_PASSWORD)" \
	SNOWFLAKE_WAREHOUSE="$(SNOWFLAKE_WAREHOUSE)" \
	SNOWFLAKE_DATABASE="$(SNOWFLAKE_DATABASE)" \
	SNOWFLAKE_SCHEMA="$(SNOWFLAKE_SCHEMA)" \
	TENANT="$(TENANT)" \
	S3_BUCKET_NAME="$(S3_BUCKET_NAME)" \
	RUST_LOG=trace \
	$(CARGO) test

s3-snowflake-run:
	@echo "Running in production mode with S3 and Snowflake..."
	@DATA_STORE="snowflake" \
	SEMANTIC_MODEL_STORE="s3" \
	SNOWFLAKE_ACCOUNT="$(SNOWFLAKE_ACCOUNT)" \
	SNOWFLAKE_USER="$(SNOWFLAKE_USER)" \
	SNOWFLAKE_PASSWORD="$(SNOWFLAKE_PASSWORD)" \
	SNOWFLAKE_WAREHOUSE="$(SNOWFLAKE_WAREHOUSE)" \
	SNOWFLAKE_DATABASE="$(SNOWFLAKE_DATABASE)" \
	SNOWFLAKE_SCHEMA="$(SNOWFLAKE_SCHEMA)" \
	TENANT="$(TENANT)" \
	S3_BUCKET_NAME="$(S3_BUCKET_NAME)" \
	RUST_LOG=info \
	$(CARGO) run

# Shared commands for both configurations
setup-db:
	@echo "Starting Postgres container..."
	@docker run --name postgres -e POSTGRES_PASSWORD=$(DB_PASSWORD) -p $(DB_PORT):$(DB_PORT) -d postgres
	@echo "Waiting for Postgres to be ready..."
	@sleep 2

create-db:
	@echo "Creating databases $(DB_NAME) and $(SCHEMA_DB_NAME)..."
	@$(PSQL) postgres://$(DB_USER):$(DB_PASSWORD)@127.0.0.1:$(DB_PORT) -c "CREATE DATABASE $(DB_NAME)"

populate-db:
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

.PHONY: local-pg-test local-pg-run s3-snowflake-test s3-snowflake-run setup-db create-db populate-db populate-schema teardown clean
