# MVP

## Steps to Run

1. **Start Postgres in Docker:**
   ```bash
   make setup-postgres
   ```

2. **Create and Populate Databases:**
   ```bash
   make create-postgres populate-postgres
   ```

3. **Run the Proxy Server in Local Mode:**
   ```bash
   make local-run
   ```

4. **Connect to the Proxy Server:**
   ```bash
   psql postgres://postgres:postgres@127.0.0.1:5433/main
   ```

## Testing
   ```bash
   make test
   ```

## Example Queries

- **Check Postgres Version:**
   ```sql
   SELECT version();
   ```

- **Read Data:**
   ```sql
   SELECT * FROM employees;
   ```

- **Measure Data:**
   ```sql
   SELECT name, MEASURE(head_count) FROM employees GROUP BY name;
   -- Same as:
   SELECT name, COUNT(id) FROM employees GROUP BY name;
   ```

## Environment Variables

- **SERVER_HOST**: Host address for the server.  
  Default: `127.0.0.1`

- **SERVER_PORT**: Port number for the server.  
  Default: `5433`

- **POSTGRES_USER**: PostgreSQL username.  
  Default: `postgres`

- **POSTGRES_PASSWORD**: PostgreSQL password.  
  Default: `postgres`

- **POSTGRES_HOST**: PostgreSQL host address.  
  Default: `localhost:5432`

- **POSTGRES_DB**: PostgreSQL database name.  
  Default: `main`

- **SNOWFLAKE_ACCOUNT**: Snowflake account identifier.  
  Default: *None*

- **SNOWFLAKE_USER**: Snowflake username.  
  Default: *None*

- **SNOWFLAKE_PASSWORD**: Snowflake password.  
  Default: *None*

- **SNOWFLAKE_WAREHOUSE**: Snowflake warehouse name (optional).  
  Default: *None*

- **SNOWFLAKE_DATABASE**: Snowflake database name (optional).  
  Default: *None*

- **SNOWFLAKE_SCHEMA**: Snowflake schema name (optional).  
  Default: *None*

- **SNOWFLAKE_ROLE**: Snowflake role name (optional).  
  Default: *None*

- **SNOWFLAKE_TIMEOUT**: Timeout for Snowflake connection in seconds (optional).  
  Default: *None*

- **TENANT**: Tenant name for S3.  
  Default: *None*

- **S3_BUCKET_NAME**: Name of the S3 bucket.  
  Default: *None*

- **JSON_PATH**: Path to the JSON file for the semantic model.  
  Default: *None*

## Makefile Commands

- `make setup-postgres`: Pull and run Postgres in Docker.
- `make create-postgres`: Create the necessary databases.
- `make populate-postgres`: Populate the main database with sample data.
- `make local-run`: Start the PGWire proxy server in local mode.
- `make production-run`: Start the PGWire proxy server in production mode.
- `make test`: Run the test suite.
- `make teardown`: Stop and remove Docker containers.
- `make clean`: Stop and clean up containers and build files.

## Troubleshooting

- Ensure the proxy server is running on port `5433`.
- Ensure `psql` is installed:
```bash
   psql --version
   ```
   If not, install it:
   - **MacOS:**
     ```bash
     brew doctor
     brew update
     brew install libpq
     brew link --force libpq
     ```
   - **Ubuntu:**
     ```bash
     sudo apt update
     sudo apt install postgresql-client
     ```
   Verify installation:
   ```bash
   psql --version
   ```
