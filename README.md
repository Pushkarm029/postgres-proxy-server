# MVP

## Steps to Run

1. **Start Postgres in Docker:**
   ```bash
   make setup-db
   ```

2. **Create and Populate Databases:**
   ```bash
   make create-db populate-schema populate-db
   ```

3. **Run the PGWire Server:**
   ```bash
   make run
   ```

4. **Connect to the Proxy Server:**
   ```bash
   psql postgres://postgres:postgres@127.0.0.1:5433/main
   ```

## Testing
   **Currently**, you have to populate db following above steps to run tests successfully.
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

- **SCHEMA_DB_CONN_STRING**: Address of the schema database (`information_schema`).
  
  Default: `postgres://postgres:postgres@localhost:5432/information_schema`

- **DATA_DB_CONN_STRING**: Address of the main database (`main`).
  
  Default: `postgres://postgres:postgres@localhost:5432/main`

- **SERVER_ADDR**: Proxy server address (`127.0.0.1:5433`).

## Makefile Commands

- `make setup-db`: Pull and run Postgres in Docker.
- `make create-db`: Create the necessary databases.
- `make populate-db`: Populate the `main` database with sample data.
- `make populate-schema`: Populate the `information_schema` with schema definitions.
- `make run-proxy`: Start the PGWire proxy server.
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
