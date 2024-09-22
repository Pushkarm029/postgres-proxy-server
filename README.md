# MVP

## Steps to Run

1. **Pull and Start Postgres on Docker:**
   ```bash
   docker pull postgres
   docker run --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres
   ```

2. **Install Postgres Client (`psql`):**
   Check if `psql` is installed:
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

3. **Create and Populate Databases:**
   - Connect to Postgres:
     ```bash
     psql postgres://postgres:postgres@127.0.0.1:5432
     ```
   - Create and populate `information_schema`:
     ```sql
     CREATE DATABASE information_schema;
     \c information_schema
     CREATE TABLE IF NOT EXISTS measures (name TEXT PRIMARY KEY, query TEXT NOT NULL);
     INSERT INTO measures (name, query) VALUES ('head_count', 'COUNT(id)'), ('revenue', 'SUM(amount)'), ('average_salary', 'AVG(salary)');
     ```
   - Create and populate `main`:
     ```sql
      CREATE DATABASE main;
      \c main
      CREATE TABLE IF NOT EXISTS employees (
      id SERIAL PRIMARY KEY,
      name VARCHAR(50),
      salary DECIMAL(10, 2)
      );

      INSERT INTO employees (name, salary) VALUES 
      ('Alice Johnson', 70000.00), 
      ('Bob Smith', 60000.00), 
      ('Charlie Brown', 80000.00), 
      ('Diana Prince', 90000.00), 
      ('Ethan Hunt', 55000.00);
     ```

     exit the psql

4. **Run the PGWire Server:**
   ```bash
   cargo run
   ```

5. **Connect to the Proxy Server:**
   ```bash
   psql postgres://postgres:postgres@127.0.0.1:5433/main
   ```

## Example Queries
- **Check Postgres Version:**
   ```sql
   SELECT version();
   ```
- **Create Employees Table:**
   ```sql
   CREATE TABLE employees (id SERIAL PRIMARY KEY, name VARCHAR(50), email VARCHAR(50));
   ```
- **Insert Data:**: **NOT ALLOWED**
- **Update Data:**: **NOT ALLOWED**
- **Read Data:**
   ```sql
   SELECT * FROM employees;
   ```
- **Measure Data:**
   ```sql
   SELECT name, MEASURE(head_count) from employees GROUP BY name;
   -- it should work same as
   SELECT name, COUNT(id) FROM employees GROUP BY name;
   ```
## Logging
- RUST_LOG=INFO
https://docs.rs/env_logger/latest/env_logger/

## Environment Variables
- **SCHEMA_DB_ADDRESS**: The address of the database containing the schema-related information.

   `Default: postgres://postgres:postgres@localhost:5432/information_schema`

- **SCHEMA_TABLE_NAME**: The table name where the measure definitions are stored (e.g., head_count, revenue).
   
   `Default: measures`

- **DB_ADDRESS**: The address of the primary database that stores application-specific data (e.g., employee records).

   `Default: postgres://postgres:postgres@localhost:5432/main`

- **SERVER_ADDR**: The address the proxy server listens on for incoming connections.

   `Default: 127.0.0.1:5433`

## Notes
- Check logs in the terminal where the Rust server is running.

## Troubleshooting
- Ensure the proxy server is running and port `5433` is open.
- Verify `psql` installation with `psql --version`.
