# Postgres Proxy Server

This project is a Postgres proxy server written in Rust using the PGWire protocol. It allows you to run a lightweight server and interact with it using `psql` or any other Postgres client.

### Steps to Run:

1. **Pull Postgres on Docker:**
   ```bash
   docker pull postgres
   ```

2. **Start Postgres:**
   ```bash
   docker run --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres
   ```

3. **Run the PGWire Server:**
   ```bash
   # It will automatically create a database "new" and run functions on it
   cargo run
   ```

4. **Install Postgres Client (`psql`):**
   
   If you don’t have `psql` installed, follow the steps for your platform:

   - **Check if `psql` is already installed:**
     ```bash
     psql --version
     ```

   - **For MacOS:**
     ```bash
     brew doctor
     brew update
     brew install libpq
     brew link --force libpq
     ```

   - **For Ubuntu 23.10, 22.04, and Debian 12:**
     ```bash
     sudo apt update
     sudo apt install postgresql-client
     ```

   - **Verify Installation:**
     ```bash
     psql --version
     ```

5. **Connect to the Proxy Server:**
   
   Open a terminal and run the following command to connect to the PGWire proxy server:
   ```bash
   psql postgresql://admin:admin@127.0.0.1:5433/new
   ```

6. **Run Queries:**
   
   Once connected, you can execute SQL queries and see the output both in the `psql` client and the server logs. Example queries are provided below.

7. **Check Database State (Main Postgres):**
   ```bash
   psql postgresql://admin:admin@127.0.0.1:5432/new
   ```

### Example Queries

1. **Check the Postgres Version:**
   ```sql
   SELECT version();
   ```

2. **Create a Table:**
   Create a `users` table with `id`, `name`, and `email` columns.
   ```sql
   CREATE TABLE users (
       id SERIAL PRIMARY KEY,
       name VARCHAR(50),
       email VARCHAR(50)
   );
   ```

3. **Insert Data (WRITE Operation):**
   Insert data into the `users` table.
   ```sql
   INSERT INTO users (name, email) VALUES ('John Doe', 'john.doe@example.com');
   INSERT INTO users (name, email) VALUES ('Jane Smith', 'jane.smith@example.com');
   ```

   This will trigger a `WRITE` log warning on the server side:
   ```
   [yyyy-mm-dd HH:MM:SS WARNING] WRITE operation detected! ⚠️ Writing new data may impact database integrity if not handled carefully.
   ```

4. **Update Data (UPDATE Operation):**
   Update the `email` of a user.
   ```sql
   UPDATE users SET email = 'new.email@example.com' WHERE name = 'John Doe';
   ```

   This will trigger an `UPDATE` log warning on the server side:
   ```
   [yyyy-mm-dd HH:MM:SS WARNING] UPDATE operation detected! ⚠️ This will modify existing data.
   ```

4. **Read Data (READ Operation):**
   ```sql
   SELECT * FROM users;
   ```

### Notes:
- The server logs will display warnings when `WRITE` or `UPDATE` queries are executed, as they can potentially overwrite data.
- Logs can be viewed in the terminal where the Rust server is running.

### Troubleshooting:
- If the connection to the proxy server fails, ensure the server is running and that port `5433` is open.
- Verify that your PostgreSQL client is correctly installed by running `psql --version`.
