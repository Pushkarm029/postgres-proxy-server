# Postgres Proxy Server

This project is a Postgres proxy server written in Rust using the PGWire protocol. It allows you to run a lightweight server and interact with it using `psql` or any other Postgres client.

### Steps to Run:

1. **Run the PGWire Server**
   ```bash
   cargo run
   ```

2. **Install Postgres Client (`psql`)**
   
   If you don’t have `psql` installed, follow these steps for your platform:

   #### Check if `psql` is already installed:
   ```bash
   psql --version
   ```

   #### For MacOS:
   ```bash
   brew doctor
   brew update
   brew install libpq
   brew link --force libpq
   ```

   #### For Ubuntu 23.10, 22.04, and Debian 12:
   ```bash
   sudo apt update
   sudo apt install postgresql-client
   ```

   #### Verify Installation:
   ```bash
   psql --version
   ```

3. **Connect to the Proxy Server**

   Open a terminal and run the following command to connect to the PGWire proxy server:
   ```bash
   psql postgresql://admin:admin@127.0.0.1:5433/new
   ```

4. **Run Queries**

   Once connected, you can execute SQL queries and see the output both in the `psql` client and the server logs. Here are some example queries to get started.

### Example Queries

1. **Check the Postgres version:**
   ```sql
   SELECT version();
   ```

2. **Create a Table:**
   Create a table called `users` with `id`, `name`, and `email` columns.
   ```sql
   CREATE TABLE users (
       id SERIAL PRIMARY KEY,
       name VARCHAR(50),
       email VARCHAR(50)
   );
   ```

3. **Insert Data (WRITE operation):**
   Insert data into the `users` table.
   ```sql
   INSERT INTO users (name, email) VALUES ('John Doe', 'john.doe@example.com');
   INSERT INTO users (name, email) VALUES ('Jane Smith', 'jane.smith@example.com');
   ```

   This will trigger the `WRITE` log warning on the server side.
   ```
   [yyyy-mm-dd HH:MM:SS WARNING] WRITE operation detected! ⚠️ Writing new data may impact database integrity if not handled carefully.
   ```

4. **Update Data (UPDATE operation):**
   Update the `email` of a user.
   ```sql
   UPDATE users SET email = 'new.email@example.com' WHERE name = 'John Doe';
   ```

    This will trigger the `UPDATE` log warning on the server side.
   ```
   [yyyy-mm-dd HH:MM:SS WARNING] UPDATE operation detected! ⚠️ This will modify existing data.
   ```

### Notes:
- The server logs will display warnings whenever you use `WRITE` or `UPDATE` queries, as they indicate potential data overwrites.
- You can see these logs in the terminal running the Rust server.

### Troubleshooting:
- If the connection to the proxy server fails, ensure that the server is running and check if the port `5433` is open.
- Verify your PostgreSQL client is installed correctly by running `psql --version`.
