-- Create the database if it doesn't exist
CREATE DATABASE IF NOT EXISTS test_db;

-- Switch to the newly created or existing database
\c test_db

-- Create tables
CREATE TABLE IF NOT EXISTS companies (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    industry VARCHAR(50),
    founded_year INTEGER
);

CREATE TABLE IF NOT EXISTS employees (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    position VARCHAR(100),
    salary NUMERIC(10, 2)
);

-- Insert sample data into the companies table
INSERT INTO companies (name, industry, founded_year) VALUES
('TechCorp', 'Technology', 2000),
('FinanceHub', 'Finance', 1995),
('GreenEnergy', 'Renewable Energy', 2010);

-- Insert sample data into the employees table
INSERT INTO employees (company_id, first_name, last_name, position, salary) VALUES
(1, 'John', 'Doe', 'Software Engineer', 85000.00),
(1, 'Jane', 'Smith', 'Product Manager', 95000.00),
(2, 'Mike', 'Johnson', 'Financial Analyst', 75000.00),
(2, 'Emily', 'Brown', 'HR Manager', 70000.00),
(3, 'David', 'Lee', 'Research Scientist', 90000.00);
