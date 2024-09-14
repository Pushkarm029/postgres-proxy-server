-- Create the database (cannot use IF NOT EXISTS in PostgreSQL)
CREATE DATABASE custom_schema_db;

-- Switch to the newly created or existing database
\c custom_schema_db

-- Create a custom schema (do not use reserved information_schema)
CREATE SCHEMA IF NOT EXISTS custom_schema;

-- Create a simplified table in custom_schema.measures
CREATE TABLE IF NOT EXISTS custom_schema.measures (
    name VARCHAR(50) PRIMARY KEY,
    query TEXT NOT NULL
);

-- Insert dummy values for the MVP
INSERT INTO custom_schema.measures (name, query) VALUES
('head_count', 'count(id)'),
('avg_salary', 'avg(salary)'),
('total_revenue', 'sum(revenue)'),
('employee_count', 'count(employee_id)'),
('max_age', 'max(age)'),
('min_experience', 'min(experience)'),
('total_assets', 'sum(assets_value)');
