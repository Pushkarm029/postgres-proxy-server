-- Create the employees table if it doesn't exist
CREATE TABLE IF NOT EXISTS employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    salary DECIMAL(10, 2)
);

-- Insert sample data into the employees table
INSERT INTO employees (name, salary) VALUES 
    ('Alice Johnson', 70000.00), 
    ('Bob Smith', 60000.00), 
    ('Charlie Brown', 80000.00), 
    ('Diana Prince', 90000.00), 
    ('Ethan Hunt', 55000.00);
