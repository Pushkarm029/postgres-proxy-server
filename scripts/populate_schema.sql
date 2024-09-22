-- Create the measures table if it doesn't exist
CREATE TABLE IF NOT EXISTS measures (
    name TEXT PRIMARY KEY,
    query TEXT NOT NULL
);

-- Insert measure definitions into the measures table
INSERT INTO measures (name, query) VALUES 
    ('head_count', 'COUNT(id)'), 
    ('revenue', 'SUM(amount)'), 
    ('average_salary', 'AVG(salary)');
