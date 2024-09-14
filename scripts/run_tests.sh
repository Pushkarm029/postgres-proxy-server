#!/bin/bash

# Function to run a query and check the result
run_query_test() {
    local query="$1"
    local expected_output="$2"
    local actual_output=$(psql -h localhost -p 5433 -U postgres -d test_db -c "$query" -t -A)
    
    if [ "$actual_output" = "$expected_output" ]; then
        echo "Test passed: $query"
    else
        echo "Test failed: $query"
        echo "Expected: $expected_output"
        echo "Actual: $actual_output"
        exit 1
    fi
}

# Test 1: Count number of companies
run_query_test "SELECT COUNT(*) FROM companies;" "3"

# Test 2: Check average salary
run_query_test "SELECT ROUND(AVG(salary), 2) FROM employees;" "83000.00"

# Test 3: Check number of employees in TechCorp
run_query_test "SELECT COUNT(*) FROM employees e JOIN companies c ON e.company_id = c.id WHERE c.name = 'TechCorp';" "2"

# Test 4: Check if MEASURE function works (assuming it's implemented in your proxy)
run_query_test "SELECT MEASURE('total_salary') FROM employees;" "415000.00"

echo "All tests passed successfully!"