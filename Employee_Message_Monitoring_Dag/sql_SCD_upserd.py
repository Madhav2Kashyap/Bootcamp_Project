SCD_upserd = '''
-- Update existing records in SCD_employee_strikes_table
UPDATE SCD_employee_strikes_table AS scd
SET
    strike_count = dim.strike_count,
    last_strike_date = dim.last_strike_date,
    salary = dim.curr_salary / POWER(0.9, scd.strike_count)
FROM employee_strike_table_dim AS dim
WHERE scd.employee_id = dim.emp_id;

INSERT INTO SCD_employee_strikes_table (
    employee_id, strike_count, last_strike_date, salary
)
SELECT 
    emp_id,
    strike_count,
    last_strike_date,
    curr_salary
FROM employee_strike_table_dim AS dim
WHERE NOT EXISTS (
    SELECT 1 FROM SCD_employee_strikes_table AS scd
    WHERE scd.employee_id = dim.emp_id
);
'''