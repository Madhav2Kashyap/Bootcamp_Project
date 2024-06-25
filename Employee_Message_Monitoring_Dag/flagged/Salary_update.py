salary_updation = '''
-- Update existing records in SCD_employee_strikes_table
UPDATE SCD_employee_strikes_table AS scd
SET
    strike_count = dim.strike_count,
    last_strike_date = dim.last_strike_date,
    salary = dim.curr_salary / POWER(0.9, dim.strike_count)
FROM employee_strike_table_dim AS dim
WHERE scd.employee_id = dim.emp_id;

'''