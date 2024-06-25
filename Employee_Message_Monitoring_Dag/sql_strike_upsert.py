
strike_upsert = '''update employee_strike_table_dim as h
set strike_count = strike_count + i.strk_count,
curr_salary = i.salary,
from staging_strike_table as i 
where emp_id = i.employee_id;

INSERT INTO employee_strike_table_dim (emp_id, strike_count, last_strike_date, curr_salary)
SELECT employee_id, strk_count, lst_strike_date, salary
FROM staging_strike_table
WHERE employee_id NOT IN (
    SELECT emp_id
    FROM employee_strike_table_dim
);

'''
