employee_leave_data_transformation_sql = """
UPDATE employee_leave_data_dim AS e
SET status = s.status
FROM staging_employee_leave_data AS s
WHERE e.emp_id = s.emp_id AND e.date = s.date;

-- Insert new records into employee_leave_data from staging_employee_leave_data
INSERT INTO employee_leave_data_dim (emp_id, date, status)
SELECT emp_id, date , status
FROM staging_employee_leave_data
EXCEPT
SELECT emp_id, date , status
FROM employee_leave_data_dim;

"""
