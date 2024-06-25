compare_stg_tbl_task1='''
INSERT INTO employee_data_dim (name, age, emp_id)
SELECT name, age, emp_id
FROM employee_data_stg
EXCEPT
SELECT name, age, emp_id
FROM employee_data_dim;

'''
