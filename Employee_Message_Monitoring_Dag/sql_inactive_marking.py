inactive_marking = '''

UPDATE employee_ts_table_dim
SET status = 'INACTIVE',end_date = current_date
WHERE status = 'ACTIVE' AND 
emp_id IN (SELECT emp_id FROM employee_strike_table_dim WHERE strike_count>=10 ); 

DELETE FROM employee_strike_table_dim
WHERE strike_count = 0 or strike_count >= 10;

DELETE FROM employee_messages_dim
WHERE time_stamp::date < CURRENT_DATE;

'''