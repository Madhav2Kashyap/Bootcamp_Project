
cooldown_activation = '''
UPDATE employee_strike_table_dim
SET last_strike_date = CURRENT_DATE,
	new_salary = curr_salary * power(0.9,strike_count)
WHERE CURRENT_DATE - last_strike_date = 30;
'''
