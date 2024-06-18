compare_staging_upcomming_leaves = '''
        MERGE INTO employee_upcoming_leaves_table_dim as h
        USING stg_employee_upcoming_leaves as i
        ON h.emp_id = i.emp_id 
             WHEN MATCHED AND h.upcoming_leaves != i.upcoming_leaves THEN UPDATE
                 SET upcoming_leaves = (
                     SELECT upcoming_leaves
                     FROM
                     stg_employee_upcoming_leaves
                     WHERE emp_id = i.emp_id)
             WHEN NOT MATCHED THEN
                 INSERT (emp_id, upcoming_leaves)
                 VALUES (i.emp_id, i.upcoming_leaves);

'''
