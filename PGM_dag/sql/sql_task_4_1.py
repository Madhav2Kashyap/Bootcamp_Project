compare_staging_desig_count = '''
        MERGE INTO employee_desig_count_table_dim AS h
            USING staging_desig_count AS i
                ON h.designation = i.designation
                WHEN MATCHED AND h.active_count != i.active_count THEN
                    UPDATE SET active_count = i.active_count
                WHEN NOT MATCHED THEN
                    INSERT (designation, active_count)
                    VALUES (i.designation, i.active_count);

'''
