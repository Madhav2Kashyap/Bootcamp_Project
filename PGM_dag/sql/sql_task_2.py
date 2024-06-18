compare_staging_employee_ts_table_query = '''
Truncate table employee_ts_table_dim;

Insert into employee_ts_table_dim select * from staging_employee_ts_table;

'''
  



















# -- Update end_date and status for existing records
# UPDATE employee_ts_table AS h
#SET end_date = i.min_start_date, status = 'INACTIVE'
#FROM (
#    SELECT emp_id, MIN(start_date) AS min_start_date
#    FROM staging_employee_ts_table
#    WHERE end_date IS NULL
#    GROUP BY emp_id
#) AS i
#WHERE h.emp_id = i.emp_id AND h.end_date IS NULL;

#-- Insert new records
#INSERT INTO employee_ts_table
#SELECT * 
#FROM staging_employee_ts_table st
#WHERE NOT EXISTS (
#    SELECT 1 
#    FROM employee_ts_table et 
#    WHERE et.emp_id = st.emp_id AND et.end_date IS NULL
#);

#-- Truncate staging table
#TRUNCATE TABLE staging_employee_ts_table;


#
# compare_staging_employee_ts_table_query =
#         MERGE INTO employee_ts_table AS h
#              USING staging_employee_ts_table AS i
#              ON h.emp_id = i.emp_id AND h.end_date IS NULL AND i.end_date IS NULL
#              WHEN MATCHED THEN UPDATE
#                  SET end_date = (
#                      SELECT MIN(start_date)
#                      FROM staging_employee_ts_table
#                      WHERE emp_id = i.emp_id
#                  ),
#                  status = 'INACTIVE';
#
#         INSERT INTO employee_ts_table SELECT * FROM staging_employee_ts_table;
#
#         with duplicates_cte as(
#             select ctid,*,row_number() over(partition by emp_id,start_date,end_date,status order by salary desc) as rn from employee_ts_table
#         )
#         DELETE FROM employee_ts_table
#         where ctid in (
#             select ctid from duplicates_cte
#             where rn > 1
#         );
#
#
#         -- Truncate staging_employee_ts_table table after processing
#         TRUNCATE table staging_employee_ts_table;
#
# 
