create_tables_sql = """

CREATE TABLE IF NOT EXISTS employee_data_stg (
    name VARCHAR(100) NOT NULL,
    age INT NOT NULL,
    emp_id BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS employee_data_dim (
    name VARCHAR(100) NOT NULL,
    age INT NOT NULL,
    emp_id BIGINT NOT NULL,
    primary key(emp_id)

);

CREATE TABLE IF NOT EXISTS employee_leave_data_dim (
    emp_id BIGINT NOT NULL,
    date DATE NOT NULL,
    status VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS employee_leave_quota_data_dim (
    emp_id BIGINT NOT NULL,
    leave_quota INT NOT NULL,
    year INT NOT NULL
    );

CREATE TABLE IF NOT EXISTS employee_leave_calender_data_dim (
    reason VARCHAR(10) NOT NULL,
    date DATE NOT NULL
    );

CREATE TABLE IF NOT EXISTS staging_employee_leave_data (
    emp_id BIGINT NOT NULL,
    date DATE NOT NULL,
    status VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS employee_ts_table_dim (
    emp_id bigint,
    designation text,
    start_date DATE,
    end_date DATE,
    salary INT,
    status text
    );
    

CREATE TABLE IF NOT EXISTS employee_desig_count_table_dim (
    designation text,
    active_count INT
    );

CREATE TABLE IF NOT EXISTS staging_desig_count(
    designation text,
    active_count INT
    );

CREATE TABLE IF NOT EXISTS employee_upcoming_leaves_table_dim (
    emp_id BIGINT,
    upcoming_leaves INT
    );
    
CREATE TABLE IF NOT EXISTS stg_employee_upcoming_leaves (
    emp_id BIGINT,
    upcoming_leaves INT
    );
    

CREATE TABLE IF NOT EXISTS employee_leaves_spend_table_dim (
    emp_id BIGINT,
    year INT,
    leave_quota INT,
    total_leaves_taken INT,
    percentage_used DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS employee_messages_dim(
    sender_id bigint,
    receiver_id bigint,
    msg_text text,
    time_stamp timestamp
);


CREATE TABLE IF NOT EXISTS employee_strike_table_dim(
    emp_id bigint,
    strike_count bigint,
    last_strike_date date,
    curr_salary integer
);

CREATE TABLE IF NOT EXISTS scd_employee_strikes_table (
    employee_id BIGINT ,
    temp_strike_count INTEGER,
    strike_count INTEGER,
    strike_date DATE,
    original_salary NUMERIC,
    current_salary NUMERIC,
    sc_1 NUMERIC DEFAULT 0,
    sc_2 NUMERIC DEFAULT 0,
    sc_3 NUMERIC DEFAULT 0,
    sc_4 NUMERIC DEFAULT 0,
    sc_5 NUMERIC DEFAULT 0,
    sc_6 NUMERIC DEFAULT 0,
    sc_7 NUMERIC DEFAULT 0,
    sc_8 NUMERIC DEFAULT 0,
    sc_9 NUMERIC DEFAULT 0,
    status VARCHAR(10) DEFAULT 'ACTIVE'
);

"""
