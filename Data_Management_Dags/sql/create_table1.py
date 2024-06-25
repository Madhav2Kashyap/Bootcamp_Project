
create_table_sql1 = '''CREATE TABLE "employee_leaves_spend_dim(WE)"(
    "emp_id" BIGINT NOT NULL,
    "year" INTEGER NOT NULL,
    "leave_quota" INTEGER NOT NULL,
    "total_leaves_taken(DA)" INTEGER NOT NULL,
    "percentage_used(DA)" DOUBLE PRECISION NOT NULL
);
ALTER TABLE
    "employee_leaves_spend_dim(WE)" ADD PRIMARY KEY("emp_id");
ALTER TABLE
    "employee_leaves_spend_dim(WE)" ADD PRIMARY KEY("year");
CREATE TABLE "employee_messages(WE)"(
    "msg_id" UUID NOT NULL,
    "sender_id(WKA)" BIGINT NOT NULL,
    "receiver_id(WKA)" BIGINT NOT NULL,
    "msg_text(WKA)" TEXT NOT NULL,
    "is_flagged" BOOLEAN NOT NULL,
    "msg_timestamp" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL
);
ALTER TABLE
    "employee_messages(WE)" ADD PRIMARY KEY("msg_id");
ALTER TABLE
    "employee_messages(WE)" ADD CONSTRAINT "employee_messages(we)_sender_id(wka)_unique" UNIQUE("sender_id(WKA)");
ALTER TABLE
    "employee_messages(WE)" ADD CONSTRAINT "employee_messages(we)_receiver_id(wka)_unique" UNIQUE("receiver_id(WKA)");
ALTER TABLE
    "employee_messages(WE)" ADD CONSTRAINT "employee_messages(we)_msg_text(wka)_unique" UNIQUE("msg_text(WKA)");
COMMENT
ON COLUMN
    "employee_messages(WE)"."sender_id(WKA)" IS 'Weak Key Attribute';
CREATE TABLE "employee_fact_table(WE)"(
    "emp_id(WKA)" BIGINT NOT NULL,
    "year(WKA)" INTEGER NOT NULL,
    "salary" INTEGER NOT NULL,
    "current_salary" INTEGER NOT NULL,
    "strike_count" INTEGER NOT NULL,
    "leave_quota" INTEGER NOT NULL,
    "total_leaves_taken" INTEGER NOT NULL
);
ALTER TABLE
    "employee_fact_table(WE)" ADD PRIMARY KEY("emp_id(WKA)");
ALTER TABLE
    "employee_fact_table(WE)" ADD PRIMARY KEY("year(WKA)");
CREATE TABLE "employee_leave_quota_dim(WE)"(
    "emp_id(WKA)" BIGINT NOT NULL,
    "year(WKA)" INTEGER NOT NULL,
    "leave_quota" INTEGER NOT NULL
);
ALTER TABLE
    "employee_leave_quota_dim(WE)" ADD PRIMARY KEY("emp_id(WKA)");
ALTER TABLE
    "employee_leave_quota_dim(WE)" ADD PRIMARY KEY("year(WKA)");
CREATE TABLE "active_employee_timeframe_dim(SE)"(
    "designation" TEXT NOT NULL,
    "active_count" INTEGER NOT NULL
);
ALTER TABLE
    "active_employee_timeframe_dim(SE)" ADD PRIMARY KEY("designation");
CREATE TABLE "employee_leave_dim(WE)"(
    "emp_id(WKA)" BIGINT NOT NULL,
    "date(WKA)" DATE NOT NULL,
    "status" VARCHAR(255) NOT NULL
);
ALTER TABLE
    "employee_leave_dim(WE)" ADD PRIMARY KEY("emp_id(WKA)");
ALTER TABLE
    "employee_leave_dim(WE)" ADD PRIMARY KEY("date(WKA)");
CREATE TABLE "employee_leave_calendar_dim(SE)"(
    "date" DATE NOT NULL,
    "reason" VARCHAR(255) NOT NULL
);
ALTER TABLE
    "employee_leave_calendar_dim(SE)" ADD PRIMARY KEY("date");
CREATE TABLE "employee_timeframe_dim(SE)"(
    "emp_id" BIGINT NOT NULL,
    "designation" TEXT NOT NULL,
    "start_date" DATE NOT NULL,
    "end_date" DATE NOT NULL,
    "salary" INTEGER NOT NULL,
    "status" TEXT NOT NULL
);
ALTER TABLE
    "employee_timeframe_dim(SE)" ADD PRIMARY KEY("emp_id");
CREATE TABLE "strike_count_dim(SE)"(
    "emp_id" BIGINT NOT NULL,
    "strike_count" INTEGER NOT NULL,
    "last_strike_date" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    "salary" INTEGER NOT NULL,
    "current_salary" INTEGER NOT NULL
);
ALTER TABLE
    "strike_count_dim(SE)" ADD PRIMARY KEY("emp_id");
CREATE TABLE "employee_dim(SE)"(
    "emp_id" BIGINT NOT NULL,
    "name" VARCHAR(255) NOT NULL,
    "age" INTEGER NOT NULL
);
ALTER TABLE
    "employee_dim(SE)" ADD PRIMARY KEY("emp_id");
CREATE TABLE "employee_upcoming_leaves_dim(SE)"(
    "emp_id" BIGINT NOT NULL,
    "upcoming_leaves(DA)" BIGINT NOT NULL
);
ALTER TABLE
    "employee_upcoming_leaves_dim(SE)" ADD PRIMARY KEY("emp_id");
ALTER TABLE
    "employee_fact_table(WE)" ADD CONSTRAINT "employee_fact_table(we)_emp_id(wka)_foreign" FOREIGN KEY("emp_id(WKA)") REFERENCES "employee_upcoming_leaves_dim(SE)"("emp_id");
ALTER TABLE
    "employee_upcoming_leaves_dim(SE)" ADD CONSTRAINT "employee_upcoming_leaves_dim(se)_emp_id_foreign" FOREIGN KEY("emp_id") REFERENCES "employee_leave_dim(WE)"("emp_id(WKA)");
ALTER TABLE
    "employee_fact_table(WE)" ADD CONSTRAINT "employee_fact_table(we)_emp_id(wka)_foreign" FOREIGN KEY("emp_id(WKA)") REFERENCES "employee_leaves_spend_dim(WE)"("emp_id");
ALTER TABLE
    "employee_upcoming_leaves_dim(SE)" ADD CONSTRAINT "employee_upcoming_leaves_dim(se)_emp_id_foreign" FOREIGN KEY("emp_id") REFERENCES "employee_leave_quota_dim(WE)"("emp_id(WKA)");
ALTER TABLE
    "employee_leaves_spend_dim(WE)" ADD CONSTRAINT "employee_leaves_spend_dim(we)_emp_id_foreign" FOREIGN KEY("emp_id") REFERENCES "employee_leave_dim(WE)"("emp_id(WKA)");
ALTER TABLE
    "employee_upcoming_leaves_dim(SE)" ADD CONSTRAINT "employee_upcoming_leaves_dim(se)_emp_id_foreign" FOREIGN KEY("emp_id") REFERENCES "employee_leave_calendar_dim(SE)"("date");
ALTER TABLE
    "employee_fact_table(WE)" ADD CONSTRAINT "employee_fact_table(we)_emp_id(wka)_foreign" FOREIGN KEY("emp_id(WKA)") REFERENCES "employee_dim(SE)"("emp_id");
ALTER TABLE
    "employee_timeframe_dim(SE)" ADD CONSTRAINT "employee_timeframe_dim(se)_designation_foreign" FOREIGN KEY("designation") REFERENCES "active_employee_timeframe_dim(SE)"("designation");
ALTER TABLE
    "employee_fact_table(WE)" ADD CONSTRAINT "employee_fact_table(we)_emp_id(wka)_foreign" FOREIGN KEY("emp_id(WKA)") REFERENCES "employee_timeframe_dim(SE)"("emp_id");
ALTER TABLE
    "employee_messages(WE)" ADD CONSTRAINT "employee_messages(we)_sender_id(wka)_foreign" FOREIGN KEY("sender_id(WKA)") REFERENCES "strike_count_dim(SE)"("emp_id");
ALTER TABLE
    "employee_fact_table(WE)" ADD CONSTRAINT "employee_fact_table(we)_emp_id(wka)_foreign" FOREIGN KEY("emp_id(WKA)") REFERENCES "strike_count_dim(SE)"("emp_id");

'''