# Business Requirement Document (BRD)

## Business Needs
The following business needs must be fulfilled:
- **Tracking Employee Leave History and Quotas:** Maintain records of leave history and quotas for each employee.
- **Identifying Employees with Excessive Leave Requests:** Detect patterns of excessive leave requests among employees.
- **Monitoring Employee Communications for Misuse:** Ensure employee communications are monitored for potential misuse.
- **Efficient Management of Employee Data:** Streamline the management of employee-related data.

## Data Sources
1. **employee_data.csv**
   - **Description:** Contains employee data, including employee ID, age, and name. The data clarity is high.
   - **Location:** `S3://{bucket-name}/{emp-data}/`
   - **Frequency:** Daily at 00:00 UTC

2. **employee_timeframe_data.csv, employee_timeframe_data_1.csv**
   - **Description:** Provide information about an employee's tenure at a particular designation with a specific salary. The start_date and end_date are given as Unix timestamps.
   - **Location:** `S3://{bucket-name}/{emp-time-data}/`
   - **Frequency:** Daily at 00:00 UTC

3. **employee_leave_quota_data.csv**
   - **Description:** Represents the leave quota allocated to every employee annually.
   - **Location:** `S3://{bucket-name}/leave-quota/`
   - **Frequency:** Yearly

4. **employee_leave_calendar_data.csv**
   - **Description:** Represents the mandatory holidays for the year. These leaves are in addition to the allocated leave quota for employees.
   - **Location:** `S3://{bucket-name}/leave-calendar/`
   - **Frequency:** Yearly on January 1st

5. **employee_leave_data.csv**
   - **Description:** Represents the actual leaves applied for or taken by employees.
   - **Location:** `S3://{bucket-name}/leave-data/`
   - **Frequency:** Daily at 07:00 UTC

6. **vocab.json**
   - **Description:** Contains all words used in the messages sent by employees.
   - **Location:** `S3://{bucket-name}/vocab/`
   - **Frequency:** As needed

7. **marked_word.json**
   - **Description:** Contains reserved words that are flagged in messages.
   - **Location:** `S3://{bucket-name}/marked-words/`
   - **Frequency:** As needed

8. **message.json**
   - **Description:** Contains sample messages that will be coming through Kafka servers.
   - **Location:** `S3://{bucket-name}/messages/`
   - **Frequency:** Real-time streaming via Kafka Confluent

## Data Processing Details

### Task 1
**Append-Only Incremental Table from Employee Data**
- **Input Data:** `employee_data.csv` from S3 bucket-Bronze

### Task 2
**Incremental Table from Employee Time Data**
- **Input Data:** `employee_timeframe_data.csv` and `employee_timeframe_data_1.csv` from S3 bucket-Bronze

### Task 3
**Append-Only Yearly Incremental Table for Leave Quota**
- **Input Data:** `employee_leave_quota_data.csv` from S3 bucket-Bronze

**Append-Only Yearly Incremental Table for Leave Calendar**
- **Input Data:** `employee_leave_calendar_data.csv` from S3 bucket-Bronze

**Daily Update Table for Leaves Taken or Applied**
- **Input Data:** `employee_leave_data.csv` from S3 bucket-Bronze

### Task 4
**Active Employee by Designation Report**
- **Input Data:** `employee_timeframe_data.csv` from S3 bucket-Silver

**Employees with Potential Excessive Leaves**
- **Input Data:** `employee_leave_data.csv`, `employee_leave_calendar_data.csv` from S3 bucket-Silver

**Percentage of Leave Quota Used and Alert Generation**
- **Input Data:** `employee_leave_data.csv`, `employee_leave_quota_data.csv` from S3 bucket-Silver

### Task 5
**Streaming System for Flagging Messages and Salary Deduction**
- **Input Data:** Kafka stream with messages
