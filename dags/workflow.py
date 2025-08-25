from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Define DAG arguments
default_args = {
    "owner": "airflow",
    "start_date": days_ago(0),
    "email": ["your_email@example.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    "ETL_toll_data",
    default_args=default_args,
    description="Apache Airflow Final Assignmentn",
    schedule_interval=timedelta(days=1),
)


# Task 1: Unzip tolldata.tgz file
unzip_data = BashOperator(
    task_id="unzip_data",
    bash_command="'cd /opt/airflow/dags/finalassignment && tar xzf tolldata.tgz",
    dag=dag,
)

# Task 2: Extract data from CSV
extract_data_from_csv = BashOperator(
    task_id="extract_data_from_csv",
    bash_command="""
         cd  /opt/airflow/dags/finalassignment && 
         echo "Rowid,Timestamp,Anonymized Vehicle number,Vehicle type" > csv_data.csv &&
         cut -d"," -f1-4 vehicle-data.csv >> csv_data.csv
    """,
    dag=dag,
)

# Task 3: Extract data from TSV file
extract_data_from_tsv = BashOperator(
    task_id="extract_data_from_tsv",
    bash_command="""
         cd  /opt/airflow/dags/finalassignment && 
         echo "Number of axles,Tollplaza id,Tollplaza code" > tsv_data.csv &&
         cut -f5,6,7 tollplaza-data.tsv | tr '\t' ',' >> tsv_data.csv
    """,
    dag=dag,
)

# Task 4: Extract data from fixed width file
extract_data_from_fixed_width = BashOperator(
    task_id="extract_data_from_fixed_width",
    bash_command="""
         'cd /opt/airflow/dags/finalassignment && 
         echo "Type of Payment code,Vehicle Code" > fixed_width_data.csv &&
         awk '{print substr($0,59,3) "," substr($0,63,5)}' payment-data.txt >> fixed_width_data.csv
    """,
    dag=dag,
)

# Task 5: Consolidate data
consolidate_data = BashOperator(
    task_id="consolidate_data",
    bash_command="""
         cd ./airflow/dags/finalassignment && 
         echo "Rowid,Timestamp,Anonymized Vehicle number,Vehicle type,Number of axles,Tollplaza id,Tollplaza code, \
         Type of Payment code,Vehicle Code" > extracted_data.csv &&
         paste -d',' csv_data.csv tsv_data.csv fixed_width_data.csv | tail -n +2 >> extracted_data.csv
    """,
    dag=dag,
)

# Task 6: Transform data
transform_data = BashOperator(
    task_id="transform_data",
    bash_command="""
         cd ./airflow/dags/finalassignment && 
         mkdir -p staging &&
         head -1 extracted_data.csv > staging/transformed_data.csv &&
         tail -n +2 extracted_data.csv | while IFS=, read -r col1 col2 col3 col4 col5 col6 col7 col8 col9; do
             echo "$col1,$col2,$col3,$(echo $col4 | tr '[a-z]' '[A-Z]'),$col5,$col6,$col7,$col8,$col9"
         done >> staging/transformed_data.csv
    """,
    dag=dag,
)

# task 7: Define the task pipeline
(
    unzip_data
    >> extract_data_from_csv
    >> extract_data_from_tsv
    >> extract_data_from_fixed_width
    >> consolidate_data
    >> transform_data
)
