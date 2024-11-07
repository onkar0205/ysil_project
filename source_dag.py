# import json
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# from project.connectors.oracle_connector import OracleConnector
# from project.connectors.postgres_connector import PostgresConnector
# from project.connectors.mssql_connector import MSSQLConnector

# from project.utils.db_utils import fetch_data_from_table
# from project.utils.s3_utils import dump_data_into_s3_bucket, get_previous_date, update_previous_date

# access_key="AKIAZI2LB77CCADMIH44"
# secret_key="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"
# s3_bucket = "ysilbucket"

# with open('/home/onkar/airflow/dags/project/config/source_config.json') as f:
#     config = json.load(f)

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 11, 4),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# def extract_data(table, query, database_type, params, source_name, s3_bucket, access_key, secret_key):
#     previous_date_json_path = f"{source_name}/{source_name}.json"
#     read_data_from_date = datetime.strptime(get_previous_date(s3_bucket, previous_date_json_path, access_key, secret_key, table, config, source_name), '%Y-%m-%d') + timedelta(days=1)    
#     read_data_till_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)    
#     print("read_data_from_date :",read_data_from_date)
#     print("read_data_till_date :",read_data_till_date)

#     while read_data_from_date < read_data_till_date:
#         year = read_data_from_date.year
#         month = read_data_from_date.strftime('%B')
#         read_data_of_date = read_data_from_date.strftime('%Y-%m-%d')

#         print(f"Processing data for date: {read_data_of_date}")
        
#         formatted_query = query.format(read_data_of_date=read_data_of_date)
#         print("Query: ", formatted_query)

#         connector_class = {
#             "oracle": OracleConnector,
#             "postgresql": PostgresConnector,
#             "mssql": MSSQLConnector
#         }.get(database_type)
#         connector = connector_class(**params)

#         df_data = fetch_data_from_table(connector, table, formatted_query)
#         s3_path = f"{source_name}/{table}/Year_{year}/{month}_{year}/{read_data_of_date}.parquet"
#         print("S3 Path:", s3_path)
#         dump_data_into_s3_bucket(df_data, s3_bucket, s3_path, access_key, secret_key)
        
#         read_data_from_date += timedelta(days=1)

# for source_name, source_config in config['sources'].items():
#     schedule_time = source_config.get('schedule_time', '00:00')
#     hour, minute = map(int, schedule_time.split(':'))
#     hour_utc, minute_utc = (hour - 5) % 24, (minute - 30) % 60
#     if minute < 30:
#         hour_utc = (hour_utc - 1) if hour_utc > 0 else 23
#     schedule_interval = f'{minute_utc} {hour_utc} * * *'

#     dag_id = f'daily_etl_{source_name}'
#     dag = DAG(
#         dag_id=dag_id,
#         default_args=default_args,
#         description=f'Daily ETL for {source_name}',
#         schedule_interval=schedule_interval,
#     )

#     for table, query_template in source_config['table_queries'].items():
#         task_id = f'{table}_data'
#         task = PythonOperator(
#             task_id=task_id,
#             python_callable=extract_data,
#             op_kwargs={
#                 'table': table,
#                 'query': query_template,
#                 'database_type': source_config['database_type'],
#                 'params': source_config['params'],
#                 'source_name': source_name,
#                 's3_bucket': s3_bucket,
#                 'access_key': access_key,
#                 'secret_key': secret_key
#             },
#             dag=dag,
#         )

#     globals()[dag_id] = dag

# import json
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# from project.connectors.oracle_connector import OracleConnector
# from project.connectors.postgres_connector import PostgresConnector
# from project.connectors.mssql_connector import MSSQLConnector
# from project.utils.s3_utils import S3Connector  
# from project.utils.db_utils import fetch_data_from_table

# access_key="AKIAZI2LB77CCADMIH44"
# secret_key="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"
# s3_bucket = "ysilbucket"

# # Load source configuration
# with open('/home/onkar/airflow/dags/project/config/source_config.json') as f:
#     config = json.load(f)

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 11, 4),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# def extract_data(table, query, database_type, params, source_name, s3_connector):
#     read_data_from_date = datetime.strptime(
#         s3_connector.get_previous_date(table, config, source_name),
#         '%Y-%m-%d'
#     ) + timedelta(days=1)

#     read_data_till_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
#     print("read_data_from_date:", read_data_from_date)
#     print("read_data_till_date:", read_data_till_date)

#     while read_data_from_date < read_data_till_date:
#         year = read_data_from_date.year
#         month = read_data_from_date.strftime('%B')
#         read_data_of_date = read_data_from_date.strftime('%Y-%m-%d')

#         print(f"Processing data for date: {read_data_of_date}")
        
#         formatted_query = query.format(read_data_of_date=read_data_of_date)
#         print("Query:", formatted_query)

#         # Initialize the connector class based on database type
#         connector_class = {
#             "oracle": OracleConnector,
#             "postgresql": PostgresConnector,
#             "mssql": MSSQLConnector
#         }.get(database_type)
#         connector = connector_class(**params)

#         # Fetch data and upload to S3
#         df_data = fetch_data_from_table(connector, table, formatted_query)
#         s3_path = f"{source_name}/{table}/Year_{year}/{month}_{year}/{read_data_of_date}.parquet"
#         print("S3 Path:", s3_path)
#         s3_connector.upload_dataframe_as_parquet(df_data, s3_path)

#         # Update the last processed date in the JSON file on S3
#         s3_connector.update_previous_date(source_name, table, read_data_of_date)
#         read_data_from_date += timedelta(days=1)

# for source_name, source_config in config['sources'].items():
#     schedule_time = source_config.get('schedule_time', '00:00')
#     hour, minute = map(int, schedule_time.split(':'))
#     hour_utc, minute_utc = (hour - 5) % 24, (minute - 30) % 60
#     if minute < 30:
#         hour_utc = (hour_utc - 1) if hour_utc > 0 else 23
#     schedule_interval = f'{minute_utc} {hour_utc} * * *'

#     dag_id = f'daily_etl_{source_name}'
#     dag = DAG(
#         dag_id=dag_id,
#         default_args=default_args,
#         description=f'Daily ETL for {source_name}',
#         schedule_interval=schedule_interval,
#     )

#     # Define the path for storing last processed date based on the source name
#     previous_date_json_path = f"{source_name}/{source_name}.json"
    
#     # Initialize S3Connector instance per source with the dynamic JSON path
#     s3_connector = S3Connector(
#         access_key=access_key,
#         secret_key=secret_key,
#         bucket_name=s3_bucket,
#         previous_date_json_path=previous_date_json_path
#     )

#     for table, query_template in source_config['table_queries'].items():
#         task_id = f'{table}_data'
#         task = PythonOperator(
#             task_id=task_id,
#             python_callable=extract_data,
#             op_kwargs={
#                 'table': table,
#                 'query': query_template,
#                 'database_type': source_config['database_type'],
#                 'params': source_config['params'],
#                 'source_name': source_name,
#                 's3_connector': s3_connector  
#             },
#             dag=dag,
#         )

#     globals()[dag_id] = dag


import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from project.connectors.oracle_connector import OracleConnector
from project.connectors.postgres_connector import PostgresConnector
from project.connectors.mssql_connector import MSSQLConnector
from project.utils.db_utils import fetch_data_from_table
from project.connectors.s3_connector import S3Connector 

access_key="AKIAZI2LB77CCADMIH44"
secret_key="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"
s3_bucket = "ysilbucket"

# Load configuration file
with open('/home/onkar/airflow/dags/project/config/source_config_new.json') as f:
    config = json.load(f)

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def extract_data(table, query, database_type, params, source_name):
    previous_date_json_path = f"{source_name}/{source_name}_last_processed_dates.json"
    connector_class = {
            "oracle": OracleConnector,
            "postgresql": PostgresConnector,
            "mssql": MSSQLConnector
        }.get(database_type)
    connector = connector_class(**params)

    # Get the last processed date from S3
    s3_connector = S3Connector(access_key, secret_key, s3_bucket, config)
    last_processed_date = s3_connector.read_previous_date_json(previous_date_json_path, source_name ,connector)
    if last_processed_date:
        read_data_from_date = datetime.strptime(
            last_processed_date[source_name][table]['last_processed_date'], '%Y-%m-%d'
        ) + timedelta(days=1)

    read_data_till_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    print("read_data_from_date :",read_data_from_date)
    print("read_data_till_date :",read_data_till_date)
    while read_data_from_date < read_data_till_date:
        year = read_data_from_date.year
        month = read_data_from_date.strftime('%B')
        read_data_of_date = read_data_from_date.strftime('%Y-%m-%d')
        print(f"Processing data for date: {read_data_of_date}")
        
        formatted_query = query.format(read_data_of_date=read_data_of_date)
        print("Query: ", formatted_query)

        df_data = fetch_data_from_table(connector, table, formatted_query)

        s3_path = f"{source_name}/{table}/Year_{year}/{month}_{year}/{read_data_of_date}.parquet"
        print("S3 Path:", s3_path)
        s3_connector.upload_parquet_to_s3(df_data, s3_path)
        s3_connector.update_previous_date_json(previous_date_json_path, source_name, table, read_data_of_date,connector)        
        read_data_from_date += timedelta(days=1)

for source_name, source_config in config['sources'].items():
    schedule_time = source_config.get('schedule_time', '00:00')
    hour, minute = map(int, schedule_time.split(':'))
    hour_utc, minute_utc = (hour - 5) % 24, (minute - 30) % 60
    if minute < 30:
        hour_utc = (hour_utc - 1) if hour_utc > 0 else 23
    schedule_interval = f'{minute_utc} {hour_utc} * * *'

    dag_id = f'daily_etl_{source_name}'
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f'Daily ETL for {source_name}',
        schedule_interval=schedule_interval,
    )

    for table, queries in source_config['table_queries'].items():
        task_id = f'{table}_data'
        task = PythonOperator(
            task_id=task_id,
            python_callable=extract_data,
            op_kwargs={
                'table': table,
                'query': queries['data_query'],
                'database_type': source_config['database_type'],
                'params': source_config['params'],
                'source_name': source_name
            },
            dag=dag,
        )

    globals()[dag_id] = dag
