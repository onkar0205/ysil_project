import boto3
import io
import pandas as pd
import json
from botocore.exceptions import ClientError
from pyspark.sql import DataFrame
from project.utils.db_utils import fetch_data_from_table
from datetime import datetime, timedelta

class S3Connector:
    def __init__(self, access_key: str, secret_key: str, bucket_name: str, config: dict):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        self.bucket_name = bucket_name
        self.config = config

    def upload_parquet_to_s3(self, df_data: DataFrame, s3_path: str):
        try:
            data_list = df_data.collect()
            pdf_data = pd.DataFrame(data_list)
            parquet_buffer = io.BytesIO()
            pdf_data.to_parquet(parquet_buffer, engine='pyarrow', index=False)
            parquet_buffer.seek(0)

            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_path,
                Body=parquet_buffer.getvalue()
            )
            print(f"Data successfully uploaded to S3 at '{s3_path}' in bucket '{self.bucket_name}'")
            
        except Exception as e:
            print(f"Error writing data to S3: {str(e)}")

    def get_min_date_for_table(self, table_name, min_date_query, connector):
        try:
            print(min_date_query)
            df = connector.read_table(min_date_query)
            min_date = df.collect()[0][0]
            if isinstance(min_date, datetime):
                min_date = min_date.date()  # Convert to date only
            print(f"Min date for table '{table_name}' is {min_date}")
            return min_date
        except Exception as e:
            print(f"Error fetching min date for table '{table_name}': {str(e)}")
            return None

    def initialize_previous_date_json(self, s3_path: str, source_name: str, connector):
        json_data = {source_name: {}}
        source_config = self.config['sources'].get(source_name)
        
        if source_config:
            for table, queries in source_config['table_queries'].items():
                min_date_query = queries['min_date_query']
                historic_date = self.get_min_date_for_table(table, min_date_query, connector)
                print("historic_date :",historic_date)
                historic_date = (historic_date - timedelta(days=1)).isoformat()
                json_data[source_name][table] = {'last_processed_date': f'{historic_date}'}
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_path,
                Body=json.dumps(json_data, indent=2),
                ContentType='application/json'
            )
            print(f"Initialized JSON at {s3_path} for '{source_name}' with historic dates.")
        else:
            print(f"Source '{source_name}' not found in configuration.")


    def read_previous_date_json(self, s3_path: str, source_name: str, connector):
        try:
            # Attempt to read JSON from S3
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_path)
            json_data = json.loads(response['Body'].read().decode('utf-8'))
            
            # Check for new tables in the configuration and update JSON if needed
            source_config = self.config['sources'].get(source_name)
            if source_config:
                for table, table_config in source_config['table_queries'].items():
                    # Fetch the historic date from the table if it’s missing in JSON
                    if table not in json_data.get(source_name, {}):
                        historic_date = self.get_min_date_for_table(table, table_config['min_date_query'], connector)
                        historic_date = (historic_date - timedelta(days=1)).isoformat()
                        json_data[source_name][table] = {'last_processed_date': historic_date}
                        print(f"Added new table '{table}' to JSON with min date '{historic_date}'.")

                # Update JSON in S3 if changes were made
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_path,
                    Body=json.dumps(json_data, indent=2),
                    ContentType='application/json'
                )
            
            return json_data

        except self.s3_client.exceptions.NoSuchKey:
            # Initialize JSON if it doesn't exist
            print(f"No existing JSON file at {s3_path}. Initializing with default structure.")
            self.initialize_previous_date_json(s3_path, source_name, connector)
            return self.read_previous_date_json(s3_path, source_name, connector)  # Load initialized JSON
        except Exception as e:
            print(f"Error loading JSON from S3: {str(e)}")
            return None



    def update_previous_date_json(self, s3_path: str, source_name: str, table_name: str, last_processed_date: str, connector):
        try:
            json_data = self.read_previous_date_json(s3_path, source_name, connector) or {}

            if source_name not in json_data:
                json_data[source_name] = {}
            if table_name not in json_data[source_name]:
                json_data[source_name][table_name] = {}

            json_data[source_name][table_name]['last_processed_date'] = last_processed_date

            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_path,
                Body=json.dumps(json_data, indent=2),
                ContentType='application/json'
            )
            print(f"Updated JSON file at {s3_path} with last processed date for '{table_name}'.")

        except Exception as e:
            print(f"Error updating JSON on S3: {str(e)}")
