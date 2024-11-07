# # import boto3
# # import io
# # import pandas as pd
# # import json
# # from botocore.exceptions import ClientError

# # def create_s3_client(access_key: str, secret_key: str):
# #     return boto3.client(
# #         's3',
# #         aws_access_key_id=access_key,
# #         aws_secret_access_key=secret_key
# #     )

# # def update_previous_date():
# #     try:
        
# #         print(f"Previous pipeline run date updated successfully!")
# #     except Exception as e:
# #         print(f"Error while updating previous_date in S3: {str(e)}")

# # def dump_data_into_s3_bucket(df_data, s3_bucket, s3_path, access_key, secret_key):
# #     try:
# #         data_list = df_data.collect()
# #         pdf_data = pd.DataFrame(data_list)
# #         parquet_buffer = io.BytesIO()
# #         pdf_data.to_parquet(parquet_buffer, engine='pyarrow', index=False)
# #         parquet_buffer.seek(0)

# #         s3_client = create_s3_client(access_key, secret_key)
# #         s3_client.put_object(
# #             Bucket=s3_bucket,
# #             Key=s3_path,
# #             Body=parquet_buffer.getvalue()
# #         )

# #         print(f"Data successfully uploaded to S3 at '{s3_path}' in bucket '{s3_bucket}'\n\n")

# #     except Exception as e:
# #         print(f"Error writing data to S3: {str(e)}")


# # def get_previous_date(s3_bucket, previous_date_json_path, access_key, secret_key, table_name, config, source_name):
# #     try:
# #         s3_client = create_s3_client(access_key, secret_key)

# #         try:
# #             response = s3_client.get_object(Bucket=s3_bucket, Key=previous_date_json_path)
# #             json_data = json.loads(response['Body'].read().decode('utf-8'))
# #             print("json is present")
    
# #         except ClientError as e:
# #             if e.response['Error']['Code'] == 'NoSuchKey':
# #                 json_data = {}
# #                 for source, source_config in config['sources'].items():
# #                     json_data[source] = {}
# #                     for table, _ in source_config['table_queries'].items():
# #                         json_data[source][table] = {'last_processed_date': source_config['historic_date']}
                
# #                 s3_client.put_object(
# #                     Bucket=s3_bucket,
# #                     Key=previous_date_json_path,
# #                     Body=json.dumps(json_data, indent=2),
# #                     ContentType='application/json'
# #                 )
# #                 print(f"Generated data has been dumped to {previous_date_json_path}")
# #             else:
# #                 raise 

# #         for source, source_data in json_data.items():
# #             if table_name in source_data:
# #                 last_date = source_data[table_name].get('last_processed_date')
# #                 print(f"Read data of date for table '{table_name}': {last_date}") 
# #                 return last_date

# #         print(f"Table '{table_name}' not found in the JSON data.")
# #         return None

# #     except Exception as e:
# #         print(f"Error getting previous date for {table_name}: {str(e)}")
# #         return None

# import boto3
# import io
# import pandas as pd
# import json
# from datetime import datetime, timedelta
# from botocore.exceptions import ClientError

# class S3Utils:
#     def __init__(self, access_key: str, secret_key: str, bucket_name: str, previous_date_json_path: str):
#         self.s3_client = boto3.client(
#             's3',
#             aws_access_key_id=access_key,
#             aws_secret_access_key=secret_key
#         )
#         self.bucket_name = bucket_name
#         self.previous_date_json_path = previous_date_json_path

#     def upload_dataframe_as_parquet(self, df_data, s3_path):
#         try:
#             data_list = df_data.collect()
#             pdf_data = pd.DataFrame(data_list)
            
#             parquet_buffer = io.BytesIO()
#             pdf_data.to_parquet(parquet_buffer, engine='pyarrow', index=False)
#             parquet_buffer.seek(0)

#             self.s3_client.put_object(
#                 Bucket=self.bucket_name,
#                 Key=s3_path,
#                 Body=parquet_buffer.getvalue()
#             )
#             print(f"Data successfully uploaded to S3 at '{s3_path}' in bucket '{self.bucket_name}'")
            
#         except Exception as e:
#             print(f"Error writing data to S3: {str(e)}")

#     def get_previous_date(self, table_name, config, source_name):
#         try:
#             try:
#                 response = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.previous_date_json_path)
#                 json_data = json.loads(response['Body'].read().decode('utf-8'))
#                 print("Previous date JSON is present.")
#             except ClientError as e:
#                 if e.response['Error']['Code'] == 'NoSuchKey':
#                     # Initialize JSON structure if file doesn't exist
#                     json_data = self._initialize_previous_date_json(config,source_name)
#                     self._upload_json_to_s3(json_data)
#                     print(f"Initialized data has been dumped to {self.previous_date_json_path}")
#                 else:
#                     raise

#             last_date = json_data.get(source_name, {}).get(table_name, {}).get('last_processed_date')
#             if last_date:
#                 print(f"Last processed date for table '{table_name}': {last_date}")
#                 return last_date

#             print(f"Table '{table_name}' not found in the JSON data.")
#             return None

#         except Exception as e:
#             print(f"Error getting previous date for {table_name}: {str(e)}")
#             return None

#     def update_previous_date(self, source_name, table_name, new_date):
#         try:
#             response = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.previous_date_json_path)
#             json_data = json.loads(response['Body'].read().decode('utf-8'))

#             if source_name not in json_data:
#                 json_data[source_name] = {}
#             json_data[source_name][table_name] = {'last_processed_date': new_date}

#             self._upload_json_to_s3(json_data)
#             print(f"Previous pipeline run date updated successfully for table '{table_name}'")

#         except Exception as e:
#             print(f"Error while updating previous_date in S3: {str(e)}")

#     def _initialize_previous_date_json(self, config, source_name):
#         """Initialize JSON structure if the previous date JSON file does not exist in S3."""
#         json_data = {}
#         for source, source_config in config['sources'].items():
#             if source == source_name:
#                 json_data[source] = {}
#                 for table, _ in source_config['table_queries'].items():
#                     json_data[source][table] = {'last_processed_date': source_config['historic_date']}
#         return json_data

#     def _upload_json_to_s3(self, json_data):
#         """Upload a JSON object to S3."""
#         self.s3_client.put_object(
#             Bucket=self.bucket_name,
#             Key=self.previous_date_json_path,
#             Body=json.dumps(json_data, indent=2),
#             ContentType='application/json'
#         )
