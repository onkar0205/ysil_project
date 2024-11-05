import boto3
import io
import pandas as pd
import json

def create_s3_client(access_key: str, secret_key: str):
    return boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

def dump_data_into_s3_bucket(df_data, s3_bucket, s3_path, access_key, secret_key):
    try:
        data_list = df_data.collect()
        pdf_data = pd.DataFrame(data_list)
        parquet_buffer = io.BytesIO()
        pdf_data.to_parquet(parquet_buffer, engine='pyarrow', index=False)
        parquet_buffer.seek(0)

        s3_client = create_s3_client(access_key, secret_key)
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=s3_path,
            Body=parquet_buffer.getvalue()
        )

        print(f"Data successfully uploaded to S3 at '{s3_path}' in bucket '{s3_bucket}'\n\n")

    except Exception as e:
        print(f"Error writing data to S3: {str(e)}")


from botocore.exceptions import ClientError

def get_previous_date(s3_bucket, s3_json_path, access_key, secret_key, table_name, config, source_name):
    try:
        s3_client = create_s3_client(access_key, secret_key)

        try:
            response = s3_client.get_object(Bucket=s3_bucket, Key=s3_json_path)
            json_data = json.loads(response['Body'].read().decode('utf-8'))
            print("json is present")
    
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                json_data = {}
                for source, source_config in config['sources'].items():
                    json_data[source] = {}
                    for table, _ in source_config['table_queries'].items():
                        json_data[source][table] = {'last_processed_date': source_config['historic_date']}
                
                s3_client.put_object(
                    Bucket=s3_bucket,
                    Key=s3_json_path,
                    Body=json.dumps(json_data, indent=2),
                    ContentType='application/json'
                )
                print(f"Generated data has been dumped to {s3_json_path}")
            else:
                raise 

        for source, source_data in json_data.items():
            if table_name in source_data:
                last_date = source_data[table_name].get('last_processed_date')
                print(f"Read data of date for table '{table_name}': {last_date}") 
                return last_date

        print(f"Table '{table_name}' not found in the JSON data.")
        return None

    except Exception as e:
        print(f"Error getting previous date for {table_name}: {str(e)}")
        return None