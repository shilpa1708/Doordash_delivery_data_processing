import boto3
import pandas as pd

def lambda_handler(event, context):
    # Define the source and destination S3 bucket and key
    source_bucket = 'doordash-landing-zone'
    source_key = 'raw_input.json'
    destination_bucket = 'doordash-target-zone'
    destination_key = 'delivered_records.json'
    
    # Initialize S3 client
    s3 = boto3.client('s3')

    try:
        # Read the JSON file from S3 into a pandas DataFrame
        response = s3.get_object(Bucket=source_bucket, Key=source_key)
        print("Get object response is", response)
        data = pd.read_json(response['Body'])
        
        print("data is :", data)
        
        # output_data = pd.read_json(io.BytesIO(response['Body'].read()))
   
        output_df = pd.DataFrame(data)
        
        filtered_data = output_df[output_df['status']=='delivered']

        # Convert DataFrame to JSON string
        output_json = filtered_data.to_json(orient='records', lines=True)

        # Write the output JSON to S3
        s3.put_object(Bucket=destination_bucket, Key=destination_key, Body=output_json.encode('utf-8'))

        return {
            'statusCode': 200,
            'body': 'Output file written to S3 successfully!'
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error processing file: {str(e)}'
        }