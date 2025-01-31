import os
import boto3
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_dynamodb_tables():
    """Get DynamoDB table resources."""
    dynamodb = boto3.resource('dynamodb', region_name=os.environ.get('AWS_REGION'))
    queue_table = dynamodb.Table('mv_queue')
    history_table = dynamodb.Table('mv_history')
    return queue_table, history_table

def get_redshift_connection():
    """Get Redshift connection."""
    return psycopg2.connect(
        host=os.environ.get('REDSHIFT_HOST'),
        dbname=os.environ.get('REDSHIFT_DB_NAME'),
        port=os.environ.get('REDSHIFT_PORT', 5439),
        user=os.environ.get('REDSHIFT_USER_NAME'),
        password=os.environ.get('REDSHIFT_PASSWORD'),
    ) 