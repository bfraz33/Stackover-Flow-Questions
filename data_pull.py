import json
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
import snowflake.connector as sf
import boto3

snowflake_credentials = {
    "user": "bfraiz",
    "password": "Lakers33",
    "account": "kveyluu-fy59452",
    "warehouse": "DATA_WH",
    "database": "STACKOVER_FLOW",
    "schema": "EXTERNAL_DATA"
}

output_file = output_file = "C:/Users/bfraz/Snowflake/creds.txt"
def kaggle_auth_setup(kaggle_auth, output_file):

    try:
        data = json.loads(kaggle_auth)
        with open(output_file, 'w') as file:
            json.dump(data, file, indent=4)
        print('Setup Successful')
    except:
        print("Setup Failed")
        
kaggle_auth_setup('{"username": "bfraiz33", "key": "9c5486a1483afd0986934880af2b5ad6"}', 'creds.txt')

csv_filename = "TotalQuestions.csv"
df = pd.read_csv('C:/Users/bfraz/Snowflake/creds.txt')
print(df.head())

try:
    # Initialize Boto3 client properly
    s3 = boto3.client('s3')

    s3_bucket_name = "project-stack-data"
    s3_folder_path = "project-stack-data/python/stackover_flow"

    # Upload the CSV file to S3
    with open(csv_filename, "rb") as f:
        s3.upload_fileobj(f, s3_bucket_name, s3_folder_path + csv_filename)
        print("CSV file uploaded to S3 successfully!")

    con = sf.connect(
        user=snowflake_credentials["user"],
        password=snowflake_credentials["password"],
        account=snowflake_credentials["account"],
        warehouse=snowflake_credentials["warehouse"],
        database=snowflake_credentials["database"],
        schema=snowflake_credentials["schema"]
    )
    print("Connection to Snowflake established successfully!")

    # Load DataFrame into Snowflake
    cur = con.cursor()

    # Create table in Snowflake
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stack_overflow_questions (
            month DATE,
            programming_language VARCHAR,
            total_questions INT
        )
    """)
    
    print("Table created successfully!")

    cur.execute("""
       CREATE OR REPLACE STAGE AWS_STAGE
       URL = 's3://project-stack-data/project-stack-data/python/'
       CREDENTIALS = (
           AWS_KEY_ID='AKIA2UC26SN4WIB7GGML',
           AWS_SECRET_KEY='gcXx/0YcW92mIXlRjIWVAkT4SmXBCjw6uTfM5GDQ'
)""")
                
    print("Stage created successfully!")

    cur.execute("""
    COPY INTO STACKOVER_FLOW.EXTERNAL_DATA.STACK_OVERFLOW_QUESTIONS
       FROM @STACKOVER_FLOW.EXTERNAL_DATA.AWS_STAGE
       FILE_FORMAT=(TYPE = CSV FIELD_DELIMITER=',' SKIP_HEADER=1)
       FILES = ('TotalQuestions.csv')
       ON_ERROR = 'CONTINUE';
""")

   
    print("Data loaded into snowflake table successfully!")

except Exception as e:
    print(f"An error occurred: {e}")

#AKIA2UC26SN4WIB7GGML
#gcXx/0YcW92mIXlRjIWVAkT4SmXBCjw6uTfM5GDQ