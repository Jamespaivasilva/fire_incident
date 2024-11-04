import boto3
import pandas as pd
import mysql.connector

# AWS credentials
AWS_ACCESS_KEY_ID = 'private'
AWS_SECRET_ACCESS_KEY = 'YOUR_SECRET_ACCESS_KEY'

# MySQL credentials
MYSQL_HOST = 'localhost'
MYSQL_USER = '3306'
MYSQL_PASSWORD = 'local123 '
MYSQL_DATABASE = 'incidents'

MYSQL_TABLE = 'incidents.fire_incidents_tmp'

# S3 bucket and file details
BUCKET_NAME = 'bronze-dl55060'
FILE_KEY = 'wr8u-xric_version_5505'

def download_from_s3(bucket_name, file_key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    return obj['Body'].read()

def load_to_mysql(df, mysql_host, mysql_user, mysql_password, mysql_database, mysql_table):
    mydb = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )

    mycursor = mydb.cursor() 
    #truncate temp table
    sql = "TRUNCATE TABLE incidents.fire_incidents_tmp"
    mycursor.execute(sql)
    
    #Insert into temp table
    for index, row in df.iterrows():
        sql = "INSERT INTO " + mysql_table + " VALUES ("
        sql += ', '.join(['%s'] * len(row)) + ")"
        val = tuple(row)
        mycursor.execute(sql, val)

    mydb.commit()
    mycursor.close()
    mydb.close()

if __name__ == "__main__":
    # Download the file from S3
    file_content = download_from_s3(BUCKET_NAME, FILE_KEY)

    # Read the file into a pandas DataFrame
    df = pd.read_csv(io.BytesIO(file_content))

    # Load the DataFrame into MySQL
    load_to_mysql(df, MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_TABLE)