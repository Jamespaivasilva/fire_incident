import boto3
import pandas as pd
import mysql.connector
import os
import logging

# AWS credentials
aws_access_key_id = os.environ.get('aws_access_key_id')
aws_secret_access_key = os.environ.get('aws_secret_access_key')

logging.basicConfig(filename='app.log', level=logging.INFO)
# MySQL credentials
mysql_host = 'localhost'
mysql_user = os.environ.get('mysql_user')
mysql_password = os.environ.get('mysql_password')
mysql_datababase = 'incidents'
mysql_port = '3306'

mysql_table = 'incidents.fire_incidents_tmp'

# S3 bucket and file details
bucket_name = 'bronze-dl55060'
file_key = 'wr8u-xric_version_5505'

def download_from_s3(bucket_name, file_key):
    try:
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        return obj['Body'].read()
    except Exception as e:
        print(e)
        

def establishConnection(mysql_host, mysql_user, mysql_password, mysql_database, mysql_table):
    mydb = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    mycursor = mydb.cursor() 
    
    return mycursor
    

def truncate(mycursor,mysql_table):
    sql = f"TRUNCATE TABLE {mysql_table}"
    try:
        result = mycursor.execute(sql)
    except Exception as e:
        print(e)
    mycursor.commit()

    return result

def load_to_mysql(df,mycursor):
    #Insert into temp table
    try:
        logger.info(f'Inserting {len(df)} rows into tmp table')
        for index, row in df.iterrows():
            sql = "INSERT INTO " + mysql_table + " VALUES ("
            sql += ', '.join(['%s'] * len(row)) + ")"
            val = tuple(row)
            mycursor.execute(sql, val)
    except Exception as e:
        print(e)

    inserted_rows = mycursor.rowcount
    logger.info('Validating insert result')
    if len(df) == inserted_rows:
        logger.info(f'Number of rows is ok!')
        mycursor.commit()
    else:
        logger.info(("Some rows failed to insert.")
        raise ExceptionType("Some rows failed to insert. The code must be checked!")

if __name__ == "__main__":
    # Download the file from S3
    try:
        logger.info('Downloading s3 file')
        file_content = download_from_s3(bucket_name, file_key)
    except Exception as e:
        print(e)
    #EstabilishConnection
    mycursor = establishConnection(mysql_host, mysql_user, mysql_password, mysql_database, mysql_table)
    logger.info('Truncating tmp table')
    # truncate tmp table
    truncate(mycursor)
    # Read the file into a pandas DataFrame
    logger.info('Reading file as pandas dataframe')
    df = pd.read_csv(io.BytesIO(file_content))

    # Load the DataFrame into MySQL
    logger.info('Loading data into tmp table')
    #renew connection to commit again
    mycursor = establishConnection(mysql_host, mysql_user, mysql_password, mysql_database, mysql_table)
    load_to_mysql(df,mycursor)
    
    mycursor.close()
    
        
