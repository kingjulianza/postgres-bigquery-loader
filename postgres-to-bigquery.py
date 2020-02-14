import psycopg2
import os
import sys
import time
from datetime import datetime, timedelta
from google.cloud import bigquery


def extract_from_postgres(**kwargs):
    table_name = os.getenv('TABLE_NAME', kwargs.get('table_name'))
    project_id = os.getenv('PROJECT_ID', kwargs.get('project_id'))
    date_yesterday =  (datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d'))
    file_path = os.getenv('CSV_FILE_PATH', kwargs.get('csv_file_path'))
    database_url = os.getenv('DATABASE_URL', kwargs.get('database_url'))

    print (database_url)

    print ("Extracting {} for {}: {} to {}".format(table_name, project_id,
        date_yesterday, file_path))

    conn = psycopg2.connect(database_url)
    db_cursor = conn.cursor()

    sql = ("COPY (select t.*, '{}' as source from {} as t where timestamp::date='{}') TO STDOUT WITH CSV HEADER"
            .format(project_id, table_name, date_yesterday))

    with open(file_path, 'w') as f_output:
        db_cursor.copy_expert(sql, f_output)

    print ("Done extracting to file")

    db_cursor.close()
    conn.close()

def upload_to_bigquery(**kwargs):
    project_id = os.getenv('PROJECT_ID', kwargs.get('project_id'))
    table_name = os.getenv('TABLE_NAME', kwargs.get('table_name'))
    file_path = os.getenv('CSV_FILE_PATH', kwargs.get('csv_file_path'))
    bq_instance = os.getenv('BQ_INSTANCE', kwargs.get('bq_instance'))
    bq_dataset = bq_instance + "." + table_name

    print ("Starting upload to bigquery")
    client = bigquery.Client(project_id)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = 'CSV'
    job_config.allow_quoted_newlines = True
    job_config.ignore_unknown_values  = True
    job_config.max_bad_records = 100
    job_config.skip_leading_rows = 1

    with open(file_path, 'rb') as f_input:
        job = client.load_table_from_file(
                f_input, bq_dataset, job_config=job_config)
        job.result()

    print ("Done uploading to Bigquery")

if __name__ == "__main__":
    extract_from_postgres(
            **dict(arg.split('=') for arg in sys.argv[1:])
    )

    upload_to_bigquery(
            **dict(arg.split('=') for arg in sys.argv[1:])
    )
