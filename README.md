# postgres-bigquery-loader
This is a simple python 3.6 script which will take all data from the previous day in postgres, export it to a csv and then import it into Google's big query product. This was mainly written for use in our company, but I could not find anything similar on the web, so decided to make it public for anybody else to use.

## Use
First install the required libs using the requirements.txt file and then simply run the script on command line by doing `python postgres-to-bigquery.py <options>`

The script can either take environement variables, or passed into the script using kwargs, however the environment variable `GOOGLE_APPLICATION_CREDENTIALS` needs to always be present and has to point to the absolute path of the Google service account key which you generate. 

The following variables are available:
| Option | Environement Variable | Use |
| --- | --- | ---|
| table_name | TABLE_NAME | The table name you wish to export your data from, this will be the same table you use to import onto BQ|
| project_id | PROJECT_ID | Google Cloud project ID where the BQ instance is hosted |
| csv_file_path | CSV_FILE_PATH | Temporary path to the exported csv file |
| database_url | DATABASE_URL | Database URL for the export, in the format postgres://user:password@ip:port/db_name |
| bq_instance | BQ_INSTANCE | Name of the Bigquery Dataset|
| | GOOGLE_APPLICATION_CREDENTIALS | Path to the google service account key |



