from google.cloud import bigquery
import os


def read_from_bigquery(sql_query):
    client = bigquery.Client()
    query = client.query(sql_query)
    return [row for row in query.result()]


project_id = os.getenv('PROJECT_ID')
sql_query = f'SELECT * FROM `{project_id}.my_dataset.my_table` LIMIT 100'
data = read_from_bigquery(sql_query)
for row in data:
    print(row)
