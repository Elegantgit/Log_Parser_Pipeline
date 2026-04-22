from google.cloud import bigquery
import pandas as pd

client = bigquery.Client()
project_id = "dataintershipprogram"
dataset = "LogParser"
table_name = "mongo_log"

table_id = "dataintershipprogram.LogParser.mongo_log"


schema = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("action", "STRING"),
    bigquery.SchemaField("success", "BOOLEAN"),
    bigquery.SchemaField("gateway", "STRING"),
    bigquery.SchemaField("ref", "STRING"),
    bigquery.SchemaField("service", "STRING"),
    bigquery.SchemaField("time", "TIMESTAMP"),
    bigquery.SchemaField("created", "TIMESTAMP"),

    bigquery.SchemaField("amount", "FLOAT"),
    bigquery.SchemaField("confirmationTime", "TIMESTAMP"),
    bigquery.SchemaField("customerAddress", "STRING"),
    bigquery.SchemaField("customerMeterNumber", "STRING"),
    bigquery.SchemaField("debtAmount", "FLOAT"),
    bigquery.SchemaField("initiationTime", "TIMESTAMP"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("units", "FLOAT"),
    bigquery.SchemaField("unitsType", "STRING"),
    bigquery.SchemaField("value", "STRING"),
    bigquery.SchemaField("vat", "FLOAT"),
]


def load_batch(records, table_id):
    df = pd.DataFrame(records)

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_APPEND",
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()