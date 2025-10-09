
from google.cloud import bigquery
from dotenv import load_dotenv

PROJECT_ID = "jovial-honor-474314-k4"
DATASET_ID = "SANDBOX_PIPELINE_DEMO"
INT_DATASET_ID = "INTEGRATION"


def check_for_table(client, table_id):
    try:
        client.get_table(table_id)
        while True:
            reply = input(
                f"WARNING: Table '{table_id}' already exists, do you want to replace it (Y/N): ").strip()
            if reply.lower() == 'y':
                client.delete_table(table_id)
                return True
            elif reply.lower() == 'n':
                return False
    except BaseException:
        return True


if __name__ == "__main__":
    load_dotenv()
    client = bigquery.Client(project=PROJECT_ID)

    table_id = f"{PROJECT_ID}.{DATASET_ID}.countries_from_API"
    if check_for_table(client, table_id):
        table_schema = [
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("cca3", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("languages", "STRING", mode="REPEATED"),
            bigquery.SchemaField("population", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("continents", "STRING", mode="REPEATED"),
            bigquery.SchemaField(
                "ingest_datetime", "DATETIME", mode="REQUIRED")
        ]
        table = bigquery.Table(table_id, schema=table_schema)
        client.create_table(table)
