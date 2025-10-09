
from google.cloud import bigquery


class BQClient:

    def __init__(self, project_id):
        self.client = bigquery.Client(project_id)

    def upload_data(self, dataset_id, table_name, data):
        table_ref = self.client.dataset(dataset_id).table(table_name)
        errors = self.client.insert_rows_json(table_ref, data)
        if errors:
            print(f"errors generated during the upload: {errors}")
