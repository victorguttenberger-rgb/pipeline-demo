
from airflow.sdk import BaseOperator
from datetime import datetime

class TimeDiff(BaseOperator):
    def __init__(self, diff_date: datetime, **kwargs):
        super().__init__(**kwargs)
        self.diff_date = diff_date

    def execute(self, context):
        print(datetime.now() - self.diff_date)

    