
from APIClient import APIClient
from BQClient import BQClient
from datetime import datetime
import os
from dotenv import load_dotenv

API_URL = "https://restcountries.com"
API_ENDPOINT = "v3.1/all"
PARAMS = {
    "fields": "cca3,name,languages,population,continents"
}


if __name__ == "__main__":

    load_dotenv()
    api = APIClient(API_URL)
    response = api.get_request(API_ENDPOINT, PARAMS)
    for country in response:
        country['name'] = country['name']['official']
        country['languages'] = list(country['languages'].values())
        country['ingest_datetime'] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S")
    client = BQClient(os.getenv("PROJECT_ID"))
    client.upload_data(os.getenv("DATASET_ID"), os.getenv("TABLE_NAME"), response)
