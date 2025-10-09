
import requests


class APIClient:

    def __init__(self, url):
        self.url = url

    def get_request(self, endpoint=None, params=None):
        url = self.url if endpoint is None else self.url + "/" + endpoint
        response = requests.get(url, params)
        return response.json()
