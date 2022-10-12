import os
import requests

CLIENT_ID = os.getenv('CLIENT_ID')
API_KEY = os.getenv('API_KEY')


class OzonApi:
    def __init__(self, client_id, api_key):
        self.url = "https://api-seller.ozon.ru"
        self.headers = {"Client-Id": client_id, "Api-Key": api_key}

    def get_promotions(self):
        """A method for getting a list of promotions."""
        try:
            response = requests.post(self.url + "/v1/actions", headers=self.headers)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            return "Error: " + str(e)
        return response.json()["result"]

    def get_promotion_candidates(self, action_id, limit=100, offset=0):
        """
        A method for getting a list of products that can participate in the promotion by the promotion identifier.
        action_id -- Promotion identifier.
        limit -- Number of values in the response.
        offset -- Number of elements that will be skipped in the response.
        """
        body = {"action_id": action_id, "limit": limit, "offset": offset}
        try:
            response = requests.post(self.url + "/v1/actions/candidates", headers=self.headers, data=body)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            return "Error: " + str(e)
        return response.json()["result"]["products"]

    def get_promotion_conditions(self):
        pass

