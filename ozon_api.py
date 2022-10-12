import os
import requests

CLIENT_ID = os.getenv('CLIENT_ID')
API_KEY = os.getenv('API_KEY')


class OzonApi:
    def __init__(self, client_id, api_key):
        self.url = "https://api-seller.ozon.ru"
        self.headers = {"Client-Id": client_id, "Api-Key": api_key}

    def get_proms(self):
        """A method for getting a list of promotions."""
        try:
            response = requests.post(self.url + "/v1/actions", headers=self.headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            return "Error: " + str(e)
        return response.json()["result"]

    def get_prom_candidates(self):
        """A method for getting a dict with proms and lists of products that can participate in these proms."""
        proms = self.get_proms()
        candidates = {}
        limit = 100        # Max number of values in the response.
        for prom in proms:
            offset = 0     # Number of elements that will be skipped in the response.
            count = 100    # Number of values in the response.
            while count >= 100:
                body = {"action_id": prom['id'], "limit": limit, "offset": offset}
                try:
                    response = requests.post(self.url + "/v1/actions/candidates", headers=self.headers, data=body)
                    response.raise_for_status()
                except requests.exceptions.RequestException as e:
                    return "Error: " + str(e)
                products = response.json()["result"]["products"]
                count = len(products)
                offset = count
                if prom['title'] not in candidates:
                    candidates[prom['title']] = []
                candidates[prom['title']].extend(products)
        return candidates

    def get_prom_conditions(self):
        """A method for getting conditions for adding products to promotions."""
        pass

    def add_products_to_prom(self):
        """A method for adding products to eligible promotions."""
        pass
