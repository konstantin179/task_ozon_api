import os
import requests
import time
import urllib.request
import csv


class Report:
    def __init__(self, client_id, client_secret):

        self.url = "https://performance.ozon.ru:443"
        self.token_data = {"client_id": client_id,
                           "client_secret": client_secret,
                           "grant_type": "client_credentials"}
        self.token_time = -1800
        self.token = None
        self.headers = {"Authorization": self.token, }

    def get_report(self, date_from, date_to, report_type):
        uuid = self._request_report(date_from, date_to, report_type)
        while True:
            ready = self._report_ready(uuid)
            if ready:
                break
            time.sleep(2)
        link = self._get_link(uuid)
        try:
            response = requests.get(link)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise Exception("Error: " + str(e))
        some_url = ''
        with urllib.request.urlopen(some_url + link) as f:
            report = csv.reader(f.read().decode('utf-8'))

    def _request_report(self, date_from, date_to, report_type):
        self._update_token()
        headers = {"Authorization": self.token["token_type"] + ' ' + self.token["access_token"]}
        try:
            response = requests.post(self.url + "/api/client/vendors/statistics", headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise Exception("Error: " + str(e))
        return response.json()["UUID"]

    def _report_ready(self, uuid):
        self._update_token()
        headers = {"Authorization": self.token["token_type"] + ' ' + self.token["access_token"]}
        try:
            response = requests.get(self.url + "/api/client/statistics/" + uuid, headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise Exception("Error: " + str(e))
        request_state = response.json()["state"]
        if request_state == "ERROR":
            raise Exception("Error: " + response.json()["error"])
        if request_state == "OK":
            return True
        return False

    def _get_link(self, uuid):
        self._update_token()
        headers = {"Authorization": self.token["token_type"] + ' ' + self.token["access_token"]}
        try:
            response = requests.get(self.url + "/api/client/statistics/report", headers=headers, params={"UUID": uuid})
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise Exception("Error: " + str(e))
        return response.json()["contentDisposition"]

    def _update_token(self):
        if self.token:
            time_left = time.time() - self.token_time
            if self.token["expires_in"] > time_left:
                return
        try:
            response = requests.post("https://performance.ozon.ru", data=self.token_data)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise Exception("Error: " + str(e))
        self.token = response.json()
        self.token_time = time.time()
