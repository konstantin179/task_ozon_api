import os
import requests
import time
import urllib.request
from pathlib import Path
import psycopg2
import zipfile
import csv


connection = psycopg2.connect(dbname="postgres_db", user="postgres")
cursor = connection.cursor()
cursor.execute("SELECT client_id, client_secret FROM client_info;")
CLIENT_ID, CLIENT_SECRET = cursor.fetchone()
cursor.close()
connection.close()


class Report:
    def __init__(self, client_id, client_secret):

        self.url = "https://performance.ozon.ru:443"
        self.token_data = {"client_id": client_id,
                           "client_secret": client_secret,
                           "grant_type": "client_credentials"}
        self.token_time = -1800
        self.token = None

    def get_report(self, date_from, date_to, report_type):
        uuid = self._request_report(date_from, date_to, report_type)
        while True:
            ready = self._report_ready(uuid)
            if ready:
                break
            time.sleep(2)
        link = self._get_link(uuid)
        file_extension = link[-4:]
        filename = uuid + file_extension
        tmp_path = Path("tmp")
        tmp_path.mkdir(exist_ok=True)
        file_path = tmp_path / filename
        some_url = ''
        urllib.request.urlretrieve(some_url + link, file_path)
        if file_extension == '.zip':
            zipdir_path = file_path.with_suffix('')
            zipdir_path.mkdir()
            with zipfile.ZipFile(file_path, mode="r") as archive:
                archive.extractall(zipdir_path)
            csv_files = zipdir_path.glob('*')
            for file in csv_files:
                self.add_csvfile_to_db(file)
                os.remove(file)
            os.rmdir(zipdir_path)
            os.remove(file_path)
        else:
            self.add_csvfile_to_db(file_path)
            os.remove(file_path)

    @staticmethod
    def add_csvfile_to_db(filepath):
        conn = psycopg2.connect(dbname="postgres_db", user="postgres")
        cur = conn.cursor()
        table_name = filepath.stem
        with open(filepath, 'r', encoding="utf-8") as f:
            reader = csv.reader(f)
            # Headers without id
            headers = next(reader)[1:]
        query_info = [table_name].extend(headers)
        # Change the query to match the actual file!
        cur.execute("""CREATE TABLE %s (
            id integer PRIMARY KEY,
            %s text,
            %s text,
            %s text
        );
        """, query_info)
        with open(filepath, 'r', encoding="utf-8") as f:
            next(f)  # Skip the header row.
            cur.copy_from(f, table_name, sep=',')
        conn.commit()
        cur.close()
        conn.close()

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
