import os
import requests
import time
import urllib.request
from pathlib import Path
import zipfile
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from postgres import psql_insert_copy
from dotenv import load_dotenv


class PerformanceOzonClient:
    """Class represents a client to work with Ozon Performance API."""
    def __init__(self, client_id, client_secret):
        self.url = "https://performance.ozon.ru:443"
        self.token_data = {"client_id": client_id,
                           "client_secret": client_secret,
                           "grant_type": "client_credentials"}
        self.token_time = -1800
        self.token = None

    def get_report(self, date_from, date_to, report_type):
        """Downloads report and writes it in db."""
        uuid = self._request_report(date_from, date_to, report_type)
        client_id = self.token_data["client_id"]
        timer = 0
        timestep = 5
        timeout = 20
        ready = False
        while timer < timeout:
            ready = self._report_ready(uuid)
            if ready:
                break
            time.sleep(timestep)
            timer += timestep
        if not ready:
            print(f"Report is not ready. client_id: {client_id}, dates: {date_to} -> {date_from}, type: {report_type}")
            return None
        link = self._get_link(uuid)
        file_extension = Path(link).suffix
        filename = uuid + file_extension
        tmp_path = Path("tmp")
        tmp_path.mkdir(exist_ok=True)
        file_path = tmp_path / filename
        some_url = ''
        urllib.request.urlretrieve(some_url + link, file_path)
        if file_extension == '.zip':
            # extract csv files from zip and add them to db
            zipdir_path = file_path.with_suffix('')
            zipdir_path.mkdir()
            try:
                with zipfile.ZipFile(file_path, mode="r") as archive:
                    archive.extractall(zipdir_path)
            except zipfile.BadZipFile as e:
                print(f"Bad zipfile. client_id: {client_id}, dates: {date_to} -> {date_from}, type: {report_type}\n{e}")
                return None
            csv_files = zipdir_path.glob('*')
            for file in csv_files:
                self._add_csvfile_to_db(file)
                os.remove(file)
            os.rmdir(zipdir_path)
            os.remove(file_path)
        else:    # add one csv file to db
            self._add_csvfile_to_db(file_path)
            os.remove(file_path)
        print(f"Report saved to db. client_id: {client_id}, dates: {date_to} -> {date_from}, type: {report_type}")

    def _add_csvfile_to_db(self, filepath):
        """Adds report's data from csv to db."""
        client_id = self.token_data["client_id"]
        df = pd.read_csv(filepath, encoding="utf-8")
        # Drop first column with id.
        df.drop(columns=df.columns[0], axis=1, inplace=True)
        # Place client_id to api_id field.
        df["api_id"] = client_id
        # Change date format in df["data"]
        df["data"] = pd.to_datetime(df["data"])
        try:
            load_dotenv()
            sqlalch_db_conn_str = os.getenv("SQLALCH_DB_CONN")
            # Like 'postgresql://myusername:mypassword@myhost:5432/mydatabase'
            engine = create_engine(sqlalch_db_conn_str)
            df.to_sql('reports', engine, if_exists='append', method=psql_insert_copy)
        except (Exception, SQLAlchemyError) as e:
            print("DB error:", e)

    def _request_report(self, date_from, date_to, report_type):
        """Requests report from ozon performance.
        Returns report's UUID."""
        self._update_token()
        headers = {"Authorization": self.token["token_type"] + ' ' + self.token["access_token"]}
        try:
            response = requests.post(self.url + "/api/client/vendors/statistics", headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise Exception("Error in request_report: " + str(e))
        return response.json()["UUID"]

    def _report_ready(self, uuid):
        """Checks if report is ready.
        Returns True if ready, False if not."""
        self._update_token()
        headers = {"Authorization": self.token["token_type"] + ' ' + self.token["access_token"]}
        try:
            response = requests.get(self.url + "/api/client/statistics/" + uuid,
                                    headers=headers, params={"vendor": True})
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise Exception("Error in report_ready: " + str(e))
        request_state = response.json()["state"]
        if request_state == "ERROR":
            raise Exception("Error in report_ready: " + response.json()["error"])
        if request_state == "OK":
            return True
        return False

    def _get_link(self, uuid):
        """Returns link to download report file."""
        self._update_token()
        headers = {"Authorization": self.token["token_type"] + ' ' + self.token["access_token"]}
        try:
            response = requests.get(self.url + "/api/client/statistics/report", headers=headers, params={"UUID": uuid})
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise Exception("Error in get_link: " + str(e))
        return response.json()["contentDisposition"]

    def _update_token(self):
        """Updates the token if its time has expired."""
        if self.token:
            time_left = time.time() - self.token_time
            if self.token["expires_in"] > time_left:
                return
        try:
            response = requests.post("https://performance.ozon.ru", data=self.token_data)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise Exception("Error in update_token: " + str(e))
        self.token = response.json()
        self.token_time = time.time()








