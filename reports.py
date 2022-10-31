import os
import requests
import time
import urllib.request
from pathlib import Path
import psycopg2
import zipfile
import csv
from concurrent.futures import ThreadPoolExecutor


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
        timer = 0
        timestep = 5
        timeout = 20
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
            try:
                with zipfile.ZipFile(file_path, mode="r") as archive:
                    archive.extractall(zipdir_path)
            except zipfile.BadZipFile as e:
                print(f"Bad zipfile. client_id: {client_id}, dates: {date_to} -> {date_from}, type: {report_type}\n{e}")
                return None
            csv_files = zipdir_path.glob('*')
            for file in csv_files:
                self.add_csvfile_to_db(file)
                os.remove(file)
            os.rmdir(zipdir_path)
            os.remove(file_path)
        else:
            self.add_csvfile_to_db(file_path)
            os.remove(file_path)
        print(f"Report saved to db. client_id: {client_id}, dates: {date_to} -> {date_from}, type: {report_type}")

    def add_csvfile_to_db(self, filepath):
        try:
            conn = psycopg2.connect(dbname="postgres_db", user="postgres")
            cur = conn.cursor()
            table_name = filepath.stem
            with open(filepath, 'r', encoding="utf-8") as f:
                reader = csv.reader(f)
                next(reader)  # Skip the header row.
                for row in reader:
                    query_data = [self.token_data["client_id"]].extend(row[1:])
                    cur.execute("""INSERT INTO reports 
                        (client_id, banner, pagetype, viewtype, platfrom, request_type, sku,name, order_id,
                         order_number, ozon_id, ozon_id_ad_sku, articul, empty, account_id, views, clicks, audience,
                         exp_bonus, actionnum, avrg_bid, search_price_rur, search_price_perc, price, orders,
                         revenue_model, orders_model, revenue, expense, cpm, ctr, data, api_id)
                        VALUES 
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                         %s, %s, %s, %s, %s, %s, %s, %s, %s);""", query_data)

            conn.commit()
            cur.close()
            conn.close()
        except (Exception, psycopg2.Error) as e:
            print("PostgreSQL error:", e)

    def _request_report(self, date_from, date_to, report_type):
        self._update_token()
        headers = {"Authorization": self.token["token_type"] + ' ' + self.token["access_token"]}
        try:
            response = requests.post(self.url + "/api/client/vendors/statistics", headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise Exception("Error in request_report: " + str(e))
        return response.json()["UUID"]

    def _report_ready(self, uuid):
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
        self._update_token()
        headers = {"Authorization": self.token["token_type"] + ' ' + self.token["access_token"]}
        try:
            response = requests.get(self.url + "/api/client/statistics/report", headers=headers, params={"UUID": uuid})
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise Exception("Error in get_link: " + str(e))
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
            raise Exception("Error in update_token: " + str(e))
        self.token = response.json()
        self.token_time = time.time()


def get_client_reports(client_id, client_secret, dates_and_types):
    client_report = Report(client_id, client_secret)
    for date_from, date_to, report_type in dates_and_types:
        client_report.get_report(date_from, date_to, report_type)


date_from = "2022-08-01"
date_to = "2022-10-25"
report_type = "TRAFFIC_SOURCES"  # or "ORDERS"
report_dates_and_types = [(date_from, date_to, report_type), ]
clients_and_dates = []
try:
    connection = psycopg2.connect(dbname="postgres_db", user="postgres")
    cursor = connection.cursor()
    cursor.execute("SELECT client_id_performance, client_secret_performance FROM service_attr;")
    for client_id, client_secret in cursor.fetchall():
        clients_and_dates.append((client_id, client_secret, report_dates_and_types))
    cursor.close()
    connection.close()
except (Exception, psycopg2.Error) as error:
    print("PostgreSQL error:", error)

with ThreadPoolExecutor(16) as executor:
    executor.map(get_client_reports, clients_and_dates)
