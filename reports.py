import os
import requests
import time
import urllib.request
from pathlib import Path
import psycopg2
import zipfile
import csv
import datetime
import csv
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from concurrent.futures import ThreadPoolExecutor


def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join(['"{}"'.format(k) for k in keys])
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


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
        client_id = self.token_data["client_id"]
        df = pd.read_csv(filepath, encoding="utf-8")
        # Drop first column with id.
        df.drop(columns=df.columns[0], axis=1, inplace=True)
        # Place client_id to api_id field.
        df["api_id"] = client_id
        try:
            engine = create_engine('postgresql://myusername:mypassword@myhost:5432/mydatabase')
            df.to_sql('reports', engine, if_exists='append', method=psql_insert_copy)
        except (Exception, SQLAlchemyError) as e:
            print("DB error:", e)

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


def delete_duplicates_from_db_table(table_name):
    try:
        conn = psycopg2.connect(dbname="postgres_db", user="postgres")
        cur = conn.cursor()
        delete_query = f"""DELETE FROM {table_name} 
                            WHERE ctid IN 
                                (SELECT ctid 
                                   FROM (SELECT ctid,
                                                row_number() OVER (PARTITION BY pagetype, sku, data, api_id
                                                ORDER BY id DESC) AS row_num
                                         FROM {table_name}
                                        ) t
                                  WHERE t.row_num > 1
                                );"""
        cur.execute(delete_query)
        conn.commit()
        cur.close()
        conn.close()
    except (Exception, psycopg2.Error) as e:
        print("PostgreSQL error:", e)


def get_accounts_data_from_db():
    attribute_name = {8: "client_secret", 9: "client_id"}
    accounts_data = {}  # {acc_id: {"client_id": ,"client_secret": }, }
    used_clients = set()
    try:
        connection = psycopg2.connect(dbname="postgres_db", user="postgres")
        cursor = connection.cursor()
        cursor.execute("""SELECT al.id, asd.attribute_id, asd.attribute_value
                            FROM account_list al JOIN account_service_data asd ON al.id = asd.account_id
                           WHERE al.mp_id = 14 AND al.status = 'Active'
                           ORDER BY al.id, asd.attribute_id DESC;""")
        used_acc_id = None
        for acc_id, attribute_id, attribute_value in cursor.fetchall():
            if acc_id == used_acc_id:
                continue
            if attribute_name[attribute_id] == "client_id":
                if attribute_value in used_clients:
                    used_acc_id = acc_id
                    continue
                else:
                    used_clients.add(attribute_value)
            if acc_id not in accounts_data:
                accounts_data[acc_id] = {}
            accounts_data[acc_id][attribute_name[attribute_id]] = attribute_value
        cursor.close()
        connection.close()
    except (Exception, psycopg2.Error) as error:
        print("PostgreSQL error:", error)
    return accounts_data


date_from = "2022-08-01"
date_to = "2022-10-25"
report_type = "TRAFFIC_SOURCES"  # or "ORDERS"
report_dates_and_types = [(date_from, date_to, report_type), ]
clients_and_dates = []
accounts_data = get_accounts_data_from_db()

for acc_id, acc_data in accounts_data.values():
    clients_and_dates.append((acc_data["client_id"], acc_data["client_secret"], report_dates_and_types))

with ThreadPoolExecutor(16) as executor:
    executor.map(get_client_reports, clients_and_dates)

delete_duplicates_from_db_table("reports")
