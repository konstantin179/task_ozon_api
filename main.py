import os
from PerformanceOzonApi import PerformanceOzonClient
from postgres import DB
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv


def get_client_reports(client_id, client_secret, dates_and_types):
    """Downloads reports for a client and writes them to the database."""
    client = PerformanceOzonClient(client_id, client_secret)
    for date_from, date_to, report_type in dates_and_types:
        client.get_report(date_from, date_to, report_type)


if __name__ == "__main__":
    date_from = "2022-08-01"
    date_to = "2022-10-25"
    report_type = "TRAFFIC_SOURCES"  # or "ORDERS"
    report_dates_and_types = [(date_from, date_to, report_type), ]

    load_dotenv()
    conn_string = os.getenv("DB_CONN_STR")
    with DB(conn_string) as db:
        accounts_data = db.get_accounts_data()

    clients_and_dates = []
    for acc_id, acc_data in accounts_data.values():
        clients_and_dates.append((acc_data["client_id"], acc_data["client_secret"], report_dates_and_types))

    with ThreadPoolExecutor(16) as executor:
        executor.map(get_client_reports, clients_and_dates)

    with DB(conn_string) as db:
        db.delete_duplicates_from_table("reports")
