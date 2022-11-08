import psycopg2
import os
import csv
from io import StringIO
from dotenv import load_dotenv


class DB:
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.connection = None
        self.connect()

    def connect(self):
        if not self.connection:
            try:
                self.connection = psycopg2.connect(self.connection_string)
            except (Exception, psycopg2.Error) as error:
                print("PostgreSQL error:", error)
        return self.connection

    def __enter__(self):
        return self

    def create_reports_table(self):
        """Create table in db for reports data."""
        try:
            cursor = self.connection.cursor()
            cursor.execute("""CREATE TABLE reports (
                                     id SERIAL PRIMARY KEY,
                                     banner VARCHAR,
                                     pagetype VARCHAR(20),
                                     viewtype VARCHAR,                            
                                     platfrom VARCHAR,
                                     request_type VARCHAR,
                                     sku INT,
                                     name VARCHAR,
                                     order_id INT,
                                     order_number INT,
                                     ozon_id INT,
                                     ozon_id_ad_sku INT,
                                     articul INT,
                                     empty INT,
                                     account_id INT,
                                     views INT,
                                     clicks INT,
                                     audience INT,
                                     exp_bonus INT,
                                     actionnum INT,
                                     avrg_bid FLOAT,
                                     search_price_rur FLOAT,
                                     search_price_perc FLOAT,
                                     price FLOAT,
                                     orders INT,
                                     revenue_model INT,
                                     orders_model INT,
                                     revenue FLOAT,
                                     expense FLOAT,
                                     cpm INT,
                                     ctr FLOAT,
                                     data DATE,
                                     api_id INT
            );""")
            self.connection.commit()
            cursor.close()
        except (Exception, psycopg2.Error) as error:
            print("PostgreSQL error:", error)

    def delete_duplicates_from_table(self, table_name):
        """Delete duplicates from table."""
        try:
            cursor = self.connection.cursor()
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
            cursor.execute(delete_query)
            self.connection.commit()
            cursor.close()
        except (Exception, psycopg2.Error) as e:
            print("PostgreSQL error:", e)

    def get_accounts_data(self):
        """Return accounts data (dict) from db."""
        attribute_name = {8: "client_secret", 9: "client_id"}
        accounts_data = {}  # {acc_id: {"client_id": ,"client_secret": }, }
        used_clients = set()
        try:
            cursor = self.connection.cursor()
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
        except (Exception, psycopg2.Error) as error:
            print("PostgreSQL error:", error)
        return accounts_data

    def close(self):
        if self.connection:
            self.connection.close()

    def __exit__(self):
        self.close()


# Method to inserting pandas DataFrame into db with df.to_sql()
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


if __name__ == "__main__":
    load_dotenv()
    conn_string = os.getenv("DB_CONN_STR")
    if conn_string:
        with DB(conn_string) as db:
            db.create_reports_table()
    else:
        print("DB connection string is not found.")
