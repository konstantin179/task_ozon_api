import psycopg2

try:
    connection = psycopg2.connect(dbname="postgres_db", user="postgres")
    cursor = connection.cursor()
    cursor.execute("""CREATE TABLE reports (
                             id SERIAL PRIMARY KEY,
                             client_id INT,
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
                             api_id INT,
                             FOREIGN KEY (client_id)
                                 REFERENCES service_attr (client_id_performance)
    );""")
    connection.commit()
    cursor.close()
    connection.close()
except (Exception, psycopg2.Error) as error:
    print("PostgreSQL error:", error)
