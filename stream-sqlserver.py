import json 
import random 
import time 
import os 
import yaml 
import shutil 
import pandas as pd 
from datetime import datetime 
from faker import Faker 
import yaml 
import pandas as pd 
from sqlalchemy import create_engine,text, Table, Column, MetaData, Integer, String, Boolean, TIMESTAMP, DateTime
import os 
import jaydebeapi



# Load config
with open("config.yaml") as f:
    config = yaml.safe_load(f)["database_sqlserver"]


# databse configuration 
host = config["host"]
port = config["port"]
dbname = config["name"]
user = config["user"]
password = config["password"]

# Path ke JDBC JAR
jar_path = os.path.join("jars", "mssql-jdbc-12.10.1.jre11.jar")

# JDBC URL
jdbc_url = (
    f"jdbc:sqlserver://{host}:{port};"
    f"databaseName={dbname};"
    f"user={user};"
    f"password={password};"
    f"encrypt=false;"
)

# Buat koneksi
conn = jaydebeapi.connect(
    "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    jdbc_url,
    jars=jar_path
)

TABLE_NAME = 'tbl_sales'


fake = Faker()

CSV_DIR = "database-source"

def get_list_from_csv(column, csv_name):
    try:
        path = os.path.join(CSV_DIR, f"{csv_name}.csv")
        df = pd.read_csv(path)
        return df[column].dropna().astype(int).tolist()
    except Exception as e:
        print(f" Failed read {csv_name}.csv: {e}")
        return []
    
product_list = get_list_from_csv("id", "tbl_product")
customer_list = get_list_from_csv("id", "tbl_customers")
branch_list = get_list_from_csv("id", "tbl_branch")
payment_method_list = get_list_from_csv("id", "tbl_payment_method")
order_status_list = sorted(get_list_from_csv("id", "tbl_order_status"))
payment_status_list = get_list_from_csv("id", "tbl_payment_status")
shipping_status_list = get_list_from_csv("id", "tbl_shipping_status")

# weight status 
order_status_list_weights = [0.7,0.3]
payment_status_list_weights = [0.05, 0.9, 0.05]
shipping_status_list_weights = [0.05, 0.9, 0.05]

def generate_id_from_timestamp(source, ts: datetime):
    return "TX" + source + ts.strftime("%Y%m%d%H%M%S")

def generates_sales_data():
    timestamp = datetime.now()
    source= "SQLS-"
    id = generate_id_from_timestamp(source, timestamp)
    product_id = random.choice(product_list)
    customer_id = random.choice(customer_list)
    branch_id = random.choice(branch_list)
    quantity = random.randint(1,5)
    payment_method = random.choice(payment_method_list)
    order_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    order_status = random.choices(order_status_list, weights=order_status_list_weights, k=1)[0]
    payment_status = None
    shipping_status = None
    is_online_transaction = random.choice([True, False])
    delivery_fee = 0 
    is_free_delivery_fee = None 

    if is_online_transaction:
        delivery_fee = random.randint(12000, 50000)
        is_free_delivery_fee = random.choice([True, False])

    if order_status == 2:
         payment_status = random.choices(payment_status_list, weights=payment_status_list_weights, k=1)[0]

    if payment_status == 2:
         shipping_status = random.choices(shipping_status_list, weights=shipping_status_list_weights, k=1)[0]
         
    if not is_online_transaction:
        shipping_status = None
    print("[DEBUG] order_status_list:", order_status_list)
    print("[DEBUG] order_status_list_weights:", order_status_list_weights)
    print("[DEBUG] result:", random.choices(order_status_list, weights=order_status_list_weights, k=10))
    print(f"[DEBUG] order_status: {order_status}, payment_status: {payment_status}, is_online_transaction: {is_online_transaction}")

    return {
        "id": id,
        "product_id": product_id,
        "customer_id": customer_id,
        "branch_id": branch_id,
        "quantity": quantity,
        "payment_method": payment_method,
        "order_date": order_date,
        "order_status": order_status,
        "payment_status": payment_status,
        "shipping_status": shipping_status, 
        "is_online_transaction": is_online_transaction,
        "delivery_fee": delivery_fee,
        "is_free_delivery_fee": is_free_delivery_fee,
        "created_at": order_date,
        "modified_at": None
    }
# ======================== STREAMING ========================
cursor = conn.cursor()
print("✅ Connected to SQL Server. Streaming dummy data... (CTRL+C to stop)")
try:
    while True:
        data = generates_sales_data()
        insert_query = """
            INSERT INTO tbl_sales (
                id, product_id, customer_id, branch_id, quantity, payment_method, 
                order_date, order_status, payment_status, shipping_status, 
                is_online_transaction, delivery_fee, is_free_delivery_fee, 
                created_at, modified_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        values = (
            data["id"], data["product_id"], data["customer_id"], data["branch_id"],
            data["quantity"], data["payment_method"], data["order_date"],
            data["order_status"], data["payment_status"], data["shipping_status"],
            data["is_online_transaction"], data["delivery_fee"],
            data["is_free_delivery_fee"], data["created_at"], data["modified_at"]
        )

        cursor.execute(insert_query, values)
        conn.commit()
        print(f"✅ Inserted: {data['id']}")
        print("✅ Inserted:")
        print(json.dumps(data, indent=2, default=str))
        time.sleep(2)

except KeyboardInterrupt:
    print("\n⛔ Streaming stopped.")
except Exception as e:
    print(f"❌ Error: {e}")
finally:
    cursor.close()
    conn.close()