import logging 
import sys 
import traceback 
from pyflink.table import EnvironmentSettings, TableEnvironment
from helper.function import create_env, create_table_if_not_exists
from transaction_postgres.lib import create_tbl_sales_postgres, insert_into_tbl_sales
from transactions.lib import create_tbl_sales
logging.basicConfig(level=logging)
logger = logging.getLogger(__name__)

table_env = create_env()
tbl_sales_postgres = create_tbl_sales_postgres()
create_table_if_not_exists(table_env, "tbl_sales_postgres", tbl_sales_postgres)


def create_print_sink(table_env):
    table_env.execute_sql("""
        CREATE TABLE print_sink (
             id INT,
    product_id INT,
    customer_id INT,
    branch_id INT,
    quantity INT,
    payment_method INT,
    order_date BIGINT,
    order_status INT,
    payment_status FLOAT,
    shipping_status FLOAT,
    is_online_transaction BOOLEAN,
    delivery_fee INT,
    is_free_delivery_fee STRING,
    created_at BIGINT,
    modified_at BIGINT
        ) WITH (
            'connector' = 'print'
        )
    """)

# Create a clean query that flattens payload and filters null id
result_table = table_env.sql_query("""
    SELECT 
        payload.id,
        payload.product_id,
        payload.customer_id,
        payload.branch_id,
        payload.quantity,
        payload.payment_method,
        payload.order_date,
        payload.order_status,
        payload.payment_status,
        payload.shipping_status,
        payload.is_online_transaction,
        payload.delivery_fee,
        payload.is_free_delivery_fee,
        payload.created_at,
        payload.modified_at
    FROM tbl_sales_postgres
    WHERE payload.id IS NULL
""")


table_env.execute_sql("""
CREATE TABLE print_sink (
    id INT,
    product_id INT,
    customer_id INT,
    branch_id INT,
    quantity INT,
    payment_method INT,
    order_date BIGINT,
    order_status INT,
    payment_status FLOAT,
    shipping_status FLOAT,
    is_online_transaction BOOLEAN,
    delivery_fee INT,
    is_free_delivery_fee STRING,
    created_at BIGINT,
    modified_at BIGINT
) WITH (
    'connector' = 'print'
)
""")



result_table.execute_insert("print_sink").wait()




