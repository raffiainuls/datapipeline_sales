import logging 
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_tbl_sales():
    create_sql_tbl_sales = """
        CREATE TABLE tbl_sales (
            id string,
            product_id INT,
            customer_id INT,
            branch_id INT,
            quantity INT,
            payment_method INT,
            order_date STRING,
            order_status INT,
            payment_status FLOAT,
            shipping_status FLOAT,
            is_online_transaction BOOLEAN,
            delivery_fee INT,
            is_free_delivery_fee STRING,
            created_at STRING,
            modified_at STRING,
            PRIMARY KEY (id) NOT ENFORCED
    )  WITH (
            'connector' = 'upsert-kafka',
            'topic' = 'transaction_tbl_sales',
            'properties.bootstrap.servers' = 'host.docker.internal:9093',
            'key.format' = 'json',
            'value.format' = 'json'
        )
        """
    return create_sql_tbl_sales
