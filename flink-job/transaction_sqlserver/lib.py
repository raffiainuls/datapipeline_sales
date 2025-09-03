import logging 
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_tbl_sales_sqlserver():
    create_sql_tbl_sales = """
        CREATE TABLE tbl_sales_sqlserver (
        payload ROW<
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
            modified_at STRING
        >
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'sqlserver-source1.salesdb.dbo.tbl_sales',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
    """
    return create_sql_tbl_sales

def insert_into_tbl_sales(table_env):
    logger.info("🔁 Inserting data into sink table 'tbl_sales'....")
    table_env.execute_sql(
        f"""
    INSERT INTO tbl_sales
    SELECT 
    payload.id, 
    payload.product_id,
    payload.customer_id, 
    payload.branch_id,
    payload.quantity,
    payload.payment_method,
    DATE_FORMAT(
    TO_TIMESTAMP_LTZ(CAST(payload.order_date AS BIGINT) / 1000000, 3),
    'yyyy-MM-dd HH:mm:ss'
    ) AS order_date,
    payload.order_status,
    payload.payment_status, 
    payload.shipping_status, 
    payload.is_online_transaction,
    payload.delivery_fee,
    payload.is_free_delivery_fee,
    DATE_FORMAT(
    TO_TIMESTAMP_LTZ(CAST(payload.created_at AS BIGINT) / 1000000, 3),
    'yyyy-MM-dd HH:mm:ss'
    ) as created_at,
    DATE_FORMAT(
    TO_TIMESTAMP_LTZ(CAST(payload.modified_at AS BIGINT) / 1000000, 3),
    'yyyy-MM-dd HH:mm:ss'
    ) as modified_at
    from tbl_sales_sqlserver                                 
    """)
    logger.info("✅ Data inserted into sink table 'tbl_sales'")