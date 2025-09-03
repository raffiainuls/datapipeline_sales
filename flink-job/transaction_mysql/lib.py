import logging 
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_tbl_sales_mysql():
    create_sql_tbl_sales = """
        CREATE TABLE tbl_sales_mysql (
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
        'topic' = 'mysql_source1.salesdb.tbl_sales',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
    """
    return create_sql_tbl_sales

def insert_into_transactions(table_env):
    logger.info(" Inserting data into sink table......")
    table_env.execute_sql(f"""
    INSERT INTO tbl_sales
    select 
    payload.id, 
    payload.product_id,
    payload.customer_id,
    payload.branch_id,                       
    payload.quantity,
    payload.payment_method,
    DATE_FORMAT(
    TO_TIMESTAMP(REPLACE(SUBSTRING(payload.order_date, 1, 19), 'T', ' ')),
    'yyyy-MM-dd HH:mm:ss'
    ) as order_date,
    payload.order_status,
    payload.payment_status,
    payload.shipping_status,
    payload.is_online_transaction,
    payload.delivery_fee,
    payload.is_free_delivery_fee,                    
    DATE_FORMAT(
    TO_TIMESTAMP(REPLACE(SUBSTRING(payload.created_at, 1, 19), 'T', ' ')),
    'yyyy-MM-dd HH:mm:ss'
    ) AS created_at,
    DATE_FORMAT(
    TO_TIMESTAMP(REPLACE(SUBSTRING(payload.modified_at, 1, 19), 'T', ' ')),
    'yyyy-MM-dd HH:mm:ss'
    ) AS modified_at           
    from tbl_sales_mysql 
    """ 
    )
    logger.info("✅ Data inserted into sink table.")