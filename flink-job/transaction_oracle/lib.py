import logging 
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_tbl_sales_oracle():
    create_sql_tbl_sales = """
        CREATE TABLE tbl_sales_oracle (
        payload ROW<
            ID string,
            PRODUCT_ID INT,
            CUSTOMER_ID INT,
            BRANCH_ID INT,
            QUANTITY INT,
            PAYMENT_METHOD INT,
            ORDER_DATE BIGINT,
            ORDER_STATUS INT,
            PAYMENT_STATUS FLOAT,
            SHIPPING_STATUS FLOAT,
            IS_ONLINE_TRANSACTION BOOLEAN,
            DELIVERY_FEE INT,
            IS_FREE_DELIVERY_FEE STRING,
            CREATED_AT BIGINT,
            MODIFIED_AT BIGINT
        >
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'oracle-source5.C__CDC.TBL_SALES',
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
    payload.ID as id , 
    payload.PRODUCT_ID as product_id,
    payload.CUSTOMER_ID as customer_id, 
    payload.BRANCH_ID as branch_id,
    payload.QUANTITY as quantity,
    payload.PAYMENT_METHOD as payment_method,
    DATE_FORMAT(
    TO_TIMESTAMP_LTZ(CAST(payload.ORDER_DATE AS BIGINT) / 1000, 3),
    'yyyy-MM-dd HH:mm:ss'
    ) as order_date,
    payload.ORDER_STATUS as order_status,
    payload.PAYMENT_STATUS as payment_status, 
    payload.SHIPPING_STATUS as shipping_status, 
    payload.IS_ONLINE_TRANSACTION as is_online_transaction,
    payload.DELIVERY_FEE as delivery_fee,
    payload.IS_FREE_DELIVERY_FEE as is_free_delivery_fee,
    DATE_FORMAT(
    TO_TIMESTAMP_LTZ(CAST(payload.CREATED_AT AS BIGINT) / 1000, 3),
    'yyyy-MM-dd HH:mm:ss'
    ) as created_at,
    DATE_FORMAT(
    TO_TIMESTAMP_LTZ(CAST(payload.MODIFIED_AT AS BIGINT) / 1000, 3),
    'yyyy-MM-dd HH:mm:ss'
    ) as modified_at
    from tbl_sales_oracle                              
    """)
    logger.info("✅ Data inserted into sink table 'tbl_sales'")