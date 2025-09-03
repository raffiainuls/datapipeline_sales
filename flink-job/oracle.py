import logging 
import sys 
import traceback 
from pyflink.table import EnvironmentSettings, TableEnvironment
from helper.function import create_env, create_table_if_not_exists
from transaction_oracle.lib import create_tbl_sales_oracle, insert_into_tbl_sales
from transactions.lib import create_tbl_sales


logging.basicConfig(level=logging)
logger = logging.getLogger(__name__)

def main():
    try:
        table_env = create_env()

        table_env.execute_sql("DROP TABLE IF EXISTS tbl_sales_oracle")
        table_env.execute_sql("DROP TABLE IF EXISTS tbl_sales")
        tbl_sales_oracle = create_tbl_sales_oracle()
        create_table_if_not_exists(table_env, "tbl_sales_oracle", tbl_sales_oracle)

        logger.info("🧾 Creating kafka sink: tbl_sales....")
        tbl_sales = create_tbl_sales()
        create_table_if_not_exists(table_env, "tbl_sales", tbl_sales)

        insert_into_tbl_sales(table_env)
        logger.info("✅ All steps completed successfully.")
    except Exception as e:
        logger.error("❌ An error occurred!")
        traceback.print_exc(file=sys.stdout)

if __name__ == "__main__":
    main()



