import logging 
import sys 
import traceback 
from pyflink.table import EnvironmentSettings, TableEnvironment
from helper.function import create_env, create_table_if_not_exists
from transaction_sqlserver.lib import create_tbl_sales_sqlserver, insert_into_tbl_sales
from transactions.lib import create_tbl_sales


logging.basicConfig(level=logging)
logger = logging.getLogger(__name__)

def main():
    try:
        table_env = create_env()

       
        tbl_sales_sqlserver = create_tbl_sales_sqlserver()
        create_table_if_not_exists(table_env, "tbl_sales_sqlserver", tbl_sales_sqlserver)

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



