import yaml 
import pandas as pd 
from sqlalchemy import create_engine
import os 

with open("config.yaml") as f: 
    config = yaml.safe_load(f)


# databse configuration 
db = config["database"]
user = db["user"]
password = db["password"]
host = db["host"]
port = db["port"]
dbname = db["name"]

engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}")

# improt each file csv into table 
for item in config["csv_files"]:
    csv_path = item["file"]
    table_name = item["table"]

    if not os.path.exists(csv_path):
        print(f"❌ File not found:{csv_path}")
        continue 
    
    print(f"📥 Importing '{csv_path}' to table '{table_name}'...... ")
          
    try:
        df = pd.read_csv(csv_path)
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        print(f"✅ Done importing to '{table_name}'\n")
    except Exception as e:
        print(f"⚠️ Failed to import '{csv_path}': {e}\n")