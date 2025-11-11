import subprocess
import time
from sodapy import Socrata
import pandas as pd 
import json
from sqlalchemy import create_engine

source_config = {
    "user": "postgres",
    "password": "secret",
    "host" : "source_postgres",
    "port" : "5432",
    "database" : "source_db"
}


def connect_to_db(host, max_retries=5, delay=3):
    current_retries = 0
    while current_retries<=max_retries:
        result = subprocess.run(["pg_isready", "-h", host], capture_output=True, text=True)  
        if "accepting connections" in result.stdout:
          print("Success")
          return True
        else:
            current_retries+=1 
            print(f"Connection wasn't able to be made, retrying in {delay}")
            time.sleep(delay)
    return False



def fetch_data(base_url, dataset_id, limit =999):
    client = Socrata(base_url, None, timeout=60)
    results = client.get(dataset_id, limit=limit)
    results_df = pd.DataFrame.from_records(results)
    results_df = results_df.map(
        lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
    )
    print(len(results_df))
    return results_df
        
    
def load_to_source(config):
    data = fetch_data("data.cityofchicago.org", "v6vf-nfxy")
    engine = create_engine(
        f"postgresql+psycopg://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    )

    data.to_sql("raw_data", con=engine, if_exists="append", index=False)
    print("data loaded successfully")
    return True

# def load_to_source(config):
#     df = fetch_data("data.cityofchicago.org", "v6vf-nfxy")  # prints 999 already
#     assert len(df) == 999, f"unexpected df size: {len(df)}"

#     url = f"postgresql+psycopg://{config['user']}:{config['password']}@" \
#           f"{config['host']}:{config['port']}/{config['database']}"
#     print("[connect]", url)

#     engine = create_engine(url, pool_pre_ping=True)

#     with engine.begin() as conn:
#         df.to_sql("raw_data", con=conn, schema="public",
#                   if_exists="replace", index=False, method="multi", chunksize=1000)

#         total = conn.exec_driver_sql("SELECT COUNT(*) FROM public.raw_data").scalar()
#         db, addr, port = conn.exec_driver_sql(
#             "SELECT current_database(), inet_server_addr(), inet_server_port()"
#         ).fetchone()
#         print(f"[post-write] db={db} host={addr} port={port} rows={total}")

def load_to_postgres(source_config):
    postgres_connection = connect_to_db(source_config['host'])
    if postgres_connection:
        load_to_source(source_config)


load_to_postgres(source_config)

