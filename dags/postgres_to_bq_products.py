from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud import bigquery
from datetime import datetime, timedelta
import json
import os
import pendulum

# ==========================================
# 1. CONFIGURATION AND INITIATION
# ==========================================
# AIRFLOW CONFIG
local_tz = pendulum.timezone("Asia/Jakarta")
POSTGRES_CONN_ID = "postgres_default"

# GOOGLE CLOUD CONFIG
CREDENTIALS_PATH = '/opt/airflow/dags/gcp-key.json' 

BIGQUERY_PROJECT = 'jcdeah-006'
BIGQUERY_DATASET = 'adit_base_finpro'
TABLE_NAME = 'ecommersssProducts'

# SCHEMA DEFINITION
# Menyesuaian dengan Postgres columns, datatype mengikuti standar BQ
BQ_SCHEMA = [
    bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("product_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("price", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
]

# ==========================================
# 2. EXTRACT TO POSTGRES AND PUT INTO TMP JSON
# ==========================================
def extract_from_postgres(**kwargs):
    """
    Extract data dari tabel orders dan disimpan ke .json
    """
    print(f"Extracting {TABLE_NAME} from Postgres...")
    
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # LOGIC: Ambil data hanya yang kemarin saja
    # Dipastikan di table postgres memang ada kolom 'updated_at'
    sql_query = f"""
        SELECT product_id, product_name, category, price, updated_at
        FROM {TABLE_NAME}
        WHERE DATE(updated_at) = CURRENT_DATE - INTERVAL '1 day'
    """
    
    cursor.execute(sql_query)
    
    # Hasil dari extraction
    columns = [desc[0] for desc in cursor.description] # ambil header column
    rows = cursor.fetchall() # ambil semua data
    
    # Print hasil
    print(f"Columns: {columns}")
    print(f"Rows found: {len(rows)}\n")
    
    # Konversi data dari Postgres ke struktur JSON
    results = []
    for row in rows:
        row_dict = {}
        for idx, col in enumerate(columns):
            val = row[idx]
            
            # Penyesuaian Datetime objects untuk JSON
            if hasattr(val, 'isoformat'):
                val = val.isoformat()

            # Pengubahan angka Desimal menjadi Int
            # if col == 'price' and val is not None:
            #     val = int(val)
            
            row_dict[col] = val
        
        results.append(row_dict)
        
    print(f"Extracted {len(results)} rows. Save to JSON format")
    
    # Simpan ke temp file ---> Airflow Scheduler
    filename = f'/tmp/{TABLE_NAME}_extract.json'
    
    with open(filename, 'w') as f:
        json.dump(results, f, indent=2) 
        
    print(f"File saved: {filename}")
        
    cursor.close()
    conn.close()
    
    return filename

def load_to_bigquery(**kwargs):
    """
    Baca JSON orders dan export ke BIGQUERY
    """
    print(f"Loading {TABLE_NAME} to BigQuery...")
    
    # Read the JSON file
    filename = f'/tmp/{TABLE_NAME}_extract.json'
    if not os.path.exists(filename):
        print("No file found. Skipping load.")
        return

    with open(filename, 'r') as f:
        data = json.load(f)
        
    if not data:
        print("Data is empty. Skipping load.")
        return

    print(f"File loaded: {filename}")
    print(f"Rows in file: {len(data)}")

    # Connect ke BQ Client
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIALS_PATH
    client = bigquery.Client(project=BIGQUERY_PROJECT)
    
    table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{TABLE_NAME}"
    
    # Configure Job
    job_config = bigquery.LoadJobConfig(
        schema=BQ_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND, # menambah ke row baru
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        
        # Partitioning: partisi table_id berdasarkan 'updated_at'
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="updated_at", 
        )
    )
    
    # Load Data
    try:
        job = client.load_table_from_json(data, table_id, job_config=job_config)
        job.result() # Wait for completion
        print(f"Loaded {len(data)} rows to {table_id}")
    except Exception as e:
        print(f"Error loading to BQ: {e}")
        raise e

# ==========================================
# 3. DAG DEFINITION
# ==========================================
with DAG(
    dag_id=f'5_ingest_{TABLE_NAME}',
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule_interval='30 3 * * *', # Run at 4 AM Jakarta time
    catchup=False,
    tags=['bigquery', 'ingestion', TABLE_NAME],
) as dag:

    task_extract = PythonOperator(
        task_id=f'extract_{TABLE_NAME}',
        python_callable=extract_from_postgres
    )

    task_load = PythonOperator(
        task_id=f'load_{TABLE_NAME}',
        python_callable=load_to_bigquery
    )

    task_extract >> task_load