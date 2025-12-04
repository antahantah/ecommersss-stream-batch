import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import random

# ==========================================
# 1. INITIATION CONFIGURATION
# ==========================================
# Timezone Configuration
local_tz = pendulum.timezone("Asia/Jakarta")

DB_CONN_ID = "postgres_default"
TABLE_NAME = "ecommersssUsers"

# ==========================================
# 2. DATA CONSTANTS
# ==========================================
FIRST_NAMES = [
    "Aditya", "Agus", "Ahmad", "Aisha", "Andi", "Angga", "Annisa", "Arief", "Ayu", "Bagus",
    "Bambang", "Bayu", "Budi", "Chandra", "Citra", "Dewi", "Dian", "Dimas", "Dina", "Dinda",
    "Eka", "Fajar", "Farhan", "Fitri", "Gita", "Hadi", "Hendra", "Indah", "Indra", "Intan",
    "Irfan", "Joko", "Kartika", "Kiki", "Lestari", "Lia", "Linda", "Maya", "Mega", "Muhamad",
    "Nadia", "Nina", "Nur", "Putra", "Putri", "Rahmat", "Rina", "Rizky", "Sari", "Siti"
]

LAST_NAMES = [
    "Abdullah", "Adriansyah", "Akbar", "Anwar", "Arifin", "Asyhari", "Basri", "Budiman", "Cahyono", "Darmawan",
    "Fauzi", "Firdaus", "Gunawan", "Hakim", "Hidayat", "Hidayatullah", "Irawan", "Iskandar", "Kusuma", "Lubis",
    "Mahendra", "Maulana", "Mustafa", "Nasution", "Nugraha", "Nugroho", "Pamungkas", "Pangestu", "Permana", "Pradana",
    "Prakoso", "Pratama", "Pratiwi", "Purnomo", "Putra", "Rahman", "Ramadhan", "Riyadi", "Rosyid", "Safitri",
    "Santoso", "Saputra", "Setiawan", "Siregar", "Suharto", "Sulistyo", "Utama", "Wibowo", "Wijaya", "Yuliana"
]

DOMAINS = ["gmail.com", "yahoo.com", "outlook.com"]
PAYMENTS = ["Gopay", "OVO", "DANA", "Paylater", "Credit Card", "Bank Transfer"]

# ==========================================
# 3. RANDOM NAME AND EMAIL LOGIC
# ==========================================
def get_random_people():
    return random.choice(FIRST_NAMES), random.choice(LAST_NAMES), random.choice(DOMAINS), random.choice(PAYMENTS)

def run_sql_via_hook(sql_query):
    hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    hook.run(sql_query)

# EMAIL STYLE 1: [firstname].[lastname]@domain
def generate_and_insert_style_1(num_rows, **kwargs):
    rows = []
    for _ in range(num_rows):
        first, last, domain, payment = get_random_people()
        
        # 1. Generate Custom ID: U + 3 random digits (e.g., U105)
        u_id = f"U{random.randint(100, 999)}"
        
        # full_name = f"{first} {last}"
        email = f"{first.lower()}.{last.lower()}@{domain}"
        
        # JAKARTA TIME
        now_jakarta = pendulum.now("Asia/Jakarta").to_datetime_string()

        rows.append(f"('{u_id}', '{first}', '{last}', '{email}', '{payment}', '{now_jakarta}')")
    
    # Added ON CONFLICT DO NOTHING to prevent crashing if ID already exists
    sql = f"""
        INSERT INTO {TABLE_NAME} (user_id, first_name, last_name, email, default_payment, created_date) 
        VALUES {','.join(rows)} 
        ON CONFLICT (user_id) DO NOTHING;
    """
    run_sql_via_hook(sql)

# EMAIL STYLE 2: [firstname].[random 1-9]@domain
def generate_and_insert_style_2(num_rows, **kwargs):
    rows = []
    for _ in range(num_rows):
        first, last, domain, payment = get_random_people()
        
        u_id = f"U{random.randint(100, 999)}"
        
        # full_name = f"{first} {last}"
        random_num = random.randint(1, 9)
        email = f"{first.lower()}.{random_num}@{domain}"
        
        # JAKARTA TIME
        now_jakarta = pendulum.now("Asia/Jakarta").to_datetime_string()

        rows.append(f"('{u_id}', '{first}', '{last}', '{email}', '{payment}', '{now_jakarta}')")
    
    sql = f"""
        INSERT INTO {TABLE_NAME} (user_id, first_name, last_name, email, default_payment, created_date)
        VALUES {','.join(rows)}
        ON CONFLICT (user_id) DO NOTHING;
    """
    run_sql_via_hook(sql)

# EMAIL STYLE 3: [lastname]_[firstname first character]@domain
def generate_and_insert_style_3(num_rows, **kwargs):
    rows = []
    for _ in range(num_rows):
        first, last, domain, payment = get_random_people()
        
        u_id = f"U{random.randint(100, 999)}"
        
        # full_name = f"{first} {last}"
        first_char = first[0].lower()
        email = f"{last.lower()}_{first_char}@{domain}"
        
        # JAKARTA TIME
        now_jakarta = pendulum.now("Asia/Jakarta").to_datetime_string()

        rows.append(f"('{u_id}', '{first}', '{last}', '{email}', '{payment}', '{now_jakarta}')")
        
    
    sql = f"""
        INSERT INTO {TABLE_NAME} (user_id, first_name, last_name, email, default_payment, created_date)
        VALUES {','.join(rows)}
        ON CONFLICT (user_id) DO NOTHING;
    """
    run_sql_via_hook(sql)

def decide_email_style():
    return random.choice(['task_style_1', 'task_style_2', 'task_style_3'])

# ==========================================
# 4. DAG
# ==========================================
with DAG(
    dag_id="1_generate_users_custom_id",
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule_interval='0 * * * *',
    catchup=False,
    tags=["data_eng", "simple", "custom_id"],
) as dag:

    branching = BranchPythonOperator(
        task_id='branch_email_style',
        python_callable=decide_email_style
    )

    task_1 = PythonOperator(
        task_id="task_style_1",
        python_callable=generate_and_insert_style_1,
        op_kwargs={"num_rows": 10}
    )

    task_2 = PythonOperator(
        task_id="task_style_2",
        python_callable=generate_and_insert_style_2,
        op_kwargs={"num_rows": 10}
    )

    task_3 = PythonOperator(
        task_id="task_style_3",
        python_callable=generate_and_insert_style_3,
        op_kwargs={"num_rows": 10}
    )

    branching >> [task_1, task_2, task_3]