import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import random

# ==========================================
# 1. INITIATION CONFIGURATION
# ==========================================
# Timezone Configuration
local_tz = pendulum.timezone("Asia/Jakarta")

DB_CONN_ID = "postgres_default"
TABLE_NAME = "ecommersssProducts"

# ==========================================
# 2. COMBINAITON OF DATA COMPONENTS
# ==========================================

# CATEGORY 1: ELECTRONICS
ELEC_BRANDS = ["Samsung", "Apple", "Sony", "Logitech", "Asus", "Dell", "HP", "Lenovo"]
ELEC_ITEMS  = ["Monitor", "Mouse", "Keyboard", "Laptop", "Headset", "Smartphone", "Tablet", "Smartwatch"]
ELEC_MODELS = ["Pro", "Lite", "Ultra", "X-Series", "Gaming", "Office", "Mini", "Max"]

# CATEGORY 2: OFFICE TOOLS
OFFICE_BRANDS = ["Joyko", "Kenko", "Bantex", "3M", "Faber-Castell", "Paperline", "Snowman", "Deli"]
OFFICE_ITEMS  = ["Stapler", "Scissors", "Binder Clip", "Calculator", "Sticky Notes", "Whiteboard Marker", "File Folder", "Cutter"]
OFFICE_ATTRS  = ["Heavy Duty", "Portable", "Set of 12", "A4 Size", "F4 Size", "Neon Colors", "Permanent", "Refillable"]

# CATEGORY 3: NON-FICTION BOOKS
BOOK_TOPICS = ["Leadership", "Data", "Personal Finance", "Modern History", "Psychology", "Digital Marketing", "Python Programming", "Startup Business"]
BOOK_SUFFIX = ["101", "Handbook", "Masterclass", "for Beginners", "Principles", "Strategy", "Analysis", "Guide", "Master Edition"]
BOOK_PUBS   = ["Gramedia", "Elex Media", "Bentang Pustaka", "Wiley", "O'Reilly", "Penguin", "Mizan", "Scholatics"]

# ==========================================
# 3. PYTHON LOGIC
# ==========================================
def generate_product_data(num_rows, **kwargs):
    rows = []
    
    for _ in range(num_rows):
        # 1. Pick a Random Category
        category_choice = random.choice(["Electronics", "Office Tools", "Non-Fiction Books"])
        
        product_name = ""
        price = 0
        
        # 2. Assemble the Name based on Category
        if category_choice == "Electronics":
            # [Brand] + [Model] + [Item] -> "Samsung Pro Monitor"
            brand = random.choice(ELEC_BRANDS)
            item = random.choice(ELEC_ITEMS)
            model = random.choice(ELEC_MODELS)
            
            product_name = f"{brand} {model} {item}"
            price = random.randint(500, 30000) * 1000 # 500 ribu s/d 30 juta
            
        elif category_choice == "Office Tools":
            # [Brand] + [Attribute] + [Item] -> "Joyko Heavy Duty Stapler"
            brand = random.choice(OFFICE_BRANDS)
            item = random.choice(OFFICE_ITEMS)
            attr = random.choice(OFFICE_ATTRS)
            
            product_name = f"{brand} {attr} {item}"
            price = random.randint(5, 150) * 1000 # 5 ribu - 150 ribu
            
        else: # Non-Fiction Books
            # [Topic] + [Suffix] + "by" + [Publisher] -> "Data Science 101 by O'Reilly"
            topic = random.choice(BOOK_TOPICS)
            suffix = random.choice(BOOK_SUFFIX)
            pub = random.choice(BOOK_PUBS)
            
            product_name = f"{topic} {suffix} by {pub}"
            price = random.randint(45, 300) * 1000 # 45 ribu - 300 ribu

        # 3. Generate ID (P + 4 digits)
        p_id = f"P{random.randint(1000, 9999)}"
        
        # 4. Escape single quotes for SQL safety
        safe_name = product_name.replace("'", "''")
        
        # JAKARTA TIME
        now_jakarta = pendulum.now("Asia/Jakarta").to_datetime_string()

        rows.append(f"('{p_id}', '{safe_name}', '{category_choice}', {price}, '{now_jakarta}')")
    
    # 5. Build SQL
    sql = f"""
        INSERT INTO {TABLE_NAME} (product_id, product_name, category, price, updated_at) 
        VALUES {','.join(rows)}
        ON CONFLICT (product_id) 
        DO UPDATE SET
            product_name = EXCLUDED.product_name,
            category = EXCLUDED.category,
            price = EXCLUDED.price,
            updated_at = EXCLUDED.updated_at;
    """
    
    # 6. Run via Hook
    hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    hook.run(sql)

# ==========================================
# 4. DAG DEFINITION
# ==========================================
with DAG(
    dag_id="2_generate_products_naming_combination",
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule_interval='0 * * * *',
    catchup=False,
    tags=["data_eng", "products", "combination"],
) as dag:
    
    # Task: Generate Data
    generate_task = PythonOperator(
        task_id="generate_product_data",
        python_callable=generate_product_data,
        op_kwargs={"num_rows": 10}
    )