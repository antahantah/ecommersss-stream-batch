import psycopg2

# Konfigurasi Database awal
# disesuaikan dengan docker-compose.yaml 
DB_CONFIG = {
    "dbname": "fraud_detection_db",
    "user": "myuser",
    "password": "mypassword",
    "host": "localhost", 
    "port": "5433"  # Custom host port, berbeda dengan 5432:5432
}

def get_db_connection():
    """
    Koneksi awal ke PostgreSQL database.
    Return konfigurasi conn dbconfig dengan autocommit = True
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        
        conn.autocommit = True 
        return conn
        
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def init_db():
    """
    Jika belum ada maka Create Table sebagai berikut
    1. Table "ecommersssOrders" untuk fase streaming pipeline
    2. Table "ecommersssProducts" utk Batch pipeline
    3. Table "ecommersssUsers" utk Batch pipeline

    Functions juga sudah termasuk Schema Table
    """
    conn = get_db_connection()
    if conn is None:
        return
    
    cursor = conn.cursor()
    
    # SQL untuk Create table "ecommersssOrders"
    # Amount masih menggunakan VARCHAR untuk handle currency Rp.

    create_order_query = """
    CREATE TABLE IF NOT EXISTS ecommersssOrders (
        id SERIAL PRIMARY KEY,
        order_id VARCHAR(50),
        user_id VARCHAR(50),
        product_id VARCHAR(50),
        quantity INTEGER,
        amount VARCHAR(50),
        payment_method VARCHAR(50),
        country VARCHAR(10),
        created_date TIMESTAMP,
        status VARCHAR(20)
    );
    """

    # SQL untuk Create table "ecommersssProducts"
    # Dihandle oleh Airflow

    create_product_query = """
    CREATE TABLE IF NOT EXISTS ecommersssProducts (
        product_id VARCHAR(50) PRIMARY KEY,
        product_name VARCHAR(100),
        category VARCHAR(50),
        price NUMERIC(18, 2),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    # SQL untuk Create table "ecommersssUsers"
    # Dihandle oleh Airflow

    create_user_query = """
    CREATE TABLE IF NOT EXISTS ecommersssUsers (
        user_id VARCHAR(50) PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        email VARCHAR(100),
        phone_number VARCHAR(20),
        registration_date TIMESTAMP
    );
    """
    
    try:
        # Execute creation for all 3 tables
        cursor.execute(create_order_query)
        print("Table 'ecommersssOrders' checked/created successfully.")
        
        cursor.execute(create_product_query)
        print("Table 'ecommersssProducts' checked/created successfully.")
        
        cursor.execute(create_user_query)
        print("Table 'ecommersssUsers' checked/created successfully.")
    
    except Exception as e:
        print(f" Error creating table: {e}")
    finally:
        cursor.close()
        conn.close()

# This allows you to run 'python src/db.py' directly to verify everything
if __name__ == "__main__":
    print("--- Initializing Database Schema ---")
    init_db()