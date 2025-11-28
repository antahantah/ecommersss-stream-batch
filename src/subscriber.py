import datetime
import re       # untuk cleaning string seperti pada value AMOUNT
import psycopg2
import json     # Untuk read JSONL
import time
import os
import glob
import sys
from .db import get_db_connection # Import connection function dari inisiasi db.py

def clean_and_process_order(raw_order):
    """
    merapikan raw data (amount, date) untuk checking fraud,
    namun raw data yang diinput ke Postgre.
    """
    
    # --- 1. DATA CLEANING & MINOR TRANSFORMATION ---
    
    # Merapikan value amount ("Rp." dan koma pemisah dihapus and convert ke float(NUMERIC) )
    amount_str = raw_order['amount']
    cleaned_amount = re.sub(r'[^\d]', '', amount_str)
    
    try:
        amount_numeric = float(cleaned_amount)
    except ValueError:
        print(f"Skipping order {raw_order['order_id']}: Could not clean amount '{amount_str}'")
        return None 
    
    # mengubah created_date string menjadi datetime object, ambil value hour
    try:
        created_dt = datetime.datetime.fromisoformat(raw_order['created_date'])
        order_hour = created_dt.hour # 0 s/d 23
    except ValueError:
        print(f"Skipping order {raw_order['order_id']}: Invalid date format.")
        return None
        
    # --- 2. FRAUD RULE STATEMENT (GENUINE OR FRAUD?) ---
    
    status = "genuine"
    
    # A. Transaksi antara jam 12 malam s/d 4 pagi (00:00 to 04:59)
    # kalau sudah fraud, tidak perlu dicek pada rule berikutnya. rule berikutnya berlaku jika masih "genuine"
    is_risky_hour = (order_hour >= 0) and (order_hour < 5)
    if is_risky_hour and ((amount_numeric >= 100000000) or raw_order['quantity'] > 100 ): 
        # transaksi lebih dari 100 juta dan quantity lebih dari 100
        status = "fraud"
        
    # B. Lokasi bukan Indonesia
    # kalau sudah fraud, tidak perlu dicek pada rule berikutnya. rule berikutnya berlaku jika masih "genuine"
    if status == "genuine" and raw_order['country'] != 'ID':
        status = "fraud"
        
    # C. Anomali data yang berlebih (transaksi up to 300 juta atau pembelian lebih dari 300 quantity)
    # kalau sudah fraud, tidak perlu dicek pada rule berikutnya. rule berikutnya berlaku jika masih "genuine"
    if status == "genuine" and (amount_numeric >= 300000000 or raw_order['quantity'] > 300):
        status = "fraud"

    # --- 3. RETURN ENRICHED DATA FOR POSTGRE INSERT ---
    
    return {
        "order_id": raw_order['order_id'],
        "user_id": raw_order['user_id'],
        "product_id": raw_order['product_id'],
        "quantity": raw_order['quantity'],
        "amount": raw_order['amount'],       # tetap dibiarkan menjadi string agar nanti dirapikan oleh dbt
        "payment_method": raw_order['payment_method'],
        "country": raw_order['country'],
        "created_date": created_dt,          # DATETIME object (from str to DATETIME) agar mudah diinsert ke Postgre
        "status": status,                    # The judgment
    }

def insert_processed_order(enriched_order):
    """
    Inserts data final, enriched ke dalam PostgreSQL database.
    """
    conn = get_db_connection()
    if conn is None:
        print("Could not insert data: DB connection failed.")
        return
        
    cursor = conn.cursor()
    
    # %s digunakan untuk insert POSTGRE placeholder
    insert_query = """
    INSERT INTO ecommersssOrders 
    (order_id, user_id, product_id, quantity, amount, payment_method, country, created_date, status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    data = (
        enriched_order['order_id'],
        enriched_order['user_id'],
        enriched_order['product_id'],
        enriched_order['quantity'],
        enriched_order['amount'],          # Inserting the Raw String
        enriched_order['payment_method'],  
        enriched_order['country'],
        enriched_order['created_date'],    # Inserting TIMESTAMP datatype
        enriched_order['status']
    )
    
    try:
        cursor.execute(insert_query, data)
        # Note: Autocommit=True in db.py handles the commit here
        print(f"-> SAVED | Status: {enriched_order['status']} | QTY: {enriched_order['quantity']} | Amount: {enriched_order['amount']} | Country: {enriched_order['country']}")
    except psycopg2.Error as e:
        print(f"DB INSERT FAILED for order {enriched_order['order_id']}: {e}")
    finally:
        cursor.close()
        conn.close()

def run_subscriber():
    """
    Main function to read from the JSONL file in tail-mode.
    """
    file_path = "topicOrders.jsonl"
    
    print("--- SUBSCRIBER STARTED ---")
    print(f"Reading from '{file_path}' (Tail Mode)...")
    
    # 1. Wait for file to exist (The 30-second delay logic happens here implicitly)
    while not os.path.exists(file_path):
        print("Waiting for producer to create file...", end='\r')
        time.sleep(1)
        
    # 2. Open the file in Read Mode
    # Using 'with' is important to handle file closing automatically
    with open(file_path, 'r') as f:
        while True:
            # Try to read the next line
            line = f.readline()
            
            if line:
                # A. DATA FOUND: Process it
                try:
                    raw_order = json.loads(line)
                    enriched = clean_and_process_order(raw_order)
                    if enriched:
                        insert_processed_order(enriched)
                except json.JSONDecodeError:
                    # Catch incomplete lines that might happen during simultaneous write
                    print("⚠️ Incomplete JSON line found, skipping...")
            else:
                # B. NO DATA (End of File): Wait and try again (The Polling/Tail logic)
                time.sleep(0.5)

if __name__ == "__main__":
    try:
        run_subscriber()
    except KeyboardInterrupt:
        print("\nSubscriber stopped (Ctrl+C).")
        sys.exit(0)