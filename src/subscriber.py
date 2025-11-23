import datetime
import re # Used for cleaning the amount string
import psycopg2
from .db import get_db_connection # Import the connection function from db.py

def clean_and_process_order(raw_order):
    """
    Cleans the data, applies fraud rules, and returns an enriched order dictionary.
    """
    
    # --- 1. DATA CLEANING & TRANSFORMATION ---
    
    # Clean Amount (Remove "Rp." and periods, then convert to float/NUMERIC)
    amount_str = raw_order['amount']
    # Use regex to remove non-numeric characters (except the final decimal, if needed)
    cleaned_amount = re.sub(r'[^\d]', '', amount_str)
    try:
        amount_numeric = float(cleaned_amount)
    except ValueError:
        print(f"Skipping order {raw_order['order_id']}: Could not clean amount '{amount_str}'")
        return None # Skip invalid records
    
    # Convert created_date string to datetime object and extract time components
    try:
        created_dt = datetime.datetime.fromisoformat(raw_order['created_date'])
        order_hour = created_dt.hour # 0 through 23
    except ValueError:
        print(f"Skipping order {raw_order['order_id']}: Invalid date format.")
        return None
        
    # --- 2. FRAUD RULE EVALUATION (THE JUDGMENT) ---
    
    status = "genuine"
    
    # A. Night-Time Attack Check (Rule 4 consolidated)
    # Time is between 00:00 (0) and 04:00 (4) AND Amount is high
    is_risky_hour = (order_hour >= 0) and (order_hour < 4)
    if is_risky_hour and (amount_numeric >= 100000000): # >= 100 Million
        status = "fraud"
        
    # B. Geographic Risk Check (Rule 1)
    # Check if status is already fraud, or apply the new rule
    if status == "genuine" and raw_order['country'] != 'ID':
        status = "fraud"
        
    # C. High-Value/Outlier Check (Rules 2 & 5 consolidated)
    if status == "genuine" and (amount_numeric > 300000000 or raw_order['quantity'] > 100):
        status = "fraud"

    # --- 3. RETURN ENRICHED DATA ---
    
    # Create the final dictionary ready for the database
    return {
        "order_id": raw_order['order_id'],
        "user_id": raw_order['user_id'],
        "product_id": raw_order['product_id'],
        "quantity": raw_order['quantity'],
        "amount": amount_numeric,            # Cleaned NUMERIC value
        "country": raw_order['country'],
        "created_date": created_dt,          # Cleaned DATETIME object
        "status": status,                    # The judgment
        "ingestion_time": datetime.datetime.now(datetime.timezone.utc) # For audit
    }

def insert_processed_order(enriched_order):
    """
    Inserts the final, enriched record into the PostgreSQL database.
    """
    conn = get_db_connection()
    if conn is None:
        print("Could not insert data: DB connection failed.")
        return
        
    cursor = conn.cursor()
    
    # PostgreSQL uses %s placeholders
    insert_query = """
    INSERT INTO ecommerceOrders 
    (order_id, user_id, product_id, quantity, amount, country, created_date, status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    data = (
        enriched_order['order_id'],
        enriched_order['user_id'],
        enriched_order['product_id'],
        enriched_order['quantity'],
        enriched_order['amount'],
        enriched_order['country'],
        enriched_order['created_date'],
        enriched_order['status']
    )
    
    try:
        cursor.execute(insert_query, data)
        # Note: Autocommit=True in db.py handles the commit here
        print(f"-> SAVED | Status: {enriched_order['status']} | Amount: {enriched_order['amount']} | Country: {enriched_order['country']}")
    except psycopg2.Error as e:
        print(f"❌ DB INSERT FAILED for order {enriched_order['order_id']}: {e}")
    finally:
        cursor.close()
        conn.close()