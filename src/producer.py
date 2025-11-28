import time
import random
import datetime
import uuid
import json
import os 
import sys 

# Konfigurasi Simpel untuk Dummy Products dan Countries
COUNTRIES = ['ID', 'ID', 'ID', 'ID', 'ID', 'ID', 'US', 'SG', 'MY'] # Menitikberatkan kepada ID
PAYMENT = ['Bank Transfer', 'Paylater', 'Credit Card', 'Gopay', 'OVO', 'DANA']
PRODUCTS = ['P1111', 'P2222', 'P3333', 'P4444', 'P5555']

def generate_raw_order():
    """
    Melakukan generate pada data dummy yang mana beberapa sedikit lebih kompleks
    Ada beberapa data yang Raw dan perlu ditranslate atau ditransform pada step berikutnya.
    """
    
    # 1. Randomize id dan country
    order_id = str(uuid.uuid4())[:8] # unique ID dengan kombinasi hex, ambil 8 karakter paling awal
    user_id = f"U{random.randint(100, 999)}"
    product_id = random.choice(PRODUCTS)
    payment_method = random.choice(PAYMENT)
    country = random.choice(COUNTRIES)
    
    # 2. Randomize quantity
    #quantity = random.randint(1, 150) # random quantity antara 1 s/d 150 untuk deteksi 'Quantity > 100"
    # revisi sedikit untuk deteksi fraud quantity di jam kritis dengan menitikberatkan pada amount LOW
    die = random.randint(1, 6) 

    if die < 4:  # 1, 2, 3 ---> maksimal 100 quantity
        quantity = random.randint(1, 100)
    elif die < 6:  # 4, 5 ---> antara 101 s/d 300 
        quantity = random.randint(101, 300)
    else:  # 6 ---> lebih dari 300
        quantity = random.randint(301, 500)
    
    # Random price antara 100 ribu s/d 600 juta untuk deteksi 'Amount > 300 juta'
    # raw_price = random.randint(100000, 600000000) ---> direvisi karena sering menampilkan angka diatas 100000000

    # Random price antara 100 ribu s/d 600 juta untuk deteksi 'Amount > 300 juta', namun menitikberatkan pada amount LOW
    # menggunakan dice untuk probabilitas 50%, 33.33%, dan 16.67%
    dice = random.randint(1, 6)

    if dice < 4:  # 1, 2, 3 ---> dibawah 100 juta
        raw_price = random.randint(100000, 99999999)
    elif dice < 6:  # 4, 5 ---> antara 100 juta s/d 300 juta
        raw_price = random.randint(100000000, 299999999)
    else:  # 6 ---> 300 juta keatas
        raw_price = random.randint(300000000, 500000000)
   
    
    # 3. Tambah string Rp dan titik koma sebagai penanda cash pembayaran (e.g., "Rp.125,000,000")
    amount_str = f"Rp.{raw_price:,}"
    
    # 4. Generate Timestamp
    # Simulasikan dengan waktu sekarang atau waktu eksekusi
    created_date = datetime.datetime.now().isoformat()
    
    return {
        "order_id": order_id,
        "user_id": user_id,
        "product_id": product_id,
        "quantity": quantity,
        "amount": amount_str,
        "payment_method" : payment_method,     
        "country": country,
        "created_date": created_date
    }

def run_producer():
    """
    Generates orders, taruh per order ke dalam JSONL line sehingga hanya butuh satu file (append-only).
    """
    file_path = "topicOrders.jsonl"
    print(f"--- PRODUCER STARTED ---")
    print(f"Appending to '{file_path}'... (Ctrl+C to stop)")

# Open file hanya sekali, dalam mode append ('a')
    with open(file_path, 'a') as f:
        while True:
            order = generate_raw_order()
            
            # Write JSON object cukup dalam satu baris (JSONL format)
            f.write(json.dumps(order) + "\n")
            
            # CRITICAL: FLUSH data untuk write secepatnya agar dapat dibaca subscriber
            f.flush()
            
            print(f"-> Appended order {order['order_id']} | QTY: {order['quantity']} | Amount: {order['amount']}")
            
            # Jeda 0.5 detik
            time.sleep(0.5)

if __name__ == "__main__":
    try:
        run_producer()
    except KeyboardInterrupt:
        print("\nProducer stopped.")
        sys.exit(0)