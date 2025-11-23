import time
import random
import datetime
import uuid

# Konfigurasi Simpel untuk Dummy Products dan Countries
COUNTRIES = ['ID', 'ID', 'ID', 'ID', 'ID', 'ID', 'US', 'SG', 'MY'] # Menitikberatkan kepada ID
PRODUCTS = ['P1111', 'P2222', 'P3333', 'P4444', 'P5555']

def generate_raw_order():
    """
    Melakukan generate pada data dummy yang mana beberapa sedikit lebih kompleks
    Ada beberapa data yang Raw dan perlu ditranslate atau ditransform.
    """
    
    # 1. Randomize id dan country
    order_id = str(uuid.uuid4())[:8] # unique ID dengan 8 kombinasi hex, ambil 8 karakter paling awal
    user_id = f"U{random.randint(100, 999)}"
    product_id = random.choice(PRODUCTS)
    country = random.choice(COUNTRIES)
    
    # 2. Randomize quantity
    quantity = random.randint(1, 150) # random quantity antara 1 s/d 150 untuk deteksi 'Quantity > 100'
    
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
        raw_price = random.randint(300000000, 700000000)
   
    
    # 3. Tambah string Rp sebagai penanda cash pembayaran (e.g., "Rp.125000000")
    amount_str = f"Rp.{raw_price}"
    
    # 4. Generate Timestamp
    # Simulasikan dengan waktu sekarang atau waktu eksekusi
    created_date = datetime.datetime.now().isoformat()
    
    return {
        "order_id": order_id,
        "user_id": user_id,
        "product_id": product_id,
        "quantity": quantity,
        "amount": amount_str,     
        "country": country,
        "created_date": created_date
    }

def stream_orders():
    """
    Generator function. Memanfaatkan perintah yield untuk non-stop (lazy) generator order value tanpa mengorbankan memory
    """
    while True:
        yield generate_raw_order()
        # berikan delay sebesar 1 detik
        time.sleep(1)

# Test block
if __name__ == "__main__":
    print("--- Testing Producer (Generating 3 Orders) ---")
    gen = stream_orders()
    for _ in range(3):
        print(next(gen))