import os
import pandas as pd
import numpy as np
from faker import Faker
import os, json, random, string
from datetime import datetime, timedelta
fake = Faker()

np.random.seed(42)

 


base_dir = r"C:/Users/ANayak4/OneDrive - Rockwell Automation, Inc/Desktop/ETL Job Runner/input_files/data"
product_dir, store_dir, txn_dir = [os.path.join(base_dir, d) for d in ["Products","Stores","Transactions"]]
for d in [product_dir, store_dir, txn_dir]: os.makedirs(d, exist_ok=True)

start_date = datetime(2025,10,26)
num_days = 12

def rand_str(n=6): return ''.join(random.choices(string.ascii_letters, k=n))

def inject_invalid(df, col, frac, val):
    n = int(len(df)*frac)
    if n>0:
        idx = np.random.choice(df.index, n, replace=False)
        df.loc[idx,col] = val
    return df

for day in range(num_days):
    date_str = (start_date+timedelta(days=day)).strftime("%Y_%m_%d")

    # Products
    products = pd.DataFrame({
        "product_id": range(1000,1200),
        "product_name": [fake.word().capitalize() for _ in range(200)],
        "unit_price": np.random.randint(50,2000,200),
        "effective_date": (start_date+timedelta(days=day)).strftime("%Y-%m-%d")
    })
    products = inject_invalid(products,"unit_price",0.05,-100)
    products = inject_invalid(products,"product_name",0.05,None)
    products.to_csv(f"{product_dir}/products_{date_str}.csv",index=False)

    # Stores
    stores = pd.DataFrame({
        "store_id": range(2000,2080),
        "store_name": [fake.company() for _ in range(80)],
        "capacity": np.random.randint(100,1000,80)
    })
    stores = inject_invalid(stores,"store_name",0.05,None)
    stores = inject_invalid(stores,"capacity",0.05,-1)
    stores.to_csv(f"{store_dir}/stores_{date_str}.csv",index=False)

    # Transactions
    txns = pd.DataFrame({
        "txn_num": range(300000+day*1000,300000+(day+1)*1000),
        "product_id": np.random.choice(products["product_id"],1000),
        "store_id": np.random.choice(stores["store_id"],1000),
        "qty": np.random.randint(1,10,1000),
        "unit_price": np.random.randint(50,2000,1000),
        "txn_date": (start_date+timedelta(days=day)).strftime("%Y-%m-%d")
    })
    txns["final_amount"] = txns["qty"]*txns["unit_price"]
    txns = inject_invalid(txns,"product_id",0.01,999999)
    txns = inject_invalid(txns,"store_id",0.01,888888)
    txns = inject_invalid(txns,"qty",0.05,-5)
    txns = inject_invalid(txns,"final_amount",0.05,-1)
    txns.to_csv(f"{txn_dir}/transactions_{date_str}.csv",index=False)

print("✅ Generated 12 days of Products, Stores, Transactions with invalids for cleaning tests.")


# -------------------Logistic data --------------------------------

# import os, json, random, string
# from datetime import datetime, timedelta

# # --- Parameters ---
# base_dir = "C:/Users/ANayak4/OneDrive - Rockwell Automation, Inc/Desktop/ETL Job Runner/input_files/data"
# supplier_dir = os.path.join(base_dir, "Supplier")
# warehouse_dir = os.path.join(base_dir, "Warehouse")
# shipment_dir = os.path.join(base_dir, "Shipment")

# # Ensure folders exist
# os.makedirs(supplier_dir, exist_ok=True)
# os.makedirs(warehouse_dir, exist_ok=True)
# os.makedirs(shipment_dir, exist_ok=True)

# num_days = 12
# start_date = datetime(2025, 10, 26)

# # --- Helpers ---
# def rand_str(n=8): 
#     return ''.join(random.choices(string.ascii_letters, k=n))

# def gen_supplier(supplier_id):
#     """SCD1 style: overwrite attributes, no history"""
#     country = random.choice(["Germany","India","China","USA","Brazil", None])
#     return {
#         "supplier_id": supplier_id,
#         "supplier_name": rand_str(6) if random.random() > 0.05 else None,
#         "country": country
#     }

# def gen_warehouse(warehouse_id):
#     """Full load each day"""
#     return {
#         "warehouse_id": warehouse_id,
#         "warehouse_name": rand_str(10) if random.random() > 0.05 else None,
#         "location": random.choice(["Mumbai","LA","Dubai","London",None]),
#         "capacity": random.choice([random.randint(1000,10000), -1])  # inject invalid
#     }

# def gen_shipment(shipment_id, supplier_ids, warehouse_ids, day):
#     """Incremental append"""
#     return {
#         "shipment_id": shipment_id,
#         "supplier_id": random.choice(supplier_ids + [None, 9999]),  # invalid FK
#         "warehouse_id": random.choice(warehouse_ids + [None, 8888]), # invalid FK
#         "shipment_date": (start_date + timedelta(days=day)).strftime("%Y-%m-%d"),
#         "quantity": random.choice([random.randint(1,500), None, -10])  # invalids
#     }

# # --- Generate files per day ---
# for d in range(num_days):
#     date_str = (start_date + timedelta(days=d)).strftime("%Y_%m_%d")

#     # Suppliers (SCD1 incremental)
#     suppliers = [gen_supplier(sid) for sid in range(1000, 1015)]
#     with open(f"{supplier_dir}/suppliers_{date_str}.json","w") as f:
#         json.dump(suppliers,f,indent=2)

#     # Warehouses (full load each day)
#     warehouses = [gen_warehouse(wid) for wid in range(200,210)]
#     with open(f"{warehouse_dir}/warehouses_{date_str}.json","w") as f:
#         json.dump(warehouses,f,indent=2)

#     # Shipments (incremental append)
#     supplier_ids = [s["supplier_id"] for s in suppliers if s["supplier_id"]]
#     warehouse_ids = [w["warehouse_id"] for w in warehouses if w["warehouse_id"]]
#     shipments = [gen_shipment(3000+d*100+i, supplier_ids, warehouse_ids, d) for i in range(100)]
#     with open(f"{shipment_dir}/shipments_{date_str}.json","w") as f:
#         json.dump(shipments,f,indent=2)

# print(f"✅ Generated {num_days*3} JSON files under {base_dir}")




























