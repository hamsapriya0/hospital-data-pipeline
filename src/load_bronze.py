import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Load DB config
load_dotenv("config/db_config.env")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT") or "5432"  # default to 5432 if missing
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

# Check that all required variables are present
missing_vars = [var for var, val in [("DB_HOST", DB_HOST), ("DB_NAME", DB_NAME),
                                     ("DB_USER", DB_USER), ("DB_PASS", DB_PASS)] if not val]
if missing_vars:
    print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
    exit(1)

# Ensure DB_PORT is an integer
try:
    DB_PORT = int(DB_PORT)
except ValueError:
    print(f"❌ Invalid DB_PORT value: {DB_PORT}. Must be a number.")
    exit(1)

# Construct SQLAlchemy connection string
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create SQLAlchemy engine
try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("✅ Connected to PostgreSQL!")
except SQLAlchemyError as e:
    print(f"❌ Could not connect to PostgreSQL: {e}")
    exit(1)

# Ensure 'bronze' schema exists
with engine.connect() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
    print("✅ Ensured 'bronze' schema exists")

# Input folder
INPUT_DIR = "bronze_inputs"

# List of CSV files (table_name : filename)
csv_files = {
    "appointments_raw": "appointments.csv",
    "billing_raw": "billing.csv",
    "doctors_raw": "doctors.csv",
    "patients_raw": "patients.csv",
    "treatments_raw": "treatments.csv",
}

# Load each CSV into bronze schema
for table_name, filename in csv_files.items():
    file_path = os.path.join(INPUT_DIR, filename)
    
    if not os.path.exists(file_path):
        print(f"⚠️ File not found: {file_path}")
        continue
    
    print(f"📂 Loading {filename} into bronze.{table_name} ...")
    
    try:
        df = pd.read_csv(file_path, encoding="utf-8")
        
        df.to_sql(
            table_name,
            engine,
            schema="bronze",
            if_exists="replace",   # change to "append" to keep old data
            index=False,
            method="multi",
            chunksize=5000
        )
        
        with engine.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) FROM bronze."{table_name}"'))
            row_count = result.scalar()
        print(f"✅ Loaded {row_count} rows into bronze.{table_name}")
        
    except (SQLAlchemyError, pd.errors.ParserError, UnicodeDecodeError) as e:
        print(f"❌ Failed to load {filename}: {e}")
