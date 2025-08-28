import pandas as pd
import psycopg2
import os

# Output folder (Downloads/gold_csv)
output_folder = "/home/nineleaps/Downloads/gold_csv"
os.makedirs(output_folder, exist_ok=True)

# DB connection
conn = psycopg2.connect(
    host="localhost",
    database="mydb",
    user="hamsapriya",
    password="password"
)

# Gold tables list
tables = [
    "dashboard_table",
    "patients_per_department",
    "revenue_per_department_comp",
    "appointments_per_doctor",
    "treatments_per_disease",
    "patients_per_city",
    "yearly_patient_count",
    "top_doctors_completed",
    "top_disease_per_year",
    "age_disease_distribution",
    "top_disease_per_age_group"
]

# Export loop
for table in tables:
    try:
        df = pd.read_sql(f"SELECT * FROM gold.{table};", conn)
        if not df.empty:
            filepath = f"{output_folder}/{table}.csv"
            df.to_csv(filepath, index=False)
            print(f"{table}.csv exported to {filepath} successfully!")
        else:
            print(f"{table} exists but is empty.")
    except Exception as e:
        print(f"⚠️ Skipping {table}: {e}")

# Close connection
conn.close()
