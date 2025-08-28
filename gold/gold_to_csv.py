import pandas as pd
import psycopg2
import os

# Path to Downloads
output_folder = "/home/nineleaps/Downloads/gold_csv"
os.makedirs(output_folder, exist_ok=True)

# DB connection
conn = psycopg2.connect(
    host="localhost",
    database="mydb",
    user="hamsapriya",
    password="password"
)

tables = [
    "dashboard_table",
    "patients_per_department",
    "revenue_per_department_comp",
    "appointments_per_doctor",
    "treatments_per_disease",
    "patients_per_city",
    "yearly_patient_count",
    "top5_doctors_completed",
    "top_disease_per_year",
    "age_disease_distribution"
]

for table in tables:
    df = pd.read_sql(f"SELECT * FROM gold.{table};", conn)
    output_file = os.path.join(output_folder, f"{table}.csv")
    df.to_csv(output_file, index=False)
    print(f"{table}.csv exported to {output_file} successfully!")

conn.close()
