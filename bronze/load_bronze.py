import pandas as pd
from sqlalchemy import create_engine
import os

# PostgreSQL connection details
db_config = {
    "user": "hamsapriya",    # your db username
    "password": "password",   # update with your password
    "host": "localhost",
    "port": 5432,
    "database": "mydb"        # bronze schema lives inside mydb
}

# Create SQLAlchemy engine
engine = create_engine(
    f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@"
    f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

# Folder where your CSVs actually live
csv_folder = "/home/nineleaps/PyCharmMiscProject/bronze/"

# Function to load CSV safely
def load_csv(file_name):
    path = os.path.join(csv_folder, file_name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV file not found: {path}")
    return pd.read_csv(path)

# Load CSVs
df_patients = load_csv("patients.csv")
df_doctors = load_csv("doctors.csv")
df_appointments = load_csv("appointments.csv")
df_billing = load_csv("billing.csv")
df_treatments = load_csv("diagnosis.csv")  # rename to treatments for clarity

# Save to Postgres (inside bronze schema)
df_patients.to_sql("patients", engine, schema="bronze", if_exists="replace", index=False)
df_doctors.to_sql("doctors", engine, schema="bronze", if_exists="replace", index=False)
df_appointments.to_sql("appointments", engine, schema="bronze", if_exists="replace", index=False)
df_billing.to_sql("billing", engine, schema="bronze", if_exists="replace", index=False)
df_treatments.to_sql("diagnosis", engine, schema="bronze", if_exists="replace", index=False)

print(" All CSV files successfully loaded into bronze schema.")
