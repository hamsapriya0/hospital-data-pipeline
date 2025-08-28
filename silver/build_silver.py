import pandas as pd
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert as pg_insert

# PostgreSQL connection
engine = create_engine("postgresql+psycopg2://hamsapriya:password@localhost:5432/mydb")
SILVER = "silver"

# Read CSVs from bronze folder
df_patients = pd.read_csv("../bronze/patients.csv")
df_doctors = pd.read_csv("../bronze/doctors.csv")
df_appointments = pd.read_csv("../bronze/appointments.csv")
df_billing = pd.read_csv("../bronze/billing.csv")
df_diagnosis = pd.read_csv("../bronze/diagnosis.csv")

# ---------- Data Cleaning ----------

# Patients
df_patients_clean = df_patients.drop_duplicates(subset=["patient_id"])
df_patients_clean = df_patients_clean.fillna({"first_name": "Unknown", "age": 0, "gender": "Unknown", "phone": "", "city": ""})

# Doctors
df_doctors_clean = df_doctors.drop_duplicates(subset=["doctor_id"])
df_doctors_clean = df_doctors_clean.fillna({"name": "Unknown", "specialty": "General", "department_id": 0})

# Appointments
df_appointments_clean = df_appointments.drop_duplicates(subset=["appointment_id"])
df_appointments_clean = df_appointments_clean.fillna({"status": "scheduled"})

# Billing
df_billing_clean = df_billing.drop_duplicates(subset=["appointment_id"])
df_billing_clean = df_billing_clean.fillna({"amount": 0})

# Diagnosis
df_diagnosis_clean = df_diagnosis.drop_duplicates(subset=["appointment_id"])
df_diagnosis_clean = df_diagnosis_clean.fillna({"disease": "Unknown", "department_id": 0})

# ---------- Append to Silver Schema ----------

metadata = MetaData(schema=SILVER)

def append_safely(df, table_name, conflict_cols=None):
    # Reflect existing table
    table = Table(table_name, metadata, autoload_with=engine)
    with engine.begin() as conn:
        for _, row in df.iterrows():
            stmt = pg_insert(table).values(**row.to_dict())
            if conflict_cols:
                stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)
            conn.execute(stmt)

# Append cleaned data
append_safely(df_patients_clean, "patients", conflict_cols=["patient_id"])
print("✅ Patients appended")

append_safely(df_doctors_clean, "doctors", conflict_cols=["doctor_id"])
print("✅ Doctors appended")

append_safely(df_appointments_clean, "appointments", conflict_cols=["appointment_id"])
print("✅ Appointments appended")

append_safely(df_billing_clean, "billing", conflict_cols=["appointment_id"])
print("✅ Billing appended")

append_safely(df_diagnosis_clean, "diagnosis", conflict_cols=["appointment_id"])
print("✅ Diagnosis appended")
