import sys
import psycopg2


# ---------------------------
# Database connection helper
# ---------------------------
def get_connection():
    return psycopg2.connect(
        host="localhost",  # your host
        database="mydb",  # your DB name
        user="hamsapriya",  # your username
        password="password"  # your password
    )


def run_sql(sql):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


# ---------------------------
# ETL Layers
# ---------------------------
def build_bronze():
    # Your existing code to load CSVs into bronze tables
    print("Bronze layer built (placeholder).")


def build_silver():
    # Your existing code to transform bronze → silver
    print("Silver layer built (placeholder).")


def build_gold():
    # Create gold schema
    run_sql("CREATE SCHEMA IF NOT EXISTS gold;")

    # Patients per department
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.patients_per_department AS
    SELECT
        d.department_name,
        COUNT(a.patient_id) AS total_patients
    FROM silver.appointments a
    JOIN silver.doctors d ON a.doctor_id = d.doctor_id
    GROUP BY d.department_name
    ORDER BY total_patients DESC;
    """)

    # Revenue per department
    run_sql("""
CREATE TABLE gold.revenue_per_department_comp AS
SELECT
    d.department_name,
    SUM(b.amount) AS total_revenue
FROM silver.billing b
JOIN silver.appointments a ON b.appointment_id = a.appointment_id
JOIN silver.doctors d ON a.doctor_id = d.doctor_id
WHERE a.status = 'completed'
GROUP BY d.department_name
ORDER BY total_revenue DESC;

    """)

    # Appointments per doctor
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.appointments_per_doctor AS
    SELECT
        d.name AS doctor_name,
        d.department_name,
        COUNT(a.appointment_id) AS total_appointments
    FROM silver.appointments a
    JOIN silver.doctors d ON a.doctor_id = d.doctor_id
    GROUP BY d.name, d.department_name
    ORDER BY total_appointments DESC;
    """)

    # Treatments per disease
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.treatments_per_disease AS
    SELECT
        diag.disease,
        d.department_name,
        COUNT(diag.appointment_id) AS total_treatments
    FROM silver.diagnosis diag
    JOIN silver.doctors d ON diag.department_id = d.department_id
    GROUP BY diag.disease, d.department_name
    ORDER BY total_treatments DESC;
    """)

    # Dashboard table
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.dashboard_table AS
    SELECT
        d.department_name,
        COUNT(DISTINCT a.patient_id) AS total_patients,
        SUM(b.amount) AS total_revenue,
        COUNT(a.appointment_id) AS total_appointments,
        COUNT(diag.appointment_id) AS total_treatments
    FROM silver.doctors d
    LEFT JOIN silver.appointments a ON d.doctor_id = a.doctor_id
    LEFT JOIN silver.billing b ON a.appointment_id = b.appointment_id
    LEFT JOIN silver.diagnosis diag ON a.appointment_id = diag.appointment_id
    GROUP BY d.department_name;
    """)

    # Patients per city
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.patients_per_city AS
    SELECT city, COUNT(*) AS total_patients
    FROM silver.patients
    GROUP BY city;
    """)

    print("Gold layer built successfully.")


# ---------------------------
# Main execution
# ---------------------------
if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "all":
        build_bronze()
        build_silver()
        build_gold()
        print("ETL pipeline completed successfully!")
    else:
        print("Usage: python etl.py all")
