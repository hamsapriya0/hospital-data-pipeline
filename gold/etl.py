def build_gold():
    # Create schema
    run_sql("CREATE SCHEMA IF NOT EXISTS gold;")

    # Create gold tables
    run_sql("""
    CREATE TABLE gold.patients_per_department AS
    SELECT
        d.department_name,
        COUNT(a.patient_id) AS total_patients
    FROM silver.appointments a
    JOIN silver.doctors d ON a.doctor_id = d.doctor_id
    GROUP BY d.department_name
    ORDER BY total_patients DESC;
    """)

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

    run_sql("""
    CREATE TABLE gold.appointments_per_doctor AS
    SELECT
        d.name AS doctor_name,
        d.department_name,
        COUNT(a.appointment_id) AS total_appointments
    FROM silver.appointments a
    JOIN silver.doctors d ON a.doctor_id = d.doctor_id
    GROUP BY d.name, d.department_name
    ORDER BY total_appointments DESC;
    """)

    run_sql("""
    CREATE TABLE gold.treatments_per_disease AS
    SELECT
        diag.disease,
        d.department_name,
        COUNT(diag.appointment_id) AS total_treatments
    FROM silver.diagnosis diag
    JOIN silver.doctors d ON diag.department_id = d.department_id
    GROUP BY diag.disease, d.department_name
    ORDER BY total_treatments DESC;
    """)

    run_sql("""
    CREATE TABLE gold.dashboard_table AS
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

    run_sql("""
    CREATE TABLE gold.patients_per_city AS
    SELECT city, COUNT(*) AS total_patients
    FROM silver.patients
    GROUP BY city;
    """)

    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.yearly_patient_count AS
    SELECT 
        EXTRACT(YEAR FROM a.appointment_date) AS year,
        COUNT(DISTINCT a.patient_id) AS total_patients
    FROM silver.appointments a
    GROUP BY year
    ORDER BY year;
    """)

    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.top5_doctors_completed AS
    SELECT 
        d.doctor_id,
        d.name AS doctor_name,
        COUNT(DISTINCT a.patient_id) AS total_patients_attended
    FROM silver.appointments a
    JOIN silver.doctors d ON a.doctor_id = d.doctor_id
    WHERE a.status = 'completed'
    GROUP BY d.doctor_id, d.name
    ORDER BY total_patients_attended DESC
    LIMIT 5;
    """)

    run_sql("""
    CREATE TABLE gold.top_disease_per_year AS
    WITH yearly_counts AS (
        SELECT
            EXTRACT(YEAR FROM a.appointment_date) AS year,
            diag.disease,
            COUNT(*) AS disease_count,
            ROW_NUMBER() OVER (
                PARTITION BY EXTRACT(YEAR FROM a.appointment_date)
                ORDER BY COUNT(*) DESC
            ) AS rn
        FROM silver.diagnosis diag
        JOIN silver.appointments a ON diag.appointment_id = a.appointment_id
        GROUP BY year, diag.disease
    )
    SELECT year, disease, disease_count
    FROM yearly_counts
    WHERE rn = 1;
    """)


    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.age_disease_distribution AS
    SELECT 
        CASE
            WHEN p.age BETWEEN 1 AND 10 THEN '1-10'
            WHEN p.age BETWEEN 11 AND 25 THEN '11-25'
            WHEN p.age BETWEEN 26 AND 55 THEN '26-55'
            ELSE '55+'
        END AS age_group,
        diag.disease,
        COUNT(*) AS patient_count
    FROM silver.patients p
    JOIN silver.appointments a ON p.patient_id = a.patient_id
    JOIN silver.diagnosis diag ON a.appointment_id = diag.appointment_id
    GROUP BY age_group, diag.disease
    ORDER BY age_group, patient_count DESC;
    """)
