import pandas as pd
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="mydb",
    user="hamsapriya",
    password="password"
)

# Example: load dashboard_table
df_dashboard = pd.read_sql("SELECT * FROM gold.dashboard_table;", conn)
print(df_dashboard.head())
