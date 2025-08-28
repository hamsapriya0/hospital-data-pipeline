import psycopg2

# connection details
connection = psycopg2.connect(
    dbname="mydb",          # your database name
    user="hamsapriya",      # your username
    password="password",  # replace with your actual password
    host="localhost",       # since DB is on your machine
    port="5432"             # default PostgreSQL port
)

print("✅ Connected to PostgreSQL successfully!")

connection.close()
