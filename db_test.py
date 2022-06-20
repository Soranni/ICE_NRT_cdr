import psycopg2

print("CONNECTIONG")
conn = psycopg2.connect(
    host="localhost",
    database="nrt_test",
    user="postgres",
    password="Nizam2007")

cursor = conn.cursor()

cursor.execute("select version()")

data = cursor.fetchone()
print("Connection established to: ", data)

conn.cloase()
print("CLOSED")
