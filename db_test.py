import psycopg2

conn = psycopg2.connect(database="nrt_test", user="postgres", password="Nizam2007", host="127.0.0.1", port ="5432")

cursor = conn.cursor()

cursor.execute("select version()")

data = cursor.fetchone()
print("Connection established to: ", data)

conn.cloase()
print("CLOSED")
