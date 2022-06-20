import psycopg2

print("CONNECTIONG")

conn = psycopg2.connect("dbname=NRT_test user=postgres password=admin")

cursor = conn.cursor()

postgreSQL_select_Query = """
SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'calls';
"""
cursor.execute(postgreSQL_select_Query)

records = cursor.fetchall()
print(records)

conn.close()
print("CLOSED")
