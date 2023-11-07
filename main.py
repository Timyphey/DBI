import mysql.connector

conn = mysql.connector.connect(
    host="127.0.0.1",
    port=3306,
    user="root",
    password="J4p4nr3is32015!",
    database="dbi"
)
cursor = conn.cursor()

cursor.execute('SELECT * FROM agents JOIN products ON agents.city = products.city')

# Datensätze abrufen
datensätze = cursor.fetchall()

# Die abgerufenen Datensätze ausgeben
for datensatz in datensätze:
    print(datensatz)

# Verbindung schließen
conn.close()