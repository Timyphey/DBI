import mysql.connector

conn = mysql.connector.connect(
    host="127.0.0.1",
    port=3306,
    user="root",
    password="J4p4nr3is32015!",
    database="dbi"
)
cursor = conn.cursor()

cursor.execute('SELECT agents.aid, agents.aname, sum(dollars) FROM orders, agents WHERE pid="p01" and orders.aid = agents.aid GROUP BY agents.aid, agents.aname')

# Datensätze abrufen
datensätze = cursor.fetchall()

# Die abgerufenen Datensätze ausgeben
for datensatz in datensätze:
    print(datensatz)

# Verbindung schließen
conn.close()