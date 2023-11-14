import sqlalchemy
from sqlalchemy import text

engine = sqlalchemy.create_engine('mysql+pymysql://root:J4p4nr3is32015!@127.0.0.1:3306/dbi')

conn = engine.connect()

pid_input = input("Enter the pid: ")

res = conn.execute(text(f"SELECT agents.aid, agents.aname, sum(dollars) as sumdollar FROM orders, agents WHERE pid='{pid_input}' and orders.aid = agents.aid GROUP BY agents.aid, agents.aname ORDER BY sumdollar DESC;")).fetchall()

conn.close()

print(res)

#for row in res:
#    aid = row[0]
#    aname = row[1]
#    total_dollars = row[2]
#
#    print(f"{aid}\t{aname}\t{total_dollars}")