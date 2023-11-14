import configparser
import sqlalchemy
from sqlalchemy import text
import random

engine = sqlalchemy.create_engine('mysql+pymysql://root:password!@127.0.0.1:3306/Benchmark_dbi')


def create_tupel(n):
    conn = engine.connect()
    #if conn.is_connected():
    for x in range(1,n+1):

        #stmt =insert(branches)

        create_branchname_tupel= f"""
        
        insert into Benchmark_dbi.branches
            values({x},"Sparkasse Rhein-Main", 0, "Adresse mit 72 Chars")

        """

        conn.execute(text(create_branchname_tupel))

        conn.commit()

    for x in range(1,n*100000+1):
        random_branchid= random.randint(1,n)
        create_accounts_tupel= f"""

        insert into Benchmark_dbi.accounts
            values({x},"Name mit 20 Char", 0, {random_branchid},"Adresse mit 68 Chars")

        """
        conn.execute(text(create_accounts_tupel))            

        conn.commit()

    for x in range(1,n*10+1):
        random_branchid= random.randint(1,n)
        create_tellers_tupel= f"""

        insert into Benchmark_dbi.tellers
            values({x},"Name mit 20 Char", 0, {random_branchid},"Adresse mit 68 Chars")

        """
        conn.execute(create_tellers_tupel)

        conn.commit()

        conn.close()
    






















def read_config():
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config

def connection():
      
    config = read_config()
    host = config.get('Database', 'host')
    port = config.getint('Database', 'port')
    database = config.get('Database', 'database')
    user = config.get('Database', 'user')
    password = config.get('Database', 'password')

engine = sqlalchemy.create_engine('mysql+pymysql://root:J4p4nr3is32015!@127.0.0.1:3306/dbi')

def umsatzagents(pid_input):
    return conn.execute(text(f"SELECT agents.aid, agents.aname, sum(dollars) as sumdollar FROM orders, agents WHERE pid='{pid_input}' and orders.aid = agents.aid GROUP BY agents.aid, agents.aname ORDER BY sumdollar DESC;")).fetchall()


conn = engine.connect()

#pid_input = input("Enter the pid: ")

#res = conn.execute(text(f"SELECT agents.aid, agents.aname, sum(dollars) as sumdollar FROM orders, agents WHERE pid='{pid_input}' and orders.aid = agents.aid GROUP BY agents.aid, agents.aname ORDER BY sumdollar DESC;")).fetchall()

res = umsatzagents("p01")

conn.close()