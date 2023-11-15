import configparser
import sqlalchemy
from sqlalchemy import text, MetaData, create_engine, Table
import random 
import time	
metadata = MetaData()

engine = sqlalchemy.create_engine('mysql+pymysql://root:J4p4nr3is32015!@127.0.0.1:3306/Benchmark_dbi')
    
branches_table = Table('branches', metadata, autoload_with=engine)
accounts_table = Table('accounts', metadata, autoload_with=engine)
tellers_table = Table('tellers', metadata, autoload_with=engine)
    
def create_tupel(n):
    with engine.connect() as conn:
        start_time = time.time() 
        for x in range(1,n+1):
            
            stmt = branches_table.insert().values(branchid = {x}, branchname = "Sparkasse Rhein-Main", balance = 0, address = "Branches der Sparkasse Rhein-Main in der Innenstadt Adresse mit 72 Chars")
            conn.execute(stmt)

        for x in range(1,n*100000+1):
            random_branchid= random.randint(1,n)
            
            stmt = accounts_table.insert().values(accid = {x}, name = "Acc Name mit 20 Char", balance = 0, branchid = {random_branchid}, address = "Account von vielen coolen hunderttausend Kunden Adresse mit 68 Chars")
            conn.execute(stmt)

        for x in range(1,n*10+1):
            random_branchid= random.randint(1,n)
            stmt = tellers_table.insert().values(tellerid = {x}, tellername = "Tel Name mit 20 Char", balance = 0, branchid = {random_branchid}, address = "bester Teller der nicen Sparkasse der Innenstadt Adresse mit 68 Char")
            conn.execute(stmt)

        conn.commit()
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Time taken to create with n={n}:    {elapsed_time} seconds")
    

n_input = int(input("Enter n: "))
if n_input>0:
    create_tupel(n_input)



















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