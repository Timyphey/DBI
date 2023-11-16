import sqlalchemy
from sqlalchemy import text, MetaData, create_engine, Table
import random 
import time	

# Create engine to connect to MySQL database
engine = sqlalchemy.create_engine('mysql+pymysql://root:J4p4nr3is32015!@127.0.0.1:3306/Benchmark_dbi')

# Create MetaData object that holds the database schema information
metadata = MetaData()
    
# Reflect the tables from the database using the engine
branches_table = Table('branches', metadata, autoload_with=engine)
accounts_table = Table('accounts', metadata, autoload_with=engine)
tellers_table = Table('tellers', metadata, autoload_with=engine)
    
def create_tupel(n):
    with engine.connect() as conn:
        # Save starting time for benchmarking
        start_time = time.perf_counter() 
        # Insert data into branches_table
        branch_values = [{'branchid': x, 
                          'branchname': 'Sparkasse Rhein-Main', 
                          'balance': 0, 
                          'address': 'Branches der Sparkasse Rhein-Main in der Innenstadt Adresse mit 72 Chars'
                          } for x in range(1, n+1)]
        
        conn.execute(branches_table.insert(), branch_values)


        # Insert data into accounts_table
        account_values = [{'accid': x, 
                           'name': 'Acc Name mit 20 Char', 
                           'balance': 0, 
                           'branchid': random.randint(1, n), 
                           'address': 'Account von vielen coolen hunderttausend Kunden Adresse mit 68 Chars'
                           } for x in range(1, n*100000+1)]

        conn.execute(accounts_table.insert(), account_values)


        # Insert data into tellers_table
        teller_values = [{'tellerid': x, 
                          'tellername': 'Tel Name mit 20 Char', 
                          'balance': 0, 
                          'branchid': random.randint(1, n), 
                          'address': 'bester Teller der nicen Sparkasse der Innenstadt Adresse mit 68 Char'
                          } for x in range(1, n*10+1)]
        
        conn.execute(tellers_table.insert(), teller_values)


        # Commit the executed inserts
        conn.commit()
        
        # Save end time, calculate and print the time taken
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        print(f"Time taken to create with n={n}:    {round(elapsed_time, 5)} seconds")
    
    
def delete_all_tuples():
    with engine.connect() as conn:
        # Delete all records 
        
        conn.execute(accounts_table.delete())
        conn.execute(tellers_table.delete())
        conn.execute(branches_table.delete())

        conn.commit()


# Delete all tuples
delete_all_tuples()


# Get n as an input
n_input = int(input("Enter n: "))

if n_input>0:
    create_tupel(n_input)
