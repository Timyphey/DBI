##################### Imports
import sqlalchemy
from sqlalchemy import text
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy import Table
import random 
import time 

from sqlalchemy.orm import scoped_session, sessionmaker
import threading

# Sqlalchemy initialization
# Create engine to connect to MySQL database
engine = sqlalchemy.create_engine('mysql+pymysql://root:J4p4nr3is32015!@192.168.122.37:3306/Benchmark_dbi', pool_size=5, max_overflow=15, pool_timeout=120)

# Create MetaData object that holds the database schema information
metadata = MetaData()
    
# Reflect the tables from the database using the engine
branches_table = Table('branches', metadata, autoload_with=engine)
accounts_table = Table('accounts', metadata, autoload_with=engine)
tellers_table = Table('tellers', metadata, autoload_with=engine)
history_table = Table('history', metadata, autoload_with=engine)

# Create a session factory and bind it to the engine
session_factory = sessionmaker(bind=engine)

##################### Fuctions
# Define a function that creates and inserts values for all tables
def create_and_insert(n, batch_size):
    # Create a list of tuples for each table
    tuples = [create_batches(table, n, batch_size) for table in [branches_table, accounts_table, tellers_table]]
    # For each tuple, insert the values into the table using multithreading
    for table, batches in tuples:
        insert_values(table, batches)
        
# Define a function that creates a tuple of values for a table
def create_batches(table, n, batch_size):
    # Get the list of values for the table
    values = create_values(table, n)
    # Split the list into batches of batch_size values
    batches = [values[i:i+batch_size] for i in range(0, len(values), batch_size)]
    # Return a tuple of the table and the batches
    return (table, batches)

# Define a function that creates a list of values for a table
def create_values(table, n):
    if table == branches_table:
        return [{'branchid': x, 
                 'branchname': 'Sparkasse Rhein-Main', 
                 'balance': 0, 
                 'address': 'Branches der Sparkasse Rhein-Main in der Innenstadt Adresse mit 72 Chars'
                 } for x in range(1, n+1)]
    elif table == accounts_table:
        return [{'accid': x, 
                 'name': 'Acc Name mit 20 Char', 
                 'balance': 0, 
                 'branchid': random.randint(1, n), 
                 'address': 'Account von vielen coolen hunderttausend Kunden Adresse mit 68 Chars'
                 } for x in range(1, n*100000+1)]
    elif table == tellers_table:
        return [{'tellerid': x, 
                 'tellername': 'Tel Name mit 20 Char', 
                 'balance': 0, 
                 'branchid': random.randint(1, n), 
                 'address': 'bester Teller der nicen Sparkasse der Innenstadt Adresse mit 68 Char'
                 } for x in range(1, n*10+1)]

# Define a function that inserts values into a table using multithreading
def insert_values(table, batches):
    # Create a list of threads
    threads = []
    # For each batch, create a thread that inserts the batch into the table
    for batch in batches:
        thread = threading.Thread(target=insert_batch, args=(table, batch))
        # Start the thread
        thread.start()
        # Append the thread to the list
        threads.append(thread)
    # Wait for all threads to finish
    for thread in threads:
        thread.join()

# Define a function that inserts a batch of values into a table
def insert_batch(table, values):
    # Create a scoped session object
    session = scoped_session(session_factory)
    s = session()
    # Execute the query
    s.execute(table.insert(), values)
    # Commit the changes
    s.commit()
    # Remove the session
    session.remove()


##################### Executed code
# Get n as an input
n_input = int(input("Enter n: "))
batch_size_input = 10000
# Check that n is greater than 0
if n_input>0:
    # Save starting time for benchmarking
    start_time = time.perf_counter() 
    
    create_and_insert(n_input, batch_size_input)

    # Save end time, calculate and print the time taken
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print(f"Time taken to create with n={n_input}:    {round(elapsed_time, 5)} seconds")