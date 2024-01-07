import sqlalchemy
from sqlalchemy import text
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy import Table
from sqlalchemy import update
import random 
import time 
#import multiprocessing


import threading

# Sqlalchemy initialization
# Create engine to connect to MySQL database
engine = sqlalchemy.create_engine('mysql+pymysql://root:J4p4nr3is32015!@127.0.0.1:3306/Benchmark_dbi', pool_size=5, max_overflow=15, pool_timeout=120)

# Create MetaData object that holds the database schema information
metadata = MetaData()
    
# Reflect the tables from the database using the engine
branches_table = Table('branches', metadata, autoload_with=engine)
accounts_table = Table('accounts', metadata, autoload_with=engine)
tellers_table = Table('tellers', metadata, autoload_with=engine)
history_table = Table('history', metadata, autoload_with=engine)


# Create a session factory and bind it to the engine
session_factory = sessionmaker(bind=engine)
ScopedSession = scoped_session(session_factory)

# Kontostands-TX (Account Balance Transaction)
def get_account_balance(accid, session):
    account = session.query(accounts_table).filter_by(accid=accid).one()
    return account.balance

# Einzahlungs-TX (Deposit Transaction)
def deposit(accid, tellerid, branchid, delta, session):
    
    try:
        # Update Branch Balance
        upd_branch = update(branches_table).where(branches_table.c.branchid == branchid).values(balance=branches_table.c.balance + delta)
        session.execute(upd_branch)

        # Update Teller Balance
        upd_teller = update(tellers_table).where(tellers_table.c.tellerid == tellerid).values(balance=tellers_table.c.balance + delta)
        session.execute(upd_teller)

        # Update Account Balance
        upd_account = update(accounts_table).where(accounts_table.c.accid == accid).values(balance=accounts_table.c.balance + delta)
        session.execute(upd_account)


        # Retrieve updated account balance
        updated_account = session.query(accounts_table).filter_by(accid=accid).one()
        updated_balance = updated_account.balance

        # Add to History
        history_entry = {
            "accid": accid,
            "tellerid": tellerid,
            "delta": delta,
            "branchid": branchid,
            "accbalance": updated_balance,  # Use the retrieved updated balance
            "cmmnt": "Deposit made"
        }
        session.execute(history_table.insert(), history_entry)
        session.commit()
        
        return updated_balance  # Return the updated balance
    
    # Error Message
    except Exception as e:
        session.rollback()
        print(f"Error during deposit: {e}")
        return None

# Analyse-TX (Analysis Transaction)
def count_deposits_by_amount(delta, session):
    count = session.query(history_table).filter_by(delta=delta).count()
    return count


def load_driver_task(duration=600, think_time=0.05):
    session = scoped_session(session_factory)
    start_time = time.time()

    # Definieren der Phasen
    end_of_warmup = start_time + 4 * 60  # 4 Minuten Einschwingphase
    end_of_measurement = end_of_warmup + 5 * 60  # 5 Minuten Messphase
    end_time = end_of_measurement + 1 * 60  # 1 Minute Ausschwingphase

    transaction_count = 0
    in_measurement_phase = False
    start_of_measurement = start_time
    print("starting while")
    while time.time() < end_time:
        current_time = time.time()

        # Überprüfen, ob die Messphase begonnen hat
        if not in_measurement_phase and current_time >= end_of_warmup:
            in_measurement_phase = True
            #start_of_measurement = current_time

        # Überprüfen, ob die Messphase vorbei ist
        if in_measurement_phase and current_time >= end_of_measurement:
            in_measurement_phase = False
            end_of_measurement = current_time

        tx_choice = random.choices(['balance', 'deposit', 'analysis'], weights=[35, 50, 15], k=1)[0]
        print(current_time, tx_choice)
        
        # Zählen der Transaktion, wenn in der Messphase
        if in_measurement_phase:
            transaction_count += 1

        # Ausführen der jeweiligen 
        if tx_choice == 'balance':
            accid = random.randint(1, 10000000)  
            get_account_balance(accid, session)  
        elif tx_choice == 'deposit':
            accid = random.randint(1, 10000000)
            tellerid = random.randint(1, 100)
            branchid = random.randint(1, 100)
            delta = random.randint(1, 10000)  
            deposit(accid, tellerid, branchid, delta, session)
        elif tx_choice == 'analysis':
            delta = random.randint(1, 10000)
            count_deposits_by_amount(delta, session)

        time.sleep(think_time)

    # Berechnen der durchschnittlichen Transaktionsrate
    measurement_duration = round((end_of_measurement - start_of_measurement), 2)

    if measurement_duration > 0:
        transactions_per_second = round((transaction_count / measurement_duration), 2)
        
    else:
        transactions_per_second = 0

    print(f"start_of_measurement: {start_of_measurement}")
    print(f"end_of_measurement: {end_of_measurement}")
    print(f"measurement_duration: {measurement_duration} seconds")

    print(f"Anzahl der Transaktionen in der Messphase: {transaction_count}")
    print(f"Durchschnittliche Transaktionen pro Sekunde: {transactions_per_second}")


    session.remove()
    
def load_driver(duration=600, think_time=0.05, num_threads=5):
    threads = []

    for _ in range(num_threads):
        process = threading.Thread(target=load_driver_task, args=(duration, think_time))
        print("Starting thread")
        process.start()
        threads.append(process)

    return threads

def delete_history():
    with engine.connect() as conn:
        conn.execute(history_table.delete())
        conn.commit()

if __name__ == '__main__':
    delete_history()
    print("delete history")
    
    load_driver_process = load_driver()
    for thread in load_driver_process:
        thread.join()
    print("done")