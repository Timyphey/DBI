import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy import Table
from sqlalchemy import update
import random 
import time 
#import multiprocessing

from sqlalchemy.orm import scoped_session, sessionmaker
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
history_table = Table('history', metadata, autoload_with=engine)  # Assuming there is a history table


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
        print("Nach commit")
        return updated_balance  # Return the updated balance
    
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
    end_time = time.time() + duration
    while time.time() < end_time:
        tx_choice = random.choices(['balance', 'deposit', 'analysis'], weights=[35, 50, 15], k=1)[0]
        
        if tx_choice == 'balance':
            accid = random.randint(1, 100)  # Beispiel: Kontonummern zwischen 1 und 100
            get_account_balance(accid, session)
            
        elif tx_choice == 'deposit':
            accid = random.randint(1, 100)
            tellerid = random.randint(1, 10)
            branchid = random.randint(1, 10)
            delta = random.randint(1, 10000)  # Beispiel: Einzahlungsbetrag zwischen 1 und 10000
            deposit(accid, tellerid, branchid, delta, session)
            
        elif tx_choice == 'analysis':
            delta = random.randint(1, 10000)
            count_deposits_by_amount(delta, session)

        time.sleep(think_time)

    ScopedSession.remove()
    
def load_driver(duration=600, think_time=0.05):
    process = threading.Thread(target=load_driver_task, args=(duration, think_time))
    process.start()
    return process

if __name__ == '__main__':
    load_driver_process = load_driver()
