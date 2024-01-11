import sqlalchemy
from sqlalchemy import text, MetaData, Table, update
from sqlalchemy.orm import scoped_session, sessionmaker
import time
import threading
import numpy as np

global_transactions = []
global_transactions_lock = threading.Lock()

# Sqlalchemy initialization as a function
def init_sqlalchemy():
    engine = sqlalchemy.create_engine('mysql+pymysql://root:J4p4nr3is32015!@127.0.0.1:3306/Benchmark_dbi', pool_size=5, max_overflow=15, pool_timeout=500)
    metadata = MetaData()
    branches_table = Table('branches', metadata, autoload_with=engine)
    accounts_table = Table('accounts', metadata, autoload_with=engine)
    tellers_table = Table('tellers', metadata, autoload_with=engine)
    history_table = Table('history', metadata, autoload_with=engine)
    return engine, metadata, branches_table, accounts_table, tellers_table, history_table

# Session management as a function
def create_session_factory(engine):
    session_factory = sessionmaker(bind=engine)
    return scoped_session(session_factory)

# Kontostands-TX (Account Balance Transaction)
def get_account_balance(accid, session):
    account = session.query(accounts_table).filter_by(accid=accid).one()
    #return account.balance

# Einzahlungs-TX (Deposit Transaction)
def deposit(accid, tellerid, branchid, delta, session):
        try:
            result = session.execute(text("CALL Deposit(:accid, :tellerid, :branchid, :delta)"), {'accid': accid, 'tellerid': tellerid, 'branchid': branchid, 'delta': delta})
            updated_balance = result.fetchone()[0]

            session.execute(text("CALL InsertHistory(:accid, :tellerid, :branchid, :delta, :updated_balance)"), 
                    {'accid': accid, 'tellerid': tellerid, 'branchid': branchid, 'delta': delta, 'updated_balance': updated_balance})
            session.commit()
            """
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
            updated_balance = updated_account.balance"""

            """# Add to History
            history_entry = {
                "accid": accid,
                "tellerid": tellerid,
                "delta": delta,
                "branchid": branchid,
                "accbalance": updated_balance,  # Use the retrieved updated balance
                "cmmnt": "Deposit made"
            }
            session.execute(history_table.insert(), history_entry)"""
            #return updated_balance  # Return the updated balance
    
        # Error Message
        except Exception as e:
            session.rollback()
            print(f"Error during deposit: {e}")
            return None

# Analyse-TX (Analysis Transaction)
def count_deposits_by_amount(delta, session):
    count = session.query(history_table).filter(history_table.c.delta == delta).count() # Index wird verwendet
    #count = session.query(history_table).filter_by(delta=delta).count()
    return count

def load_driver_task(think_time):
    global total_transactions

    session = create_session_factory(engine)
    start_time = time.time()

    # Definieren der Phasen
    end_of_warmup = start_time + 4 * 60  # 4 Minuten Einschwingphase
    end_of_measurement = end_of_warmup + 5 * 60  # 5 Minuten Messphase
    end_time = end_of_measurement + 1 * 60  # 1 Minute Ausschwingphase

    in_measurement_phase = False

    while time.time() < end_time:
        current_time = time.time()

        # Überprüfen, ob die Messphase begonnen hat
        if not in_measurement_phase and current_time >= end_of_warmup:
            in_measurement_phase = True

        # Überprüfen, ob die Messphase vorbei ist
        if in_measurement_phase and current_time >= end_of_measurement:
            in_measurement_phase = False
            end_of_measurement = current_time

        tx_choice = np.random.choice(['balance', 'deposit', 'analysis'], p=np.array([0.35, 0.50, 0.15]))
        
        # Zählen der Transaktion, wenn in der Messphase
        if in_measurement_phase:
            with lock:
                total_transactions += 1

        # Ausführen der jeweiligen 
        if tx_choice == 'balance':
            accid = np.random.randint(1, 10000000)  
            get_account_balance(accid, session)  
        elif tx_choice == 'deposit':
            accid = np.random.randint(1, 10000000)
            tellerid = np.random.randint(1, 100)
            branchid = np.random.randint(1, 100)
            delta = np.random.randint(1, 10000)  
            deposit(accid, tellerid, branchid, delta, session)
        elif tx_choice == 'analysis':
            delta = np.random.randint(1, 10000)
            count_deposits_by_amount(delta, session)

        time.sleep(think_time)

    session.remove()
    
def load_driver(think_time=0.05, num_threads=5):
    threads = []
    for _ in range(num_threads):
        t = threading.Thread(target=load_driver_task, args=(think_time,))
        print("Starting threading")
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    return total_transactions

def delete_history():
    with engine.connect() as conn:
        conn.execute(history_table.delete())
        conn.commit()

if __name__ == '__main__':
    engine, metadata, branches_table, accounts_table, tellers_table, history_table = init_sqlalchemy()
    ScopedSession = create_session_factory(engine)
    delete_history()
    print("deleted history")
    total_transactions = 0
    lock = threading.Lock()

    results = load_driver()
    
    # Output the total transactions
    print(f"Total transactions: {results}")
    total_tps = round((total_transactions / 300), 2)
    print(f"Total tps: {total_tps}")
    print("done")