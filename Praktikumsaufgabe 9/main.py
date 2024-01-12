import sqlalchemy
from sqlalchemy import text, MetaData, Table, update
from sqlalchemy.orm import scoped_session, sessionmaker
import time
import threading
import numpy as np

# Verbindung mit dem Datenbank-Server und Sessionfactory
engine = sqlalchemy.create_engine('mysql+pymysql://root:J4p4nr3is32015!@127.0.0.1:3306/Benchmark_dbi', pool_size=5, max_overflow=15, pool_timeout=500)
session_factory = sessionmaker(bind=engine)

# Tabelleninformationen aus der Datenbank laden
metadata = MetaData()
branches_table = Table('branches', metadata, autoload_with=engine)
accounts_table = Table('accounts', metadata, autoload_with=engine)
tellers_table = Table('tellers', metadata, autoload_with=engine)
history_table = Table('history', metadata, autoload_with=engine)

# Kontostands-TX (Account Balance Transaction)
def get_account_balance(accid, session):
    account = session.query(accounts_table).filter_by(accid=accid).one()
    return account.balance

# Einzahlungs-TX (Deposit Transaction)
def deposit(accid, tellerid, branchid, delta, session):
        try:
            # Procedure "Deposit" Aufrufen
            result = session.execute(text("CALL Deposit(:accid, :tellerid, :branchid, :delta)"), {'accid': accid, 'tellerid': tellerid, 'branchid': branchid, 'delta': delta})
            # Aktualisierte balance abrufen
            updated_balance = result.fetchone()[0]

            # Änderungen bestätigen
            session.commit()
            
            return updated_balance
        
        # Error Message
        except Exception as e:
            # Änderungen zurückziehen
            session.rollback()
            print(f"Error during deposit: {e}")
            return None

# Analyse-TX (Analysis Transaction)
def count_deposits_by_amount(delta, session):
    count = session.query(history_table).filter(history_table.c.delta == delta).count()
    return count

def load_driver_task(think_time, x):
    global total_transactions
    
    # Session erstellen
    session = scoped_session(session_factory)
    
    # Startzeit speichern
    start_time = time.time()

    # Definieren der Phasen
    end_of_warmup = start_time + 4 * 60  # 4 Minuten Einschwingphase
    end_of_measurement = end_of_warmup + 5 * 60  # 5 Minuten Messphase
    end_time = end_of_measurement + 1 * 60  # 1 Minute Ausschwingphase
    
    in_measurement_phase = False
    
    # Random-Seed erstellen und setzen
    seed =  round(start_time) + x
    np.random.seed(seed)

    while time.time() < end_time:
        # Aktuelle Zeit speichern
        current_time = time.time()

        # Überprüfen, ob die Messphase begonnen hat
        if not in_measurement_phase and current_time >= end_of_warmup:
            in_measurement_phase = True

        # Überprüfen, ob die Messphase vorbei ist
        if in_measurement_phase and current_time >= end_of_measurement:
            in_measurement_phase = False
            end_of_measurement = current_time

        # Zufällige Auswahl der Transaktionen nach Aufgabenstellung
        tx_choice = np.random.choice(['balance', 'deposit', 'analysis'], p=np.array([0.35, 0.50, 0.15]))

        # Ausführen der jeweiligen Transaktion
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

        # Zählen der Transaktion, wenn in der Messphase
        if in_measurement_phase:
            with lock:
                total_transactions += 1
                
        # "Nachdenkzeit"
        time.sleep(think_time)


    # Session schließen
    session.remove()
    
def load_driver(think_time, num_threads):
    # Liste für alle Threads
    threads = []
    
    # Starten von load_driver_task in den Threads
    for x in range(num_threads):
        thread = threading.Thread(target=load_driver_task, args=(think_time, x))
        thread.start()
        threads.append(thread)

    # Warten auf das Beenden aller Threads
    for thread in threads:
        thread.join()

    # Zurückgeben der gesamten Anzahl der Transaktionen
    return total_transactions

# Ausgeführter Code
if __name__ == '__main__':
    # Globale Variable, die die Anzahl der Transaktionen speichert
    total_transactions = 0
    # Lock als Thread-Sicherheitsmechanismus
    lock = threading.Lock()
    
    # Aufruf der load_driver Funktion mit 50ms "Nachdenkzeit" und 5 Threads
    results = load_driver(0.05, 5)
    
    # Berechnen der Transaktionen pro Sekunde
    total_tps = round((total_transactions / 300), 2)
    
    # Ausgeben der Werte
    print(f"Total transactions: {results}")
    print(f"Total tps: {total_tps}")