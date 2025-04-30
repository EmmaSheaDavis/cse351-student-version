"""
Course    : CSE 351
Assignment: 02
Student   : <your name here>

Instructions:
    - review instructions in the course
"""

# Don't import any other packages for this assignment
import os
import random
import threading
from money import *
from cse351 import Log

# ---------------------------------------------------------------------------
def main(): 

    print('\nATM Processing Program:')
    print('=======================\n')

    create_data_files_if_needed()

    # Load ATM data files
    data_files = get_filenames('data_files')
    # print(data_files)
    
    log = Log(show_terminal=True)
    log.start_timer()

    bank = Bank()

    # TODO - Add a ATM_Reader for each data file

    threads = []

    for file_path in data_files:
         reader = ATM_Reader(bank, file_path)
         threads.append(reader)
         reader.start()

    for thread in threads:
         thread.join()

    test_balances(bank)

    log.stop_timer('Total time')


# ===========================================================================
class ATM_Reader(threading.Thread):
    def __init__(self, bank, file_path):
        super().__init__()
        self.bank = bank
        self.file_path = file_path

    def run(self):
        with open(self.file_path, "r") as file:
            for line in file:
                if line.strip().startswith('#') or not line.strip():
                        continue
                
                parts = line.strip().split(',')
                if len(parts) != 3:
                        continue

                account_number, transaction_type, amount = parts
                money = Money(amount)

                if transaction_type.lower() == 'd':
                        self.bank.deposit(account_number, money)
                elif transaction_type.lower() == 'w':
                        self.bank.withdraw(account_number, money)
    ...


# ===========================================================================
class Account():
    def __init__(self, account_number, lock):
         self.account_number = account_number
         self.balance = Money("0.00")
         self.lock = lock
    
    def deposit(self, amount):
         with self.lock:
            self.balance.add(amount)
    
    def withdraw(self, amount):
         with self.lock:
            self.balance.sub(amount)
    
    def get_balance(self):
         with self.lock:
            return self.balance
    ...


# ===========================================================================
class Bank():
    def __init__(self):
         self.accounts = {}
         self.lock = threading.Lock()

    def get_account(self, account_number):
         with self.lock:
            if account_number not in self.accounts:
                self.accounts[account_number] = Account(account_number, threading.Lock())
            return self.accounts[account_number]
         
    def deposit(self, account_number, amount):
         account = self.get_account(account_number)
         account.deposit(amount)

    def withdraw(self, account_number, amount):
         account = self.get_account(account_number)
         account.withdraw(amount)

    def get_balance(self, account_number):
         account = self.get_account(account_number)
         return account.get_balance()
                
                
    ...


# ---------------------------------------------------------------------------

def get_filenames(folder):
    """ Don't Change """
    filenames = []
    for filename in os.listdir(folder):
        if filename.endswith(".dat"):
            filenames.append(os.path.join(folder, filename))
    return filenames

# ---------------------------------------------------------------------------
def create_data_files_if_needed():
    """ Don't Change """
    ATMS = 10
    ACCOUNTS = 20
    TRANSACTIONS = 250000

    sub_dir = 'data_files'
    if os.path.exists(sub_dir):
        return

    print('Creating Data Files: (Only runs once)')
    os.makedirs(sub_dir)

    random.seed(102030)
    mean = 100.00
    std_dev = 50.00

    for atm in range(1, ATMS + 1):
        filename = f'{sub_dir}/atm-{atm:02d}.dat'
        print(f'- {filename}')
        with open(filename, 'w') as f:
            f.write(f'# Atm transactions from machine {atm:02d}\n')
            f.write('# format: account number, type, amount\n')

            # create random transactions
            for i in range(TRANSACTIONS):
                account = random.randint(1, ACCOUNTS)
                trans_type = 'd' if random.randint(0, 1) == 0 else 'w'
                amount = f'{(random.gauss(mean, std_dev)):0.2f}'
                f.write(f'{account},{trans_type},{amount}\n')

    print()

# ---------------------------------------------------------------------------
def test_balances(bank):
    """ Don't Change """

    # Verify balances for each account
    correct_results = (
        (1, '59362.93'),
        (2, '11988.60'),
        (3, '35982.34'),
        (4, '-22474.29'),
        (5, '11998.99'),
        (6, '-42110.72'),
        (7, '-3038.78'),
        (8, '18118.83'),
        (9, '35529.50'),
        (10, '2722.01'),
        (11, '11194.88'),
        (12, '-37512.97'),
        (13, '-21252.47'),
        (14, '41287.06'),
        (15, '7766.52'),
        (16, '-26820.11'),
        (17, '15792.78'),
        (18, '-12626.83'),
        (19, '-59303.54'),
        (20, '-47460.38'),
    )

    wrong = False
    for account_number, balance in correct_results:
        bal = bank.get_balance(account_number)
        print(f'{account_number:02d}: balance = {bal}')
        if Money(balance) != bal:
            wrong = True
            print(f'Wrong Balance: account = {account_number}, expected = {balance}, actual = {bal}')

    if not wrong:
        print('\nAll account balances are correct')



if __name__ == "__main__":
    main()

