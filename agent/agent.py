import time

import psycopg2

from transaction_validator import TransactionValidator
import threading


class Agent:
    def __init__(self):
        self.pending_queue = {}
        self.validator = TransactionValidator()
        self.lock = threading.Lock()
        self.conn = None

        try:
            # initialise the database instance
            conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                password="dbpassword",
                port="5432")

            create_bank_balance_table = """
            CREATE TABLE IF NOT EXISTS bank_balance (
                account_number INTEGER NOT NULL,
                balance INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT now(),
                updated_at TIMESTAMP DEFAULT now(),
                PRIMARY KEY (account_number)
            );
            """

            with self.conn:
                with conn.cursor() as cur:
                    cur.execute(create_bank_balance_table)

        except Exception as e:
            print(str(e))

    # We are going to use time in nanos for transaction id
    def get_transaction_id(self):
        # synchronized block to get the next transaction id.
        self.lock.acquire()
        time.sleep(0.0001)
        tid = time.time_ns()
        self.lock.release()
        return tid

    def get_account_balances(self, account_numbers):
        # return the balances

        get_balances = """
        SELECT account_number, balance FROM bank_balance
        WHERE account_number IN (%s);
        """

        try:
            balances = []
            curr_time = time.time_ns()
            with self.conn:
                with self.conn.cursor() as cur:
                    cur.execute(get_balances, list(account_numbers))
                    row = cur.fetchone()
                    while row is not None:
                        print(row)
                        balances.append([row, curr_time])
                        row = cur.fetchone()

            return balances

        except Exception as e:
            print(str(e))

    def update_account(self, account_number, balance):
        # update the balance of the account with account number(account_number)
        # append the log
        pass

    def get_timestamp(self, account_number):
        # return the timestamp of the latest write with account number(account_number) 
        pass

    def update_timestamp(self, account_number, timestamp):
        # update the timestamp of the account with account number
        # append the log
        pass

    def commit_transaction(self, read_set, write_set, read_time):
        # validate the commit ?
        if self.validator.check_resource_availability(read_set, write_set):
            # 2 options a. keep it in pending queue b. abort the transaction altogether
            return "ABORT"

        if self.validator.validate_transactions(read_set, write_set, self.database):
            # return abort message as there is a conflict with another client service
            return "ABORT"

        self.validator.lock_resources(write_set)

        # 2pc implementation

        for account_number, balance in write_set:
            timeStamp = 0
            # update the database with new values
            self.update_account(account_number, balance)
            self.update_timestamp(account_number, timeStamp)

            # write replication log with the latest transaction
            self.write_replication_log(transaction["transaction_id"], account_number, balance)

        self.validator.unlock_resources(transaction)

        return True

    def write_replication_log(self):
        # TODO decide on format to write the replication log
        pass
