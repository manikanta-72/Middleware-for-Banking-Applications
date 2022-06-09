import time
import requests
import psycopg2

from transaction_validator import TransactionValidator
import threading


class Agent:
    def __init__(self, url, port):
        self.pending_queue = {}
        self.validator = TransactionValidator()
        self.lock = threading.Lock()
        self.conn = None
        self.URL = url
        self.PORT = port
        self.log_file_path = "recovery_log.txt"
        print("XYAZZZ")
        try:
            # initialise the database instance
            self.conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                password="DB1",
                port="5432")

            self.conn.autocommit = True

            create_bank_balance_table = """
            CREATE TABLE IF NOT EXISTS bank_balance (
                account_number INTEGER NOT NULL,
                balance INTEGER NOT NULL,
                created_at BIGINT,
                updated_at BIGINT,
                PRIMARY KEY (account_number)
            );
            """

            cursor = self.conn.cursor()
            cursor.execute(create_bank_balance_table)
            cursor.close()
            self.conn.commit()

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
            cursor = self.conn.cursor()
            cursor.execute(get_balances, list(account_numbers))
            row = cursor.fetchone()
            while row is not None:
                print(row)
                balances.append([row, curr_time])
                row = cursor.fetchone()

            cursor.close()

            return balances

        except Exception as e:
            print(str(e))

    def update_account(self, account_number, balance):
        # update the balance of the account and write current timestamp for an account number
        sql_query = """UPDATE bank_balance SET balance = %s, updated_at = %s WHERE account_number = %s;"""
        try:
            # execute the UPDATE query
            self.conn.cursor().execute(sql_query, balance, time.time_ns(), account_number)
            self.conn.commit()
        except Exception as e:
            print(str(e))

    def get_timestamp(self, account_number):
        # return the timestamp of the latest write of an account number
        sql_query = """SELECT updated_at FROM bank_balance WHERE account_number = %s;"""
        try:
            # execute the UPDATE query
            self.conn.cursor().execute(sql_query, account_number)
            ts = self.conn.cursor().fetchone()
            return ts
        except Exception as e:
            print(str(e))

    def read_transaction(self, transaction):
        '''
        Input: 
            transaction: Transaction - sent by client
        Output: 
            read_status: True/False - whether read can be done successfully or not
            return_data: list of balances wrt to input account numbers
        '''
        if not self.validator.check_resource_availability(transaction, 0):
            return False, []

        print("Available resources")

        # return the current timestamp
        return True, self.get_account_balances(transaction['read_set'])

    def prepare_for_commit(self, transaction):
        if not self.validator.check_resource_availability(transaction):
            log_message = "{}$$ABORT".format(transaction['transaction_id'])
            self.write_log(log_message)
            return "NO"

        self.validator.lock_resources(transaction)
        log_message = "{}$$PREPARED".format(transaction['transaction_id'])
        self.write_log(log_message)
        return "YES"

    def log_commit_transaction(self, transaction):
        log_message = "{}$$COMMIT".format(transaction['transaction_id'])
        self.write_log(log_message)

        for account_number, balance in transaction['write_set']:
            # update the database with new values
            self.update_account(account_number, balance)

        self.validator.unlock_resources(transaction)

        log_message = "{}$$COMPLETED".format(transaction['transaction_id'])
        self.write_log(log_message)

        return "YES"

    # def commit_transaction(self, read_set, write_set, read_time):
    def commit_transaction(self, transaction):
        '''
        Input:
            transaction: Transaction sent by client
        Output:
            commit_status: "COMMIT"/"ABORT" 
        '''
        transaction['transaction_id'] = self.get_transaction_id()

        # validate the commit ?
        # if self.validator.check_resource_availability(read_set, write_set):
        if self.validator.check_resource_availability(transaction, 1):
            # 2 options a. keep it in pending queue b. abort the transaction altogether
            # log_message = "{}$$ABORT".format(transaction['transaction_id'])
            # self.write_log(log_message)
            return "ABORT"

        # if self.validator.validate_transactions(read_set, write_set, self.database):
        if self.validator.validate_transactions(transaction, self.get_timestamp):
            # return abort message as there is a conflict with another client service
            # log_message = "{}$$ABORT".format(transaction['transaction_id'])
            # self.write_log(log_message)
            return "ABORT"

        # 2pc implementation

        # self.validator.lock_resources(write_set)
        if not self.validator.lock_resources(transaction):
            return "ABORT"

        log_message = "{}$$START$${}".format(str(transaction['transaction_id']), str(transaction))
        self.write_log(log_message)

        log_message = "{}$$PREPARED".format(transaction['transaction_id'])
        self.write_log(log_message)

        # prepared received

        for i in range(1, 3):
            if not self.send_prepare_message(self.PORT + i, transaction):
                log_message = "{}$$ABORT".format(transaction['transaction_id'])
                self.write_log(log_message)
                self.validator.unlock_resources(transaction)
                return "ABORT"

        log_message = "{}$$COMMIT".format(transaction['transaction_id'])
        self.write_log(log_message)

        # commit message to followers
        for i in range(1, 3):
            # This will not happen in our model
            if not self.send_commit_message(self.PORT + i, transaction):
                log_message = "{}$$ABORT".format(transaction['transaction_id'])
                self.write_log(log_message)
                return "ABORT"
        # commit acknowledge

        for account_number, balance in transaction['write_set']:
            timeStamp = 0
            # update the database with new values
            self.update_account(account_number, balance)

        self.validator.unlock_resources(transaction)

        log_message = "{}$$COMPLETED".format(transaction['transaction_id'])
        self.write_log(log_message)

        return "COMMIT"

    def write_log(self, message):
        # TODO decide on format to write the replication log
        with open(self.log_file_path, 'w') as f:
            f.write(message + '\n')

    def send_prepare_message(self, port, transaction):
        # requests timeout
        url = self.URL + ':' + port + '/prepare_message/'
        r = requests.post(url, data={'transaction': transaction}, timeout=5)
        if r.status_code != 200:
            return False
        r_json = r.json()
        if r_json['prepare_status'] != "YES":
            return False
        return True

    def send_commit_message(self, port, transaction):
        # requests timeout
        url = self.URL + ':' + port + '/commit_message/'
        r = requests.post(url, data={'transaction': transaction}, timeout=5)
        if r.status_code != 200:
            return False
        r_json = r.json()
        if r_json['commit_status'] != "YES":
            return False
        return True
