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

    def read_transaction(self, transaction):
        '''
        Input: 
            transaction: Transaction - sent by client
        Output: 
            read_status: True/False - whether read can be done successfully or not
            return_data: list of balances wrt to input account numbers
        '''
        if not self.validator.check_resource_availability(transaction):
            return False, []
        
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
        for account_number, balance in transaction['write_set']:
            timeStamp = 0
            # update the database with new values
            self.update_account(account_number, balance)
            self.update_timestamp(account_number, timeStamp)

            # write replication log with the latest transaction
            self.write_replication_log(transaction["transaction_id"], account_number, balance)

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
        log_message = "{}$$START$${}".format(str(transaction['transaction_id']), str(transaction))
        self.write_log(log_message)
        # validate the commit ?
        # if self.validator.check_resource_availability(read_set, write_set):
        if self.validator.check_resource_availability(transaction):
            # 2 options a. keep it in pending queue b. abort the transaction altogether
            log_message = "{}$$ABORT".format(transaction['transaction_id'])
            self.write_log(log_message)
            return "ABORT"

        # if self.validator.validate_transactions(read_set, write_set, self.database):
        if self.validator.validate_transactions(transaction, self.get_timestamp):
            # return abort message as there is a conflict with another client service
            log_message = "{}$$ABORT".format(transaction['transaction_id'])
            self.write_log(log_message)
            return "ABORT"

        # self.validator.lock_resources(write_set)
        self.validator.lock_resources(transaction)

        log_message = "{}$$PREPARED".format(transaction['transaction_id'])
        self.write_log(log_message)

        # 2pc implementation
        for i in range(1,3):
            if not self.send_prepare_message(self.PORT+i, transaction):
                log_message = "{}$$ABORT".format(transaction['transaction_id'])
                self.write_log(log_message)
                return "ABORT"

        # prepared received

        log_message = "{}$$COMMIT".format(transaction['transaction_id'])
        self.write_log(log_message)

        # commit message to followers
        for i in range(1,3):
            if not self.send_commit_message(self.PORT+i, transaction):
                return "ABORT"
        # commit acknowledge

        for account_number, balance in transaction['write_set']:
            timeStamp = 0
            # update the database with new values
            self.update_account(account_number, balance)
            self.update_timestamp(account_number, timeStamp)

            # write replication log with the latest transaction
            self.write_replication_log(transaction["transaction_id"], account_number, balance)

        self.validator.unlock_resources(transaction)
        
        log_message = "{}$$COMPLETED".format(transaction['transaction_id'])
        self.write_log(log_message)

        return "COMMIT"

    def write_log(self, message):
        # TODO decide on format to write the replication log
        with open(self.log_file_path, 'w') as f:
            f.write(message + '\n')
            
    
    def send_prepare_message(self,port, transaction):
        # requests timeout
        url = self.URL + ':' + port + '/prepare_message/'
        r = requests.post(url, data={'transaction': transaction}, timeout=5)
        if r.status_code is not 200:
            return False
        r_json = r.json()
        if r_json['prepare_status'] is not "YES":
            return False
        return True

    def send_commit_message(self,port, transaction):
        # requests timeout
        url = self.URL + ':' + port + '/commit_message/'
        r = requests.post(url, data={'transaction': transaction}, timeout=5)
        if r.status_code is not 200:
            return False
        r_json = r.json()
        if r_json['commit_status'] is not "YES":
            return False
        return True