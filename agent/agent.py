import time
import traceback

import requests
import psycopg2

from transaction_validator import TransactionValidator
import threading

NUMBER_OF_PARTICIPANTS = 3
agent_ports = [8000, 8001, 8002]


class Agent:

    def __init__(self, url, port):
        self.transaction_queue = {'Started': list(), 'Prepared': list(), 'Commited': list(), 'Completed': list()}
        self.validator = TransactionValidator()
        self.lock = threading.Lock()
        self.conn = None
        self.URL = url
        self.PORT = port
        self.log_file_path = "recovery_log.txt"
        self.leader = True
        self.up = True
        self.stop_receiving = False
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
            traceback.print_exc()

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
        WHERE account_number IN %s;
        """

        if not account_numbers:
            return None

        try:
            balances = []
            curr_time = time.time_ns()
            cursor = self.conn.cursor()
            print("Account numbers", account_numbers)
            cursor.execute(get_balances, (tuple(account_numbers),))
            row = cursor.fetchone()
            while row is not None:
                print(row)
                balances.append([row, curr_time])
                row = cursor.fetchone()

            cursor.close()

            print("Fetched balances", balances)
            return balances

        except Exception as e:
            traceback.print_exc()

    def update_account(self, account_number, balance):
        # update the balance of the account and write current timestamp for an account number
        # Check if exists
        print("AAAA", account_number, balance)
        try:
            if not self.get_account_balances([account_number]):
                insert_sql_query = """INSERT INTO bank_balance VALUES(%s, %s, %s, %s);"""
                cursor = self.conn.cursor()
                curr_time = time.time_ns()
                cursor.execute(insert_sql_query, [account_number, balance, curr_time, curr_time])
                cursor.close()
                self.conn.commit()

            sql_query = """UPDATE bank_balance SET balance = %s, updated_at = %s WHERE account_number = %s;"""

            # execute the UPDATE query
            cursor = self.conn.cursor()
            cursor.execute(sql_query, [balance, time.time_ns(), account_number])
            cursor.close()
            self.conn.commit()
        except Exception as e:
            traceback.print_exc()

    def get_timestamp(self, account_number):
        # return the timestamp of the latest write of an account number
        sql_query = """SELECT updated_at FROM bank_balance WHERE account_number = %s;"""
        try:
            # execute the UPDATE query
            cursor = self.conn.cursor()
            cursor.execute(sql_query, [account_number])
            ts = cursor.fetchone()
            cursor.close()
            if not ts:
                return 0
            return ts[0]
        except:
            traceback.print_exc()

    def read_transaction(self, transaction):
        '''
        Input: 
            transaction: Transaction - sent by client
        Output: 
            read_status: True/False - whether read can be done successfully or not
            return_data: list of balances wrt to input account numbers
        '''
        if not self.validator.check_resource_availability(transaction, 0):
            return False, [], 0

        print("Available resources")

        # return the current timestamp
        return True, self.get_account_balances(transaction['read_set']), time.time_ns()

    def prepare_for_commit(self, transaction):
        log_message = "{}$$START$${}".format(str(transaction['transaction_id']), str(transaction))
        self.write_log(log_message)

        self.transaction_queue['Started'].append(transaction)

        if not self.validator.check_resource_availability(transaction, 1):
            log_message = "{}$$ABORT".format(transaction['transaction_id'])
            self.write_log(log_message)
            self.transaction_queue['Started'].remove(transaction)
            self.transaction_queue['Completed'].append(transaction)
            return "NO"

        self.validator.lock_resources(transaction)
        log_message = "{}$$PREPARED".format(transaction['transaction_id'])
        self.write_log(log_message)
        self.transaction_queue['Started'].remove(transaction)
        self.transaction_queue['Prepared'].append(transaction)
        return "YES"

    def log_commit_transaction(self, transaction):
        log_message = "{}$$COMMIT".format(transaction['transaction_id'])
        self.write_log(log_message)

        self.transaction_queue['Prepared'].remove(transaction)
        self.transaction_queue['Commited'].append(transaction)

        for account_number, balance in transaction['write_set'].items():
            # update the database with new values
            self.update_account(account_number, balance)

        self.validator.unlock_resources(transaction)

        log_message = "{}$$COMPLETED".format(transaction['transaction_id'])
        self.write_log(log_message)

        self.transaction_queue['Commited'].remove(transaction)
        self.transaction_queue['Completed'].append(transaction)

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

        print("TMPXXX: ", transaction)

        # validate the commit
        if not self.validator.check_resource_availability(transaction, 1):
            # 2 options a. keep it in pending queue b. abort the transaction altogether
            print('availability abort')
            return "ABORT"

        if not self.validator.validate_transactions(transaction, self.get_timestamp):
            # return abort message as there is a conflict with another client service
            print('validation abort')
            return "ABORT"

        # 2pc implementation

        log_message = "{}$$START$${}".format(str(transaction['transaction_id']), str(transaction))
        self.write_log(log_message)

        self.transaction_queue['Started'].append(transaction)

        # self.validator.lock_resources(write_set)
        if not self.validator.lock_resources(transaction):
            self.transaction_queue['Started'].remove(transaction)
            self.transaction_queue['Completed'].append(transaction)
            return "ABORT"

        log_message = "{}$$PREPARED".format(transaction['transaction_id'])
        self.write_log(log_message)
        self.transaction_queue['Started'].remove(transaction)
        self.transaction_queue['Prepared'].append(transaction)
        # prepared received

        for port in agent_ports:
            if self.PORT != port:
                if not self.send_prepare_message(port, transaction):
                    log_message = "{}$$ABORT".format(transaction['transaction_id'])
                    self.write_log(log_message)
                    self.validator.unlock_resources(transaction)
                    self.transaction_queue['Prepared'].remove(transaction)
                    self.transaction_queue['Completed'].append(transaction)
                    return "ABORT"

        log_message = "{}$$COMMIT".format(transaction['transaction_id'])
        self.write_log(log_message)

        self.transaction_queue['Prepared'].remove(transaction)
        self.transaction_queue['Commited'].append(transaction)

        # commit message to followers
        for port in agent_ports:
            if self.PORT != port:
                if not self.send_commit_message(port, transaction):
                    log_message = "{}$$ABORT".format(transaction['transaction_id'])
                    self.write_log(log_message)
                    self.validator.unlock_resources(transaction)
                    self.transaction_queue['Commited'].remove(transaction)
                    self.transaction_queue['Completed'].append(transaction)
                    return "ABORT"
        # commit acknowledge

        for account_number, balance in transaction['write_set'].items():
            timeStamp = 0
            # update the database with new values
            self.update_account(account_number, balance)

        self.validator.unlock_resources(transaction)

        log_message = "{}$$COMPLETED".format(transaction['transaction_id'])
        self.write_log(log_message)
        self.transaction_queue['Commited'].remove(transaction)
        self.transaction_queue['Completed'].append(transaction)
        return "COMMIT"

    def write_log(self, message):
        with open(self.log_file_path, 'a+') as f:
            f.write(message + '\n')

    def send_prepare_message(self, port, transaction):
        # requests timeout
        url = self.URL + ':' + str(port) + '/prepare_message/'
        r = requests.post(url, json={'transaction': transaction})
        if r.status_code != 200:
            return False
        r_json = r.json()
        if r_json['prepare_status'] != "YES":
            return False
        return True

    def send_commit_message(self, port, transaction):
        # requests timeout
        url = self.URL + ':' + str(port) + '/commit_message/'
        r = requests.post(url, json={'transaction': transaction})
        if r.status_code != 200:
            return False
        r_json = r.json()
        if r_json['commit_status'] != "YES":
            return False
        return True

    def leader_changed(self):
        # Abort all the ongoing transactions -- ignore them as they are not processed by other participants 
        for transaction in self.transaction_queue['Started']:
            log_message = "{}$$ABORT".format(transaction['transaction_id'])
            self.write_log(log_message)

        for transaction in self.transaction_queue['Prepared']:
            self.validator.unlock_resources(transaction)
            log_message = "{}$$ABORT".format(transaction['transaction_id'])
            self.write_log(log_message)

        return {'status': 'OK'}

    def become_leader(self):
        # set the leader flag to true
        self.leader = True
        for transaction in self.transaction_queue['Started']:
            log_message = "{}$$ABORT".format(transaction['transaction_id'])
            self.write_log(log_message)

        for transaction in self.transaction_queue['Prepared']:
            self.validator.unlock_resources(transaction)
            log_message = "{}$$ABORT".format(transaction['transaction_id'])
            self.write_log(log_message)

        return {'status': 'OK'}

    def down_leader(self):
        self.leader = False
        self.up = False
        # delete the local cache
        self.validator = TransactionValidator()
        return 200

    def up_node(self):
        url = self.URL + ':' + str(9000) + '/restart/' + str(self.PORT) + "/"
        r = requests.get(url)
        print("Received:", r)
        self.up = True
        return 200

    def stop_receiving_set(self):
        self.stop_receiving = True

    def stop_receiving_reset(self):
        self.stop_receiving = False
