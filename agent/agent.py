import time

from transaction_validator import TransactionValidator
import threading


class Agent:
    def __init__(self):
        self.pending_queue = {}
        self.validator = TransactionValidator()
        self.lock = threading.Lock()

        # initialise the database instance
        # create/init replication log

        pass

    # We are going to use time in nanos for transaction id
    def get_transaction_id(self):
        # synchronized block to get the next transaction id.
        self.lock.acquire()
        time.sleep(0.0001)
        tid = time.time_ns()
        self.lock.release()
        return tid

    def get_account_balances(self, account_numbers):
        # return the balance of the account with account number(account_number) 
        pass

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

    def commit_transaction(self, read_set, write_set):
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
