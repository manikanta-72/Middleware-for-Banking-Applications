from transaction_validator import TransactionValidator

class Agent():
    def __init__(self):
        self.pending_queue = {}
        self.validator = TransactionValidator()
        # initialise the database instance
        # create/init replication log
        pass

    def get_account_balance(self, account_number):
        # return the balance of the account with account number(account_number) 
        pass

    def update_account(self, account_number, balance):
        # update the balance of the account with account number(account_number)
        # append the log
        pass
    
    def commit_transaction(self, transaction):
        # validate the commit ?
        if self.validator.check_resource_availability(transaction):
            # 2 options a. keep it in pending queue b. abort the transaction altogether
            pass

        if self.validator.validate_transactions(transaction,self.database):
            # return abort message as there is a conflict with another client service
            pass
        
        self.validator.lock_resources(transaction)

        for account_number, balance in transaction["write_set"]:
            # update the database with new values
            self.update_account(account_number,balance)
            
            # write replication log with the latest transaction
            self.write_replication_log(transaction["transaction_id"],account_number,balance)

        self.validator.unlock_resources(transaction)
        
        return True        

    def write_replication_log(self):
        # TODO decide on format to write the replication log
        pass