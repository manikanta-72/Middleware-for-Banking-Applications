
class Agent():
    def __init__(self):
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
    
    def commit_transaction(self, transaction_id, write_set, read_set):
        # validate the commit ?
        
        for account_number, balance in write_set:
            # update the database with new values
            self.update_account(account_number,balance)
            
            # write replication log with the latest transaction
            self.write_replication_log(transaction_id,account_number,balance)

        return True        

    def write_replication_log(self):
        # TODO decide on format to write the replication log
        pass