class TransactionValidator():
    def __init__(self, ):
        self.resource_locks = {}
        # self.database = database

    def check_resource_availability(self, transaction):
        for resource, _ in transaction["write_set"]:
            if self.resource_locks[resource]:
                return False
        return True

    def validate_transactions(self, transaction, database):
        for resource in transaction["read_set"]:
            # get timestamp of the resource from the database and check it with timestamp of 
            # transaction . if timestamp of transaction is less than timestamp of resource
            # abort the transaction as some other process has updated the database
            pass

    def lock_resources(self, transaction):
        for resource, _ in transaction["write_set"]:
            self.resource_locks[resource] = 1
        return

    def unlock_resources(self, transaction):
        for resource, _ in transaction["write_set"]:
            self.resource_locks[resource] = 0
        return
