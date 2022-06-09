import threading


class TransactionValidator():
    def __init__(self, ):
        self.resource_locks = set()
        self.global_lock = threading.Lock()

    # need to check different values for different operations 
    def check_resource_availability(self, transaction, WRITE):
        if WRITE:
            for resource, _ in transaction["write_set"].items():
                if resource in self.resource_locks:
                    return False
        else:
            for resource in transaction["read_set"]:
                if resource in self.resource_locks:
                    return False
        return True

    def validate_transactions(self, transaction, get_timestamp):
        for resource in transaction["read_set"]:
            # get timestamp of the resource from the database and check it with timestamp of 
            # transaction . if timestamp of transaction is less than timestamp of resource
            # abort the transaction as some other process has updated the database
            if transaction["time_stamp"] < get_timestamp(resource):
                return False

        return True

    def lock_resources(self, transaction):
        self.global_lock.acquire()
        can_acquire = True
        for resource, _ in transaction["write_set"].items():
            if resource in self.resource_locks:
                can_acquire = False
                break
        if can_acquire:
            for resource, _ in transaction["write_set"].items():
                self.resource_locks.add(resource)

        self.global_lock.release()

        return can_acquire

    def unlock_resources(self, transaction):
        self.global_lock.acquire()
        for resource, _ in transaction["write_set"].items():
            self.resource_locks.remove(resource)
        self.global_lock.release()

        return
