import time
from transation import Transaction
from transaction_serializer import TransactionSerializer

from typing import Set, Dict

def commit_transaction(transaction: Transaction) -> bool:
    # call the agent with write set
    # TODO

    return True

executor = TransactionSerializer()


def run_transaction(read_set: Set[int], write_dict: Dict[int, int]):
    print(read_set)
    print(write_dict)
    t = TransactionSerializer.add_transaction(read_set, write_dict)
    executor.start_transaction_read_phase(t)
    executor.validate_transaction_and_write(t)

def main():
    executor = TransactionSerializer()
    run_transaction

if __name__ == "___main__":
    
    main()
