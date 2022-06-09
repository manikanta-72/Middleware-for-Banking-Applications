import time
from transation import Transaction
from transaction_serializer import TransactionSerializer

from typing import Set, Dict

executor = TransactionSerializer()

current_leader = 8000


def run_transaction(read_set: Set[int], write_dict: Dict[int, int]):
    print(read_set)
    print(write_dict)
    t = TransactionSerializer.add_transaction(read_set, write_dict)
    executor.start_transaction_read_phase(t)
    executor.validate_transaction_and_write(t)


def leader_changed(leader_port):
    global current_leader
    current_leader = leader_port
