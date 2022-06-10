from transaction_serializer import TransactionSerializer

from typing import Set, Dict

executor = TransactionSerializer()


def run_transaction(read_set: Set[int], write_dict: Dict[int, int], current_leader):
    print(read_set)
    print(write_dict)
    t = TransactionSerializer.add_transaction(read_set, write_dict)
    executor.start_transaction_read_phase(t, current_leader)
    if len(write_dict) != 0:
        executor.validate_transaction_and_write(t, current_leader)
