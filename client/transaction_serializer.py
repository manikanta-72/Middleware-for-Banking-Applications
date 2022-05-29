import time
from transation import Transaction

from typing import Dict, Set

# OCC implementation in the client

# Transaction with its local copy of reads and writes.

# Transactions should be serialized in the order they are validated
class TransactionSerializer:
    def __init__(self):
        self.transactions: Dict[int, Transaction] = {}
        self.validated_transactions: Dict[int, Transaction] = {}
        self.finished_transactions: Set[int] = set()

    @staticmethod
    def add_transaction(read_set, write_dict) -> Transaction:
        t = Transaction()
        t.set_reads(read_set)
        t.set_writes(write_dict)
        return t

    def start_transaction_read_phase(self, tx: Transaction) -> None:
        # Note the current timestamp
        time_ns = time.time_ns()
        tx.set_timestamp(time_ns)
        # TODO call agent for reads
        self.transactions[time_ns] = tx

    # this part is serialized
    def validate_transaction_and_write(self, tx: Transaction) -> bool:
        finished_before_tx: Set[int] = set()
        for t in self.finished_transactions:
            if self.transactions.get(t).finished_timestamp < tx.timestamp:
                finished_before_tx.add(self.transactions.get(t).timestamp)

        filtered_txs: Set[Transaction] = set()
        for k, v in self.validated_transactions.items():
            if k not in finished_before_tx:
                filtered_txs.add(v)

        for t in filtered_txs:
            # if current tx read set overlaps with t's write set return false
            if not t.write_buffer.keys().isdisjoint(tx.read_set):
                return False

        for ftx in self.finished_transactions:
            filtered_txs.discard(self.validated_transactions.get(ftx))

        for t in filtered_txs:
            # if current tx write set overlaps with t's write set return false
            if not t.write_buffer.keys().isdisjoint(tx.write_buffer.keys()):
                return False

        # Add the transaction to the validated dict
        self.validated_transactions[tx.timestamp] = tx

        # Commit tx to Database
        result = commit_transaction(tx)

        # Add to finished if the commit is successful
        if result:
            self.finished_transactions.add(tx.timestamp)
            tx.finished_timestamp = time.time_ns()
            return True
        else:
            self.transactions.pop(tx.timestamp)
            self.validated_transactions.pop(tx.timestamp)
            return False
