import json
import time
from transation import Transaction
import threading
import requests
from typing import Dict, Set

CLIENT_URL = ""


def commit_transaction(tx: Transaction, current_leader) -> bool:
    # call the agent with write set

    url = "http://127.0.0.1" + ':' + str(current_leader) + '/commit/'
    r = requests.post(url, json={
        'transaction': {'read_set': list(tx.read_set), 'write_set': tx.write_buffer, 'time_stamp': tx.read_timestamp}})

    print("Response for commit is:", r.json())

    if r.json()['commit'] == "503":
        return False

    return True


# OCC implementation in the client

# Transaction with its local copy of reads and writes.

# Transactions should be serialized in the order they are validated
class TransactionSerializer:
    def __init__(self):
        self.transactions: Dict[int, Transaction] = {}
        self.validated_transactions: Dict[int, Transaction] = {}
        self.finished_transactions: Set[int] = set()
        self.url = CLIENT_URL
        self.lock = threading.Lock()

    @staticmethod
    def add_transaction(read_set, write_dict) -> Transaction:
        t = Transaction()
        t.set_reads(read_set)
        t.set_writes(write_dict)
        return t

    # Use the time nanos for the transaction id
    def start_transaction_read_phase(self, tx: Transaction, current_leader) -> None:
        # Note the current timestamp
        self.lock.acquire()
        time.sleep(0.0001)
        time_ns = time.time_ns()
        self.lock.release()
        tx.set_timestamp(time_ns)

        url = "http://127.0.0.1" + ':' + str(current_leader) + '/read/'
        print("Agent URL: ", url)
        r = requests.post(url, json={'transaction': {'read_set': list(tx.read_set), 'write_set': tx.write_buffer}})

        print("Response for read is:", r.json())

        if r.json()['read_status'] == "503":
            raise Exception("Leader is down")

        tx.read_timestamp = r.json()['timestamp']

        self.transactions[time_ns] = tx

    # this part is serialized/synchronized
    def validate_transaction_and_write(self, tx: Transaction, current_leader) -> bool:
        self.lock.acquire()
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
                self.lock.release()
                return False

        for ftx in self.finished_transactions:
            filtered_txs.discard(self.validated_transactions.get(ftx))

        for t in filtered_txs:
            # if current tx write set overlaps with t's write set return false
            if not t.write_buffer.keys().isdisjoint(tx.write_buffer.keys()):
                self.lock.release()
                return False

        # Add the transaction to the validated dict
        self.validated_transactions[tx.timestamp] = tx

        # Commit tx to Database
        result = commit_transaction(tx, current_leader)

        # Add to finished if the commit is successful
        if result:
            self.finished_transactions.add(tx.timestamp)
            tx.finished_timestamp = time.time_ns()
            self.lock.release()
            return True
        else:
            self.transactions.pop(tx.timestamp)
            self.validated_transactions.pop(tx.timestamp)
            self.lock.release()
            return False
