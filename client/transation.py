from typing import Dict, Set


# Transaction class: save the details of a single transaction

class Transaction:
    def __init__(self):
        self.timestamp: int = 0
        self.finished_timestamp: int = 0
        self.write_buffer: Dict[int, int] = {}
        self.read_set: Set[int] = set()
        self.read_timestamp = 0

    def set_writes(self, write_dict) -> None:
        self.write_buffer = write_dict

    def set_reads(self, read_set) -> None:
        self.read_set = read_set

    def set_timestamp(self, timestamp) -> None:
        self.timestamp = timestamp

    def set_finished_timestamp(self, finished_timestamp) -> None:
        self.finished_timestamp = finished_timestamp
