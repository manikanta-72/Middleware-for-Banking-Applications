# Client test
import time
import unittest

from client import TransactionSerializer


class TestClient(unittest.TestCase):
    def test_occ_fail(self):
        executor = TransactionSerializer()
        t1_read_set = {1, 2, 3}
        t1_write_dict = {4: 10}
        t1 = TransactionSerializer.add_transaction(t1_read_set, t1_write_dict)

        t2_read_set = {4, 5, 6}
        t2_write_dict = {7: 10}
        t2 = TransactionSerializer.add_transaction(t2_read_set, t2_write_dict)

        executor.start_transaction_read_phase(t1)
        time.sleep(0.05)
        executor.start_transaction_read_phase(t2)

        self.assertTrue(executor.validate_transaction_and_write(t1))
        self.assertFalse(executor.validate_transaction_and_write(t2))

    def test_occ_success(self):
        executor = TransactionSerializer()
        t1_read_set = {1, 2, 3}
        t1_write_dict = {4: 10}
        t1 = TransactionSerializer.add_transaction(t1_read_set, t1_write_dict)

        t2_read_set = {5, 6, 7}
        t2_write_dict = {7: 10}
        t2 = TransactionSerializer.add_transaction(t2_read_set, t2_write_dict)

        executor.start_transaction_read_phase(t1)
        time.sleep(0.05)
        executor.start_transaction_read_phase(t2)

        self.assertTrue(executor.validate_transaction_and_write(t1))
        self.assertTrue(executor.validate_transaction_and_write(t2))
