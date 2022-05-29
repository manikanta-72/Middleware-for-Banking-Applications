
class validator():
    def __init__(self):
        self.read_set = {}
        self.write_set = {}

    def validate_transaction(self, t_j):
        ongoing_transactions = []
        for t_i in ongoing_transactions:
            if (self.read_set[t_j] & self.write_set[t_i]) :
                return False
            elif (self.write_set[t_j] & self.write_set[t_i]):
                return False
        
        return True
