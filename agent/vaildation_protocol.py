
class validator():
    def __init__(self):
        pass

    def validate_transaction(self, t_j, ongoing_transactions):
        for t_i in ongoing_transactions:
            if (t_j.read_set & t_i.write_set) :
                return False
            elif (t_j.write_set & t_i.write_set):
                return False
        
        return True
