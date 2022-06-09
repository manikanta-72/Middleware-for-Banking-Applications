import traceback

import psycopg2
import ast

def write_to_database(transactions_dict, transaction_id):
    try:
        # initialise the database instance
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="dbpassword",
            port="5432")
        cursor = conn.cursor()
        sql_query = """UPDATE bank_balance SET balance = (%s) WHERE account_number = (%s);"""
        # execute the UPDATE query
        cursor.execute(sql_query, (transactions_dict[transaction_id].values(), transactions_dict[transaction_id].keys()))
        conn.commit()
        cursor.close()
    except Exception as e:
            traceback.print_exc()
    
def replay_missed_transactions(new_leader_recovery_log, last_completed_transaction):
    start_replay = False
    transactions_dict = dict()
    with open(new_leader_recovery_log) as file:
        for line in file:
            log_line = line.rstrip().split("$$")
            if start_replay == True:
                if log_line[1] == "START":
                    transactions_dict[log_line[0]] = ast.literal_eval(log_line[2])
                elif log_line[1] == "ABORT":
                    transactions_dict.pop(log_line[0], None)
                elif log_line[1] == "COMPLETED":
                    # write dict values to database
                    write_to_database(transactions_dict, log_line[0])
                    # for key, value in transactions_dict[log_line[0]].items():
                    #     print(key, value)
                continue
            if log_line[1] == "COMPLETED" and log_line[0] == last_completed_transaction:
                start_replay = True

    print("Transaction dict:", transactions_dict)

# input is leader's recovery log
def recovery(recovery_log):
    # ask the service and get the new leader recovery log
    new_leader_recovery_log = "agent/new_leader_recovery_log.txt" # service returns log file here
    last_completed_transaction = "" # to check which transactions are missed during crash
    with open(recovery_log) as file:
        for line in file:
            #print(line.rstrip())
            log_line = line.rstrip().split("$$")
            if log_line[1] == "COMPLETED":
                last_completed_transaction = log_line[0]
    replay_missed_transactions(new_leader_recovery_log, last_completed_transaction)

#recovery("agent/recovery_log.txt")