import traceback

import ast


def write_to_database(agent_instance, transactions_dict, transaction_id):
    try:
        print("Recovering:", transactions_dict)
        for k, v in transactions_dict[transaction_id]['write_set'].items():
            agent_instance.update_account(k, v)

    except Exception as e:
        traceback.print_exc()


def replay_missed_transactions(agent_instance, new_leader_recovery_log, last_completed_transaction):
    start_replay = False
    transactions_dict = dict()
    with open(new_leader_recovery_log) as file:
        for line in file:
            log_line = line.rstrip().split("$$")
            if start_replay:
                if log_line[1] == "START":
                    transactions_dict[log_line[0]] = ast.literal_eval(log_line[2])
                elif log_line[1] == "ABORT":
                    transactions_dict.pop(log_line[0], None)
                elif log_line[1] == "COMPLETED":
                    # write dict values to database
                    write_to_database(agent_instance, transactions_dict, log_line[0])
                    transactions_dict.pop(log_line[0], None)
                    # for key, value in transactions_dict[log_line[0]].items():
                    #     print(key, value)

                with open('recovery_log.txt', 'a') as f:
                    f.write(line)
                continue
            if log_line[1] == "COMPLETED" and log_line[0] == last_completed_transaction:
                start_replay = True

    print("Transaction dict:", transactions_dict)


# input is leader's recovery log
def recovery(agent_instance):
    # ask the service and get the new leader recovery log
    recovery_log = "recovery_log.txt"
    new_leader_recovery_log = "new_leader_recovery_log.txt"  # service returns log file here
    last_completed_transaction = ""  # to check which transactions are missed during crash
    with open(recovery_log) as file:
        for line in file:
            # print(line.rstrip())
            log_line = line.rstrip().split("$$")
            if log_line[1] == "COMPLETED":
                last_completed_transaction = log_line[0]
    replay_missed_transactions(agent_instance, new_leader_recovery_log, last_completed_transaction)

# recovery("agent/recovery_log.txt")
