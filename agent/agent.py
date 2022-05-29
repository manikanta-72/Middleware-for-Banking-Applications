from flask import Flask, render_template, request
app = Flask(__name__)
config_file = "config.json"

with open(config_file, "r") as f:
    server_configs = json.load(f)

IP = server_configs["ip"]
PORT = server_configs["port"]
app.config["DEBUG"] = True

# Handle Read
def read(account_numbers):
    # return a dict of account number and balance along with current timestamp
    pass


# Handle COMMIT
def commit(read_account_numbers, write_account_numbers, read_timestamp, transaction_sno):
    pass
