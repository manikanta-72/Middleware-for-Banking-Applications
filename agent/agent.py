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


# # Handle COMMIT
# def commit(read_account_numbers, write_account_numbers, read_timestamp, transaction_sno):
#     pass

from flask import Flask
from flask import request
import json

def database_connect():
	import psycopg2
	conn = psycopg2.connect(database="test_db", user='postgres', password='', host='localhost', port='5432')
	cursor = conn.cursor()
	return cursor

app = Flask(__name__)

@app.route('/read/', methods=['POST'])
def index(cursor):
	#print(request.read_set)
	read_set_test = [{"acc_number": 1},{"acc_number": 2},{"acc_number": 3}]
	#print(json.dumps(read_set_test))
	for acc in read_set_test:
		cursor.execute("select * from testdb where id={acc['acc_number']}")
		print(acc['acc_number'])
		result = cursor.fetchall()
		print(result)
	return 'Hello, Flask!'

if __name__ == '__main__':
	cursor = database_connect()
	app.run(cursor, debug=True)

'''
[
	{
		"acc_number": 12345
	},
	{
		"acc_number": 34567
	},
	{
		"acc_number": 56789
	}
]

[
	{
		"acc_number": 12345,
		"balance": 5
	},
	{
		"acc_number": 34567,
		"balance": 5
	},
	{
		"acc_number": 56789,
		"balance": 5
	}
]
'''