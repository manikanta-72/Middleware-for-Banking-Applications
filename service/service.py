import requests
import time


class Service:
    def __init__(self, url):
        self.URL = url
        self.number_of_nodes = 3
        self.node_ports = [8000, 8001, 8002]
        self.client_ports = [5000]
        self.current_leader = 8000

    def polling(self):
        while True:
            time.sleep(5)
            api_call = self.URL + ':' + str(self.current_leader) + '/poll/'
            r = requests.post(api_call)
            if r.json()['status'] == 200:
                print(r)
            else:
                old_leader = self.current_leader
                self.current_leader = ((int(self.current_leader)-8000 + 1) % self.number_of_nodes) + 8000

                for node in self.node_ports:
                    if node != self.current_leader and node != old_leader:
                        # sending notification to other participants about the leader change
                        api_call = self.URL + ':' + str(node) + '/leader_changed/'
                        r = requests.post(api_call, json={'leader': self.current_leader})

                api_call = self.URL + ':' + str(self.current_leader) + '/become_leader/'
                # post request to set next leader
                r = requests.post(api_call, json={'leader': self.current_leader})
                # send a notification to clients to send transactions to new leader
                if r.status_code == 200:
                    for node in self.client_ports:
                        client_api_call = self.URL + ':' + str(node) + '/leader_changed/'
                        c = requests.post(client_api_call, json={'leader': self.current_leader})

    def node_recover(self, port):
        # Halt leader from processing transactions
        api_call = self.URL + ':' + str(self.current_leader) + '/stop_receiving/'
        r = requests.get(api_call)
        print(port)
        api_call = self.URL + ':' + str(self.current_leader) + '/replication_log/'
        # get request to get latest replication log
        r = requests.get(api_call)
        data = r.json()['data']
        if r.status_code == 200:
            # receive the file from leader and send to the recovering node
            api_call = self.URL + ':' + str(port) + '/sync_log/'
            r = requests.post(api_call, json={'data': data})
            # resume leader from processing transactions
            api_call = self.URL + ':' + str(self.current_leader) + '/resume_receiving/'
            r = requests.get(api_call)