#!/usr/bin/python3
import datetime
import random
import socket
from dataclasses import dataclass
import sys
from threading import Thread
from time import sleep
from collections import Counter
import rpyc
from rpyc.utils.factory import DiscoveryError
from rpyc.utils.registry import (DEFAULT_PRUNING_TIMEOUT, REGISTRY_PORT,
                                 UDPRegistryClient, UDPRegistryServer)
from rpyc.utils.server import ThreadedServer

from constants import _ACTION_KEY, _ACTION_VALUE, _PORT, _STATE_KEY, _STATE_VALUE
from rpc import RPCService

# global variables
system_start = datetime.datetime.now()
date_time = datetime.datetime.now()


@dataclass
class RegistryService():
    server = UDPRegistryServer(
        port=REGISTRY_PORT, pruning_timeout=DEFAULT_PRUNING_TIMEOUT)

    def start(self):
        self.server.start()


def get_server_list() -> list:
    registrar = UDPRegistryClient(port=REGISTRY_PORT)
    sleep(2)
    servers_lst = registrar.discover("RPC")

    return servers_lst


def on_disconnect(self, conn):
    print("\ndisconneced on {}".format(date_time))


def on_connect(self, conn):
    print("\nconnected on {}".format(date_time))


if __name__ == '__main__':
    # if(len(sys.argv) < 2):
    #     print("Missing Args")
    #     sys.exit(1)
    # N = int(sys.argv[1])

    N = 4

    # Get localhost ip
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)

    registry = RegistryService()
    t = Thread(target=registry.start)
    t.daemon = True
    t.start()

    nodes = [_PORT + i for i in range(N)]
    primary = random.choice(nodes)

    print(primary)
    print(nodes)

    # Init node server
    for i in range(N):
        service = RPCService(id=i+1, nodes=nodes, ip=ip,
                             port=int(nodes[i]), primary=primary)
        server = ThreadedServer(
            service, port=int(nodes[i]), auto_register=True)
        t = Thread(target=server.start, name=str(i))
        t.daemon = True
        t.start()

    while True:
        # command = input("\nCommand: ")
        # cmd = command.split(" ")
        cmd = ["g-kill", "3"]

        if(cmd[0].lower() == "actual-order"):
            if(cmd[1].lower() == "attack" or cmd[1].lower() == "retreat"):
                node_service = []
                try:
                    # Get nodes server list
                    servers_lst = get_server_list()
                    print(f'length of list {len(servers_lst)}')

                    conn = rpyc.connect(servers_lst[0][0], servers_lst[0][1])
                    primary_port = int(conn.root.get_primary())

                    # Broadcast action info to secondary
                    conn = rpyc.connect(servers_lst[0][0], primary_port)
                    conn.root.send_order_from_primary(_ACTION_KEY[cmd[1]])

                    # Information exchange among secondary
                    for node in servers_lst:
                        node_ip, node_port = node
                        if (node_port != primary_port):
                            s_conn = rpyc.connect(node_ip, node_port)
                            s_conn.root.share_action_info()
                        s_conn.close()

                    # Secondary votes the final action
                    for node in servers_lst:
                        node_ip, node_port = node
                        if (node_port != primary_port):
                            s_conn = rpyc.connect(node_ip, node_port)
                            s_conn.root.vote_final_action()
                        s_conn.close()

                    # Print The Result
                    node_detail_lst = []
                    f_cnt = 0
                    for server in servers_lst:
                        server_ip, server_port = server
                        conn = rpyc.connect(server_ip, server_port)
                        get_detail = conn.root.get_detail()
                        ide = "primary" if get_detail[3] == get_detail[4] else "secondary"
                        if get_detail[2] == 1:
                            f_cnt += 1
                        node_detail_lst.append(get_detail)
                        # G1, primary, majority=attack, state=NF
                        print(
                            f"G{get_detail[0]}, {ide}, majority={_ACTION_VALUE[get_detail[1]]}, state={_STATE_VALUE[get_detail[2]]}")
                        conn.close()

                    final_order_key = Counter(
                        [action[1] for action in node_detail_lst]).most_common(1)[0][0]
                    final_order = _ACTION_VALUE[final_order_key]

                    if len(node_detail_lst) >= ((3*f_cnt)+1):
                        if f_cnt == 0:
                            print(
                                f'Execute order:{final_order}!, Non-faulty nodes in System, {len(node_detail_lst)-1} out of {len(node_detail_lst)} quorum suggest {final_order}')

                        else:
                            print(
                                f'Execute order:{final_order}, {f_cnt} faulty node in System, {len(node_detail_lst)-1-f_cnt} out of {len(node_detail_lst)} quorum suggest {final_order}')
                    else:
                        print(
                            f"Execute order: cannot be determined – not enough generals in the system! {f_cnt} faulty node in the system - {len(node_detail_lst)-1-f_cnt} out of {len(node_detail_lst)} quorum not consistent")
                except DiscoveryError:
                    print(f"DiscoveryError :{DiscoveryError}")
            else:
                print(
                    "Incorrect order, please proposes an order to the primary again (\"attack\" or \"retreat\")")
                continue
        elif (cmd[0].lower() == "g-state"):
            try:
                servers_lst = get_server_list()

                if len(cmd) == 1:
                    for server in servers_lst:
                        server_ip, server_port = server
                        conn = rpyc.connect(server_ip, server_port)
                        get_detail = conn.root.get_detail()
                        ide = "primary" if get_detail[3] == get_detail[4] else "secondary"
                        print(
                            f"G{get_detail[0]}, {ide}, state={_STATE_VALUE[get_detail[2]]}")
                        conn.close()
                elif (len(cmd) == 3) and (cmd[1].isnumeric()) and ((cmd[2] == "faulty") or (cmd[2] == "non-faulty")):
                    target_modify_state_id = int(cmd[1])
                    target_state = cmd[2]
                    for server in servers_lst:
                        server_ip, server_port = server
                        conn = rpyc.connect(server_ip, server_port)
                        get_detail = conn.root.get_detail()
                        if get_detail[0] == target_modify_state_id:
                            conn.root.set_state(_STATE_KEY[target_state])
                            get_detail = conn.root.get_detail()
                        print(
                            f"G{get_detail[0]}, state={_STATE_VALUE[get_detail[2]]}")

                        conn.close()
                else:
                    continue
            except DiscoveryError:
                print(f"DiscoveryError :{DiscoveryError}")

        elif (cmd[0].lower() == "g-kill") and (len(cmd) == 2) and (cmd[1].isnumeric()):
            target_kill_id = int(cmd[1])
            target_kill_port = 0
            port_lst = []
            is_primary = False

            try:
                registrar = UDPRegistryClient(port=REGISTRY_PORT)
                servers_lst = registrar.discover("RPC")
                sleep(2)

                for server in servers_lst:
                    server_ip, server_port = server
                    conn = rpyc.connect(server_ip, server_port)
                    get_detail = conn.root.get_detail()
                    if(get_detail[0] == target_kill_id):
                        target_kill_port = server_port
                        if(get_detail[3] == get_detail[4]):
                            is_primary = True
                    else:
                        port_lst.append(server[1])

                # Show Target port and position
                print(f"{target_kill_port}: {is_primary}")

                if target_kill_port != 0:
                    ip = servers_lst[0][0]
                    if(is_primary):
                        new_primary_port = random.choice(port_lst)
                        for port in port_lst:
                            conn = rpyc.connect(ip, port)
                            conn.root.set_primary(new_primary_port)

                    registrar.unregister(target_kill_port)
                    conn = rpyc.connect(ip, target_kill_port)
                    try:
                        conn.root.kill()
                    except EOFError:
                        print(f'{target_kill_id} Connection Closed')
                
                servers_lst = registrar.discover("RPC")
                sleep(2)
                for server in servers_lst:
                    server_ip, server_port = server
                    conn = rpyc.connect(server_ip, server_port)
                    get_detail = conn.root.get_detail()
                    print(
                        f"G{get_detail[0]}, state={_STATE_VALUE[get_detail[2]]}")
                    conn.close()


            except DiscoveryError:
                print(f"DiscoveryError :{DiscoveryError}")
        elif (cmd[0].lower() == "g-add") and (len(cmd) == 2):
            pass
        else:
            pass
