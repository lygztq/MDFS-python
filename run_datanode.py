import socket
import json
import logging

from common import load_config, file_default_config
from datanode import DataNode

if __name__ == "__main__":
    # socket.setdefaulttimeout(20)
    config = load_config("config/datanode.json")
    client_datanode_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    host = ""
    port = config["client_comm_port"]
    client_datanode_socket.bind((host, port))
    client_datanode_socket.listen(config["max_client_num"])

    datanode_instance = DataNode(config)
    while True:
        client, addr = client_datanode_socket.accept()
        print("Connect from {}".format(addr))

        # receive command from client
        client_command = client.recv(2048).decode('utf-8')

        # accept command successfully
        client.send(bytes("ack", encoding="utf-8"))
        client_command = json.loads(client_command, encoding="utf-8")
        blockinfo = client_command.get("block_info", None)
        block = None
        ret_block = None
        
        if client_command["command"] == "write_whole_block":
            # recv block and return ack signal
            length = client_command["block_info"]["length"]
            total_recd = 0
            chunks = []
            success = True
            while total_recd < length:
                chunk = client.recv(min(2048, length - total_recd))
                if chunk == b"":
                    success = False
                    break
                chunks.append(chunk)
                total_recd += len(chunk)
            if not success:
                client.close()
                continue
            block = b''.join(chunks)
            client.send(b"ack")
            result = datanode_instance.create_and_recv_block(blockinfo, block)
        elif client_command["command"] == "write_block":
            # recv block and return ack signal
            length = client_command["block_info"]["length"]
            total_recd = 0
            chunks = []
            success = True
            while total_recd < length:
                chunk = client.recv(min(2048, length - total_recd))
                if chunk == b"":
                    success = False
                    break
                chunks.append(chunk)
                total_recd += len(chunk)
            if not success:
                client.close()
                continue
            block = b''.join(chunks)
            client.send(b"ack")
            result = datanode_instance.recv_block(blockinfo, block)
        elif client_command["command"] == "read_block":
            result, ret_block = datanode_instance.read_block(blockinfo)
        else:
            result = {"success": False, "message": "Unknown command"}
        
        # client ready for result message
        client.recv(2048)

        # send result message
        result = bytes(json.dumps(result), encoding=("utf-8"))
        client.send(result)

        # client recv result message successfully
        a = client.recv(2048)
        logging.debug(a)

        # for read block
        if ret_block is not None:
            length = len(ret_block)
            total_sent = 0
            success = True
            while total_sent < length:
                sent = client.send(ret_block[total_sent:])
                if sent == 0:
                    success = False
                    break
                total_sent += sent
            if not success:
                client.close()
                continue

        client.close()
