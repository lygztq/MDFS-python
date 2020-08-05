import socket
import json

from common import load_config
from namenoode import FileSystem

normal_message = {
    "success": True,
    "message": "ack"
}
normal_message_bytes = bytes(json.dumps(normal_message).encode('utf-8'))

if __name__ == "__main__":
    # socket.setdefaulttimeout(20)
    config = load_config("config/namenode.json")
    client_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    host = ""
    port = config["client_comm_port"]
    client_server_socket.bind((host, port))
    client_server_socket.listen(config["max_client_num"])

    file_system = FileSystem(config=config["file_system_config"])
    while True:
        client, addr = client_server_socket.accept()
        print("client address: {}".format(addr))

        # recv command from client
        client_command = client.recv(4096).decode('utf-8')
        client_command = json.loads(client_command, encoding='utf-8')
        
        if client_command["command"] == "test_list":
            file_system.test_out()
            client.send(normal_message_bytes)
        if client_command["command"] == "list":
            res = file_system.list_path(client_command["args"]["path"])
            client.send(bytes(json.dumps(res).encode('utf-8')))
        elif client_command["command"] == "update_block_status":
            file_system.update_block(client_command["args"]["fid"], client_command["args"]["block_info"])
            client.send(normal_message_bytes)
        elif client_command["command"] == "update_file_status":
            file_system.update_file(client_command["args"]["fid"], client_command["args"]["file_status"])
            client.send(normal_message_bytes)
        elif client_command["command"] == "file_status":
            fid = client_command["args"].get("fid", None)
            path = client_command["args"].get("path", None)
            status = file_system.file_status(fid, path)
            client.send(bytes(json.dumps(status).encode('utf-8')))
        elif client_command["command"] == "new_block":
            blk_info = file_system.request_new_block(client_command["args"]["fid"])
            client.send(bytes(json.dumps(blk_info).encode('utf-8')))
        elif client_command["command"] == "create":
            path = client_command["args"].get("path", "")
            name = client_command["args"].get("name", None)
            res = file_system.create(path, name)
            client.send(bytes(json.dumps(res).encode('utf-8')))
        elif client_command["command"] == "delete":
            fid = client_command["args"].get("fid", None)
            path = client_command["args"].get("path", None)
            res = file_system.delete(fid, path)
            client.send(bytes(json.dumps(res).encode('utf-8')))
        elif client_command["command"] == "mkdir":
            path = client_command["args"].get("path", "")
            name = client_command["args"].get("name", None)
            res = file_system.mkdir(path, name)
            client.send(bytes(json.dumps(res).encode('utf-8')))
        else:
            res = {"success": False, "message": "invalid command"}
            client.send(bytes(json.dumps(res).encode('utf-8')))
        client.close()
